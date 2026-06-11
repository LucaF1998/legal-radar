import streamlit as st
import pandas as pd
import feedparser
import time
import requests
import os
import re
import json
import html
import logging
from contextlib import contextmanager
from bs4 import BeautifulSoup
from datetime import datetime
import psycopg2
import psycopg2.errors
import psycopg2.extras
import bcrypt
from typing import List, Dict, Tuple, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# --- 1. CLASSE ARCHITETTURALE DATABASE (POSTGRESQL MULTI-TENANT) ---
class LegalRadarDB:
    def __init__(self, db_url: str):
        self.db_url = db_url

    # --- CONNESSIONE: context manager con commit/rollback/close GARANTITI ---
    @contextmanager
    def get_cursor(self, dict_cursor: bool = False):
        conn = psycopg2.connect(self.db_url)
        factory = psycopg2.extras.DictCursor if dict_cursor else None
        cur = conn.cursor(cursor_factory=factory)
        try:
            yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    def init_db(self) -> None:
        commands = (
            """
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(150) UNIQUE NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                role VARCHAR(50) DEFAULT 'user'
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS sources (
                id SERIAL PRIMARY KEY,
                nome VARCHAR(150) NOT NULL,
                url TEXT UNIQUE NOT NULL,
                area VARCHAR(150),
                macro VARCHAR(150),
                tipo_fonte VARCHAR(50) DEFAULT 'Ufficiale',
                tipo_ingestion VARCHAR(50) DEFAULT 'rss',
                ultima_sync TIMESTAMP,
                ultimo_esito VARCHAR(20),
                ultimo_messaggio TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS articles (
                id SERIAL PRIMARY KEY,
                titolo TEXT NOT NULL,
                link TEXT UNIQUE NOT NULL,
                preview TEXT,
                macro VARCHAR(150),
                area VARCHAR(150),
                fonte VARCHAR(150),
                data_scansione TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data_pubblicazione TIMESTAMP,
                riassunto_ai TEXT,
                rilevanza VARCHAR(20),
                tipo_atto VARCHAR(30),
                tema VARCHAR(100)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS bookmarks (
                user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                article_id INTEGER REFERENCES articles(id) ON DELETE CASCADE,
                PRIMARY KEY (user_id, article_id)
            )
            """,
            # --- AGGIUNTA TABELLA: PREFERENZE PERSONALI FONTI (Punto 1) ---
            """
            CREATE TABLE IF NOT EXISTS user_source_preferences (
                user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                source_id INTEGER REFERENCES sources(id) ON DELETE CASCADE,
                is_active BOOLEAN DEFAULT TRUE,
                PRIMARY KEY (user_id, source_id)
            )
            """,
            # --- AGGIUNTA TABELLA: STATO LETTO/NON LETTO PER UTENTE ---
            """
            CREATE TABLE IF NOT EXISTS user_article_status (
                user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                article_id INTEGER REFERENCES articles(id) ON DELETE CASCADE,
                letto BOOLEAN DEFAULT FALSE,
                letto_il TIMESTAMP,
                PRIMARY KEY (user_id, article_id)
            )
            """,
            # --- INDICI PER PERFORMANCE (idempotenti) ---
            "CREATE INDEX IF NOT EXISTS idx_articles_macro_data ON articles(macro, data_scansione DESC)",
            "CREATE INDEX IF NOT EXISTS idx_articles_fonte ON articles(fonte)",
            "CREATE INDEX IF NOT EXISTS idx_bookmarks_user ON bookmarks(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_usp_user ON user_source_preferences(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_uas_user_letto ON user_article_status(user_id, letto)",
            # --- MIGRAZIONE: aggiunge le colonne a sources esistenti senza distruggere dati ---
            "ALTER TABLE sources ADD COLUMN IF NOT EXISTS tipo_fonte VARCHAR(50) DEFAULT 'Ufficiale'",
            "ALTER TABLE sources ADD COLUMN IF NOT EXISTS tipo_ingestion VARCHAR(50) DEFAULT 'rss'",
            "ALTER TABLE sources ADD COLUMN IF NOT EXISTS ultima_sync TIMESTAMP",
            "ALTER TABLE sources ADD COLUMN IF NOT EXISTS ultimo_esito VARCHAR(20)",
            "ALTER TABLE sources ADD COLUMN IF NOT EXISTS ultimo_messaggio TEXT",
            "ALTER TABLE articles ADD COLUMN IF NOT EXISTS riassunto_ai TEXT",
            "ALTER TABLE articles ADD COLUMN IF NOT EXISTS rilevanza VARCHAR(20)",
            "ALTER TABLE articles ADD COLUMN IF NOT EXISTS tipo_atto VARCHAR(30)",
            "ALTER TABLE articles ADD COLUMN IF NOT EXISTS tema VARCHAR(100)",
            "ALTER TABLE articles ADD COLUMN IF NOT EXISTS data_pubblicazione TIMESTAMP",
            "CREATE INDEX IF NOT EXISTS idx_articles_tipoatto ON articles(tipo_atto, data_scansione DESC)",
            # --- TABELLA KEYWORD DEGLI ALERT (gestibile dagli admin) ---
            """
            CREATE TABLE IF NOT EXISTS alert_keywords (
                id SERIAL PRIMARY KEY,
                keyword VARCHAR(100) UNIQUE NOT NULL
            )
            """,
            # Seed iniziale: le keyword storiche, inserite solo se la tabella è vuota
            """
            INSERT INTO alert_keywords (keyword)
            SELECT k FROM (VALUES ('sanzion'), ('ordinanza'), ('condanna'), ('violazion'),
                                  ('scadenza'), ('obbligo'), ('divieto'), ('sentenza')) AS v(k)
            WHERE NOT EXISTS (SELECT 1 FROM alert_keywords)
            """,
        )
        try:
            with self.get_cursor() as cur:
                for command in commands:
                    cur.execute(command)
        except Exception as e:
            logging.error("Errore critico di connessione al database: %s", e)
            st.error(f"Errore critico di connessione al database: {e}")

    def registra_utente(self, username: str, password: str) -> bool:
        hashed = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        try:
            with self.get_cursor() as cur:
                cur.execute("INSERT INTO users (username, password_hash) VALUES (%s, %s)", (username.strip(), hashed))
            return True
        except psycopg2.errors.UniqueViolation:
            return False
        except Exception as e:
            logging.error("Errore registrazione utente: %s", e)
            return False

    def verifica_utente(self, username: str, password: str) -> Optional[Dict]:
        with self.get_cursor(dict_cursor=True) as cur:
            cur.execute("SELECT * FROM users WHERE username = %s", (username.strip(),))
            user = cur.fetchone()
        if user and bcrypt.checkpw(password.encode('utf-8'), user['password_hash'].encode('utf-8')):
            return dict(user)
        return None

    # --- AGGIUNTA: GESTIONE RUOLI (admin / user) ---
    def conta_admin(self) -> int:
        """Quanti admin esistono. Serve per il bootstrap del primo admin."""
        with self.get_cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM users WHERE role = 'admin'")
            n = cur.fetchone()[0]
        return n

    def bootstrap_primo_admin(self) -> Optional[str]:
        """Se non esiste alcun admin, promuove il primo utente registrato (id minimo).
        Ritorna lo username promosso, oppure None se non c'era nulla da fare."""
        if self.conta_admin() > 0:
            return None
        with self.get_cursor() as cur:
            cur.execute("SELECT id, username FROM users ORDER BY id ASC LIMIT 1")
            primo = cur.fetchone()
            if not primo:
                return None
            cur.execute("UPDATE users SET role = 'admin' WHERE id = %s", (primo[0],))
        logging.info("Bootstrap: '%s' promosso ad admin (nessun admin presente).", primo[1])
        return primo[1]

    def lista_utenti(self) -> List[Dict]:
        with self.get_cursor(dict_cursor=True) as cur:
            cur.execute("SELECT id, username, role FROM users ORDER BY id ASC")
            utenti = cur.fetchall()
        return [dict(u) for u in utenti]

    def imposta_ruolo(self, target_user_id: int, nuovo_ruolo: str) -> bool:
        """Imposta il ruolo di un utente. Impedisce di rimuovere l'ultimo admin."""
        if nuovo_ruolo not in ("admin", "user"):
            return False
        # Salvaguardia: non lasciare il sistema senza admin
        if nuovo_ruolo == "user":
            with self.get_cursor() as cur:
                cur.execute("SELECT role FROM users WHERE id = %s", (target_user_id,))
                row = cur.fetchone()
            if row and row[0] == "admin" and self.conta_admin() <= 1:
                return False  # è l'ultimo admin: non si declassa
        with self.get_cursor() as cur:
            cur.execute("UPDATE users SET role = %s WHERE id = %s", (nuovo_ruolo, target_user_id))
        return True

    def carica_fonti(self) -> List[Dict]:
        with self.get_cursor(dict_cursor=True) as cur:
            cur.execute("SELECT * FROM sources ORDER BY nome ASC")
            fonti = cur.fetchall()
        return [dict(f) for f in fonti]

    # --- AGGIUNTA: LETTURA DELLE FONTI CON STATO DI ACCENSIONE UTENTE ---
    def carica_fonti_con_preferenze(self, user_id: int) -> List[Dict]:
        query = """
            SELECT s.*, COALESCE(usp.is_active, TRUE) as utente_attiva
            FROM sources s
            LEFT JOIN user_source_preferences usp ON s.id = usp.source_id AND usp.user_id = %s
            ORDER BY s.nome ASC
        """
        with self.get_cursor(dict_cursor=True) as cur:
            cur.execute(query, (user_id,))
            fonti = cur.fetchall()
        return [dict(f) for f in fonti]

    # --- AGGIUNTA: SALVATAGGIO ACCENSIONE/SPEGNIMENTO PERSONALE ---
    def imposta_preferenza_fonte(self, user_id: int, source_id: int, is_active: bool) -> None:
        query = """
            INSERT INTO user_source_preferences (user_id, source_id, is_active)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id, source_id) 
            DO UPDATE SET is_active = EXCLUDED.is_active
        """
        with self.get_cursor() as cur:
            cur.execute(query, (user_id, source_id, is_active))

    def aggiungi_fonte(self, nome: str, url: str, area: str, macro: str,
                       tipo_fonte: str = "Ufficiale", tipo_ingestion: str = "rss") -> bool:
        try:
            with self.get_cursor() as cur:
                cur.execute(
                    "INSERT INTO sources (nome, url, area, macro, tipo_fonte, tipo_ingestion) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (url) DO NOTHING",
                    (nome, url, area, macro, tipo_fonte, tipo_ingestion),
                )
            return True
        except Exception as e:
            logging.error("Errore aggiunta fonte: %s", e)
            return False

    def rimuovi_fonte(self, fonte_id: int) -> None:
        with self.get_cursor() as cur:
            cur.execute("DELETE FROM sources WHERE id = %s", (fonte_id,))

    def imposta_tipo_fonte(self, fonte_id: int, tipo_fonte: str) -> None:
        """Aggiorna il tipo di una fonte (Ufficiale/Editoriale)."""
        if tipo_fonte not in ("Ufficiale", "Editoriale"):
            return
        with self.get_cursor() as cur:
            cur.execute("UPDATE sources SET tipo_fonte = %s WHERE id = %s", (tipo_fonte, fonte_id))

    def reset_archivio(self) -> int:
        """Azzera l'archivio: articoli + bookmark + stato letto. Riparte da t0 (ID da 1).
        NON tocca utenti, fonti né preferenze. Ritorna il numero di articoli rimossi."""
        with self.get_cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM articles")
            n = cur.fetchone()[0]
            # TRUNCATE con CASCADE svuota anche bookmark e user_article_status (riferiscono articles);
            # RESTART IDENTITY riazzera i contatori degli ID.
            cur.execute("TRUNCATE TABLE articles RESTART IDENTITY CASCADE")
        logging.info("Reset archivio: %d articoli rimossi.", n)
        return n

    # --- AGGIUNTA: SALUTE DELLE FONTI ---
    def registra_esito_sync(self, source_id: int, esito: str, messaggio: str = "") -> None:
        """Registra l'esito dell'ultima sincronizzazione di una fonte.
        esito atteso: 'ok' | 'vuoto' | 'errore'."""
        query = """
            UPDATE sources
            SET ultima_sync = CURRENT_TIMESTAMP,
                ultimo_esito = %s,
                ultimo_messaggio = %s
            WHERE id = %s
        """
        with self.get_cursor() as cur:
            cur.execute(query, (esito, messaggio[:500] if messaggio else None, source_id))

    def carica_salute_fonti(self) -> List[Dict]:
        """Stato di salute di ogni fonte + n. articoli negli ultimi 30 giorni."""
        query = """
            SELECT s.id, s.nome, s.url, s.area, s.macro, s.tipo_fonte, s.tipo_ingestion,
                   s.ultima_sync, s.ultimo_esito, s.ultimo_messaggio,
                   COUNT(a.id) FILTER (
                       WHERE a.data_scansione >= CURRENT_TIMESTAMP - INTERVAL '30 days'
                   ) AS articoli_30gg
            FROM sources s
            LEFT JOIN articles a ON a.fonte = s.nome
            GROUP BY s.id, s.nome, s.url, s.area, s.macro, s.tipo_fonte, s.tipo_ingestion,
                     s.ultima_sync, s.ultimo_esito, s.ultimo_messaggio
            ORDER BY s.nome ASC
        """
        with self.get_cursor(dict_cursor=True) as cur:
            cur.execute(query)
            righe = cur.fetchall()
        return [dict(r) for r in righe]

    def salva_articoli_storico(self, articoli_lista: List[Dict]) -> None:
        query = """
            INSERT INTO articles (titolo, link, preview, macro, area, fonte, riassunto_ai, rilevanza, tipo_atto, tema, data_pubblicazione) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
            ON CONFLICT (link) DO NOTHING
        """
        params = [
            (art['Titolo'], art['Link'], art['Preview'], art['Macro'], art['Area'], art['Fonte'],
             art.get('RiassuntoAI'), art.get('Rilevanza'), art.get('TipoAtto'), art.get('Tema'),
             art.get('DataPubblicazione'))
            for art in articoli_lista
        ]
        with self.get_cursor() as cur:
            cur.executemany(query, params)

    def aggiorna_riassunto_articolo(self, article_id: int, riassunto: str, rilevanza: Optional[str]) -> None:
        """Salva un micro-riassunto generato tardivamente (per articoli già in archivio)."""
        with self.get_cursor() as cur:
            cur.execute(
                "UPDATE articles SET riassunto_ai = %s, rilevanza = %s WHERE id = %s",
                (riassunto[:600] if riassunto else None, rilevanza, article_id)
            )

    # --- MODIFICA: ESTRAZIONE ARCHIVIO FILTRATO SULLE PREFERENZE UTENTE + STATO LETTO ---
    def estrai_archivio(self, filtro_macro: str, user_id: int, ricerca_testo: str = "") -> List[Dict]:
        query = """
            SELECT a.*, COALESCE(uas.letto, FALSE) AS letto, src.tipo_fonte
            FROM articles a
            LEFT JOIN user_article_status uas
                ON uas.article_id = a.id AND uas.user_id = %s
            LEFT JOIN sources src ON src.nome = a.fonte
            WHERE a.macro = %s 
            AND a.fonte NOT IN (
                SELECT s.nome FROM sources s
                JOIN user_source_preferences usp ON s.id = usp.source_id
                WHERE usp.user_id = %s AND usp.is_active = FALSE
            )
        """
        params = [user_id, filtro_macro, user_id]
        if ricerca_testo:
            query += " AND (a.titolo ILIKE %s OR a.preview ILIKE %s OR a.area ILIKE %s)"
            text_param = f"%{ricerca_testo}%"
            params.extend([text_param, text_param, text_param])
        query += " ORDER BY COALESCE(a.data_pubblicazione, a.data_scansione) DESC LIMIT 100"
        with self.get_cursor(dict_cursor=True) as cur:
            cur.execute(query, params)
            articoli = cur.fetchall()
        return [dict(a) for a in articoli]

    def estrai_per_tipo_atto(self, tipo_atto: str, user_id: int, ricerca_testo: str = "",
                             tema: Optional[str] = None, limite: int = 100) -> List[Dict]:
        """Estrae articoli per categoria AI (sentenza/provvedimento/news), con filtro tema opzionale,
        rispettando le fonti spente dall'utente e portando stato letto + tipo fonte."""
        query = """
            SELECT a.*, COALESCE(uas.letto, FALSE) AS letto, src.tipo_fonte,
                   CASE
                       WHEN LOWER(COALESCE(src.tipo_fonte,'')) = 'editoriale' THEN 'news'
                       ELSE COALESCE(a.tipo_atto, 'provvedimento')
                   END AS tipo_atto_eff
            FROM articles a
            LEFT JOIN user_article_status uas
                ON uas.article_id = a.id AND uas.user_id = %s
            LEFT JOIN sources src ON src.nome = a.fonte
            WHERE CASE
                      WHEN LOWER(COALESCE(src.tipo_fonte,'')) = 'editoriale' THEN 'news'
                      ELSE COALESCE(a.tipo_atto, 'provvedimento')
                  END = %s
            AND a.fonte NOT IN (
                SELECT s.nome FROM sources s
                JOIN user_source_preferences usp ON s.id = usp.source_id
                WHERE usp.user_id = %s AND usp.is_active = FALSE
            )
        """
        params = [user_id, tipo_atto, user_id]
        if tema:
            query += " AND a.tema = %s"
            params.append(tema)
        if ricerca_testo:
            query += " AND (a.titolo ILIKE %s OR a.preview ILIKE %s OR a.area ILIKE %s)"
            text_param = f"%{ricerca_testo}%"
            params.extend([text_param, text_param, text_param])
        query += " ORDER BY COALESCE(a.data_pubblicazione, a.data_scansione) DESC LIMIT %s"
        params.append(limite)
        with self.get_cursor(dict_cursor=True) as cur:
            cur.execute(query, params)
            articoli = cur.fetchall()
        return [dict(a) for a in articoli]

    def lista_temi(self, tipo_atto: Optional[str] = None) -> List[str]:
        """Elenco dei temi presenti in archivio (per popolare il filtro)."""
        if tipo_atto:
            query = "SELECT DISTINCT tema FROM articles WHERE tema IS NOT NULL AND tipo_atto = %s ORDER BY tema ASC"
            args = (tipo_atto,)
        else:
            query = "SELECT DISTINCT tema FROM articles WHERE tema IS NOT NULL ORDER BY tema ASC"
            args = ()
        with self.get_cursor() as cur:
            cur.execute(query, args)
            righe = cur.fetchall()
        return [r[0] for r in righe]

    # --- METODI PER LA DASHBOARD "PRIMA PAGINA" ---
    def _filtro_fonti_attive(self, user_id: int) -> str:
        """Frammento SQL riusabile per escludere le fonti spente dall'utente."""
        return """a.fonte NOT IN (
            SELECT s.nome FROM sources s
            JOIN user_source_preferences usp ON s.id = usp.source_id
            WHERE usp.user_id = %s AND usp.is_active = FALSE
        )"""

    def estrai_in_evidenza(self, user_id: int, limite: int = 4) -> List[Dict]:
        """Articoli per apertura + griglia: priorità ad alta rilevanza e non letti, poi recenti."""
        query = f"""
            SELECT a.*, COALESCE(uas.letto, FALSE) AS letto, src.tipo_fonte
            FROM articles a
            LEFT JOIN user_article_status uas ON uas.article_id = a.id AND uas.user_id = %s
            LEFT JOIN sources src ON src.nome = a.fonte
            WHERE {self._filtro_fonti_attive(user_id)}
            ORDER BY
                CASE WHEN a.rilevanza = 'alta' THEN 0 ELSE 1 END,
                COALESCE(uas.letto, FALSE) ASC,
                COALESCE(a.data_pubblicazione, a.data_scansione) DESC
            LIMIT %s
        """
        with self.get_cursor(dict_cursor=True) as cur:
            cur.execute(query, (user_id, user_id, limite))
            righe = cur.fetchall()
        return [dict(r) for r in righe]

    def estrai_ultima_ora(self, user_id: int, limite: int = 5) -> List[Dict]:
        """Flusso cronologico grezzo, indipendente dalla rilevanza."""
        query = f"""
            SELECT a.*, src.tipo_fonte
            FROM articles a
            LEFT JOIN sources src ON src.nome = a.fonte
            WHERE {self._filtro_fonti_attive(user_id)}
            ORDER BY COALESCE(a.data_pubblicazione, a.data_scansione) DESC
            LIMIT %s
        """
        with self.get_cursor(dict_cursor=True) as cur:
            cur.execute(query, (user_id, limite))
            righe = cur.fetchall()
        return [dict(r) for r in righe]

    def estrai_per_tema_blocco(self, user_id: int, tema: str, limite: int = 3) -> List[Dict]:
        """Ultimi articoli di un tema specifico, per i blocchi tematici."""
        query = f"""
            SELECT a.*, src.tipo_fonte
            FROM articles a
            LEFT JOIN sources src ON src.nome = a.fonte
            WHERE a.tema = %s AND {self._filtro_fonti_attive(user_id)}
            ORDER BY COALESCE(a.data_pubblicazione, a.data_scansione) DESC
            LIMIT %s
        """
        with self.get_cursor(dict_cursor=True) as cur:
            cur.execute(query, (tema, user_id, limite))
            righe = cur.fetchall()
        return [dict(r) for r in righe]

    def temi_piu_presenti(self, user_id: int, limite: int = 3) -> List[str]:
        """I temi con più articoli (per scegliere quali blocchi tematici mostrare)."""
        query = f"""
            SELECT a.tema, COUNT(*) AS n
            FROM articles a
            WHERE a.tema IS NOT NULL AND {self._filtro_fonti_attive(user_id)}
            GROUP BY a.tema
            ORDER BY n DESC
            LIMIT %s
        """
        with self.get_cursor(dict_cursor=True) as cur:
            cur.execute(query, (user_id, limite))
            righe = cur.fetchall()
        return [r['tema'] for r in righe]

    def aggiungi_bookmark(self, user_id: int, article_id: int) -> None:
        try:
            with self.get_cursor() as cur:
                cur.execute("INSERT INTO bookmarks (user_id, article_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (user_id, article_id))
        except Exception as e:
            logging.error("Errore aggiunta bookmark: %s", e)

    def rimuovi_bookmark(self, user_id: int, article_id: int) -> None:
        with self.get_cursor() as cur:
            cur.execute("DELETE FROM bookmarks WHERE user_id = %s AND article_id = %s", (user_id, article_id))

    def estrai_bookmarks(self, user_id: int, ricerca_testo: str = "") -> List[Dict]:
        query = """
            SELECT a.*, COALESCE(uas.letto, FALSE) AS letto, src.tipo_fonte
            FROM articles a 
            JOIN bookmarks b ON a.id = b.article_id 
            LEFT JOIN user_article_status uas
                ON uas.article_id = a.id AND uas.user_id = %s
            LEFT JOIN sources src ON src.nome = a.fonte
            WHERE b.user_id = %s
        """
        params = [user_id, user_id]
        if ricerca_testo:
            query += " AND (a.titolo ILIKE %s OR a.preview ILIKE %s)"
            text_param = f"%{ricerca_testo}%"
            params.extend([text_param, text_param])
        query += " ORDER BY COALESCE(a.data_pubblicazione, a.data_scansione) DESC"
        with self.get_cursor(dict_cursor=True) as cur:
            cur.execute(query, params)
            salvati = cur.fetchall()
        return [dict(s) for s in salvati]

    def check_bookmark_esiste(self, user_id: int, article_id: int) -> bool:
        with self.get_cursor() as cur:
            cur.execute("SELECT 1 FROM bookmarks WHERE user_id = %s AND article_id = %s", (user_id, article_id))
            esiste = cur.fetchone() is not None
        return esiste

    def estrai_metriche_dashboard(self, user_id: int) -> Dict[str, int]:
        with self.get_cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM articles")
            tot_articoli = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM sources")
            tot_fonti = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM bookmarks WHERE user_id = %s", (user_id,))
            tot_salvati = cur.fetchone()[0]
        return {"articoli": tot_articoli, "fonti": tot_fonti, "salvati": tot_salvati}

    # --- AGGIUNTA: GESTIONE STATO LETTO/NON LETTO ---
    def segna_letto(self, user_id: int, article_id: int) -> None:
        """Marca un articolo come letto. Idempotente: preserva la data di prima lettura."""
        query = """
            INSERT INTO user_article_status (user_id, article_id, letto, letto_il)
            VALUES (%s, %s, TRUE, CURRENT_TIMESTAMP)
            ON CONFLICT (user_id, article_id)
            DO UPDATE SET letto = TRUE,
                          letto_il = COALESCE(user_article_status.letto_il, CURRENT_TIMESTAMP)
        """
        with self.get_cursor() as cur:
            cur.execute(query, (user_id, article_id))

    def segna_tutti_letti(self, user_id: int, filtro_macro: Optional[str] = None) -> None:
        """Azzera l'arretrato: marca letti tutti gli articoli (o solo di una macro-categoria)."""
        query = """
            INSERT INTO user_article_status (user_id, article_id, letto, letto_il)
            SELECT %s, a.id, TRUE, CURRENT_TIMESTAMP FROM articles a
            WHERE (%s IS NULL OR a.macro = %s)
            ON CONFLICT (user_id, article_id)
            DO UPDATE SET letto = TRUE,
                          letto_il = COALESCE(user_article_status.letto_il, CURRENT_TIMESTAMP)
        """
        with self.get_cursor() as cur:
            cur.execute(query, (user_id, filtro_macro, filtro_macro))

    def conta_non_letti(self, user_id: int) -> Dict[str, int]:
        """Conta i non-letti per macro-categoria, rispettando le fonti spente dall'utente."""
        query = """
            SELECT a.macro, COUNT(*) AS n
            FROM articles a
            LEFT JOIN user_article_status uas
                ON uas.article_id = a.id AND uas.user_id = %s
            WHERE COALESCE(uas.letto, FALSE) = FALSE
            AND a.fonte NOT IN (
                SELECT s.nome FROM sources s
                JOIN user_source_preferences usp ON s.id = usp.source_id
                WHERE usp.user_id = %s AND usp.is_active = FALSE
            )
            GROUP BY a.macro
        """
        with self.get_cursor(dict_cursor=True) as cur:
            cur.execute(query, (user_id, user_id))
            righe = cur.fetchall()
        return {r['macro']: r['n'] for r in righe}

    # --- KEYWORD DEGLI ALERT: GESTIBILI DAGLI ADMIN, CONDIVISE DAL TEAM ---
    def lista_keywords_alert(self) -> List[Dict]:
        """Tutte le keyword di alert configurate, in ordine alfabetico."""
        with self.get_cursor(dict_cursor=True) as cur:
            cur.execute("SELECT id, keyword FROM alert_keywords ORDER BY keyword ASC")
            righe = cur.fetchall()
        return [dict(r) for r in righe]

    def aggiungi_keyword_alert(self, keyword: str) -> bool:
        """Aggiunge una keyword (minuscola, senza spazi esterni). False se già esistente o vuota."""
        k = (keyword or "").strip().lower()
        if not k or len(k) > 100:
            return False
        try:
            with self.get_cursor() as cur:
                cur.execute("INSERT INTO alert_keywords (keyword) VALUES (%s) ON CONFLICT (keyword) DO NOTHING", (k,))
                return cur.rowcount > 0
        except Exception as e:
            logging.error("Errore aggiunta keyword alert: %s", e)
            return False

    def rimuovi_keyword_alert(self, keyword_id: int) -> None:
        with self.get_cursor() as cur:
            cur.execute("DELETE FROM alert_keywords WHERE id = %s", (keyword_id,))

    # Gli alert usano le keyword della tabella ed escludono le fonti spente dall'utente
    def estrai_ultimi_alert_urgenti(self, user_id: int, limite: int = 4) -> List[Dict]:
        kw = [k['keyword'] for k in self.lista_keywords_alert()]
        if not kw:
            return []  # nessuna keyword configurata, nessun alert
        pattern = [f"%{k}%" for k in kw]

        query = """
            SELECT * FROM articles 
            WHERE ({}) 
            AND fonte NOT IN (
                SELECT s.nome FROM sources s
                JOIN user_source_preferences usp ON s.id = usp.source_id
                WHERE usp.user_id = %s AND usp.is_active = FALSE
            )
            ORDER BY COALESCE(data_pubblicazione, data_scansione) DESC LIMIT %s
        """.format(" OR ".join(["titolo ILIKE %s" for _ in pattern]))

        params = pattern + [user_id, limite]
        with self.get_cursor(dict_cursor=True) as cur:
            cur.execute(query, params)
            alert = cur.fetchall()
        return [dict(a) for a in alert]

# --- 2. CONFIGURAZIONE INIZIALE ---
DB_URL = st.secrets.get("DB_URL", "")
if not DB_URL:
    st.error("Rilevamento fallito: inserisci DB_URL nei Secrets di Streamlit.")
    st.stop()

DEFAULT_FONTI = [
    {"nome": "Agenzia Entrate", "url": "https://www.agenziaentrate.gov.it/portale/web/guest/rss/novita", "area": "Generale", "macro": "Leggi & Normativa", "tipo_fonte": "Ufficiale", "tipo_ingestion": "rss"},
    {"nome": "Garante Privacy", "url": "https://www.garanteprivacy.it/o/gpdp-rss/rss?c=10490", "area": "Generale", "macro": "Provvedimenti & Sentenze", "tipo_fonte": "Ufficiale", "tipo_ingestion": "rss"},
    {"nome": "EDPB Europa", "url": "https://edpb.europa.eu/rss.xml", "area": "Generale", "macro": "Provvedimenti & Sentenze", "tipo_fonte": "Ufficiale", "tipo_ingestion": "rss"},
    {"nome": "Banca d'Italia", "url": "https://www.bancaditalia.it/rss/media.xml", "area": "Generale", "macro": "Provvedimenti & Sentenze", "tipo_fonte": "Ufficiale", "tipo_ingestion": "rss"},
    {"nome": "Consob", "url": "https://www.consob.it/web/area-pubblica/rss", "area": "Generale", "macro": "Provvedimenti & Sentenze", "tipo_fonte": "Ufficiale", "tipo_ingestion": "rss"},
    {"nome": "IVASS", "url": "https://www.ivass.it/util/index.rss.html?lingua=it", "area": "Generale", "macro": "Leggi & Normativa", "tipo_fonte": "Ufficiale", "tipo_ingestion": "rss"},
    {"nome": "CGUE", "url": "https://curia.europa.eu/site/rss.jsp?lang=it&secondLang=en", "area": "Generale", "macro": "Provvedimenti & Sentenze", "tipo_fonte": "Ufficiale", "tipo_ingestion": "rss"},
    {"nome": "Altalex", "url": "https://www.altalex.com/rss", "area": "Generale", "macro": "News & Aggiornamenti", "tipo_fonte": "Editoriale", "tipo_ingestion": "rss"},
    {"nome": "Cybersecurity360", "url": "https://www.cybersecurity360.it/feed/", "area": "Generale", "macro": "News & Aggiornamenti", "tipo_fonte": "Editoriale", "tipo_ingestion": "rss"}
]

# --- ISTANZA DB CACHATA: init_db() e seeding fonti girano UNA SOLA VOLTA per deploy ---
@st.cache_resource
def get_db() -> LegalRadarDB:
    db = LegalRadarDB(DB_URL)
    db.init_db()
    if len(db.carica_fonti()) == 0:
        for f in DEFAULT_FONTI:
            db.aggiungi_fonte(f['nome'], f['url'], f['area'], f['macro'], f['tipo_fonte'], f['tipo_ingestion'])
    return db

db = get_db()

if 'user' not in st.session_state: st.session_state.user = None
if 'ai_summaries' not in st.session_state: st.session_state.ai_summaries = {}
if 'micro_riassunti' not in st.session_state: st.session_state.micro_riassunti = {}

# --- 3. STILE GRAFICO ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Newsreader:opsz,wght@6..72,400..600&display=swap');

    :root{
        --bg:#f5f5f7; --surface:#ffffff;
        --ink:#1d1d1f; --ink-soft:#6e6e73; --ink-faint:#a1a1a6;
        --hair:#ededf0;
        --accent:#0071e3; --accent-soft:#e8f1fd; --accent-tint:#dbeafe;
        --legge-bg:#fdf2e3; --legge-tx:#9a6712;
        --provv-bg:#eaf0f7; --provv-tx:#2c4a6e;
        --sent-bg:#f0ebf7; --sent-tx:#5b3f87;
        --news-bg:#e9f4ee; --news-tx:#1f7a52;
        --alta:#e8543f; --alta-soft:rgba(232,84,63,.12);
        --sf:-apple-system,BlinkMacSystemFont,'SF Pro Display','Helvetica Neue',sans-serif;
    }

    /* superficie generale */
    .stApp{ background:var(--bg); }
    html, body, [class*="css"]{ font-family:var(--sf); -webkit-font-smoothing:antialiased; color:var(--ink); }
    .block-container{ padding-top:2.4rem; padding-bottom:4rem; max-width:960px; }

    /* titoli di sistema */
    h1, h2, h3{ letter-spacing:-0.3px; color:var(--ink); font-weight:600; }
    h1{ font-size:30px; }
    h2{ font-size:23px; }

    /* sidebar pulita */
    section[data-testid="stSidebar"]{ background:var(--surface); border-right:1px solid var(--hair); }
    section[data-testid="stSidebar"] .stRadio label{ font-size:14px; }

    /* ---- CARD ATTI (lista sezioni) ---- */
    .radar-card{
        background:var(--surface); border-radius:18px; padding:22px 26px;
        box-shadow:0 1px 3px rgba(0,0,0,.04), 0 6px 22px rgba(0,0,0,.045);
        margin-bottom:14px; transition:transform .2s, box-shadow .2s;
    }
    .radar-card:hover{ transform:translateY(-2px); box-shadow:0 2px 6px rgba(0,0,0,.06), 0 12px 34px rgba(0,0,0,.08); }
    .radar-card.letta{ opacity:.55; }

    .card-title{
        font-family:'Newsreader',serif; font-weight:500; font-size:22px; line-height:1.25;
        letter-spacing:-0.2px; color:var(--ink); text-decoration:none; display:block; margin:13px 0 0;
    }
    .card-title:hover{ color:var(--accent); }
    .card-preview{ margin-top:11px; font-size:14.5px; line-height:1.55; color:var(--ink-soft); }
    .card-microsummary{
        margin-top:11px; font-size:14px; line-height:1.55; color:var(--ink-soft);
        background:var(--accent-soft); border-radius:12px; padding:11px 15px;
    }

    /* badge a pillola */
    .meta-tag{
        display:inline-block; font-size:11.5px; font-weight:600; letter-spacing:.2px;
        padding:4px 11px; border-radius:980px; margin-right:7px; vertical-align:middle;
    }
    .tag-area{ background:rgba(0,0,0,.05); color:var(--ink-soft); }
    .tag-fonte{ background:rgba(0,0,0,.05); color:var(--ink-soft); }
    .tag-rango{ background:rgba(0,0,0,.04); color:var(--ink-faint); }
    .badge-ril{ font-size:11.5px; font-weight:600; padding:4px 11px; border-radius:980px; }
    .badge-ril-alta{ background:var(--alta-soft); color:var(--alta); }
    .badge-ril-media{ background:rgba(0,0,0,.05); color:var(--ink-soft); }
    .badge-nuovo{
        font-size:11px; font-weight:600; color:var(--accent); background:var(--accent-soft);
        border-radius:980px; padding:3px 10px; margin-left:8px; vertical-align:2px;
    }

    /* categorie come badge pastello */
    .cat-badge{ display:inline-block; font-size:11.5px; font-weight:600; padding:4px 11px; border-radius:980px; }
    .cat-legge{ background:var(--legge-bg); color:var(--legge-tx); }
    .cat-provv{ background:var(--provv-bg); color:var(--provv-tx); }
    .cat-sent{ background:var(--sent-bg); color:var(--sent-tx); }
    .cat-news{ background:var(--news-bg); color:var(--news-tx); }

    /* bottoni Streamlit -> pillole Apple */
    .stButton > button{
        font-family:var(--sf); font-weight:500; font-size:14px;
        border-radius:980px; border:none; background:rgba(0,0,0,.05); color:var(--ink);
        padding:9px 20px; transition:.18s;
    }
    .stButton > button:hover{ background:rgba(0,0,0,.09); color:var(--ink); border:none; }
    .stButton > button[kind="primary"]{ background:var(--accent); color:#fff; }
    .stButton > button[kind="primary"]:hover{ background:#0077ed; color:#fff; }

    /* input e select arrotondati */
    .stTextInput input, .stSelectbox div[data-baseweb="select"] > div{
        border-radius:12px !important; border-color:var(--hair) !important;
    }

    /* ---- DASHBOARD PRIMA PAGINA ---- */
    .pp-hero-eyebrow{ font-size:13px; font-weight:600; color:var(--ink-soft); letter-spacing:.2px; }
    .pp-hero-title{
        font-family:'Newsreader',serif; font-weight:500; font-size:40px; line-height:1.13;
        letter-spacing:-0.5px; color:var(--ink); text-decoration:none; display:block; margin:10px 0 0;
    }
    .pp-hero-title:hover{ color:var(--accent); }
    .pp-hero-sub{ margin-top:16px; font-size:17px; line-height:1.55; color:var(--ink-soft); }
    .pp-hero-row{ margin-top:22px; }

    .pp-card{
        background:var(--surface); border-radius:18px; padding:22px 24px; height:100%;
        box-shadow:0 1px 3px rgba(0,0,0,.04), 0 6px 22px rgba(0,0,0,.045);
        transition:transform .2s, box-shadow .2s;
    }
    .pp-card:hover{ transform:translateY(-2px); box-shadow:0 2px 6px rgba(0,0,0,.06), 0 12px 34px rgba(0,0,0,.08); }
    .pp-card-title{
        font-family:'Newsreader',serif; font-weight:500; font-size:20px; line-height:1.28;
        letter-spacing:-0.2px; color:var(--ink); text-decoration:none; display:block; margin:13px 0 0;
    }
    .pp-card-title:hover{ color:var(--accent); }
    .pp-card-sum{ margin-top:10px; font-size:14px; line-height:1.5; color:var(--ink-soft); }
    .pp-foot{ margin-top:16px; font-size:12.5px; color:var(--ink-faint); font-weight:500; }

    .pp-section{ display:flex; align-items:baseline; justify-content:space-between; margin:42px 0 18px; }
    .pp-section h2{ font-size:23px; font-weight:600; letter-spacing:-0.3px; }

    .ticker-box{ background:var(--surface); border-radius:18px; padding:20px 22px;
        box-shadow:0 1px 3px rgba(0,0,0,.04), 0 6px 22px rgba(0,0,0,.045); }
    .ticker-box h3{ font-size:13px; font-weight:600; color:var(--ink-soft); text-transform:uppercase; letter-spacing:.5px; margin:0 0 12px; }
    .ti{ padding:10px 0; border-bottom:1px solid var(--hair); }
    .ti:last-child{ border:none; }
    .ti .tm{ font-size:11.5px; color:var(--ink-faint); font-weight:500; margin-bottom:3px; }
    .ti a{ font-size:14px; font-weight:500; color:var(--ink); text-decoration:none; line-height:1.4; }
    .ti a:hover{ color:var(--accent); }

    .mini{ background:var(--surface); border-radius:14px; padding:15px 17px; height:100%;
        box-shadow:0 1px 3px rgba(0,0,0,.04), 0 5px 18px rgba(0,0,0,.04); }
    .mini .mm{ font-size:11.5px; color:var(--ink-faint); font-weight:500; margin-bottom:6px; }
    .mini a{ font-family:'Newsreader',serif; font-size:15.5px; font-weight:500; color:var(--ink); text-decoration:none; line-height:1.35; }
    .mini a:hover{ color:var(--accent); }

    div[data-testid="stHorizontalBlock"]{ gap:18px; }
    hr{ border-color:var(--hair); }
</style>
""", unsafe_allow_html=True)

# --- 4. MOTORE LOGICO E SCRAPING ---
def estrai_testo_pulito(url: str) -> str:
    if url.lower().endswith(('.pdf', '.zip', '.doc')): return ""
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        res = requests.get(url, headers=headers, timeout=6)
        soup = BeautifulSoup(res.text, 'html.parser')
        paragraphs = soup.find_all(['p', 'div'])
        return " ".join([p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 45])[:6000]
    except: return ""

# --- MODIFICA: POTENZIAMENTO PROMPT AI VERTICALE (Punto 2) ---
def genera_sintesi_groq(url: str, preview_text: str) -> str:
    raw_key = st.secrets.get("GROQ_API_KEY", "").strip()
    if not raw_key.startswith("gsk_"): return "⚠️ Configura la chiave GROQ_API_KEY nei Secrets."
    
    testo_sito = estrai_testo_pulito(url)
    input_ai = testo_sito if len(testo_sito) > 200 else preview_text
    
    api_url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {raw_key}", "Content-Type": "application/json"}
    
    system_prompt = (
        "Sei un Senior Legal Counsel esperto di compliance e mercati digitali, specializzato nel settore dei "
        "comparatori online e aggregatori di tariffe in Italia (es. Facile.it, Segugio.it). "
        "Analizza il testo fornito ed elabora un report strutturato rigorosamente in lingua italiana diviso in tre sezioni precise:\n\n"
        "1) 📝 EXECUTIVE SUMMARY: Una sintesi chiarissima del nucleo normativo o giuridico dell'atto (max 2 frasi).\n"
        "2) ⚖️ ANALISI LEGALE: I profili di rischio, gli obblighi o le opportunità giuridiche emergenti dall'atto.\n"
        "3) 🚀 IMPATTO COMPARATORI ONLINE: Una valutazione verticale di come questa novità impatti specificamente sull'operatività, "
        "sul business, sul marketing o sulla compliance dei siti di comparazione tariffe/assicurazioni/finanza in Italia.\n\n"
        "Sii autorevole, schematico e pragmatico."
    )
    
    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Testo da analizzare:\n\n{input_ai}"}
        ],
        "temperature": 0.2
    }
    try:
        r = requests.post(api_url, headers=headers, json=payload, timeout=15)
        if r.status_code == 200: return r.json()['choices'][0]['message']['content'].strip()
        return f"⚠️ Errore AI ({r.status_code})"
    except: return "⚠️ Connessione AI fallita."

def genera_microriassunto_groq(titolo: str, preview: str, e_ufficiale: bool = True,
                               serve_titolo: bool = False) -> Dict[str, Optional[str]]:
    """Genera riassunto + rilevanza + (per fonti ufficiali) categoria legge/provvedimento + tema.
    Se serve_titolo=True (titolo del feed rotto), chiede all'AI anche un titolo dal testo.
    Ritorna {'riassunto','rilevanza','categoria','tema','titolo'}. Fail-safe: None su errore.
    Nota: per le fonti editoriali la categoria è imposta a 'news' a monte (l'AI non la decide)."""
    vuoto = {"riassunto": None, "rilevanza": None, "categoria": None, "tema": None, "titolo": None}
    raw_key = st.secrets.get("GROQ_API_KEY", "").strip()
    if not raw_key.startswith("gsk_"):
        return vuoto

    # Se il titolo è rotto non lo passo all'AI (la confonderebbe): solo il testo
    testo = f"Anteprima: {preview}" if serve_titolo else f"Titolo: {titolo}\n\nAnteprima: {preview}"
    api_url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {raw_key}", "Content-Type": "application/json"}

    if e_ufficiale:
        regola_categoria = (
            '"categoria": "<legge|provvedimento|sentenza>", '
        )
        spiega_categoria = (
            "- categoria: classifica in base all'organo che ha emanato l'atto:\n"
            "  * \"legge\" = testi normativi (legge, decreto legge, decreto legislativo, decreto ministeriale, "
            "regolamento UE, direttiva, testo unico, codice), tipicamente da Gazzetta Ufficiale, Normattiva, Parlamento, Governo;\n"
            "  * \"provvedimento\" = atti di AUTORITÀ AMMINISTRATIVE indipendenti (Garante Privacy, AGCOM, AGCM, "
            "IVASS, Consob, Banca d'Italia): sanzioni, ordinanze, delibere, linee guida, pareri, comunicazioni;\n"
            "  * \"sentenza\" = pronunce di ORGANI GIURISDIZIONALI (tribunali, Corte d'Assise, Corte Costituzionale, "
            "TAR, Consiglio di Stato, Corte di Cassazione, CGUE, Corte EDU): sentenze, ordinanze giurisdizionali, decreti del giudice.\n"
            "  Nel dubbio tra legge e provvedimento scegli \"provvedimento\"; se è chiaramente una pronuncia di un giudice, \"sentenza\".\n"
        )
    else:
        regola_categoria = ""
        spiega_categoria = ""

    if serve_titolo:
        regola_titolo = '"titolo": "<titolo conciso e informativo in italiano, max 12 parole>", '
        spiega_titolo = ("- titolo: il feed non fornisce un titolo valido; scrivilo tu, conciso e informativo, "
                         "come lo scriverebbe una testata giuridica (niente virgolette interne).\n")
    else:
        regola_titolo = ""
        spiega_titolo = ""

    system_prompt = (
        "Sei un assistente legale che pre-analizza novità normative, giurisprudenziali e di settore per un team "
        "di compliance specializzato nei comparatori online italiani (finanza, assicurazioni, utility). "
        "Dato il contenuto, rispondi ESCLUSIVAMENTE con un oggetto JSON valido, senza testo prima o dopo:\n"
        '{' + regola_titolo + '"riassunto": "<1-2 frasi in italiano, chiare e concrete>", '
        '"rilevanza": "<alta|media>", '
        + regola_categoria +
        '"tema": "<tema giuridico principale>"}\n\n'
        "Regole:\n"
        + spiega_titolo +
        "- rilevanza: \"alta\" se impatta direttamente i comparatori (sanzioni, telemarketing, consenso, "
        "trasparenza tariffaria, data breach, intermediazione); \"media\" altrimenti.\n"
        + spiega_categoria +
        "- tema: il tema giuridico principale. Usa preferibilmente uno tra: Privacy, Cybersecurity, "
        "Assicurativo, Bancario e finanziario, Tributario, Consumatori e pratiche commerciali, Concorrenza, "
        "Intelligenza artificiale. Se nessuno calza, indica tu il tema più appropriato in 1-3 parole.\n"
        "Non aggiungere campi."
    )
    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": testo}
        ],
        "temperature": 0.1,
        "response_format": {"type": "json_object"}
    }
    try:
        r = requests.post(api_url, headers=headers, json=payload, timeout=15)
        if r.status_code != 200:
            logging.error("Microriassunto: errore AI %s", r.status_code)
            return vuoto
        dati = json.loads(r.json()['choices'][0]['message']['content'].strip())
        riassunto = (dati.get("riassunto") or "").strip()[:600] or None
        rilevanza = (dati.get("rilevanza") or "").strip().lower()
        if rilevanza not in ("alta", "media"):
            rilevanza = None
        categoria = (dati.get("categoria") or "").strip().lower()
        if categoria not in ("legge", "provvedimento", "sentenza"):
            categoria = None
        tema = (dati.get("tema") or "").strip()[:100] or None
        titolo_ai = (dati.get("titolo") or "").strip()[:200] or None
        return {"riassunto": riassunto, "rilevanza": rilevanza, "categoria": categoria, "tema": tema, "titolo": titolo_ai}
    except Exception as e:
        logging.error("Microriassunto fallito: %s", e)
        return vuoto

def _classifica_con_fallback(meta: Dict, fonte: Dict) -> Dict:
    """Determina la categoria finale (legge/provvedimento/news) combinando regola-fonte e AI.
    - Fonte editoriale -> sempre 'news'.
    - Fonte ufficiale/autorità -> 'legge' o 'provvedimento' dall'AI, con fallback 'provvedimento'.
    Garantisce categoria e tema mai vuoti."""
    tf = (fonte.get('tipo_fonte') or 'Ufficiale').lower()
    if tf == "editoriale":
        categoria = "news"
    else:
        categoria = meta.get("categoria") or "provvedimento"  # fallback sicuro per atti ufficiali
    tema = meta.get("tema") or fonte.get('area') or "Generale"
    return {
        "riassunto": meta.get("riassunto"),
        "rilevanza": meta.get("rilevanza") or "media",
        "categoria": categoria,
        "tema": tema,
        "titolo": meta.get("titolo"),
    }

def pulisci_titolo(titolo: str) -> str:
    """Normalizza i titoli sporchi dei feed istituzionali.
    Es. CGUE: '78/Thu Jun 04 00:00:00 CEST 2026 : null - Sentenza...' -> 'Sentenza...'"""
    if not titolo:
        return titolo
    t = titolo.strip()
    # Pattern CGUE: 'NN/<data java> : null - <vero titolo>'
    m = re.match(r"^\d+/\w{3}\s\w{3}\s\d{1,2}.*?:\s*null\s*-\s*(.+)$", t)
    if m:
        t = m.group(1).strip()
    # Rimuove 'null' isolati residui e spazi multipli
    t = re.sub(r"\bnull\b\s*-?\s*", "", t).strip()
    t = re.sub(r"\s{2,}", " ", t)
    return t or titolo  # se la pulizia svuota tutto, tieni l'originale

def titolo_invalido(titolo: str) -> bool:
    """Rileva i titoli-segnaposto rotti alla fonte (es. AGCM: '$con.titolo1').
    Un titolo è invalido se vuoto, se è solo 'null', o se è/contiene solo variabili
    di template non risolte tipo $var, ${var}, {{var}}, %var%."""
    if not titolo or not titolo.strip():
        return True
    t = titolo.strip()
    if t.lower() in ("null", "none", "undefined"):
        return True
    # Solo variabili di template, eventualmente più d'una separate da spazi/punteggiatura
    if re.fullmatch(r"[\s\W]*(?:\$\{?[\w.]+\}?|\{\{[\w.\s]+\}\}|%[\w.]+%)[\s\W]*", t):
        return True
    return False

def titolo_da_testo(testo: str, max_len: int = 110) -> str:
    """Deriva un titolo leggibile dalla prima frase del testo (fallback deterministico)."""
    if not testo:
        return "Aggiornamento dalla fonte"
    t = re.sub(r"\s{2,}", " ", testo.strip())
    # Prima frase: taglio al primo punto 'forte' se cade in un punto ragionevole
    m = re.match(r"^(.{30,}?[.!?])\s", t + " ")
    frase = m.group(1) if m else t
    if len(frase) > max_len:
        frase = frase[:max_len].rsplit(" ", 1)[0] + "…"
    return frase.strip()

def estrai_data_pubblicazione(entry) -> Optional[datetime]:
    """Estrae la data di pubblicazione reale dal feed RSS, se presente."""
    for campo in ("published_parsed", "updated_parsed"):
        st_time = getattr(entry, campo, None)
        if st_time:
            try:
                return datetime(*st_time[:6])
            except Exception:
                continue
    return None

def _ingest_rss(f: Dict) -> List[Dict]:
    """Strategia di ingestion per fonti con feed RSS."""
    risultati = []
    e_ufficiale = (f.get('tipo_fonte') or 'Ufficiale').lower() != "editoriale"
    feed = feedparser.parse(f['url'])
    for entry in feed.entries[:5]:
        sommario = entry.summary if hasattr(entry, 'summary') else ""
        testo_completo = BeautifulSoup(sommario, "html.parser").get_text().strip()
        preview = testo_completo[:250] + ("..." if len(testo_completo) > 250 else "")
        titolo = pulisci_titolo(getattr(entry, 'title', '') or '')
        # Titolo rotto alla fonte (es. AGCM '$con.titolo1')? Lo faremo generare all'AI
        serve_titolo = titolo_invalido(titolo)
        data_pub = estrai_data_pubblicazione(entry)
        # All'AI passo il testo esteso (fino a 1200 caratteri), non la preview troncata:
        # più contesto = classificazione e riassunto più precisi, stessa singola chiamata.
        meta = _classifica_con_fallback(
            genera_microriassunto_groq(titolo, testo_completo[:1200],
                                       e_ufficiale=e_ufficiale, serve_titolo=serve_titolo), f
        )
        if serve_titolo:
            # Titolo dall'AI; se l'AI non risponde, prima frase del testo (mai vuoto)
            titolo = meta.get("titolo") or titolo_da_testo(testo_completo)
        risultati.append({
            "Titolo": titolo, "Link": entry.link, "Preview": preview,
            "Macro": f['macro'], "Area": f['area'], "Fonte": f['nome'],
            "RiassuntoAI": meta["riassunto"], "Rilevanza": meta["rilevanza"],
            "TipoAtto": meta["categoria"], "Tema": meta["tema"],
            "DataPubblicazione": data_pub
        })
    return risultati

def _ingest_scraper(f: Dict) -> List[Dict]:
    """Strategia di ingestion per fonti senza RSS (parser HTML dedicato per fonte).

    Punto di aggancio per fonti istituzionali che non espongono RSS.
    Ogni fonte 'scraper' richiede un parser specifico: la struttura HTML
    cambia da sito a sito e va gestita caso per caso. Finché non è
    implementato un parser per la fonte, non produce articoli (fail-safe).
    """
    logging.info("Fonte '%s' di tipo scraper: parser dedicato non ancora implementato.", f['nome'])
    return []

# Dispatcher: associa il tipo_ingestion alla strategia corretta
STRATEGIE_INGESTION = {
    "rss": _ingest_rss,
    "scraper": _ingest_scraper,
}

def sincronizza_radar_in_database() -> None:
    fonti = db.carica_fonti()
    articoli_scovati = []
    for f in fonti:
        tipo = f.get('tipo_ingestion', 'rss')
        strategia = STRATEGIE_INGESTION.get(tipo, _ingest_rss)
        try:
            trovati = strategia(f)
            articoli_scovati.extend(trovati)
            # Registra la salute: ok se ha prodotto articoli, vuoto altrimenti
            if trovati:
                db.registra_esito_sync(f['id'], "ok", f"{len(trovati)} elementi rilevati")
            else:
                db.registra_esito_sync(f['id'], "vuoto", "Nessun elemento dal feed")
        except Exception as e:
            logging.error("Ingestion fallita per %s (%s): %s", f['nome'], tipo, e)
            db.registra_esito_sync(f['id'], "errore", str(e))
            continue
    if articoli_scovati:
        db.salva_articoli_storico(articoli_scovati)

# --- 5. SCHERMATA DI AUTENTICAZIONE ---
if st.session_state.user is None:
    st.title("⚖️ Legal Radar | Autenticazione")
    st.caption("Accedi all'Intelligence Normativa Personalizzata")
    
    scelta = st.radio("Seleziona Azione", ["Accedi", "Registrati"], horizontal=True)
    with st.form("auth_form"):
        username = st.text_input("Username / Email")
        password = st.text_input("Password", type="password")
        submit = st.form_submit_button("Conferma")
        
        if submit:
            if not username or not password:
                st.error("Compila tutti i campi.")
            elif scelta == "Registrati":
                if db.registra_utente(username, password):
                    st.success("Registrazione completata! Ora puoi effettuare il login.")
                else:
                    st.error("Username già esistente o errore.")
            elif scelta == "Accedi":
                user = db.verifica_utente(username, password)
                if user:
                    # Bootstrap: se non esiste alcun admin, il primo utente viene promosso
                    db.bootstrap_primo_admin()
                    # Ricarico l'utente per avere il ruolo aggiornato in sessione
                    user = db.verifica_utente(username, password)
                    st.session_state.user = user
                    st.success(f"Accesso eseguito. Benvenuto {user['username']}!")
                    st.rerun()
                else:
                    st.error("Credenziali errate.")
    st.stop()

# --- 6. INTERFACCIA UTENTE AUTENTICATO ---
with st.sidebar:
    st.title("⚖️ Legal Radar")
    ruolo_corrente = st.session_state.user.get('role', 'user')
    badge_ruolo = "👑 Admin" if ruolo_corrente == "admin" else "👤 Utente"
    st.write(f"{badge_ruolo}: **{st.session_state.user['username']}**")

    # Conteggi non-letti mostrati come riepilogo (le label del radio restano fisse)
    non_letti = db.conta_non_letti(st.session_state.user['id'])
    tot_non_letti = sum(non_letti.values())
    if tot_non_letti:
        st.caption(f"📬 {tot_non_letti} da leggere")

    opzioni_nav = [
        "🏠 Dashboard",
        "📖 Leggi",
        "🏛️ Provvedimenti",
        "⚖️ Sentenze",
        "📰 News",
        "🔖 I Miei Salvati",
        "⚙️ Gestione Fonti"
    ]
    pagina = st.radio("Navigazione", opzioni_nav, key="nav_pagina")
    
    st.divider()
    if st.button("🔄 Sincronizza ed Espandi Archivio", type="primary", use_container_width=True):
        with st.spinner("Scansione fonti legali e scrittura in archivio..."):
            sincronizza_radar_in_database()
            st.success("Archivio aggiornato!")
            st.rerun()
            
    if st.button("🚪 Esci", use_container_width=True):
        st.session_state.user = None
        st.rerun()

def mostra_hub_legale(lista_articoli: List[Dict], tipo_bacheca: str):
    if not lista_articoli:
        st.info("Nessun articolo trovato in questo archivio storico filtrato.")
        return
        
    for art in lista_articoli:
        with st.container():
            e_letto = art.get('letto', False)
            classe_card = "radar-card letta" if e_letto else "radar-card"
            badge_nuovo = "" if e_letto else '<span class="badge-nuovo">Nuovo</span>'
            # Escape di tutti i testi: evita che contenuti con caratteri HTML (es. < > nel riassunto AI) rompano la card
            e_area = html.escape(str(art.get('area') or ''))
            e_fonte = html.escape(str(art.get('fonte') or ''))
            e_titolo = html.escape(str(art.get('titolo') or ''))
            e_preview = html.escape(str(art.get('preview') or ''))
            e_link = html.escape(str(art.get('link') or ''), quote=True)
            # Badge categoria pastello (Legge/Provvedimento/Sentenza/News)
            sc = _stile_categoria(art.get('tipo_atto'))
            badge_cat = f'<span class="cat-badge cat-{sc["cls_badge"]}">{sc["label"]}</span>'
            # Data dell'atto: la pubblicazione reale se disponibile, altrimenti la scansione
            data_rif = art.get('data_pubblicazione') or art.get('data_scansione')
            data_str = data_rif.strftime('%d/%m/%Y') if data_rif else ''
            tag_data = f'<span class="meta-tag tag-rango">{data_str}</span>' if data_str else ''
            # Tag tema (dall'AI): se assente, ripiega sull'area manuale
            tema = art.get('tema') or art.get('area')
            tag_tema = f'<span class="meta-tag tag-area">{html.escape(str(tema))}</span>' if tema else ''
            # Badge rilevanza (solo priorità visiva, non nasconde nulla)
            rilevanza = art.get('rilevanza')
            if rilevanza == "alta":
                badge_ril = '<span class="badge-ril badge-ril-alta">Alta</span>'
            else:
                badge_ril = ''
            # Micro-riassunto AI sotto il titolo: dal DB, o da quello appena generato in sessione
            riassunto = art.get('riassunto_ai') or st.session_state.get('micro_riassunti', {}).get(art['id'])
            blocco_riassunto = f'<div class="card-microsummary">✦ {html.escape(str(riassunto))}</div>' if riassunto else ''
            card_html = (
                f'<div class="{classe_card}">'
                f'<div>{badge_cat}{badge_ril}<span class="meta-tag tag-fonte">{e_fonte}</span>'
                f'{tag_tema}{tag_data}</div>'
                f'<a href="{e_link}" target="_blank" class="card-title">{e_titolo}{badge_nuovo}</a>'
                f'{blocco_riassunto}'
                f'<div class="card-preview">{e_preview}</div>'
                f'</div>'
            )
            st.markdown(card_html, unsafe_allow_html=True)
            
            c1, c2, c3 = st.columns([1, 1, 2])
            with c1:
                if tipo_bacheca == "bookmarks":
                    if st.button("🗑️ Rimuovi dai preferiti", key=f"rem_{art['id']}"):
                        db.rimuovi_bookmark(st.session_state.user['id'], art['id'])
                        st.rerun()
                else:
                    if db.check_bookmark_esiste(st.session_state.user['id'], art['id']):
                        st.caption("🔖 Già salvato nei tuoi preferiti")
                    else:
                        if st.button("🔖 Salva per dopo", key=f"save_{art['id']}"):
                            db.aggiungi_bookmark(st.session_state.user['id'], art['id'])
                            st.success("Articolo salvato!")
                            st.rerun()
            with c2:
                # Bottone esplicito per marcare letto (copre il caso del link esterno)
                if not e_letto:
                    if st.button("✓ Segna letto", key=f"read_{art['id']}"):
                        db.segna_letto(st.session_state.user['id'], art['id'])
                        st.rerun()
                else:
                    st.caption("✓ Letto")
            
            link = art['link']
            # Se l'analisi non c'è ancora, mostro il bottone per generarla
            if link not in st.session_state.ai_summaries:
                with c3:
                    if st.button("✨ Genera Analisi AI Strategica", key=f"ai_{art['id']}"):
                        with st.spinner("L'AI sta conducendo l'analisi verticale per i comparatori..."):
                            st.session_state.ai_summaries[link] = genera_sintesi_groq(link, art['preview'])
                            # Se l'articolo non ha ancora il micro-riassunto, lo genero e lo salvo ora
                            if not art.get('riassunto_ai') and art['id'] not in st.session_state.micro_riassunti:
                                meta = genera_microriassunto_groq(art.get('titolo',''), art.get('preview',''))
                                if meta['riassunto']:
                                    st.session_state.micro_riassunti[art['id']] = meta['riassunto']
                                    db.aggiorna_riassunto_articolo(art['id'], meta['riassunto'], meta['rilevanza'])
                            # Generare l'analisi implica aver "consumato" l'articolo: marca letto
                            db.segna_letto(st.session_state.user['id'], art['id'])
                        # niente st.rerun(): mostro il risultato qui sotto, nello stesso ciclo
            # Se l'analisi esiste (appena generata o già presente), la mostro sotto la card
            if link in st.session_state.ai_summaries:
                st.markdown("<div style='font-size:13px; font-weight:600; color:var(--accent); margin-top:6px;'>✦ Analisi strategica</div>", unsafe_allow_html=True)
                st.markdown(st.session_state.ai_summaries[link])
            st.write("")

def _stile_categoria(tipo_atto: Optional[str]) -> Dict[str, str]:
    """Ritorna classi CSS ed etichetta per categoria (badge pastello + thumb dashboard)."""
    t = (tipo_atto or "provvedimento").lower()
    if t == "legge":
        return {"cls": "legge", "cls_badge": "legge", "glyph": "§", "label": "Legge"}
    if t == "sentenza":
        return {"cls": "sent", "cls_badge": "sent", "glyph": "⚖", "label": "Sentenza"}
    if t == "news":
        return {"cls": "news", "cls_badge": "news", "glyph": "▤", "label": "News"}
    return {"cls": "provv", "cls_badge": "provv", "glyph": "▣", "label": "Provvedimento"}

def _pp_card(art: Dict, lead: bool = False) -> str:
    """Costruisce l'HTML di una card della prima pagina (griglia 'In evidenza')."""
    s = _stile_categoria(art.get('tipo_atto'))
    tema = html.escape(str(art.get('tema') or art.get('area') or 'Generale'))
    fonte = html.escape(str(art.get('fonte') or ''))
    titolo = html.escape(str(art.get('titolo') or ''))
    link = html.escape(str(art.get('link') or ''), quote=True)
    riassunto = html.escape(str(art.get('riassunto_ai') or art.get('preview') or ''))
    badge_cat = f'<span class="cat-badge cat-{s["cls_badge"]}">{s["label"]}</span>'
    badge_ril = '<span class="badge-ril badge-ril-alta" style="margin-left:7px;">Alta</span>' if art.get('rilevanza') == 'alta' else ''
    data_rif = art.get('data_pubblicazione') or art.get('data_scansione')
    data_str = data_rif.strftime('%d/%m') if data_rif else ''
    return (
        f'<div class="pp-card">'
        f'<div>{badge_cat}{badge_ril}</div>'
        f'<a href="{link}" target="_blank" class="pp-card-title">{titolo}</a>'
        f'<div class="pp-card-sum">{riassunto}</div>'
        f'<div class="pp-foot">{fonte} · {tema}{(" · " + data_str) if data_str else ""}</div>'
        f'</div>'
    )

def _semaforo_fonte(s: Dict) -> Tuple[str, str]:
    """Calcola il semaforo di salute di una fonte.
    Ritorna (emoji, descrizione)."""
    esito = s.get('ultimo_esito')
    ultima = s.get('ultima_sync')
    if esito == "errore":
        return "🔴", "In errore all'ultima sincronizzazione"
    if not ultima:
        return "⚪", "Mai sincronizzata"
    giorni = (datetime.now() - ultima).days
    if giorni <= 10:
        return "🟢", "Attiva"
    if giorni <= 30:
        return "🟡", "Silenziosa da oltre 10 giorni"
    return "🟡", "Silenziosa da oltre 30 giorni"

# Le label di navigazione possono avere un suffisso conteggio "(3)": lo rimuovo per il routing
pagina_pulita = re.sub(r"\s*\(\d+\)$", "", pagina)

ricerca = ""
if pagina_pulita not in ["⚙️ Gestione Fonti", "🏠 Dashboard"]:
    ricerca = st.text_input("🔍 Cerca parole chiave nell'archivio storico...")

# --- ROUTING PAGINE ---
if pagina_pulita == "🏠 Dashboard":
    oggi = datetime.now().strftime('%d/%m/%Y')
    in_evidenza = db.estrai_in_evidenza(st.session_state.user['id'], limite=4)

    # Stato vuoto curato (utile soprattutto dopo un reset a t0)
    if not in_evidenza:
        st.markdown('<div class="pp-hero-eyebrow">La tua prima pagina</div>', unsafe_allow_html=True)
        st.info("Nessun articolo ancora in archivio. Lancia una sincronizzazione dalla barra laterale per popolare la prima pagina.")
    else:
        # --- HERO: l'atto di apertura (più rilevante non letto) ---
        top = in_evidenza[0]
        sc_top = _stile_categoria(top.get('tipo_atto'))
        e_titolo = html.escape(str(top.get('titolo') or ''))
        e_link = html.escape(str(top.get('link') or ''), quote=True)
        e_sum = html.escape(str(top.get('riassunto_ai') or top.get('preview') or ''))
        e_tema = html.escape(str(top.get('tema') or top.get('area') or 'Generale'))
        e_fonte = html.escape(str(top.get('fonte') or ''))
        n_nuovi = len(in_evidenza)
        badge_alta = '<span class="badge-ril badge-ril-alta">Rilevanza alta</span>' if top.get('rilevanza') == 'alta' else ''
        hero_html = (
            f'<div class="pp-hero-eyebrow">Aggiornato al {oggi}</div>'
            f'<a href="{e_link}" target="_blank" class="pp-hero-title">{e_titolo}</a>'
            f'<div class="pp-hero-sub">{e_sum}</div>'
            f'<div class="pp-hero-row">'
            f'<span class="cat-badge cat-{sc_top["cls_badge"]}">{sc_top["label"]}</span> '
            f'{badge_alta} '
            f'<span class="meta-tag tag-area">{e_tema} · {e_fonte}</span>'
            f'</div>'
        )
        st.markdown(hero_html, unsafe_allow_html=True)
        st.markdown("<hr>", unsafe_allow_html=True)

        # --- ULTIM'ORA ---
        ultima_ora = db.estrai_ultima_ora(st.session_state.user['id'], limite=5)
        if ultima_ora:
            voci = ""
            for al in ultima_ora:
                tm = html.escape(str(al.get('fonte') or ''))
                tt = html.escape(str(al.get('titolo') or ''))
                lk = html.escape(str(al.get('link') or ''), quote=True)
                voci += f'<div class="ti"><div class="tm">{tm}</div><a href="{lk}" target="_blank">{tt}</a></div>'
            st.markdown(f'<div class="ticker-box"><h3>Ultim\'ora</h3>{voci}</div>', unsafe_allow_html=True)

        # --- STRISCIA ALERT (keyword configurate dal team) ---
        alert_urgenti = db.estrai_ultimi_alert_urgenti(st.session_state.user['id'], limite=3)
        if alert_urgenti:
            voci_alert = ""
            for al in alert_urgenti:
                a_tt = html.escape(str(al.get('titolo') or ''))
                a_lk = html.escape(str(al.get('link') or ''), quote=True)
                a_fn = html.escape(str(al.get('fonte') or ''))
                voci_alert += (
                    f'<div style="padding:9px 0; border-bottom:1px solid rgba(232,84,63,.14);">'
                    f'<span style="font-size:11px; font-weight:600; color:var(--alta); letter-spacing:.2px;">{a_fn}</span> '
                    f'<a href="{a_lk}" target="_blank" style="font-size:14px; font-weight:500; color:var(--ink); text-decoration:none;">{a_tt}</a>'
                    f'</div>'
                )
            st.markdown(
                f'<div style="background:var(--alta-soft); border-radius:14px; padding:14px 18px; margin-top:22px;">'
                f'<div style="font-size:13px; font-weight:600; color:var(--alta); margin-bottom:6px; letter-spacing:.2px;">Alert del team</div>'
                f'{voci_alert}</div>',
                unsafe_allow_html=True
            )

        # --- GRIGLIA IN EVIDENZA (i successivi 3) ---
        secondari = in_evidenza[1:4]
        if secondari:
            st.markdown('<div class="pp-section"><h2>In evidenza</h2></div>', unsafe_allow_html=True)
            cols = st.columns(len(secondari))
            for col, art in zip(cols, secondari):
                with col:
                    st.markdown(_pp_card(art, lead=False), unsafe_allow_html=True)

        # --- BLOCCHI TEMATICI (i temi più presenti) ---
        temi_top = db.temi_piu_presenti(st.session_state.user['id'], limite=3)
        for tema in temi_top:
            articoli_tema = db.estrai_per_tema_blocco(st.session_state.user['id'], tema, limite=3)
            if not articoli_tema:
                continue
            st.markdown(f'<div class="pp-section"><h2>{html.escape(str(tema))}</h2></div>', unsafe_allow_html=True)
            cols = st.columns(len(articoli_tema))
            for col, art in zip(cols, articoli_tema):
                with col:
                    s = _stile_categoria(art.get('tipo_atto'))
                    mm = html.escape(f"{art.get('fonte','')} · {s['label']}")
                    tt = html.escape(str(art.get('titolo') or ''))
                    lk = html.escape(str(art.get('link') or ''), quote=True)
                    st.markdown(f'<div class="mini"><div class="mm">{mm}</div><a href="{lk}" target="_blank">{tt}</a></div>', unsafe_allow_html=True)

elif pagina_pulita in ["📖 Leggi", "🏛️ Provvedimenti", "⚖️ Sentenze", "📰 News"]:
    # Mappa la voce di menu alla categoria
    mappa_tipo = {"📖 Leggi": "legge", "🏛️ Provvedimenti": "provvedimento", "⚖️ Sentenze": "sentenza", "📰 News": "news"}
    tipo_atto = mappa_tipo[pagina_pulita]
    etichetta = pagina_pulita.split(" ", 1)[1]

    col_h, col_btn = st.columns([3, 1])
    col_h.header(etichetta)
    with col_btn:
        st.write("")
        if st.button("✓ Segna tutto come letto", key=f"readall_{tipo_atto}", use_container_width=True):
            db.segna_tutti_letti(st.session_state.user['id'])
            st.rerun()

    # Filtro per tema (popolato dai temi realmente presenti in questa categoria)
    temi_disponibili = db.lista_temi(tipo_atto)
    tema_sel = None
    if temi_disponibili:
        scelta_tema = st.selectbox("Filtra per tema", ["Tutti i temi"] + temi_disponibili, key=f"tema_{tipo_atto}")
        if scelta_tema != "Tutti i temi":
            tema_sel = scelta_tema

    # --- PAGINAZIONE "CARICA ALTRI" ---
    PAGINA_DIM = 25
    if 'paginazione' not in st.session_state:
        st.session_state.paginazione = {}
    # La chiave di contesto include sezione + filtri: se cambiano, il contatore riparte
    ctx_key = f"{tipo_atto}|{tema_sel or ''}|{ricerca or ''}"
    stato_pag = st.session_state.paginazione
    if stato_pag.get('ctx') != ctx_key:
        stato_pag['ctx'] = ctx_key
        stato_pag['n'] = PAGINA_DIM
    limite_corrente = stato_pag['n']

    # Chiedo un articolo in più del necessario: se arriva, esiste un'altra pagina
    dati_db = db.estrai_per_tipo_atto(
        tipo_atto, st.session_state.user['id'],
        ricerca_testo=ricerca, tema=tema_sel, limite=limite_corrente + 1
    )
    ci_sono_altri = len(dati_db) > limite_corrente
    mostra_hub_legale(dati_db[:limite_corrente], tipo_bacheca="radar")

    if ci_sono_altri:
        if st.button(f"⬇️ Carica altri {PAGINA_DIM}", key=f"more_{tipo_atto}", use_container_width=True):
            stato_pag['n'] += PAGINA_DIM
            st.rerun()
    elif limite_corrente > PAGINA_DIM:
        st.caption("Hai raggiunto la fine dell'archivio per questi filtri.")

elif pagina_pulita == "🔖 I Miei Salvati":
    st.header("I Miei Articoli Salvati")
    dati_salvati = db.estrai_bookmarks(user_id=st.session_state.user['id'], ricerca_testo=ricerca)
    mostra_hub_legale(dati_salvati, tipo_bacheca="bookmarks")

elif pagina_pulita == "⚙️ Gestione Fonti":
    st.header("Database & Personalizzazione Fonti")

    # SEZIONE 0 - STATO DI SALUTE DEI CANALI
    st.subheader("🩺 Stato di Salute dei Canali")
    st.caption("Semaforo basato sull'ultima sincronizzazione. 🟢 attiva · 🟡 silenziosa · 🔴 in errore · ⚪ mai sincronizzata.")
    salute = db.carica_salute_fonti()
    n_rosse = sum(1 for s in salute if _semaforo_fonte(s)[0] == "🔴")
    if n_rosse:
        st.warning(f"Attenzione: {n_rosse} fonte/i in errore. Verifica l'URL del feed.")
    for s in salute:
        emoji, descrizione = _semaforo_fonte(s)
        sync_txt = s['ultima_sync'].strftime('%d/%m/%Y %H:%M') if s.get('ultima_sync') else "mai"
        col_s1, col_s2, col_s3 = st.columns([3, 2, 2])
        col_s1.markdown(f"{emoji} **{s['nome']}**")
        col_s2.caption(f"Ultima sync: {sync_txt}")
        col_s3.caption(f"Articoli 30gg: {s.get('articoli_30gg', 0)}")
        if emoji == "🔴" and s.get('ultimo_messaggio'):
            col_s1.caption(f"↳ {s['ultimo_messaggio'][:120]}")
    st.divider()

    # MODIFICA: SEZIONE 1 - INTERFACCIA ON/OFF PERSONALE (Punto 1)
    st.subheader("🎛️ Il Tuo Pannello di Controllo Canali (Personale)")
    st.caption("Spegni i canali che non vuoi vedere nel tuo feed. Questa modifica ha effetto solo sul tuo account.")
    
    fonti_personali = db.carica_fonti_con_preferenze(st.session_state.user['id'])
    for f in fonti_personali:
        col_info, col_toggle = st.columns([4, 1])
        # Normalizzo: tutto ciò che non è 'Editoriale' è trattato come 'Ufficiale'
        tipo_f = "Editoriale" if (f.get('tipo_fonte') or '').lower() == "editoriale" else "Ufficiale"
        emoji_tipo = "📰" if tipo_f == "Editoriale" else "🏛️"
        col_info.markdown(
            f"**{html.escape(str(f['nome']))}** {emoji_tipo} "
            f"<span style='font-size:12px;color:#888;'>· {tipo_f}</span>",
            unsafe_allow_html=True
        )
        
        # Gestione interruttore ON/OFF in tempo reale
        is_on = col_toggle.toggle("Attivo", value=f['utente_attiva'], key=f"tog_{f['id']}")
        if is_on != f['utente_attiva']:
            db.imposta_preferenza_fonte(st.session_state.user['id'], f['id'], is_on)
            st.rerun()
            
    st.divider()
    
    # SEZIONE 2 - AGGIUNTA GLOBALE (Per tutti)
    st.subheader("➕ Aggiungi Nuova Fonte (Globale)")
    st.caption("Indica solo il tipo di fonte. La categoria (Leggi / Provvedimenti / Sentenze) e il tema "
               "sono determinati automaticamente dall'AI. Le fonti Editoriali producono sempre News.")
    with st.form("form_aggiunta_fonte", clear_on_submit=True):
        c1, c2 = st.columns(2)
        n_nome = c1.text_input("Nome della fonte")
        n_url = c1.text_input("URL Feed RSS")
        n_tipo_fonte = c2.selectbox("Tipo di fonte", ["Ufficiale", "Editoriale"],
                                    help="Ufficiale → fonti del diritto (Gazzetta Ufficiale, autorità, tribunali): "
                                         "gli articoli diventano Leggi, Provvedimenti o Sentenze. "
                                         "Editoriale → testate e portali (Altalex, Cybersecurity360): diventano News.")
        n_tipo_ingestion = c2.selectbox("Modalità di acquisizione", ["rss", "scraper"],
                                        help="'rss' per feed standard. 'scraper' richiede un parser dedicato (avanzato).")
        # Campi legacy mantenuti nel DB con valori neutri (non più gestiti dall'utente)
        n_area = "Generale"
        n_macro = "News & Aggiornamenti" if n_tipo_fonte == "Editoriale" else "Provvedimenti & Sentenze"
        if st.form_submit_button("➕ Salva Fonte nel Database Comune"):
            if n_nome and n_url:
                if db.aggiungi_fonte(n_nome, n_url, n_area, n_macro, n_tipo_fonte, n_tipo_ingestion):
                    st.success(f"Fonte '{n_nome}' registrata nel database globale!")
                    st.rerun()
                else:
                    st.error("Errore. URL probabilmente già registrato.")
            else:
                st.error("Compila nome e URL.")
                
    st.divider()
    
    # SEZIONE 3 - GESTIONE CATALOGO GLOBALE (riservata agli admin)
    is_admin = st.session_state.user.get('role', 'user') == 'admin'
    st.subheader("🗂️ Database Globale Fonti")
    if is_admin:
        st.caption("Come admin puoi cambiare il tipo di ogni fonte (Ufficiale/Editoriale) ed eliminarle. "
                   "Le modifiche valgono per tutti gli utenti.")
    else:
        st.caption("Elenco delle fonti del catalogo comune. La gestione è riservata agli amministratori.")
    for f in fonti_personali:
        tipo_corrente = "Editoriale" if (f.get('tipo_fonte') or '').lower() == "editoriale" else "Ufficiale"
        if is_admin:
            col_t, col_tipo, col_b = st.columns([3, 1.4, 1])
            col_t.markdown(f"**{html.escape(str(f['nome']))}**<br><span style='font-size:11px;color:#888;'>{html.escape(str(f['url']))}</span>", unsafe_allow_html=True)
            nuovo_tipo = col_tipo.selectbox(
                "Tipo", ["Ufficiale", "Editoriale"],
                index=0 if tipo_corrente == "Ufficiale" else 1,
                key=f"tipo_{f['id']}", label_visibility="collapsed"
            )
            if nuovo_tipo != tipo_corrente:
                db.imposta_tipo_fonte(f['id'], nuovo_tipo)
                st.rerun()
            if col_b.button("Elimina", key=f"del_src_{f['id']}"):
                db.rimuovi_fonte(f['id'])
                st.success("Fonte rimossa dal sistema.")
                st.rerun()
        else:
            col_t, col_b = st.columns([5, 1])
            col_t.markdown(f"**{html.escape(str(f['nome']))}** <span style='font-size:11px;color:#888;'>· {tipo_corrente}</span>", unsafe_allow_html=True)
            col_b.caption("🔒")
        st.write("---")

    # SEZIONE 3-BIS - KEYWORD DEGLI ALERT (solo admin)
    if is_admin:
        st.divider()
        st.subheader("🚨 Parole chiave degli Alert (Admin)")
        st.caption("Gli articoli il cui titolo contiene una di queste parole compaiono nella striscia "
                   "'Alert del team' in Dashboard. La corrispondenza è parziale: 'sanzion' intercetta "
                   "sanzione, sanzioni, sanzionato. Valgono per tutto il team.")
        kw_correnti = db.lista_keywords_alert()
        if kw_correnti:
            n_col = 4
            righe_kw = [kw_correnti[i:i+n_col] for i in range(0, len(kw_correnti), n_col)]
            for riga in righe_kw:
                cols = st.columns(n_col)
                for col, kw in zip(cols, riga):
                    with col:
                        if st.button(f"✕ {kw['keyword']}", key=f"delkw_{kw['id']}", use_container_width=True,
                                     help="Clicca per rimuovere questa parola chiave"):
                            db.rimuovi_keyword_alert(kw['id'])
                            st.rerun()
        else:
            st.info("Nessuna parola chiave configurata: la striscia Alert non mostrerà nulla.")
        with st.form("form_add_keyword", clear_on_submit=True):
            c_kw, c_btn = st.columns([3, 1])
            nuova_kw = c_kw.text_input("Nuova parola chiave", placeholder="es. ai act, dora, telemarketing...",
                                       label_visibility="collapsed")
            if c_btn.form_submit_button("➕ Aggiungi", use_container_width=True):
                if db.aggiungi_keyword_alert(nuova_kw):
                    st.rerun()
                else:
                    st.warning("Parola vuota o già presente.")

    # SEZIONE 4 - GESTIONE UTENTI (solo admin)
    if is_admin:
        st.divider()
        st.subheader("👥 Gestione Utenti (Admin)")
        st.caption("Promuovi un utente ad admin o riportalo a utente standard. Non è possibile declassare l'ultimo admin rimasto.")
        utenti = db.lista_utenti()
        for u in utenti:
            col_u, col_r, col_act = st.columns([3, 1, 2])
            icona = "👑" if u['role'] == 'admin' else "👤"
            col_u.markdown(f"{icona} **{u['username']}**")
            col_r.caption(u['role'])
            with col_act:
                if u['role'] == 'user':
                    if st.button("Promuovi ad admin", key=f"promote_{u['id']}"):
                        db.imposta_ruolo(u['id'], 'admin')
                        st.rerun()
                else:
                    # Non mostro il declassamento per se stessi per evitare auto-lock confusi
                    if u['id'] != st.session_state.user['id']:
                        if st.button("Declassa a utente", key=f"demote_{u['id']}"):
                            if db.imposta_ruolo(u['id'], 'user'):
                                st.rerun()
                            else:
                                st.warning("Impossibile: deve restare almeno un admin.")
                    else:
                        col_act.caption("(tu)")

        # SEZIONE 5 - RESET ARCHIVIO (solo admin, con conferma)
        st.divider()
        st.subheader("🧹 Reset archivio (Admin)")
        st.caption("Azzera tutti gli articoli, i bookmark e lo stato letto, riportando l'archivio a zero (t0). "
                   "Utenti, fonti e preferenze restano intatti. L'operazione è irreversibile.")
        conferma_reset = st.checkbox("Confermo: voglio svuotare l'intero archivio articoli", key="conf_reset")
        if st.button("🧹 Azzera archivio ora", disabled=not conferma_reset, key="btn_reset"):
            n = db.reset_archivio()
            # Pulisco anche le cache di sessione legate agli articoli
            st.session_state.ai_summaries = {}
            st.session_state.micro_riassunti = {}
            st.success(f"Archivio azzerato: rimossi {n} articoli. Riparti da t0.")
            st.rerun()
