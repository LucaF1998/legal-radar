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
import pdf_export
from contextlib import contextmanager
from bs4 import BeautifulSoup
from datetime import datetime
import psycopg2
import psycopg2.errors
import psycopg2.extras
import bcrypt
from typing import List, Dict, Tuple, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# --- TESTO DELL'INFORMATIVA PRIVACY (art. 13 GDPR) ---
# NB: i campi tra parentesi quadre vanno compilati prima della pubblicazione.
TESTO_PRIVACY_POLICY = """
**Ultimo aggiornamento:** [data]

#### 1. Titolare del trattamento
Il titolare del trattamento dei dati è **[Nome del titolare / ragione sociale]**, con sede in **[indirizzo]**, contattabile all'indirizzo email **[email di contatto]**.

#### 2. Quali dati raccogliamo
Nell'ambito dell'utilizzo della piattaforma "Legal Radar" trattiamo:
- **Dati di registrazione e accesso:** il nome utente (o indirizzo email) e la password da te scelti per creare l'account. La password non è mai conservata in chiaro, ma esclusivamente in forma cifrata (hash).
- **Dati di utilizzo del servizio:** le preferenze e le attività che generi usando la piattaforma, ad esempio le fonti che attivi o disattivi, gli articoli e i report che salvi tra i preferiti e lo stato di lettura dei contenuti.

Non raccogliamo categorie particolari di dati (art. 9 GDPR) e non effettuiamo attività di profilazione.

#### 3. Perché trattiamo i tuoi dati e su quale base giuridica
- **Erogazione del servizio:** consentirti la registrazione, l'autenticazione e l'uso delle funzioni della piattaforma. Base giuridica: esecuzione di un contratto/servizio di cui sei parte (art. 6.1.b GDPR).
- **Sicurezza e corretto funzionamento:** garantire l'integrità degli accessi e prevenire usi non autorizzati. Base giuridica: legittimo interesse del titolare (art. 6.1.f GDPR).

#### 4. Come trattiamo e dove conserviamo i dati
I dati sono conservati in un database (PostgreSQL) ospitato su infrastruttura cloud. Adottiamo misure tecniche adeguate, tra cui la cifratura delle password e l'accesso riservato tramite credenziali. I dati possono essere trattati su server situati nell'Unione Europea o, ove i fornitori lo prevedano, in paesi che garantiscono un livello di protezione adeguato ai sensi del GDPR.

#### 5. Per quanto tempo conserviamo i dati
I dati dell'account sono conservati per tutta la durata dell'utilizzo del servizio e fino a **[periodo, es. 12 mesi]** dall'ultimo accesso o dalla richiesta di cancellazione, salvo diversi obblighi di legge.

#### 6. A chi comunichiamo i dati
I dati non sono diffusi né ceduti a terzi per finalità commerciali. Possono essere trattati, per nostro conto e come responsabili del trattamento, dai fornitori dei servizi di hosting e infrastruttura cloud necessari al funzionamento della piattaforma.

#### 7. I tuoi diritti
In qualità di interessato puoi in ogni momento esercitare i diritti previsti dagli artt. 15-22 del GDPR: accesso ai tuoi dati, rettifica, cancellazione, limitazione e opposizione al trattamento, nonché portabilità dei dati. Hai inoltre il diritto di proporre reclamo all'Autorità Garante per la protezione dei dati personali (www.garanteprivacy.it). Per esercitare i tuoi diritti puoi scrivere a **[email di contatto]**.
"""

# --- 1. CLASSE ARCHITETTURALE DATABASE (POSTGRESQL MULTI-TENANT) ---
@st.cache_resource
def _conn_condivisa():
    """Contenitore cachato della connessione condivisa al database.
    È un dict mutabile {'c': connessione} così posso rimpiazzare la connessione
    interna (se Neon la chiude) senza perdere il contenitore cachato.
    Cachato da Streamlit = vive tra i rerun = niente riapertura ad ogni clic."""
    return {"c": None}


class LegalRadarDB:
    def __init__(self, db_url: str):
        self.db_url = db_url

    def _apri_connessione(self):
        """Apre una connessione nuova. keepalives: aiuta a tenere viva la connessione
        ed evitare che firewall/Neon la chiudano silenziosamente."""
        return psycopg2.connect(
            self.db_url,
            connect_timeout=10,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
        )

    def _connessione_valida(self):
        """Restituisce la connessione condivisa, RIAPRENDOLA se assente o chiusa/stantia.
        È la chiave dell'ottimizzazione: riusa la stessa connessione tra i rerun
        (niente handshake ad ogni clic), ma si auto-ripara se Neon l'ha chiusa.
        Il ping SELECT 1 su connessione già aperta costa pochi ms: trascurabile
        rispetto ai ~250ms di un handshake completo che facevamo prima ad ogni query."""
        conn = _conn_condivisa()
        need_new = conn["c"] is None or getattr(conn["c"], "closed", 1) != 0
        if not need_new:
            try:
                with conn["c"].cursor() as ping:
                    ping.execute("SELECT 1")
            except Exception:
                need_new = True
        if need_new:
            try:
                if conn["c"] is not None:
                    conn["c"].close()
            except Exception:
                pass
            conn["c"] = self._apri_connessione()
        return conn["c"]

    # --- CONNESSIONE: context manager con commit/rollback GARANTITI ---
    # La connessione viene RIUSATA tra i rerun (non più aperta/chiusa ad ogni metodo):
    # è questo che elimina il "paio di secondi" ad ogni clic.
    @contextmanager
    def get_cursor(self, dict_cursor: bool = False):
        conn = self._connessione_valida()
        factory = psycopg2.extras.DictCursor if dict_cursor else None
        cur = conn.cursor(cursor_factory=factory)
        try:
            yield cur
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            cur.close()  # chiudo solo il cursore, NON la connessione (la riuso)

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
            # --- INTEGRAZIONE RADAR (Fase 1): tabella dedicata ai report ricchi del Radar ---
            """
            CREATE TABLE IF NOT EXISTS legal_radar_reports (
                id SERIAL PRIMARY KEY,
                id_report TEXT UNIQUE NOT NULL,
                data_report DATE,
                area TEXT,
                tag VARCHAR(10),
                titolo TEXT NOT NULL,
                livello_rischio VARCHAR(10),
                score_rischio VARCHAR(10),
                fatto_nuovo TEXT,
                scadenza TEXT,
                sintesi TEXT,
                analisi TEXT,
                impatto TEXT,
                action_point TEXT,
                riferimenti TEXT,
                fonte_ufficiale TEXT,
                link_documento TEXT,
                link_fonti TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_lrr_data ON legal_radar_reports(data_report DESC)",
            # Banner: collegamento manuale notizia RSS (articles) <-> report (legal_radar_reports)
            """
            CREATE TABLE IF NOT EXISTS report_links (
                article_id INTEGER REFERENCES articles(id) ON DELETE CASCADE,
                report_id INTEGER REFERENCES legal_radar_reports(id) ON DELETE CASCADE,
                PRIMARY KEY (article_id, report_id)
            )
            """,
            # Stato letto/non letto dei report, per utente (parallelo a user_article_status)
            """
            CREATE TABLE IF NOT EXISTS user_report_status (
                user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                report_id INTEGER REFERENCES legal_radar_reports(id) ON DELETE CASCADE,
                letto BOOLEAN DEFAULT TRUE,
                PRIMARY KEY (user_id, report_id)
            )
            """,
            # Report salvati, per utente (parallelo a bookmarks)
            """
            CREATE TABLE IF NOT EXISTS report_bookmarks (
                user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                report_id INTEGER REFERENCES legal_radar_reports(id) ON DELETE CASCADE,
                PRIMARY KEY (user_id, report_id)
            )
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

    def reimposta_password(self, target_user_id: int, nuova_password: str) -> bool:
        """Reimposta la password di un utente (azione admin). Salva solo l'hash bcrypt:
        la password in chiaro non viene mai memorizzata. Ritorna False se troppo corta o su errore."""
        if not nuova_password or len(nuova_password) < 8:
            return False
        hashed = bcrypt.hashpw(nuova_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        try:
            with self.get_cursor() as cur:
                cur.execute("UPDATE users SET password_hash = %s WHERE id = %s", (hashed, target_user_id))
                return cur.rowcount > 0
        except Exception as e:
            logging.error("Errore reset password: %s", e)
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
                             tema: Optional[str] = None, fonti: Optional[List[str]] = None,
                             giorni: Optional[int] = None, limite: int = 100) -> List[Dict]:
        """Estrae articoli per categoria AI (sentenza/provvedimento/news), con filtro tema, fonte e data opzionali,
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
        if fonti:
            # Filtro per una o più fonti selezionate nella sessione corrente
            placeholders = ", ".join(["%s"] * len(fonti))
            query += f" AND a.fonte IN ({placeholders})"
            params.extend(fonti)
        if giorni:
            # Filtro temporale sulla data dell'atto (pubblicazione reale, o scansione come fallback)
            query += " AND COALESCE(a.data_pubblicazione, a.data_scansione) >= NOW() - (INTERVAL '1 day' * %s)"
            params.append(giorni)
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

    def lista_fonti_per_tipo(self, tipo_atto: str, user_id: int) -> List[str]:
        """Fonti che hanno almeno un articolo della categoria data (per popolare il filtro fonte),
        escludendo quelle spente dall'utente."""
        query = """
            SELECT DISTINCT a.fonte
            FROM articles a
            LEFT JOIN sources src ON src.nome = a.fonte
            WHERE CASE
                      WHEN LOWER(COALESCE(src.tipo_fonte,'')) = 'editoriale' THEN 'news'
                      ELSE COALESCE(a.tipo_atto, 'provvedimento')
                  END = %s
            AND a.fonte IS NOT NULL
            AND a.fonte NOT IN (
                SELECT s.nome FROM sources s
                JOIN user_source_preferences usp ON s.id = usp.source_id
                WHERE usp.user_id = %s AND usp.is_active = FALSE
            )
            ORDER BY a.fonte ASC
        """
        with self.get_cursor() as cur:
            cur.execute(query, (tipo_atto, user_id))
            return [r[0] for r in cur.fetchall()]

    def ricerca_globale(self, user_id: int, testo: str, limite: int = 40) -> Dict[str, List[Dict]]:
        """Cerca lo stesso testo su articoli e report del Radar. Ritorna due liste separate.
        Rispetta le fonti spente dall'utente per gli articoli."""
        if not testo or not testo.strip():
            return {"articoli": [], "report": []}
        tp = f"%{testo.strip()}%"
        # Articoli
        q_art = f"""
            SELECT a.*, COALESCE(uas.letto, FALSE) AS letto, src.tipo_fonte,
                   CASE WHEN LOWER(COALESCE(src.tipo_fonte,'')) = 'editoriale' THEN 'news'
                        ELSE COALESCE(a.tipo_atto, 'provvedimento') END AS tipo_atto_eff
            FROM articles a
            LEFT JOIN user_article_status uas ON uas.article_id = a.id AND uas.user_id = %s
            LEFT JOIN sources src ON src.nome = a.fonte
            WHERE (a.titolo ILIKE %s OR a.preview ILIKE %s OR a.area ILIKE %s OR a.riassunto_ai ILIKE %s)
            AND {self._filtro_fonti_attive(user_id)}
            ORDER BY COALESCE(a.data_pubblicazione, a.data_scansione) DESC LIMIT %s
        """
        # Report del Radar
        q_rep = """
            SELECT r.*, (urs.report_id IS NOT NULL) AS letto, (rb.report_id IS NOT NULL) AS salvato
            FROM legal_radar_reports r
            LEFT JOIN user_report_status urs ON urs.report_id = r.id AND urs.user_id = %s
            LEFT JOIN report_bookmarks rb ON rb.report_id = r.id AND rb.user_id = %s
            WHERE (r.titolo ILIKE %s OR r.sintesi ILIKE %s OR r.area ILIKE %s OR r.analisi ILIKE %s)
            ORDER BY r.data_report DESC, r.created_at DESC LIMIT %s
        """
        with self.get_cursor(dict_cursor=True) as cur:
            cur.execute(q_art, (user_id, tp, tp, tp, tp, user_id, limite))
            articoli = [dict(a) for a in cur.fetchall()]
            cur.execute(q_rep, (user_id, user_id, tp, tp, tp, tp, limite))
            report = [dict(r) for r in cur.fetchall()]
        return {"articoli": articoli, "report": report}

    def lista_temi(self, tipo_atto: Optional[str] = None) -> List[str]:
        """Elenco dei temi presenti in archivio, normalizzati e deduplicati (per il filtro)."""
        if tipo_atto:
            query = "SELECT DISTINCT tema FROM articles WHERE tema IS NOT NULL AND tipo_atto = %s"
            args = (tipo_atto,)
        else:
            query = "SELECT DISTINCT tema FROM articles WHERE tema IS NOT NULL"
            args = ()
        with self.get_cursor() as cur:
            cur.execute(query, args)
            righe = cur.fetchall()
        # Normalizzo e deduplico: le varianti collassano sulla forma canonica
        temi = sorted({normalizza_tema(r[0]) for r in righe})
        return temi

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
        """I temi (normalizzati) con più articoli, per scegliere i blocchi tematici della dashboard."""
        query = f"""
            SELECT a.tema, COUNT(*) AS n
            FROM articles a
            WHERE a.tema IS NOT NULL AND {self._filtro_fonti_attive(user_id)}
            GROUP BY a.tema
        """
        with self.get_cursor(dict_cursor=True) as cur:
            cur.execute(query, (user_id,))
            righe = cur.fetchall()
        # Accorpo i conteggi sulle forme canoniche (le varianti si sommano)
        conteggi: Dict[str, int] = {}
        for r in righe:
            canon = normalizza_tema(r['tema'])
            conteggi[canon] = conteggi.get(canon, 0) + r['n']
        ordinati = sorted(conteggi.items(), key=lambda x: x[1], reverse=True)
        return [t for t, _ in ordinati[:limite]]

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

    # --- REPORT DEL RADAR (Fase 1: vetrina dei report mattutini) ---
    def estrai_report_radar(self, user_id: int, ricerca_testo: str = "", filtro_rischio: str = "",
                            filtro_tag: str = "", solo_salvati: bool = False,
                            limite: int = 25, offset: int = 0) -> List[Dict]:
        """Legge i report del Radar con filtri, paginazione e stato letto/salvato per l'utente.
        Tecnica limite+1: chiedo un elemento in più per sapere se esistono altri, senza COUNT."""
        query = """
            SELECT r.*,
                   (urs.report_id IS NOT NULL) AS letto,
                   (rb.report_id IS NOT NULL) AS salvato
            FROM legal_radar_reports r
            LEFT JOIN user_report_status urs ON urs.report_id = r.id AND urs.user_id = %s
            LEFT JOIN report_bookmarks rb ON rb.report_id = r.id AND rb.user_id = %s
        """
        condizioni: List[str] = []
        params: List = [user_id, user_id]
        if solo_salvati:
            condizioni.append("rb.report_id IS NOT NULL")
        if ricerca_testo:
            condizioni.append("(r.titolo ILIKE %s OR r.sintesi ILIKE %s OR r.area ILIKE %s OR r.analisi ILIKE %s)")
            tp = f"%{ricerca_testo}%"
            params.extend([tp, tp, tp, tp])
        if filtro_rischio:
            condizioni.append("UPPER(r.livello_rischio) = %s")
            params.append(filtro_rischio.upper())
        if filtro_tag:
            condizioni.append("UPPER(r.tag) = %s")
            params.append(filtro_tag.upper())
        if condizioni:
            query += " WHERE " + " AND ".join(condizioni)
        query += " ORDER BY r.data_report DESC, r.created_at DESC LIMIT %s OFFSET %s"
        params.extend([limite + 1, offset])
        with self.get_cursor(dict_cursor=True) as cur:
            cur.execute(query, params)
            righe = cur.fetchall()
        return [dict(r) for r in righe]

    def conta_report_radar(self) -> int:
        with self.get_cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM legal_radar_reports")
            return cur.fetchone()[0]

    def segna_report_letto(self, user_id: int, report_id: int) -> None:
        with self.get_cursor() as cur:
            cur.execute(
                "INSERT INTO user_report_status (user_id, report_id, letto) VALUES (%s, %s, TRUE) "
                "ON CONFLICT (user_id, report_id) DO NOTHING",
                (user_id, report_id)
            )

    def segna_tutti_report_letti(self, user_id: int) -> None:
        with self.get_cursor() as cur:
            cur.execute("""
                INSERT INTO user_report_status (user_id, report_id, letto)
                SELECT %s, id, TRUE FROM legal_radar_reports
                ON CONFLICT (user_id, report_id) DO NOTHING
            """, (user_id,))

    def aggiungi_report_bookmark(self, user_id: int, report_id: int) -> None:
        with self.get_cursor() as cur:
            cur.execute(
                "INSERT INTO report_bookmarks (user_id, report_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (user_id, report_id)
            )

    def rimuovi_report_bookmark(self, user_id: int, report_id: int) -> None:
        with self.get_cursor() as cur:
            cur.execute("DELETE FROM report_bookmarks WHERE user_id = %s AND report_id = %s", (user_id, report_id))

    def estrai_ultimi_report_dashboard(self, limite: int = 3) -> List[Dict]:
        """Ultimi report del Radar per la fascia in dashboard (senza stato per utente)."""
        with self.get_cursor(dict_cursor=True) as cur:
            cur.execute(
                "SELECT id, titolo, area, tag, livello_rischio, data_report, sintesi "
                "FROM legal_radar_reports ORDER BY data_report DESC, created_at DESC LIMIT %s",
                (limite,)
            )
            return [dict(r) for r in cur.fetchall()]

    def lista_report_per_collegamento(self, limite: int = 50) -> List[Dict]:
        """Report recenti, in forma compatta, per popolare il selettore del banner."""
        with self.get_cursor(dict_cursor=True) as cur:
            cur.execute("SELECT id, titolo, data_report FROM legal_radar_reports ORDER BY data_report DESC, created_at DESC LIMIT %s", (limite,))
            return [dict(r) for r in cur.fetchall()]

    def collega_articolo_report(self, article_id: int, report_id: int) -> None:
        with self.get_cursor() as cur:
            cur.execute(
                "INSERT INTO report_links (article_id, report_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (article_id, report_id)
            )

    def scollega_articolo_report(self, article_id: int, report_id: int) -> None:
        with self.get_cursor() as cur:
            cur.execute("DELETE FROM report_links WHERE article_id = %s AND report_id = %s", (article_id, report_id))

    def elimina_report_radar(self, report_id: int) -> None:
        """Elimina un report del Radar. I collegamenti in report_links vengono rimossi
        automaticamente dal vincolo ON DELETE CASCADE."""
        with self.get_cursor() as cur:
            cur.execute("DELETE FROM legal_radar_reports WHERE id = %s", (report_id,))

    def report_collegati_a_articolo(self, article_id: int) -> List[Dict]:
        """Report collegati a una notizia RSS (per mostrare il banner di rimando)."""
        with self.get_cursor(dict_cursor=True) as cur:
            cur.execute("""
                SELECT r.id, r.titolo, r.data_report
                FROM legal_radar_reports r
                JOIN report_links rl ON rl.report_id = r.id
                WHERE rl.article_id = %s
                ORDER BY r.data_report DESC
            """, (article_id,))
            return [dict(r) for r in cur.fetchall()]

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

# --- LEVA 2: CACHE DEI DATI STABILI ---
# Cacho solo dati che cambiano di rado (fonti, temi, metriche), MAI dati "vivi"
# come stato letto/salvato o contenuto articoli. ttl = scadenza in secondi: dopo
# quel tempo la cache si rinfresca da sola, così le novità arrivano comunque.
# Quando l'utente modifica le fonti, svuotiamo esplicitamente la cache (vedi più sotto).

@st.cache_data(ttl=300)  # 5 min: i temi cambiano solo quando arrivano nuovi articoli
def cache_lista_temi(tipo_atto):
    return db.lista_temi(tipo_atto)

@st.cache_data(ttl=300)  # 5 min: l'elenco fonti per categoria cambia di rado
def cache_lista_fonti_per_tipo(tipo_atto, user_id):
    return db.lista_fonti_per_tipo(tipo_atto, user_id)

@st.cache_data(ttl=60)   # 1 min: le metriche dashboard possono aggiornarsi, ma non a ogni clic
def cache_metriche_dashboard(user_id):
    return db.estrai_metriche_dashboard(user_id)

def svuota_cache_fonti():
    """Da chiamare dopo modifiche alle fonti (aggiunta/rimozione/preferenze),
    così i filtri si aggiornano subito invece di aspettare la scadenza."""
    cache_lista_temi.clear()
    cache_lista_fonti_per_tipo.clear()


if 'user' not in st.session_state: st.session_state.user = None
if 'ai_summaries' not in st.session_state: st.session_state.ai_summaries = {}
if 'micro_riassunti' not in st.session_state: st.session_state.micro_riassunti = {}

# --- 3. STILE GRAFICO ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Roboto:wght@400;500;600;700&family=Roboto+Condensed:wght@500;600;700&family=Roboto+Mono:wght@400;500;700&display=swap');

    :root{
        /* ===== Design System Facile.it ===== */
        --bg:#F8F8F8; --surface:#ffffff;
        --ink:#3A3A3A; --ink-soft:#505050; --ink-faint:#888888;
        --hair:#E4E4E4;
        --accent:#FF6600; --accent-hover:#E34713; --accent-soft:#FFF1E8; --accent-tint:#FFF1E8;
        /* categorie (palette Facile) */
        --legge-bg:#FFF1E8; --legge-tx:#E34713;
        --provv-bg:#E6F3FC; --provv-tx:#154F8A;
        --sent-bg:#F2F2F2;  --sent-tx:#3A3A3A;
        --news-bg:#F1F9E9;  --news-tx:#549116;
        --alta:#C2212E; --alta-soft:#FFEFEF;
        --sf:'Roboto',-apple-system,BlinkMacSystemFont,sans-serif;
        --display:'Roboto Condensed','Roboto',sans-serif;
        --mono:'Roboto Mono',ui-monospace,monospace;
    }

    /* superficie generale */
    .stApp{ background:var(--bg); }
    html, body, [class*="css"]{ font-family:var(--sf); -webkit-font-smoothing:antialiased; color:var(--ink); }
    .block-container{ padding-top:3.5rem; padding-bottom:4rem; max-width:960px; }

    /* titoli di sistema */
    h1, h2, h3{ font-family:var(--display); letter-spacing:0; color:var(--ink); font-weight:700; }
    h1{ font-size:32px; }
    h2{ font-size:24px; }

    /* ---- SIDEBAR ---- */
    section[data-testid="stSidebar"]{ background:var(--surface); border-right:1px solid var(--hair); }
    section[data-testid="stSidebar"] .block-container{ padding-top:2.2rem; }
    .sb-brand{ font-family:var(--mono); font-size:14px; font-weight:700; letter-spacing:.08em;
               text-transform:uppercase; color:var(--ink); margin-bottom:14px; }
    .sb-logo{ height:24px; margin-bottom:10px; display:block; }
    .sb-user{ font-size:13px; font-weight:500; color:var(--ink-soft); margin-bottom:6px;
              white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
    .sb-role{ font-family:var(--mono); font-size:10px; font-weight:700; letter-spacing:.06em; text-transform:uppercase;
              background:var(--ink); color:#fff; border-radius:4px; padding:3px 7px; margin-right:5px; }
    .sb-unread{ display:inline-block; font-size:12px; font-weight:700; color:var(--accent);
                background:var(--accent-soft); border-radius:999px; padding:4px 11px; margin-bottom:18px; }

    /* radio di navigazione -> lista */
    section[data-testid="stSidebar"] div[role="radiogroup"]{ gap:2px; }
    section[data-testid="stSidebar"] div[role="radiogroup"] > label{
        padding:9px 13px; border-radius:8px; transition:background .12s; width:100%;
        margin:0; cursor:pointer;
    }
    section[data-testid="stSidebar"] div[role="radiogroup"] > label:hover{ background:var(--bg); }
    section[data-testid="stSidebar"] div[role="radiogroup"] > label > div:first-child{ display:none; }
    section[data-testid="stSidebar"] div[role="radiogroup"] > label p{
        font-size:14px; font-weight:500; color:var(--ink-soft);
    }
    /* voce selezionata: tinta arancio */
    section[data-testid="stSidebar"] div[role="radiogroup"] > label:has(input:checked){
        background:var(--accent-soft);
    }
    section[data-testid="stSidebar"] div[role="radiogroup"] > label:has(input:checked) p{
        color:var(--accent); font-weight:700;
    }

    /* ---- CARD ATTI (lista sezioni) ---- */
    .radar-card{
        background:var(--surface); border-radius:16px; padding:22px 26px;
        box-shadow:0 1px 2px 0 rgba(0,0,0,.05);
        margin-bottom:14px; transition:border-color .12s, box-shadow .18s;
        border:1px solid var(--hair);
    }
    .radar-card:hover{ box-shadow:0 4px 12px 0 rgba(0,0,0,.1); }
    .radar-card.letta{ opacity:.55; }

    .card-title{
        font-family:var(--display); font-weight:700; font-size:21px; line-height:1.3;
        color:var(--ink); text-decoration:none; display:block; margin:12px 0 0;
    }
    .card-title:hover{ color:var(--accent); }
    .card-preview{ margin-top:10px; font-size:14px; line-height:1.55; color:var(--ink-soft); }
    .card-microsummary{
        margin-top:14px; font-size:14px; line-height:1.6; color:var(--ink-soft);
        border-left:3px solid var(--accent); padding:4px 0 4px 16px;
    }

    /* badge a pillola */
    .meta-tag{
        display:inline-block; font-size:11px; font-weight:600; letter-spacing:.2px;
        padding:3px 10px; border-radius:999px; margin-right:6px; vertical-align:middle;
    }
    .tag-area{ background:#F2F2F2; color:var(--ink-faint); }
    .tag-fonte{ background:#F2F2F2; color:var(--ink-faint); }
    .tag-rango{ background:transparent; color:var(--ink-faint); padding-left:2px; font-family:var(--mono); font-size:11px; }
    .badge-ril{ font-size:11px; font-weight:700; padding:3px 10px; border-radius:999px; }
    .badge-ril-alta{ background:var(--alta-soft); color:var(--alta); }
    .badge-ril-media{ background:var(--accent-soft); color:var(--accent-hover); }
    .badge-nuovo{
        font-size:11px; font-weight:700; color:#fff; background:var(--accent);
        border-radius:999px; padding:3px 9px; margin-left:8px; vertical-align:2px;
    }

    /* categorie come badge (palette Facile) */
    .cat-badge{ display:inline-block; font-size:11px; font-weight:700; padding:3px 10px; border-radius:999px; }
    .cat-legge{ background:var(--legge-bg); color:var(--legge-tx); }
    .cat-provv{ background:var(--provv-bg); color:var(--provv-tx); }
    .cat-sent{ background:var(--sent-bg); color:var(--sent-tx); }
    .cat-news{ background:var(--news-bg); color:var(--news-tx); }

    /* bottoni Streamlit */
    .stButton > button{
        font-family:var(--sf); font-weight:600; font-size:13.5px;
        border-radius:8px; border:1px solid var(--hair); background:var(--surface); color:var(--ink);
        padding:9px 18px; transition:.12s;
    }
    .stButton > button:hover{ border-color:var(--accent); color:var(--accent); background:var(--surface); }
    .stButton > button[kind="primary"]{ background:var(--accent); color:#fff; border:none; }
    .stButton > button[kind="primary"]:hover{ background:var(--accent-hover); color:#fff; }
    .stButton > button:disabled{ background:transparent; color:var(--ink-faint); border-color:transparent; opacity:1; }
    .stButton > button:disabled:hover{ background:transparent; }

    /* input e select */
    .stTextInput input, .stSelectbox div[data-baseweb="select"] > div{
        border-radius:8px !important; border-color:var(--hair) !important;
    }
    .stTextInput input:focus{
        border-color:var(--accent) !important; box-shadow:0 0 0 3px rgba(255,102,0,.25) !important;
    }

    /* ---- DASHBOARD PRIMA PAGINA ---- */
    .pp-hero-eyebrow{ font-family:var(--mono); font-size:11px; font-weight:700; color:var(--ink-soft);
                      letter-spacing:.08em; text-transform:uppercase; }
    .pp-hero-eyebrow::before{ content:""; display:inline-block; width:8px; height:8px; border-radius:50%;
                              background:var(--accent); margin-right:8px; vertical-align:middle; }
    .pp-hero-title{
        font-family:var(--display); font-weight:700; font-size:42px; line-height:1.15;
        color:var(--ink); text-decoration:none; display:block; margin:14px 0 0;
    }
    .pp-hero-title:hover{ color:var(--accent); }
    .pp-hero-sub{ margin-top:14px; font-size:17px; line-height:1.55; color:var(--ink-soft); }
    .pp-hero-row{ margin-top:20px; }

    .pp-card{
        background:var(--surface); border-radius:16px; padding:22px 24px; height:100%;
        box-shadow:0 1px 2px 0 rgba(0,0,0,.05); border:1px solid var(--hair);
        transition:box-shadow .18s;
    }
    .pp-card:hover{ box-shadow:0 4px 12px 0 rgba(0,0,0,.1); }
    .pp-card-title{
        font-family:var(--display); font-weight:700; font-size:19px; line-height:1.3;
        color:var(--ink); text-decoration:none; display:block; margin:12px 0 0;
    }
    .pp-card-title:hover{ color:var(--accent); }
    .pp-card-sum{ margin-top:10px; font-size:14px; line-height:1.5; color:var(--ink-soft); }
    .pp-foot{ margin-top:16px; font-size:12px; color:var(--ink-faint); font-weight:500; font-family:var(--mono); }

    .pp-section{ display:flex; align-items:baseline; justify-content:space-between; margin:42px 0 18px; }
    .pp-section h2{ font-family:var(--mono); font-size:13px; font-weight:700; letter-spacing:.08em;
                    text-transform:uppercase; color:var(--ink-soft); }
    .pp-section h2::before{ content:"[ "; color:var(--ink-faint); }
    .pp-section h2::after{ content:" ]"; color:var(--ink-faint); }

    .ticker-box{ background:var(--surface); border-radius:16px; padding:20px 24px;
        box-shadow:0 1px 2px 0 rgba(0,0,0,.05); border:1px solid var(--hair); }
    .ticker-box h3{ font-family:var(--mono); font-size:11px; font-weight:700; color:var(--ink-soft);
                    text-transform:uppercase; letter-spacing:.08em; margin:0 0 12px; }
    .ticker-box h3::before{ content:"[ "; color:var(--ink-faint); }
    .ticker-box h3::after{ content:" ]"; color:var(--ink-faint); }
    .ti{ padding:12px 0; border-bottom:1px solid #F2F2F2; }
    .ti:last-child{ border:none; }
    .ti .tm{ font-size:12px; color:var(--ink-faint); font-weight:500; margin-bottom:4px; }
    .ti a{ font-size:14.5px; font-weight:500; color:var(--ink); text-decoration:none; line-height:1.4; }
    .ti a:hover{ color:var(--accent); }

    .mini{ background:var(--surface); border-radius:12px; padding:16px 18px; height:100%;
        box-shadow:0 1px 2px 0 rgba(0,0,0,.05); border:1px solid var(--hair); }
    .mini .mm{ font-size:11.5px; color:var(--ink-faint); font-weight:500; margin-bottom:6px; font-family:var(--mono); }
    .mini a{ font-family:var(--display); font-size:15px; font-weight:600; color:var(--ink); text-decoration:none; line-height:1.35; }
    .mini a:hover{ color:var(--accent); }

    div[data-testid="stHorizontalBlock"]{ gap:18px; }
    hr{ border-color:var(--hair); }

    /* ---- REPORT RICCHI DEL RADAR ---- */
    .report-card{ background:var(--surface); border-radius:16px; padding:26px 30px;
        box-shadow:0 1px 2px 0 rgba(0,0,0,.05); border:1px solid var(--hair); margin-bottom:18px; }
    .report-title{ font-family:var(--display); font-weight:700; font-size:25px; line-height:1.25;
        color:var(--ink); margin-top:4px; }
    .report-sec{ margin-top:18px; }
    .report-sec-h{ font-family:var(--mono); font-size:10.5px; font-weight:700; letter-spacing:.08em; text-transform:uppercase;
        color:var(--accent); margin-bottom:5px; }
    .report-sec-b{ font-size:15px; line-height:1.62; color:var(--ink-soft); }
    .report-link{ font-size:14px; font-weight:500; color:var(--accent); text-decoration:none; }
    .report-link:hover{ color:var(--accent-hover); text-decoration:underline; }
    .rk{ display:inline-block; font-size:11px; font-weight:700; padding:3px 10px; border-radius:999px; margin-right:7px; }
    .rk-alto{ background:var(--alta-soft); color:var(--alta); }
    .rk-medio{ background:var(--accent-soft); color:var(--accent-hover); }
    .rk-basso{ background:#F1F9E9; color:#549116; }
    .rk-certo{ background:#E6F3FC; color:#154F8A; }
    .rk-segnale{ background:#F2F2F2; color:var(--ink-soft); }
    /* banner di rimando al report sulle card RSS */
    .report-banner{ background:var(--accent-soft); border-radius:10px; padding:11px 15px; margin-top:11px;
        font-size:13.5px; color:var(--accent-hover); font-weight:500; }
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

def estrai_testo_completo(url: str) -> str:
    """Estrazione più completa del testo di un articolo, per l'export PDF.
    Rispetto a estrai_testo_pulito: soglia paragrafi più bassa (non scarta righe
    corte come elenchi e dati), timeout più lungo, limite più alto. Toglie prima
    gli elementi di navigazione noti (nav, header, footer, script)."""
    if url.lower().endswith(('.pdf', '.zip', '.doc')):
        return ""
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        res = requests.get(url, headers=headers, timeout=15)
        soup = BeautifulSoup(res.text, 'html.parser')
        # Rimuovo i blocchi di navigazione/struttura prima di estrarre il testo
        for tag in soup.find_all(['nav', 'header', 'footer', 'script', 'style', 'aside']):
            tag.decompose()
        # Preferisco il contenuto dentro <article> o <main> se presenti (è il corpo vero)
        corpo = soup.find('article') or soup.find('main') or soup
        blocchi = corpo.find_all(['p', 'li', 'h2', 'h3', 'td'])
        testo = " ".join(
            b.get_text(strip=True) for b in blocchi
            if len(b.get_text(strip=True)) > 15  # soglia bassa: tengo anche righe corte
        )
        # Fallback: se ho raccolto poco, prendo tutto il testo del corpo
        if len(testo) < 200:
            testo = corpo.get_text(" ", strip=True)
        return testo[:12000]  # limite generoso; il taglio fine lo fa il PDF
    except Exception:
        return ""

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
    # Normalizzo il tema all'origine: i nuovi articoli entrano già con la forma canonica
    tema = normalizza_tema(meta.get("tema") or fonte.get('area'))
    return {
        "riassunto": meta.get("riassunto"),
        "rilevanza": meta.get("rilevanza") or "media",
        "categoria": categoria,
        "tema": tema,
        "titolo": meta.get("titolo"),
    }

def normalizza_tema(tema: Optional[str]) -> str:
    """Riconduce le varianti di tema (campo libero dell'AI) a un insieme canonico.
    Risolve i doppioni nei menu (es. 'Privacy', 'privacy ', 'Privacy e protezione dati' -> 'Privacy').
    Usata sia in lettura (app) sia dal backfill che consolida lo storico nel DB."""
    if not tema or not str(tema).strip():
        return "Generale"
    t = str(tema).strip().lower()
    # Mappa di sinonimi -> forma canonica. Il match è "contiene la chiave".
    regole = [
        (("privacy", "protezione dei dati", "protezione dati", "data protection", "gdpr", "dati personali"), "Privacy"),
        (("cyber", "sicurezza informatica", "nis2", "nis 2"), "Cybersecurity"),
        (("assicurat", "ivass", "polizz"), "Assicurativo"),
        (("banc", "finanziar", "credito", "consob", "pagam: ", "pagament"), "Bancario e finanziario"),
        (("tribut", "fiscal", "imposta", "agenzia delle entrate"), "Tributario"),
        (("consumat", "pratiche commerciali", "agcm", "antitrust"), "Consumatori e pratiche commerciali"),
        (("concorrenz", "competition"), "Concorrenza"),
        (("intelligenza artificiale", "ai act", "ia ", "machine learning"), "Intelligenza artificiale"),
        (("telemarket", "marketing"), "Telemarketing e marketing"),
    ]
    for chiavi, canonico in regole:
        if any(k in t for k in chiavi):
            return canonico
    # Voci troppo generiche -> "Generale"
    if t in ("diritto", "generale", "varie", "altro", "n/d", "nd", "null", "none"):
        return "Generale"
    # Altrimenti: forma con iniziale maiuscola, ripulita
    return str(tema).strip().capitalize()

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
def mostra_privacy_policy() -> None:
    """Pagina dell'informativa privacy (raggiungibile anche prima del login via ?policy=1)."""
    st.title("Informativa sul trattamento dei dati personali")
    st.caption("ai sensi dell'art. 13 del Regolamento UE 2016/679 (GDPR)")
    st.markdown(TESTO_PRIVACY_POLICY)
    st.write("")
    if st.button("← Torna indietro"):
        st.query_params.clear()
        st.rerun()

# Se l'utente chiede la policy (link interno), mostro quella e mi fermo qui.
# Funziona sia prima sia dopo il login, senza dipendere dalla sidebar.
if st.query_params.get("policy"):
    mostra_privacy_policy()
    st.stop()

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

    # Nota privacy: testo + link interno alla policy (nessuna spunta richiesta)
    st.markdown(
        '<div style="margin-top:14px; font-size:12.5px; color:var(--ink-soft);">'
        'Registrandoti confermi di aver letto la '
        '<a href="?policy=1" target="_self" style="color:var(--accent); text-decoration:none;">Privacy Policy</a>.'
        '</div>',
        unsafe_allow_html=True
    )
    st.stop()

# --- 6. INTERFACCIA UTENTE AUTENTICATO ---
with st.sidebar:
    # Brand pulito
    st.markdown('<img class="sb-logo" src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIyOTQuNjIxIiBoZWlnaHQ9IjgwIiB2aWV3Qm94PSIwIDAgMjk0LjYyMSA4MCIgZmlsbD0iI0ZGNjYwMCI+CiAgPHBhdGggZD0iTSA4MCA0Ny4xOTcgTCA4MCAzMC44MzMgQyA4MCAxMy43ODggNjYuMjEyIDAgNDkuMTY3IDAgTCAzMC44MzMgMCBDIDEzLjc4OCAwIDAgMTMuNzg4IDAgMzAuODMzIEwgMCA0OS4xNjcgQyAwIDY2LjIxMiAxMy43ODggODAgMzAuODMzIDgwIEwgOTAuOTA5IDgwIEMgOTAuOTA5IDgwIDgwIDc2Ljc0MiA4MCA0Ny4xOTcgWiBNIDI4LjU2MSA2Ni4yODggQyAyMy40ODUgNjYuMjg4IDE5LjA5MSA2My43ODggMTYuMjg4IDU5Ljg0OCBDIDE0LjYyMSA1Ny4zNDggMTMuNjM2IDU0LjU0NSAxMy42MzYgNTEuMzY0IEwgMTMuNjM2IDI4LjYzNiBDIDEzLjYzNiAyMy40ODUgMTYuMTM2IDE5LjE2NyAyMC4wNzYgMTYuMzY0IEMgMjIuNSAxNC42OTcgMjUuMzAzIDEzLjcxMiAyOC41NjEgMTMuNzEyIEwgNTEuMjg4IDEzLjcxMiBDIDU2LjQzOSAxMy43MTIgNjAuNzU4IDE2LjIxMiA2My41NjEgMjAuMTUyIEMgNjUuMjI3IDIyLjU3NiA2Ni4yMTIgMjUuMzc5IDY2LjIxMiAyOC42MzYgTCA2Ni4yMTIgNDguOTM5IEMgNjYuMjEyIDU1LjMwMyA2Ni41OTEgNjAuOTg1IDY3LjE5NyA2Ni4yMTIgTCAyOC41NjEgNjYuMjg4IFogTSAxNDMuNTYxIDQyLjE5NyBDIDE0My41NjEgMzIuNDI0IDEzNS44MzQgMzAuNjgyIDEyNy42NTIgMzAuNjgyIEMgMTIzLjcxMiAzMC42ODIgMTE5Ljc3MyAzMS4zNjQgMTE2LjY2NyAzMy4xMDYgQyAxMTUuMjI3IDMzLjk0IDExMy45NCAzNS4wNzYgMTEzLjAzMSAzNi41MTUgTCAxMTMuMDMxIDMxLjY2NyBMIDEwNi4yODggMzEuNjY3IEwgMTA2LjI4OCAzMC4xNTIgQyAxMDYuMjg4IDI3LjA0NiAxMDcuODc5IDI2LjY2NyAxMTAuNDU1IDI2LjY2NyBDIDExMS40NCAyNi42NjcgMTEyLjQyNCAyNi43NDMgMTEzLjQ4NSAyNi44OTQgTCAxMTMuNDg1IDE4Ljk0IEMgMTEyLjg3OSAxOC45MTQgMTEyLjI3MyAxOC44ODEgMTExLjY2NyAxOC44NDcgQyAxMTAuNDU1IDE4Ljc4IDEwOS4yNDMgMTguNzEyIDEwOC4wMzEgMTguNzEyIEMgOTguNDA5IDE4LjcxMiA5NC45MjQgMjEuNzQzIDk0LjkyNCAzMS42NjcgTCA4OS40NyAzMS42NjcgTCA4OS40NyAzOC45NCBMIDk0LjkyNCAzOC45NCBMIDk0LjkyNCA2Ni4wNjEgTCAxMDYuMjEyIDY2LjA2MSBMIDEwNi4yMTIgMzguOTQgTCAxMTEuODE4IDM4Ljk0IEMgMTExLjUxNSAzOS45MjQgMTExLjI4OCA0MS4wNjEgMTExLjIxMiA0Mi4yNzMgTCAxMjEuNzQzIDQyLjI3MyBDIDEyMi4yNzMgMzkuMzE4IDEyNC4yNDMgMzcuOTU1IDEyNy4zNDkgMzcuOTU1IEMgMTI5LjY5NyAzNy45NTUgMTMyLjgwMyAzOC45NCAxMzIuODAzIDQxLjUxNSBDIDEzMi44MDMgNDMuNjM3IDEzMS43NDMgNDQuMzE4IDEyOS44NDkgNDQuNjk3IEMgMTI4LjQzNyA0NC45ODUgMTI2Ljg4IDQ1LjE5MyAxMjUuMjc1IDQ1LjQwOCBDIDExOC4wMDggNDYuMzgxIDEwOS43NzMgNDcuNDgzIDEwOS43NzMgNTYuNjY3IEMgMTA5Ljc3MyA2My43ODggMTE0LjkyNCA2Ny4xMjEgMTIxLjUxNSA2Ny4xMjEgQyAxMjUuNjA2IDY3LjEyMSAxMjkuOTI0IDY1LjkwOSAxMzIuODc5IDYyLjg3OSBDIDEzMy4wMzEgNjQuMDE1IDEzMy4xODIgNjUuMDc2IDEzMy41NjEgNjYuMTM3IEwgMTQ0LjkyNCA2Ni4xMzcgQyAxNDMuNTYxIDYzLjQwOSAxNDMuNTYxIDYwLjIyNyAxNDMuNTYxIDU3LjI3MyBMIDE0My41NjEgNDIuMTk3IFogTSAxMjUuOTg1IDYwLjE1MiBDIDEyMy40ODUgNjAuMTUyIDEyMS4yODggNTkuMDE1IDEyMS4yODggNTYuMjEyIEMgMTIxLjI4OCA1My40ODUgMTIzLjQwOSA1Mi40MjQgMTI1LjkwOSA1MS44MTggQyAxMjYuNzYgNTEuNTc5IDEyNy42NzUgNTEuNDA1IDEyOC41NzQgNTEuMjM1IEMgMTMwLjIzOCA1MC45MTggMTMxLjg0NiA1MC42MTMgMTMyLjg3OSA0OS45MjQgQyAxMzMuMTA2IDU3LjA0NiAxMzEuMzY0IDYwLjE1MiAxMjUuOTg1IDYwLjE1MiBaIE0gMTYyLjE5NyA1OC40ODUgQyAxNTcuNDI1IDU4LjQ4NSAxNTUuNDU1IDUzLjk0IDE1NS40NTUgNDkuNDcgQyAxNTUuNDU1IDQ0LjY5NyAxNTYuNDQgMzkuMzE4IDE2Mi43MjggMzkuMzE4IEMgMTY1LjYwNyAzOS4zMTggMTY4LjE4MiA0MS4zNjQgMTY4LjI1OCA0NC4zMTggTCAxNzkuMjQzIDQ0LjMxOCBDIDE3OC40MSAzNS4yMjcgMTcwLjgzNCAzMC42ODIgMTYyLjI3MyAzMC42ODIgQyAxNTEuMjEzIDMwLjY4MiAxNDQuMjQzIDM4LjQ4NSAxNDQuMjQzIDQ5LjQ3IEMgMTQ0LjI0MyA2MCAxNTEuOTcgNjcuMTIxIDE2Mi4yNzMgNjcuMTIxIEMgMTcxLjQ0IDY3LjEyMSAxNzguNjM3IDYxLjgxOCAxNzkuNjIyIDUyLjU3NiBMIDE2OC42MzcgNTIuNTc2IEMgMTY4LjEwNyA1Ni4yMTIgMTY2LjEzNyA1OC40ODUgMTYyLjE5NyA1OC40ODUgWiBNIDE4MC4zMDMgMTguNzEyIEwgMTkxLjU5MSAxOC43MTIgTCAxOTEuNTkxIDI3LjM0OSBMIDE4MC4zMDMgMjcuMzQ5IEwgMTgwLjMwMyAxOC43MTIgWiBNIDE5MS41OTEgMzEuNjY2IEwgMTgwLjMwMyAzMS42NjYgTCAxODAuMzAzIDY2LjEzNiBMIDE5MS41OTEgNjYuMTM2IEwgMTkxLjU5MSAzMS42NjYgWiBNIDE5NC42OTcgMTguNzEyIEwgMjA1Ljk4NSAxOC43MTIgTCAyMDUuOTg1IDY2LjEzNyBMIDE5NC42OTcgNjYuMTM3IEwgMTk0LjY5NyAxOC43MTIgWiBNIDIyNS41MyAzMC42ODIgQyAyMTUuMzc5IDMwLjY4MiAyMDYuOTcgMzguMjU4IDIwNi45NyA0OC43ODggQyAyMDYuOTcgNjAuMzAzIDIxNC42MjEgNjcuMTIxIDIyNS44MzMgNjcuMTIxIEMgMjMzLjU2MSA2Ny4xMjEgMjQwLjkwOSA2My42MzcgMjQyLjk1NCA1NS42ODIgTCAyMzIuNDI0IDU1LjY4MiBDIDIzMS4zNjQgNTguMTgyIDIyOC43MTIgNTkuNDcgMjI1Ljk4NSA1OS40NyBDIDIyMS4yMTIgNTkuNDcgMjE4LjQ4NSA1Ni4zNjQgMjE4LjI1OCA1MS42NjcgTCAyNDMuNTYxIDUxLjY2NyBDIDI0NC4wOTEgMzkuOTI0IDIzNy44NzkgMzAuNjgyIDIyNS41MyAzMC42ODIgWiBNIDIxOC4yNTggNDUuMzAzIEMgMjE4LjkzOSA0MS4xMzcgMjIxLjU5MSAzOC4yNTggMjI1LjUzIDM4LjI1OCBDIDIyOS4zMTggMzguMjU4IDIzMi4wNDUgNDEuNDQgMjMyLjI3MyA0NS4zMDMgTCAyMTguMjU4IDQ1LjMwMyBaIE0gMjQ0LjM5NCA1My43ODggTCAyNTYuNzQyIDUzLjc4OCBMIDI1Ni43NDIgNjYuMTM2IEwgMjQ0LjM5NCA2Ni4xMzYgTCAyNDQuMzk0IDUzLjc4OCBaIE0gMjcxLjIxMiAxOC43MTIgTCAyNTkuOTI0IDE4LjcxMiBMIDI1OS45MjQgMjcuMzQ5IEwgMjcxLjIxMiAyNy4zNDkgTCAyNzEuMjEyIDE4LjcxMiBaIE0gMjk0LjYyMSAzMS42NjcgTCAyOTQuNjIxIDM4LjkzOSBMIDI4Ny42NTEgMzguOTM5IEwgMjg3LjY1MSA1NC41NDUgQyAyODcuNjUxIDU3LjI3MyAyODkuMjQyIDU3Ljg3OSAyOTEuNjY2IDU3Ljg3OSBDIDI5Mi4xNTkgNTcuODc5IDI5Mi42NTEgNTcuODQxIDI5My4xNDQgNTcuODAzIEMgMjkzLjYzNiA1Ny43NjUgMjk0LjEyOCA1Ny43MjcgMjk0LjYyMSA1Ny43MjcgTCAyOTQuNjIxIDY2LjA2MSBDIDI5My43NTYgNjYuMDkyIDI5Mi45MDMgNjYuMTQ4IDI5Mi4wNTkgNjYuMjAzIEMgMjkwLjgzMSA2Ni4yODQgMjg5LjYyIDY2LjM2NCAyODguNDA5IDY2LjM2NCBDIDI3OC43ODggNjYuMzY0IDI3Ni4zNjMgNjMuNjM2IDI3Ni4zNjMgNTQuMjQyIEwgMjc2LjM2MyAzOC45MzkgTCAyNzEuMjEyIDM4LjkzOSBMIDI3MS4yMTIgNjYuMTM2IEwgMjU5LjkyNCA2Ni4xMzYgTCAyNTkuOTI0IDMxLjY2NyBMIDI3MC42ODIgMzEuNjY3IEwgMjcxLjIxMiAzMS42NjcgTCAyNzYuMzYzIDMxLjY2NyBMIDI3Ni4zNjMgMjEuMjEyIEwgMjg3LjY1MSAyMS4yMTIgTCAyODcuNjUxIDMxLjY2NyBMIDI5NC42MjEgMzEuNjY3IFogTSAyNC4wOTEgNDEuMDIzIEMgMjQuNjQgMzkuODQ4IDI1LjE5IDM4LjY3NCAyNS43NTggMzcuNSBDIDM0LjkyNSA0MS44MTggNDUuNTMxIDQxLjgxOCA1NC42MjEgMzcuNSBDIDU1LjE5IDM4LjY3NCA1NS43MzkgMzkuODQ4IDU2LjI4OCA0MS4wMjMgQyA1Ni44MzcgNDIuMTk3IDU3LjM4NyA0My4zNzEgNTcuOTU1IDQ0LjU0NSBDIDQ2LjY2NyA0OS44NDggMzMuNjM3IDQ5Ljg0OCAyMi40MjUgNDQuNTQ1IEMgMjIuOTkzIDQzLjM3MSAyMy41NDIgNDIuMTk3IDI0LjA5MSA0MS4wMjMgWiIgZmlsbD0iY3VycmVudENvbG9yIiBmaWxsLXJ1bGU9ImV2ZW5vZGQiPjwvcGF0aD4KPC9zdmc+" alt="Facile.it"/>', unsafe_allow_html=True)
    st.markdown('<div class="sb-brand">Legal Radar</div>', unsafe_allow_html=True)
    # Utente sobrio + contatore come badge
    ruolo_corrente = st.session_state.user.get('role', 'user')
    ruolo_label = "Admin" if ruolo_corrente == "admin" else "Utente"
    e_username = html.escape(str(st.session_state.user['username']))
    non_letti = db.conta_non_letti(st.session_state.user['id'])
    tot_non_letti = sum(non_letti.values())
    badge_unread = f'<span class="sb-unread">{tot_non_letti} da leggere</span>' if tot_non_letti else ''
    st.markdown(
        f'<div class="sb-user"><span class="sb-role">{ruolo_label}</span> {e_username}</div>{badge_unread}',
        unsafe_allow_html=True
    )

    opzioni_nav = [
        "🏠 Dashboard",
        "🔎 Cerca",
        "📨 Report Radar",
        "📖 Leggi",
        "🏛️ Provvedimenti",
        "⚖️ Sentenze",
        "📰 News",
        "🔖 I Miei Salvati",
        "⚙️ Gestione Fonti"
    ]
    # Navigazione programmatica (es. dal bottone "Apri nei Report Radar" in dashboard):
    # uso una variabile-ponte 'vai_a' e il parametro index, senza scrivere la key del widget.
    indice_iniziale = 0
    if st.session_state.get('vai_a') in opzioni_nav:
        indice_iniziale = opzioni_nav.index(st.session_state['vai_a'])
        del st.session_state['vai_a']  # consumo la richiesta una sola volta
        # Rimuovo lo stato del widget così il parametro index viene rispettato
        st.session_state.pop('nav_pagina', None)
    pagina = st.radio("Navigazione", opzioni_nav, index=indice_iniziale,
                      key="nav_pagina", label_visibility="collapsed")
    
    st.divider()
    if st.button("Sincronizza archivio", type="primary", use_container_width=True):
        with st.spinner("Scansione fonti in corso…"):
            sincronizza_radar_in_database()
            st.rerun()
            
    if st.button("Esci", use_container_width=True):
        st.session_state.user = None
        st.rerun()

    st.markdown(
        '<div style="margin-top:18px; font-size:11.5px;">'
        '<a href="?policy=1" target="_self" style="color:var(--ink-faint); text-decoration:none;">Privacy Policy</a>'
        '</div>',
        unsafe_allow_html=True
    )

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
            link = art['link']
            # Bottoni azione: pillole compatte e ravvicinate (l'ultima colonna vuota le tiene strette a sinistra)
            b1, b2, b3, b4, _sp = st.columns([1, 1, 1, 1, 1])
            with b1:
                if tipo_bacheca == "bookmarks":
                    if st.button("Rimuovi", key=f"rem_{art['id']}", use_container_width=True):
                        db.rimuovi_bookmark(st.session_state.user['id'], art['id'])
                        st.rerun()
                else:
                    if db.check_bookmark_esiste(st.session_state.user['id'], art['id']):
                        st.button("Salvato ✓", key=f"saved_{art['id']}", use_container_width=True, disabled=True)
                    else:
                        if st.button("Salva", key=f"save_{art['id']}", use_container_width=True):
                            db.aggiungi_bookmark(st.session_state.user['id'], art['id'])
                            st.rerun()
            with b2:
                if not e_letto:
                    if st.button("Segna letto", key=f"read_{art['id']}", use_container_width=True):
                        db.segna_letto(st.session_state.user['id'], art['id'])
                        st.rerun()
                else:
                    st.button("Letto ✓", key=f"readd_{art['id']}", use_container_width=True, disabled=True)
            with b3:
                # Mostro il bottone Analisi solo se non già generata
                if link not in st.session_state.ai_summaries:
                    if st.button("✦ Analisi AI", key=f"ai_{art['id']}", use_container_width=True):
                        with st.spinner("Analisi in corso…"):
                            st.session_state.ai_summaries[link] = genera_sintesi_groq(link, art['preview'])
                            if not art.get('riassunto_ai') and art['id'] not in st.session_state.micro_riassunti:
                                meta = genera_microriassunto_groq(art.get('titolo',''), art.get('preview',''))
                                if meta['riassunto']:
                                    st.session_state.micro_riassunti[art['id']] = meta['riassunto']
                                    db.aggiorna_riassunto_articolo(art['id'], meta['riassunto'], meta['rilevanza'])
                            db.segna_letto(st.session_state.user['id'], art['id'])
                        # niente st.rerun(): mostro il risultato qui sotto, nello stesso ciclo
            with b4:
                # PDF del singolo articolo: lo preparo al clic (genero l'analisi AI
                # se manca), poi mostro il download.
                stato_pdf = st.session_state.setdefault('pdf_singolo_pronti', {})
                if art['id'] not in stato_pdf:
                    if st.button("⬇ Prepara PDF", key=f"preppdf_{art['id']}", use_container_width=True):
                        with st.spinner("Preparo il PDF…"):
                            # Analisi: uso quella in sessione, o la genero ora se manca
                            analisi = st.session_state.ai_summaries.get(link)
                            if not analisi:
                                analisi = genera_sintesi_groq(link, art.get('preview', ''))
                                st.session_state.ai_summaries[link] = analisi
                            stato_pdf[art['id']] = pdf_export.pdf_singolo(art, analisi_extra=analisi)
                        st.rerun()
                else:
                    st.download_button("⬇ Scarica PDF", data=stato_pdf[art['id']],
                                       file_name=f"articolo_{art['id']}.pdf", mime="application/pdf",
                                       key=f"dlpdf_{art['id']}", use_container_width=True)
            # Se l'analisi esiste (appena generata o già presente), la mostro sotto la card
            if link in st.session_state.ai_summaries:
                st.markdown("<div style='font-size:13px; font-weight:600; color:var(--accent); margin-top:6px;'>✦ Analisi strategica</div>", unsafe_allow_html=True)
                st.markdown(st.session_state.ai_summaries[link])

            # Banner di rimando ai report del Radar collegati a questa notizia
            if tipo_bacheca != "bookmarks":
                collegati = db.report_collegati_a_articolo(art['id'])
                for rc in collegati:
                    dt = rc['data_report'].strftime('%d/%m/%Y') if rc.get('data_report') else ''
                    st.markdown(
                        f'<div class="report-banner">📨 C\'è un report del Legal Radar su questo tema: '
                        f'<b>{html.escape(str(rc["titolo"]))}</b>{(" · " + dt) if dt else ""}</div>',
                        unsafe_allow_html=True
                    )
                # Controllo di collegamento riservato agli admin
                if st.session_state.user.get('role') == 'admin':
                    with st.expander("🔗 Collega a un report Legal Radar"):
                        opzioni_rep = db.lista_report_per_collegamento()
                        if not opzioni_rep:
                            st.caption("Nessun report disponibile da collegare.")
                        else:
                            id_collegati = {rc['id'] for rc in collegati}
                            mappa = {f"{r['titolo'][:70]} ({r['data_report'].strftime('%d/%m/%Y') if r.get('data_report') else '—'})": r['id'] for r in opzioni_rep}
                            scelta = st.selectbox("Report", list(mappa.keys()), key=f"sel_rep_{art['id']}", label_visibility="collapsed")
                            rid = mappa[scelta]
                            cc1, cc2 = st.columns(2)
                            if rid not in id_collegati:
                                if cc1.button("Collega", key=f"link_{art['id']}_{rid}"):
                                    db.collega_articolo_report(art['id'], rid)
                                    st.rerun()
                            else:
                                if cc1.button("Scollega", key=f"unlink_{art['id']}_{rid}"):
                                    db.scollega_articolo_report(art['id'], rid)
                                    st.rerun()
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

def _rischio_badge(livello: Optional[str]) -> str:
    """Badge colorato per il livello di rischio del report."""
    l = (livello or "").upper()
    if l == "ALTO":
        return '<span class="rk rk-alto">Rischio alto</span>'
    if l == "MEDIO":
        return '<span class="rk rk-medio">Rischio medio</span>'
    if l == "BASSO":
        return '<span class="rk rk-basso">Rischio basso</span>'
    return ''

def mostra_report_radar(report: List[Dict]) -> None:
    """Renderizza i report ricchi del Radar in stile 'report' (non elenco-link)."""
    if not report:
        st.info("Nessun report del Legal Radar ancora ricevuto. I report mattutini compariranno qui dopo l'invio dal Radar.")
        return
    for r in report:
        tag = (r.get('tag') or '').upper()
        tag_badge = ''
        if tag == 'CERTO':
            tag_badge = '<span class="rk rk-certo">Certo</span>'
        elif tag == 'SEGNALE':
            tag_badge = '<span class="rk rk-segnale">Segnale</span>'
        data_str = r['data_report'].strftime('%d/%m/%Y') if r.get('data_report') else ''
        e_area = html.escape(str(r.get('area') or ''))
        e_tit = html.escape(str(r.get('titolo') or ''))
        score = html.escape(str(r.get('score_rischio') or ''))
        rk = _rischio_badge(r.get('livello_rischio'))
        score_txt = f'<span class="meta-tag tag-rango">{score}</span>' if score else ''
        data_badge = f'<span class="meta-tag tag-rango">{data_str}</span>' if data_str else ''
        badge_nuovo = '' if r.get('letto') else '<span class="badge-nuovo">Nuovo</span>'
        intestazione = (
            f'<div class="report-card">'
            f'<div style="margin-bottom:10px;">{tag_badge}{rk}{score_txt}'
            f'<span class="meta-tag tag-area">{e_area}</span>{data_badge}</div>'
            f'<div class="report-title">{e_tit}{badge_nuovo}</div>'
        )
        st.markdown(intestazione, unsafe_allow_html=True)

        # Sezioni ricche: ognuna mostrata solo se valorizzata
        def blocco(label, valore, righe=False):
            if not valore:
                return
            v = html.escape(str(valore))
            if righe:
                v = "<br>".join(f"• {l}" for l in str(valore).split("\n") if l.strip())
                v = v  # già escapato a monte? no: escapo i singoli
                v = "<br>".join(f"• {html.escape(l)}" for l in str(valore).split("\n") if l.strip())
            else:
                v = v.replace("\n\n", "<br><br>").replace("\n", "<br>")
            st.markdown(
                f'<div class="report-sec"><div class="report-sec-h">{label}</div>'
                f'<div class="report-sec-b">{v}</div></div>',
                unsafe_allow_html=True
            )

        if r.get('fatto_nuovo'):
            blocco("Fatto nuovo", r.get('fatto_nuovo'))
        if r.get('sintesi'):
            blocco("Sintesi", r.get('sintesi'))
        if r.get('analisi'):
            blocco("Analisi giuridica", r.get('analisi'))
        if r.get('impatto'):
            blocco("Impatto di settore", r.get('impatto'))
        if r.get('action_point'):
            blocco("Action point", r.get('action_point'), righe=True)
        if r.get('scadenza'):
            blocco("Scadenza", r.get('scadenza'))
        if r.get('riferimenti'):
            blocco("Riferimenti normativi", r.get('riferimenti'))

        # Fonti e documento ufficiale come link
        link_doc = r.get('link_documento')
        if link_doc and str(link_doc).upper() != "N/D" and str(link_doc).startswith("http"):
            ld = html.escape(str(link_doc), quote=True)
            fonte_uff = html.escape(str(r.get('fonte_ufficiale') or 'Documento ufficiale'))
            st.markdown(f'<div class="report-sec"><a href="{ld}" target="_blank" class="report-link">↗ {fonte_uff}</a></div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

        rid = r['id']
        # Bottoni azione: Salva / Segna letto, come per gli articoli (pillole compatte)
        b1, b2, _sp = st.columns([1, 1, 3])
        with b1:
            if st.session_state.get('report_bacheca') == "salvati":
                if st.button("Rimuovi", key=f"rem_rep_{rid}", use_container_width=True):
                    db.rimuovi_report_bookmark(st.session_state.user['id'], rid)
                    st.rerun()
            elif r.get('salvato'):
                st.button("Salvato ✓", key=f"saved_rep_{rid}", use_container_width=True, disabled=True)
            else:
                if st.button("Salva", key=f"save_rep_{rid}", use_container_width=True):
                    db.aggiungi_report_bookmark(st.session_state.user['id'], rid)
                    st.rerun()
        with b2:
            if not r.get('letto'):
                if st.button("Segna letto", key=f"read_rep_{rid}", use_container_width=True):
                    db.segna_report_letto(st.session_state.user['id'], rid)
                    st.rerun()
            else:
                st.button("Letto ✓", key=f"readd_rep_{rid}", use_container_width=True, disabled=True)

        # Eliminazione riservata agli admin, con conferma esplicita
        if st.session_state.user.get('role') == 'admin':
            with st.expander("🗑️ Elimina report"):
                conferma = st.checkbox("Confermo l'eliminazione di questo report", key=f"confdel_rep_{rid}")
                if st.button("Elimina definitivamente", key=f"del_rep_{rid}", disabled=not conferma):
                    db.elimina_report_radar(rid)
                    st.rerun()
        st.write("")

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
if pagina_pulita not in ["⚙️ Gestione Fonti", "🏠 Dashboard", "🔎 Cerca"]:
    ricerca = st.text_input("🔍 Cerca parole chiave nell'archivio storico...")

# --- ROUTING PAGINE ---
if pagina_pulita == "🔎 Cerca":
    st.header("Ricerca")
    st.caption("Cerca in tutto l'archivio: articoli e report del Radar insieme.")
    q = st.text_input("Cosa cerchi?", placeholder="es. telemarketing, sanzione, AI Act…", key="ricerca_globale_input")
    if not q or not q.strip():
        st.info("Digita una o più parole per cercare tra articoli e report.")
    else:
        risultati = db.ricerca_globale(st.session_state.user['id'], q)
        n_art = len(risultati["articoli"])
        n_rep = len(risultati["report"])
        if n_art == 0 and n_rep == 0:
            st.warning(f"Nessun risultato per «{q}».")
        else:
            st.caption(f"{n_art} articoli · {n_rep} report trovati")
            if n_rep:
                st.subheader("Report del Radar")
                st.session_state['report_bacheca'] = "sezione"
                mostra_report_radar(risultati["report"])
            if n_art:
                st.subheader("Articoli")
                mostra_hub_legale(risultati["articoli"], tipo_bacheca="radar")

elif pagina_pulita == "🏠 Dashboard":
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

        # --- FASCIA: ULTIMI REPORT DEL RADAR ---
        ultimi_report = db.estrai_ultimi_report_dashboard(limite=3)
        if ultimi_report:
            st.markdown('<div class="pp-section"><h2>Ultimi report del Radar</h2></div>', unsafe_allow_html=True)
            cols = st.columns(len(ultimi_report))
            for col, rep in zip(cols, ultimi_report):
                with col:
                    rk = _rischio_badge(rep.get('livello_rischio'))
                    e_tit = html.escape(str(rep.get('titolo') or ''))
                    e_sint = html.escape(str(rep.get('sintesi') or ''))
                    e_area = html.escape(str(rep.get('area') or ''))
                    data_str = rep['data_report'].strftime('%d/%m') if rep.get('data_report') else ''
                    foot = f"{e_area}{(' · ' + data_str) if data_str else ''}"
                    st.markdown(
                        f'<div class="pp-card">'
                        f'<div>{rk}</div>'
                        f'<div class="pp-card-title" style="cursor:default;">{e_tit}</div>'
                        f'<div class="pp-card-sum">{e_sint}</div>'
                        f'<div class="pp-foot">{foot}</div>'
                        f'</div>',
                        unsafe_allow_html=True
                    )
                    if st.button("Apri nei Report Radar →", key=f"open_rep_{rep['id']}", use_container_width=True):
                        st.session_state['vai_a'] = "📨 Report Radar"
                        st.rerun()

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

elif pagina_pulita == "📨 Report Radar":
    col_h, col_btn = st.columns([3, 1])
    col_h.header("Report del Legal Radar")
    with col_btn:
        st.write("")
        if st.button("✓ Segna tutto come letto", key="readall_report", use_container_width=True):
            db.segna_tutti_report_letti(st.session_state.user['id'])
            st.rerun()
    st.caption("I report mattutini ricevuti dal motore di intelligence del Radar, nel loro formato completo.")

    # Filtri: rischio e tag (in due colonne)
    cf1, cf2 = st.columns(2)
    scelta_rischio = cf1.selectbox("Livello di rischio", ["Tutti", "Alto", "Medio", "Basso"], key="rep_rischio")
    scelta_tag = cf2.selectbox("Tipo", ["Tutti", "Certo", "Segnale"], key="rep_tag")
    filtro_rischio = "" if scelta_rischio == "Tutti" else scelta_rischio
    filtro_tag = "" if scelta_tag == "Tutti" else scelta_tag

    st.session_state['report_bacheca'] = "sezione"

    # --- PAGINAZIONE "CARICA ALTRI" (stessa tecnica delle altre sezioni) ---
    PAGINA_DIM = 25
    if 'pag_report' not in st.session_state:
        st.session_state.pag_report = {}
    ctx_key = f"{filtro_rischio}|{filtro_tag}|{ricerca or ''}"
    stato_pag = st.session_state.pag_report
    if stato_pag.get('ctx') != ctx_key:
        stato_pag['ctx'] = ctx_key
        stato_pag['n'] = PAGINA_DIM
    limite_corrente = stato_pag['n']

    dati_db = db.estrai_report_radar(
        st.session_state.user['id'], ricerca_testo=ricerca,
        filtro_rischio=filtro_rischio, filtro_tag=filtro_tag, limite=limite_corrente
    )
    ci_sono_altri = len(dati_db) > limite_corrente
    mostra_report_radar(dati_db[:limite_corrente])

    if ci_sono_altri:
        if st.button(f"⬇️ Carica altri {PAGINA_DIM}", key="more_report", use_container_width=True):
            stato_pag['n'] += PAGINA_DIM
            st.rerun()
    elif limite_corrente > PAGINA_DIM:
        st.caption("Hai raggiunto la fine dei report per questi filtri.")

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

    # Filtri per tema, fonte e periodo, affiancati
    col_t, col_f, col_d = st.columns(3)
    temi_disponibili = cache_lista_temi(tipo_atto)
    tema_sel = None
    with col_t:
        if temi_disponibili:
            scelta_tema = st.selectbox("Filtra per tema", ["Tutti i temi"] + temi_disponibili, key=f"tema_{tipo_atto}")
            if scelta_tema != "Tutti i temi":
                tema_sel = scelta_tema
    fonti_disponibili = cache_lista_fonti_per_tipo(tipo_atto, st.session_state.user['id'])
    fonti_sel = None
    with col_f:
        if fonti_disponibili:
            scelta_fonti = st.multiselect("Filtra per fonte", fonti_disponibili, key=f"fonti_{tipo_atto}",
                                          placeholder="Tutte le fonti")
            if scelta_fonti:
                fonti_sel = scelta_fonti
    with col_d:
        opzioni_periodo = {"Sempre": None, "Ultimi 7 giorni": 7, "Ultimi 30 giorni": 30, "Ultimi 90 giorni": 90}
        scelta_periodo = st.selectbox("Periodo", list(opzioni_periodo.keys()), key=f"periodo_{tipo_atto}")
        giorni_sel = opzioni_periodo[scelta_periodo]

    # --- PAGINAZIONE "CARICA ALTRI" ---
    PAGINA_DIM = 25
    if 'paginazione' not in st.session_state:
        st.session_state.paginazione = {}
    # La chiave di contesto include sezione + filtri: se cambiano, il contatore riparte
    ctx_key = f"{tipo_atto}|{tema_sel or ''}|{','.join(fonti_sel) if fonti_sel else ''}|{giorni_sel or ''}|{ricerca or ''}"
    stato_pag = st.session_state.paginazione
    if stato_pag.get('ctx') != ctx_key:
        stato_pag['ctx'] = ctx_key
        stato_pag['n'] = PAGINA_DIM
    limite_corrente = stato_pag['n']

    # Chiedo un articolo in più del necessario: se arriva, esiste un'altra pagina
    dati_db = db.estrai_per_tipo_atto(
        tipo_atto, st.session_state.user['id'],
        ricerca_testo=ricerca, tema=tema_sel, fonti=fonti_sel, giorni=giorni_sel, limite=limite_corrente + 1
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
    st.header("I Miei Salvati")
    dati_salvati = db.estrai_bookmarks(user_id=st.session_state.user['id'], ricerca_testo=ricerca)

    # Export della rassegna in PDF: preparazione progressiva (genera l'analisi AI
    # mancante per ogni articolo), poi download.
    if dati_salvati:
        with st.expander("⬇ Esporta rassegna in PDF"):
            if 'rassegna_pdf_pronta' not in st.session_state:
                if st.button("Prepara rassegna PDF", key="prep_rassegna"):
                    analisi_map = {}
                    barra = st.progress(0.0, text="Preparazione in corso…")
                    tot = len(dati_salvati)
                    for i, art in enumerate(dati_salvati):
                        link_a = art.get('link', '')
                        # Analisi: quella in sessione, o salvata, o generata ora
                        analisi = st.session_state.ai_summaries.get(link_a)
                        if not analisi and not art.get('riassunto_ai'):
                            analisi = genera_sintesi_groq(link_a, art.get('preview', ''))
                        if analisi:
                            analisi_map[art['id']] = analisi
                        barra.progress((i + 1) / tot, text=f"Elaborato {i+1} di {tot}…")
                    st.session_state.rassegna_pdf_pronta = pdf_export.pdf_rassegna(
                        dati_salvati,
                        titolo_rassegna=f"Rassegna salvati - {st.session_state.user['username']}",
                        analisi_per_id=analisi_map)
                    barra.empty()
                    st.rerun()
            else:
                st.download_button("⬇ Scarica rassegna PDF",
                                   data=st.session_state.rassegna_pdf_pronta,
                                   file_name="rassegna_salvati.pdf", mime="application/pdf",
                                   use_container_width=True)
                if st.button("Rigenera", key="rigen_rassegna"):
                    del st.session_state.rassegna_pdf_pronta
                    st.rerun()

    st.session_state['report_bacheca'] = "sezione"
    if dati_salvati:
        st.subheader("Articoli")
    mostra_hub_legale(dati_salvati, tipo_bacheca="bookmarks")

    # Report del Radar salvati
    report_salvati = db.estrai_report_radar(
        st.session_state.user['id'], ricerca_testo=ricerca, solo_salvati=True, limite=100
    )
    if report_salvati:
        st.subheader("Report del Radar")
        st.session_state['report_bacheca'] = "salvati"
        mostra_report_radar(report_salvati[:100])
    elif not dati_salvati:
        st.info("Non hai ancora salvato nulla. Usa il pulsante Salva su articoli e report.")

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
            svuota_cache_fonti()
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
                    svuota_cache_fonti()
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
                svuota_cache_fonti()
                st.rerun()
            if col_b.button("Elimina", key=f"del_src_{f['id']}"):
                db.rimuovi_fonte(f['id'])
                svuota_cache_fonti()
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

            # Reset password (admin): assegna una nuova password, non mostra mai quella vecchia
            with st.expander(f"🔑 Reimposta password di {u['username']}"):
                st.caption("Le password sono cifrate e non sono visibili a nessuno, nemmeno agli admin. "
                           "Qui puoi assegnarne una nuova (almeno 8 caratteri) e comunicarla all'utente, "
                           "che potrà poi cambiarla. Min. 8 caratteri.")
                np1 = st.text_input("Nuova password", type="password", key=f"np1_{u['id']}")
                np2 = st.text_input("Ripeti la nuova password", type="password", key=f"np2_{u['id']}")
                conf_pw = st.checkbox("Confermo la reimpostazione", key=f"confpw_{u['id']}")
                if st.button("Reimposta password", key=f"resetpw_{u['id']}", disabled=not conf_pw):
                    if not np1 or len(np1) < 8:
                        st.warning("La password deve avere almeno 8 caratteri.")
                    elif np1 != np2:
                        st.warning("Le due password non coincidono.")
                    elif db.reimposta_password(u['id'], np1):
                        st.success(f"Password di {u['username']} reimpostata. Comunicagliela: potrà cambiarla in seguito.")
                    else:
                        st.error("Reimpostazione non riuscita.")

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
