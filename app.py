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
            "CREATE INDEX IF NOT EXISTS idx_articles_tipoatto ON articles(tipo_atto, data_scansione DESC)",
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
            INSERT INTO articles (titolo, link, preview, macro, area, fonte, riassunto_ai, rilevanza, tipo_atto, tema) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
            ON CONFLICT (link) DO NOTHING
        """
        params = [
            (art['Titolo'], art['Link'], art['Preview'], art['Macro'], art['Area'], art['Fonte'],
             art.get('RiassuntoAI'), art.get('Rilevanza'), art.get('TipoAtto'), art.get('Tema'))
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
        query += " ORDER BY a.data_scansione DESC LIMIT 100"
        with self.get_cursor(dict_cursor=True) as cur:
            cur.execute(query, params)
            articoli = cur.fetchall()
        return [dict(a) for a in articoli]

    def estrai_per_tipo_atto(self, tipo_atto: str, user_id: int, ricerca_testo: str = "",
                             tema: Optional[str] = None) -> List[Dict]:
        """Estrae articoli per categoria AI (sentenza/provvedimento/news), con filtro tema opzionale,
        rispettando le fonti spente dall'utente e portando stato letto + tipo fonte."""
        query = """
            SELECT a.*, COALESCE(uas.letto, FALSE) AS letto, src.tipo_fonte
            FROM articles a
            LEFT JOIN user_article_status uas
                ON uas.article_id = a.id AND uas.user_id = %s
            LEFT JOIN sources src ON src.nome = a.fonte
            WHERE a.tipo_atto = %s
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
        query += " ORDER BY a.data_scansione DESC LIMIT 100"
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
        query += " ORDER BY a.data_scansione DESC"
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

    # --- MODIFICA: ANCHE GLI ALERT DELLA HOME ESCLUDONO LE FONTI SPENTE ---
    def estrai_ultimi_alert_urgenti(self, user_id: int) -> List[Dict]:
        keywords = ['%sanzion%', '%ordinanza%', '%condanna%', '%violazion%', '%scadenza%', '%obbligo%', '%divieto%', '%sentenza%']

        query = """
            SELECT * FROM articles 
            WHERE ({}) 
            AND fonte NOT IN (
                SELECT s.nome FROM sources s
                JOIN user_source_preferences usp ON s.id = usp.source_id
                WHERE usp.user_id = %s AND usp.is_active = FALSE
            )
            ORDER BY data_scansione DESC LIMIT 4
        """.format(" OR ".join(["titolo ILIKE %s" for _ in keywords]))

        params = keywords + [user_id]
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
    {"nome": "Agenzia Entrate", "url": "https://www.agenziaentrate.gov.it/portale/web/guest/rss/novita", "area": "Diritto Tributario", "macro": "Leggi & Normativa", "tipo_fonte": "Ufficiale", "tipo_ingestion": "rss"},
    {"nome": "Garante Privacy", "url": "https://www.garanteprivacy.it/o/gpdp-rss/rss?c=10490", "area": "Privacy", "macro": "Provvedimenti & Sentenze", "tipo_fonte": "Autorità", "tipo_ingestion": "rss"},
    {"nome": "EDPB Europa", "url": "https://edpb.europa.eu/rss.xml", "area": "Privacy", "macro": "Provvedimenti & Sentenze", "tipo_fonte": "Autorità", "tipo_ingestion": "rss"},
    {"nome": "Banca d'Italia", "url": "https://www.bancaditalia.it/rss/media.xml", "area": "Diritto Bancario", "macro": "Provvedimenti & Sentenze", "tipo_fonte": "Autorità", "tipo_ingestion": "rss"},
    {"nome": "Consob", "url": "https://www.consob.it/web/area-pubblica/rss", "area": "Diritto Bancario", "macro": "Provvedimenti & Sentenze", "tipo_fonte": "Autorità", "tipo_ingestion": "rss"},
    {"nome": "IVASS", "url": "https://www.ivass.it/util/index.rss.html?lingua=it", "area": "Diritto assicurativo", "macro": "Leggi & Normativa", "tipo_fonte": "Autorità", "tipo_ingestion": "rss"},
    {"nome": "CGUE", "url": "https://curia.europa.eu/site/rss.jsp?lang=it&secondLang=en", "area": "Giurisprudenza UE", "macro": "Provvedimenti & Sentenze", "tipo_fonte": "Ufficiale", "tipo_ingestion": "rss"},
    {"nome": "Altalex", "url": "https://www.altalex.com/rss", "area": "Legale Generale", "macro": "News & Aggiornamenti", "tipo_fonte": "Editoriale", "tipo_ingestion": "rss"}
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
    @import url('https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,400;9..144,500;9..144,600&family=Inter:wght@400;500;600;700&display=swap');

    :root{
        --ink:#211a14; --ink-soft:#5c5147; --ink-faint:#9a8f82;
        --paper:#f6f1ea; --surface:#fffdf9; --line:#e7ddd0;
        --brand:#c2410c; --brand-deep:#9a330a; --brand-soft:#fbe9d9; --brand-tint:#fdf2ea;
        --gold-soft:#f5ead6; --gold-ink:#7a5414;
        --danger:#b3261e; --danger-soft:#fbe7e4;
    }

    /* sfondo app e tipografia base */
    .stApp { background-color: var(--paper); }
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; color: var(--ink); }

    /* titoli nativi (st.title/header/subheader) in serif */
    h1, h2, h3 { font-family: 'Fraunces', serif !important; letter-spacing: -0.3px; color: var(--ink) !important; }

    /* sidebar */
    section[data-testid="stSidebar"] { background-color: var(--surface); border-right: 1px solid var(--line); }

    /* bottoni nativi */
    div.stButton > button { font-family:'Inter',sans-serif; font-weight:600; border-radius:6px; border:1px solid var(--line); color:var(--ink-soft); background:var(--surface); transition:all .15s; }
    div.stButton > button:hover { border-color:var(--brand); color:var(--brand); }
    div.stButton > button[kind="primary"] { background-color: var(--brand); color: #fff; border: none; }
    div.stButton > button[kind="primary"]:hover { background-color: var(--brand-deep); box-shadow: 0 4px 14px rgba(194,65,12,0.25); color:#fff; }

    /* input e ricerca */
    div[data-testid="stTextInput"] input { border-radius:6px; border:1px solid var(--line); }
    div[data-testid="stTextInput"] input:focus { border-color:var(--brand); box-shadow:0 0 0 2px var(--brand-soft); }

    /* metriche native */
    div[data-testid="stMetric"] { background: var(--surface); border:1px solid var(--line); border-top:3px solid var(--brand); border-radius:6px; padding:16px 18px; }
    div[data-testid="stMetricValue"] { font-family:'Fraunces',serif; color:var(--brand); }
    div[data-testid="stMetricLabel"] { color:var(--ink-faint); text-transform:uppercase; letter-spacing:0.5px; font-size:12px; }

    /* ---- CARD ARTICOLO ---- */
    .radar-card { background: var(--surface); border-radius: 8px; padding: 22px 24px; border: 1px solid var(--line); border-left: 4px solid var(--brand); margin-bottom: 18px; transition: transform .18s, box-shadow .18s; }
    .radar-card:hover { transform: translateY(-2px); box-shadow: 0 6px 20px rgba(194,65,12,0.08); }
    .radar-card-letto { background: #faf6f0; border-radius: 8px; padding: 22px 24px; border: 1px solid var(--line); border-left: 4px solid var(--line); margin-bottom: 18px; opacity: 0.72; }
    .badge-nuovo { display: inline-block; padding: 3px 10px; border-radius: 4px; font-size: 10px; font-weight: 700; text-transform: uppercase; margin-left: 8px; background: var(--brand); color: #fff; letter-spacing: 1px; vertical-align: middle; }

    .card-title { font-family:'Fraunces',serif; font-size: 20px; font-weight: 600; color: var(--ink); text-decoration: none; margin-bottom: 10px; display: block; line-height: 1.3; }
    .card-title:hover { color: var(--brand); }
    .card-preview { font-size: 14.5px; color: var(--ink-soft); margin-bottom: 4px; line-height: 1.65; }

    .card-summary { font-size: 14px; color: var(--brand-deep); line-height: 1.5; background: var(--brand-tint); border: 1px solid #f3d5bf; padding: 14px 18px; border-radius: 6px; margin-top: 15px; font-weight:600; }

    /* ---- TAG ---- */
    .meta-tag { display: inline-block; padding: 3px 10px; border-radius: 4px; font-size: 11px; font-weight: 700; text-transform: uppercase; margin-right: 8px; margin-bottom: 12px; letter-spacing:0.4px; }
    .tag-area { background: var(--brand-soft); color: var(--brand-deep); }
    .tag-fonte { background: var(--gold-soft); color: var(--gold-ink); }
    .tag-rango { background: transparent; color: var(--ink-faint); border: 1px solid var(--line); }
    .card-microsummary { font-family:'Inter',sans-serif; font-size:13.5px; color:var(--brand-deep); background:var(--brand-tint); border-left:2px solid var(--brand); padding:8px 12px; border-radius:0 5px 5px 0; margin:0 0 12px; line-height:1.5; }
    .badge-ril { font-size:10px; font-weight:700; letter-spacing:.4px; padding:3px 9px; border-radius:4px; text-transform:uppercase; margin-bottom:12px; display:inline-block; }
    .badge-ril-alta { background:var(--danger-soft); color:var(--danger); }
    .badge-ril-media { background:var(--gold-soft); color:var(--gold-ink); }
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

def genera_microriassunto_groq(titolo: str, preview: str) -> Dict[str, Optional[str]]:
    """Genera micro-riassunto + rilevanza + tipo_atto + tema, via Groq (JSON). Usato all'ingestion.
    Ritorna {'riassunto','rilevanza','tipo_atto','tema'}. Fail-safe: None su errore."""
    vuoto = {"riassunto": None, "rilevanza": None, "tipo_atto": None, "tema": None}
    raw_key = st.secrets.get("GROQ_API_KEY", "").strip()
    if not raw_key.startswith("gsk_"):
        return vuoto

    testo = f"Titolo: {titolo}\n\nAnteprima: {preview}"
    api_url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {raw_key}", "Content-Type": "application/json"}
    system_prompt = (
        "Sei un assistente legale che pre-analizza novità normative, giurisprudenziali e di settore per un team "
        "di compliance specializzato nei comparatori online italiani (finanza, assicurazioni, utility). "
        "Dato titolo e anteprima, rispondi ESCLUSIVAMENTE con un oggetto JSON valido, senza testo prima o dopo:\n"
        '{"riassunto": "<1-2 frasi in italiano, chiare e concrete>", '
        '"rilevanza": "<alta|media>", '
        '"tipo_atto": "<sentenza|provvedimento|news>", '
        '"tema": "<tema giuridico principale>"}\n\n'
        "Regole:\n"
        "- rilevanza: \"alta\" se impatta direttamente i comparatori (sanzioni, telemarketing, consenso, "
        "trasparenza tariffaria, data breach, intermediazione); \"media\" altrimenti.\n"
        "- tipo_atto: \"sentenza\" per pronunce giurisdizionali (Corti, tribunali, CGUE); "
        "\"provvedimento\" per atti di autorità/regolatori (Garante, IVASS, Consob, delibere, linee guida, ordinanze); "
        "\"news\" per articoli giornalistici/editoriali e comunicati divulgativi.\n"
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
        tipo_atto = (dati.get("tipo_atto") or "").strip().lower()
        if tipo_atto not in ("sentenza", "provvedimento", "news"):
            tipo_atto = None
        tema = (dati.get("tema") or "").strip()[:100] or None
        return {"riassunto": riassunto, "rilevanza": rilevanza, "tipo_atto": tipo_atto, "tema": tema}
    except Exception as e:
        logging.error("Microriassunto fallito: %s", e)
        return vuoto

def _ingest_rss(f: Dict) -> List[Dict]:
    """Strategia di ingestion per fonti con feed RSS."""
    risultati = []
    feed = feedparser.parse(f['url'])
    for entry in feed.entries[:5]:
        sommario = entry.summary if hasattr(entry, 'summary') else ""
        preview = BeautifulSoup(sommario, "html.parser").get_text()[:250] + "..."
        # Pre-analisi AI: riassunto + rilevanza + tipo_atto + tema (fail-safe se l'AI non risponde)
        meta = genera_microriassunto_groq(entry.title, preview)
        risultati.append({
            "Titolo": entry.title, "Link": entry.link, "Preview": preview,
            "Macro": f['macro'], "Area": f['area'], "Fonte": f['nome'],
            "RiassuntoAI": meta["riassunto"], "Rilevanza": meta["rilevanza"],
            "TipoAtto": meta["tipo_atto"], "Tema": meta["tema"]
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
            classe_card = "radar-card-letto" if e_letto else "radar-card"
            badge_nuovo = "" if e_letto else '<span class="badge-nuovo">Nuovo</span>'
            # Escape di tutti i testi: evita che contenuti con caratteri HTML (es. < > nel riassunto AI) rompano la card
            e_area = html.escape(str(art.get('area') or ''))
            e_fonte = html.escape(str(art.get('fonte') or ''))
            e_titolo = html.escape(str(art.get('titolo') or ''))
            e_preview = html.escape(str(art.get('preview') or ''))
            e_link = html.escape(str(art.get('link') or ''), quote=True)
            rango = art.get('tipo_fonte')
            tag_rango = f'<span class="meta-tag tag-rango">{html.escape(str(rango))}</span>' if rango else ''
            # Tag tema (dall'AI): se assente, ripiega sull'area manuale
            tema = art.get('tema') or art.get('area')
            tag_tema = f'<span class="meta-tag tag-area">{html.escape(str(tema))}</span>' if tema else ''
            # Badge rilevanza (solo priorità visiva, non nasconde nulla)
            rilevanza = art.get('rilevanza')
            if rilevanza == "alta":
                badge_ril = '<span class="badge-ril badge-ril-alta">● Rilevanza alta</span>'
            elif rilevanza == "media":
                badge_ril = '<span class="badge-ril badge-ril-media">● Rilevanza media</span>'
            else:
                badge_ril = ''
            # Micro-riassunto AI sotto il titolo: dal DB, o da quello appena generato in sessione
            riassunto = art.get('riassunto_ai') or st.session_state.get('micro_riassunti', {}).get(art['id'])
            blocco_riassunto = f'<div class="card-microsummary">✦ {html.escape(str(riassunto))}</div>' if riassunto else ''
            card_html = (
                f'<div class="{classe_card}">'
                f'<div>{tag_tema}'
                f'<span class="meta-tag tag-fonte">{e_fonte}</span>{tag_rango}{badge_ril}</div>'
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
                st.markdown("<div class='card-summary'>✦ Analisi strategica legal-tech</div>", unsafe_allow_html=True)
                st.markdown(st.session_state.ai_summaries[link])
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
if pagina_pulita not in ["⚙️ Gestione Fonti", "🏠 Dashboard"]:
    ricerca = st.text_input("🔍 Cerca parole chiave nell'archivio storico...")

# --- ROUTING PAGINE ---
if pagina_pulita == "🏠 Dashboard":
    oggi = datetime.now().strftime('%d/%m/%Y')
    e_user = html.escape(str(st.session_state.user['username']))
    masthead_html = (
        '<div style="border-bottom:3px solid var(--brand); padding-bottom:14px; margin-bottom:6px; display:flex; align-items:flex-end; justify-content:space-between;">'
        '<div style="font-family:\'Fraunces\',serif; font-weight:600; font-size:34px; letter-spacing:-0.5px; color:var(--ink);">'
        'Legal Radar<span style="color:var(--brand);">.</span></div>'
        '<div style="font-size:12px; text-transform:uppercase; letter-spacing:2px; color:var(--brand); font-weight:700;">Regulatory Intelligence</div>'
        '</div>'
        f'<div style="font-size:13px; color:var(--ink-soft); font-style:italic; margin-bottom:24px;">Rassegna per {e_user} · {oggi}</div>'
    )
    st.markdown(masthead_html, unsafe_allow_html=True)
    
    metriche = db.estrai_metriche_dashboard(st.session_state.user['id'])
    c1, c2, c3 = st.columns(3)
    c1.metric("📚 Archivio Storico Comune", f"{metriche['articoli']} articoli")
    c2.metric("📡 Canali Radar Attivi", f"{metriche['fonti']} fonti")
    c3.metric("🔖 La Tua Rassegna", f"{metriche['salvati']} salvati")
    
    st.divider()
    st.subheader("🔥 Ultimi Alert Urgenti Rilevati (Personalizzati)")
    
    # MODIFICA: Gli alert della Home ora seguono i filtri personali dell'utente
    alert_urgenti = db.estrai_ultimi_alert_urgenti(st.session_state.user['id'])
    if alert_urgenti:
        for al in alert_urgenti:
            a_fonte = html.escape(str(al.get('fonte') or ''))
            a_area = html.escape(str(al.get('area') or ''))
            a_titolo = html.escape(str(al.get('titolo') or ''))
            a_link = html.escape(str(al.get('link') or ''), quote=True)
            alert_html = (
                '<div style="background: white; border-radius: 8px; padding: 15px; border: 1px solid #eaeaea; border-left: 4px solid #d32f2f; margin-bottom: 10px;">'
                '<span style="font-size: 11px; font-weight: bold; color: #d32f2f; text-transform: uppercase;">⚠️ ALERT</span> | '
                f'<span style="font-size: 12px; color: #666;">{a_fonte} ({a_area})</span><br>'
                f'<a href="{a_link}" target="_blank" style="font-weight: 600; color: #1a1a1a; text-decoration: none; font-size: 15px;">{a_titolo}</a>'
                '</div>'
            )
            st.markdown(alert_html, unsafe_allow_html=True)
    else:
        st.info("Nessun alert urgente rilevato dalle tue fonti attive.")

elif pagina_pulita in ["🏛️ Provvedimenti", "⚖️ Sentenze", "📰 News"]:
    # Mappa la voce di menu alla categoria AI
    mappa_tipo = {"🏛️ Provvedimenti": "provvedimento", "⚖️ Sentenze": "sentenza", "📰 News": "news"}
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

    dati_db = db.estrai_per_tipo_atto(tipo_atto, st.session_state.user['id'], ricerca_testo=ricerca, tema=tema_sel)
    mostra_hub_legale(dati_db, tipo_bacheca="radar")

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
        tipo_f = f.get('tipo_fonte', 'Ufficiale')
        tipo_i = f.get('tipo_ingestion', 'rss')
        emoji_tipo = {"Ufficiale": "🏛️", "Autorità": "⚖️", "Editoriale": "📰"}.get(tipo_f, "📄")
        col_info.markdown(
            f"**{html.escape(str(f['nome']))}** {emoji_tipo} <span style='font-size:11px;color:#888;'>({tipo_f} · {tipo_i})</span><br>"
            f"<span style='font-size:13px;color:#555;'>{html.escape(str(f['area']))} — {html.escape(str(f['macro']))}</span>",
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
    st.caption("Le sezioni *Leggi* e *Provvedimenti* sono riservate a fonti Ufficiali e Autorità. "
               "Le fonti Editoriali sono ammesse solo in *News & Aggiornamenti*.")
    with st.form("form_aggiunta_fonte", clear_on_submit=True):
        c1, c2 = st.columns(2)
        n_nome = c1.text_input("Nome Autorità / Sito")
        n_url = c1.text_input("URL Feed RSS / Pagina")
        n_tipo_fonte = c1.selectbox("Tipo di fonte", ["Ufficiale", "Autorità", "Editoriale"])
        n_macro = c2.selectbox("Categoria Macro", ["Leggi & Normativa", "Provvedimenti & Sentenze", "News & Aggiornamenti"])
        n_area = c2.text_input("Materia Giuridica (es. Compliance, Privacy)")
        n_tipo_ingestion = c2.selectbox("Modalità di acquisizione", ["rss", "scraper"],
                                        help="'rss' per feed standard. 'scraper' richiede un parser dedicato (avanzato).")
        if st.form_submit_button("➕ Salva Fonte nel Database Comune"):
            # Regola di coerenza: Editoriale ammessa solo in News
            if n_tipo_fonte == "Editoriale" and n_macro != "News & Aggiornamenti":
                st.error("Le fonti Editoriali sono ammesse solo nella sezione 'News & Aggiornamenti'. "
                         "Per Leggi e Provvedimenti usa fonti Ufficiali o Autorità.")
            elif n_nome and n_url and n_area:
                if db.aggiungi_fonte(n_nome, n_url, n_area, n_macro, n_tipo_fonte, n_tipo_ingestion):
                    st.success(f"Fonte '{n_nome}' registrata nel database globale!")
                    st.rerun()
                else:
                    st.error("Errore. URL probabilmente già registrato.")
            else:
                st.error("Compila tutti i campi.")
                
    st.divider()
    
    # SEZIONE 3 - ELIMINAZIONE GLOBALE (riservata agli admin)
    is_admin = st.session_state.user.get('role', 'user') == 'admin'
    st.subheader("🗑️ Database Globale Fonti")
    if is_admin:
        st.caption("Come admin puoi eliminare le fonti dal catalogo comune. L'azione vale per tutti gli utenti.")
    else:
        st.caption("Elenco delle fonti del catalogo comune. L'eliminazione è riservata agli amministratori.")
    for f in fonti_personali:
        col_t, col_b = st.columns([5, 1])
        col_t.markdown(f"**{html.escape(str(f['nome']))}** - <span style='font-size:12px;color:#888;'>{html.escape(str(f['url']))}</span>", unsafe_allow_html=True)
        if is_admin:
            if col_b.button("Elimina", key=f"del_src_{f['id']}"):
                db.rimuovi_fonte(f['id'])
                st.success("Fonte rimossa dal sistema.")
                st.rerun()
        else:
            col_b.caption("🔒")
        st.write("---")

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
