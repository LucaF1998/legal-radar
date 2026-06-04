import streamlit as st
import pandas as pd
import feedparser
import time
import requests
import os
import re
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
                tipo_ingestion VARCHAR(50) DEFAULT 'rss'
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
                data_scansione TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

    def salva_articoli_storico(self, articoli_lista: List[Dict]) -> None:
        query = """
            INSERT INTO articles (titolo, link, preview, macro, area, fonte) 
            VALUES (%s, %s, %s, %s, %s, %s) 
            ON CONFLICT (link) DO NOTHING
        """
        params = [
            (art['Titolo'], art['Link'], art['Preview'], art['Macro'], art['Area'], art['Fonte'])
            for art in articoli_lista
        ]
        with self.get_cursor() as cur:
            cur.executemany(query, params)

    # --- MODIFICA: ESTRAZIONE ARCHIVIO FILTRATO SULLE PREFERENZE UTENTE + STATO LETTO ---
    def estrai_archivio(self, filtro_macro: str, user_id: int, ricerca_testo: str = "") -> List[Dict]:
        query = """
            SELECT a.*, COALESCE(uas.letto, FALSE) AS letto
            FROM articles a
            LEFT JOIN user_article_status uas
                ON uas.article_id = a.id AND uas.user_id = %s
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
            SELECT a.*, COALESCE(uas.letto, FALSE) AS letto
            FROM articles a 
            JOIN bookmarks b ON a.id = b.article_id 
            LEFT JOIN user_article_status uas
                ON uas.article_id = a.id AND uas.user_id = %s
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

# --- 3. STILE GRAFICO ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; background-color: #f7f9fc; }
    div.stButton > button[kind="primary"] { background-color: #ff6600; color: white; border: none; font-weight: 600; border-radius: 8px;}
    div.stButton > button[kind="primary"]:hover { background-color: #e65c00; box-shadow: 0 4px 8px rgba(255, 102, 0, 0.3); }
    .radar-card { background: white; border-radius: 12px; padding: 24px; border: 1px solid #eaeaea; margin-bottom: 20px; border-left: 6px solid #ff6600; box-shadow: 0 2px 8px rgba(0,0,0,0.04); }
    .radar-card-letto { background: #fbfbfc; border-radius: 12px; padding: 24px; border: 1px solid #eee; margin-bottom: 20px; border-left: 6px solid #cfd3da; box-shadow: none; opacity: 0.78; }
    .badge-nuovo { display: inline-block; padding: 3px 9px; border-radius: 20px; font-size: 10px; font-weight: 800; text-transform: uppercase; margin-left: 8px; background: #ff6600; color: white; letter-spacing: 0.5px; vertical-align: middle; }
    .card-title { font-size: 19px; font-weight: 700; color: #1a1a1a; text-decoration: none; margin-bottom: 12px; display: block; line-height: 1.3; }
    .card-title:hover { color: #ff6600; }
    .card-preview { font-size: 14px; color: #4a4a4a; margin-bottom: 15px; line-height: 1.6; }
    .card-summary { font-size: 14px; color: #222; line-height: 1.6; background: #fff5eb; border: 1px solid #ffd6b3; padding: 18px; border-radius: 8px; margin-top: 15px; box-shadow: inset 0 1px 3px rgba(0,0,0,0.02); }
    .meta-tag { display: inline-block; padding: 4px 10px; border-radius: 20px; font-size: 11px; font-weight: 700; text-transform: uppercase; margin-right: 8px; margin-bottom: 12px;}
    .tag-area { background: #eef2ff; color: #4338ca; }
    .tag-fonte { background: #fff3eb; color: #ff6600; border: 1px solid #ffd6b3;}
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

def _ingest_rss(f: Dict) -> List[Dict]:
    """Strategia di ingestion per fonti con feed RSS."""
    risultati = []
    feed = feedparser.parse(f['url'])
    for entry in feed.entries[:5]:
        sommario = entry.summary if hasattr(entry, 'summary') else ""
        preview = BeautifulSoup(sommario, "html.parser").get_text()[:250] + "..."
        risultati.append({
            "Titolo": entry.title, "Link": entry.link, "Preview": preview,
            "Macro": f['macro'], "Area": f['area'], "Fonte": f['nome']
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
            articoli_scovati.extend(strategia(f))
        except Exception as e:
            logging.error("Ingestion fallita per %s (%s): %s", f['nome'], tipo, e)
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
                    st.session_state.user = user
                    st.success(f"Accesso eseguito. Benvenuto {user['username']}!")
                    st.rerun()
                else:
                    st.error("Credenziali errate.")
    st.stop()

# --- 6. INTERFACCIA UTENTE AUTENTICATO ---
with st.sidebar:
    st.title("⚖️ Legal Radar")
    st.write(f"👤 Utente: **{st.session_state.user['username']}**")

    # Conteggi non-letti per badge nella navigazione
    non_letti = db.conta_non_letti(st.session_state.user['id'])
    def _lbl(emoji_label: str, macro: str) -> str:
        n = non_letti.get(macro, 0)
        return f"{emoji_label} ({n})" if n else emoji_label

    opzioni_nav = [
        "🏠 Dashboard",
        _lbl("📖 Leggi & Normativa", "Leggi & Normativa"),
        _lbl("🏛️ Provvedimenti & Sentenze", "Provvedimenti & Sentenze"),
        _lbl("📰 News & Aggiornamenti", "News & Aggiornamenti"),
        "🔖 I Miei Salvati",
        "⚙️ Gestione Fonti"
    ]
    pagina = st.radio("Navigazione", opzioni_nav)
    
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
            st.markdown(f"""
            <div class="{classe_card}">
                <div>
                    <span class="meta-tag tag-area">{art['area']}</span>
                    <span class="meta-tag tag-fonte">{art['fonte']}</span>
                </div>
                <a href="{art['link']}" target="_blank" class="card-title">{art['titolo']}{badge_nuovo}</a>
                <div class="card-preview">{art['preview']}</div>
            </div>
            """, unsafe_allow_html=True)
            
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
            if link in st.session_state.ai_summaries:
                # Sfruttiamo il markdown nativo di Streamlit per interpretare l'output strutturato dell'AI
                st.markdown("<div class='card-summary'>🤖 <b>Analisi Strategica Legal-Tech:</b></div>", unsafe_allow_html=True)
                st.markdown(st.session_state.ai_summaries[link])
            else:
                with c3:
                    if st.button("✨ Genera Analisi AI Strategica", key=f"ai_{art['id']}"):
                        with st.spinner("L'AI sta conducendo l'analisi verticale per i comparatori..."):
                            st.session_state.ai_summaries[link] = genera_sintesi_groq(link, art['preview'])
                            # Generare l'analisi implica aver "consumato" l'articolo: marca letto
                            db.segna_letto(st.session_state.user['id'], art['id'])
                            st.rerun()
            st.write("")

# Le label di navigazione possono avere un suffisso conteggio "(3)": lo rimuovo per il routing
pagina_pulita = re.sub(r"\s*\(\d+\)$", "", pagina)

ricerca = ""
if pagina_pulita not in ["⚙️ Gestione Fonti", "🏠 Dashboard"]:
    ricerca = st.text_input("🔍 Cerca parole chiave nell'archivio storico...")

# --- ROUTING PAGINE ---
if pagina_pulita == "🏠 Dashboard":
    st.title(f"Benvenuto nel tuo Hub, {st.session_state.user['username']}! 👋")
    st.caption(f"Stato dell'Intelligence Normativa al {datetime.now().strftime('%d/%m/%Y')}")
    st.write("")
    
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
            st.markdown(f"""
            <div style="background: white; border-radius: 8px; padding: 15px; border: 1px solid #eaeaea; border-left: 4px solid #d32f2f; margin-bottom: 10px;">
                <span style="font-size: 11px; font-weight: bold; color: #d32f2f; text-transform: uppercase;">⚠️ ALERT</span> | 
                <span style="font-size: 12px; color: #666;">{al['fonte']} ({al['area']})</span><br>
                <a href="{al['link']}" target="_blank" style="font-weight: 600; color: #1a1a1a; text-decoration: none; font-size: 15px;">{al['titolo']}</a>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("Nessun alert urgente rilevato dalle tue fonti attive.")

elif pagina_pulita in ["📖 Leggi & Normativa", "🏛️ Provvedimenti & Sentenze", "📰 News & Aggiornamenti"]:
    macro_categoria = pagina_pulita.replace("📖 ", "").replace("🏛️ ", "").replace("📰 ", "")
    col_h, col_btn = st.columns([3, 1])
    col_h.header(macro_categoria)
    with col_btn:
        st.write("")
        if st.button("✓ Segna tutto come letto", key=f"readall_{macro_categoria}", use_container_width=True):
            db.segna_tutti_letti(st.session_state.user['id'], filtro_macro=macro_categoria)
            st.rerun()
    # MODIFICA: Passiamo l'ID utente per nascondere gli articoli delle fonti spente
    dati_db = db.estrai_archivio(filtro_macro=macro_categoria, user_id=st.session_state.user['id'], ricerca_testo=ricerca)
    mostra_hub_legale(dati_db, tipo_bacheca="radar")

elif pagina_pulita == "🔖 I Miei Salvati":
    st.header("I Miei Articoli Salvati")
    dati_salvati = db.estrai_bookmarks(user_id=st.session_state.user['id'], ricerca_testo=ricerca)
    mostra_hub_legale(dati_salvati, tipo_bacheca="bookmarks")

elif pagina_pulita == "⚙️ Gestione Fonti":
    st.header("Database & Personalizzazione Fonti")
    
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
            f"**{f['nome']}** {emoji_tipo} <span style='font-size:11px;color:#888;'>({tipo_f} · {tipo_i})</span><br>"
            f"<span style='font-size:13px;color:#555;'>*{f['area']}* — {f['macro']}</span>",
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
    
    # SEZIONE 3 - ELIMINAZIONE GLOBALE
    st.subheader("🗑️ Database Globale Fonti (Eliminazione per tutti)")
    for f in fonti_personali:
        col_t, col_b = st.columns([5, 1])
        col_t.markdown(f"**{f['nome']}** - <span style='font-size:12px;color:#888;'>{f['url']}</span>", unsafe_allow_html=True)
        if col_b.button("Elimina", key=f"del_src_{f['id']}"):
            db.rimuovi_fonte(f['id'])
            st.success("Fonte rimossa dal sistema.")
            st.rerun()
        st.write("---")
