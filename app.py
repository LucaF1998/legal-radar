import os
import logging
from contextlib import contextmanager
from datetime import datetime
from typing import List, Dict, Optional, Tuple

import requests
from bs4 import BeautifulSoup
import feedparser
import bcrypt
import psycopg2
import psycopg2.errors
import psycopg2.extras
import streamlit as st

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class LegalRadarDB:
    def __init__(self, db_url: str):
        self.db_url = db_url

    # ------------------------------------------------------------------
    # CONNESSIONE: context manager con commit/rollback/close garantiti
    # ------------------------------------------------------------------
    @contextmanager
    def get_cursor(self, dict_cursor: bool = False):
        """Fornisce un cursore con commit/rollback/close automatici e garantiti.

        - commit() solo se il blocco termina senza eccezioni
        - rollback() se qualcosa lancia
        - close() di cursore e connessione SEMPRE (anche su errore)
        """
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

    # ------------------------------------------------------------------
    # SCHEMA: chiamato una sola volta (vedi get_db + cache_resource)
    # ------------------------------------------------------------------
    def init_db(self) -> None:
        commands = (
            """CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, username VARCHAR(150) UNIQUE NOT NULL, password_hash VARCHAR(255) NOT NULL, role VARCHAR(50) DEFAULT 'user')""",
            """CREATE TABLE IF NOT EXISTS sources (id SERIAL PRIMARY KEY, nome VARCHAR(150) NOT NULL, url TEXT UNIQUE NOT NULL, area VARCHAR(150), macro VARCHAR(150))""",
            """CREATE TABLE IF NOT EXISTS articles (id SERIAL PRIMARY KEY, titolo TEXT NOT NULL, link TEXT UNIQUE NOT NULL, preview TEXT, macro VARCHAR(150), area VARCHAR(150), fonte VARCHAR(150), data_scansione TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
            """CREATE TABLE IF NOT EXISTS bookmarks (user_id INTEGER REFERENCES users(id) ON DELETE CASCADE, article_id INTEGER REFERENCES articles(id) ON DELETE CASCADE, PRIMARY KEY (user_id, article_id))""",
            """CREATE TABLE IF NOT EXISTS user_source_preferences (user_id INTEGER REFERENCES users(id) ON DELETE CASCADE, source_id INTEGER REFERENCES sources(id) ON DELETE CASCADE, is_active BOOLEAN DEFAULT TRUE, PRIMARY KEY (user_id, source_id))""",
            # Indici per performance (idempotenti)
            """CREATE INDEX IF NOT EXISTS idx_articles_macro_data ON articles(macro, data_scansione DESC)""",
            """CREATE INDEX IF NOT EXISTS idx_articles_fonte ON articles(fonte)""",
            """CREATE INDEX IF NOT EXISTS idx_bookmarks_user ON bookmarks(user_id)""",
            """CREATE INDEX IF NOT EXISTS idx_usp_user ON user_source_preferences(user_id)""",
        )
        try:
            with self.get_cursor() as cur:
                for command in commands:
                    cur.execute(command)
        except Exception as e:
            logging.error("Inizializzazione database fallita: %s", e)
            st.error(f"Inizializzazione database fallita: {e}")

    # ------------------------------------------------------------------
    # UTENTI
    # ------------------------------------------------------------------
    def registra_utente(self, username: str, password: str) -> bool:
        # bcrypt fuori dal blocco DB: non serve tenere aperta la connessione
        hashed: str = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        try:
            with self.get_cursor() as cur:
                cur.execute(
                    "INSERT INTO users (username, password_hash) VALUES (%s, %s)",
                    (username.strip(), hashed),
                )
            return True
        except psycopg2.errors.UniqueViolation:
            return False  # caso atteso: username gia' esistente
        except Exception as e:
            logging.error("Errore registrazione utente: %s", e)
            raise  # gli errori veri (DB down, ecc.) non vanno nascosti

    def verifica_utente(self, username: str, password: str) -> Optional[Dict]:
        with self.get_cursor(dict_cursor=True) as cur:
            cur.execute("SELECT * FROM users WHERE username = %s", (username.strip(),))
            user = cur.fetchone()
        if user and bcrypt.checkpw(password.encode('utf-8'), user['password_hash'].encode('utf-8')):
            return dict(user)
        return None

    # ------------------------------------------------------------------
    # FONTI
    # ------------------------------------------------------------------
    def carica_fonti_con_preferenze(self, user_id: int) -> List[Dict]:
        query = """SELECT s.*, COALESCE(usp.is_active, TRUE) as utente_attiva FROM sources s LEFT JOIN user_source_preferences usp ON s.id = usp.source_id AND usp.user_id = %s ORDER BY s.nome ASC"""
        with self.get_cursor(dict_cursor=True) as cur:
            cur.execute(query, (user_id,))
            fonti = cur.fetchall()
        return [dict(f) for f in fonti]

    def carica_fonti(self) -> List[Dict]:
        with self.get_cursor(dict_cursor=True) as cur:
            cur.execute("SELECT * FROM sources ORDER BY nome ASC")
            fonti = cur.fetchall()
        return [dict(f) for f in fonti]

    def imposta_preferenza_fonte(self, user_id: int, source_id: int, is_active: bool) -> None:
        query = """INSERT INTO user_source_preferences (user_id, source_id, is_active) VALUES (%s, %s, %s) ON CONFLICT (user_id, source_id) DO UPDATE SET is_active = EXCLUDED.is_active"""
        with self.get_cursor() as cur:
            cur.execute(query, (user_id, source_id, is_active))

    def aggiungi_fonte(self, nome: str, url: str, area: str, macro: str) -> bool:
        try:
            with self.get_cursor() as cur:
                cur.execute(
                    "INSERT INTO sources (nome, url, area, macro) VALUES (%s, %s, %s, %s) ON CONFLICT (url) DO NOTHING",
                    (nome, url, area, macro),
                )
            return True
        except Exception as e:
            logging.error("Errore aggiunta fonte: %s", e)
            return False

    def rimuovi_fonte(self, fonte_id: int) -> None:
        with self.get_cursor() as cur:
            cur.execute("DELETE FROM sources WHERE id = %s", (fonte_id,))

    # ------------------------------------------------------------------
    # ARTICOLI / ARCHIVIO
    # ------------------------------------------------------------------
    def salva_articoli_storico(self, articoli_lista: List[Dict]) -> None:
        query = """INSERT INTO articles (titolo, link, preview, macro, area, fonte) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (link) DO NOTHING"""
        params = [
            (art['Titolo'], art['Link'], art['Preview'], art['Macro'], art['Area'], art['Fonte'])
            for art in articoli_lista
        ]
        with self.get_cursor() as cur:
            cur.executemany(query, params)  # executemany invece di loop: piu' efficiente

    def estrai_archivio(self, filtro_macro: str, user_id: int, ricerca_testo: str = "") -> List[Dict]:
        query = """SELECT * FROM articles WHERE macro = %s AND fonte NOT IN (SELECT s.nome FROM sources s JOIN user_source_preferences usp ON s.id = usp.source_id WHERE usp.user_id = %s AND usp.is_active = FALSE)"""
        params: List = [filtro_macro, user_id]
        if ricerca_testo:
            query += " AND (titolo ILIKE %s OR preview ILIKE %s OR area ILIKE %s)"
            text_param = f"%{ricerca_testo}%"
            params.extend([text_param, text_param, text_param])
        query += " ORDER BY data_scansione DESC LIMIT 100"
        with self.get_cursor(dict_cursor=True) as cur:
            cur.execute(query, params)
            articoli = cur.fetchall()
        return [dict(a) for a in articoli]

    # ------------------------------------------------------------------
    # BOOKMARKS
    # ------------------------------------------------------------------
    def aggiungi_bookmark(self, user_id: int, article_id: int) -> None:
        try:
            with self.get_cursor() as cur:
                cur.execute(
                    "INSERT INTO bookmarks (user_id, article_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (user_id, article_id),
                )
        except Exception as e:
            logging.error("Errore aggiunta bookmark: %s", e)

    def rimuovi_bookmark(self, user_id: int, article_id: int) -> None:
        with self.get_cursor() as cur:
            cur.execute(
                "DELETE FROM bookmarks WHERE user_id = %s AND article_id = %s",
                (user_id, article_id),
            )

    def estrai_bookmarks(self, user_id: int, ricerca_testo: str = "") -> List[Dict]:
        query = """SELECT a.* FROM articles a JOIN bookmarks b ON a.id = b.article_id WHERE b.user_id = %s"""
        params: List = [user_id]
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
            cur.execute(
                "SELECT 1 FROM bookmarks WHERE user_id = %s AND article_id = %s",
                (user_id, article_id),
            )
            esiste: bool = cur.fetchone() is not None
        return esiste

    # ------------------------------------------------------------------
    # DASHBOARD / ALERT
    # ------------------------------------------------------------------
    def estrai_metriche_dashboard(self, user_id: int) -> Dict[str, int]:
        with self.get_cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM articles")
            tot_articoli = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM sources")
            tot_fonti = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM bookmarks WHERE user_id = %s", (user_id,))
            tot_salvati = cur.fetchone()[0]
        return {"articoli": tot_articoli, "fonti": tot_fonti, "salvati": tot_salvati}

    def estrai_ultimi_alert_urgenti(self, user_id: int) -> List[Dict]:
        keywords = ['%sanzion%', '%ordinanza%', '%condanna%', '%violazion%',
                    '%scadenza%', '%obbligo%', '%divieto%', '%sentenza%']
        condizioni = " OR ".join(["titolo ILIKE %s" for _ in keywords])
        query = """SELECT * FROM articles WHERE ({}) AND fonte NOT IN (SELECT s.nome FROM sources s JOIN user_source_preferences usp ON s.id = usp.source_id WHERE usp.user_id = %s AND usp.is_active = FALSE) ORDER BY data_scansione DESC LIMIT 4""".format(condizioni)
        params = keywords + [user_id]
        with self.get_cursor(dict_cursor=True) as cur:
            cur.execute(query, params)
            alert = cur.fetchall()
        return [dict(a) for a in alert]


# ======================================================================
# ISTANZA DB CACHATA: creata una sola volta per sessione del server.
# init_db() gira una sola volta per deploy grazie a @st.cache_resource.
# ======================================================================
@st.cache_resource
def get_db() -> LegalRadarDB:
    db_url = st.secrets.get("DB_URL", "")
    if not db_url:
        st.error("Configurazione critica corrotta: DB_URL non definito nei Secrets.")
        st.stop()
    db = LegalRadarDB(db_url)
    db.init_db()  # eseguito una volta sola: la funzione cachata non si ripete
    return db


db = get_db()

if 'user' not in st.session_state:
    st.session_state.user = None
if 'ai_summaries' not in st.session_state:
    st.session_state.ai_summaries = {}


# ======================================================================
# AI / SCRAPING
# ======================================================================
def estrai_testo_pulito(url: str) -> str:
    if url.lower().endswith(('.pdf', '.zip', '.doc')):
        return ""
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) LegalRadar/3.0"}
        res = requests.get(url, headers=headers, timeout=6)
        soup = BeautifulSoup(res.text, 'html.parser')
        paragraphs = soup.find_all(['p', 'div'])
        return " ".join([p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 45])[:6000]
    except Exception:
        return ""


def genera_sintesi_groq(url: str, preview_text: str) -> str:
    raw_key: str = st.secrets.get("GROQ_API_KEY", "").strip()
    if not raw_key.startswith("gsk_"):
        return "⚠️ Configura la chiave GROQ_API_KEY nei Secrets."
    testo_sito: str = estrai_testo_pulito(url)
    input_ai: str = testo_sito if len(testo_sito) > 200 else preview_text
    api_url: str = "https://api.groq.com/openai/v1/chat/completions"
    headers: Dict[str, str] = {"Authorization": f"Bearer {raw_key}", "Content-Type": "application/json"}

    system_prompt: str = (
        "Sei un Senior Legal Counsel esperto di compliance e mercati digitali, specializzato nel settore dei "
        "comparatori online e aggregatori di tariffe in Italia (es. Facile.it, Segugio.it). "
        "Analizza il testo fornito ed elabora un report strutturato in tre sezioni: "
        "1) EXECUTIVE SUMMARY: sintesi nucleo normativo. "
        "2) ANALISI LEGALE: profili di rischio. "
        "3) IMPATTO COMPARATORI ONLINE: impatti operativi e di compliance."
    )

    payload: Dict = {
        "model": "llama-3.3-70b-versatile",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Testo:\n\n{input_ai}"},
        ],
        "temperature": 0.2,
    }
    try:
        r = requests.post(api_url, headers=headers, json=payload, timeout=15)
        if r.status_code == 200:
            return r.json()['choices'][0]['message']['content'].strip()
        return f"⚠️ Errore AI ({r.status_code})"
    except Exception:
        return "⚠️ Connessione AI fallita."


def sincronizza_radar_in_database() -> None:
    fonti: List[Dict] = db.carica_fonti()
    articoli_scovati: List[Dict] = []
    for f in fonti:
        try:
            feed = feedparser.parse(f['url'])
            for entry in feed.entries[:5]:
                sommario: str = entry.summary if hasattr(entry, 'summary') else ""
                preview: str = BeautifulSoup(sommario, "html.parser").get_text()[:250] + "..."
                articoli_scovati.append({
                    "Titolo": entry.title, "Link": entry.link, "Preview": preview,
                    "Macro": f['macro'], "Area": f['area'], "Fonte": f['nome'],
                })
        except Exception:
            continue
    if articoli_scovati:
        db.salva_articoli_storico(articoli_scovati)


# ======================================================================
# AUTENTICAZIONE
# ======================================================================
if st.session_state.user is None:
    st.title("⚖️ Legal Radar | Autenticazione")
    scelta: str = st.radio("Seleziona Azione", ["Accedi", "Registrati"], horizontal=True)
    with st.form("auth_form"):
        username: str = st.text_input("Username / Email")
        password: str = st.text_input("Password", type="password")
        submit: bool = st.form_submit_button("Conferma")
        if submit:
            if not username or not password:
                st.error("Compila tutti i campi.")
            elif scelta == "Registrati":
                try:
                    if db.registra_utente(username, password):
                        st.success("Registrazione completata!")
                    else:
                        st.error("Username esistente.")
                except Exception:
                    st.error("Errore del servizio. Riprova piu' tardi.")
            elif scelta == "Accedi":
                user: Optional[Dict] = db.verifica_utente(username, password)
                if user:
                    st.session_state.user = user
                    st.rerun()
                else:
                    st.error("Credenziali non valide.")
    st.stop()


# ======================================================================
# SIDEBAR
# ======================================================================
with st.sidebar:
    st.title("⚖️ Legal Radar")
    pagina: str = st.radio("Navigazione", ["🏠 Dashboard", "📖 Leggi & Normativa", "🏛️ Provvedimenti & Sentenze", "📰 News & Aggiornamenti", "🔖 I Miei Salvati", "⚙️ Gestione Fonti"])
    if st.button("🔄 Sincronizza ed Espandi Archivio", type="primary", use_container_width=True):
        with st.spinner("Scansione in corso..."):
            sincronizza_radar_in_database()
            st.rerun()
    if st.button("🚪 Esci", use_container_width=True):
        st.session_state.user = None
        st.rerun()


def mostra_hub_legale(lista_articoli: List[Dict], tipo_bacheca: str) -> None:
    if not lista_articoli:
        st.info("Nessun record presente.")
        return
    for art in lista_articoli:
        with st.container():
            st.markdown(f"**{art['titolo']}** ({art['fonte']}) - {art['preview']}")
            c1, c2 = st.columns([1, 3])
            with c1:
                if tipo_bacheca == "bookmarks":
                    if st.button("Rimuovi", key=f"rem_{art['id']}"):
                        db.rimuovi_bookmark(st.session_state.user['id'], art['id'])
                        st.rerun()
                else:
                    if not db.check_bookmark_esiste(st.session_state.user['id'], art['id']):
                        if st.button("Salva", key=f"save_{art['id']}"):
                            db.aggiungi_bookmark(st.session_state.user['id'], art['id'])
                            st.rerun()
            link: str = art['link']
            if link in st.session_state.ai_summaries:
                st.markdown(st.session_state.ai_summaries[link])
            else:
                with c2:
                    if st.button("Analisi AI", key=f"ai_{art['id']}"):
                        st.session_state.ai_summaries[link] = genera_sintesi_groq(link, art['preview'])
                        st.rerun()


# ======================================================================
# ROUTING PAGINE
# ======================================================================
if pagina == "🏠 Dashboard":
    st.title("Dashboard")
    metriche = db.estrai_metriche_dashboard(st.session_state.user['id'])
    st.write(f"Articoli totali: {metriche['articoli']} | Fonti: {metriche['fonti']} | Salvati: {metriche['salvati']}")

elif pagina in ["📖 Leggi & Normativa", "🏛️ Provvedimenti & Sentenze", "📰 News & Aggiornamenti"]:
    macro_categoria = pagina.replace("📖 ", "").replace("🏛️ ", "").replace("📰 ", "")
    dati_db = db.estrai_archivio(macro_categoria, st.session_state.user['id'])
    mostra_hub_legale(dati_db, "radar")

elif pagina == "🔖 I Miei Salvati":
    dati_salvati = db.estrai_bookmarks(st.session_state.user['id'])
    mostra_hub_legale(dati_salvati, "bookmarks")

elif pagina == "⚙️ Gestione Fonti":
    st.title("Gestione Fonti")
    fonti_personali = db.carica_fonti_con_preferenze(st.session_state.user['id'])
    for f in fonti_personali:
        is_on = st.toggle(f"{f['nome']}", value=f['utente_attiva'], key=f"tog_{f['id']}")
        if is_on != f['utente_attiva']:
            db.imposta_preferenza_fonte(st.session_state.user['id'], f['id'], is_on)
            st.rerun()
