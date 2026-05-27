import streamlit as st
import pandas as pd
import feedparser
import time
import requests
import os
from bs4 import BeautifulSoup
from datetime import datetime
import psycopg2
import psycopg2.extras
import bcrypt
from typing import List, Dict, Tuple, Optional

# --- 1. CLASSE ARCHITETTURALE DATABASE (POSTGRESQL MULTI-TENANT) ---
class LegalRadarDB:
    def __init__(self, db_url: str):
        self.db_url = db_url
        self.init_db()

    def get_connection(self):
        return psycopg2.connect(self.db_url)

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
                macro VARCHAR(150)
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
            """
        )
        conn = None
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            for command in commands:
                cur.execute(command)
            cur.close()
            conn.commit()
        except Exception as e:
            st.error(f"Errore critico di connessione al database: {e}")
        finally:
            if conn: conn.close()

    def registra_utente(self, username: str, password: str) -> bool:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            hashed = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
            cur.execute("INSERT INTO users (username, password_hash) VALUES (%s, %s)", (username.strip(), hashed))
            conn.commit()
            cur.close()
            return True
        except: return False
        finally: conn.close()

    def verifica_utente(self, username: str, password: str) -> Optional[Dict]:
        conn = self.get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT * FROM users WHERE username = %s", (username.strip(),))
        user = cur.fetchone()
        cur.close()
        conn.close()
        if user and bcrypt.checkpw(password.encode('utf-8'), user['password_hash'].encode('utf-8')):
            return dict(user)
        return None

    def carica_fonti(self) -> List[Dict]:
        conn = self.get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT * FROM sources ORDER BY nome ASC")
        fonti = cur.fetchall()
        cur.close()
        conn.close()
        return [dict(f) for f in fonti]

    # --- AGGIUNTA: LETTURA DELLE FONTI CON STATO DI ACCENSIONE UTENTE ---
    def carica_fonti_con_preferenze(self, user_id: int) -> List[Dict]:
        conn = self.get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        query = """
            SELECT s.*, COALESCE(usp.is_active, TRUE) as utente_attiva
            FROM sources s
            LEFT JOIN user_source_preferences usp ON s.id = usp.source_id AND usp.user_id = %s
            ORDER BY s.nome ASC
        """
        cur.execute(query, (user_id,))
        fonti = cur.fetchall()
        cur.close()
        conn.close()
        return [dict(f) for f in fonti]

    # --- AGGIUNTA: SALVATAGGIO ACCENSIONE/SPEGNIMENTO PERSONALE ---
    def imposta_preferenza_fonte(self, user_id: int, source_id: int, is_active: bool) -> None:
        conn = self.get_connection()
        cur = conn.cursor()
        query = """
            INSERT INTO user_source_preferences (user_id, source_id, is_active)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id, source_id) 
            DO UPDATE SET is_active = EXCLUDED.is_active
        """
        cur.execute(query, (user_id, source_id, is_active))
        conn.commit()
        cur.close()
        conn.close()

    def aggiungi_fonte(self, nome: str, url: str, area: str, macro: str) -> bool:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("INSERT INTO sources (nome, url, area, macro) VALUES (%s, %s, %s, %s) ON CONFLICT (url) DO NOTHING", (nome, url, area, macro))
            conn.commit()
            cur.close()
            return True
        except: return False
        finally: conn.close()

    def rimuovi_fonte(self, fonte_id: int) -> None:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM sources WHERE id = %s", (fonte_id,))
        conn.commit()
        cur.close()
        conn.close()

    def salva_articoli_storico(self, articoli_lista: List[Dict]) -> None:
        conn = self.get_connection()
        cur = conn.cursor()
        query = """
            INSERT INTO articles (titolo, link, preview, macro, area, fonte) 
            VALUES (%s, %s, %s, %s, %s, %s) 
            ON CONFLICT (link) DO NOTHING
        """
        for art in articoli_lista:
            cur.execute(query, (art['Titolo'], art['Link'], art['Preview'], art['Macro'], art['Area'], art['Fonte']))
        conn.commit()
        cur.close()
        conn.close()

    # --- MODIFICA: ESTRAZIONE ARCHIVIO FILTRATO SULLE PREFERENZE UTENTE ---
    def estrai_archivio(self, filtro_macro: str, user_id: int, ricerca_testo: str = "") -> List[Dict]:
        conn = self.get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        query = """
            SELECT * FROM articles 
            WHERE macro = %s 
            AND fonte NOT IN (
                SELECT s.nome FROM sources s
                JOIN user_source_preferences usp ON s.id = usp.source_id
                WHERE usp.user_id = %s AND usp.is_active = FALSE
            )
        """
        params = [filtro_macro, user_id]
        if ricerca_testo:
            query += " AND (titolo ILIKE %s OR preview ILIKE %s OR area ILIKE %s)"
            text_param = f"%{ricerca_testo}%"
            params.extend([text_param, text_param, text_param])
        query += " ORDER BY data_scansione DESC LIMIT 100"
        cur.execute(query, params)
        articoli = cur.fetchall()
        cur.close()
        conn.close()
        return [dict(a) for a in articoli]

    def aggiungi_bookmark(self, user_id: int, article_id: int) -> None:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("INSERT INTO bookmarks (user_id, article_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (user_id, article_id))
            conn.commit()
            cur.close()
        except: pass
        finally: conn.close()

    def rimuovi_bookmark(self, user_id: int, article_id: int) -> None:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM bookmarks WHERE user_id = %s AND article_id = %s", (user_id, article_id))
        conn.commit()
        cur.close()
        conn.close()

    def estrai_bookmarks(self, user_id: int, ricerca_testo: str = "") -> List[Dict]:
        conn = self.get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        query = """
            SELECT a.* FROM articles a 
            JOIN bookmarks b ON a.id = b.article_id 
            WHERE b.user_id = %s
        """
        params = [user_id]
        if ricerca_testo:
            query += " AND (a.titolo ILIKE %s OR a.preview ILIKE %s)"
            text_param = f"%{ricerca_testo}%"
            params.extend([text_param, text_param])
        query += " ORDER BY a.data_scansione DESC"
        cur.execute(query, params)
        salvati = cur.fetchall()
        cur.close()
        conn.close()
        return [dict(s) for s in salvati]

    def check_bookmark_esiste(self, user_id: int, article_id: int) -> bool:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM bookmarks WHERE user_id = %s AND article_id = %s", (user_id, article_id))
        esiste = cur.fetchone() is not None
        cur.close()
        conn.close()
        return esiste

    def estrai_metriche_dashboard(self, user_id: int) -> Dict[str, int]:
        conn = self.get_connection()
        cur = conn.cursor()
        
        cur.execute("SELECT COUNT(*) FROM articles")
        tot_articoli = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM sources")
        tot_fonti = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM bookmarks WHERE user_id = %s", (user_id,))
        tot_salvati = cur.fetchone()[0]
        
        cur.close()
        conn.close()
        return {"articoli": tot_articoli, "fonti": tot_fonti, "salvati": tot_salvati}

    # --- MODIFICA: ANCHE GLI ALERT DELLA HOME ESCLUDONO LE FONTI SPENTE ---
    def estrai_ultimi_alert_urgenti(self, user_id: int) -> List[Dict]:
        conn = self.get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
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
        cur.execute(query, params)
        alert = cur.fetchall()
        cur.close()
        conn.close()
        return [dict(a) for a in alert]

# --- 2. CONFIGURAZIONE INIZIALE ---
DB_URL = st.secrets.get("DB_URL", "")
if not DB_URL:
    st.error("Rilevamento fallito: inserisci DB_URL nei Secrets di Streamlit.")
    st.stop()

db = LegalRadarDB(DB_URL)

if len(db.carica_fonti()) == 0:
    DEFAULT_FONTI = [
        {"nome": "Agenzia Entrate", "url": "https://www.agenziaentrate.gov.it/portale/web/guest/rss/novita", "area": "Diritto Tributario", "macro": "Leggi & Normativa"},
        {"nome": "Garante Privacy", "url": "https://www.garanteprivacy.it/o/gpdp-rss/rss?c=10490", "area": "Privacy", "macro": "Provvedimenti & Sentenze"},
        {"nome": "EDPB Europa", "url": "https://edpb.europa.eu/rss.xml", "area": "Privacy", "macro": "Provvedimenti & Sentenze"},
        {"nome": "Banca d'Italia", "url": "https://www.bancaditalia.it/rss/media.xml", "area": "Diritto Bancario", "macro": "Provvedimenti & Sentenze"},
        {"nome": "Consob", "url": "https://www.consob.it/web/area-pubblica/rss", "area": "Diritto Bancario", "macro": "Provvedimenti & Sentenze"},
        {"nome": "IVASS", "url": "https://www.ivass.it/util/index.rss.html?lingua=it", "area": "Diritto assicurativo", "macro": "Leggi & Normativa"},
        {"nome": "CGUE", "url": "https://curia.europa.eu/site/rss.jsp?lang=it&secondLang=en", "area": "Giurisprudenza UE", "macro": "Provvedimenti & Sentenze"},
        {"nome": "Altalex", "url": "https://www.altalex.com/rss", "area": "Legale Generale", "macro": "News & Aggiornamenti"}
    ]
    for f in DEFAULT_FONTI:
        db.aggiungi_fonte(f['nome'], f['url'], f['area'], f['macro'])

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

def sincronizza_radar_in_database() -> None:
    fonti = db.carica_fonti()
    articoli_scovati = []
    for f in fonti:
        try:
            feed = feedparser.parse(f['url'])
            for entry in feed.entries[:5]:
                sommario = entry.summary if hasattr(entry, 'summary') else ""
                preview = BeautifulSoup(sommario, "html.parser").get_text()[:250] + "..."
                articoli_scovati.append({
                    "Titolo": entry.title, "Link": entry.link, "Preview": preview,
                    "Macro": f['macro'], "Area": f['area'], "Fonte": f['nome']
                })
        except: continue
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
    
    pagina = st.radio("Navigazione", [
        "🏠 Dashboard",
        "📖 Leggi & Normativa", 
        "🏛️ Provvedimenti & Sentenze", 
        "📰 News & Aggiornamenti",
        "🔖 I Miei Salvati",
        "⚙️ Gestione Fonti"
    ])
    
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
            st.markdown(f"""
            <div class="radar-card">
                <div>
                    <span class="meta-tag tag-area">{art['area']}</span>
                    <span class="meta-tag tag-fonte">{art['fonte']}</span>
                </div>
                <a href="{art['link']}" target="_blank" class="card-title">{art['titolo']}</a>
                <div class="card-preview">{art['preview']}</div>
            </div>
            """, unsafe_allow_html=True)
            
            c1, c2 = st.columns([1, 3])
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
            
            link = art['link']
            if link in st.session_state.ai_summaries:
                # Sfruttiamo il markdown nativo di Streamlit per interpretare l'output strutturato dell'AI
                st.markdown("<div class='card-summary'>🤖 <b>Analisi Strategica Legal-Tech:</b></div>", unsafe_allow_html=True)
                st.markdown(st.session_state.ai_summaries[link])
            else:
                with c2:
                    if st.button("✨ Genera Analisi AI Strategica", key=f"ai_{art['id']}"):
                        with st.spinner("L'AI sta conducendo l'analisi verticale per i comparatori..."):
                            st.session_state.ai_summaries[link] = genera_sintesi_groq(link, art['preview'])
                            st.rerun()
            st.write("")

ricerca = ""
if pagina not in ["⚙️ Gestione Fonti", "🏠 Dashboard"]:
    ricerca = st.text_input("🔍 Cerca parole chiave nell'archivio storico...")

# --- ROUTING PAGINE ---
if pagina == "🏠 Dashboard":
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

elif pagina in ["📖 Leggi & Normativa", "🏛️ Provvedimenti & Sentenze", "📰 News & Aggiornamenti"]:
    macro_categoria = pagina.replace("📖 ", "").replace("🏛️ ", "").replace("📰 ", "")
    st.header(macro_categoria)
    # MODIFICA: Passiamo l'ID utente per nascondere gli articoli delle fonti spente
    dati_db = db.estrai_archivio(filtro_macro=macro_categoria, user_id=st.session_state.user['id'], ricerca_testo=ricerca)
    mostra_hub_legale(dati_db, tipo_bacheca="radar")

elif pagina == "🔖 I Miei Salvati":
    st.header("I Miei Articoli Salvati")
    dati_salvati = db.estrai_bookmarks(user_id=st.session_state.user['id'], ricerca_testo=ricerca)
    mostra_hub_legale(dati_salvati, tipo_bacheca="bookmarks")

elif pagina == "⚙️ Gestione Fonti":
    st.header("Database & Personalizzazione Fonti")
    
    # MODIFICA: SEZIONE 1 - INTERFACCIA ON/OFF PERSONALE (Punto 1)
    st.subheader("🎛️ Il Tuo Pannello di Controllo Canali (Personale)")
    st.caption("Spegni i canali che non vuoi vedere nel tuo feed. Questa modifica ha effetto solo sul tuo account.")
    
    fonti_personali = db.carica_fonti_con_preferenze(st.session_state.user['id'])
    for f in fonti_personali:
        col_info, col_toggle = st.columns([4, 1])
        col_info.markdown(f"**{f['nome']}** — *{f['area']}* ({f['macro']})")
        
        # Gestione interruttore ON/OFF in tempo reale
        is_on = col_toggle.toggle("Attivo", value=f['utente_attiva'], key=f"tog_{f['id']}")
        if is_on != f['utente_attiva']:
            db.imposta_preferenza_fonte(st.session_state.user['id'], f['id'], is_on)
            st.rerun()
            
    st.divider()
    
    # SEZIONE 2 - AGGIUNTA GLOBALE (Per tutti)
    st.subheader("➕ Aggiungi Nuova Fonte (Globale)")
    with st.form("form_aggiunta_fonte", clear_on_submit=True):
        c1, c2 = st.columns(2)
        n_nome = c1.text_input("Nome Autorità / Sito")
        n_url = c1.text_input("URL Feed RSS")
        n_macro = c2.selectbox("Categoria Macro", ["Leggi & Normativa", "Provvedimenti & Sentenze", "News & Aggiornamenti"])
        n_area = c2.text_input("Materia Giuridica (es. Compliance, Privacy)")
        if st.form_submit_button("➕ Salva Fonte nel Database Comune"):
            if n_nome and n_url and n_area:
                if db.aggiungi_fonte(n_nome, n_url, n_area, n_macro):
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
