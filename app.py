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

# --- 1. CLASSE ARCHITETTURALE DATABASE (POSTGRESQL) ---
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

    def estrai_archivio(self, filtro_macro: str, ricerca_testo: str = "") -> List[Dict]:
        conn = self.get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        query = "SELECT * FROM articles WHERE macro = %s"
        params = [filtro_macro]
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

    def estrai_ultimi_alert_urgenti(self) -> List[Dict]:
        conn = self.get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        keywords = ['%sanzion%', '%ordinanza%', '%condanna%', '%violazion%', '%scadenza%', '%obbligo%', '%divieto%', '%sentenza%']
        
        query = "SELECT * FROM articles WHERE " + " OR ".join(["titolo ILIKE %s" for _ in keywords]) + " ORDER BY data_scansione DESC LIMIT 4"
        cur.execute(query, keywords)
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

# Seed delle fonti predefinite se il DB è vuoto
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
    .card-meta-rich { font-size: 13px; color: #666; margin-bottom: 15px; display: flex; flex-wrap: wrap; gap: 12px; border-bottom: 1px solid #f0f0f0; padding-bottom: 10px;}
    .card-preview { font-size: 14px; color: #4a4a4a; margin-bottom: 15px; line-height: 1.6; }
    .card-summary { font-size: 14px; color: #333; line-height: 1.6; background: #fff5eb; border: 1px solid #ffd6b3; padding: 15px; border-radius: 8px; margin-top: 15px; }
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

def genera_sintesi_groq(url: str, preview_text: str) -> str:
    raw_key = st.secrets.get("GROQ_API_KEY", "").strip()
    if not raw_key.startswith("gsk_"): return "⚠️ Configura la chiave GROQ_API_KEY nei Secrets."
    
    testo_sito = estrai_testo_pulito(url)
    input_ai = testo_sito if len(testo_sito) > 200 else preview_text
    
    api_url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {raw_key}", "Content-Type": "application/json"}
    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [
            {"role": "system", "content": "Sei un esperto legale italiano. Fai un sunto di max 3 frasi evidenziando nucleo normativo e impatti pratici."},
            {"role": "user", "content": input_ai}
        ],
        "temperature": 0.2
    }
    try:
        r = requests.post(api_url, headers=headers, json=payload, timeout=12)
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
        st.info("Nessun articolo trovato in questo archivio storico.")
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
                st.markdown(f"<div class='card-summary'>✨ <b>Sintesi AI:</b><br>{st.session_state.ai_summaries[link]}</div>", unsafe_allow_html=True)
            else:
                with c2:
                    if st.button("✨ Genera Analisi AI", key=f"ai_{art['id']}"):
                        with st.spinner("Analisi in corso..."):
                            st.session_state.ai_summaries[link] = genera_sintesi_groq(link, art['preview'])
                            st.rerun()
            st.write("")

ricerca = ""
if pagina not in ["⚙️ Gestione Fonti", "🏠 Dashboard"]:
    ricerca = st.text_input("🔍 Cerca parole chiave nell'archivio storico...")

# --- ROUTING DELLE PAGINE ---
if pagina == "🏠 Dashboard":
    st.title(f"Benvenuto nel tuo Hub, {st.session_state.user['username']}! 👋")
    st.caption(f"Stato dell'Intelligence Normativa al {datetime.now().strftime('%d/%m/%Y')}")
    st.write("")
    
    metriche = db.estrai_metriche_dashboard(st.session_state.user['id'])
    c1, c2, c3 = st.columns(3)
    c1.metric("📚 Archivio Storico Comune", f"{metriche['articoli']} articoli", help="Totale articoli accumulati")
    c2.metric("📡 Canali Radar Attivi", f"{metriche['fonti']} fonti", help="Siti istituzionali monitorati")
    c3.metric("🔖 La Tua Rassegna", f"{metriche['salvati']} salvati", help="Articoli preferiti custoditi")
    
    st.divider()
    
    st.subheader("🔥 Ultimi Alert Urgenti Rilevati")
    st.caption("Notizie recenti contenenti parole chiave critiche (sanzioni, obblighi, sentenze)")
    
    alert_urgenti = db.estrai_ultimi_alert_urgenti()
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
        st.info("Nessun alert urgente rilevato nelle ultime scansioni.")
        
    st.divider()
    st.subheader("⚡ Azioni Rapide")
    st.info("💡 **Consiglio del Team:** Ricordati di cliccare su **'Sincronizza ed Espandi Archivio'** nella barra laterale per addestrare lo storico e scovare nuovi provvedimenti.")

elif pagina in ["📖 Leggi & Normativa", "🏛️ Provvedimenti & Sentenze", "📰 News & Aggiornamenti"]:
    macro_categoria = pagina.replace("📖 ", "").replace("🏛️ ", "").replace("📰 ", "")
    st.header(macro_categoria)
    dati_db = db.estrai_archivio(filtro_macro=macro_categoria, ricerca_testo=ricerca)
    mostra_hub_legale(dati_db, tipo_bacheca="radar")

elif pagina == "🔖 I Miei Salvati":
    st.header("I Miei Articoli Salvati")
    dati_salvati = db.estrai_bookmarks(user_id=st.session_state.user['id'], ricerca_testo=ricerca)
    mostra_hub_legale(dati_salvati, tipo_bacheca="bookmarks")

elif pagina == "⚙️ Gestione Fonti":
    st.header("Database Persistente Fonti")
    
    with st.form("form_aggiunta_fonte", clear_on_submit=True):
        c1, c2 = st.columns(2)
        n_nome = c1.text_input("Nome Autorità / Sito")
        n_url = c1.text_input("URL Feed RSS")
        n_macro = c2.selectbox("Categoria Macro", ["Leggi & Normativa", "Provvedimenti & Sentenze", "News & Aggiornamenti"])
        n_area = c2.text_input("Materia Giuridica (es. Compliance, Privacy)")
        if st.form_submit_button("➕ Salva Fonte nel Database"):
            if n_nome and n_url and n_area:
                if db.aggiungi_fonte(n_nome, n_url, n_area, n_macro):
                    st.success(f"Fonte '{n_nome}' registrata nel DB cloud!")
                    st.rerun()
                else:
                    st.error("Errore. URL probabilmente già registrato.")
            else:
                st.error("Compila tutti i campi.")
                
    st.divider()
    st.subheader("📚 Fonti Attualmente Sincronizzate")
    fonti_attive = db.carica_fonti()
    for f in fonti_attive:
        col_t, col_b = st.columns([5, 1])
        col_t.markdown(f"**{f['nome']}** - {f['macro']} (*{f['area']}*)<br><span style='font-size:12px;color:#888;'>{f['url']}</span>", unsafe_allow_html=True)
        if col_b.button("🗑️ Elimina", key=f"del_src_{f['id']}"):
            db.rimuovi_fonte(f['id'])
            st.success("Fonte rimossa.")
            st.rerun()
        st.write("---")
