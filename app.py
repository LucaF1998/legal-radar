import streamlit as st
import pandas as pd
import feedparser
import time
import requests
import json
import os
from bs4 import BeautifulSoup
from datetime import datetime
import google.generativeai as genai
from typing import List, Dict, Tuple

# --- 1. SETUP AMBIENTE E SICUREZZA ---
st.set_page_config(page_title="Legal Radar | Hub", layout="wide", page_icon="⚖️")

# Inizializzazione Sicura della API Key di Gemini tramite st.secrets
def inizializza_ai() -> bool:
    try:
        # Cerca la chiave nei secrets di Streamlit Cloud, o nelle variabili d'ambiente (per test locale)
        api_key = st.secrets.get("GEMINI_API_KEY", os.getenv("GEMINI_API_KEY"))
        if api_key and api_key.startswith("AIza"):
            genai.configure(api_key=api_key)
            return True
        return False
    except FileNotFoundError:
        return False

AI_ATTIVA = inizializza_ai()

DEFAULT_FONTI: List[Dict[str, str]] = [
    {"nome": "Agenzia Entrate", "url": "https://www.agenziaentrate.gov.it/portale/web/guest/rss/novita", "area": "Diritto Tributario", "macro": "Leggi & Normativa", "tipo": "RSS"},
    {"nome": "Garante Privacy", "url": "https://www.garanteprivacy.it/o/gpdp-rss/rss?c=10490", "area": "Privacy", "macro": "Provvedimenti & Sentenze", "tipo": "RSS"},
    {"nome": "EDPB Europa", "url": "https://edpb.europa.eu/rss.xml", "area": "Privacy", "macro": "Provvedimenti & Sentenze", "tipo": "RSS"},
    {"nome": "Banca d'Italia", "url": "https://www.bancaditalia.it/rss/media.xml", "area": "Diritto Bancario", "macro": "Provvedimenti & Sentenze", "tipo": "RSS"},
    {"nome": "Consob", "url": "https://www.consob.it/web/area-pubblica/rss", "area": "Diritto Bancario", "macro": "Provvedimenti & Sentenze", "tipo": "RSS"},
    {"nome": "IVASS", "url": "https://www.ivass.it/util/index.rss.html?lingua=it", "area": "Diritto assicurativo", "macro": "Leggi & Normativa", "tipo": "RSS"},
    {"nome": "CGUE", "url": "https://curia.europa.eu/site/rss.jsp?lang=it&secondLang=en", "area": "Giurisprudenza UE", "macro": "Provvedimenti & Sentenze", "tipo": "RSS"},
    {"nome": "EBA", "url": "https://www.eba.europa.eu/news-press/news/rss.xml", "area": "Diritto Bancario", "macro": "Leggi & Normativa", "tipo": "RSS"},
    {"nome": "Altalex", "url": "https://www.altalex.com/rss", "area": "Legale Generale", "macro": "News & Aggiornamenti", "tipo": "RSS"},
    {"nome": "Google News", "url": "https://news.google.com/rss/search?q=garante+privacy+OR+diritto+bancario+OR+agenzia+entrate+OR+legale+cybersecurity&hl=it&gl=IT&ceid=IT:it", "area": "News dal Web", "macro": "News & Aggiornamenti", "tipo": "RSS"}
]

FILE_FONTI = "fonti.json"

def carica_fonti() -> List[Dict[str, str]]:
    if os.path.exists(FILE_FONTI):
        try:
            with open(FILE_FONTI, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            st.warning(f"Errore caricamento fonti: {e}. Uso predefinite.")
            return DEFAULT_FONTI
    return DEFAULT_FONTI

def salva_fonti(fonti_list: List[Dict[str, str]]) -> None:
    with open(FILE_FONTI, "w", encoding="utf-8") as f:
        json.dump(fonti_list, f, indent=4)

if 'fonti_attive' not in st.session_state: 
    st.session_state.fonti_attive = carica_fonti()
if 'ai_summaries' not in st.session_state: 
    st.session_state.ai_summaries = {}

# --- 2. STILE GRAFICO (Mantenuto il tuo design originale pulito) ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; background-color: #f7f9fc; }
    div.stButton > button[kind="primary"] { background-color: #ff6600; color: white; border: none; font-weight: 600; border-radius: 8px;}
    div.stButton > button[kind="primary"]:hover { background-color: #e65c00; box-shadow: 0 4px 8px rgba(255, 102, 0, 0.3); }
    .radar-card { background: white; border-radius: 12px; padding: 24px; border: 1px solid #eaeaea; margin-bottom: 20px; border-left: 6px solid #ff6600; box-shadow: 0 2px 8px rgba(0,0,0,0.04); transition: transform 0.2s, box-shadow 0.2s; }
    .radar-card:hover { transform: translateY(-3px); box-shadow: 0 8px 16px rgba(0,0,0,0.08); border-left-color: #ff8533; }
    .priority-alta { border-left-color: #d32f2f; } 
    .priority-media { border-left-color: #f57c00; } 
    .card-title { font-size: 19px; font-weight: 700; color: #1a1a1a; text-decoration: none; margin-bottom: 12px; display: block; line-height: 1.3; }
    .card-title:hover { color: #ff6600; }
    .card-meta-rich { font-size: 13px; color: #666; margin-bottom: 15px; display: flex; flex-wrap: wrap; gap: 12px; align-items: center; border-bottom: 1px solid #f0f0f0; padding-bottom: 10px;}
    .meta-item { display: flex; align-items: center; gap: 4px; }
    .card-preview { font-size: 14px; color: #4a4a4a; margin-bottom: 15px; line-height: 1.6; }
    .card-summary { font-size: 14px; color: #333; line-height: 1.6; background: #fff5eb; border: 1px solid #ffd6b3; padding: 15px; border-radius: 8px; margin-top: 15px; }
    .meta-tag { display: inline-block; padding: 4px 10px; border-radius: 20px; font-size: 11px; font-weight: 700; text-transform: uppercase; margin-right: 8px; margin-bottom: 12px; letter-spacing: 0.5px;}
    .tag-area { background: #eef2ff; color: #4338ca; }
    .tag-fonte { background: #fff3eb; color: #ff6600; border: 1px solid #ffd6b3;}
    .tag-alta { background: #fef2f2; color: #dc2626; border: 1px solid #fecaca;}
    .tag-media { background: #fffbeb; color: #d97706; border: 1px solid #fde68a;}
    .tag-info { background: #f3f4f6; color: #4b5563; }
</style>
""", unsafe_allow_html=True)

# --- 3. MOTORE LOGICO E INTEGRAZIONE AI (Robustezza e Respectful Crawling) ---
KEYWORDS_ALTA_PRIORITA = ['sanzion', 'ordinanza', 'condanna', 'violazion', 'scadenza', 'obbligo', 'divieto', 'sentenza']
KEYWORDS_MEDIA_PRIORITA = ['linee guida', 'consultazione', 'parere', 'chiariment', 'orientament', 'regolamento', 'decreto']

def valuta_priorita(titolo: str) -> Tuple[str, str, str]:
    t = titolo.lower()
    if any(k in t for k in KEYWORDS_ALTA_PRIORITA): 
        return "ALTA (Urgenti/Sanzioni)", "priority-alta", "tag-alta"
    elif any(k in t for k in KEYWORDS_MEDIA_PRIORITA): 
        return "MEDIA (Norme/Linee Guida)", "priority-media", "tag-media"
    else: 
        return "INFO (Generale)", "", "tag-info"

def estrai_testo_difensivo(url: str) -> str:
    """Estrae testo bypassando i blocchi comuni ma rispettando le regole base del web."""
    if url.lower().endswith(('.pdf', '.zip', '.doc')):
        return "" # Logica per PDF da gestire in futuro
        
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, come Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        response = requests.get(url, headers=headers, timeout=8)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        paragrafi = soup.find_all(['p', 'div'])
        testo = " ".join([p.get_text(strip=True) for p in paragrafi if len(p.get_text(strip=True)) > 40])
        return testo[:8000] # Passiamo massimo 8k caratteri a Gemini per ottimizzare velocità e costi
    except Exception as e:
        return ""

def genera_sintesi_gemini(url: str, preview_text: str = "") -> str:
    if not AI_ATTIVA:
        return "⚠️ Configura la chiave GEMINI_API_KEY nei Secrets di Streamlit per attivare l'AI."
    
    testo_estratto = estrai_testo_difensivo(url)
    testo_per_ai = testo_estratto if len(testo_estratto) > 150 else preview_text
            
    if len(testo_per_ai.strip()) < 30:
        return "⚠️ Il contenuto originale non è accessibile e l'anteprima è assente. Sintesi non disponibile."

    prompt = (
        "Sei un Senior Legal Counsel. Fai una sintesi chiarissima e ultra-rapida (max 3 frasi) "
        "indicando il nucleo normativo/giuridico e gli impatti pratici del seguente testo. "
        "Se è una sentenza o sanzione, indica l'entità o il principio di diritto.\n\n"
        f"Testo: {testo_per_ai}"
    )
    
    try:
        model = genai.GenerativeModel('gemini-1.5-flash')
        response = model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        return f"⚠️ Errore di comunicazione con l'AI: {e}"

# --- 4. CRAWLER E GESTIONE DATI ---
def calcola_tempo_lettura(testo: str) -> int:
    parole = len(str(testo).split())
    if parole < 50: return 3 
    return max(1, parole // 150)

@st.cache_data(ttl=3600, show_spinner=False) # Cache ridotta a 1 ora per maggiore reattività
def raccogli_notizie_veloce(fonti_list: List[Dict[str, str]]) -> pd.DataFrame:
    dati = []
    barra = st.progress(0, "Sincronizzazione Fonti in corso...")
    
    for i, fonte in enumerate(fonti_list):
        if fonte.get('tipo', 'RSS') == 'RSS':
            try:
                feed = feedparser.parse(fonte['url'])
                limite = 4 if "Google" in fonte['nome'] else 3
                for entry in feed.entries[:limite]:
                    priorita_lbl, css_border, css_tag = valuta_priorita(entry.title)
                    sommario = entry.summary if hasattr(entry, 'summary') else ""
                    autore = entry.author if hasattr(entry, 'author') else "Redazione"
                    
                    if hasattr(entry, 'published_parsed') and entry.published_parsed:
                        dt = datetime(*entry.published_parsed[:6])
                        data_formattata = dt.strftime("%d/%m/%Y %H:%M")
                        sort_date = entry.published_parsed
                    else:
                        data_formattata = datetime.now().strftime("%d/%m/%Y")
                        sort_date = time.localtime()
                        
                    tags = [t.term for t in entry.tags if isinstance(t.term, str)] if hasattr(entry, 'tags') else []
                    tag_str = ", ".join(tags[:2]) if tags else "Diritto"

                    dati.append({
                        "Data": data_formattata,
                        "Data_Sort": sort_date,
                        "Macro": fonte.get('macro', 'News & Aggiornamenti'), 
                        "Area": fonte['area'], 
                        "Fonte": fonte['nome'],
                        "Titolo": entry.title, 
                        "Autore": autore,
                        "TempoLettura": calcola_tempo_lettura(sommario),
                        "Argomenti": tag_str,
                        "Priorità": priorita_lbl, 
                        "Link": entry.link,
                        "Preview": BeautifulSoup(sommario, "html.parser").get_text()[:250] + "...",
                        "CSS_Border": css_border, 
                        "CSS_Tag": css_tag
                    })
            except Exception as e:
                pass # Skipping broken feeds silently
        barra.progress(int((i+1)/len(fonti_list)*100))
        
    barra.empty()
    df = pd.DataFrame(dati)
    if not df.empty:
        df['Sort_Priorita'] = df['Priorità'].apply(lambda x: 0 if "ALTA" in x else (1 if "MEDIA" in x else 2))
        df = df.sort_values(by=['Sort_Priorita', 'Data_Sort'], ascending=[True, False]).drop(columns=['Sort_Priorita', 'Data_Sort'])
    return df

def mostra_cards_interattive(dataframe: pd.DataFrame) -> None:
    if dataframe.empty:
        st.info("Nessun aggiornamento in questa sezione.")
        return
    for idx, row in dataframe.iterrows():
        with st.container():
            html_str = f"""
            <div class="radar-card {row['CSS_Border']}">
                <div>
                    <span class="meta-tag {row['CSS_Tag']}">{row['Priorità']}</span>
                    <span class="meta-tag tag-area">{row['Area']}</span>
                    <span class="meta-tag tag-fonte">{row['Fonte']}</span>
                </div>
                <a href="{row['Link']}" target="_blank" class="card-title">{row['Titolo']}</a>
                <div class="card-meta-rich">
                    <div class="meta-item">🗓️ <b>{row['Data']}</b></div>
                    <div class="meta-item">✍️ {row['Autore']}</div>
                    <div class="meta-item">⏱️ ~{row['TempoLettura']} min</div>
                    <div class="meta-item">🏷️ {row['Argomenti']}</div>
                </div>
                <div class="card-preview">{row['Preview']}</div>
            </div>
            """
            st.markdown(html_str, unsafe_allow_html=True)
            
            link = row['Link']
            if link in st.session_state.ai_summaries:
                st.markdown(f"<div class='card-summary'>✨ <b>Sintesi AI:</b><br>{st.session_state.ai_summaries[link]}</div>", unsafe_allow_html=True)
            else:
                if st.button("✨ Genera Executive Summary", key=f"ai_btn_{idx}"):
                    with st.spinner("L'AI sta analizzando la normativa..."):
                        sintesi = genera_sintesi_gemini(link, row['Preview'])
                        st.session_state.ai_summaries[link] = sintesi
                        st.rerun()
            st.write("")

# --- 5. STRUTTURA DEL SITO E INTERFACCIA ---
if 'df_news' not in st.session_state or st.session_state.df_news.empty:
    st.session_state.df_news = raccogli_notizie_veloce(st.session_state.fonti_attive)
df = st.session_state.df_news

with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/3214/3214746.png", width=60)
    st.title("Legal Radar")
    st.caption("Intelligence Normativa Architettata")
    
    pagina = st.radio("Navigazione", [
        "📖 Leggi & Normativa", 
        "🏛️ Provvedimenti & Sentenze", 
        "📰 News & Aggiornamenti",
        "⚙️ Gestione Fonti"
    ])
    
    st.divider()
    if st.button("🔄 Sincronizza Hub", type="primary", use_container_width=True):
        st.cache_data.clear() 
        st.session_state.df_news = raccogli_notizie_veloce(st.session_state.fonti_attive)
        st.rerun()
        
    if not AI_ATTIVA:
        st.error("⚠️ Motore AI Disconnesso.")
    else:
        st.success("✅ Motore AI Operativo.")

# --- GESTIONE PAGINE ---
if pagina == "📖 Leggi & Normativa":
    st.header("Leggi & Normativa")
    if not df.empty: mostra_cards_interattive(df[df['Macro'] == "Leggi & Normativa"])

elif pagina == "🏛️ Provvedimenti & Sentenze":
    st.header("Provvedimenti & Sentenze")
    if not df.empty: mostra_cards_interattive(df[df['Macro'] == "Provvedimenti & Sentenze"])

elif pagina == "📰 News & Aggiornamenti":
    st.header("News & Aggiornamenti")
    if not df.empty: mostra_cards_interattive(df[df['Macro'] == "News & Aggiornamenti"])

elif pagina == "⚙️ Gestione Fonti":
    st.header("Gestione Database Fonti")
    
    with st.form("form_nuova_fonte", clear_on_submit=True):
        c1, c2 = st.columns(2)
        with c1:
            n_nome = st.text_input("Nome Autorità/Sito")
            n_url = st.text_input("URL (Feed RSS)")
        with c2:
            n_macro = st.selectbox("Categoria Macro", ["Leggi & Normativa", "Provvedimenti & Sentenze", "News & Aggiornamenti"])
            n_area = st.text_input("Materia (es. Penale, Compliance)")
        
        if st.form_submit_button("➕ Aggiungi al Radar"):
            if n_nome and n_url and n_area:
                st.session_state.fonti_attive.append({"nome": n_nome, "url": n_url, "area": n_area, "macro": n_macro, "tipo": "RSS"})
                salva_fonti(st.session_state.fonti_attive)
                st.cache_data.clear()
                st.session_state.df_news = raccogli_notizie_veloce(st.session_state.fonti_attive)
                st.success(f"Fonte '{n_nome}' validata e inserita!")
                st.rerun()
            else:
                st.error("Compilare tutti i campi obbligatori.")
                
    st.divider()
    st.subheader(f"📚 Repository Fonti Attuali ({len(st.session_state.fonti_attive)})")
    
    for i, fonte in enumerate(st.session_state.fonti_attive):
        col_testo, col_btn = st.columns([5, 1])
        with col_testo:
            st.markdown(f"**{fonte['nome']}** - {fonte['macro']} ({fonte['area']})<br><span style='font-size:12px;color:#888;'>{fonte['url']}</span>", unsafe_allow_html=True)
        with col_btn:
            if st.button("🗑️ Rimuovi", key=f"del_{i}"):
                st.session_state.fonti_attive.pop(i)
                salva_fonti(st.session_state.fonti_attive)
                st.cache_data.clear()
                st.session_state.df_news = raccogli_notizie_veloce(st.session_state.fonti_attive)
                st.rerun()
        st.write("---")
