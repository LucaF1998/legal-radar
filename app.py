import streamlit as st
import pandas as pd
import feedparser
import time
import requests
import json
import os
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List, Dict, Tuple

# --- 1. SETUP AMBIENTE ---
st.set_page_config(page_title="Legal Radar | Hub", layout="wide", page_icon="⚖️")

# Configurazione Fonti Predefinite
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
        except Exception:
            return DEFAULT_FONTI
    return DEFAULT_FONTI

def salva_fonti(fonti_list: List[Dict[str, str]]) -> None:
    with open(FILE_FONTI, "w", encoding="utf-8") as f:
        json.dump(fonti_list, f, indent=4)

if 'fonti_attive' not in st.session_state: 
    st.session_state.fonti_attive = carica_fonti()
if 'ai_summaries' not in st.session_state: 
    st.session_state.ai_summaries = {}

# --- 2. STILE GRAFICO ---
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

# --- 3. LOGICA DI ANALISI E AI (REST API DIRETTA) ---
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

def estrai_testo_pulito(url: str) -> str:
    """Scarica e pulisce il testo dell'articolo bypassando blocchi semplici."""
    if url.lower().endswith(('.pdf', '.zip', '.doc')):
        return ""
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        response = requests.get(url, headers=headers, timeout=8)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        paragrafi = soup.find_all(['p', 'div'])
        testo = " ".join([p.get_text(strip=True) for p in paragrafi if len(p.get_text(strip=True)) > 45])
        return testo[:8000]
    except:
        return ""

def genera_sintesi_gemini(url: str, preview_text: str = "") -> str:
    """Chiamata REST diretta a Gemini 1.5 Flash."""
    # Recupero e PULIZIA forzata della chiave
    raw_key = st.secrets.get("GEMINI_API_KEY", os.getenv("GEMINI_API_KEY", ""))
    api_key = str(raw_key).strip() # Rimuove spazi e ritorni a capo invisibili
    
    if not api_key.startswith("AIza"):
        return "⚠️ Errore: API Key non trovata o non valida nei Secrets."
    
    testo_sito = estrai_testo_pulito(url)
    testo_per_ai = testo_sito if len(testo_sito) > 200 else preview_text
            
    if len(testo_per_ai.strip()) < 30:
        return "⚠️ Contenuto non accessibile per l'analisi."

    prompt = (
        "Sei un esperto legale. Fornisci una sintesi ultra-rapida (max 3 frasi) "
        "indicando il nucleo giuridico e gli impatti pratici del seguente testo:\n\n"
        f"{testo_per_ai}"
    )
    
    # Endpoint ufficiale v1beta per massima compatibilità
    api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={api_key}"
    headers = {'Content-Type': 'application/json'}
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.2}
    }
    
    try:
        response = requests.post(api_url, headers=headers, json=payload, timeout=15)
        if response.status_code == 200:
            result = response.json()
            return result['candidates'][0]['content']['parts'][0]['text'].strip()
        else:
            return f"⚠️ Errore Google API {response.status_code}: {response.text[:100]}"
    except Exception as e:
        return f"⚠️ Errore connessione AI: {str(e)}"

# --- 4. MOTORE DI RICERCA NOTIZIE ---
def calcola_tempo_lettura(testo: str) -> int:
    parole = len(str(testo).split())
    return max(1, parole // 150) if parole > 50 else 3

@st.cache_data(ttl=3600, show_spinner=False)
def raccogli_notizie(fonti_list: List[Dict[str, str]]) -> pd.DataFrame:
    dati = []
    progress_bar = st.progress(0, "Aggiornamento radar...")
    
    for i, fonte in enumerate(fonti_list):
        try:
            feed = feedparser.parse(fonte['url'])
            # Limitiamo a 3/4 notizie per fonte per velocità
            for entry in feed.entries[:4]:
                priorita_lbl, css_border, css_tag = valuta_priorita(entry.title)
                sommario = entry.summary if hasattr(entry, 'summary') else ""
                
                # Gestione Data
                if hasattr(entry, 'published_parsed') and entry.published_parsed:
                    dt = datetime(*entry.published_parsed[:6])
                    data_str = dt.strftime("%d/%m/%Y %H:%M")
                    data_sort = entry.published_parsed
                else:
                    data_str = datetime.now().strftime("%d/%m/%Y")
                    data_sort = time.localtime()

                dati.append({
                    "Data": data_str,
                    "Data_Sort": data_sort,
                    "Macro": fonte.get('macro', 'News'), 
                    "Area": fonte['area'], 
                    "Fonte": fonte['nome'],
                    "Titolo": entry.title, 
                    "Autore": entry.get('author', 'Redazione'),
                    "TempoLettura": calcola_tempo_lettura(sommario),
                    "Link": entry.link,
                    "Preview": BeautifulSoup(sommario, "html.parser").get_text()[:250] + "...",
                    "CSS_Border": css_border, 
                    "CSS_Tag": css_tag,
                    "Priorità": priorita_lbl
                })
        except:
            continue
        progress_bar.progress(int((i+1)/len(fonti_list)*100))
    
    progress_bar.empty()
    df = pd.DataFrame(dati)
    if not df.empty:
        # Ordiniamo per priorità (ALTA prima) e poi per data decrescente
        df['P_Sort'] = df['Priorità'].apply(lambda x: 0 if "ALTA" in x else (1 if "MEDIA" in x else 2))
        df = df.sort_values(by=['P_Sort', 'Data_Sort'], ascending=[True, False]).drop(columns=['P_Sort', 'Data_Sort'])
    return df

def rendering_notizie(df_filtrato: pd.DataFrame):
    if df_filtrato.empty:
        st.info("Nessun aggiornamento trovato in questa categoria.")
        return
    
    for idx, row in df_filtrato.iterrows():
        st.markdown(f"""
        <div class="radar-card {row['CSS_Border']}">
            <div>
                <span class="meta-tag {row['CSS_Tag']}">{row['Priorità']}</span>
                <span class="meta-tag tag-area">{row['Area']}</span>
                <span class="meta-tag tag-fonte">{row['Fonte']}</span>
            </div>
            <a href="{row['Link']}" target="_blank" class="card-title">{row['Titolo']}</a>
            <div class="card-meta-rich">
                <div class="meta-item">🗓️ <b>{row['Data']}</b></div>
                <div class="meta-item">⏱️ ~{row['TempoLettura']} min</div>
            </div>
            <div class="card-preview">{row['Preview']}</div>
        </div>
        """, unsafe_allow_html=True)
        
        # Gestione AI Summary
        link = row['Link']
        if link in st.session_state.ai_summaries:
            st.markdown(f"<div class='card-summary'>✨ <b>Executive Summary:</b><br>{st.session_state.ai_summaries[link]}</div>", unsafe_allow_html=True)
        else:
            if st.button("✨ Analizza con AI", key=f"ai_{idx}"):
                with st.spinner("Analisi in corso..."):
                    sintesi = genera_sintesi_gemini(link, row['Preview'])
                    st.session_state.ai_summaries[link] = sintesi
                    st.rerun()
        st.write("")

# --- 5. INTERFACCIA UTENTE ---
if 'df_news' not in st.session_state:
    st.session_state.df_news = raccogli_notizie(st.session_state.fonti_attive)

with st.sidebar:
    st.title("⚖️ Legal Radar")
    st.caption("Intelligence Normativa v2.0")
    
    pagina = st.radio("Sezioni", [
        "📖 Leggi & Normativa", 
        "🏛️ Provvedimenti & Sentenze", 
        "📰 News & Aggiornamenti",
        "⚙️ Gestione Fonti"
    ])
    
    st.divider()
    if st.button("🔄 Sincronizza Adesso", type="primary", use_container_width=True):
        st.cache_data.clear()
        st.session_state.df_news = raccogli_notizie(st.session_state.fonti_attive)
        st.rerun()

# Routing Pagine
df = st.session_state.df_news
if pagina == "📖 Leggi & Normativa":
    st.header("Leggi & Normativa")
    rendering_notizie(df[df['Macro'] == "Leggi & Normativa"])

elif pagina == "🏛️ Provvedimenti & Sentenze":
    st.header("Provvedimenti & Sentenze")
    rendering_notizie(df[df['Macro'] == "Provvedimenti & Sentenze"])

elif pagina == "📰 News & Aggiornamenti":
    st.header("News & Aggiornamenti")
    rendering_notizie(df[df['Macro'] == "News & Aggiornamenti"])

elif pagina == "⚙️ Gestione Fonti":
    st.header("Database Fonti")
    # Form aggiunta
    with st.form("nuova_fonte"):
        c1, c2 = st.columns(2)
        n_nome = c1.text_input("Nome Autorità")
        n_url = c1.text_input("URL Feed RSS")
        n_macro = c2.selectbox("Categoria", ["Leggi & Normativa", "Provvedimenti & Sentenze", "News & Aggiornamenti"])
        n_area = c2.text_input("Area Legale (es. Privacy)")
        if st.form_submit_button("Aggiungi"):
            if n_nome and n_url:
                st.session_state.fonti_attive.append({"nome": n_nome, "url": n_url, "area": n_area, "macro": n_macro})
                salva_fonti(st.session_state.fonti_attive)
                st.success("Fonte aggiunta! Sincronizza per vedere i dati.")
    
    # Lista fonti per eliminazione
    for i, f in enumerate(st.session_state.fonti_attive):
        col1, col2 = st.columns([4,1])
        col1.write(f"**{f['nome']}** ({f['area']})")
        if col2.button("Elimina", key=f"del_{i}"):
            st.session_state.fonti_attive.pop(i)
            salva_fonti(st.session_state.fonti_attive)
            st.rerun()
