import streamlit as st
import pandas as pd
import feedparser
import time
import requests
import json
import os
from bs4 import BeautifulSoup

# --- 1. SETUP AMBIENTE ---
st.set_page_config(page_title="Legal Radar | Hub", layout="wide", page_icon="⚖️")

# Configurazione Fonti Predefinite
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

if 'fonti_attive' not in st.session_state: st.session_state.fonti_attive = DEFAULT_FONTI
if 'ai_summaries' not in st.session_state: st.session_state.ai_summaries = {}

# --- 2. STILE GRAFICO ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; background-color: #f7f9fc; }
    .radar-card { background: white; border-radius: 12px; padding: 20px; border: 1px solid #eaeaea; margin-bottom: 15px; border-left: 5px solid #ff6600; box-shadow: 0 2px 5px rgba(0,0,0,0.05); }
    .card-title { font-size: 18px; font-weight: 700; color: #1a1a1a; text-decoration: none; display: block; margin-bottom: 10px; }
    .card-summary { font-size: 14px; color: #333; background: #fff5eb; border: 1px solid #ffd6b3; padding: 12px; border-radius: 8px; margin-top: 10px; }
    .meta-tag { display: inline-block; padding: 3px 8px; border-radius: 15px; font-size: 10px; font-weight: 700; text-transform: uppercase; margin-right: 5px; }
    .tag-area { background: #eef2ff; color: #4338ca; }
    .tag-fonte { background: #fff3eb; color: #ff6600; }
</style>
""", unsafe_allow_html=True)

# --- 3. LOGICA AI (NUOVO MOTORE: GROQ + LLAMA 3) ---
def estrai_testo(url: str) -> str:
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=6)
        soup = BeautifulSoup(res.text, 'html.parser')
        paragraphs = soup.find_all(['p', 'div'])
        return " ".join([p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 40])[:6000]
    except: return ""

def genera_sintesi_ai(url: str, preview: str) -> str:
    # Lettura della nuova chiave Groq
    api_key = st.secrets.get("GROQ_API_KEY", "").strip()
    
    if not api_key.startswith("gsk_"):
        return "⚠️ Configura una chiave GROQ_API_KEY (inizia con gsk_) nei Secrets di Streamlit."

    testo = estrai_testo(url)
    input_ai = testo if len(testo) > 200 else preview
    
    api_url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": "llama3-70b-8192",  # Modello potentissimo e veloce
        "messages": [
            {"role": "system", "content": "Sei un Senior Legal Counsel. Analizza il testo fornito e scrivi un executive summary in lingua italiana di massimo 3 frasi. Evidenzia solo il nucleo normativo e l'impatto pratico."},
            {"role": "user", "content": f"Testo da analizzare: {input_ai}"}
        ],
        "temperature": 0.2 # Bassa temperatura per risposte più precise e fattuali
    }
    
    try:
        response = requests.post(api_url, headers=headers, json=payload, timeout=15)
        if response.status_code == 200:
            return response.json()['choices'][0]['message']['content'].strip()
        else:
            return f"❌ Errore AI: {response.text}"
    except Exception as e:
        return f"⚠️ Errore di rete: {str(e)}"

# --- 4. ENGINE SCRAPING ---
@st.cache_data(ttl=3600)
def fetch_news(fonti):
    news = []
    for f in fonti:
        try:
            d = feedparser.parse(f['url'])
            for e in d.entries[:3]:
                news.append({
                    "Titolo": e.title, "Link": e.link, "Fonte": f['nome'], "Area": f['area'], "Macro": f['macro'],
                    "Preview": BeautifulSoup(e.summary, "html.parser").get_text()[:200] if 'summary' in e else ""
                })
        except: continue
    return pd.DataFrame(news)

# --- 5. UI PRINCIPALE ---
st.title("⚖️ Legal Radar")
if st.sidebar.button("🔄 Sincronizza Dati"): st.cache_data.clear()

df = fetch_news(st.session_state.fonti_attive)
tab1, tab2, tab3 = st.tabs(["Normativa", "Sentenze", "News"])

def render_tab(macro):
    if df.empty: return
    items = df[df['Macro'] == macro]
    for i, r in items.iterrows():
        st.markdown(f"""
        <div class="radar-card">
            <span class="meta-tag tag-area">{r['Area']}</span>
            <span class="meta-tag tag-fonte">{r['Fonte']}</span>
            <a href="{r['Link']}" target="_blank" class="card-title">{r['Titolo']}</a>
            <p style='font-size:13px; color:#444;'>{r['Preview']}...</p>
        </div>
        """, unsafe_allow_html=True)
        
        if r['Link'] in st.session_state.ai_summaries:
            st.markdown(f"<div class='card-summary'>✨ <b>Executive Summary:</b><br>{st.session_state.ai_summaries[r['Link']]}</div>", unsafe_allow_html=True)
        else:
            if st.button(f"✨ Genera Sintesi", key=f"btn_{macro}_{i}"):
                with st.spinner("L'AI sta analizzando il testo..."):
                    st.session_state.ai_summaries[r['Link']] = genera_sintesi_ai(r['Link'], r['Preview'])
                    st.rerun()

with tab1: render_tab("Leggi & Normativa")
with tab2: render_tab("Provvedimenti & Sentenze")
with tab3: render_tab("News & Aggiornamenti")
