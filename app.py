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
    .priority-alta { border-left-color: #d32f2f; } 
    .priority-media { border-left-color: #f57c00; } 
    .card-title { font-size: 18px; font-weight: 700; color: #1a1a1a; text-decoration: none; display: block; margin-bottom: 10px; }
    .card-meta { font-size: 12px; color: #666; margin-bottom: 10px; }
    .card-summary { font-size: 14px; color: #333; background: #fff5eb; border: 1px solid #ffd6b3; padding: 12px; border-radius: 8px; margin-top: 10px; }
    .meta-tag { display: inline-block; padding: 3px 8px; border-radius: 15px; font-size: 10px; font-weight: 700; text-transform: uppercase; margin-right: 5px; }
    .tag-area { background: #eef2ff; color: #4338ca; }
    .tag-fonte { background: #fff3eb; color: #ff6600; }
</style>
""", unsafe_allow_html=True)

# --- 3. LOGICA AI (v1 STABLE + DEBUG) ---
def estrai_testo(url: str) -> str:
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=5)
        soup = BeautifulSoup(res.text, 'html.parser')
        paragraphs = soup.find_all('p')
        return " ".join([p.get_text() for p in paragraphs])[:6000]
    except: return ""

def genera_sintesi_gemini(url: str, preview: str) -> str:
    # RECUPERO E PULIZIA CHIAVE
    raw_key = st.secrets.get("GEMINI_API_KEY", "")
    api_key = str(raw_key).replace('"', '').replace("'", "").strip()
    
    if not api_key.startswith("AIza"):
        return f"⚠️ Chiave non valida. Inizia con: '{api_key[:4]}...'"

    testo = estrai_testo(url)
    input_ai = testo if len(testo) > 200 else preview
    
    # ENDPOINT v1 STABLE
    api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={api_key}"
    
    payload = {
        "contents": [{"parts": [{"text": f"Fai una sintesi legale di 3 frasi: {input_ai}"}]}],
        "generationConfig": {"temperature": 0.1}
    }
    
    try:
        response = requests.post(api_url, json=payload, timeout=10)
        if response.status_code == 200:
            return response.json()['candidates'][0]['content']['parts'][0]['text']
        else:
            # Ora stampiamo TUTTO il messaggio originale di Google
            return f"❌ DIAGNOSTICA GOOGLE: {response.text}"
    except Exception as e:
        return f"⚠️ Errore connessione: {str(e)}"

# --- 4. ENGINE ---
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

# --- 5. UI ---
st.title("⚖️ Legal Radar")
if st.sidebar.button("🔄 Aggiorna Dati"): st.cache_data.clear()

df = fetch_news(st.session_state.fonti_attive)
tab1, tab2, tab3 = st.tabs(["Normativa", "Sentenze", "News"])

def render_tab(macro):
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
            st.markdown(f"<div class='card-summary'>✨ {st.session_state.ai_summaries[r['Link']]}</div>", unsafe_allow_html=True)
        else:
            if st.button(f"Analizza con AI", key=f"btn_{macro}_{i}"):
                with st.spinner("Analisi..."):
                    st.session_state.ai_summaries[r['Link']] = genera_sintesi_gemini(r['Link'], r['Preview'])
                    st.rerun()

with tab1: render_tab("Leggi & Normativa")
with tab2: render_tab("Provvedimenti & Sentenze")
with tab3: render_tab("News & Aggiornamenti")
