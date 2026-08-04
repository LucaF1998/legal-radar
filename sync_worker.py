import os
import re
import sys
import json
import time
import logging
from datetime import datetime
from typing import List, Tuple, Dict, Optional

import psycopg2
import psycopg2.extras
import feedparser
import requests
from bs4 import BeautifulSoup

# ============================================================
# MODELLO DI INFERENZA (Groq)
# llama-3.3-70b-versatile dismesso il 16/08/2026 -> openai/gpt-oss-120b.
# Alternativa: qwen/qwen3.6-27b. Cambiare solo questa riga.
# Tenere allineato con MODELLO_GROQ in app.py.
MODELLO_GROQ = "openai/gpt-oss-120b"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "").strip()


def normalizza_tema(tema):
    """Riconduce le varianti di tema alla forma canonica. Allineata all'app e al backfill_temi."""
    if not tema or not str(tema).strip():
        return "Generale"
    t = str(tema).strip().lower()
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
    if t in ("diritto", "generale", "varie", "altro", "n/d", "nd", "null", "none"):
        return "Generale"
    return str(tema).strip().capitalize()


def pulisci_titolo(titolo: str) -> str:
    """Normalizza i titoli sporchi dei feed istituzionali (es. CGUE)."""
    if not titolo:
        return titolo
    t = titolo.strip()
    m = re.match(r"^\d+/\w{3}\s\w{3}\s\d{1,2}.*?:\s*null\s*-\s*(.+)$", t)
    if m:
        t = m.group(1).strip()
    t = re.sub(r"\bnull\b\s*-?\s*", "", t).strip()
    t = re.sub(r"\s{2,}", " ", t)
    return t or titolo


def titolo_invalido(titolo: str) -> bool:
    """Rileva i titoli-segnaposto rotti alla fonte (es. AGCM: '$con.titolo1')."""
    if not titolo or not titolo.strip():
        return True
    t = titolo.strip()
    if t.lower() in ("null", "none", "undefined"):
        return True
    if re.fullmatch(r"[\s\W]*(?:\$\{?[\w.]+\}?|\{\{[\w.\s]+\}\}|%[\w.]+%)[\s\W]*", t):
        return True
    return False


def titolo_da_testo(testo: str, max_len: int = 110) -> str:
    """Deriva un titolo leggibile dalla prima frase del testo (fallback deterministico)."""
    if not testo:
        return "Aggiornamento dalla fonte"
    t = re.sub(r"\s{2,}", " ", testo.strip())
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


def genera_microriassunto(titolo: str, preview: str, e_ufficiale: bool = True,
                          serve_titolo: bool = False) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Riassunto + rilevanza + categoria + tema (+ titolo se serve_titolo) via Groq (JSON).
    Ritorna (riassunto, rilevanza, categoria, tema, titolo_ai). Fail-safe: tutti None su errore.
    Per fonti ufficiali la categoria è legge/provvedimento/sentenza; per editoriali non viene chiesta (sarà 'news')."""
    if not GROQ_API_KEY.startswith("gsk_"):
        return None, None, None, None, None
    api_url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}

    if e_ufficiale:
        regola_cat = '"categoria": "<legge|provvedimento|sentenza>", '
        spiega_cat = (
            "- categoria, in base all'organo emanante:\n"
            "  * \"legge\" = testi normativi (legge, decreto legge/legislativo/ministeriale, regolamento UE, "
            "direttiva, testo unico, codice), tipicamente Gazzetta Ufficiale/Normattiva/Parlamento/Governo;\n"
            "  * \"provvedimento\" = atti di autorità amministrative indipendenti (Garante, AGCOM, AGCM, IVASS, "
            "Consob, Banca d'Italia): sanzioni, ordinanze, delibere, linee guida, pareri;\n"
            "  * \"sentenza\" = pronunce di organi giurisdizionali (tribunali, Corte d'Assise, Corte Costituzionale, "
            "TAR, Consiglio di Stato, Cassazione, CGUE, Corte EDU).\n"
            "  Nel dubbio tra legge e provvedimento scegli \"provvedimento\"; se è una pronuncia di un giudice, \"sentenza\".\n"
        )
    else:
        regola_cat = ""
        spiega_cat = ""

    if serve_titolo:
        regola_tit = '"titolo": "<titolo conciso e informativo in italiano, max 12 parole>", '
        spiega_tit = ("- titolo: il feed non fornisce un titolo valido; scrivilo tu, conciso e informativo, "
                      "come lo scriverebbe una testata giuridica (niente virgolette interne).\n")
    else:
        regola_tit = ""
        spiega_tit = ""

    system_prompt = (
        "Sei un assistente legale che pre-analizza novità normative, giurisprudenziali e di settore per un team "
        "di compliance specializzato nei comparatori online italiani (finanza, assicurazioni, utility). "
        "Dato il contenuto, rispondi ESCLUSIVAMENTE con un oggetto JSON valido, senza testo prima o dopo:\n"
        '{' + regola_tit + '"riassunto": "<1-2 frasi in italiano>", "rilevanza": "<alta|media>", '
        + regola_cat +
        '"tema": "<tema giuridico principale>"}\n\n'
        "Regole:\n"
        + spiega_tit +
        "- rilevanza: \"alta\" se impatta direttamente i comparatori (sanzioni, telemarketing, consenso, "
        "trasparenza tariffaria, data breach, intermediazione); \"media\" altrimenti.\n"
        + spiega_cat +
        "- tema: tema giuridico principale. Preferisci uno tra: Privacy, Cybersecurity, Assicurativo, "
        "Bancario e finanziario, Tributario, Consumatori e pratiche commerciali, Concorrenza, Intelligenza artificiale. "
        "Se nessuno calza, indica tu il tema più appropriato in 1-3 parole."
    )
    contenuto_utente = f"Anteprima: {preview}" if serve_titolo else f"Titolo: {titolo}\n\nAnteprima: {preview}"
    payload = {
        "model": MODELLO_GROQ,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": contenuto_utente}
        ],
        "temperature": 0.1,
        "max_tokens": 800,
        "response_format": {"type": "json_object"}
    }
    if MODELLO_GROQ.startswith("openai/gpt-oss"):
        # Modelli di ragionamento: sforzo basso, il compito e' una classificazione
        # secca e i token di ragionamento consumerebbero il budget della risposta.
        payload["reasoning_effort"] = "low"
    try:
        # GESTIONE QUOTA (429). Il piano gratuito concede 8.000 token al minuto:
        # classificando decine di articoli di fila il tetto si raggiunge di sicuro.
        # Qui nessuno attende davanti allo schermo, quindi la cosa giusta e' avere
        # pazienza: rispettiamo il tempo indicato da Groq e riproviamo, invece di
        # lasciare l'articolo senza classificazione.
        r = None
        for tentativo in range(3):
            r = requests.post(api_url, headers=headers, json=payload, timeout=20)
            if r.status_code != 429:
                break
            attesa = r.headers.get("retry-after")
            try:
                attesa_sec = min(int(float(attesa)), 70) if attesa else 20
            except (TypeError, ValueError):
                attesa_sec = 20
            logging.warning("Quota AI raggiunta: attendo %ss e riprovo (%d/3).", attesa_sec, tentativo + 1)
            time.sleep(attesa_sec + 1)
        if r is None or r.status_code != 200:
            codice = r.status_code if r is not None else "assente"
            logging.error("Microriassunto: errore AI %s per '%s'", codice, titolo[:50])
            return None, None, None, None, None
        contenuto = r.json()['choices'][0]['message']['content'].strip()
        dati = json.loads(contenuto.replace("```json", "").replace("```", "").strip())
        riassunto = (dati.get("riassunto") or "").strip()[:600] or None
        rilevanza = (dati.get("rilevanza") or "").strip().lower()
        if rilevanza not in ("alta", "media"):
            rilevanza = None
        categoria = (dati.get("categoria") or "").strip().lower()
        if categoria not in ("legge", "provvedimento", "sentenza"):
            categoria = None
        tema = (dati.get("tema") or "").strip()[:100] or None
        titolo_ai = (dati.get("titolo") or "").strip()[:200] or None
        return riassunto, rilevanza, categoria, tema, titolo_ai
    except Exception as e:
        logging.error("Microriassunto fallito per '%s': %s", titolo[:50], e)
        return None, None, None, None, None


# ----------------------------------------------------------------------
# STRATEGIE DI INGESTION (coerenti con app.py)
# ----------------------------------------------------------------------
def _ingest_rss(f: Dict) -> List[Tuple]:
    """Ingestion per fonti con feed RSS. Ritorna tuple pronte per executemany."""
    risultati: List[Tuple] = []
    tf = (f.get('tipo_fonte') or 'Ufficiale').lower()
    e_ufficiale = tf != "editoriale"
    feed = feedparser.parse(f['url'])
    for entry in feed.entries[:10]:
        sommario = entry.summary if hasattr(entry, 'summary') else ""
        testo_completo = BeautifulSoup(sommario, "html.parser").get_text().strip()
        preview = testo_completo[:250] + ("..." if len(testo_completo) > 250 else "")
        titolo = pulisci_titolo(getattr(entry, 'title', '') or '')
        serve_titolo = titolo_invalido(titolo)
        data_pub = estrai_data_pubblicazione(entry)
        # All'AI il testo esteso (fino a 1200 char) per classificazione/riassunto più precisi
        time.sleep(PAUSA_FRA_CLASSIFICAZIONI)  # distribuisce il consumo di token nel minuto
        riassunto, rilevanza, categoria, tema, titolo_ai = genera_microriassunto(
            titolo, testo_completo[:1200], e_ufficiale=e_ufficiale, serve_titolo=serve_titolo)
        if serve_titolo:
            # Titolo rotto alla fonte (es. AGCM '$con.titolo1'): AI, poi prima frase del testo
            titolo = titolo_ai or titolo_da_testo(testo_completo)
        # Regola fonte -> categoria, con fallback garantito (mai vuota)
        if not e_ufficiale:
            categoria = "news"
        elif not categoria:
            categoria = "provvedimento"  # fallback sicuro per atti ufficiali
        if not tema:
            tema = f.get('area') or "Generale"
        tema = normalizza_tema(tema)
        if not rilevanza:
            rilevanza = "media"
        risultati.append((
            titolo,
            entry.link,
            preview,
            f['macro'],
            f['area'],
            f['nome'],
            riassunto,
            rilevanza,
            categoria,
            tema,
            data_pub,
        ))
    return risultati


def _ingest_scraper(f: Dict) -> List[Tuple]:
    """Ingestion per fonti senza RSS (parser HTML dedicato per fonte).

    Riconosce la fonte dal campo 'url': se punta alla ricerca provvedimenti del
    Garante, usa scraper_garante. Ogni provvedimento passa per la stessa AI degli
    altri articoli (classificazione, riassunto, rilevanza, tema), così entra nel
    flusso normale e compare nella sezione Provvedimenti del Portale.
    """
    url = (f.get('url') or '').lower()
    if 'garanteprivacy.it' not in url:
        logging.info("Fonte scraper '%s' non riconosciuta: nessun parser dedicato.", f['nome'])
        return []

    # Import locale: lo scraper è un modulo a sé, così se manca non blocca il worker.
    try:
        from scraper_garante import scarica_provvedimenti, GaranteScraperError
    except ImportError as e:
        logging.error("Modulo scraper_garante non disponibile: %s", e)
        return []

    # scarica_provvedimenti solleva GaranteScraperError se non trova nulla:
    # lo lasciamo propagare, così esegui_scansione_notturna lo registra come
    # errore visibile nella salute della fonte (allarme anti-silenzio).
    provvedimenti = scarica_provvedimenti()

    risultati: List[Tuple] = []
    for p in provvedimenti:
        titolo = p['titolo']
        link = p['link']
        estratto = p.get('estratto') or ''
        preview = estratto[:250] + ("..." if len(estratto) > 250 else "")
        # I provvedimenti del Garante sono atti ufficiali: classificazione AI come tale.
        riassunto, rilevanza, categoria, tema, _titolo_ai = genera_microriassunto(
            titolo, estratto[:1200], e_ufficiale=True, serve_titolo=False)
        if not categoria:
            categoria = "provvedimento"  # fallback sicuro
        if not tema:
            tema = f.get('area') or "Privacy"
        tema = normalizza_tema(tema)
        if not rilevanza:
            rilevanza = "media"
        risultati.append((
            titolo,
            link,
            preview,
            f['macro'],
            f['area'],
            f['nome'],
            riassunto,
            rilevanza,
            categoria,
            tema,
            p.get('data'),  # data ISO 'YYYY-MM-DD' o None
        ))
    return risultati


STRATEGIE_INGESTION = {
    "rss": _ingest_rss,
    "scraper": _ingest_scraper,
}


# Pausa fra le chiamate di classificazione: con 8.000 token/min una raffica di
# richieste esaurirebbe il budget. Mezzo secondo distribuisce il consumo senza
# allungare in modo sensibile il lavoro notturno.
PAUSA_FRA_CLASSIFICAZIONI = 0.5


def esegui_scansione_notturna() -> None:
    db_url: Optional[str] = os.getenv("DB_URL")
    if not db_url:
        logging.error("Variabile DB_URL mancante nei Secrets.")
        sys.exit(1)

    logging.info("Connessione con PostgreSQL...")
    conn = None
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cur.execute("SELECT * FROM sources")
        fonti: List[Dict] = cur.fetchall()

        if not fonti:
            logging.warning("Nessuna fonte registrata.")
            cur.close()
            conn.close()
            return

        articoli_scovati: List[Tuple] = []
        for f in fonti:
            # smista in base al tipo_ingestion; default 'rss' per retrocompatibilità
            tipo = f['tipo_ingestion'] if 'tipo_ingestion' in f.keys() and f['tipo_ingestion'] else 'rss'
            strategia = STRATEGIE_INGESTION.get(tipo, _ingest_rss)
            try:
                trovati = strategia(dict(f))
                articoli_scovati.extend(trovati)
                esito = "ok" if trovati else "vuoto"
                messaggio = f"{len(trovati)} elementi rilevati" if trovati else "Nessun elemento dal feed"
                cur.execute(
                    "UPDATE sources SET ultima_sync = CURRENT_TIMESTAMP, ultimo_esito = %s, ultimo_messaggio = %s WHERE id = %s",
                    (esito, messaggio[:500], f['id'])
                )
            except Exception as e:
                logging.error("Ingestion fallita per %s (%s): %s", f['nome'], tipo, str(e))
                cur.execute(
                    "UPDATE sources SET ultima_sync = CURRENT_TIMESTAMP, ultimo_esito = %s, ultimo_messaggio = %s WHERE id = %s",
                    ("errore", str(e)[:500], f['id'])
                )
                continue

        # Salva l'esito salute anche se non ci sono nuovi articoli
        conn.commit()

        if articoli_scovati:
            query_insert = """
                INSERT INTO articles (titolo, link, preview, macro, area, fonte, riassunto_ai, rilevanza, tipo_atto, tema, data_pubblicazione)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (link) DO NOTHING
            """
            cur.executemany(query_insert, articoli_scovati)
            conn.commit()
            logging.info("Database aggiornato: %d articoli candidati.", len(articoli_scovati))
        else:
            logging.info("Nessuna novità rilevata.")

        cur.close()
    except Exception as e:
        logging.error("Errore di runtime: %s", str(e))
        if conn:
            conn.rollback()
        sys.exit(1)
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    esegui_scansione_notturna()
