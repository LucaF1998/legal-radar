import os
import re
import sys
import json
import logging
from datetime import datetime
from typing import List, Tuple, Dict, Optional

import psycopg2
import psycopg2.extras
import feedparser
import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "").strip()


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


def genera_microriassunto(titolo: str, preview: str, e_ufficiale: bool = True) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Riassunto + rilevanza + categoria + tema via Groq (JSON). Fail-safe: tutti None su errore.
    Per fonti ufficiali la categoria è legge/provvedimento; per editoriali non viene chiesta (sarà 'news')."""
    if not GROQ_API_KEY.startswith("gsk_"):
        return None, None, None, None
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

    system_prompt = (
        "Sei un assistente legale che pre-analizza novità normative, giurisprudenziali e di settore per un team "
        "di compliance specializzato nei comparatori online italiani (finanza, assicurazioni, utility). "
        "Dato titolo e anteprima, rispondi ESCLUSIVAMENTE con un oggetto JSON valido, senza testo prima o dopo:\n"
        '{"riassunto": "<1-2 frasi in italiano>", "rilevanza": "<alta|media>", '
        + regola_cat +
        '"tema": "<tema giuridico principale>"}\n\n'
        "Regole:\n"
        "- rilevanza: \"alta\" se impatta direttamente i comparatori (sanzioni, telemarketing, consenso, "
        "trasparenza tariffaria, data breach, intermediazione); \"media\" altrimenti.\n"
        + spiega_cat +
        "- tema: tema giuridico principale. Preferisci uno tra: Privacy, Cybersecurity, Assicurativo, "
        "Bancario e finanziario, Tributario, Consumatori e pratiche commerciali, Concorrenza, Intelligenza artificiale. "
        "Se nessuno calza, indica tu il tema più appropriato in 1-3 parole."
    )
    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Titolo: {titolo}\n\nAnteprima: {preview}"}
        ],
        "temperature": 0.1,
        "response_format": {"type": "json_object"}
    }
    try:
        r = requests.post(api_url, headers=headers, json=payload, timeout=15)
        if r.status_code != 200:
            logging.error("Microriassunto: errore AI %s per '%s'", r.status_code, titolo[:50])
            return None, None, None, None
        dati = json.loads(r.json()['choices'][0]['message']['content'].strip())
        riassunto = (dati.get("riassunto") or "").strip()[:600] or None
        rilevanza = (dati.get("rilevanza") or "").strip().lower()
        if rilevanza not in ("alta", "media"):
            rilevanza = None
        categoria = (dati.get("categoria") or "").strip().lower()
        if categoria not in ("legge", "provvedimento", "sentenza"):
            categoria = None
        tema = (dati.get("tema") or "").strip()[:100] or None
        return riassunto, rilevanza, categoria, tema
    except Exception as e:
        logging.error("Microriassunto fallito per '%s': %s", titolo[:50], e)
        return None, None, None, None


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
        titolo = pulisci_titolo(entry.title)
        data_pub = estrai_data_pubblicazione(entry)
        # All'AI il testo esteso (fino a 1200 char) per classificazione/riassunto più precisi
        riassunto, rilevanza, categoria, tema = genera_microriassunto(titolo, testo_completo[:1200], e_ufficiale=e_ufficiale)
        # Regola fonte -> categoria, con fallback garantito (mai vuota)
        if not e_ufficiale:
            categoria = "news"
        elif not categoria:
            categoria = "provvedimento"  # fallback sicuro per atti ufficiali
        if not tema:
            tema = f.get('area') or "Generale"
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

    Punto di aggancio per fonti istituzionali che non espongono RSS.
    Ogni fonte 'scraper' richiede un parser specifico: finché non è
    implementato, non produce articoli (fail-safe, niente crash).
    """
    logging.info("Fonte '%s' di tipo scraper: parser dedicato non ancora implementato.", f['nome'])
    return []


STRATEGIE_INGESTION = {
    "rss": _ingest_rss,
    "scraper": _ingest_scraper,
}


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
