"""
Script una-tantum: riempie riassunto_ai, rilevanza, tipo_atto, tema
per gli articoli GIA' in archivio che ne sono privi.

Uso:
    DB_URL e GROQ_API_KEY come variabili d'ambiente, poi:
    python backfill_classificazione.py

Sicuro da rieseguire: elabora solo gli articoli con tipo_atto mancante.
Garantisce sempre un valore (fallback se l'AI non risponde).
"""
import os
import sys
import json
import time
import logging
from typing import Optional, Tuple

import psycopg2
import psycopg2.extras
import requests

# Modello di inferenza: llama-3.3-70b dismesso il 16/08/2026.
# Tenere allineato con MODELLO_GROQ in app.py e sync_worker.py.
MODELLO_GROQ = "openai/gpt-oss-120b"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.StreamHandler(sys.stdout)])

DB_URL = os.getenv("DB_URL", "")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "").strip()


def classifica(titolo: str, preview: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    if not GROQ_API_KEY.startswith("gsk_"):
        return None, None, None, None
    api_url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    system_prompt = (
        "Sei un assistente legale che pre-analizza novità normative, giurisprudenziali e di settore per un team "
        "di compliance specializzato nei comparatori online italiani (finanza, assicurazioni, utility). "
        "Dato titolo e anteprima, rispondi ESCLUSIVAMENTE con un oggetto JSON valido, senza testo prima o dopo:\n"
        '{"riassunto": "<1-2 frasi in italiano>", "rilevanza": "<alta|media>", '
        '"tipo_atto": "<sentenza|provvedimento|news>", "tema": "<tema giuridico principale>"}\n\n'
        "Regole:\n"
        "- rilevanza: \"alta\" se impatta direttamente i comparatori (sanzioni, telemarketing, consenso, "
        "trasparenza tariffaria, data breach, intermediazione); \"media\" altrimenti.\n"
        "- tipo_atto: \"sentenza\" per pronunce giurisdizionali; \"provvedimento\" per atti di autorità/regolatori; "
        "\"news\" per articoli giornalistici/editoriali e comunicati divulgativi.\n"
        "- tema: tema giuridico principale. Preferisci uno tra: Privacy, Cybersecurity, Assicurativo, "
        "Bancario e finanziario, Tributario, Consumatori e pratiche commerciali, Concorrenza, Intelligenza artificiale. "
        "Se nessuno calza, indica tu il tema in 1-3 parole."
    )
    payload = {
        "model": MODELLO_GROQ,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Titolo: {titolo}\n\nAnteprima: {preview}"}
        ],
        "temperature": 0.1,
        "max_tokens": 800,
        "response_format": {"type": "json_object"}
    }
    if MODELLO_GROQ.startswith("openai/gpt-oss"):
        payload["reasoning_effort"] = "low"
    try:
        r = requests.post(api_url, headers=headers, json=payload, timeout=15)
        if r.status_code != 200:
            logging.error("AI errore %s per '%s'", r.status_code, titolo[:50])
            return None, None, None, None
        dati = json.loads(r.json()['choices'][0]['message']['content'].strip())
        riassunto = (dati.get("riassunto") or "").strip()[:600] or None
        rilevanza = (dati.get("rilevanza") or "").strip().lower()
        if rilevanza not in ("alta", "media"):
            rilevanza = None
        tipo_atto = (dati.get("tipo_atto") or "").strip().lower()
        if tipo_atto not in ("sentenza", "provvedimento", "news"):
            tipo_atto = None
        tema = (dati.get("tema") or "").strip()[:100] or None
        return riassunto, rilevanza, tipo_atto, tema
    except Exception as e:
        logging.error("AI fallita per '%s': %s", titolo[:50], e)
        return None, None, None, None


def main():
    if not DB_URL:
        logging.error("DB_URL mancante.")
        sys.exit(1)

    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Articoli da classificare: quelli senza tipo_atto. Porto anche il tipo_fonte per il fallback.
    cur.execute("""
        SELECT a.id, a.titolo, a.preview, a.area, src.tipo_fonte
        FROM articles a
        LEFT JOIN sources src ON src.nome = a.fonte
        WHERE a.tipo_atto IS NULL
        ORDER BY a.id ASC
    """)
    da_fare = cur.fetchall()
    logging.info("Articoli da classificare: %d", len(da_fare))

    aggiornati = 0
    for art in da_fare:
        riassunto, rilevanza, tipo_atto, tema = classifica(art['titolo'] or "", art['preview'] or "")
        # Fallback garantito
        if not tipo_atto:
            tf = (art['tipo_fonte'] or 'Ufficiale').lower()
            tipo_atto = "news" if tf == "editoriale" else "provvedimento"
        if not tema:
            tema = art['area'] or "Generale"
        if not rilevanza:
            rilevanza = "media"

        cur.execute("""
            UPDATE articles
            SET riassunto_ai = COALESCE(riassunto_ai, %s),
                rilevanza = %s,
                tipo_atto = %s,
                tema = %s
            WHERE id = %s
        """, (riassunto, rilevanza, tipo_atto, tema, art['id']))
        conn.commit()
        aggiornati += 1
        if aggiornati % 10 == 0:
            logging.info("Classificati %d/%d...", aggiornati, len(da_fare))
        time.sleep(0.5)  # gentile coi rate limit di Groq

    cur.close()
    conn.close()
    logging.info("Completato. Articoli aggiornati: %d", aggiornati)


if __name__ == "__main__":
    main()
