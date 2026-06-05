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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.StreamHandler(sys.stdout)])

DB_URL = os.getenv("DB_URL", "")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "").strip()


def classifica(titolo: str, preview: str, e_ufficiale: bool = True) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
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
        "Se nessuno calza, indica tu il tema in 1-3 parole."
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
            logging.error("AI errore %s per '%s'", r.status_code, titolo[:50])
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
        logging.error("AI fallita per '%s': %s", titolo[:50], e)
        return None, None, None, None


def main():
    if not DB_URL:
        logging.error("DB_URL mancante.")
        sys.exit(1)

    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Da classificare: articoli senza categoria, OPPURE editoriali classificati male (non-news).
    cur.execute("""
        SELECT a.id, a.titolo, a.preview, a.area, src.tipo_fonte
        FROM articles a
        LEFT JOIN sources src ON src.nome = a.fonte
        WHERE a.tipo_atto IS NULL
           OR (LOWER(COALESCE(src.tipo_fonte,'')) = 'editoriale' AND a.tipo_atto <> 'news')
        ORDER BY a.id ASC
    """)
    da_fare = cur.fetchall()
    logging.info("Articoli da classificare: %d", len(da_fare))

    aggiornati = 0
    for art in da_fare:
        tf = (art['tipo_fonte'] or 'Ufficiale').lower()
        e_ufficiale = tf != "editoriale"
        riassunto, rilevanza, categoria, tema = classifica(art['titolo'] or "", art['preview'] or "", e_ufficiale=e_ufficiale)
        # Regola fonte -> categoria, con fallback garantito (mai vuota)
        if not e_ufficiale:
            categoria = "news"
        elif not categoria:
            categoria = "provvedimento"
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
        """, (riassunto, rilevanza, categoria, tema, art['id']))
        conn.commit()
        aggiornati += 1
        if aggiornati % 10 == 0:
            logging.info("Classificati %d/%d...", aggiornati, len(da_fare))
        time.sleep(0.5)

    cur.close()
    conn.close()
    logging.info("Completato. Articoli aggiornati: %d", aggiornati)


if __name__ == "__main__":
    main()
