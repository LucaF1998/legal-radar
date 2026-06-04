import os
import sys
import logging
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


# ----------------------------------------------------------------------
# STRATEGIE DI INGESTION (coerenti con app.py)
# ----------------------------------------------------------------------
def _ingest_rss(f: Dict) -> List[Tuple]:
    """Ingestion per fonti con feed RSS. Ritorna tuple pronte per executemany."""
    risultati: List[Tuple] = []
    feed = feedparser.parse(f['url'])
    for entry in feed.entries[:10]:
        sommario = entry.summary if hasattr(entry, 'summary') else ""
        preview = BeautifulSoup(sommario, "html.parser").get_text()[:250] + "..."
        risultati.append((
            entry.title,
            entry.link,
            preview,
            f['macro'],
            f['area'],
            f['nome'],
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
                articoli_scovati.extend(strategia(dict(f)))
            except Exception as e:
                logging.error("Ingestion fallita per %s (%s): %s", f['nome'], tipo, str(e))
                continue

        if articoli_scovati:
            query_insert = """
                INSERT INTO articles (titolo, link, preview, macro, area, fonte)
                VALUES (%s, %s, %s, %s, %s, %s)
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
