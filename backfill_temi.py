"""
Script una-tantum: normalizza il campo 'tema' degli articoli già in archivio,
consolidando le varianti sulla forma canonica (es. 'privacy ', 'Privacy e
protezione dei dati', 'Data Protection' -> 'Privacy'; 'Diritto', 'varie' ->
'Generale'). Risolve i doppioni nei menu di filtro e nei blocchi tematici.

Uso (in locale o da GitHub Actions):
    DB_URL come variabile d'ambiente, poi:
    python backfill_temi.py

Sicuro da rieseguire: aggiorna solo le righe il cui tema cambia dopo la
normalizzazione; le righe già canoniche restano intatte. Non richiede l'AI.
"""
import os
import sys
import logging
from typing import Optional

import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.StreamHandler(sys.stdout)])

DB_URL = os.getenv("DB_URL", "")


def normalizza_tema(tema: Optional[str]) -> str:
    """IDENTICA alla funzione nell'app: tenerle allineate se si aggiungono regole."""
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


def main() -> None:
    if not DB_URL:
        logging.error("DB_URL mancante.")
        sys.exit(1)
    conn = psycopg2.connect(DB_URL)
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT id, tema FROM articles")
            righe = cur.fetchall()
            aggiornati = 0
            riepilogo = {}
            for art_id, tema in righe:
                canonico = normalizza_tema(tema)
                if canonico != (tema or ""):
                    cur.execute("UPDATE articles SET tema = %s WHERE id = %s", (canonico, art_id))
                    aggiornati += 1
                    chiave = f"{tema!r} -> {canonico}"
                    riepilogo[chiave] = riepilogo.get(chiave, 0) + 1
        logging.info("Articoli totali: %d | Temi normalizzati: %d", len(righe), aggiornati)
        for k, n in sorted(riepilogo.items(), key=lambda x: -x[1]):
            logging.info("  %s  (%d)", k, n)
        logging.info("Backfill temi completato.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
