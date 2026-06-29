"""
Scraper dei provvedimenti del Garante Privacy.

Legge la pagina pubblica di ricerca dei provvedimenti e ne estrae l'elenco
(titolo, data, numero di registro, codice docweb, link, anteprima). Pensato per
essere richiamato UNA VOLTA AL GIORNO dal worker notturno.

Principi:
- Cortesia: User-Agent identificabile, timeout generosi, pausa tra le pagine,
  numero di pagine limitato (le ultime N, sufficienti a coprire le novità del giorno).
- Robustezza con allarme: se non trova NESSUN provvedimento (segno che il sito è
  cambiato o ci blocca), solleva GaranteScraperError invece di restituire una lista
  vuota silenziosa. Così il worker se ne accorge e puo avvisare.

Dipendenze: requests, beautifulsoup4
"""
import re
import logging
from typing import List, Dict
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

BASE = "https://www.garanteprivacy.it"
# Pagina di ricerca dei provvedimenti (ordinata per data, i piu recenti in cima).
URL_PROVVEDIMENTI = "https://www.garanteprivacy.it/home/ricerca/-/search/tipologia/Provvedimenti"

HEADERS = {
    # User-Agent onesto: dichiariamo chi siamo. Sostituire l'email col contatto reale.
    "User-Agent": "LegalRadar/1.0 (monitoraggio normativo interno; contatto: mastermonster981066@gmail.com)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "it-IT,it;q=0.9",
}

MESI = {
    "gennaio": 1, "febbraio": 2, "marzo": 3, "aprile": 4, "maggio": 5, "giugno": 6,
    "luglio": 7, "agosto": 8, "settembre": 9, "ottobre": 10, "novembre": 11, "dicembre": 12,
}


class GaranteScraperError(Exception):
    """Sollevata quando lo scraping non produce risultati: probabile blocco o sito cambiato."""


def _data_iso(testo_data: str):
    """Converte '28 maggio 2026' in '2026-05-28'. None se non riconosciuta."""
    if not testo_data:
        return None
    m = re.match(r"(\d{1,2})\s+(\w+)\s+(\d{4})", testo_data.strip().lower())
    if not m:
        return None
    g, mese, a = m.group(1), m.group(2), m.group(3)
    if mese not in MESI:
        return None
    return f"{a}-{MESI[mese]:02d}-{int(g):02d}"


def _parse_pagina(html: str) -> List[Dict]:
    """Estrae i provvedimenti da una pagina. Filtra le voci di menu via classe CSS."""
    soup = BeautifulSoup(html, "html.parser")
    risultati = []
    # I provvedimenti veri hanno SEMPRE la classe 'titolo-risultato' (le voci di menu no).
    for a in soup.select("a.titolo-risultato"):
        href = a.get("href", "")
        m = re.search(r"docweb/(\d+)", href)
        if not m:
            continue
        docweb = m.group(1)
        titolo = (a.get("title") or a.get_text(strip=True)).strip()
        link = href if href.startswith("http") else urljoin(BASE, href)

        estratto = ""
        cont = a.find_parent("div")
        if cont:
            p = cont.find("p", class_="estratto-risultato")
            if p:
                estratto = p.get_text(" ", strip=True)

        # Numero di registro + data dall'estratto ("Registro dei provvedimenti n. 377 del 28 maggio 2026")
        reg = re.search(r"Registro dei provvedimenti n\.\s*(\d+)\s*del\s*(\d{1,2}\s+\w+\s+\d{4})", estratto)
        if reg:
            num_reg = reg.group(1)
            data_txt = reg.group(2)
        else:
            # Fallback 1: numero di registro senza data accanto
            solo_num = re.search(r"Registro dei provvedimenti n\.\s*(\d+)", estratto)
            num_reg = solo_num.group(1) if solo_num else None
            # Fallback 2: data dal titolo
            dm = re.search(r"(\d{1,2}\s+\w+\s+\d{4})", titolo)
            data_txt = dm.group(1) if dm else None

        risultati.append({
            "docweb": docweb,
            "titolo": titolo,
            "link": link,
            "num_registro": num_reg,
            "data": _data_iso(data_txt),
            "estratto": estratto,
        })
    return risultati


def scarica_provvedimenti(pausa_sec: float = 0.0) -> List[Dict]:
    """Scarica i provvedimenti piu recenti dalla pagina di ricerca del Garante.
    Per il giro quotidiano basta la prima pagina (i piu recenti stanno in cima, e
    l'idempotenza del DB evita i doppioni). Solleva GaranteScraperError se non trova
    nulla (allarme anti-silenzio: blocco o sito cambiato)."""
    try:
        r = requests.get(URL_PROVVEDIMENTI, headers=HEADERS, timeout=30)
    except Exception as e:
        raise GaranteScraperError(f"Errore di rete verso il Garante: {e}")
    if r.status_code != 200:
        raise GaranteScraperError(f"Il Garante ha risposto con status {r.status_code} (atteso 200).")

    provvedimenti = _parse_pagina(r.text)
    logging.info("Provvedimenti estratti: %d", len(provvedimenti))

    # ALLARME ANTI-SILENZIO: zero risultati = qualcosa non va (blocco o sito cambiato).
    if not provvedimenti:
        raise GaranteScraperError(
            "Nessun provvedimento estratto dal sito del Garante. "
            "Possibile blocco (403) o cambio della struttura della pagina."
        )
    return provvedimenti


if __name__ == "__main__":
    # Esecuzione diretta = prova manuale: stampa quello che troverebbe.
    try:
        prov = scarica_provvedimenti()
        for p in prov:
            print(f"[{p['data']}] reg n.{p['num_registro']} docweb {p['docweb']}")
            print(f"   {p['titolo'][:90]}")
            print(f"   {p['link']}")
        print(f"\nTotale: {len(prov)} provvedimenti.")
    except GaranteScraperError as e:
        print("ALLARME:", e)
