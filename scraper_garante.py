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
import time
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
        num_reg = reg.group(1) if reg else None
        data_txt = reg.group(2) if reg else None
        if not data_txt:
            # Fallback: data dal titolo
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


def scarica_provvedimenti(max_pagine: int = 2, pausa_sec: float = 5.0) -> List[Dict]:
    """Scarica i provvedimenti dalle prime `max_pagine` pagine di risultati.
    Per il giro quotidiano bastano 1-2 pagine (i piu recenti stanno in cima).
    Solleva GaranteScraperError se non trova nulla (allarme anti-silenzio)."""
    tutti: List[Dict] = []
    visti = set()
    for pagina in range(1, max_pagine + 1):
        # La prima pagina e l'URL semplice; per le successive si usa il parametro cur.
        if pagina == 1:
            url = URL_PROVVEDIMENTI
        else:
            url = f"{URL_PROVVEDIMENTI}?cur={pagina}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
        except Exception as e:
            logging.error("Errore di rete su pagina %d: %s", pagina, e)
            break
        if r.status_code != 200:
            logging.warning("Pagina %d: status %d, interrompo.", pagina, r.status_code)
            break
        voci = _parse_pagina(r.text)
        logging.info("Pagina %d: %d provvedimenti.", pagina, len(voci))
        for v in voci:
            if v["docweb"] not in visti:
                visti.add(v["docweb"])
                tutti.append(v)
        if pagina < max_pagine:
            time.sleep(pausa_sec)  # cortesia verso il server

    # ALLARME ANTI-SILENZIO: zero risultati = qualcosa non va (blocco o sito cambiato).
    if not tutti:
        raise GaranteScraperError(
            "Nessun provvedimento estratto dal sito del Garante. "
            "Possibile blocco (403) o cambio della struttura della pagina."
        )
    logging.info("Totale provvedimenti unici raccolti: %d", len(tutti))
    return tutti


if __name__ == "__main__":
    # Esecuzione diretta = prova manuale: stampa quello che troverebbe.
    try:
        prov = scarica_provvedimenti(max_pagine=2)
        for p in prov[:10]:
            print(f"[{p['data']}] reg n.{p['num_registro']} docweb {p['docweb']}")
            print(f"   {p['titolo'][:90]}")
            print(f"   {p['link']}")
        print(f"\nTotale: {len(prov)} provvedimenti.")
    except GaranteScraperError as e:
        print("ALLARME:", e)
