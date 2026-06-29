"""
SCRIPT DIAGNOSTICO — Accessibilità degli URL del Garante Privacy.

NON è uno scraper. Fa UNA richiesta educata a ciascun tipo di URL e riporta
l'esito (status HTTP, dimensione risposta, indizi di contenuto), così da capire
se l'ambiente reale (Render/GitHub Actions/locale) riesce ad accedere o riceve
un blocco (403). In base a questo decidiamo se costruire o no lo scraper.

Principi di cortesia: un solo accesso per URL, pausa di 5 secondi tra una
richiesta e l'altra, User-Agent identificabile e onesto.

Uso:
    python diagnostica_garante.py
"""
import time
import requests

# User-Agent onesto e identificabile: dichiariamo chi siamo, niente travestimenti.
HEADERS = {
    "User-Agent": "LegalRadar/1.0 (monitoraggio normativo interno; contatto: mastermonster981066@gmail.com)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "it-IT,it;q=0.9",
}

# I tre tipi di URL che ci interessano, dal più importante al meno.
URL_DA_TESTARE = [
    ("Ricerca 'key/PROVVEDIMENTI' (indice provvedimenti)",
     "https://www.garanteprivacy.it/home/ricerca/-/search/key/PROVVEDIMENTI"),
    ("Ricerca 'tipologia/Provvedimenti' (pagina che dava 403 nei test)",
     "https://www.garanteprivacy.it/home/ricerca/-/search/tipologia/Provvedimenti"),
    ("Singolo provvedimento (pagina docweb di esempio)",
     "https://www.garanteprivacy.it/home/docweb/-/docweb-display/docweb/9899929"),
    ("Home del sito (per capire se il blocco è generale)",
     "https://www.garanteprivacy.it/home"),
]


def prova(nome: str, url: str) -> None:
    print(f"\n=== {nome} ===")
    print(f"URL: {url}")
    try:
        r = requests.get(url, headers=HEADERS, timeout=25)
        testo = r.text or ""
        # Indizi di contenuto utile: codici docweb e la parola 'provvedimento'
        n_docweb = testo.count("docweb-display") + testo.count("doc. web")
        n_provv = testo.lower().count("provvedimento")
        n_registro = testo.lower().count("registro dei provvedimenti")
        print(f"  STATUS: {r.status_code}")
        print(f"  Dimensione risposta: {len(testo)} caratteri")
        print(f"  Indizi -> 'docweb': {n_docweb} | 'provvedimento': {n_provv} | 'registro dei provvedimenti': {n_registro}")
        if r.status_code == 200 and (n_docweb > 0 or n_registro > 0):
            print("  ESITO: ACCESSIBILE e con contenuto utile ✓")
        elif r.status_code == 200:
            print("  ESITO: risponde 200 ma senza contenuto utile (probabile caricamento JS)")
        elif r.status_code == 403:
            print("  ESITO: BLOCCATO (403) ✗  — il server rifiuta l'accesso automatico")
        else:
            print(f"  ESITO: status inatteso {r.status_code}")
    except Exception as e:
        print(f"  ERRORE di connessione: {e}")


def main() -> None:
    print("Diagnostica accessibilità Garante Privacy — una richiesta per URL, 5s di pausa.")
    for i, (nome, url) in enumerate(URL_DA_TESTARE):
        prova(nome, url)
        if i < len(URL_DA_TESTARE) - 1:
            time.sleep(5)  # cortesia verso il server
    print("\n--- Fine diagnostica ---")
    print("Se vedi 403 ovunque: il sito blocca gli accessi automatici, ci fermiamo qui.")
    print("Se le pagine docweb sono ACCESSIBILI: c'è una strada percorribile, ne riparliamo.")


if __name__ == "__main__":
    main()
