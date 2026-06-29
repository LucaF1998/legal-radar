"""
SCRIPT DIAGNOSTICO #2 — Anatomia dell'HTML della pagina provvedimenti.

Scarica UNA volta la pagina di ricerca dei provvedimenti e stampa un estratto
strutturato, per capire come sono annidati i singoli provvedimenti (tag, classi,
dove stanno titolo / data / numero di registro / docweb / link). Serve solo a
scrivere un parser affidabile. Una sola richiesta, User-Agent identificabile.

Uso: python anatomia_garante.py
"""
import re
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "LegalRadar/1.0 (monitoraggio normativo interno; contatto: mastermonster981066@gmail.com)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "it-IT,it;q=0.9",
}
URL = "https://www.garanteprivacy.it/home/ricerca/-/search/tipologia/Provvedimenti"


def main() -> None:
    r = requests.get(URL, headers=HEADERS, timeout=25)
    print(f"STATUS: {r.status_code} | lunghezza: {len(r.text)}\n")
    if r.status_code != 200:
        print("Non accessibile, mi fermo.")
        return

    soup = BeautifulSoup(r.text, "html.parser")

    # 1) Cerco i link ai documenti docweb: sono il perno di ogni provvedimento.
    link_docweb = soup.find_all("a", href=re.compile(r"docweb-display/docweb/\d+"))
    print(f"--- Trovati {len(link_docweb)} link a documenti docweb ---\n")

    # 2) Per i primi 3, stampo il link, il testo, e il blocco HTML del contenitore
    #    "genitore" (il riquadro che racchiude un singolo provvedimento), così
    #    vedo dove stanno data, numero di registro, anteprima.
    for i, a in enumerate(link_docweb[:3]):
        print(f"========== PROVVEDIMENTO #{i+1} ==========")
        print("href:", a.get("href"))
        print("testo del link:", a.get_text(strip=True)[:200])
        # Risalgo di un paio di livelli per vedere il contenitore della voce
        contenitore = a
        for _ in range(3):
            if contenitore.parent:
                contenitore = contenitore.parent
        # Stampo l'HTML del contenitore, accorciato, per leggerne la struttura
        html_contenitore = str(contenitore)
        print("--- HTML del contenitore (primi 1200 char) ---")
        print(html_contenitore[:1200])
        print("...\n")

    # 3) Indizi sui pattern testuali utili al parser
    testo = soup.get_text(" ", strip=True)
    reg = re.findall(r"Registro dei provvedimenti n\.\s*\d+\s*del\s*\d+\s+\w+\s+\d{4}", testo)
    print(f"--- Esempi di 'Registro dei provvedimenti' trovati: {len(reg)} ---")
    for r_ in reg[:5]:
        print("  ", r_)

    # 4) Verifico se c'è una paginazione (per scorrere oltre la prima pagina)
    print("\n--- Indizi di paginazione ---")
    for a in soup.find_all("a", href=True):
        if "cur=" in a["href"] or "page" in a["href"].lower():
            print("  link paginazione:", a["href"][:160])


if __name__ == "__main__":
    main()
