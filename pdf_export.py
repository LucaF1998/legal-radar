"""
Generazione PDF per il Legal Radar: export di un singolo articolo o di una
rassegna di articoli (i salvati). Usa fpdf2 (nessuna dipendenza di sistema).

Pensato per essere importato dall'app: le funzioni restituiscono i byte del PDF,
pronti per st.download_button.
"""
from typing import List, Dict, Optional
from datetime import datetime
from fpdf import FPDF
import re as _re

# Colori coerenti col Portale (accento blu, testo scuro)
ACCENT = (0, 113, 227)
INK = (29, 29, 31)
GREY = (110, 110, 115)


import re as _re

def pulisci_testo_completo(testo: str) -> str:
    """Ripulisce il testo grezzo estratto da una pagina web, togliendo gli elementi
    di navigazione più comuni (menu, 'apre una nuova finestra', 'seguici su', ecc.).
    NB: è un best-effort - ogni sito è diverso, quindi qualche residuo può restare.
    Meglio dell'anteprima, ma non perfetto."""
    if not testo:
        return ""
    s = _re.sub(r"\s+", " ", str(testo))
    rumore = [
        r"apre una nuova finestra", r"seguici su", r"Apri menu principale",
        r"apri_ricerca", r"Condividi(Facebook|Twitter|LinkedIn|Whatsapp)+",
        r"linkedin:", r"youtube:", r"Twitter:", r"Telegram:", r"RSS:",
        r"Lingue disponibili.*?english",
    ]
    for pat in rumore:
        s = _re.sub(pat, " ", s, flags=_re.IGNORECASE)
    # Spezzo le parole "incollate" dei menu (minuscola seguita da maiuscola)
    s = _re.sub(r"([a-zà-ù])([A-ZÀ-Ù])", r"\1 \2", s)
    s = _re.sub(r"\s{2,}", " ", s).strip()
    return s[:12000]  # taglio generoso; coerente con l'estrazione lato app


def _t(testo) -> str:
    """Rende il testo SEMPRE rappresentabile dal font PDF (Helvetica = latin-1).
    Normalizza i caratteri tipografici, RIMUOVE emoji/simboli non rappresentabili
    (invece di lasciare '?') e ripulisce la sintassi markdown (**grassetto**, #, *)
    che l'AI inserisce ma che il PDF non interpreta."""
    if testo is None:
        return ""
    s = str(testo)
    sostituzioni = {
        "\u2018": "'", "\u2019": "'", "\u201c": '"', "\u201d": '"',
        "\u2013": "-", "\u2014": "-", "\u2026": "...", "\u00a0": " ",
        "\u2022": "-", "\u00b7": "-", "\r\n": "\n", "\r": "\n", "\t": " ",
        "\u2192": "->", "\u2190": "<-", "\u20ac": "EUR",
    }
    for a, b in sostituzioni.items():
        s = s.replace(a, b)
    # Ripulisco il markdown: **grassetto** -> grassetto, ## titoli, * elenchi
    s = _re.sub(r"\*\*(.+?)\*\*", r"\1", s)   # **bold**
    s = _re.sub(r"(?m)^\s*#{1,6}\s*", "", s)   # ## titoli a inizio riga
    s = _re.sub(r"(?m)^\s*[\*\-]\s+", "- ", s)  # bullet normalizzati a "- "
    s = s.replace("**", "").replace("__", "")
    # Rimuovo caratteri di controllo (tranne newline)
    s = "".join(ch for ch in s if ch == "\n" or ord(ch) >= 32)
    # Rete di sicurezza: i caratteri non latin-1 (emoji, ideogrammi, simboli vari)
    # vengono RIMOSSI (non sostituiti con '?'), così niente "?" orfani nei titoli.
    s = s.encode("latin-1", "ignore").decode("latin-1")
    # Compatto eventuali spazi doppi lasciati dalla rimozione di emoji
    s = _re.sub(r"[ ]{2,}", " ", s)
    return s


class _PDF(FPDF):
    def header(self):
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(*GREY)
        self.cell(0, 8, _t("Legal Radar - Rassegna normativa"), align="L")
        self.ln(2)
        self.set_draw_color(220, 220, 222)
        self.line(self.l_margin, self.get_y() + 1, self.w - self.r_margin, self.get_y() + 1)
        self.ln(4)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "", 8)
        self.set_text_color(*GREY)
        self.cell(0, 10, _t(f"Pagina {self.page_no()} - generato il ")
                  + datetime.now().strftime("%d/%m/%Y"), align="C")

    def riga(self, testo: str, h: float = 5) -> None:
        """multi_cell robusta: resetta X al margine e usa larghezza esplicita,
        così non va mai in 'Not enough horizontal space' dopo altre celle."""
        self.set_x(self.l_margin)
        larghezza = self.w - self.l_margin - self.r_margin
        self.multi_cell(larghezza, h, _t(testo))


def _scrivi_articolo(pdf: _PDF, art: Dict, analisi_extra: Optional[str] = None,
                     testo_completo: Optional[str] = None) -> None:
    """Scrive un blocco-articolo nel PDF: titolo, metadati, anteprima, analisi AI,
    e (opzionale) il testo completo dell'articolo."""
    # Categoria + fonte + data come riga di metadati
    tipo = _t(art.get("tipo_atto_eff") or art.get("tipo_atto") or "")
    fonte = _t(art.get("fonte") or "")
    tema = _t(art.get("tema") or "")
    data = art.get("data_pubblicazione") or art.get("data_scansione")
    data_str = ""
    if data:
        try:
            data_str = data.strftime("%d/%m/%Y")
        except Exception:
            data_str = _t(data)

    # Titolo
    pdf.set_font("Helvetica", "B", 13)
    pdf.set_text_color(*INK)
    pdf.riga(art.get("titolo") or "(senza titolo)", 6)
    pdf.ln(1)

    # Riga metadati
    meta = " | ".join([x for x in [tipo.capitalize(), fonte, tema, data_str] if x])
    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(*ACCENT)
    pdf.riga(meta, 5)
    pdf.ln(1)

    # Anteprima
    preview = art.get("preview") or ""
    if preview:
        pdf.set_font("Helvetica", "", 10)
        pdf.set_text_color(*INK)
        pdf.riga(preview, 5)
        pdf.ln(1)

    # Riassunto AI salvato nel DB
    riass = art.get("riassunto_ai")
    if riass:
        pdf.set_font("Helvetica", "I", 10)
        pdf.set_text_color(*GREY)
        pdf.riga("Sintesi AI: " + str(riass), 5)
        pdf.ln(1)

    # Analisi strategica estesa (se passata, es. generata in sessione)
    if analisi_extra:
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_text_color(*ACCENT)
        pdf.riga("Analisi strategica", 5)
        pdf.set_font("Helvetica", "", 9)
        pdf.set_text_color(*INK)
        pdf.riga(analisi_extra, 5)
        pdf.ln(1)

    # Testo completo dell'articolo (opzionale, scaricato dalla fonte)
    if testo_completo:
        pulito = pulisci_testo_completo(testo_completo)
        if pulito:
            pdf.set_font("Helvetica", "B", 9)
            pdf.set_text_color(*ACCENT)
            pdf.riga("Testo completo", 5)
            pdf.set_font("Helvetica", "I", 7)
            pdf.set_text_color(*GREY)
            pdf.riga("(estratto automatico dalla fonte; puo' contenere residui di menu)", 4)
            pdf.set_font("Helvetica", "", 9)
            pdf.set_text_color(*INK)
            pdf.riga(pulito, 5)
            pdf.ln(1)

    # Link
    link = art.get("link")
    if link:
        pdf.set_font("Helvetica", "", 8)
        pdf.set_text_color(*ACCENT)
        pdf.riga(link, 5)

    pdf.ln(3)
    pdf.set_draw_color(230, 230, 232)
    pdf.line(pdf.l_margin, pdf.get_y(), pdf.w - pdf.r_margin, pdf.get_y())
    pdf.ln(4)


def pdf_singolo(art: Dict, analisi_extra: Optional[str] = None,
                testo_completo: Optional[str] = None) -> bytes:
    """PDF di un singolo articolo. Ritorna i byte."""
    pdf = _PDF()
    pdf.set_auto_page_break(auto=True, margin=18)
    pdf.add_page()
    _scrivi_articolo(pdf, art, analisi_extra, testo_completo)
    return bytes(pdf.output())


def pdf_rassegna(articoli: List[Dict], titolo_rassegna: str = "Rassegna dei salvati",
                 analisi_per_id: Optional[Dict] = None,
                 testo_per_id: Optional[Dict] = None) -> bytes:
    """PDF di una rassegna di articoli. Ritorna i byte.
    analisi_per_id e testo_per_id: dizionari {id_articolo: testo} per allegare
    a ciascun articolo la sua analisi AI e/o il suo testo completo."""
    analisi_per_id = analisi_per_id or {}
    testo_per_id = testo_per_id or {}
    pdf = _PDF()
    pdf.set_auto_page_break(auto=True, margin=18)
    pdf.add_page()
    # Intestazione della rassegna
    pdf.set_font("Helvetica", "B", 16)
    pdf.set_text_color(*INK)
    pdf.riga(titolo_rassegna, 8)
    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(*GREY)
    pdf.riga(f"{len(articoli)} documenti - generato il " + datetime.now().strftime("%d/%m/%Y"), 5)
    pdf.ln(5)
    for art in articoli:
        aid = art.get("id")
        _scrivi_articolo(pdf, art,
                         analisi_extra=analisi_per_id.get(aid),
                         testo_completo=testo_per_id.get(aid))
    return bytes(pdf.output())
