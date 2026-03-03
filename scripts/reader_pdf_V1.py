# -*- coding: utf-8 -*-
"""
reader_pdf_v1.py
Extracción OCR de campos SWIFT (estructura V1):
- Receiver (cerca de "Receiver")
- Date (bloque 32A)
- Amount (bloque 32A, fallback 33B)
- Beneficiary/Proveedor (bloque 59)
Salida: Excel con UNA sola hoja llamada "V1" (sin Sheet1).
"""

from __future__ import annotations

import re
import logging
from pathlib import Path
import os
import shutil
from typing import Optional, Dict, List, Tuple
from datetime import datetime

import pdfplumber
import pytesseract
from PIL import Image
import pandas as pd


# =========================================================
# CONFIGURACIÓN
# =========================================================
def _resolve_tesseract_cmd() -> str:
    env_cmd = os.environ.get("TESSERACT_CMD")
    if env_cmd and Path(env_cmd).exists():
        return env_cmd
    which = shutil.which("tesseract")
    if which:
        return which
    candidates = [
        r"C:\Program Files\Tesseract-OCR\tesseract.exe",
        r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
        os.path.join(os.environ.get("USERPROFILE", ""), r"AppData\Local\Programs\Tesseract-OCR\tesseract.exe"),
    ]
    for c in candidates:
        if c and Path(c).exists():
            return c
    raise FileNotFoundError("No se encontró tesseract.exe. Instalar Tesseract OCR o definir TESSERACT_CMD.")

pytesseract.pytesseract.tesseract_cmd = _resolve_tesseract_cmd()

OCR_LANG = "eng"
OCR_CONFIG = r"--oem 3 --psm 6"
OCR_DPI = 300


# =========================================================
# LOGGING
# =========================================================
LOGGER = logging.getLogger("reader_pdf_ocr_v1")
LOGGER.setLevel(logging.INFO)

if not LOGGER.handlers:
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("[%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    LOGGER.addHandler(handler)


# =========================================================
# REGEX
# =========================================================
# Receiver
RE_BIC = re.compile(r"\b([A-Z0-9]{8}(?:[A-Z0-9]{3})?)\b", re.IGNORECASE)
RE_RECEIVER = re.compile(r"Recei\s*ver\s*[:;]\s*([A-Z0-9]{8}(?:[A-Z0-9]{3})?)", re.IGNORECASE)

# Anclas
RE_32A = re.compile(r"\b32A\b\s*[:;]?", re.IGNORECASE)
RE_33B = re.compile(r"\b33B\b\s*[:;]?", re.IGNORECASE)

# 59:
RE_59 = re.compile(r"(?:^|\s)59\s*[:;]", re.IGNORECASE)

# Fechas
RE_INTERBANK_ISO = re.compile(
    r"Interbank\s+Settlement\s+Date\s*[:;]\s*(\d{4}-\d{2}-\d{2})",
    re.IGNORECASE
)
RE_ISO_DATE = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")
RE_DD_MON_YYYY_ANY = re.compile(r"\b(\d{1,2}\s+[A-Za-z]{3,4}\s+[0-9A-Za-z]{4})\b")

# Amount
RE_AMOUNT_HASH_STRICT = re.compile(r"\bAmount\b\s*[:;]\s*#([^#]*)#", re.IGNORECASE)
RE_ANY_HASH_TOKEN = re.compile(r"#([^#]+)#")  # fallback si OCR rompe "Amount"

# Beneficiary heurística
RE_HAS_LETTERS = re.compile(r"[A-Z]", re.IGNORECASE)

MONTH_FIX = {
    "JAN": "JAN",
    "FEB": "FEB", "PEB": "FEB", "FEE": "FEB",
    "MAR": "MAR",
    "APR": "APR",
    "MAY": "MAY",
    "JUN": "JUN",
    "JUL": "JUL",
    "AUG": "AUG",
    "SEP": "SEP", "SEPT": "SEP",
    "OCT": "OCT",
    "NOV": "NOV",
    "DEC": "DEC",
}


# =========================================================
# UTILIDADES
# =========================================================
def normalize_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r", " ").replace("\n", " ")
    text = text.replace("\u00A0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_line(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\u00A0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_receiver_code(ocr_text: str) -> Optional[str]:
    if not ocr_text:
        return None

    norm = normalize_text(ocr_text)

    # patrón principal
    m = RE_RECEIVER.search(norm)
    if m:
        return m.group(1).upper().strip()

    # fallback por ventana
    idx = re.search(r"Recei\s*ver", norm, flags=re.IGNORECASE)
    if idx:
        start = max(0, idx.start() - 40)
        end = min(len(norm), idx.end() + 120)
        window = norm[start:end]
        m2 = RE_BIC.search(window)
        if m2:
            return m2.group(1).upper().strip()

    return None


def clean_ocr_date_token(s: str) -> str:
    s = (s or "").upper().strip()
    s = s.replace("\u00A0", " ")
    s = re.sub(r"[,\.]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def fix_ocr_year(y: str) -> str:
    y = (y or "").upper()
    y = y.replace("O", "0").replace("I", "1").replace("L", "1")
    y = y.replace("S", "5").replace("B", "8")
    return y


def parse_dd_mon_yyyy_robust(raw_date: str) -> Optional[str]:
    s = clean_ocr_date_token(raw_date)
    m = re.match(r"^([0-9]{1,2})\s+([A-Z]{3,4})\s+([0-9A-Z]{4})$", s)
    if not m:
        return None

    day, mon, year = m.groups()
    mon = MONTH_FIX.get(mon, mon[:3])
    year = fix_ocr_year(year)

    try:
        dt = datetime.strptime(f"{int(day):02d} {mon} {year}", "%d %b %Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


def fix_amount_value(amount_value: str) -> str:
    """
    Entrada sin #, ej: '30.449,84' o '26.550,'
    Salida:           '30.449,84' o '26.550,00'
    """
    v = (amount_value or "").strip()
    v = v.replace("\u00A0", " ")
    v = re.sub(r"\s+", " ", v).strip()

    if re.search(r",\s*$", v):
        v = re.sub(r",\s*$", ",00", v)

    return v


def extract_amount_from_window(window: List[str], debug: bool = False) -> Optional[str]:
    # 1) Amount : #...#
    for w in window:
        m = RE_AMOUNT_HASH_STRICT.search(w)
        if m:
            raw = m.group(1)
            fixed = fix_amount_value(raw)
            if debug:
                LOGGER.info(f"[DEBUG] Amount strict raw: {repr(raw)} -> fixed: {fixed}")
            return fixed

    # 2) Fallback: primer #...#
    for w in window:
        m2 = RE_ANY_HASH_TOKEN.search(w)
        if m2:
            raw = m2.group(1)
            fixed = fix_amount_value(raw)
            if debug:
                LOGGER.info(f"[DEBUG] Amount fallback raw: {repr(raw)} -> fixed: {fixed}")
            return fixed

    return None


def extract_date_from_32a_window(window: List[str], debug: bool = False) -> Optional[str]:
    # ISO
    for w in window:
        m_iso = RE_ISO_DATE.search(w)
        if m_iso:
            iso = m_iso.group(1)
            try:
                dt = datetime.strptime(iso, "%Y-%m-%d")
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

    # DD Mon YYYY
    for w in window:
        m = RE_DD_MON_YYYY_ANY.search(w)
        if m:
            parsed = parse_dd_mon_yyyy_robust(m.group(1))
            if debug:
                LOGGER.info(f"[DEBUG] Date candidato: {repr(m.group(1))} -> {parsed}")
            if parsed:
                return parsed

    return None


def extract_date_and_amount(ocr_text: str, debug: bool = False) -> Tuple[Optional[str], Optional[str]]:
    if not ocr_text:
        return None, None

    lines = [normalize_line(ln) for ln in ocr_text.splitlines() if normalize_line(ln)]
    if not lines:
        return None, None

    found_date: Optional[str] = None
    found_amount: Optional[str] = None

    # 32A para Date y Amount
    for i, line in enumerate(lines):
        if RE_32A.search(line):
            window32 = lines[i + 1: i + 12]
            if debug:
                LOGGER.info(f"[DEBUG] 32A -> ventana: {window32}")

            if not found_date:
                found_date = extract_date_from_32a_window(window32, debug=debug)

            if not found_amount:
                found_amount = extract_amount_from_window(window32, debug=debug)

            if found_date and found_amount:
                return found_date, found_amount

    # 33B fallback para Amount
    if not found_amount:
        for i, line in enumerate(lines):
            if RE_33B.search(line):
                window33 = lines[i + 1: i + 12]
                if debug:
                    LOGGER.info(f"[DEBUG] 33B -> ventana: {window33}")
                found_amount = extract_amount_from_window(window33, debug=debug)
                if found_amount:
                    break

    return found_date, found_amount


def extract_value_date_fallback(ocr_text: str) -> Optional[str]:
    if not ocr_text:
        return None

    lines = [normalize_line(ln) for ln in ocr_text.splitlines() if normalize_line(ln)]

    for ln in lines:
        m_iso = RE_INTERBANK_ISO.search(ln)
        if m_iso:
            iso = m_iso.group(1).strip()
            try:
                dt = datetime.strptime(iso, "%Y-%m-%d")
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

    for ln in lines:
        m = re.search(r"\bDate\b\s*[:;]\s*(.+)$", ln, flags=re.IGNORECASE)
        if m:
            parsed = parse_dd_mon_yyyy_robust(m.group(1))
            if parsed:
                return parsed

    return None


def extract_beneficiary_from_59(ocr_text: str, debug: bool = False) -> Optional[str]:
    if not ocr_text:
        return None

    lines = [normalize_line(ln) for ln in ocr_text.splitlines()]
    non_empty = [ln for ln in lines if ln]

    for i, ln in enumerate(non_empty):
        if RE_59.search(ln):
            window = non_empty[i:i + 12]

            if debug:
                LOGGER.info(f"[DEBUG] 59 encontrado. Ventana: {window}")

            for cand in window[1:]:
                c = cand.strip()

                if re.search(r"Beneficiary\s+Customer", c, flags=re.IGNORECASE):
                    continue
                if c.startswith("/"):
                    continue
                if not RE_HAS_LETTERS.search(c):
                    continue
                if c.upper() in {"CHINA", "TURKEY", "COLOMBIA", "PANAMA", "US", "USA"}:
                    continue

                if debug:
                    LOGGER.info(f"[DEBUG] Beneficiary capturado: {repr(c)}")

                return c

    return None


# =========================================================
# OCR POR PÁGINA
# =========================================================
def ocr_page_image(page, dpi: int = OCR_DPI) -> str:
    pil_img: Image.Image = page.to_image(resolution=dpi).original
    return pytesseract.image_to_string(pil_img, lang=OCR_LANG, config=OCR_CONFIG) or ""


def extract_data_from_pdf(pdf_path: Path, debug: bool = False) -> Dict[str, Optional[str]]:
    try:
        receiver: Optional[str] = None
        value_date: Optional[str] = None
        amount: Optional[str] = None
        beneficiary: Optional[str] = None
        pages_scanned = 0

        with pdfplumber.open(str(pdf_path)) as pdf:
            total_pages = len(pdf.pages)

            for i, page in enumerate(pdf.pages, start=1):
                pages_scanned += 1
                ocr_text = ocr_page_image(page, dpi=OCR_DPI)

                if debug:
                    sample = normalize_text(ocr_text)[:260]
                    LOGGER.info(f"[DEBUG] {pdf_path.name} | pág {i}/{total_pages} | sample: {repr(sample)}")

                if not receiver:
                    receiver = extract_receiver_code(ocr_text)

                if not beneficiary:
                    beneficiary = extract_beneficiary_from_59(ocr_text, debug=debug)

                if not value_date or not amount:
                    d, a = extract_date_and_amount(ocr_text, debug=debug)
                    if not value_date and d:
                        value_date = d
                    if not amount and a:
                        amount = a

                if not value_date:
                    value_date = extract_value_date_fallback(ocr_text)

                if receiver and value_date and amount and beneficiary:
                    LOGGER.info(
                        f"Datos encontrados en {pdf_path.name} (página {i}) "
                        f"-> Receiver: {receiver} | Date: {value_date} | Amount: {amount} | Beneficiary: {beneficiary}"
                    )
                    break

        return {
            "file_name": pdf_path.name,
            "file_path": str(pdf_path),
            "receiver": receiver,
            "date": value_date,
            "amount": amount,
            "beneficiary": beneficiary,
            "pages_scanned": pages_scanned
        }

    except Exception as e:
        LOGGER.error(f"Error procesando {pdf_path.name}: {e}")
        return {
            "file_name": pdf_path.name,
            "file_path": str(pdf_path),
            "receiver": None,
            "date": None,
            "amount": None,
            "beneficiary": None,
            "pages_scanned": 0
        }


def process_folder(input_folder: Path, debug: bool = False) -> List[Dict[str, Optional[str]]]:
    pdf_files = sorted(input_folder.glob("*.pdf"))
    LOGGER.info(f"PDFs encontrados: {len(pdf_files)}")

    results: List[Dict[str, Optional[str]]] = []
    for pdf_file in pdf_files:
        results.append(extract_data_from_pdf(pdf_file, debug=debug))

    return results


# =========================================================
# EXPORT EXCEL (SOLO V1, sin Sheet1)
# =========================================================
def build_output_excel(results: List[Dict[str, Optional[str]]], output_path: Path, sheet_name: str = "V1") -> None:
    """
    Genera el archivo Excel desde cero y deja SOLO una hoja: `sheet_name` (V1).
    Nota: sobrescribe el archivo completo.
    """
    rows = []
    for r in results:
        receiver = r.get("receiver")
        date_ = r.get("date")
        amount_ = r.get("amount")
        beneficiary_ = r.get("beneficiary")

        estado = "Completo" if (receiver and date_ and amount_ and beneficiary_) else "Incompleto"

        rows.append({
            "Nombre archivo": r.get("file_name"),
            "Receiver": receiver,
            "Date": date_,
            "Amount": amount_,
            "Proveedor": beneficiary_,
            "Estado": estado
        })

    df = pd.DataFrame(rows, columns=["Nombre archivo", "Receiver", "Date", "Amount", "Proveedor", "Estado"])
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # ✅ modo "w": crea un Excel nuevo con una sola hoja
    with pd.ExcelWriter(output_path, engine="openpyxl", mode="w") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)

    LOGGER.info(f"Excel generado (solo hoja {sheet_name}): {output_path}")


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    INPUT_FOLDER = Path(r"C:\Users\johangc\Desktop\Desarrollo\Origen_Destino DIAN\pdfs V1")
    OUTPUT_EXCEL = Path(r"C:\Users\johangc\Desktop\Desarrollo\Origen_Destino DIAN\resultado_extraccion.xlsx")
    DEBUG = False

    results = process_folder(INPUT_FOLDER, debug=DEBUG)

    LOGGER.info("=== RESUMEN V1 ===")
    for r in results:
        LOGGER.info(
            f"{r['file_name']} | Receiver: {r['receiver']} | Date: {r['date']} | Amount: {r['amount']} | "
            f"Beneficiary: {r['beneficiary']} | Páginas escaneadas: {r['pages_scanned']}"
        )

    build_output_excel(results, OUTPUT_EXCEL, sheet_name="V1")
