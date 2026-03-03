# -*- coding: utf-8 -*-
"""
reader_pdf_v2.py
Extracción OCR (estructura V2) para PDFs SWIFT.

Campos:
- Receiver: "Receiver: <BIC>"
- Date: robusto (YYYY-MM-DD o DD Mon YYYY)
- Amount: por anclas:
    1) "Interbank Settlement Amount:"
    2) "Instructed Amount:"
  (omite 'USD' y normaliza decimales)
- Proveedor: por ancla "Creditor:"
  (toma la PRIMERA línea con letras debajo del código, sin asumir formato del código)

Salida:
- Escribe/actualiza en el MISMO Excel, hoja "V2" (reemplaza esa hoja si existe).
"""

from __future__ import annotations

import re
import logging
from pathlib import Path
from typing import Optional, Dict, List
from datetime import datetime

import pdfplumber
import pytesseract
from PIL import Image
import pandas as pd


# =========================================================
# CONFIGURACIÓN
# =========================================================
TESSERACT_CMD = r"C:\Users\miguelz\AppData\Local\Programs\Tesseract-OCR\tesseract.exe"
pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD

OCR_LANG = "eng"
OCR_CONFIG = r"--oem 3 --psm 6"
OCR_DPI = 300


# =========================================================
# LOGGING
# =========================================================
LOGGER = logging.getLogger("reader_pdf_ocr_v2")
LOGGER.setLevel(logging.INFO)

if not LOGGER.handlers:
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("[%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    LOGGER.addHandler(handler)


# =========================================================
# REGEX (V2)
# =========================================================
# Receiver: BIC 8 u 11
RE_RECEIVER_V2 = re.compile(
    r"\bReceiver\b\s*[:;]\s*([A-Z0-9]{8}(?:[A-Z0-9]{3})?)",
    re.IGNORECASE
)

# Date
RE_ISO_DATE = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")
RE_DD_MON_YYYY = re.compile(r"\b(\d{1,2}\s+[A-Za-z]{3,}\s+\d{4})\b")
RE_DATE_LINE = re.compile(r"\bDate\b\s*[:;]?\s*(.+)$", re.IGNORECASE)

# Amount (dos anclas)
RE_IB_SETTLE_AMOUNT = re.compile(r"\bInterbank\s+Settlement\s+Amount\b\s*[:;]\s*(.+)$", re.IGNORECASE)
RE_INSTRUCTED_AMOUNT = re.compile(r"\bInstructed\s+Amount\b\s*[:;]\s*(.+)$", re.IGNORECASE)

# USD pegado o con espacio (SIN frontera final)
RE_USD_NUMBER = re.compile(r"\bUSD\s*([0-9][0-9\.,]*)", re.IGNORECASE)

# Creditor
RE_CREDITOR = re.compile(r"\bCreditor\b\s*[:;]", re.IGNORECASE)

RE_HAS_LETTERS = re.compile(r"[A-Z]", re.IGNORECASE)


# =========================================================
# UTILIDADES
# =========================================================
def normalize_line(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\u00A0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r", " ").replace("\n", " ")
    text = text.replace("\u00A0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


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


def parse_dd_mon_yyyy(raw: str) -> Optional[str]:
    if not raw:
        return None

    s = raw.strip().replace("\u00A0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    s_up = s.upper()

    m = re.match(r"^(\d{1,2})\s+([A-Z]{3,4})\s+(\d{4})$", s_up)
    if not m:
        return None

    d, mon, y = m.groups()
    mon = MONTH_FIX.get(mon, mon[:3])

    try:
        dt = datetime.strptime(f"{int(d):02d} {mon} {y}", "%d %b %Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


def normalize_amount_decimals(value: str) -> str:
    """
    - termina en '.' o ',' -> + '00'
    - 1 dígito tras '.'/',' -> + '0'
    - sin separador -> + '.00'
    Preserva separadores existentes.
    """
    if not value:
        return value

    v = value.strip().replace("\u00A0", " ")
    v = re.sub(r"\s+", "", v)

    if re.search(r"[.,]$", v):
        return v + "00"

    if re.search(r"[.,]\d$", v):
        return v + "0"

    if re.search(r"[.,]\d{2,}$", v):
        return v

    if "." not in v and "," not in v:
        return v + ".00"

    return v


# =========================================================
# EXTRACCIÓN (V2)
# =========================================================
def extract_receiver_v2(ocr_text: str, debug: bool = False) -> Optional[str]:
    if not ocr_text:
        return None

    lines = [normalize_line(ln) for ln in ocr_text.splitlines() if normalize_line(ln)]
    for ln in lines:
        m = RE_RECEIVER_V2.search(ln)
        if m:
            receiver = m.group(1).upper().strip()
            if debug:
                LOGGER.info(f"[DEBUG V2] Receiver: {repr(ln)} -> {receiver}")
            return receiver
    return None


def extract_date_v2(ocr_text: str, debug: bool = False) -> Optional[str]:
    if not ocr_text:
        return None

    lines = [normalize_line(ln) for ln in ocr_text.splitlines() if normalize_line(ln)]

    # ISO directo
    for ln in lines:
        m = RE_ISO_DATE.search(ln)
        if m:
            iso = m.group(1)
            try:
                dt = datetime.strptime(iso, "%Y-%m-%d")
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

    # Línea con Date ...
    for ln in lines:
        m = RE_DATE_LINE.search(ln)
        if m:
            tail = m.group(1).strip()

            m2 = RE_ISO_DATE.search(tail)
            if m2:
                iso = m2.group(1)
                try:
                    dt = datetime.strptime(iso, "%Y-%m-%d")
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass

            m3 = RE_DD_MON_YYYY.search(tail)
            if m3:
                parsed = parse_dd_mon_yyyy(m3.group(1))
                if debug:
                    LOGGER.info(f"[DEBUG V2] Date candidato: {repr(m3.group(1))} -> {parsed}")
                if parsed:
                    return parsed

    # DD Mon YYYY en cualquier línea
    for ln in lines:
        m = RE_DD_MON_YYYY.search(ln)
        if m:
            parsed = parse_dd_mon_yyyy(m.group(1))
            if debug:
                LOGGER.info(f"[DEBUG V2] Date candidato: {repr(m.group(1))} -> {parsed}")
            if parsed:
                return parsed

    return None


def extract_amount_v2(ocr_text: str, debug: bool = False) -> Optional[str]:
    if not ocr_text:
        return None

    lines = [normalize_line(ln) for ln in ocr_text.splitlines() if normalize_line(ln)]

    def parse_tail(tail: str) -> Optional[str]:
        if not tail:
            return None

        t = tail.replace("\u00A0", " ").strip()

        m = RE_USD_NUMBER.search(t)
        if m:
            raw_num = m.group(1)
            fixed = normalize_amount_decimals(raw_num)
            if debug:
                LOGGER.info(f"[DEBUG V2] Amount USD raw: {raw_num} -> fixed: {fixed}")
            return fixed

        # fallback: primer número plausible
        m2 = re.search(r"([0-9][0-9\.,]*)", t)
        if m2:
            raw_num = m2.group(1)
            fixed = normalize_amount_decimals(raw_num)
            if debug:
                LOGGER.info(f"[DEBUG V2] Amount num raw: {raw_num} -> fixed: {fixed}")
            return fixed

        return None

    # 1) Interbank Settlement Amount
    for ln in lines:
        m = RE_IB_SETTLE_AMOUNT.search(ln)
        if m:
            got = parse_tail(m.group(1))
            if debug:
                LOGGER.info(f"[DEBUG V2] Interbank line: {repr(ln)} -> {got}")
            if got:
                return got

    # 2) Instructed Amount (fallback)
    for ln in lines:
        m = RE_INSTRUCTED_AMOUNT.search(ln)
        if m:
            got = parse_tail(m.group(1))
            if debug:
                LOGGER.info(f"[DEBUG V2] Instructed line: {repr(ln)} -> {got}")
            if got:
                return got

    return None


def extract_supplier_from_creditor_v2(ocr_text: str, debug: bool = False) -> Optional[str]:
    """
    Proveedor V2 desde bloque 'Creditor:'
    Regla robusta (sin asumir formato del código):
      - Ubicar línea con 'Creditor:'
      - Considerar esa línea y siguientes como bloque
      - El "código" es el primer token a la derecha de ':' (si existe)
        (puede ser numérico, alfanumérico, largo o corto)
      - Proveedor = primera línea NO VACÍA con letras debajo del código
      - Fallback: si no se puede detectar el código, tomar la primera línea con letras
        debajo de 'Creditor:' ignorando vacíos.
    """
    if not ocr_text:
        return None

    lines = [normalize_line(ln) for ln in ocr_text.splitlines() if normalize_line(ln)]

    for i, ln in enumerate(lines):
        if RE_CREDITOR.search(ln):
            window = lines[i:i + 12]
            if debug:
                LOGGER.info(f"[DEBUG V2] Creditor window: {window}")

            # Intento 1: detectar si el código está en la misma línea, a la derecha de ':'
            code_found = False
            parts = re.split(r"[:;]", ln, maxsplit=1)
            if len(parts) == 2:
                right = parts[1].strip()
                # token "código" = primer grupo no vacío (sin asumir longitud ni tipo)
                code_token = right.split(" ")[0].strip() if right else ""
                if code_token:
                    code_found = True
                    if debug:
                        LOGGER.info(f"[DEBUG V2] Code token en misma línea: {code_token}")

            # Intento 2: si no hay token en misma línea, asumir que el código está en la siguiente línea
            # (no validamos estructura del código; solo tomamos la siguiente línea como "línea código" si existe)
            start_idx_for_supplier = 1  # por defecto: buscar proveedor en window[1:]
            if not code_found:
                # Si existe una línea siguiente, tratamos esa como línea código
                if len(window) >= 2:
                    start_idx_for_supplier = 2  # proveedor debajo del código (window[2] en adelante)
                    if debug:
                        LOGGER.info(f"[DEBUG V2] Code assumed en línea siguiente: {window[1]}")
                else:
                    start_idx_for_supplier = 1

            # Buscar proveedor: primera línea con letras después del "código"
            for cand in window[start_idx_for_supplier:]:
                c = cand.strip()
                if not c:
                    continue
                if not RE_HAS_LETTERS.search(c):
                    continue
                if c.upper() in {"CHINA", "TURKEY", "COLOMBIA", "PANAMA", "US", "USA"}:
                    continue

                if debug:
                    LOGGER.info(f"[DEBUG V2] Proveedor capturado: {repr(c)}")
                return c

            # Fallback extra: si todo falló, intenta primera línea con letras debajo de Creditor:
            for cand in window[1:]:
                c = cand.strip()
                if c and RE_HAS_LETTERS.search(c):
                    if c.upper() not in {"CHINA", "TURKEY", "COLOMBIA", "PANAMA", "US", "USA"}:
                        if debug:
                            LOGGER.info(f"[DEBUG V2] Proveedor fallback capturado: {repr(c)}")
                        return c

    return None


# =========================================================
# OCR POR PÁGINA
# =========================================================
def ocr_page_image(page, dpi: int = OCR_DPI) -> str:
    pil_img: Image.Image = page.to_image(resolution=dpi).original
    return pytesseract.image_to_string(pil_img, lang=OCR_LANG, config=OCR_CONFIG) or ""


def extract_data_from_pdf_v2(pdf_path: Path, debug: bool = False) -> Dict[str, Optional[str]]:
    try:
        receiver: Optional[str] = None
        date_: Optional[str] = None
        amount: Optional[str] = None
        supplier: Optional[str] = None
        pages_scanned = 0

        with pdfplumber.open(str(pdf_path)) as pdf:
            total_pages = len(pdf.pages)

            for i, page in enumerate(pdf.pages, start=1):
                pages_scanned += 1
                ocr_text = ocr_page_image(page, dpi=OCR_DPI)

                if debug:
                    sample = normalize_text(ocr_text)[:320]
                    LOGGER.info(f"[DEBUG] {pdf_path.name} | pág {i}/{total_pages} | sample: {repr(sample)}")

                if not receiver:
                    receiver = extract_receiver_v2(ocr_text, debug=debug)

                if not date_:
                    date_ = extract_date_v2(ocr_text, debug=debug)

                if not amount:
                    amount = extract_amount_v2(ocr_text, debug=debug)

                if not supplier:
                    supplier = extract_supplier_from_creditor_v2(ocr_text, debug=debug)

                if receiver and date_ and amount and supplier:
                    LOGGER.info(
                        f"Datos completos en {pdf_path.name} (pág {i}) -> "
                        f"Receiver: {receiver} | Date: {date_} | Amount: {amount} | Proveedor: {supplier}"
                    )
                    break

        if not receiver:
            LOGGER.warning(f"No se encontró Receiver (V2) en: {pdf_path.name}")
        if not date_:
            LOGGER.warning(f"No se encontró Date (V2) en: {pdf_path.name}")
        if not amount:
            LOGGER.warning(f"No se encontró Amount (V2) en: {pdf_path.name}")
        if not supplier:
            LOGGER.warning(f"No se encontró Proveedor (Creditor) (V2) en: {pdf_path.name}")

        return {
            "file_name": pdf_path.name,
            "file_path": str(pdf_path),
            "receiver": receiver,
            "date": date_,
            "amount": amount,
            "beneficiary": supplier,
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


def process_folder_v2(input_folder: Path, debug: bool = False) -> List[Dict[str, Optional[str]]]:
    pdf_files = sorted(input_folder.glob("*.pdf"))
    LOGGER.info(f"PDFs encontrados (V2): {len(pdf_files)}")

    results: List[Dict[str, Optional[str]]] = []
    for pdf_file in pdf_files:
        results.append(extract_data_from_pdf_v2(pdf_file, debug=debug))

    return results


# =========================================================
# EXPORT EXCEL (MISMO ARCHIVO, HOJA V2)
# =========================================================
def write_results_to_excel(results: List[Dict[str, Optional[str]]], output_excel_path: Path, sheet_name: str = "V2") -> None:
    rows = []
    for r in results:
        receiver = r.get("receiver")
        date_ = r.get("date")
        amount_ = r.get("amount")
        supplier = r.get("beneficiary")

        estado = "Completo" if (receiver and date_ and amount_ and supplier) else "Incompleto"

        rows.append({
            "Nombre archivo": r.get("file_name"),
            "Receiver": receiver,
            "Date": date_,
            "Amount": amount_,
            "Proveedor": supplier,
            "Estado": estado
        })

    df = pd.DataFrame(rows, columns=["Nombre archivo", "Receiver", "Date", "Amount", "Proveedor", "Estado"])
    output_excel_path.parent.mkdir(parents=True, exist_ok=True)

    if output_excel_path.exists():
        with pd.ExcelWriter(output_excel_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    else:
        with pd.ExcelWriter(output_excel_path, engine="openpyxl", mode="w") as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    LOGGER.info(f"Excel actualizado: {output_excel_path} | Hoja: {sheet_name}")


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    INPUT_FOLDER_V2 = Path(r"C:\Proyectos Comodin\Origen_Destino DIAN\pdfs V2")
    OUTPUT_EXCEL = Path(r"C:\Proyectos Comodin\Origen_Destino DIAN\resultado_extraccion.xlsx")

    DEBUG = False  # True para depurar Creditor

    results = process_folder_v2(INPUT_FOLDER_V2, debug=DEBUG)

    LOGGER.info("=== RESUMEN V2 ===")
    for r in results:
        LOGGER.info(
            f"{r['file_name']} | Receiver: {r['receiver']} | Date: {r['date']} | Amount: {r['amount']} | "
            f"Proveedor: {r['beneficiary']} | Páginas: {r['pages_scanned']}"
        )

    write_results_to_excel(results, OUTPUT_EXCEL, sheet_name="V2")