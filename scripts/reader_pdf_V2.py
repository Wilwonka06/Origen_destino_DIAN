# -*- coding: utf-8 -*-
"""
reader_pdf_V2.py — Extracción OCR de PDFs SWIFT estructura V2

Campos extraídos:
  - Receiver  → ancla "Receiver: <BIC>"
  - Date      → ISO directo, ancla "Date:", DD Mon YYYY
  - Amount    → ancla "Interbank Settlement Amount:" (fallback "Instructed Amount:")
  - Proveedor → ancla "Creditor:" (primera línea con letras debajo del código)

CAMBIOS vs versión anterior:
  - Eliminado: _resolve_tesseract_cmd() duplicado → usa core.ocr_engine
  - Eliminado: setup de logging propio            → usa core.logger
  - Eliminado: normalize_line / normalize_text    → implementación local _normalize_line
  - Mantenida: toda la lógica de regex y extracción (sin cambios funcionales)
  - Agregado:  soporte de caché en process_folder_v2
  - Agregado:  campo "estado" en el dict de retorno
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

import config
from core.logger import get_logger
from core.ocr_engine import get_ocr_engine

LOGGER = get_logger(__name__)


# =========================================================
# REGEX V2 — sin cambios respecto a versión original
# =========================================================
RE_RECEIVER_V2 = re.compile(
    r"\bReceiver\b\s*[:;]\s*([A-Z0-9]{8}(?:[A-Z0-9]{3})?)\b",
    re.IGNORECASE,
)

RE_ISO_DATE        = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")
RE_DD_MON_YYYY     = re.compile(r"\b(\d{1,2}\s+[A-Za-z]{3,}\s+\d{4})\b")
RE_DATE_LINE       = re.compile(r"\bDate\b\s*[:;]?\s*(.+)$", re.IGNORECASE)

RE_IB_SETTLE_AMOUNT  = re.compile(
    r"\bInterbank\s+Settlement\s+Amount\b\s*[:;]\s*(.+)$", re.IGNORECASE
)
RE_INSTRUCTED_AMOUNT = re.compile(
    r"\bInstructed\s+Amount\b\s*[:;]\s*(.+)$", re.IGNORECASE
)
RE_USD_NUMBER = re.compile(r"\bUSD\s*([0-9][0-9\.,]*)", re.IGNORECASE)

RE_CREDITOR    = re.compile(r"\bCreditor\b\s*[:;]", re.IGNORECASE)
RE_HAS_LETTERS = re.compile(r"[A-Z]", re.IGNORECASE)

MONTH_FIX = {
    "JAN": "JAN",
    "FEB": "FEB", "PEB": "FEB", "FEE": "FEB",
    "MAR": "MAR", "APR": "APR", "MAY": "MAY",
    "JUN": "JUN", "JUL": "JUL", "AUG": "AUG",
    "SEP": "SEP", "SEPT": "SEP",
    "OCT": "OCT", "NOV": "NOV", "DEC": "DEC",
}

_COUNTRY_BLACKLIST = {"CHINA", "TURKEY", "COLOMBIA", "PANAMA", "US", "USA"}


# =========================================================
# UTILIDADES DE TEXTO
# =========================================================
def _normalize_line(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.replace("\u00A0", " ")).strip()


def _normalize_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r", " ").replace("\n", " ").replace("\u00A0", " ")
    return re.sub(r"\s+", " ", text).strip()


# =========================================================
# PARSEO DE FECHAS
# =========================================================
def _parse_dd_mon_yyyy(raw: str) -> Optional[str]:
    """Parsea '10 Apr 2025' a '2025-04-10', con correcciones de OCR."""
    if not raw:
        return None
    s = re.sub(r"\s+", " ", raw.strip().replace("\u00A0", " ")).strip()
    m = re.match(r"^(\d{1,2})\s+([A-Za-z]{3,4})\s+(\d{4})$", s)
    if not m:
        return None
    d, mon_raw, y = m.groups()
    mon = MONTH_FIX.get(mon_raw.upper()[:4], MONTH_FIX.get(mon_raw.upper()[:3]))
    if not mon:
        return None
    try:
        dt = datetime.strptime(f"{int(d):02d} {mon} {y}", "%d %b %Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


# =========================================================
# NORMALIZACIÓN DE MONTOS (V2)
# =========================================================
def _normalize_amount_decimals(value: str) -> str:
    """
    Asegura que el monto tenga exactamente 2 decimales.
    - Termina en '.' o ','       → + '00'
    - 1 dígito tras separador   → + '0'
    - Sin separador              → + '.00'
    - 2+ dígitos tras separador → sin cambio
    """
    if not value:
        return value

    v = re.sub(r"\s+", "", value.strip().replace("\u00A0", " "))

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
# EXTRACCIÓN DE CAMPOS V2
# =========================================================
def _extract_receiver_v2(ocr_text: str, debug: bool = False) -> Optional[str]:
    if not ocr_text:
        return None
    lines = [_normalize_line(ln) for ln in ocr_text.splitlines() if _normalize_line(ln)]
    for ln in lines:
        m = RE_RECEIVER_V2.search(ln)
        if m:
            receiver = m.group(1).upper().strip()
            if debug:
                LOGGER.debug(f"[V2] Receiver: {repr(ln)} → {receiver}")
            return receiver
    return None


def _extract_date_v2(ocr_text: str, debug: bool = False) -> Optional[str]:
    if not ocr_text:
        return None
    lines = [_normalize_line(ln) for ln in ocr_text.splitlines() if _normalize_line(ln)]

    # 1) ISO directo en cualquier línea
    for ln in lines:
        m = RE_ISO_DATE.search(ln)
        if m:
            try:
                dt = datetime.strptime(m.group(1), "%Y-%m-%d")
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

    # 2) Línea con "Date ..."
    for ln in lines:
        m = RE_DATE_LINE.search(ln)
        if m:
            tail = m.group(1).strip()
            # ISO dentro de la línea Date
            m2 = RE_ISO_DATE.search(tail)
            if m2:
                try:
                    dt = datetime.strptime(m2.group(1), "%Y-%m-%d")
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass
            # DD Mon YYYY dentro de la línea Date
            m3 = RE_DD_MON_YYYY.search(tail)
            if m3:
                parsed = _parse_dd_mon_yyyy(m3.group(1))
                if debug:
                    LOGGER.debug(f"[V2] Date candidato: {repr(m3.group(1))} → {parsed}")
                if parsed:
                    return parsed

    # 3) DD Mon YYYY en cualquier línea
    for ln in lines:
        m = RE_DD_MON_YYYY.search(ln)
        if m:
            parsed = _parse_dd_mon_yyyy(m.group(1))
            if debug:
                LOGGER.debug(f"[V2] Date candidato: {repr(m.group(1))} → {parsed}")
            if parsed:
                return parsed

    return None


def _extract_amount_v2(ocr_text: str, debug: bool = False) -> Optional[str]:
    if not ocr_text:
        return None
    lines = [_normalize_line(ln) for ln in ocr_text.splitlines() if _normalize_line(ln)]

    def _parse_tail(tail: str) -> Optional[str]:
        if not tail:
            return None
        t = tail.replace("\u00A0", " ").strip()

        # USD + número
        m = RE_USD_NUMBER.search(t)
        if m:
            raw_num = m.group(1)
            fixed = _normalize_amount_decimals(raw_num)
            if debug:
                LOGGER.debug(f"[V2] Amount USD raw:{raw_num} → {fixed}")
            return fixed

        # Primer número plausible
        m2 = re.search(r"([0-9][0-9\.,]*)", t)
        if m2:
            raw_num = m2.group(1)
            fixed = _normalize_amount_decimals(raw_num)
            if debug:
                LOGGER.debug(f"[V2] Amount num raw:{raw_num} → {fixed}")
            return fixed

        return None

    # Ancla 1: Interbank Settlement Amount
    for ln in lines:
        m = RE_IB_SETTLE_AMOUNT.search(ln)
        if m:
            got = _parse_tail(m.group(1))
            if debug:
                LOGGER.debug(f"[V2] Interbank line: {repr(ln)} → {got}")
            if got:
                return got

    # Ancla 2: Instructed Amount (fallback)
    for ln in lines:
        m = RE_INSTRUCTED_AMOUNT.search(ln)
        if m:
            got = _parse_tail(m.group(1))
            if debug:
                LOGGER.debug(f"[V2] Instructed line: {repr(ln)} → {got}")
            if got:
                return got

    return None


def _extract_supplier_from_creditor_v2(ocr_text: str, debug: bool = False) -> Optional[str]:
    """
    Busca el proveedor debajo de la ancla 'Creditor:'.

    Estrategia robusta (sin asumir formato del código):
      1. Localizar línea con 'Creditor:'
      2. Detectar si el código está en la misma línea (a la derecha del ':')
      3. Si no, asumir que el código está en la siguiente línea
      4. Proveedor = primera línea con letras después del código
      5. Fallback: primera línea con letras debajo de 'Creditor:'
    """
    if not ocr_text:
        return None

    lines     = [_normalize_line(ln) for ln in ocr_text.splitlines() if _normalize_line(ln)]

    for i, ln in enumerate(lines):
        if not RE_CREDITOR.search(ln):
            continue

        window = lines[i: i + 12]
        if debug:
            LOGGER.debug(f"[V2] Creditor window: {window}")

        # ¿Hay código en la misma línea?
        code_found = False
        start_idx  = 1   # por defecto: proveedor desde window[1]

        parts = re.split(r"[:;]", ln, maxsplit=1)
        if len(parts) == 2:
            right = parts[1].strip()
            code_token = right.split(" ")[0].strip() if right else ""
            if code_token:
                code_found = True

        if not code_found and len(window) >= 2:
            # El código es la siguiente línea; proveedor desde window[2]
            start_idx = 2
            if debug:
                LOGGER.debug(f"[V2] Code asumido en línea siguiente: {window[1]}")

        # Buscar proveedor
        for cand in window[start_idx:]:
            c = cand.strip()
            if not c:
                continue
            if not RE_HAS_LETTERS.search(c):
                continue
            if c.upper() in _COUNTRY_BLACKLIST:
                continue
            if debug:
                LOGGER.debug(f"[V2] Proveedor capturado: {repr(c)}")
            return c

        # Fallback final: primera línea con letras desde window[1]
        for cand in window[1:]:
            c = cand.strip()
            if c and RE_HAS_LETTERS.search(c):
                if c.upper() not in _COUNTRY_BLACKLIST:
                    if debug:
                        LOGGER.debug(f"[V2] Proveedor fallback: {repr(c)}")
                    return c

    return None


# =========================================================
# PROCESAMIENTO DE UN SOLO PDF
# =========================================================
def extract_data_from_pdf_v2(pdf_path: Path, debug: bool = False) -> Dict:
    """
    Extrae los 4 campos SWIFT de un PDF con estructura V2.

    Retorna dict con:
        file_name, file_path, receiver, date, amount, beneficiary,
        pages_scanned, estado ("Completo" | "Incompleto" | "Error")
    """
    ocr = get_ocr_engine()
    resultado_base = {
        "file_name":     pdf_path.name,
        "file_path":     str(pdf_path),
        "receiver":      None,
        "date":          None,
        "amount":        None,
        "beneficiary":   None,
        "pages_scanned": 0,
        "estado":        "Error",
    }

    try:
        pages_text = ocr.extract_text_from_pdf(pdf_path, debug=debug)
        receiver: Optional[str] = None
        date_:    Optional[str] = None
        amount:   Optional[str] = None
        supplier: Optional[str] = None

        for i, ocr_text in enumerate(pages_text, start=1):
            resultado_base["pages_scanned"] = i

            if not receiver:
                receiver = _extract_receiver_v2(ocr_text, debug=debug)
            if not date_:
                date_ = _extract_date_v2(ocr_text, debug=debug)
            if not amount:
                amount = _extract_amount_v2(ocr_text, debug=debug)
            if not supplier:
                supplier = _extract_supplier_from_creditor_v2(ocr_text, debug=debug)

            if receiver and date_ and amount and supplier:
                LOGGER.info(
                    f"[V2] {pdf_path.name} (pág {i}) → "
                    f"Receiver:{receiver} | Date:{date_} | "
                    f"Amount:{amount} | Proveedor:{supplier}"
                )
                break

        estado = "Completo" if (receiver and date_ and amount and supplier) else "Incompleto"

        if estado == "Incompleto":
            campos_faltantes = [
                f for f, v in [
                    ("Receiver", receiver), ("Date", date_),
                    ("Amount", amount), ("Proveedor", supplier)
                ] if not v
            ]
            LOGGER.warning(f"[V2] {pdf_path.name} → Incompleto. Falta: {campos_faltantes}")

        return {
            **resultado_base,
            "receiver":    receiver,
            "date":        date_,
            "amount":      amount,
            "beneficiary": supplier,
            "estado":      estado,
        }

    except Exception as e:
        LOGGER.error(f"[V2] Error procesando {pdf_path.name}: {e}", exc_info=True)
        return {**resultado_base, "estado": "Error"}


# =========================================================
# PROCESAMIENTO DE CARPETA
# =========================================================
def process_folder_v2(
    input_folder,         # Path (carpeta) o List[Path] (lista de archivos)
    debug: bool = False,
    cache=None,           # Optional[PdfCache]
) -> List[Dict]:
    """
    Procesa PDFs con estructura V2.

    input_folder puede ser:
      - Path de carpeta plana  → busca *.pdf dentro de ella (comportamiento original)
      - List[Path] de archivos → los usa directamente (para fuente de red con subcarpetas)

    Si se pasa un objeto cache (core.cache.PdfCache), omite los PDFs
    ya procesados y registra los nuevos al terminar.
    """
    if isinstance(input_folder, list):
        pdf_files = sorted(input_folder, key=lambda p: p.name)
        if cache is not None:
            pdf_files = [p for p in pdf_files if not cache.is_processed(p)]
    elif cache is not None:
        pdf_files = cache.pending_files(input_folder, version="V2")
    else:
        pdf_files = sorted(input_folder.glob("*.pdf"))

    LOGGER.info(f"[V2] PDFs a procesar: {len(pdf_files)}")

    results: List[Dict] = []
    for pdf_file in pdf_files:
        result = extract_data_from_pdf_v2(pdf_file, debug=debug)
        results.append(result)

        if cache is not None:
            cache.mark(pdf_file, version="V2", estado=result["estado"])

    return results


# =========================================================
# EXPORT EXCEL (usado cuando se ejecuta standalone)
# =========================================================
def write_results_to_excel(
    results: List[Dict],
    output_excel_path: Path,
    sheet_name: str = "V2",
) -> None:
    """Escribe/actualiza hoja V2 en el Excel de resultados."""
    rows = []
    for r in results:
        rows.append({
            "Nombre archivo": r.get("file_name"),
            "Receiver":       r.get("receiver"),
            "Date":           r.get("date"),
            "Amount":         r.get("amount"),
            "Proveedor":      r.get("beneficiary"),
            "Estado":         r.get("estado", "Incompleto"),
        })

    df = pd.DataFrame(rows, columns=["Nombre archivo", "Receiver", "Date", "Amount", "Proveedor", "Estado"])
    output_excel_path.parent.mkdir(parents=True, exist_ok=True)

    mode       = "a" if output_excel_path.exists() else "w"
    extra_args = {"if_sheet_exists": "replace"} if mode == "a" else {}

    with pd.ExcelWriter(output_excel_path, engine="openpyxl", mode=mode, **extra_args) as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)

    LOGGER.info(f"[V2] Excel actualizado: {output_excel_path} (hoja: {sheet_name})")


# =========================================================
# MAIN — ejecución standalone para pruebas
# =========================================================
if __name__ == "__main__":
    results = process_folder_v2(config.DIR_PDFS_V2, debug=config.DEBUG)

    LOGGER.info("=== RESUMEN V2 ===")
    for r in results:
        LOGGER.info(
            f"{r['file_name']} | Receiver:{r['receiver']} | Date:{r['date']} | "
            f"Amount:{r['amount']} | Proveedor:{r['beneficiary']} | "
            f"Páginas:{r['pages_scanned']} | Estado:{r['estado']}"
        )