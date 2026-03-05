# -*- coding: utf-8 -*-
"""
reader_pdf_V1.py — Extracción OCR de PDFs SWIFT estructura V1

Campos extraídos:
  - Receiver   → cerca de ancla "Receiver"
  - Date       → bloque 32A (fallback: "Interbank Settlement Date", "Date:")
  - Amount     → bloque 32A (fallback: bloque 33B)
  - Proveedor  → bloque 59

CAMBIOS vs versión anterior:
  - Eliminado: _resolve_tesseract_cmd() duplicado → usa core.ocr_engine
  - Eliminado: setup de logging propio           → usa core.logger
  - Eliminado: normalize_text/normalize_line     → usa core.text_utils (cuando aplica)
  - Mantenida: toda la lógica de regex y extracción de campos (sin cambios)
  - Agregado:  soporte de caché (process_folder recibe cache opcional)
  - Agregado:  retorno de estado por PDF para PipelineResult
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

import config
from core.logger import get_logger
from core.ocr_engine import get_ocr_engine

LOGGER = get_logger(__name__)


# =========================================================
# REGEX — sin cambios respecto a versión original
# =========================================================
RE_BIC      = re.compile(r"\b([A-Z0-9]{8}(?:[A-Z0-9]{3})?)\b", re.IGNORECASE)
RE_RECEIVER = re.compile(r"Recei\s*ver\s*[:;]\s*([A-Z0-9]{8}(?:[A-Z0-9]{3})?)", re.IGNORECASE)

RE_32A = re.compile(r"\b32A\b\s*[:;]?", re.IGNORECASE)
RE_33B = re.compile(r"\b33B\b\s*[:;]?", re.IGNORECASE)
RE_59  = re.compile(r"(?:^|\s)59\s*[:;]", re.IGNORECASE)

RE_INTERBANK_ISO   = re.compile(
    r"Interbank\s+Settlement\s+Date\s*[:;]\s*(\d{4}-\d{2}-\d{2})", re.IGNORECASE
)
RE_ISO_DATE        = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")
RE_DD_MON_YYYY_ANY = re.compile(r"\b(\d{1,2}\s+[A-Za-z]{3,4}\s+[0-9A-Za-z]{4})\b")

RE_AMOUNT_HASH_STRICT = re.compile(r"\bAmount\b\s*[:;]\s*#([^#]*)#", re.IGNORECASE)
RE_ANY_HASH_TOKEN     = re.compile(r"#([^#]+)#")
RE_HAS_LETTERS        = re.compile(r"[A-Z]", re.IGNORECASE)

MONTH_FIX = {
    "JAN": "JAN",
    "FEB": "FEB", "PEB": "FEB", "FEE": "FEB",
    "MAR": "MAR", "APR": "APR", "MAY": "MAY",
    "JUN": "JUN", "JUL": "JUL", "AUG": "AUG",
    "SEP": "SEP", "SEPT": "SEP",
    "OCT": "OCT", "NOV": "NOV", "DEC": "DEC",
}

# Países que no son proveedores (lista negra para extracción de Beneficiary)
_COUNTRY_BLACKLIST = {"CHINA", "TURKEY", "COLOMBIA", "PANAMA", "US", "USA"}


# =========================================================
# UTILIDADES DE TEXTO (locales a V1, sin dependencia circular)
# =========================================================
def _normalize_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r", " ").replace("\n", " ").replace("\u00A0", " ")
    return re.sub(r"\s+", " ", text).strip()


def _normalize_line(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.replace("\u00A0", " ")).strip()


# =========================================================
# PARSEO DE FECHAS
# =========================================================
def _clean_ocr_date_token(s: str) -> str:
    s = (s or "").upper().strip().replace("\u00A0", " ")
    s = re.sub(r"[,\.\-/]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _parse_dd_mon_yyyy_robust(raw: str) -> Optional[str]:
    """Parsea fechas tipo '10 APR 2025' con correcciones de OCR."""
    if not raw:
        return None
    s = _clean_ocr_date_token(raw)
    m = re.match(r"^(\d{1,2})\s+([A-Z]{2,4})\s+(\w{4})$", s.upper())
    if not m:
        return None
    d, mon_raw, y_raw = m.groups()

    mon = MONTH_FIX.get(mon_raw[:4], MONTH_FIX.get(mon_raw[:3]))
    if not mon:
        return None

    # Correcciones OCR de año: O→0, I→1
    y_fixed = y_raw.upper().replace("O", "0").replace("I", "1")
    try:
        dt = datetime.strptime(f"{int(d):02d} {mon} {y_fixed}", "%d %b %Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


# =========================================================
# EXTRACCIÓN DE CAMPOS
# =========================================================
def _extract_receiver(ocr_text: str) -> Optional[str]:
    if not ocr_text:
        return None
    norm = _normalize_text(ocr_text)

    m = RE_RECEIVER.search(norm)
    if m:
        return m.group(1).upper().strip()

    # Fallback: ventana alrededor de "Receiver"
    idx = re.search(r"Recei\s*ver", norm, flags=re.IGNORECASE)
    if idx:
        window = norm[max(0, idx.start() - 40): idx.end() + 120]
        m2 = RE_BIC.search(window)
        if m2:
            return m2.group(1).upper().strip()

    return None


def _extract_amount_from_window(window: List[str], debug: bool = False) -> Optional[str]:
    full = " ".join(window)

    # Intento 1: Amount entre # ... #
    m = RE_AMOUNT_HASH_STRICT.search(full)
    if m:
        raw = m.group(1).strip().replace("\u00A0", "").replace(",", "")
        raw = re.sub(r"\s+", "", raw)
        if debug:
            LOGGER.debug(f"Amount hash strict: {repr(raw)}")
        return raw if raw else None

    # Intento 2: cualquier par # ... #
    m2 = RE_ANY_HASH_TOKEN.search(full)
    if m2:
        raw = m2.group(1).strip().replace("\u00A0", "").replace(",", "")
        raw = re.sub(r"\s+", "", raw)
        if re.search(r"\d", raw):
            if debug:
                LOGGER.debug(f"Amount hash fallback: {repr(raw)}")
            return raw

    # Intento 3: primera línea con solo dígitos y separadores
    for ln in window:
        clean = ln.strip().replace("\u00A0", "").replace(",", "")
        clean = re.sub(r"\s+", "", clean)
        if re.match(r"^[\d\.]+$", clean) and len(clean) >= 3:
            if debug:
                LOGGER.debug(f"Amount numérico directo: {repr(clean)}")
            return clean

    # Intento 4 (fallback): primer token numérico
    for ln in window:
        m3 = re.search(r"([0-9][0-9\.,\s]*)", ln)
        if m3:
            raw = m3.group(1).strip().replace("\u00A0", "").replace(",", "")
            raw = re.sub(r"\s+", "", raw)
            if len(raw) >= 3:
                if debug:
                    LOGGER.debug(f"Amount fallback raw: {repr(raw)}")
                return raw

    return None


def _extract_date_from_32a_window(window: List[str], debug: bool = False) -> Optional[str]:
    # ISO
    for w in window:
        m = RE_ISO_DATE.search(w)
        if m:
            try:
                dt = datetime.strptime(m.group(1), "%Y-%m-%d")
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

    # DD Mon YYYY
    for w in window:
        m = RE_DD_MON_YYYY_ANY.search(w)
        if m:
            parsed = _parse_dd_mon_yyyy_robust(m.group(1))
            if debug:
                LOGGER.debug(f"Date candidato: {repr(m.group(1))} → {parsed}")
            if parsed:
                return parsed

    return None


def _extract_date_and_amount(
    ocr_text: str, debug: bool = False
) -> Tuple[Optional[str], Optional[str]]:
    if not ocr_text:
        return None, None

    lines = [_normalize_line(ln) for ln in ocr_text.splitlines() if _normalize_line(ln)]
    found_date:   Optional[str] = None
    found_amount: Optional[str] = None

    # Ancla 32A → Date + Amount
    for i, line in enumerate(lines):
        if RE_32A.search(line):
            window32 = lines[i + 1: i + 12]
            if debug:
                LOGGER.debug(f"32A ventana: {window32}")

            if not found_date:
                found_date = _extract_date_from_32a_window(window32, debug=debug)
            if not found_amount:
                found_amount = _extract_amount_from_window(window32, debug=debug)

            if found_date and found_amount:
                return found_date, found_amount

    # Ancla 33B → Amount fallback
    if not found_amount:
        for i, line in enumerate(lines):
            if RE_33B.search(line):
                window33 = lines[i + 1: i + 12]
                if debug:
                    LOGGER.debug(f"33B ventana: {window33}")
                found_amount = _extract_amount_from_window(window33, debug=debug)
                if found_amount:
                    break

    return found_date, found_amount


def _extract_value_date_fallback(ocr_text: str) -> Optional[str]:
    if not ocr_text:
        return None
    lines = [_normalize_line(ln) for ln in ocr_text.splitlines() if _normalize_line(ln)]

    for ln in lines:
        m = RE_INTERBANK_ISO.search(ln)
        if m:
            try:
                dt = datetime.strptime(m.group(1).strip(), "%Y-%m-%d")
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

    for ln in lines:
        m = re.search(r"\bDate\b\s*[:;]\s*(.+)$", ln, flags=re.IGNORECASE)
        if m:
            parsed = _parse_dd_mon_yyyy_robust(m.group(1))
            if parsed:
                return parsed

    return None


def _extract_beneficiary_from_59(ocr_text: str, debug: bool = False) -> Optional[str]:
    if not ocr_text:
        return None

    lines     = [_normalize_line(ln) for ln in ocr_text.splitlines()]
    non_empty = [ln for ln in lines if ln]

    for i, ln in enumerate(non_empty):
        if RE_59.search(ln):
            window = non_empty[i: i + 12]
            if debug:
                LOGGER.debug(f"59 encontrado. Ventana: {window}")

            for cand in window[1:]:
                c = cand.strip()
                if re.search(r"Beneficiary\s+Customer", c, flags=re.IGNORECASE):
                    continue
                if c.startswith("/"):
                    continue
                if not RE_HAS_LETTERS.search(c):
                    continue
                if c.upper() in _COUNTRY_BLACKLIST:
                    continue
                if debug:
                    LOGGER.debug(f"Beneficiary capturado: {repr(c)}")
                return c

    return None


# =========================================================
# PROCESAMIENTO DE UN SOLO PDF
# =========================================================
def extract_data_from_pdf(pdf_path: Path, debug: bool = False) -> Dict:
    """
    Extrae los 4 campos SWIFT de un PDF con estructura V1.

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
        receiver:    Optional[str] = None
        value_date:  Optional[str] = None
        amount:      Optional[str] = None
        beneficiary: Optional[str] = None

        for i, ocr_text in enumerate(pages_text, start=1):
            resultado_base["pages_scanned"] = i

            if not receiver:
                receiver = _extract_receiver(ocr_text)
            if not beneficiary:
                beneficiary = _extract_beneficiary_from_59(ocr_text, debug=debug)
            if not value_date or not amount:
                d, a = _extract_date_and_amount(ocr_text, debug=debug)
                if not value_date and d:
                    value_date = d
                if not amount and a:
                    amount = a
            if not value_date:
                value_date = _extract_value_date_fallback(ocr_text)

            # Corte temprano si ya tenemos todo
            if receiver and value_date and amount and beneficiary:
                LOGGER.info(
                    f"[V1] {pdf_path.name} (pág {i}) → "
                    f"Receiver:{receiver} | Date:{value_date} | "
                    f"Amount:{amount} | Beneficiary:{beneficiary}"
                )
                break

        estado = "Completo" if (receiver and value_date and amount and beneficiary) else "Incompleto"

        if estado == "Incompleto":
            campos_faltantes = [
                f for f, v in [
                    ("Receiver", receiver), ("Date", value_date),
                    ("Amount", amount), ("Beneficiary", beneficiary)
                ] if not v
            ]
            LOGGER.warning(f"[V1] {pdf_path.name} → Incompleto. Falta: {campos_faltantes}")

        return {
            **resultado_base,
            "receiver":    receiver,
            "date":        value_date,
            "amount":      amount,
            "beneficiary": beneficiary,
            "estado":      estado,
        }

    except Exception as e:
        LOGGER.error(f"[V1] Error procesando {pdf_path.name}: {e}", exc_info=True)
        return {**resultado_base, "estado": "Error"}


# =========================================================
# PROCESAMIENTO DE CARPETA
# =========================================================
def process_folder(
    input_folder,         # Path (carpeta) o List[Path] (lista de archivos)
    debug: bool = False,
    cache=None,           # Optional[PdfCache] — evita reprocesar PDFs ya conocidos
) -> List[Dict]:
    """
    Procesa PDFs con estructura V1.

    input_folder puede ser:
      - Path de carpeta plana  → busca *.pdf dentro de ella (comportamiento original)
      - List[Path] de archivos → los usa directamente (para fuente de red con subcarpetas)

    Si se pasa un objeto cache (core.cache.PdfCache), omite los PDFs
    ya procesados y registra los nuevos al terminar.

    Retorna lista de dicts con los datos extraídos de cada PDF.
    """
    if isinstance(input_folder, list):
        # Lista de rutas individuales (descubiertas por _descubrir_pdfs_por_version)
        pdf_files = sorted(input_folder, key=lambda p: p.name)
        if cache is not None:
            pdf_files = [p for p in pdf_files if not cache.is_processed(p)]
    elif cache is not None:
        pdf_files = cache.pending_files(input_folder, version="V1")
    else:
        pdf_files = sorted(input_folder.glob("*.pdf"))

    LOGGER.info(f"[V1] PDFs a procesar: {len(pdf_files)}")

    results: List[Dict] = []
    for pdf_file in pdf_files:
        result = extract_data_from_pdf(pdf_file, debug=debug)
        results.append(result)

        # Registrar en caché si se proporcionó
        if cache is not None:
            cache.mark(pdf_file, version="V1", estado=result["estado"])

    return results


# =========================================================
# EXPORT EXCEL (usado cuando se ejecuta standalone)
# =========================================================
def build_output_excel(
    results: List[Dict],
    output_path: Path,
    sheet_name: str = "V1",
) -> None:
    """Genera Excel con los resultados de extracción V1. Sobrescribe el archivo."""
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
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(output_path, engine="openpyxl", mode="w") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)

    LOGGER.info(f"[V1] Excel generado: {output_path} (hoja: {sheet_name})")


# =========================================================
# MAIN — ejecución standalone para pruebas
# =========================================================
if __name__ == "__main__":
    results = process_folder(config.DIR_PDFS_V1, debug=config.DEBUG)

    LOGGER.info("=== RESUMEN V1 ===")
    for r in results:
        LOGGER.info(
            f"{r['file_name']} | Receiver:{r['receiver']} | Date:{r['date']} | "
            f"Amount:{r['amount']} | Beneficiary:{r['beneficiary']} | "
            f"Páginas:{r['pages_scanned']} | Estado:{r['estado']}"
        )