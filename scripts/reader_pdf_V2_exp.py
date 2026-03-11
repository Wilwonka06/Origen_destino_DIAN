"""
reader_pdf_V2_exp.py — Lector PDF SWIFT Exportaciones V2

Diferencias vs reader_pdf_V2 (Importaciones):
  - Lee "Sender" en lugar de "Receiver"
  - Lee Proveedor desde campo "Debtor" (nombre del deudor/ordenante)
    en lugar de Beneficiary/Creditor
  - Formato de fecha: "Interbank Settlement Date: 2025-11-25"
  - Formato de monto: "Interbank Settlement Amount: USD7297.35"

Estructura del PDF V2 Exp (pacs.008):
    Sender:           BOFAUS3MXXX
    Receiver:         COLOPAPAXXX
    Interbank Settlement Date:    2025-11-25
    Interbank Settlement Amount:  USD7297.35
    Debtor:           2100118235
                      NOVOMODE SA          ← Proveedor
                      QUITO DISTRITO...
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Optional, Union

from core.logger import get_logger

LOGGER = get_logger(__name__)

# ── Patrones de extracción ───────────────────────────────────────────────────

# Sender label
_RE_SENDER_LABEL = re.compile(r"Sender\s*:", re.IGNORECASE)

# BIC: 8 u 11 chars alfanuméricos
_RE_SENDER_BIC = re.compile(
    r"\b([A-Z]{4}[A-Z0-9]{2}[A-Z0-9]{2}(?:[A-Z0-9]{3})?)\b",
    re.IGNORECASE,
)

# Tokens que el OCR puede confundir con BICs pero no lo son
_EXCLUDED_TOKENS = {
    "COLOPAPAXXX", "COLOPAPA",
    "RECEIVER", "SENDER", "MESSAGE", "TRANSFER", "CUSTOMER",
    "CREDIT", "PAYMENT", "FINANCIAL", "INFORMATION",
}

# Fecha: "Interbank Settlement Date:    2025-11-25"
_RE_DATE = re.compile(
    r"Interbank\s+Settlement\s+Date\s*:\s*(\d{4}-\d{2}-\d{2})",
    re.IGNORECASE,
)

# Monto: "Interbank Settlement Amount:  USD7297.35"
_RE_AMOUNT = re.compile(
    r"Interbank\s+Settlement\s+Amount\s*:\s*(?:USD|EUR|GBP|CHF|JPY|CAD|AUD)?\s*([\d.,]+)",
    re.IGNORECASE,
)

# Bloque Debtor: extrae todo el contenido después de "Debtor:"
# hasta el siguiente campo de nivel superior
_RE_DEBTOR_BLOCK = re.compile(
    r"Debtor\s*:\s*(.*?)(?=\nDebtor\s+Agent\s*:|$)",
    re.IGNORECASE | re.DOTALL,
)


def _extract_sender(text: str) -> Optional[str]:
    """
    Busca Sender: y extrae el BIC en esa línea o en la siguiente,
    solo si la siguiente no empieza con otro label (Receiver:, etc.).
    """
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if not _RE_SENDER_LABEL.search(line):
            continue
        # Primero: en la misma línea del label
        for m in _RE_SENDER_BIC.finditer(line):
            code = m.group(1).strip().upper()
            if code not in _EXCLUDED_TOKENS:
                return code
        # Fallback: línea siguiente solo si no es otro label
        if i + 1 < len(lines):
            next_line = lines[i + 1].strip()
            if ":" not in next_line[:20]:
                for m in _RE_SENDER_BIC.finditer(next_line):
                    code = m.group(1).strip().upper()
                    if code not in _EXCLUDED_TOKENS:
                        return code
    return None


def _extract_date(text: str) -> Optional[str]:
    m = _RE_DATE.search(text)
    return m.group(1).strip() if m else None


def _extract_amount(text: str) -> Optional[str]:
    m = _RE_AMOUNT.search(text)
    return m.group(1).strip() if m else None


def _extract_debtor_name(text: str) -> Optional[str]:
    """
    Extrae el nombre del Debtor.

    Estructura esperada:
        Debtor:           2100118235       ← cuenta/ID (solo dígitos)
                          NOVOMODE SA      ← nombre ← esto queremos
                          QUITO DISTRITO...
                          QUITO ECUADOR

    Toma la primera línea no vacía que NO sea solo dígitos.
    """
    m = _RE_DEBTOR_BLOCK.search(text)
    if not m:
        return None

    block = m.group(1)
    lines = [ln.strip() for ln in block.splitlines() if ln.strip()]

    for ln in lines:
        # Saltar líneas que son solo números (ID de cuenta)
        if re.match(r"^\d+$", ln):
            continue
        # Saltar líneas que parecen dirección (ciudad, país, código postal)
        if re.match(r"^[A-Z]{2,3}\s*$", ln):   # solo código de país
            continue
        # Primera línea válida = nombre
        return ln.strip()

    return None


def _extract_fields(text: str, file_name: str) -> Dict:
    return {
        "file_name":   file_name,
        "receiver":    _extract_sender(text),      # Sender → stored as "receiver"
        "date":        _extract_date(text),
        "amount":      _extract_amount(text),
        "beneficiary": _extract_debtor_name(text),
    }


# ── Función principal ────────────────────────────────────────────────────────

def process_folder_v2_exp(
    source: Union[Path, List[Path]],
    debug:  bool  = False,
    cache=None,
) -> List[Dict]:
    """
    Procesa PDFs de Exportaciones V2.

    Parámetros:
        source : Path (carpeta plana) o List[Path] (lista de archivos)
        debug  : activa logs detallados
        cache  : instancia de PdfCache

    Retorna lista de dicts con: file_name, receiver, date, amount, beneficiary.
    """
    from core.ocr_engine import get_ocr_engine

    if isinstance(source, list):
        pdf_list = source
    else:
        source = Path(source)
        if not source.exists():
            LOGGER.warning(f"Carpeta EXP V2 no encontrada: {source}")
            return []
        pdf_list = sorted(source.glob("*.pdf"))

    ocr = get_ocr_engine()
    results = []

    for pdf_path in pdf_list:
        file_name = pdf_path.name

        if cache and cache.is_processed(pdf_path):
            LOGGER.debug(f"[EXP-V2] Cache hit: {file_name}")
            continue

        try:
            pages = ocr.extract_text_from_pdf(pdf_path, debug=debug)
            text = "\n".join(pages)
            data = _extract_fields(text, file_name)

            if debug:
                LOGGER.debug(
                    f"[EXP-V2] {file_name} → "
                    f"sender={data['receiver']} | "
                    f"date={data['date']} | "
                    f"amount={data['amount']} | "
                    f"proveedor={data['beneficiary']}"
                )

            results.append(data)

            if cache:
                cache.mark(pdf_path, version="V2", estado=data.get("estado", "Completo"))

        except Exception as e:
            LOGGER.error(f"[EXP-V2] Error procesando {file_name}: {e}", exc_info=debug)
            results.append({
                "file_name":   file_name,
                "receiver":    None,
                "date":        None,
                "amount":      None,
                "beneficiary": None,
                "estado":      "Error",
            })

    LOGGER.info(f"[EXP-V2] Procesados: {len(results)} PDFs")
    return results