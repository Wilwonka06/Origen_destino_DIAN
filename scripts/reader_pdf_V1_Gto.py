# -*- coding: utf-8 -*-
"""
reader_pdf_V1_exp.py — Lector PDF SWIFT Exportaciones V1

Diferencias vs reader_pdf_V1 (Importaciones):
  - Lee "Sender" en lugar de "Receiver"  (almacenado en clave "receiver"
    para que el resto del pipeline funcione sin cambios)
  - Lee Proveedor desde campo 50K o 50F (Ordering Customer)
    en lugar del campo Beneficiary
  - Estructura de carpetas: meses planos, PDFs nombrados DDMMYYYY.pdf

Campos extraídos por registro:
  receiver   → Sender SWIFT code  (ej: CITIUS33XXX)
  date       → Value Date del campo 32A
  amount     → Amount del campo 32A
  beneficiary→ Ordering Customer (50K/50F)   ← Proveedor
  file_name  → nombre del PDF
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Optional, Union

from core.logger import get_logger

LOGGER = get_logger(__name__)

# ── Patrones de extracción ───────────────────────────────────────────────────

# Sender: "Sender : CITIUS33XXX"
_RE_SENDER = re.compile(
    r"Sender\s*:\s*([A-Z]{4}[A-Z0-9]{2}[A-Z0-9]{2}(?:[A-Z0-9]{3})?)",
    re.IGNORECASE,
)

# Fecha dentro del bloque 32A: "Date : 05 Nov 2025"
_RE_DATE_LINE = re.compile(
    r"Date\s*:\s*(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})",
    re.IGNORECASE,
)

# Marcador del bloque 32A
_RE_32A = re.compile(r"\b32A\b\s*[:;]?", re.IGNORECASE)

# Amount entre # ... # (formato estándar SWIFT V1)
_RE_AMOUNT_HASH = re.compile(r"#([^#]+)#")

# Amount como número plano (fallback)
_RE_AMOUNT_NUM = re.compile(r"^[\d.,]+$")

# Campo 50K o 50F (Ordering Customer)
# Tolera espacios al final del encabezado y el label "Ordering Customer" opcional
_RE_50K_BLOCK = re.compile(
    r"50[KF]:\s*(?:Ordering\s+Customer)?\s*\r?\n(.*?)(?=\n\s*\d{2}[A-Z]:|$)",
    re.IGNORECASE | re.DOTALL,
)


def _extract_sender(text: str) -> Optional[str]:
    m = _RE_SENDER.search(text)
    return m.group(1).strip().upper() if m else None


def _extract_date(text: str) -> Optional[str]:
    """
    Extrae la fecha del bloque 32A usando ventana de líneas.
    Detecta la línea con '32A:' y busca 'Date : DD Mon YYYY'
    en las siguientes 6 líneas. Evita capturar fechas de otros
    campos (encabezados, referencias, etc.).
    """
    lines = text.splitlines()
    in_32a = False

    for i, line in enumerate(lines):
        ln = line.strip()

        if _RE_32A.search(ln):
            in_32a = True

        if not in_32a:
            continue

        if re.search(r"\bDate\b", ln, re.IGNORECASE):
            # Ventana: esta línea + las 2 siguientes
            window = lines[i: i + 3]
            full = " ".join(w.strip() for w in window)
            m = _RE_DATE_LINE.search(full)
            if m:
                return m.group(1).strip()

        # Salir del bloque 32A cuando empieza otro campo
        if in_32a and re.match(r"^\d{2}[A-Z]:", ln) and not _RE_32A.search(ln):
            break

    return None


def _extract_amount(text: str) -> Optional[str]:
    """
    Extrae el monto del bloque 32A usando ventana de líneas.
    Busca el marcador 32A y luego escanea las líneas siguientes
    buscando un valor entre #...# o un número plano.
    """
    lines = text.splitlines()
    in_32a = False

    for i, line in enumerate(lines):
        ln = line.strip()

        # Detectar inicio del bloque 32A
        if _RE_32A.search(ln):
            in_32a = True

        if not in_32a:
            continue

        # Buscar "Amount : #valor#" o "Amount : valor"
        if re.search(r"\bAmount\b", ln, re.IGNORECASE):
            # Ventana: esta línea + las 3 siguientes
            window = lines[i: i + 4]
            full = " ".join(w.strip() for w in window)

            # Intento 1: entre # ... #
            m = _RE_AMOUNT_HASH.search(full)
            if m:
                val = m.group(1).strip()
                if re.search(r"\d", val):
                    return val

            # Intento 2: número después de "Amount :"
            m2 = re.search(
                r"Amount\s*:\s*[#\$]?\s*([\d.,]+)",
                full, re.IGNORECASE
            )
            if m2:
                return m2.group(1).strip()

        # Salir del bloque 32A cuando empieza otro campo (ej: 33B:, 50K:)
        if in_32a and re.match(r"^\d{2}[A-Z]:", ln) and not _RE_32A.search(ln):
            break

    return None


def _extract_ordering_customer(text: str) -> Optional[str]:
    """
    Extrae el nombre del Ordering Customer del campo 50K o 50F.

    Estructura esperada:
        50K: Ordering Customer
             /903100550          ← cuenta (empieza con /)
             PIAMONTE S.A.       ← nombre ← esto queremos
             Z 11 CC ...
             GUATEMALA-GUATEMALA

    Toma la primera línea no vacía que NO empiece con '/'.
    """
    m = _RE_50K_BLOCK.search(text)
    if not m:
        # Fallback: buscar 50K/50F sin el label "Ordering Customer"
        alt = re.search(
            r"50[KF]:\s*\n?(.*?)(?=\n\d{2}[A-Z]:|$)",
            text, re.IGNORECASE | re.DOTALL,
        )
        if not alt:
            return None
        block = alt.group(1)
    else:
        block = m.group(1)

    lines = [ln.strip() for ln in block.splitlines() if ln.strip()]
    for ln in lines:
        # Saltar línea de cuenta (empieza con /) y la etiqueta "Ordering Customer"
        if ln.startswith("/"):
            continue
        if re.match(r"Ordering\s+Customer", ln, re.IGNORECASE):
            continue
        # Saltar líneas que parecen dirección (solo números, solo puntos, country codes)
        if re.match(r"^[\d\s]+$", ln):
            continue
        # Saltar líneas con basura OCR tipo "{229055965272" (llave + dígitos)
        if re.match(r"^[{\[]\d+", ln):
            continue

        # Eliminar prefijos estructurados del campo 50F: "1/1/", "1/", "2/", etc.
        # Ejemplos: "1/1/IMPORTADORA MADURO S.A." → "IMPORTADORA MADURO S.A."
        #           "1/PIAMONTE S.A."             → "PIAMONTE S.A."
        ln_clean = re.sub(r"^\d+/(\d+/)?", "", ln).strip()

        # Saltar si después de limpiar el prefijo queda vacío o solo dígitos
        if not ln_clean or re.match(r"^[\d\s]+$", ln_clean):
            continue

        # Primera línea válida = nombre del proveedor
        return ln_clean

    return None


def _extract_fields(text: str, file_name: str) -> Dict:
    return {
        "file_name":  file_name,
        "receiver":   _extract_sender(text),      # Sender → stored as "receiver"
        "date":       _extract_date(text),
        "amount":     _extract_amount(text),
        "beneficiary": _extract_ordering_customer(text),
    }


# ── Función principal ────────────────────────────────────────────────────────

def process_folder_v1_exp(
    source: Union[Path, List[Path]],
    debug:  bool  = False,
    cache=None,
) -> List[Dict]:
    """
    Procesa PDFs de Exportaciones V1.

    Parámetros:
        source : Path (carpeta plana con PDFs) o List[Path] (lista de archivos)
        debug  : activa logs detallados
        cache  : instancia de PdfCache; si el PDF ya fue procesado lo omite

    Retorna lista de dicts con: file_name, receiver, date, amount, beneficiary.
    """
    from core.ocr_engine import get_ocr_engine

    if isinstance(source, list):
        pdf_list = source
    else:
        source = Path(source)
        if not source.exists():
            LOGGER.warning(f"Carpeta EXP V1 no encontrada: {source}")
            return []
        pdf_list = sorted(source.glob("*.pdf"))

    ocr = get_ocr_engine()
    results = []

    for pdf_path in pdf_list:
        file_name = pdf_path.name

        if cache and cache.is_processed(pdf_path):
            LOGGER.debug(f"[EXP-V1] Cache hit: {file_name}")
            continue

        try:
            pages = ocr.extract_text_from_pdf(pdf_path, debug=debug)
            text = "\n".join(pages)
            data = _extract_fields(text, file_name)

            if debug:
                LOGGER.debug(
                    f"[EXP-V1] {file_name} → "
                    f"sender={data['receiver']} | "
                    f"date={data['date']} | "
                    f"amount={data['amount']} | "
                    f"proveedor={data['beneficiary']}"
                )

            results.append(data)

            if cache:
                cache.mark(pdf_path, version="V1", estado=data.get("estado", "Completo"))

        except Exception as e:
            LOGGER.error(f"[EXP-V1] Error procesando {file_name}: {e}", exc_info=debug)
            results.append({
                "file_name":   file_name,
                "receiver":    None,
                "date":        None,
                "amount":      None,
                "beneficiary": None,
                "estado":      "Error",
            })

    LOGGER.info(f"[EXP-V1] Procesados: {len(results)} PDFs")
    return results