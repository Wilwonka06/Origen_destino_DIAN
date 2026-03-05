# -*- coding: utf-8 -*-
"""
core/validators.py — Validación de campos y archivos de entrada

Uso en cualquier script:
    from core.validators import validate_input_files, validate_bic, validate_amount

Propósito:
  - Detectar problemas ANTES de empezar el procesamiento
  - Dar mensajes de error descriptivos al usuario
  - Evitar que el pipeline falle silenciosamente a mitad de proceso
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Sequence

from core.logger import get_logger

LOGGER = get_logger(__name__)

# =========================================================
# VALIDACIÓN DE ARCHIVOS DE ENTRADA
# =========================================================

def validate_input_files(*paths: Path, context: str = "") -> None:
    """
    Verifica que todos los archivos/carpetas de entrada existan.
    Lanza FileNotFoundError con mensaje descriptivo si alguno falta.

    Uso:
        validate_input_files(config.BD_PROVEEDORES, config.BD_SWIFT)
        validate_input_files(config.DIR_PDFS_V1, context="run_pipeline")
    """
    missing = [p for p in paths if not p.exists()]
    if not missing:
        return

    prefix = f"[{context}] " if context else ""
    lines = "\n".join(f"  ✗ {p}" for p in missing)
    raise FileNotFoundError(
        f"{prefix}Los siguientes archivos o carpetas no existen:\n{lines}\n\n"
        "Verificá que la variable BASE_ROOT en config.py apunta a la carpeta correcta del proyecto,\n"
        "o definí la variable de entorno ORIGEN_DESTINO_ROOT."
    )


def validate_output_dirs(*dirs: Path) -> None:
    """
    Crea los directorios de salida si no existen.
    No falla si ya existen.
    """
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)
        LOGGER.debug(f"Directorio de salida asegurado: {d}")


# =========================================================
# VALIDACIÓN DE CAMPOS EXTRAÍDOS
# =========================================================

# BIC: exactamente 8 u 11 caracteres alfanuméricos
_RE_BIC = re.compile(r"^[A-Z0-9]{8}([A-Z0-9]{3})?$", re.IGNORECASE)


def validate_bic(code: str | None) -> bool:
    """
    Valida que un código BIC/SWIFT tenga 8 u 11 caracteres alfanuméricos.

    Retorna True si es válido, False en caso contrario.
    """
    if not code or not isinstance(code, str):
        return False
    clean = code.strip()
    return bool(_RE_BIC.match(clean))


# Formatos de fecha aceptados
_DATE_FORMATS = [
    "%Y%m%d",       # 20250410  (formato SWIFT 32A)
    "%Y-%m-%d",     # 2025-04-10
    "%d %b %Y",     # 10 Apr 2025
    "%d/%m/%Y",     # 10/04/2025
    "%m/%d/%Y",     # 04/10/2025
    "%d-%m-%Y",     # 10-04-2025
    "%d%b%Y",       # 10APR2025
]


def validate_date(value: str | None) -> bool:
    """
    Valida que un string sea parseable como fecha en alguno de los formatos SWIFT/estándar.
    Retorna True si es válido, False en caso contrario.
    """
    if not value or not isinstance(value, str):
        return False
    s = value.strip()
    for fmt in _DATE_FORMATS:
        try:
            datetime.strptime(s, fmt)
            return True
        except ValueError:
            continue
    return False


def validate_amount(value: str | None) -> bool:
    """
    Valida que un string sea convertible a número (monto válido).
    Acepta formatos como: "1234.56", "1.234,56", "1,234.56", "1234"
    Retorna True si es válido, False en caso contrario.
    """
    if value is None:
        return False
    s = str(value).strip()
    if not s:
        return False

    # Eliminar separadores de miles y normalizar decimal
    # Detectar formato: si tiene ',' y '.' → el último es el decimal
    s_clean = re.sub(r"[^\d.,]", "", s)
    if not s_clean:
        return False

    last_dot = s_clean.rfind(".")
    last_com = s_clean.rfind(",")
    sep_pos  = max(last_dot, last_com)

    if sep_pos == -1:
        # Solo dígitos
        normalized = s_clean
    elif sep_pos == last_dot:
        # Punto como decimal: "1,234.56" → quitar coma
        normalized = s_clean.replace(",", "")
    else:
        # Coma como decimal: "1.234,56" → quitar punto, reemplazar coma
        normalized = s_clean.replace(".", "").replace(",", ".")

    try:
        float(normalized)
        return True
    except ValueError:
        return False


# =========================================================
# VALIDACIÓN DE ESTADO DE UN REGISTRO
# =========================================================

def is_registro_completo(row: dict) -> bool:
    """
    Determina si un registro tiene todos los campos requeridos para ser
    considerado "Completo" y poder pasar a Swift_completos.xlsx.

    Campos requeridos: Receiver, Date, Amount, Proveedor
    """
    def _has_value(v) -> bool:
        import pandas as pd
        if v is None:
            return False
        if isinstance(v, float) and pd.isna(v):
            return False
        return str(v).strip() not in ("", "nan", "None", "NaT", "NaN")

    return all([
        _has_value(row.get("Receiver")),
        _has_value(row.get("Date")),
        _has_value(row.get("Amount")),
        _has_value(row.get("Proveedor")),
    ])


# =========================================================
# REPORTE DE VALIDACIÓN (para summary en main.py)
# =========================================================

def validate_dataframe_fields(df, context: str = "") -> dict:
    """
    Valida campos extraídos en un DataFrame completo.
    Retorna un dict con conteos para el reporte final.

    Uso:
        reporte = validate_dataframe_fields(df_v1, context="V1")
        print(reporte)
        # {'total': 50, 'bic_invalidos': 2, 'fechas_invalidas': 1, 'montos_invalidos': 0}
    """
    import pandas as pd

    report = {
        "context":          context,
        "total":            len(df),
        "bic_invalidos":    0,
        "fechas_invalidas": 0,
        "montos_invalidos": 0,
        "sin_proveedor":    0,
    }

    if df.empty:
        return report

    if "Receiver" in df.columns:
        report["bic_invalidos"] = int(
            df["Receiver"].apply(
                lambda v: not validate_bic(str(v)) if pd.notna(v) and str(v).strip() else False
            ).sum()
        )

    if "Date" in df.columns:
        report["fechas_invalidas"] = int(
            df["Date"].apply(
                lambda v: not validate_date(str(v)) if pd.notna(v) and str(v).strip() else False
            ).sum()
        )

    if "Amount" in df.columns:
        report["montos_invalidos"] = int(
            df["Amount"].apply(
                lambda v: not validate_amount(str(v)) if pd.notna(v) and str(v).strip() else False
            ).sum()
        )

    if "Proveedor" in df.columns:
        report["sin_proveedor"] = int(
            df["Proveedor"].apply(
                lambda v: not v or not str(v).strip() or str(v).strip().lower() in ("nan", "none", "")
            ).sum()
        )

    if context:
        LOGGER.info(
            f"Validación [{context}]: total={report['total']} | "
            f"BIC inválidos={report['bic_invalidos']} | "
            f"Fechas inválidas={report['fechas_invalidas']} | "
            f"Montos inválidos={report['montos_invalidos']} | "
            f"Sin proveedor={report['sin_proveedor']}"
        )

    return report
