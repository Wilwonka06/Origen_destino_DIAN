# -*- coding: utf-8 -*-
"""
core/excel_utils.py — Utilidades Excel centralizadas

Uso en cualquier script:
    from core.excel_utils import read_sheet_safe, write_sheets, append_to_sheet

Propósito:
  - Manejo de errores robusto al leer/escribir Excel
  - Evitar repetir try/except en cada script
  - Mensajes de error descriptivos
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from core.logger import get_logger

LOGGER = get_logger(__name__)


# =========================================================
# LECTURA SEGURA
# =========================================================

def read_sheet_safe(
    path: Path,
    sheet_name: str,
    context: str = "",
) -> pd.DataFrame:
    """
    Lee una hoja de Excel de forma segura.

    Si el archivo o la hoja no existe, retorna DataFrame vacío y loguea warning
    en lugar de lanzar excepción (útil para archivos que se crean en el primer run).

    Para archivos que DEBEN existir, usar validate_input_files() antes.

    Retorna: DataFrame (vacío si no existe el archivo o la hoja)
    """
    prefix = f"[{context}] " if context else ""

    if not path.exists():
        LOGGER.warning(f"{prefix}Archivo no encontrado: {path.name} → retorna DataFrame vacío")
        return pd.DataFrame()

    try:
        df = pd.read_excel(path, sheet_name=sheet_name)
        # Limpiar nombres de columnas (quitar espacios, NBSP)
        df.columns = [str(c).replace("\u00A0", " ").strip() for c in df.columns]
        LOGGER.debug(f"{prefix}Leído {path.name}[{sheet_name}]: {len(df)} filas")
        return df
    except ValueError as e:
        # La hoja no existe en el archivo
        LOGGER.warning(f"{prefix}Hoja '{sheet_name}' no encontrada en {path.name}: {e} → retorna DataFrame vacío")
        return pd.DataFrame()
    except Exception as e:
        LOGGER.error(f"{prefix}Error al leer {path.name}[{sheet_name}]: {e}")
        raise


def read_all_sheets(path: Path, context: str = "") -> Dict[str, pd.DataFrame]:
    """
    Lee todas las hojas de un Excel.
    Retorna dict {nombre_hoja: DataFrame}.
    """
    prefix = f"[{context}] " if context else ""

    if not path.exists():
        LOGGER.warning(f"{prefix}Archivo no encontrado: {path.name} → retorna dict vacío")
        return {}

    try:
        sheets = pd.read_excel(path, sheet_name=None)
        # Limpiar columnas de cada hoja
        for name, df in sheets.items():
            df.columns = [str(c).replace("\u00A0", " ").strip() for c in df.columns]
        LOGGER.debug(f"{prefix}Leído {path.name}: {list(sheets.keys())}")
        return sheets
    except Exception as e:
        LOGGER.error(f"{prefix}Error al leer {path.name}: {e}")
        raise


# =========================================================
# ESCRITURA SEGURA
# =========================================================

def write_sheets(
    path: Path,
    sheets: Dict[str, pd.DataFrame],
    context: str = "",
) -> None:
    """
    Escribe múltiples hojas en un Excel, reemplazando el archivo completo.

    Uso:
        write_sheets(config.SWIFT_COMPLETOS, {"V1": df_v1, "V2": df_v2})

    El directorio se crea automáticamente si no existe.
    """
    prefix = f"[{context}] " if context else ""
    path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with pd.ExcelWriter(path, engine="openpyxl", mode="w") as writer:
            for sheet_name, df in sheets.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                LOGGER.debug(f"{prefix}Hoja escrita: {sheet_name} ({len(df)} filas)")
        LOGGER.info(f"{prefix}Excel guardado: {path.name} ({list(sheets.keys())})")
    except Exception as e:
        LOGGER.error(f"{prefix}Error al guardar {path.name}: {e}")
        raise


def append_to_sheet(
    path: Path,
    sheet_name: str,
    df_new: pd.DataFrame,
    id_col: str = "id",
    context: str = "",
) -> int:
    """
    Agrega filas a una hoja Excel sin duplicar por id_col.

    Si el archivo no existe, lo crea con las filas nuevas.
    Si la hoja no existe, la crea.
    Si ya existen filas con el mismo id, las omite.

    Retorna: número de filas efectivamente agregadas.
    """
    prefix = f"[{context}] " if context else ""

    if df_new.empty:
        LOGGER.info(f"{prefix}append_to_sheet: df_new vacío, nada que agregar")
        return 0

    df_existing = read_sheet_safe(path, sheet_name, context=context)

    if not df_existing.empty and id_col in df_existing.columns and id_col in df_new.columns:
        existing_ids = set(df_existing[id_col].astype(str))
        df_to_add = df_new[~df_new[id_col].astype(str).isin(existing_ids)].copy()
    else:
        df_to_add = df_new.copy()

    if df_to_add.empty:
        LOGGER.info(f"{prefix}append_to_sheet: todos los registros ya existen en {path.name}[{sheet_name}]")
        return 0

    if not df_existing.empty:
        df_final = pd.concat([df_existing, df_to_add], ignore_index=True)
    else:
        df_final = df_to_add.copy()

    # Si el archivo ya existe y tiene otras hojas, preservarlas
    if path.exists():
        try:
            all_sheets = read_all_sheets(path, context=context)
        except Exception:
            all_sheets = {}
    else:
        all_sheets = {}

    all_sheets[sheet_name] = df_final
    write_sheets(path, all_sheets, context=context)

    added = len(df_to_add)
    LOGGER.info(f"{prefix}append_to_sheet: {added} filas nuevas agregadas a {path.name}[{sheet_name}]")
    return added


# =========================================================
# UTILIDADES DE COLUMNAS
# =========================================================

def ensure_columns(df: pd.DataFrame, cols: list[str], fill_value=None) -> pd.DataFrame:
    """
    Asegura que el DataFrame tenga todas las columnas requeridas.
    Agrega las faltantes con fill_value (None por defecto).
    """
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            out[c] = fill_value
    return out


def reorder_columns(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """
    Reordena el DataFrame según la lista de columnas.
    Primero agrega las que faltan, luego reordena.
    Solo incluye columnas que estén en 'cols'.
    """
    out = ensure_columns(df, cols)
    return out[cols].copy()
