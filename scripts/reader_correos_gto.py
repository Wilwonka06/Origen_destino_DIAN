"""
reader_correos_gto.py — Generación de Swift_manuales_gto desde correos (Facturas.xlsx)

Flujo:
  1. Lee Facturas.xlsx (generado por Facturas.py desde Outlook)
  2. Normaliza fecha y monto
  3. Cruza Receiver con Bd Swift.xlsx → Pais / Ciudad
  4. Construye Nombre personalizado = Proveedor + " " + Receiver
  5. Genera id determinístico (uuid5)
  6. Determina Estado: "Completo" si tiene Receiver, Date, Amount y Proveedor
                       "Incompleto" en caso contrario
  7. Escribe Swift_manuales_gto.xlsx con hojas V1 (todos) y V2 (vacía, por compatibilidad)

Todos los GTO van a V1.  No hay separación V1/V2 por diseño de este tipo de indica.
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd

import config
from core.logger import get_logger
from core.text_utils import normalize_swift_11, build_nombre_personalizado, clean_amount_value
from core.excel_utils import write_sheets

LOGGER = get_logger("reader_correos_gto")

# =========================================================
# COLUMNAS DE SALIDA (orden canónico)
# =========================================================
COLS_OUT = [
    "id",
    "Nombre archivo",
    "Receiver",
    "Date",
    "Amount",
    "Proveedor",
    "Pais",
    "Ciudad",
    "Nombre personalizado",
    "Estado",
    "Formulario",
    "Llave",
    "Version",
]

# Columnas mínimas requeridas para Estado = "Completo"
_REQUIRED_COMPLETO = ("Receiver", "Date", "Amount", "Proveedor")

# =========================================================
# NORMALIZACIÓN DE FECHA
# =========================================================
# Formatos que puede traer Facturas.xlsx
_FECHA_FMTS = [
    "%d %b %Y",   # 08 Oct 2025
    "%Y-%m-%d",   # 2025-10-08  (ya normalizado por Facturas.py)
    "%d/%m/%Y",   # 08/10/2025
    "%d-%m-%Y",   # 08-10-2025
]

def _normalizar_fecha(valor) -> str:
    """
    Convierte cualquier representación de fecha a 'YYYY-MM-DD'.
    Retorna "" si no puede parsear.
    """
    if valor is None:
        return ""
    if isinstance(valor, (datetime,)):
        return valor.strftime("%Y-%m-%d")
    # pandas Timestamp
    if hasattr(valor, "strftime"):
        return valor.strftime("%Y-%m-%d")

    s = str(valor).strip()
    if not s or s.lower() in ("nan", "none", "nat", ""):
        return ""

    for fmt in _FECHA_FMTS:
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    LOGGER.warning(f"No se pudo parsear fecha: {repr(s)}")
    return s


# =========================================================
# NORMALIZACIÓN DE MONTO
# =========================================================
def _normalizar_monto(valor) -> str:
    """
    Convierte montos con formato europeo/con # a string numérico.
    Ejemplos:
        '#8.800,#'    → '8800.00'
        '#6.752,99#'  → '6752.99'
        '21687.04'    → '21687.04'
    Delega a clean_amount_value de text_utils después de limpiar caracteres especiales.
    """
    if valor is None:
        return ""
    s = str(valor).strip()
    if not s or s.lower() in ("nan", "none", ""):
        return ""

    # Quitar # y espacios
    s = s.replace("#", "").strip()
    # Formato europeo: punto = miles, coma = decimal  →  convertir
    # Solo si tiene punto Y coma con punto antes de la coma
    if re.search(r"\d\.\d{3},", s):
        s = s.replace(".", "").replace(",", ".")
    else:
        # Puede tener coma como decimal sin puntos de miles
        s = s.replace(",", ".")

    return clean_amount_value(s)


# =========================================================
# CRUCE RECEIVER → PAIS / CIUDAD (desde Bd Swift.xlsx)
# =========================================================
def _leer_bd_swift() -> pd.DataFrame:
    """
    Carga Bd Swift.xlsx y devuelve un DataFrame con columnas:
        swift_norm | Pais | Ciudad
    """
    path = config.BD_SWIFT
    if not path.exists():
        LOGGER.warning(f"Bd Swift no encontrada: {path}. Pais/Ciudad quedará vacío.")
        return pd.DataFrame(columns=["swift_norm", config.BD_SWIFT_COL_PAIS, config.BD_SWIFT_COL_CIUDAD])

    needed = {config.BD_SWIFT_COL_CODIGO, config.BD_SWIFT_COL_PAIS, config.BD_SWIFT_COL_CIUDAD}
    xls = pd.ExcelFile(path, engine="openpyxl")

    for sh in xls.sheet_names:
        df = pd.read_excel(path, sheet_name=sh, engine="openpyxl")
        df.columns = [str(c).strip() for c in df.columns]
        if needed.issubset(set(df.columns)):
            out = df[[config.BD_SWIFT_COL_CODIGO, config.BD_SWIFT_COL_PAIS, config.BD_SWIFT_COL_CIUDAD]].copy()
            out["swift_norm"] = out[config.BD_SWIFT_COL_CODIGO].apply(normalize_swift_11)
            out = out.loc[out["swift_norm"] != ""].drop_duplicates(subset=["swift_norm"], keep="first")
            LOGGER.info(f"Bd Swift cargada: {len(out)} entradas desde hoja '{sh}'")
            return out

    LOGGER.warning(f"Bd Swift: ninguna hoja contiene columnas {needed}.")
    return pd.DataFrame(columns=["swift_norm", config.BD_SWIFT_COL_PAIS, config.BD_SWIFT_COL_CIUDAD])


def _enriquecer_pais_ciudad(df: pd.DataFrame, bd_swift: pd.DataFrame) -> pd.DataFrame:
    """
    Hace merge left Receiver → swift_norm para poblar Pais y Ciudad.
    """
    out = df.copy()
    out["_recv_norm"] = out["Receiver"].apply(
        lambda v: normalize_swift_11(str(v).strip()) if pd.notna(v) else ""
    )

    bd_map = bd_swift[["swift_norm", config.BD_SWIFT_COL_PAIS, config.BD_SWIFT_COL_CIUDAD]].rename(
        columns={
            config.BD_SWIFT_COL_PAIS:   "__pais",
            config.BD_SWIFT_COL_CIUDAD: "__ciudad",
        }
    )

    out = out.merge(bd_map, how="left", left_on="_recv_norm", right_on="swift_norm")
    out["Pais"]   = out["__pais"]
    out["Ciudad"] = out["__ciudad"]
    out = out.drop(columns=["_recv_norm", "swift_norm", "__pais", "__ciudad"], errors="ignore")

    matches = out["Pais"].notna().sum()
    LOGGER.info(f"Cruce Bd Swift GTO: {matches}/{len(out)} Receivers con Pais/Ciudad")
    return out


# =========================================================
# ESTADO
# =========================================================
def _calcular_estado(row: pd.Series) -> str:
    for col in _REQUIRED_COMPLETO:
        val = row.get(col, "")
        if val is None or str(val).strip().lower() in ("", "nan", "none", "nat"):
            return "Incompleto"
    return "Completo"


# =========================================================
# ID DETERMINÍSTICO
# =========================================================
def _make_id(receiver: str, date: str, amount: str, proveedor: str) -> str:
    base = f"gto|{receiver}|{date}|{amount}|{proveedor}".strip()
    return str(uuid.uuid5(uuid.NAMESPACE_URL, base))


# =========================================================
# FUNCIÓN PRINCIPAL
# =========================================================
def run_lector_correos_gto() -> dict:
    """
    Lee Facturas.xlsx, procesa y escribe Swift_manuales_gto.xlsx.

    Retorna dict con estadísticas:
        total       : registros leídos de Facturas.xlsx
        completos   : registros con Estado = "Completo"
        incompletos : registros con Estado = "Incompleto"
    """
    LOGGER.info("=== INICIO LECTOR CORREOS GTO ===")

    facturas_path = getattr(config, "FACTURAS_GTO", config.DIR_RESULTADOS / "Facturas.xlsx")
    manuales_path = config.SWIFT_MANUALES_GTO

    # ── 1. Leer Facturas.xlsx ──────────────────────────────
    if not facturas_path.exists():
        raise FileNotFoundError(
            f"No existe Facturas.xlsx en: {facturas_path}\n"
            f"Ejecutá primero Facturas.py para generar el archivo desde Outlook."
        )

    df_raw = pd.read_excel(facturas_path, engine="openpyxl")
    df_raw.columns = [str(c).strip() for c in df_raw.columns]

    LOGGER.info(f"Facturas.xlsx leído: {len(df_raw)} filas | columnas: {list(df_raw.columns)}")

    # Validar columnas mínimas esperadas
    esperadas = {"Receiver", "DATE", "Amount", "Beneficiary Customer"}
    faltantes = esperadas - set(df_raw.columns)
    if faltantes:
        raise KeyError(
            f"Facturas.xlsx no tiene las columnas esperadas: {faltantes}. "
            f"Columnas presentes: {list(df_raw.columns)}"
        )

    # ── 2. Construir DataFrame intermedio ────────────────────
    rows = []
    for _, r in df_raw.iterrows():
        receiver  = str(r.get("Receiver", "") or "").strip()
        date_raw  = r.get("DATE", r.get("Date", ""))
        amount_raw = r.get("Amount", "")
        proveedor = str(r.get("Beneficiary Customer", "") or "").strip()

        date_   = _normalizar_fecha(date_raw)
        amount_ = _normalizar_monto(amount_raw)

        rows.append({
            "Receiver":  receiver,
            "Date":      date_,
            "Amount":    amount_,
            "Proveedor": proveedor,
        })

    df = pd.DataFrame(rows)

    if df.empty:
        LOGGER.warning("Facturas.xlsx sin filas procesables. Se genera Swift_manuales_gto vacío.")

    # ── 3. Enriquecer Pais / Ciudad desde Bd Swift ───────────
    bd_swift = _leer_bd_swift()
    df = _enriquecer_pais_ciudad(df, bd_swift)

    # ── 4. Nombre personalizado ───────────────────────────────
    df["Nombre personalizado"] = df.apply(
        lambda r: build_nombre_personalizado(r["Proveedor"], r["Receiver"]),
        axis=1,
    )

    # ── 5. Campos fijos ───────────────────────────────────────
    df["Nombre archivo"] = ""
    df["Formulario"]     = ""
    df["Llave"]          = ""
    df["Version"]        = "V1"  # todos GTO → V1

    # ── 6. Estado ─────────────────────────────────────────────
    df["Estado"] = df.apply(_calcular_estado, axis=1)

    # ── 7. ID determinístico ──────────────────────────────────
    df["id"] = df.apply(
        lambda r: _make_id(r["Receiver"], r["Date"], r["Amount"], r["Proveedor"]),
        axis=1,
    )

    # ── 8. Ordenar columnas ───────────────────────────────────
    for col in COLS_OUT:
        if col not in df.columns:
            df[col] = ""
    df = df[COLS_OUT]

    # ── 9. Separar Completos / Incompletos ────────────────────
    df_completos   = df.loc[df["Estado"] == "Completo"].copy()
    df_incompletos = df.loc[df["Estado"] == "Incompleto"].copy()

    total       = len(df)
    completos   = len(df_completos)
    incompletos = len(df_incompletos)

    LOGGER.info(f"Total={total} | Completos={completos} | Incompletos={incompletos}")

    # ── 10. Escribir según Estado ────────────────────────────
    # Completos  → directo a Swift_completos_gto (sin pasar por manuales)
    # Incompletos → Swift_manuales_gto para revisión manual
    df_v2_vacia    = pd.DataFrame(columns=COLS_OUT)
    completos_path = config.SWIFT_COMPLETOS_GTO

    # Completos → Swift_completos_gto (merge sin duplicar por id)
    if not df_completos.empty:
        comp_v1_existente = pd.DataFrame(columns=COLS_OUT)
        if completos_path.exists():
            try:
                comp_v1_existente = pd.read_excel(
                    completos_path, sheet_name=config.SHEET_V1, engine="openpyxl"
                )
                comp_v1_existente.columns = [str(c).strip() for c in comp_v1_existente.columns]
            except Exception as e:
                LOGGER.warning(f"No se pudo leer Swift_completos_gto existente: {e}")

        ids_existentes = set(comp_v1_existente["id"].astype(str)) if not comp_v1_existente.empty else set()
        nuevos = df_completos.loc[~df_completos["id"].astype(str).isin(ids_existentes)].copy()

        if not nuevos.empty:
            merged = pd.concat([comp_v1_existente, nuevos], ignore_index=True) if not comp_v1_existente.empty else nuevos
            completos_path.parent.mkdir(parents=True, exist_ok=True)
            write_sheets(
                completos_path,
                {config.SHEET_V1: merged, config.SHEET_V2: df_v2_vacia},
                context="reader_correos_gto_completos",
            )
            LOGGER.info(f"Swift_completos_gto.xlsx: {len(nuevos)} nuevos agregados (total={len(merged)})")
        else:
            LOGGER.info("Swift_completos_gto.xlsx: todos los registros ya existían, sin cambios.")

    # Incompletos → Swift_manuales_gto
    if not df_incompletos.empty:
        manuales_path.parent.mkdir(parents=True, exist_ok=True)
        write_sheets(
            manuales_path,
            {config.SHEET_V1: df_incompletos, config.SHEET_V2: df_v2_vacia},
            context="reader_correos_gto_manuales",
        )
        LOGGER.warning(
            f"{incompletos} registro(s) con campos faltantes. "
            f"Completar en Swift_manuales_gto.xlsx y ejecutar 'Traslado Automático'."
        )
    else:
        LOGGER.info("Sin registros incompletos — Swift_manuales_gto no requerido.")

    LOGGER.info("=== FIN LECTOR CORREOS GTO ===")

    return {
        "total":       total,
        "completos":   completos,
        "incompletos": incompletos,
    }


# =========================================================
# STANDALONE
# =========================================================
if __name__ == "__main__":
    stats = run_lector_correos_gto()
    print(f"\nResumen GTO:")
    print(f"  Total leídos  : {stats['total']}")
    print(f"  Completos     : {stats['completos']}")
    print(f"  Incompletos   : {stats['incompletos']}")