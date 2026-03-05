# -*- coding: utf-8 -*-
"""
run_formulario.py — Cruces de Formulario, Llave y Llave OD

Pasos:
  1) Lee XLSB (hoja COM) — encabezados fila 4, columnas A..F
  2) Filtra: FECHA >= config.FECHA_MIN_XLSB  y  INDICA == "Imp"
  3) Cruza COM → Swift_completos: asigna Formulario por fecha + nombre archivo
     (si hay múltiples matches y la suma de DEBITO coincide con Amount, concatena)
  4) Cruza origenDestino → Swift_completos: asigna Llave por Nombre personalizado
  5) Cruza Swift_completos.Formulario → origenDestino."Origen y destino".Consecutivo
     y escribe Llave Origen Destino

CAMBIOS vs versión anterior:
  - Rutas y constantes → config.py (eliminadas todas las hardcodeadas)
  - Logging → core.logger (sin basicConfig global)
  - normalize_text_key → core.text_utils.normalize_text_key
  - Validación de archivos al inicio → core.validators
  - Función run_cruce_completo() para ser llamada desde main.py
  - Lógica de cruces sin cambios funcionales
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List

import pandas as pd
from pyxlsb import open_workbook

import config
from core.logger import get_logger
from core.text_utils import normalize_text_key
from core.validators import validate_input_files
from core.excel_utils import write_sheets, read_sheet_safe

LOGGER = get_logger(__name__)

# Alias de config para legibilidad local
FECHA_MIN       = pd.Timestamp(config.FECHA_MIN_XLSB)
AMOUNT_TOL      = config.AMOUNT_TOL
TOKEN_MIN_RATIO = config.TOKEN_MIN_RATIO
TOKEN_MIN_OVERLAP = config.TOKEN_MIN_OVERLAP
HEADER_ROW_1BASED = 4
MAX_COLS          = 6


# =========================================================
# UTILIDADES DE NORMALIZACIÓN (locales, sin dependencia circular)
# =========================================================
def _parse_fecha_excel_series(s: pd.Series) -> pd.Series:
    """Convierte series de fechas XLSB (numéricas, string o datetime) a datetime."""
    if s is None:
        return pd.to_datetime(pd.Series([], dtype="datetime64[ns]"))
    if pd.api.types.is_datetime64_any_dtype(s):
        return pd.to_datetime(s, errors="coerce")
    if pd.api.types.is_numeric_dtype(s):
        return pd.to_datetime(s, unit="D", origin="1899-12-30", errors="coerce")
    s2 = s.astype(str).str.strip().replace({"": pd.NA, "nan": pd.NA, "NaT": pd.NA})
    return pd.to_datetime(s2, dayfirst=True, errors="coerce")

def _tokenize(s: str) -> List[str]:
    if not s:
        return []
    return re.findall(r"[a-z0-9]+", normalize_text_key(s))

def _clean_detalle(detalle: str) -> str:
    """Limpia el DETALLE de COM: elimina desde '#' y normaliza."""
    if not detalle:
        return ""
    s = str(detalle).replace("\u00A0", " ")
    s = re.split(r"#", s, maxsplit=1)[0].strip()
    return normalize_text_key(s)

def _clean_nombre_archivo(nombre: str) -> str:
    """Limpia el Nombre archivo de Swift: quita extensión .pdf y prefijos numéricos."""
    if not nombre:
        return ""
    s = str(nombre).replace("\u00A0", " ").strip()
    s = re.sub(r"\.pdf\s*$", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"^(?:\d+\s+)+", "", s).strip()
    s = re.sub(r"\s+", " ", s).strip()
    return normalize_text_key(s)

def _limpiar_formulario_str(valor: str) -> str:
    """
    Elimina ceros a la izquierda en cada parte de un formulario concatenado con \'\'-\'\'.

    Ejemplos:
      "012030"               → "12030"
      "012030-None-00012028" → "12030-None-12028"
      ""  / None             → sin cambio

    Se aplica sobre formulario_str en _build_com_keys: los valores que llegan
    a Swift_completos y a las plantillas Bancolombia ya vienen sin ceros sobrantes.
    """
    if not valor or str(valor).strip() == "":
        return valor

    partes = str(valor).split("-")
    limpias = []
    for parte in partes:
        parte_limpia = parte.lstrip("0")
        limpias.append(parte_limpia if parte_limpia != "" else "0")

    return "-".join(limpias)

def _parse_money_to_float(v) -> float:
    """Convierte un valor de monto a float, manejando formatos EU y US."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return float("nan")
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v)

    s = re.sub(r"\s+", "", str(v).replace("\u00A0", " ").strip())
    s = re.sub(r"[^0-9\.,\-]", "", s)

    if s in ("", "-", ".", ","):
        return float("nan")

    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")

    if re.search(r"\.$", s):
        s += "0"

    try:
        return float(s)
    except Exception:
        return float("nan")


# =========================================================
# MATCH POR TOKENS (COM DETALLE ↔ Swift Nombre archivo)
# =========================================================
def _tokens_match(swift_clean: str, detalle_clean: str) -> bool:
    """
    Retorna True si swift_clean hace match con detalle_clean según reglas de tokens:
      - Primeras 2 palabras deben coincidir (si existen)
      - Mínimo TOKEN_MIN_OVERLAP tokens en común
      - Ratio de coincidencia >= TOKEN_MIN_RATIO
    """
    st = _tokenize(swift_clean)
    dt = set(_tokenize(detalle_clean))

    if not st:
        return False

    overlap = sum(1 for t in st if t in dt)
    ratio   = overlap / max(len(st), 1)

    if len(st) >= 2:
        if st[0] not in dt or st[1] not in dt:
            return False
        if overlap < TOKEN_MIN_OVERLAP:
            return False
        return ratio >= TOKEN_MIN_RATIO

    return st[0] in dt


# =========================================================
# PASO 1) LECTURA XLSB
# =========================================================
def read_com_sheet(xlsb_path: Path, sheet_name: str = config.SHEET_COM) -> pd.DataFrame:
    """Lee la hoja COM del XLSB, columnas A..F, encabezados en fila 4."""
    if not xlsb_path.exists():
        raise FileNotFoundError(f"No existe el archivo XLSB: {xlsb_path}")

    header_idx = HEADER_ROW_1BASED - 1   # 0-based
    data_rows: List[List] = []
    headers = None

    with open_workbook(str(xlsb_path)) as wb:
        with wb.get_sheet(sheet_name) as sheet:
            for r_idx, row in enumerate(sheet.rows()):
                if r_idx < header_idx:
                    continue

                values = [cell.v for cell in row[:MAX_COLS]]

                if r_idx == header_idx:
                    headers = [str(v).strip() if v is not None else "" for v in values]
                    continue

                if headers is None:
                    raise RuntimeError("No se detectó header en fila 4 del XLSB.")

                if all(v is None or str(v).strip() == "" for v in values):
                    continue

                data_rows.append(values)

    df = pd.DataFrame(data_rows, columns=headers)
    df.columns = [str(c).replace("\u00A0", " ").strip() for c in df.columns]

    LOGGER.info(f"XLSB leído → filas={len(df)} | cols={list(df.columns)}")
    return df


# =========================================================
# PASO 2) FILTRO FECHA + INDICA
# =========================================================
def filter_com_df(df: pd.DataFrame) -> pd.DataFrame:
    """Filtra COM: FECHA >= FECHA_MIN y INDICA == 'Imp'."""
    if df.empty:
        LOGGER.warning("COM viene vacío antes de filtrar.")
        return df

    cols_map = {str(c).strip().upper(): c for c in df.columns}

    for req in ("FECHA", "INDICA"):
        if req not in cols_map:
            raise KeyError(
                f"No se encontró columna '{req}' en COM. "
                f"Columnas detectadas: {list(df.columns)}"
            )

    col_fecha  = cols_map["FECHA"]
    col_indica = cols_map["INDICA"]

    out = df.copy()
    out["_FECHA_DT"] = _parse_fecha_excel_series(out[col_fecha])
    out["_INDICA_NORM"] = (
        out[col_indica]
        .astype(str)
        .str.replace("\u00A0", " ", regex=False)
        .str.strip()
        .str.lower()
    )

    before = len(out)
    out = out.loc[out["_FECHA_DT"].notna()].copy()
    out = out.loc[out["_FECHA_DT"] >= FECHA_MIN].copy()
    out = out.loc[out["_INDICA_NORM"] == "imp"].copy()
    out = out.drop(columns=["_FECHA_DT", "_INDICA_NORM"], errors="ignore").reset_index(drop=True)

    LOGGER.info(
        f"Filtro COM: inicio={before} → resultado={len(out)} "
        f"(FECHA>={FECHA_MIN.date()}, INDICA=Imp)"
    )
    return out


# =========================================================
# PASO 3) CRUCE COM → SWIFT (FORMULARIO)
# =========================================================
def _build_com_keys(df_com: pd.DataFrame) -> pd.DataFrame:
    """Prepara las claves de COM para el cruce con Swift."""
    out = df_com.copy()
    cols_map = {str(c).strip().upper(): c for c in out.columns}

    required = ["FECHA", "DETALLE", "FORMULARIO", "DEBITO"]
    missing  = [c for c in required if c not in cols_map]
    if missing:
        raise KeyError(
            f"En COM faltan columnas requeridas: {missing}. "
            f"Detectadas: {list(out.columns)}"
        )

    out["_fecha_dt"]      = _parse_fecha_excel_series(out[cols_map["FECHA"]])
    out["fecha_key"]      = out["_fecha_dt"].dt.strftime("%Y-%m-%d")
    out["detalle_clean"]  = out[cols_map["DETALLE"]].apply(_clean_detalle)
    out["debito_num"]     = out[cols_map["DEBITO"]].apply(_parse_money_to_float)
    out["formulario_str"] = out[cols_map["FORMULARIO"]].apply(
        lambda x: _limpiar_formulario_str(
            "" if x is None or (isinstance(x, float) and pd.isna(x)) else str(x).strip()
        )
    )
    out["row_order"] = range(len(out))
    out = out.drop(columns=["_fecha_dt"], errors="ignore")
    return out

def _build_swift_keys(df_swift: pd.DataFrame) -> pd.DataFrame:
    """Prepara las claves de Swift para el cruce con COM."""
    out = df_swift.copy()
    needed = ["Date", "Nombre archivo", "Amount", "id"]
    miss   = [c for c in needed if c not in out.columns]
    if miss:
        raise KeyError(f"Swift_completos faltan columnas: {miss}. Detectadas: {list(out.columns)}")

    out["_date_dt"]    = pd.to_datetime(out["Date"], errors="coerce")
    out["fecha_key"]   = out["_date_dt"].dt.strftime("%Y-%m-%d")
    out["nombre_clean"] = out["Nombre archivo"].apply(_clean_nombre_archivo)
    out["amount_num"]  = out["Amount"].apply(_parse_money_to_float)
    out = out.drop(columns=["_date_dt"], errors="ignore")
    return out

def _update_formulario_for_sheet(df_swift_sheet: pd.DataFrame, df_com: pd.DataFrame,) -> pd.DataFrame:
    """
    Cruza COM contra una hoja de Swift_completos y actualiza la columna Formulario.
    Si hay múltiples matches y la suma de DEBITO coincide con Amount, concatena formularios.
    """
    if df_swift_sheet.empty:
        return df_swift_sheet

    out = df_swift_sheet.copy()
    out.columns = [str(c).strip() for c in out.columns]

    if "Formulario" not in out.columns:
        out["Formulario"] = ""

    swift_k    = _build_swift_keys(out)
    com_k      = _build_com_keys(df_com)
    com_by_date = {k: v.copy() for k, v in com_k.groupby("fecha_key")}

    updated = multi_matched = multi_ok = multi_fail = 0

    for _, srow in swift_k.iterrows():
        sid      = srow["id"]
        s_fecha  = srow["fecha_key"]
        s_name   = srow["nombre_clean"]
        s_amount = srow["amount_num"]

        if not isinstance(s_fecha, str) or not s_fecha:
            continue

        com_day = com_by_date.get(s_fecha)
        if com_day is None or com_day.empty:
            continue

        cand = com_day.loc[
            com_day["detalle_clean"].apply(lambda d: _tokens_match(s_name, d))
        ].copy()

        if cand.empty:
            continue

        # Match único
        if len(cand) == 1:
            form_val = str(cand.iloc[0]["formulario_str"]).strip()
            if form_val and form_val.lower() != "none":
                out.loc[out["id"] == sid, "Formulario"] = form_val
                updated += 1
            continue

        # Múltiples matches → validar suma DEBITO vs Amount
        multi_matched += 1
        deb_sum = cand["debito_num"].sum(skipna=True)

        if pd.notna(deb_sum) and pd.notna(s_amount) and abs(deb_sum - s_amount) <= AMOUNT_TOL:
            cand  = cand.sort_values("row_order")
            forms = [
                str(x).strip()
                for x in cand["formulario_str"].tolist()
                if str(x).strip() and str(x).strip().lower() not in ("none", "nan", "nat")
            ]
            if forms:
                out.loc[out["id"] == sid, "Formulario"] = "-".join(forms)
                updated += 1
                multi_ok += 1
        else:
            multi_fail += 1

    LOGGER.info(
        f"Cruce Formulario: updated={updated} | "
        f"multi_matched={multi_matched} | multi_ok={multi_ok} | multi_fail={multi_fail}"
    )
    return out


# =========================================================
# PASO 4) CRUCE ORIGEN DESTINO → LLAVE (Swift)
# =========================================================
def _read_od_mapping(path: Path) -> pd.DataFrame:
    """Lee el mapping Nombre personalizado → Llave carga masiva desde origenDestino."""
    df = pd.read_excel(path, sheet_name=config.SHEET_OD_DATOS)
    df.columns = [str(c).replace("\u00A0", " ").strip() for c in df.columns]

    for col in (config.OD_COL_NOMBRE, config.OD_COL_LLAVE):
        if col not in df.columns:
            raise KeyError(
                f"No se encontró columna '{col}' en {path.name}. "
                f"Columnas: {list(df.columns)}"
            )

    out = df[[config.OD_COL_NOMBRE, config.OD_COL_LLAVE]].copy()
    out[config.OD_COL_NOMBRE] = out[config.OD_COL_NOMBRE].apply(normalize_text_key)
    out[config.OD_COL_LLAVE]  = out[config.OD_COL_LLAVE].astype(str).str.strip()

    out = (
        out
        .loc[out[config.OD_COL_NOMBRE] != ""]
        .loc[out[config.OD_COL_LLAVE]  != ""]
        .drop_duplicates(subset=[config.OD_COL_NOMBRE], keep="first")
        .reset_index(drop=True)
    )

    LOGGER.info(f"OrigenDestino mapping: {len(out)} llaves únicas cargadas.")
    return out

def _apply_llave_to_sheet(df_sheet: pd.DataFrame, od_map: pd.DataFrame, ) -> pd.DataFrame:
    """Aplica el cruce Llave a una hoja de Swift_completos."""
    if df_sheet.empty:
        return df_sheet

    out = df_sheet.copy()
    out.columns = [str(c).strip() for c in out.columns]

    if config.OD_COL_NOMBRE not in out.columns:
        raise KeyError("Swift_completos no tiene columna 'Nombre personalizado'.")
    if "Llave" not in out.columns:
        out["Llave"] = ""

    map_dict      = dict(zip(od_map[config.OD_COL_NOMBRE], od_map[config.OD_COL_LLAVE]))
    out["_np_norm"]   = out[config.OD_COL_NOMBRE].apply(normalize_text_key)
    out["_llave_cur"] = out["Llave"].fillna("").astype(str).str.strip()
    out["_llave_new"] = out["_np_norm"].map(map_dict).fillna("")

    fill_mask = (out["_llave_cur"] == "") & (out["_llave_new"] != "")
    filled    = int(fill_mask.sum())
    out.loc[fill_mask, "Llave"] = out.loc[fill_mask, "_llave_new"]

    after_empty = (out["Llave"].fillna("").astype(str).str.strip() == "").sum()
    LOGGER.info(
        f"Cruce Llave: asignadas={filled} | "
        f"sin llave después={after_empty}/{len(out)}"
    )
    out = out.drop(columns=["_np_norm", "_llave_cur", "_llave_new"], errors="ignore")
    return out


# =========================================================
# PASO 5) FORMULARIO (Swift) → Consecutivo (origenDestino)
# =========================================================
def _extract_consecutivos(val) -> List[str]:
    """De '12030-None-12028' → ['12030', '12028'] (ignora None y vacíos)."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return []
    parts = [p.strip() for p in str(val).strip().split("-")]
    result = []
    for p in parts:
        if not p or p.lower() == "none":
            continue
        m = re.search(r"\d+", p)
        if m:
            result.append(m.group(0))
    return result

def _norm_consecutivo_series(s: pd.Series) -> pd.Series:
    """Normaliza Consecutivo a string de dígitos (12029.0 → '12029')."""
    def _one(x):
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return ""
        if isinstance(x, int) and not isinstance(x, bool):
            return str(x)
        if isinstance(x, float):
            return str(int(x)) if float(x).is_integer() else str(x).strip()
        sx = str(x).strip()
        m  = re.search(r"\d+", sx)
        return m.group(0) if m else ""
    return s.apply(_one)

def _update_od_llave(path: Path, df_swift_all: pd.DataFrame) -> None:
    """
    Actualiza la hoja 'Origen y destino' de origenDestino.xlsx:
    cruza Consecutivo con los formularios de Swift y escribe Llave Origen Destino.
    No pisa valores existentes distintos (los cuenta como conflictos).
    """
    if df_swift_all.empty:
        LOGGER.info("PASO 5: Swift vacío, nada que cruzar a origenDestino.")
        return

    for col in ("Formulario", "Llave"):
        if col not in df_swift_all.columns:
            raise KeyError(f"Swift_completos no tiene columna '{col}' (requerida para PASO 5).")

    df_od2 = pd.read_excel(path, sheet_name=config.SHEET_OD_ORIGEN)
    df_od2.columns = [str(c).replace("\u00A0", " ").strip() for c in df_od2.columns]

    if config.OD2_COL_CONSECUTIVO not in df_od2.columns:
        raise KeyError(
            f"No existe columna '{config.OD2_COL_CONSECUTIVO}' "
            f"en hoja '{config.SHEET_OD_ORIGEN}'."
        )
    if config.OD2_COL_LLAVE_OD not in df_od2.columns:
        df_od2[config.OD2_COL_LLAVE_OD] = ""

    df_od2["_consec_norm"] = _norm_consecutivo_series(df_od2[config.OD2_COL_CONSECUTIVO])

    # Construir mapping consecutivo → llave desde Swift
    consec_to_llave: Dict[str, str] = {}
    conflicts_swift = 0

    for _, r in df_swift_all.iterrows():
        llave = str(r.get("Llave", "")).strip()
        if not llave:
            continue
        for c in _extract_consecutivos(r.get("Formulario")):
            if c not in consec_to_llave:
                consec_to_llave[c] = llave
            elif consec_to_llave[c] != llave:
                conflicts_swift += 1

    if not consec_to_llave:
        LOGGER.info("PASO 5: No se encontraron consecutivos válidos en Swift.Formulario.")
        return

    updated = conflicts_od = 0

    def _apply_row(row):
        nonlocal updated, conflicts_od
        c = row["_consec_norm"]
        if not c:
            return row
        new_llave = consec_to_llave.get(c, "")
        if not new_llave:
            return row
        cur = str(row.get(config.OD2_COL_LLAVE_OD, "")).strip()
        if not cur:
            row[config.OD2_COL_LLAVE_OD] = new_llave
            updated += 1
        elif cur != new_llave:
            conflicts_od += 1
        return row

    df_od2 = df_od2.apply(_apply_row, axis=1)
    df_od2 = df_od2.drop(columns=["_consec_norm"], errors="ignore")

    LOGGER.info(
        f"PASO 5 → actualizado={updated} | "
        f"conflicts_swift={conflicts_swift} | conflicts_od={conflicts_od}"
    )

    with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        df_od2.to_excel(writer, sheet_name=config.SHEET_OD_ORIGEN, index=False)

    LOGGER.info(f"origenDestino.xlsx guardado ({config.SHEET_OD_ORIGEN} actualizado).")


# =========================================================
# FUNCIÓN PRINCIPAL — llamada desde main.py
# =========================================================
def run_cruce_completo() -> Dict:
    """
    Ejecuta los 5 pasos del cruce de formularios y llaves.

    Retorna dict con estadísticas para PipelineResult:
        formularios, llaves
    """
    LOGGER.info("=== INICIO CRUCE FORMULARIOS + LLAVE ===")

    validate_input_files(
        config.XLSB_CUENTA_COM,
        config.SWIFT_COMPLETOS,
        config.ORIGEN_DESTINO,
        context="run_formulario",
    )

    stats = {"formularios": 0, "llaves": 0}

    # Paso 1 + 2: Leer y filtrar COM
    df_com = read_com_sheet(config.XLSB_CUENTA_COM, config.SHEET_COM)
    df_com_filtrado = filter_com_df(df_com)

    if df_com_filtrado.empty:
        LOGGER.warning("COM filtrada vacía. No se puede ejecutar cruce.")
        return stats

    # Leer Swift_completos
    df_v1 = read_sheet_safe(config.SWIFT_COMPLETOS, config.SHEET_V1, context="cruces")
    df_v2 = read_sheet_safe(config.SWIFT_COMPLETOS, config.SHEET_V2, context="cruces")

    # Asegurar tipos object en columnas clave (evita errores de dtype)
    for df in (df_v1, df_v2):
        for col in ("Formulario", "Llave"):
            if col in df.columns:
                df[col] = df[col].astype(object)

    # Paso 3: Formulario
    LOGGER.info("Paso 3: Cruce Formulario (COM → Swift)...")
    df_v1 = _update_formulario_for_sheet(df_v1, df_com_filtrado)
    df_v2 = _update_formulario_for_sheet(df_v2, df_com_filtrado)

    forms_asignados = (
        df_v1["Formulario"].replace("", pd.NA).notna().sum()
        + df_v2["Formulario"].replace("", pd.NA).notna().sum()
    )
    stats["formularios"] = int(forms_asignados)

    # Paso 4: Llave
    LOGGER.info("Paso 4: Cruce Llave (origenDestino → Swift)...")
    od_map = _read_od_mapping(config.ORIGEN_DESTINO)
    df_v1  = _apply_llave_to_sheet(df_v1, od_map)
    df_v2  = _apply_llave_to_sheet(df_v2, od_map)

    llaves_asignadas = (
        df_v1["Llave"].replace("", pd.NA).notna().sum()
        + df_v2["Llave"].replace("", pd.NA).notna().sum()
    )
    stats["llaves"] = int(llaves_asignadas)

    # Guardar Swift_completos actualizado (una sola escritura)
    write_sheets(
        config.SWIFT_COMPLETOS,
        {config.SHEET_V1: df_v1, config.SHEET_V2: df_v2},
        context="run_formulario",
    )

    # Paso 5: Llave Origen Destino
    LOGGER.info("Paso 5: Actualizando Llave Origen Destino en origenDestino.xlsx...")
    df_swift_all = pd.concat([df_v1, df_v2], ignore_index=True)
    _update_od_llave(config.ORIGEN_DESTINO, df_swift_all)

    LOGGER.info(
        f"=== FIN CRUCE ===  "
        f"Formularios={stats['formularios']} | Llaves={stats['llaves']}"
    )
    return stats


# =========================================================
# MAIN — ejecución standalone
# =========================================================
if __name__ == "__main__":
    run_cruce_completo()
