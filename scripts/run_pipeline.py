# -*- coding: utf-8 -*-
"""
run_pipeline.py — Pipeline principal de extracción y enriquecimiento

Flujo completo:
  1) Extracción OCR → reader_pdf_V1 + reader_pdf_V2  (con soporte de caché)
  2) Enriquecimiento Proveedor → fuzzy match contra Bd Proveedores
  3) Enriquecimiento Pais/Ciudad → cruce Receiver vs Bd Swift
  4) Nombre personalizado, mayúsculas, limpieza Amount
  5) Recálculo de Estado (Completo / Incompleto)
  6) ID determinístico por registro (uuid5)
  7) Separación → Swift_completos.xlsx  (solo completos)
                → Swift_manuales.xlsx   (solo incompletos)

Escritura Excel:
  - Todas las lecturas usan engine="openpyxl" (o pyxlsb para .xlsb)
  - Todas las escrituras usan write_sheets() de excel_utils
    que internamente usa pd.ExcelWriter(engine="openpyxl")
"""

from __future__ import annotations

import re
import unicodedata
import uuid
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd

import config
from core.logger import get_logger
from core.text_utils import (
    clean_amount_value,
    normalize_swift_11,
    build_nombre_personalizado,
)
from core.excel_utils import write_sheets, reorder_columns
from core.validators import validate_input_files, validate_output_dirs

from scripts.reader_pdf_V1 import process_folder as process_folder_v1
from scripts.reader_pdf_V2 import process_folder_v2
from scripts.reader_pdf_V1_exp import process_folder_v1_exp
from scripts.reader_pdf_V2_exp import process_folder_v2_exp

LOGGER = get_logger(__name__)


# =========================================================
# DESCUBRIMIENTO DE PDFs POR VERSIÓN (fuente de red)
# =========================================================
_MESES_ES: Dict[str, int] = {
    "ENERO": 1, "FEBRERO": 2, "MARZO": 3, "ABRIL": 4,
    "MAYO": 5, "JUNIO": 6, "JULIO": 7, "AGOSTO": 8,
    "SEPTIEMBRE": 9, "SETIEMBRE": 9, "OCTUBRE": 10,
    "NOVIEMBRE": 11, "DICIEMBRE": 12,
}


def _parse_mes_carpeta(nombre: str) -> Optional[int]:
    nombre = nombre.strip().upper()
    for mes_nombre, mes_num in _MESES_ES.items():
        if mes_nombre in nombre:
            return mes_num
    return None


def _parse_fecha_carpeta_dia(nombre: str, anio: int) -> Optional[date]:
    import re as _re
    nombre = nombre.strip().upper()
    m = _re.match(r"^(\d{1,2})\s+([A-ZÁÉÍÓÚÑ]+)$", nombre)
    if not m:
        return None
    dia_str, mes_str = m.group(1), m.group(2)
    mes_num = _MESES_ES.get(mes_str)
    if not mes_num:
        return None
    try:
        return date(anio, mes_num, int(dia_str))
    except ValueError:
        return None


def _descubrir_pdfs_por_version(
    raiz: Path,
    corte_v2: date,
    anio: int,
    fecha_desde: Optional[date] = None,
) -> tuple:
    if not raiz.exists():
        LOGGER.warning(
            f"DIR_SWIFT_RAIZ no existe o no es accesible: {raiz}\n"
            "Se usarán las carpetas locales DIR_PDFS_V1 / DIR_PDFS_V2 como fallback."
        )
        return [], []

    pdfs_v1: List[tuple] = []
    pdfs_v2: List[tuple] = []
    sin_fecha: List[Path] = []
    carpetas_dia_totales = 0

    for carpeta_mes in sorted(raiz.iterdir()):
        if not carpeta_mes.is_dir():
            continue
        mes_num = _parse_mes_carpeta(carpeta_mes.name)
        if mes_num is None:
            continue

        for carpeta_dia in sorted(carpeta_mes.iterdir()):
            if not carpeta_dia.is_dir():
                continue
            fecha = _parse_fecha_carpeta_dia(carpeta_dia.name, anio)
            if fecha is None:
                continue
            if fecha_desde is not None and fecha < fecha_desde:
                continue

            carpetas_dia_totales += 1
            pdfs = sorted(carpeta_dia.glob("*.pdf"))
            if not pdfs:
                continue

            for pdf in pdfs:
                if fecha < corte_v2:
                    pdfs_v1.append((fecha, pdf))
                else:
                    pdfs_v2.append((fecha, pdf))

    pdfs_v1_sorted = [p for _, p in sorted(pdfs_v1, key=lambda x: (x[0], x[1].name))]
    pdfs_v2_sorted = [p for _, p in sorted(pdfs_v2, key=lambda x: (x[0], x[1].name))]

    LOGGER.info(
        f"Descubrimiento PDF en red → "
        f"carpetas_día={carpetas_dia_totales} | "
        f"V1={len(pdfs_v1_sorted)} | V2={len(pdfs_v2_sorted)}"
    )
    return pdfs_v1_sorted, pdfs_v2_sorted


# =========================================================
# DESCUBRIMIENTO EXP — carpetas de mes planas, fecha en nombre PDF
# =========================================================
def _parse_fecha_pdf_exp(nombre_pdf: str, anio: int) -> Optional[date]:
    """
    Parsea la fecha desde el nombre del PDF de Exportaciones.
    Formatos soportados:
      05112025.pdf    → date(2025, 11, 5)
      05112025 2.pdf  → date(2025, 11, 5)   (sufijo numérico ignorado)
      27112025.pdf    → date(2025, 11, 27)
    """
    stem = Path(nombre_pdf).stem.strip()
    # Extraer solo dígitos del inicio (ignora sufijos como " 2", " 3")
    m = re.match(r"^(\d{8})", stem.replace(" ", ""))
    if not m:
        return None
    digits = m.group(1)   # DDMMYYYY
    try:
        dia  = int(digits[0:2])
        mes  = int(digits[2:4])
        anio_ = int(digits[4:8])
        return date(anio_, mes, dia)
    except (ValueError, IndexError):
        return None


def _descubrir_pdfs_exp(
    raiz: Path,
    corte_v2: date,
    anio: int,
    fecha_desde: Optional[date] = None,
) -> tuple:
    """
    Recorre la estructura de Exportaciones:
        raiz/
          Abril/       05042025.pdf  06042025.pdf ...
          Mayo/        ...
          Noviembre/   05112025.pdf  26112025.pdf  26112025 2.pdf ...

    - No hay subcarpetas de día (PDFs directamente en la carpeta del mes)
    - La fecha se extrae del nombre del archivo (DDMMYYYY)
    - PDFs anteriores a fecha_desde son ignorados
    - Antes de corte_v2 → V1, desde corte_v2 → V2

    Retorna (pdfs_v1: List[Path], pdfs_v2: List[Path]) ordenados por fecha y nombre.
    """
    if not raiz.exists():
        LOGGER.warning(
            f"DIR_SWIFT_RAIZ_EXP no existe o no es accesible: {raiz}\n"
            "Se usarán carpetas locales DIR_PDFS_V1_EXP / DIR_PDFS_V2_EXP como fallback."
        )
        return [], []

    pdfs_v1: List[tuple] = []
    pdfs_v2: List[tuple] = []
    sin_fecha = 0

    for carpeta_mes in sorted(raiz.iterdir()):
        if not carpeta_mes.is_dir():
            continue
        mes_num = _parse_mes_carpeta(carpeta_mes.name)
        if mes_num is None:
            LOGGER.debug(f"[EXP] Carpeta de mes no reconocida: {carpeta_mes.name}")
            continue

        for pdf_path in sorted(carpeta_mes.glob("*.pdf")):
            fecha = _parse_fecha_pdf_exp(pdf_path.name, anio)

            if fecha is None:
                # Fallback: construir fecha aproximada desde mes de la carpeta
                try:
                    fecha = date(anio, mes_num, 1)
                except ValueError:
                    sin_fecha += 1
                    continue

            if fecha_desde is not None and fecha < fecha_desde:
                continue

            if fecha < corte_v2:
                pdfs_v1.append((fecha, pdf_path))
            else:
                pdfs_v2.append((fecha, pdf_path))

    pdfs_v1_sorted = [p for _, p in sorted(pdfs_v1, key=lambda x: (x[0], x[1].name))]
    pdfs_v2_sorted = [p for _, p in sorted(pdfs_v2, key=lambda x: (x[0], x[1].name))]

    LOGGER.info(
        f"[EXP] Descubrimiento PDF → "
        f"V1={len(pdfs_v1_sorted)} | V2={len(pdfs_v2_sorted)} | "
        f"sin_fecha={sin_fecha}"
    )
    return pdfs_v1_sorted, pdfs_v2_sorted


# =========================================================
# TOKENS A IGNORAR EN FUZZY MATCHING DE PROVEEDORES
# =========================================================
LEGAL_TOKENS = {
    "SA", "SAS", "S.A", "S.A.S", "S A", "S A S",
    "LTDA", "LTD", "LTD.", "LIMITED",
    "CO", "CO.", "COMPANY",
    "INC", "INC.", "CORP", "CORPORATION",
    "LLC", "SPA", "S.P.A",
    "GMBH", "BV", "B.V", "SRL", "S.R.L", "AG", "NV",
}

COUNTRY_TOKENS = {
    "CHINA", "TURKEY", "COLOMBIA", "PANAMA",
    "US", "USA", "ESPANA", "SPAIN",
}


# =========================================================
# NORMALIZACIÓN DE NOMBRES DE PROVEEDORES
# =========================================================
def _strip_accents(s: str) -> str:
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _normalize_name(s: str) -> str:
    if not s:
        return ""
    s = str(s).strip()
    if not s:
        return ""
    s = _strip_accents(s).upper()
    s = re.sub(r"[^A-Z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    tokens = [t for t in s.split() if t not in LEGAL_TOKENS and t not in COUNTRY_TOKENS]
    return " ".join(tokens).strip() or s


def _get_best_match(
    query: str,
    choices_norm: List[str],
    choices_raw: List[str],
) -> Tuple[Optional[str], int]:
    q_norm = _normalize_name(query)
    if not q_norm:
        return None, 0

    try:
        from rapidfuzz import process, fuzz
        res = process.extractOne(q_norm, choices_norm, scorer=fuzz.token_set_ratio)
        if not res:
            return None, 0
        _, score, idx = res
        return choices_raw[idx], int(score)
    except ImportError:
        import difflib
        best_score = 0
        best_idx   = None
        for i, cn in enumerate(choices_norm):
            score = int(difflib.SequenceMatcher(None, q_norm, cn).ratio() * 100)
            if score > best_score:
                best_score = score
                best_idx   = i
        if best_idx is None:
            return None, 0
        return choices_raw[best_idx], best_score


# =========================================================
# RESULTADOS OCR → DATAFRAME
# =========================================================
def _results_to_df(results: List[Dict], version_name: str) -> pd.DataFrame:
    rows = []
    for r in results:
        receiver  = r.get("receiver")
        date_     = r.get("date")
        amount_   = r.get("amount")
        proveedor = r.get("beneficiary")
        estado = "Completo" if (receiver and date_ and amount_ and proveedor) else "Incompleto"
        rows.append({
            "Nombre archivo": r.get("file_name"),
            "Receiver":       receiver,
            "Date":           date_,
            "Amount":         amount_,
            "Proveedor":      proveedor,
            "Estado":         estado,
            "Version":        version_name,
        })
    return pd.DataFrame(rows, columns=[
        "Nombre archivo", "Receiver", "Date", "Amount",
        "Proveedor", "Estado", "Version",
    ])


# =========================================================
# ENRIQUECIMIENTO 1: PROVEEDOR (fuzzy contra Bd Proveedores)
# =========================================================
def _enrich_proveedor(
    df: pd.DataFrame,
    bd_path: Path,
    threshold: int = config.FUZZY_THRESHOLD,
) -> pd.DataFrame:
    if not bd_path.exists():
        LOGGER.warning(f"Bd Proveedores no encontrada: {bd_path}. Se omite enriquecimiento.")
        return df

    # engine="openpyxl" explícito para evitar deprecation warnings y garantizar consistencia
    bd = pd.read_excel(bd_path, engine="openpyxl")
    bd.columns = [str(c).strip() for c in bd.columns]

    if config.BD_PROV_COL_NOMBRE not in bd.columns:
        raise KeyError(
            f"No se encontró columna '{config.BD_PROV_COL_NOMBRE}' en {bd_path.name}. "
            f"Columnas disponibles: {list(bd.columns)}"
        )

    choices_raw: List[str] = (
        bd[config.BD_PROV_COL_NOMBRE]
        .dropna()
        .astype(str)
        .str.strip()
        .loc[lambda s: s != ""]
        .drop_duplicates()
        .tolist()
    )
    choices_norm = [_normalize_name(x) for x in choices_raw]

    if not choices_raw:
        LOGGER.warning("Bd Proveedores sin candidatos válidos. Se omite enriquecimiento.")
        return df

    out      = df.copy()
    replaced = 0
    reviewed = 0

    # Valores que representan ausencia de proveedor — no enriquecer
    _EMPTY_PROV = {"", "nan", "none", "nat", "n/a", "nd", "null"}

    for idx, prov in out["Proveedor"].items():
        prov_str = str(prov).strip().lower() if prov is not None else ""
        if not prov_str or prov_str in _EMPTY_PROV:
            continue
        prov = str(prov).strip()  # versión limpia con capitalización original
        reviewed += 1
        best, score = _get_best_match(str(prov), choices_norm, choices_raw)
        if best and score >= threshold and best != prov:
            out.at[idx, "Proveedor"] = best
            replaced += 1
        if config.DEBUG and reviewed <= 10:
            LOGGER.debug(f"Fuzzy: '{prov}' → '{best}' | score={score}")

    LOGGER.info(
        f"Enriquecimiento Proveedor: revisados={reviewed} | "
        f"reemplazados={replaced} | threshold={threshold}"
    )
    return out


# =========================================================
# ENRIQUECIMIENTO 2: PAIS / CIUDAD (cruce con Bd Swift)
# =========================================================
def _read_bd_swift(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"No existe Bd Swift: {path}")

    needed = {
        config.BD_SWIFT_COL_CODIGO,
        config.BD_SWIFT_COL_PAIS,
        config.BD_SWIFT_COL_CIUDAD,
    }

    # engine="openpyxl" explícito para consistencia
    xls = pd.ExcelFile(path, engine="openpyxl")
    for sh in xls.sheet_names:
        df = pd.read_excel(path, sheet_name=sh, engine="openpyxl")
        df.columns = [str(c).strip() for c in df.columns]
        if needed.issubset(set(df.columns)):
            LOGGER.info(f"Bd Swift: usando hoja '{sh}'")
            return df

    raise KeyError(
        f"No se encontró hoja con columnas requeridas: {needed}. "
        f"Hojas disponibles: {xls.sheet_names}"
    )


def _build_bd_swift_norm(bd_swift_path: Path) -> pd.DataFrame:
    bd = _read_bd_swift(bd_swift_path)
    out = bd[[
        config.BD_SWIFT_COL_CODIGO,
        config.BD_SWIFT_COL_PAIS,
        config.BD_SWIFT_COL_CIUDAD,
    ]].copy()
    out.rename(columns={config.BD_SWIFT_COL_CODIGO: "swift_original"}, inplace=True)
    out["swift_norm"] = out["swift_original"].apply(normalize_swift_11)
    out = out.loc[out["swift_norm"] != ""].copy()
    out = out.drop_duplicates(subset=["swift_norm"], keep="first")
    return out


def _apply_swift_country_city(
    df: pd.DataFrame,
    bd_swift_norm: pd.DataFrame,
) -> pd.DataFrame:
    out = df.copy()
    out["receiver_norm"] = out["Receiver"].apply(normalize_swift_11)

    bd_map = bd_swift_norm[[
        "swift_norm",
        config.BD_SWIFT_COL_PAIS,
        config.BD_SWIFT_COL_CIUDAD,
    ]].rename(columns={
        config.BD_SWIFT_COL_PAIS:   "__pais_bd",
        config.BD_SWIFT_COL_CIUDAD: "__ciudad_bd",
    })

    out = out.merge(bd_map, how="left", left_on="receiver_norm", right_on="swift_norm")
    out["Pais"]   = out["__pais_bd"]
    out["Ciudad"] = out["__ciudad_bd"]
    out = out.drop(
        columns=["receiver_norm", "swift_norm", "__pais_bd", "__ciudad_bd"],
        errors="ignore",
    )
    matches = out["Pais"].notna().sum()
    LOGGER.info(f"Cruce Bd Swift: {matches}/{len(out)} coincidencias")
    return out


# =========================================================
# POST-PROCESO
# =========================================================
def _recortar_nombre_personalizado(valor, limite: int = 50):
    if pd.isna(valor):
        return valor
    val = str(valor).strip()
    if len(val) <= limite:
        return val
    partes = val.rsplit(" ", 1)
    if len(partes) != 2:
        return val
    nombre, swift = partes[0], partes[1]
    palabras = nombre.split(" ")
    while len(" ".join(palabras) + " " + swift) > limite and len(palabras) > 1:
        palabras.pop()
    return " ".join(palabras) + " " + swift


def _add_nombre_personalizado(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["Nombre personalizado"] = out.apply(
        lambda r: build_nombre_personalizado(r.get("Proveedor"), r.get("Receiver")),
        axis=1,
    )
    out["Nombre personalizado"] = out["Nombre personalizado"].apply(
        lambda v: _recortar_nombre_personalizado(v, limite=50)
    )
    out.loc[out["Nombre personalizado"] == "", "Nombre personalizado"] = pd.NA
    return out


def _upper_pais_ciudad(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ("Pais", "Ciudad"):
        if col in out.columns:
            out[col] = out[col].astype("string").str.strip().str.upper()
    return out


def _clean_amount_column(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "Amount" in out.columns:
        out["Amount"] = out["Amount"].apply(clean_amount_value)
        out.loc[out["Amount"].astype(str).str.strip() == "", "Amount"] = pd.NA
    return out


def _recalc_estado(df: pd.DataFrame) -> pd.DataFrame:
    def _is_completo(r) -> bool:
        return all([
            pd.notna(r.get("Receiver"))  and str(r.get("Receiver")).strip()  != "",
            pd.notna(r.get("Date"))      and str(r.get("Date")).strip()      != "",
            pd.notna(r.get("Amount"))    and str(r.get("Amount")).strip()    != "",
            pd.notna(r.get("Proveedor")) and str(r.get("Proveedor")).strip() != "",
        ])
    out = df.copy()
    out["Estado"] = out.apply(lambda r: "Completo" if _is_completo(r) else "Incompleto", axis=1)
    return out


# =========================================================
# ID DETERMINÍSTICO
# =========================================================
def _make_record_id(version: str, file_name: str) -> str:
    base = f"origen_destino_dian|{version}|{file_name}".strip()
    return str(uuid.uuid5(uuid.NAMESPACE_URL, base))


def _add_ids_and_tail_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["id"] = out.apply(
        lambda r: _make_record_id(
            str(r.get("Version", "")),
            str(r.get("Nombre archivo", "")),
        ),
        axis=1,
    )
    out["Formulario"] = pd.NA
    out["Llave"]      = pd.NA
    cols = ["id"] + [c for c in out.columns if c != "id"]
    return out[cols]


# =========================================================
# EXPORT EXCEL
# =========================================================
def _ensure_final_columns(df: pd.DataFrame) -> pd.DataFrame:
    return reorder_columns(df, config.FINAL_COLUMNS_ORDER)


def _write_swift_excel(
    df_v1: pd.DataFrame,
    df_v2: pd.DataFrame,
    output_path: Path,
    label: str = "",
) -> None:
    v1 = _ensure_final_columns(df_v1)
    v2 = _ensure_final_columns(df_v2)
    # write_sheets usa pd.ExcelWriter(engine="openpyxl") internamente
    write_sheets(output_path, {config.SHEET_V1: v1, config.SHEET_V2: v2}, context=label)


# =========================================================
# FUNCIÓN PRINCIPAL — llamada desde main.py
# =========================================================
def run_pipeline_completo(cache=None, debug: bool = False, tipo: str = "imp") -> Dict:
    """
    Ejecuta el pipeline completo de extracción y enriquecimiento.

    Parámetros:
        cache : instancia de PdfCache
        debug : activa logs detallados
        tipo  : "imp" (Importaciones) | "exp" (Exportaciones)

    Retorna dict con: nuevos_v1, nuevos_v2, completos, incompletos, errores
    """
    tipo = tipo.lower().strip()
    if tipo not in ("imp", "exp"):
        raise ValueError(f"tipo debe ser 'imp' o 'exp', recibido: '{tipo}'")

    LOGGER.info(f"=== INICIO PIPELINE {tipo.upper()} ===")

    # ── Configuración según tipo ───────────────────────────
    if tipo == "imp":
        raiz           = config.DIR_SWIFT_RAIZ_IMP
        fecha_desde    = config.SWIFT_FECHA_DESDE_IMP
        dir_v1_local   = config.DIR_PDFS_V1_IMP
        dir_v2_local   = config.DIR_PDFS_V2_IMP
        out_completos  = config.SWIFT_COMPLETOS_IMP
        out_manuales   = config.SWIFT_MANUALES_IMP
        fn_reader_v1   = process_folder_v1
        fn_reader_v2   = process_folder_v2
        fn_discover    = _descubrir_pdfs_por_version
    else:  # exp
        raiz           = config.DIR_SWIFT_RAIZ_EXP
        fecha_desde    = config.SWIFT_FECHA_DESDE_EXP
        dir_v1_local   = config.DIR_PDFS_V1_EXP
        dir_v2_local   = config.DIR_PDFS_V2_EXP
        out_completos  = config.SWIFT_COMPLETOS_EXP
        out_manuales   = config.SWIFT_MANUALES_EXP
        fn_reader_v1   = process_folder_v1_exp
        fn_reader_v2   = process_folder_v2_exp
        fn_discover    = _descubrir_pdfs_exp

    validate_input_files(
        config.BD_PROVEEDORES,
        config.BD_SWIFT,
        context=f"run_pipeline_{tipo}",
    )
    validate_output_dirs(config.DIR_RESULTADOS)

    stats = {
        "nuevos_v1":   0,
        "nuevos_v2":   0,
        "completos":   0,
        "incompletos": 0,
        "errores":     0,
    }

    # ── 1) Descubrir PDFs ──────────────────────────────────
    LOGGER.info(f"Paso 1: Descubriendo PDFs {tipo.upper()} en fuente de red...")

    if raiz.exists():
        pdfs_v1, pdfs_v2 = fn_discover(
            raiz=raiz,
            corte_v2=config.SWIFT_CORTE_V2,
            anio=config.SWIFT_AÑO,
            fecha_desde=fecha_desde,
        )
        fuente_v1 = pdfs_v1
        fuente_v2 = pdfs_v2
    else:
        LOGGER.warning(
            f"Red no disponible [{tipo.upper()}]. Usando carpetas locales: "
            f"V1={dir_v1_local} | V2={dir_v2_local}"
        )
        fuente_v1 = dir_v1_local
        fuente_v2 = dir_v2_local

    # ── 2) Extracción OCR ──────────────────────────────────
    LOGGER.info(f"Paso 2: Extracción V1 [{tipo.upper()}]...")
    results_v1 = fn_reader_v1(fuente_v1, debug=debug, cache=cache)
    df_v1 = _results_to_df(results_v1, version_name="V1")
    stats["nuevos_v1"] = len(results_v1)

    LOGGER.info(f"Paso 2: Extracción V2 [{tipo.upper()}]...")
    results_v2 = fn_reader_v2(fuente_v2, debug=debug, cache=cache)
    df_v2 = _results_to_df(results_v2, version_name="V2")
    stats["nuevos_v2"] = len(results_v2)

    if df_v1.empty and df_v2.empty:
        LOGGER.info("No hay PDFs nuevos que procesar (todos en caché). Pipeline finalizado.")
        return stats

    # ── 3) Enriquecimiento Proveedor ───────────────────────
    LOGGER.info("Paso 2: Enriquecimiento Proveedor (fuzzy)...")
    df_v1 = _enrich_proveedor(df_v1, config.BD_PROVEEDORES, threshold=config.FUZZY_THRESHOLD)
    df_v2 = _enrich_proveedor(df_v2, config.BD_PROVEEDORES, threshold=config.FUZZY_THRESHOLD)

    # ── 4) Cruce Bd Swift → País / Ciudad ──────────────────
    LOGGER.info("Paso 3: Cruce Bd Swift (País/Ciudad)...")
    bd_swift_norm = _build_bd_swift_norm(config.BD_SWIFT)
    df_v1 = _apply_swift_country_city(df_v1, bd_swift_norm)
    df_v2 = _apply_swift_country_city(df_v2, bd_swift_norm)

    # ── 5) Post-proceso ────────────────────────────────────
    LOGGER.info("Paso 4: Post-proceso (Nombre personalizado, mayúsculas, Amount)...")
    for apply_fn in [_add_nombre_personalizado, _upper_pais_ciudad, _clean_amount_column]:
        df_v1 = apply_fn(df_v1)
        df_v2 = apply_fn(df_v2)

    # ── 6) Recálculo de estado ─────────────────────────────
    df_v1 = _recalc_estado(df_v1)
    df_v2 = _recalc_estado(df_v2)

    # ── 7) IDs + columnas finales ──────────────────────────
    LOGGER.info("Paso 5: Generando IDs y columnas finales...")
    df_v1 = _add_ids_and_tail_cols(df_v1)
    df_v2 = _add_ids_and_tail_cols(df_v2)

    # ── 8) Separar completos / incompletos ─────────────────
    df_v1_comp   = df_v1.loc[df_v1["Estado"] == "Completo"].copy()
    df_v2_comp   = df_v2.loc[df_v2["Estado"] == "Completo"].copy()
    df_v1_incomp = df_v1.loc[df_v1["Estado"] == "Incompleto"].copy()
    df_v2_incomp = df_v2.loc[df_v2["Estado"] == "Incompleto"].copy()

    total_comp   = len(df_v1_comp) + len(df_v2_comp)
    total_incomp = len(df_v1_incomp) + len(df_v2_incomp)
    total_error  = sum(1 for r in results_v1 + results_v2 if r.get("estado") == "Error")

    LOGGER.info(f"V1 → Completos: {len(df_v1_comp)} | Incompletos: {len(df_v1_incomp)}")
    LOGGER.info(f"V2 → Completos: {len(df_v2_comp)} | Incompletos: {len(df_v2_incomp)}")

    # ── 9) Exportar ────────────────────────────────────────
    LOGGER.info("Paso 6: Exportando Swift_completos y Swift_manuales...")
    _write_swift_excel(df_v1_comp,   df_v2_comp,   out_completos, label=f"completos_{tipo}")
    _write_swift_excel(df_v1_incomp, df_v2_incomp, out_manuales,  label=f"manuales_{tipo}")

    stats["completos"]   = total_comp
    stats["incompletos"] = total_incomp
    stats["errores"]     = total_error

    LOGGER.info(
        f"=== FIN PIPELINE ===  "
        f"Completos:{total_comp} | Incompletos:{total_incomp} | Errores:{total_error}"
    )
    return stats


# =========================================================
# MAIN — ejecución standalone
# =========================================================
if __name__ == "__main__":
    from core.cache import PdfCache
    cache = PdfCache(config.CACHE_FILE)
    run_pipeline_completo(cache=cache, debug=config.DEBUG)
    cache.save()