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

CAMBIOS vs versión anterior:
  - Rutas centralizadas en config.py (eliminadas todas las hardcodeadas)
  - Logging via core.logger (sin handlers duplicados)
  - clean_amount_value → core.text_utils.clean_amount_value (mejorado EU/US)
  - normalize_swift_11 → core.text_utils.normalize_swift_11
  - ProveedorMatcher   → core.text_utils.ProveedorMatcher (encapsulado y testeable)
  - excel_utils.write_sheets para escritura robusta
  - Función run_pipeline_completo() para ser llamada desde main.py
  - normalize_name / LEGAL_TOKENS / COUNTRY_TOKENS mantenidos aquí
    (son específicos del dominio de matching de proveedores)
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

LOGGER = get_logger(__name__)


# =========================================================
# DESCUBRIMIENTO DE PDFs POR VERSIÓN (fuente de red)
# =========================================================

# Mapa de nombres de meses en español a número (soporta abreviaciones comunes)
_MESES_ES: Dict[str, int] = {
    "ENERO": 1, "FEBRERO": 2, "MARZO": 3, "ABRIL": 4,
    "MAYO": 5, "JUNIO": 6, "JULIO": 7, "AGOSTO": 8,
    "SEPTIEMBRE": 9, "SETIEMBRE": 9, "OCTUBRE": 10,
    "NOVIEMBRE": 11, "DICIEMBRE": 12,
}

def _parse_mes_carpeta(nombre: str) -> Optional[int]:
    """
    Extrae el número de mes de una carpeta de mes.
    Ejemplos:
      "3. MARZO"      → 3
      "11. NOVIEMBRE" → 11
      "3.MARZO"       → 3   (sin espacio)
    Retorna None si no reconoce el formato.
    """
    nombre = nombre.strip().upper()
    for mes_nombre, mes_num in _MESES_ES.items():
        if mes_nombre in nombre:
            return mes_num
    return None

def _parse_fecha_carpeta_dia(nombre: str, anio: int) -> Optional[date]:
    """
    Parsea la fecha de una carpeta de día.
    Ejemplos:
      "19 MARZO"      → date(2025, 3, 19)
      "1 ABRIL"       → date(2025, 4, 1)
      "20 NOVIEMBRE"  → date(2025, 11, 20)
    Retorna None si no reconoce el formato.
    """
    import re as _re
    nombre = nombre.strip().upper()
    # Patrón: uno o dos dígitos seguidos de nombre de mes
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

def _descubrir_pdfs_por_version(raiz: Path, corte_v2: date, anio: int, fecha_desde: Optional[date] = None,) -> tuple:
    """
    Recorre DIR_SWIFT_RAIZ buscando PDFs en subcarpetas mes/día y los clasifica
    en V1 (antes de corte_v2) o V2 (desde corte_v2 inclusive).

    Estructura esperada:
        raiz/
          3. MARZO/
            19 MARZO/  *.pdf
            20 MARZO/  *.pdf
          11. NOVIEMBRE/
            19 NOVIEMBRE/  *.pdf   ← V1 (< 20-nov)
            20 NOVIEMBRE/  *.pdf   ← V2 (>= 20-nov)

    fecha_desde: si se indica, ignora carpetas con fecha anterior a ese día.
                 Permite arrancar desde abril 2025 sin procesar meses anteriores.

    Retorna (pdfs_v1: List[Path], pdfs_v2: List[Path]).
    Ordena cada lista por fecha de carpeta y luego por nombre de archivo.
    """
    if not raiz.exists():
        LOGGER.warning(
            f"DIR_SWIFT_RAIZ no existe o no es accesible: {raiz}\n"
            "Se usarán las carpetas locales DIR_PDFS_V1 / DIR_PDFS_V2 como fallback."
        )
        return [], []

    pdfs_v1: List[tuple] = []   # (fecha, path)
    pdfs_v2: List[tuple] = []

    sin_fecha: List[Path] = []
    carpetas_dia_totales = 0

    # Nivel 1: carpetas de mes
    for carpeta_mes in sorted(raiz.iterdir()):
        if not carpeta_mes.is_dir():
            continue
        mes_num = _parse_mes_carpeta(carpeta_mes.name)
        if mes_num is None:
            LOGGER.debug(f"Carpeta de mes no reconocida, se omite: {carpeta_mes.name}")
            continue

        # Nivel 2: carpetas de día
        for carpeta_dia in sorted(carpeta_mes.iterdir()):
            if not carpeta_dia.is_dir():
                continue

            fecha = _parse_fecha_carpeta_dia(carpeta_dia.name, anio)
            if fecha is None:
                LOGGER.debug(f"Carpeta de día no reconocida, se omite: {carpeta_dia.name}")
                continue

            # Ignorar carpetas anteriores a fecha_desde
            if fecha_desde is not None and fecha < fecha_desde:
                LOGGER.debug(f"Carpeta omitida (anterior a fecha_desde): {carpeta_dia.name}")
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

    # Ordenar por fecha y luego por nombre
    pdfs_v1_sorted = [p for _, p in sorted(pdfs_v1, key=lambda x: (x[0], x[1].name))]
    pdfs_v2_sorted = [p for _, p in sorted(pdfs_v2, key=lambda x: (x[0], x[1].name))]

    LOGGER.info(
        f"Descubrimiento PDF en red → "
        f"carpetas_día={carpetas_dia_totales} | "
        f"V1={len(pdfs_v1_sorted)} | V2={len(pdfs_v2_sorted)}"
    )
    if sin_fecha:
        LOGGER.warning(f"Carpetas no parseadas ({len(sin_fecha)}): {[p.name for p in sin_fecha[:5]]}")

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
    """
    Normaliza nombres de proveedores para fuzzy matching:
    elimina tildes, signos, tokens legales y de país.
    """
    if not s:
        return ""
    s = str(s).strip()
    if not s:
        return ""

    s = _strip_accents(s).upper()
    s = re.sub(r"[^A-Z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    tokens = [
        t for t in s.split()
        if t not in LEGAL_TOKENS and t not in COUNTRY_TOKENS
    ]
    return " ".join(tokens).strip() or s

def _get_best_match(query: str, choices_norm: List[str], choices_raw: List[str],) -> Tuple[Optional[str], int]:
    """
    Retorna (mejor_candidato_raw, score 0-100).
    Usa rapidfuzz si está disponible, difflib como fallback.
    """
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
    """Convierte la lista de dicts de extracción OCR a DataFrame."""
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
def _enrich_proveedor(df: pd.DataFrame, bd_path: Path, threshold: int = config.FUZZY_THRESHOLD,) -> pd.DataFrame:
    """
    Reemplaza el Proveedor extraído por OCR con el nombre canónico
    de la Bd Proveedores si el score fuzzy supera el threshold.
    """
    if not bd_path.exists():
        LOGGER.warning(f"Bd Proveedores no encontrada: {bd_path}. Se omite enriquecimiento.")
        return df

    bd = pd.read_excel(bd_path)
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

    for idx, prov in out["Proveedor"].items():
        if not prov or str(prov).strip() == "":
            continue

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
    """Lee la Bd Swift detectando automáticamente la hoja correcta."""
    if not path.exists():
        raise FileNotFoundError(f"No existe Bd Swift: {path}")

    needed = {
        config.BD_SWIFT_COL_CODIGO,
        config.BD_SWIFT_COL_PAIS,
        config.BD_SWIFT_COL_CIUDAD,
    }

    xls = pd.ExcelFile(path)
    for sh in xls.sheet_names:
        df = pd.read_excel(path, sheet_name=sh)
        df.columns = [str(c).strip() for c in df.columns]
        if needed.issubset(set(df.columns)):
            LOGGER.info(f"Bd Swift: usando hoja '{sh}'")
            return df

    raise KeyError(
        f"No se encontró hoja con columnas requeridas: {needed}. "
        f"Hojas disponibles: {xls.sheet_names}"
    )

def _build_bd_swift_norm(bd_swift_path: Path) -> pd.DataFrame:
    """Construye tabla normalizada de SWIFT → País / Ciudad."""
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

def _apply_swift_country_city(df: pd.DataFrame, bd_swift_norm: pd.DataFrame,) -> pd.DataFrame:
    """Agrega columnas Pais y Ciudad mediante merge con Bd Swift normalizada."""
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
def _add_nombre_personalizado(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["Nombre personalizado"] = out.apply(
        lambda r: build_nombre_personalizado(r.get("Proveedor"), r.get("Receiver")),
        axis=1,
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
    """Aplica clean_amount_value (versión mejorada de core) a toda la columna."""
    out = df.copy()
    if "Amount" in out.columns:
        out["Amount"] = out["Amount"].apply(clean_amount_value)
        out.loc[out["Amount"].astype(str).str.strip() == "", "Amount"] = pd.NA
    return out


def _recalc_estado(df: pd.DataFrame) -> pd.DataFrame:
    """Recalcula Estado con Amount ya limpio."""
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
    """
    ID uuid5 basado en (version + file_name).
    - Estable entre ejecuciones para el mismo PDF.
    - No colisiona entre V1 y V2 con igual nombre de archivo.
    """
    base = f"origen_destino_dian|{version}|{file_name}".strip()
    return str(uuid.uuid5(uuid.NAMESPACE_URL, base))


def _add_ids_and_tail_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega columna 'id' al inicio y columnas vacías 'Formulario' y 'Llave' al final."""
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

    # id al inicio
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
    write_sheets(output_path, {config.SHEET_V1: v1, config.SHEET_V2: v2}, context=label)

# =========================================================
# FUNCIÓN PRINCIPAL — llamada desde main.py
# =========================================================
def run_pipeline_completo(cache=None, debug: bool = False,) -> Dict:
    """
    Ejecuta el pipeline completo de extracción y enriquecimiento.

    Parámetros:
        cache:  instancia de PdfCache (omite PDFs ya procesados si se pasa)
        debug:  activa logs detallados de OCR y matching

    Retorna dict con estadísticas para PipelineResult en main.py:
        nuevos_v1, nuevos_v2, completos, incompletos, errores
    """
    LOGGER.info("=== INICIO PIPELINE EXTRACCIÓN ===")

    # Validar entradas antes de empezar
    validate_input_files(
        config.DIR_PDFS_V1,
        config.DIR_PDFS_V2,
        config.BD_PROVEEDORES,
        config.BD_SWIFT,
        context="run_pipeline",
    )
    validate_output_dirs(config.DIR_RESULTADOS)

    stats = {
        "nuevos_v1":   0,
        "nuevos_v2":   0,
        "completos":   0,
        "incompletos": 0,
        "errores":     0,
    }

    # ── 1) Descubrir PDFs desde la red (o fallback local) ────
    LOGGER.info("Paso 1: Descubriendo PDFs en fuente de red...")

    if config.DIR_SWIFT_RAIZ.exists():
        # Fuente principal: red corporativa con estructura mes/día
        pdfs_v1, pdfs_v2 = _descubrir_pdfs_por_version(
            raiz=config.DIR_SWIFT_RAIZ,
            corte_v2=config.SWIFT_CORTE_V2,
            anio=config.SWIFT_AÑO,
            fecha_desde=config.SWIFT_FECHA_DESDE,
        )
        fuente_v1 = pdfs_v1   # List[Path]
        fuente_v2 = pdfs_v2
    else:
        # Fallback: carpetas locales planas (desarrollo / sin red)
        LOGGER.warning(
            "Red no disponible. Usando carpetas locales: "
            f"V1={config.DIR_PDFS_V1} | V2={config.DIR_PDFS_V2}"
        )
        fuente_v1 = config.DIR_PDFS_V1   # Path
        fuente_v2 = config.DIR_PDFS_V2

    # ── 2) Extracción OCR ──────────────────────────────────
    LOGGER.info("Paso 1: Extracción V1...")
    results_v1 = process_folder_v1(fuente_v1, debug=debug, cache=cache)
    df_v1 = _results_to_df(results_v1, version_name="V1")
    stats["nuevos_v1"] = len(results_v1)

    LOGGER.info("Paso 1: Extracción V2...")
    results_v2 = process_folder_v2(fuente_v2, debug=debug, cache=cache)
    df_v2 = _results_to_df(results_v2, version_name="V2")
    stats["nuevos_v2"] = len(results_v2)

    # Si no hay PDFs nuevos en ninguna versión, salir limpiamente
    if df_v1.empty and df_v2.empty:
        LOGGER.info("No hay PDFs nuevos que procesar (todos en caché). Pipeline finalizado.")
        return stats

    # ── 2) Enriquecimiento Proveedor (fuzzy) ───────────────
    LOGGER.info("Paso 2: Enriquecimiento Proveedor (fuzzy)...")
    df_v1 = _enrich_proveedor(df_v1, config.BD_PROVEEDORES, threshold=config.FUZZY_THRESHOLD)
    df_v2 = _enrich_proveedor(df_v2, config.BD_PROVEEDORES, threshold=config.FUZZY_THRESHOLD)

    # ── 3) Cruce Bd Swift → País / Ciudad ──────────────────
    LOGGER.info("Paso 3: Cruce Bd Swift (País/Ciudad)...")
    bd_swift_norm = _build_bd_swift_norm(config.BD_SWIFT)
    df_v1 = _apply_swift_country_city(df_v1, bd_swift_norm)
    df_v2 = _apply_swift_country_city(df_v2, bd_swift_norm)

    # ── 4) Post-proceso ────────────────────────────────────
    LOGGER.info("Paso 4: Post-proceso (Nombre personalizado, mayúsculas, Amount)...")
    for apply_fn in [_add_nombre_personalizado, _upper_pais_ciudad, _clean_amount_column]:
        df_v1 = apply_fn(df_v1)
        df_v2 = apply_fn(df_v2)

    # ── 5) Recálculo de estado ────────────────────────────
    df_v1 = _recalc_estado(df_v1)
    df_v2 = _recalc_estado(df_v2)

    # ── 6) IDs determinísticos + columnas finales ──────────
    LOGGER.info("Paso 5: Generando IDs y columnas finales...")
    df_v1 = _add_ids_and_tail_cols(df_v1)
    df_v2 = _add_ids_and_tail_cols(df_v2)

    # ── 7) Separar completos / incompletos ─────────────────
    df_v1_comp  = df_v1.loc[df_v1["Estado"] == "Completo"].copy()
    df_v2_comp  = df_v2.loc[df_v2["Estado"] == "Completo"].copy()
    df_v1_incomp = df_v1.loc[df_v1["Estado"] == "Incompleto"].copy()
    df_v2_incomp = df_v2.loc[df_v2["Estado"] == "Incompleto"].copy()

    total_comp   = len(df_v1_comp)  + len(df_v2_comp)
    total_incomp = len(df_v1_incomp) + len(df_v2_incomp)
    total_error  = sum(1 for r in results_v1 + results_v2 if r.get("estado") == "Error")

    LOGGER.info(f"V1 → Completos: {len(df_v1_comp)} | Incompletos: {len(df_v1_incomp)}")
    LOGGER.info(f"V2 → Completos: {len(df_v2_comp)} | Incompletos: {len(df_v2_incomp)}")

    # ── 8) Exportar ────────────────────────────────────────
    LOGGER.info("Paso 6: Exportando Swift_completos y Swift_manuales...")
    _write_swift_excel(df_v1_comp,  df_v2_comp,  config.SWIFT_COMPLETOS, label="completos")
    _write_swift_excel(df_v1_incomp, df_v2_incomp, config.SWIFT_MANUALES,  label="manuales")

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