# -*- coding: utf-8 -*-
"""
run_pipeline.py  (VERSIÓN LIMPIA)

Flujo:
1) Ejecuta extracción V1 y V2
2) Enriquecimientos: Proveedor (fuzzy), Pais/Ciudad (Bd Swift), Nombre personalizado, mayúsculas, limpieza Amount
3) Separa:
   - COMPLETOS  -> Swift_completos.xlsx (V1 y V2)  [REEMPLAZA cada ejecución]
   - INCOMPLETOS-> Swift_manuales.xlsx (V1 y V2)  [REEMPLAZA cada ejecución]
4) Genera ID único determinístico por registro (no se repite y se conserva entre ejecuciones)
5) Agrega columnas vacías: Formulario, Llave

NOTA:
- Se ELIMINA del script madre la lógica de:
  - Traslado a Datos_Origen_Destino_V1 / V2 (plantillas Bancolombia)
  - Acumulado_swift (append)
"""

from __future__ import annotations

import re
import logging
import unicodedata
import uuid
from pathlib import Path
from typing import Tuple, Optional, List, Dict

import pandas as pd

# Extractores
from reader_pdf_V1 import process_folder as process_folder_v1
from reader_pdf_V2 import process_folder_v2


# =========================================================
# CONFIGURACIÓN
# =========================================================
INPUT_FOLDER_V1 = Path(
    r"C:\Proyectos Comodin\Origen_Destino DIAN\pdfs V1"
)
INPUT_FOLDER_V2 = Path(
    r"C:\Proyectos Comodin\Origen_Destino DIAN\pdfs V2"
)

OUTPUT_DIR = Path(
    r"C:\Proyectos Comodin\Origen_Destino DIAN\resultados"
)

SWIFT_MANUALES = OUTPUT_DIR / "Swift_manuales.xlsx"   # SOLO INCOMPLETOS (reemplaza)
SWIFT_COMPLETOS = OUTPUT_DIR / "Swift_completos.xlsx" # SOLO COMPLETOS (reemplaza)

# BD Proveedores
BD_PROVEEDORES = Path(
    r"C:\Proyectos Comodin\Origen_Destino DIAN\Dbs\Bd Proveedores.xlsx"
)
BD_COL_NAME = "DB Nombre o razon social del beneficiario"

# BD Swift
BD_SWIFT = Path(
    r"C:\Proyectos Comodin\Origen_Destino DIAN\Dbs\Bd Swift.xlsx"
)
BD_SWIFT_CODE_COL = "CODIGO DE LOS SWIFT"
BD_SWIFT_PAIS_COL = "PAIS"
BD_SWIFT_CIUDAD_COL = "CIUDAD"

DEBUG = False
FUZZY_THRESHOLD = 85


# =========================================================
# LOGGING
# =========================================================
LOGGER = logging.getLogger("pipeline_origen_destino_dian")
LOGGER.setLevel(logging.INFO)

if not LOGGER.handlers:
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("[%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    LOGGER.addHandler(handler)


# =========================================================
# UTILIDADES: NORMALIZACIÓN (Proveedor)
# =========================================================
LEGAL_TOKENS = {
    "SA", "SAS", "S.A", "S.A.S", "S A", "S A S",
    "LTDA", "LTD", "LTD.", "LIMITED",
    "CO", "CO.", "COMPANY",
    "INC", "INC.", "CORP", "CORPORATION",
    "LLC",
    "SPA", "S.P.A",
    "GMBH", "BV", "B.V",
    "SRL", "S.R.L",
    "AG", "NV"
}

COUNTRY_TOKENS = {"CHINA", "TURKEY", "COLOMBIA", "PANAMA", "US", "USA", "ESPANA", "SPAIN"}


def strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(c for c in s if not unicodedata.combining(c))


def normalize_name(s: str) -> str:
    """Normaliza nombres para fuzzy matching."""
    if s is None:
        return ""
    s = str(s).strip()
    if not s:
        return ""

    s = strip_accents(s).upper()
    s = re.sub(r"[^A-Z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    tokens = []
    for t in s.split():
        if t in LEGAL_TOKENS:
            continue
        if t in COUNTRY_TOKENS:
            continue
        tokens.append(t)

    return " ".join(tokens).strip() or s


def get_best_match(query: str, choices_norm: List[str], choices_raw: List[str]) -> Tuple[Optional[str], int]:
    """Devuelve (mejor_match_raw, score_0_100)."""
    qn = normalize_name(query)
    if not qn:
        return None, 0

    try:
        from rapidfuzz import process, fuzz  # type: ignore
        res = process.extractOne(qn, choices_norm, scorer=fuzz.token_set_ratio)
        if not res:
            return None, 0
        _, score, idx = res
        return choices_raw[idx], int(score)
    except Exception:
        import difflib

        best_score = 0
        best_idx = None
        for i, cn in enumerate(choices_norm):
            score = int(difflib.SequenceMatcher(None, qn, cn).ratio() * 100)
            if score > best_score:
                best_score = score
                best_idx = i

        if best_idx is None:
            return None, 0
        return choices_raw[best_idx], best_score


# =========================================================
# RESULTADOS -> DATAFRAME
# =========================================================
def results_to_df(results: List[Dict], version_name: str) -> pd.DataFrame:
    rows = []
    for r in results:
        receiver = r.get("receiver")
        date_ = r.get("date")
        amount_ = r.get("amount")
        proveedor = r.get("beneficiary")

        estado = "Completo" if (receiver and date_ and amount_ and proveedor) else "Incompleto"

        rows.append({
            "Nombre archivo": r.get("file_name"),
            "Receiver": receiver,
            "Date": date_,
            "Amount": amount_,
            "Proveedor": proveedor,
            "Estado": estado,
            "Version": version_name,
        })

    return pd.DataFrame(rows, columns=[
        "Nombre archivo", "Receiver", "Date", "Amount", "Proveedor", "Estado", "Version"
    ])


# =========================================================
# ENRIQUECIMIENTC: PROVEEDOR (Bd Proveedores)
# =========================================================
def enrich_proveedor_with_bd(df: pd.DataFrame, bd_path: Path, threshold: int = 85) -> pd.DataFrame:
    if not bd_path.exists():
        raise FileNotFoundError(f"No existe Bd Proveedores: {bd_path}")

    bd = pd.read_excel(bd_path)
    bd.columns = [str(c).strip() for c in bd.columns]

    if BD_COL_NAME not in bd.columns:
        raise KeyError(f"No se encontró la columna '{BD_COL_NAME}' en {bd_path.name}")

    choices_raw = (
        bd[BD_COL_NAME]
        .dropna()
        .astype(str)
        .map(lambda x: x.strip())
        .loc[lambda s: s != ""]
        .drop_duplicates()
        .tolist()
    )
    choices_norm = [normalize_name(x) for x in choices_raw]

    if not choices_raw:
        LOGGER.warning("Bd Proveedores sin candidatos válidos. Se omite enriquecimiento.")
        return df

    updated = df.copy()
    replaced = 0
    reviewed = 0

    for idx, prov in updated["Proveedor"].items():
        if prov is None or str(prov).strip() == "":
            continue

        reviewed += 1
        best, score = get_best_match(str(prov), choices_norm, choices_raw)

        if best and score >= threshold and best != prov:
            updated.at[idx, "Proveedor"] = best
            replaced += 1

        if DEBUG and reviewed <= 10:
            LOGGER.info(f"[DEBUG] '{prov}' -> '{best}' | score={score}")

    LOGGER.info(f"Enriquecimiento Proveedor: revisados={reviewed} | reemplazados={replaced} | threshold={threshold}")
    return updated


# =========================================================
# ENRIQUECIMIENTO: SWIFT -> PAÍS / CIUDAD
# =========================================================
def normalize_swift_11(code: str) -> str:
    if code is None:
        return ""
    c = str(code).upper().replace("\u00A0", " ").strip()  # NBSP guard
    c = re.sub(r"[^A-Z0-9]", "", c)
    if not c:
        return ""
    if len(c) < 11:
        c = c + ("X" * (11 - len(c)))
    elif len(c) > 11:
        c = c[:11]
    return c


def read_bd_swift_auto(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"No existe Bd Swift: {path}")

    xls = pd.ExcelFile(path)
    needed = {BD_SWIFT_CODE_COL, BD_SWIFT_PAIS_COL, BD_SWIFT_CIUDAD_COL}

    for sh in xls.sheet_names:
        df = pd.read_excel(path, sheet_name=sh)
        df.columns = [str(c).strip() for c in df.columns]
        if needed.issubset(set(df.columns)):
            LOGGER.info(f"Bd Swift: usando hoja '{sh}'")
            return df

    raise KeyError(
        f"No se encontró una hoja con columnas: {BD_SWIFT_CODE_COL}, {BD_SWIFT_PAIS_COL}, {BD_SWIFT_CIUDAD_COL}"
    )


def build_bd_swift_normalized(bd_swift_path: Path) -> pd.DataFrame:
    bd = read_bd_swift_auto(bd_swift_path)

    out = bd[[BD_SWIFT_CODE_COL, BD_SWIFT_PAIS_COL, BD_SWIFT_CIUDAD_COL]].copy()
    out.rename(columns={BD_SWIFT_CODE_COL: "swift_original"}, inplace=True)

    out["swift_norm"] = out["swift_original"].apply(normalize_swift_11)
    out = out.loc[out["swift_norm"] != ""].copy()
    out = out.drop_duplicates(subset=["swift_norm"], keep="first")

    return out


def apply_swift_country_city(df_result: pd.DataFrame, bd_swift_norm: pd.DataFrame) -> pd.DataFrame:
    out = df_result.copy()
    out["receiver_norm"] = out["Receiver"].apply(normalize_swift_11)

    bd_map = bd_swift_norm[["swift_norm", BD_SWIFT_PAIS_COL, BD_SWIFT_CIUDAD_COL]].copy()
    bd_map = bd_map.rename(columns={
        BD_SWIFT_PAIS_COL: "__pais_bd",
        BD_SWIFT_CIUDAD_COL: "__ciudad_bd",
    })

    out = out.merge(
        bd_map,
        how="left",
        left_on="receiver_norm",
        right_on="swift_norm",
    )

    out["Pais"] = out["__pais_bd"]
    out["Ciudad"] = out["__ciudad_bd"]

    out = out.drop(columns=["receiver_norm", "swift_norm", "__pais_bd", "__ciudad_bd"], errors="ignore")

    matches = out["Pais"].notna().sum()
    LOGGER.info(f"Cruce Bd Swift aplicado: matches={matches}/{len(out)}")
    return out


# =========================================================
# POST-PROCESO: NOMBRE PERSONALIZADO / MAYÚSCULAS / AMOUNT / ESTADO
# =========================================================
def add_nombre_personalizado(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    prov = out["Proveedor"].fillna("").astype(str).str.strip()
    recv = out["Receiver"].fillna("").astype(str).str.strip()
    out["Nombre personalizado"] = (prov + " " + recv).str.strip()
    out.loc[out["Nombre personalizado"] == "", "Nombre personalizado"] = pd.NA
    return out


def upper_pais_ciudad(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "Pais" in out.columns:
        out["Pais"] = out["Pais"].astype("string").str.strip().str.upper()
    if "Ciudad" in out.columns:
        out["Ciudad"] = out["Ciudad"].astype("string").str.strip().str.upper()
    return out


def clean_amount_value(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""

    s = str(v).strip()
    if not s:
        return ""

    # quitar espacios internos (incluye NBSP y otros espacios invisibles)
    s = s.replace("\u00A0", "").replace("\u2007", "").replace("\u202F", "")
    s = re.sub(r"\s+", "", s)

    # eliminar todo lo que no sea dígito, punto o coma
    s = re.sub(r"[^0-9\.,]", "", s)

    last_dot = s.rfind(".")
    last_com = s.rfind(",")
    sep_pos = max(last_dot, last_com)

    if sep_pos == -1:
        return s

    dec = s[sep_pos + 1:]

    if dec == "":
        s = s + "00"
    elif len(dec) == 1:
        s = s + "0"

    return s


def clean_amount_column(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "Amount" in out.columns:
        out["Amount"] = out["Amount"].apply(clean_amount_value)
        out.loc[out["Amount"].astype(str).str.strip() == "", "Amount"] = pd.NA
    return out


def recalc_estado(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["Estado"] = out.apply(
        lambda r: "Completo"
        if (
            pd.notna(r.get("Receiver"))
            and pd.notna(r.get("Date"))
            and pd.notna(r.get("Amount"))
            and pd.notna(r.get("Proveedor"))
            and str(r.get("Proveedor")).strip() != ""
        )
        else "Incompleto",
        axis=1,
    )
    return out


# =========================================================
# ID ÚNICO (DETERMINÍSTICO Y NO REPETIBLE)
# =========================================================
def make_record_id(version: str, file_name: str) -> str:
    """
    ID determinístico basado en (version + file_name).
    - No cambia entre ejecuciones para el mismo PDF.
    - No colisiona entre V1 y V2 aunque tengan el mismo nombre (incluye version).
    """
    base = f"origen_destino_dian|{version}|{file_name}".strip()
    return str(uuid.uuid5(uuid.NAMESPACE_URL, base))


def add_ids_and_tail_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega:
    - id (primera columna)
    - Formulario y Llave al final (vacías)
    """
    out = df.copy()

    # id determinístico
    out["id"] = out.apply(
        lambda r: make_record_id(str(r.get("Version", "")), str(r.get("Nombre archivo", ""))),
        axis=1
    )

    # columnas finales vacías
    out["Formulario"] = pd.NA
    out["Llave"] = pd.NA

    # reorden: id al inicio
    cols = list(out.columns)
    cols.remove("id")
    cols = ["id"] + cols
    out = out[cols]

    return out


# =========================================================
# EXPORT EXCEL (V1 y V2)
# =========================================================
FINAL_COLUMNS_ORDER = [
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
]

def _ensure_final_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in FINAL_COLUMNS_ORDER:
        if c not in out.columns:
            out[c] = pd.NA
    out = out[FINAL_COLUMNS_ORDER].copy()
    return out


def write_swift_excel(df_v1: pd.DataFrame, df_v2: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    v1 = _ensure_final_columns(df_v1)
    v2 = _ensure_final_columns(df_v2)

    with pd.ExcelWriter(output_path, engine="openpyxl", mode="w") as writer:
        v1.to_excel(writer, sheet_name="V1", index=False)
        v2.to_excel(writer, sheet_name="V2", index=False)

    LOGGER.info(f"Excel generado: {output_path} (hojas: V1, V2)")


# =========================================================
# MAIN
# =========================================================
def main() -> None:
    LOGGER.info("=== INICIO PIPELINE ===")

    # 1) Extracción V1
    LOGGER.info("Ejecutando extracción V1...")
    results_v1 = process_folder_v1(INPUT_FOLDER_V1, debug=DEBUG)
    df_v1 = results_to_df(results_v1, version_name="V1")

    # 2) Extracción V2
    LOGGER.info("Ejecutando extracción V2...")
    results_v2 = process_folder_v2(INPUT_FOLDER_V2, debug=DEBUG)
    df_v2 = results_to_df(results_v2, version_name="V2")

    # 3) Enriquecer Proveedor (fuzzy)
    LOGGER.info("Enriqueciendo Proveedor con Bd Proveedores (fuzzy match)...")
    df_v1 = enrich_proveedor_with_bd(df_v1, BD_PROVEEDORES, threshold=FUZZY_THRESHOLD)
    df_v2 = enrich_proveedor_with_bd(df_v2, BD_PROVEEDORES, threshold=FUZZY_THRESHOLD)

    # 4) Bd Swift normalizada + cruce Pais/Ciudad
    LOGGER.info("Construyendo Bd Swift normalizada para cruce...")
    bd_swift_norm = build_bd_swift_normalized(BD_SWIFT)

    LOGGER.info("Aplicando cruce Swift -> Pais/Ciudad en V1...")
    df_v1 = apply_swift_country_city(df_v1, bd_swift_norm)

    LOGGER.info("Aplicando cruce Swift -> Pais/Ciudad en V2...")
    df_v2 = apply_swift_country_city(df_v2, bd_swift_norm)

    # 5) Nombre personalizado + Mayúsculas Pais/Ciudad + Limpieza Amount
    df_v1 = add_nombre_personalizado(df_v1)
    df_v2 = add_nombre_personalizado(df_v2)

    df_v1 = upper_pais_ciudad(df_v1)
    df_v2 = upper_pais_ciudad(df_v2)

    df_v1 = clean_amount_column(df_v1)
    df_v2 = clean_amount_column(df_v2)

    # 6) Recalcular estado (ya con Amount limpio)
    df_v1 = recalc_estado(df_v1)
    df_v2 = recalc_estado(df_v2)

    # 7) Agregar id + columnas finales (Formulario, Llave)
    df_v1 = add_ids_and_tail_cols(df_v1)
    df_v2 = add_ids_and_tail_cols(df_v2)

    # 8) Separar completos / incompletos
    df_v1_completo = df_v1.loc[df_v1["Estado"] == "Completo"].copy()
    df_v2_completo = df_v2.loc[df_v2["Estado"] == "Completo"].copy()

    df_v1_incomp = df_v1.loc[df_v1["Estado"] == "Incompleto"].copy()
    df_v2_incomp = df_v2.loc[df_v2["Estado"] == "Incompleto"].copy()

    LOGGER.info(f"V1 -> Completo: {len(df_v1_completo)} | Incompleto: {len(df_v1_incomp)}")
    LOGGER.info(f"V2 -> Completo: {len(df_v2_completo)} | Incompleto: {len(df_v2_incomp)}")

    # 9) Exportar:
    # - Swift_completos.xlsx  (solo completos)
    # - Swift_manuales.xlsx   (solo incompletos)
    LOGGER.info("Generando Swift_completos (SOLO COMPLETOS)...")
    write_swift_excel(df_v1_completo, df_v2_completo, SWIFT_COMPLETOS)

    LOGGER.info("Generando Swift_manuales (SOLO INCOMPLETOS)...")
    write_swift_excel(df_v1_incomp, df_v2_incomp, SWIFT_MANUALES)

    LOGGER.info("=== FIN PIPELINE ===")


if __name__ == "__main__":
    main()