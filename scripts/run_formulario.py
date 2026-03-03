# -*- coding: utf-8 -*-
"""
Cruce Formularios (Cuenta compensación 2.xlsb -> Swift_completos.xlsx)
+ Cruce Llave (origenDestino.xlsx -> Swift_completos.xlsx)
+ Cruce final: Swift_completos.Formulario -> origenDestino("Origen y destino").Consecutivo

PASO 1) Lee XLSB (hoja COM)
        - Encabezados desde fila 4
        - Toma A..F (hasta DEBITO)

PASO 2) Filtra:
        - FECHA >= 01/04/2025
        - INDICA == Imp

PASO 3) Cruza con Swift_completos:
        - Llave 1: FECHA_COM (normalizada a YYYY-MM-DD) vs Date (Swift)
        - Llave 2: DETALLE_COM (limpia desde #) vs Nombre archivo (Swift)
                 (Nombre archivo: elimina códigos iniciales + elimina .pdf)
                 Matching robusto por tokens:
                   * exige coincidencia de primeras 2 palabras (si existen)
                   * exige ratio de coincidencia >= 60%
        - Trae FORMULARIO -> Formulario (Swift)
        - Si varios matches, suma DEBITO y si coincide con Amount, concatena formularios con "-"

PASO 4) Cruce Llave (origenDestino.xlsx):
        - Llave: "Nombre personalizado" (origenDestino / hoja Datos Origen Destino)
                 vs "Nombre personalizado" (Swift)
        - Trae: "Llave carga masiva" -> "Llave" (Swift)

PASO 5) Cruce final (origenDestino.xlsx / hoja "Origen y destino"):
        - Desde Swift: columna "Formulario" (ej: "12030-None-12028")
          * extrae consecutivos numéricos
        - Relaciona con origenDestino: columna "Consecutivo"
        - Escribe Swift["Llave"] en origenDestino["Llave Origen Destino"]
        - Guarda ambos archivos
"""

from pathlib import Path
import logging
import re
import pandas as pd
from datetime import datetime

from pyxlsb import open_workbook


# =========================================================
# LOG
# =========================================================
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
LOGGER = logging.getLogger("cruce_formularios_llave")


# =========================================================
# CONFIG
# =========================================================
XLSB_PATH = Path(r"C:\Proyectos Comodin\Origen_Destino DIAN\Dbs\Cuenta compensacion 2.xlsb")
SHEET_NAME = "COM"

SWIFT_COMPLETOS = Path(r"C:\Proyectos Comodin\Origen_Destino DIAN\resultados\Swift_completos.xlsx")

ORIGEN_DESTINO = Path(r"C:\Proyectos Comodin\Origen_Destino DIAN\Dbs\origenDestino.xlsx")
ORIGEN_DESTINO_SHEET = "Datos Origen Destino"
OD_COL_NOMBRE = "Nombre personalizado"
OD_COL_LLAVE = "Llave carga masiva"

ORIGEN_DESTINO_SHEET_2 = "Origen y destino"
OD2_COL_CONSECUTIVO = "Consecutivo"
OD2_COL_LLAVE_OD = "Llave Origen Destino"

HEADER_ROW_1BASED = 4      # fila 4 headers
MAX_COLS = 6               # A..F
FECHA_MIN = pd.Timestamp("2025-04-01")  # >= 01/04/2025

# tolerancia para comparar sum(DEBITO) vs Amount
AMOUNT_TOL = 0.01

# reglas de match por tokens
TOKEN_MIN_RATIO = 0.60   # mínimo 60% de tokens del nombre deben aparecer en detalle
TOKEN_MIN_OVERLAP = 2    # mínimo 2 tokens en común (cuando el nombre tenga >=2 tokens)


# =========================================================
# UTILIDADES NORMALIZACIÓN
# =========================================================
def _parse_fecha_excel_series(s: pd.Series) -> pd.Series:
    if s is None:
        return pd.to_datetime(pd.Series([], dtype="datetime64[ns]"))

    if pd.api.types.is_datetime64_any_dtype(s):
        return pd.to_datetime(s, errors="coerce")

    if pd.api.types.is_numeric_dtype(s):
        return pd.to_datetime(s, unit="D", origin="1899-12-30", errors="coerce")

    s2 = s.astype(str).str.strip()
    s2 = s2.replace({"": pd.NA, "nan": pd.NA, "NaT": pd.NA})
    return pd.to_datetime(s2, dayfirst=True, errors="coerce")


def _normalize_text_key(x: str) -> str:
    if x is None:
        return ""
    s = str(x).replace("\u00A0", " ").strip()
    s = re.sub(r"\s+", " ", s)
    return s.casefold()


def _tokenize(s: str) -> list:
    if s is None:
        return []
    s = _normalize_text_key(s)
    return re.findall(r"[a-z0-9]+", s)


def _clean_detalle(detalle: str) -> str:
    if detalle is None:
        return ""
    s = str(detalle).replace("\u00A0", " ")
    s = re.split(r"#", s, maxsplit=1)[0].strip()
    return _normalize_text_key(s)


def _clean_nombre_archivo(nombre: str) -> str:
    if nombre is None:
        return ""
    s = str(nombre).replace("\u00A0", " ").strip()
    s = re.sub(r"\.pdf\s*$", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"^(?:\d+\s+)+", "", s).strip()  # elimina prefijos numéricos repetidos
    s = re.sub(r"\s+", " ", s).strip()
    return _normalize_text_key(s)


def _parse_money_to_float(v) -> float:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return float("nan")

    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v)

    s = str(v).replace("\u00A0", " ").strip()
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[^0-9\.,\-]", "", s)

    if s in ("", "-", ".", ","):
        return float("nan")

    if "," in s and "." in s:
        s = s.replace(".", "")
        s = s.replace(",", ".")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")

    if re.search(r"\.$", s):
        s = s + "0"

    try:
        return float(s)
    except Exception:
        return float("nan")


# =========================================================
# MATCH ROBUSTO (tokens)
# =========================================================
def _tokens_match(swift_clean: str, detalle_clean: str) -> bool:
    st = _tokenize(swift_clean)
    dt = set(_tokenize(detalle_clean))

    if not st:
        return False

    overlap = sum(1 for t in st if t in dt)
    ratio = overlap / max(len(st), 1)

    if len(st) >= 2:
        if st[0] not in dt or st[1] not in dt:
            return False
        if overlap < TOKEN_MIN_OVERLAP:
            return False
        if ratio < TOKEN_MIN_RATIO:
            return False
        return True

    return st[0] in dt


# =========================================================
# PASO 1) LECTURA XLSB COM (A..F, header fila 4)
# =========================================================
def read_com_sheet_A_to_F(xlsb_path: Path, sheet_name: str = "COM") -> pd.DataFrame:
    if not xlsb_path.exists():
        raise FileNotFoundError(f"No existe el archivo: {xlsb_path}")

    header_idx = HEADER_ROW_1BASED - 1  # 0-based
    data_rows = []
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
                    raise RuntimeError("No se detectó header en fila 4.")

                if all(v is None or str(v).strip() == "" for v in values):
                    continue

                data_rows.append(values)

    df = pd.DataFrame(data_rows, columns=headers)
    df.columns = [str(c).replace("\u00A0", " ").strip() for c in df.columns]

    LOGGER.info(f"Lectura XLSB OK -> filas={len(df)} | cols={list(df.columns)}")
    LOGGER.info("Muestra lectura (5 filas):\n" + df.head(5).to_string(index=False))
    return df


# =========================================================
# PASO 2) FILTROS FECHA>=01/04/2025 + INDICA=Imp
# =========================================================
def filter_com_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        LOGGER.warning("DF COM viene vacío antes de filtrar.")
        return df

    cols_map = {str(c).strip().upper(): c for c in df.columns}

    if "FECHA" not in cols_map:
        raise KeyError(f"No encuentro columna FECHA. Columnas detectadas: {list(df.columns)}")
    if "INDICA" not in cols_map:
        raise KeyError(f"No encuentro columna INDICA. Columnas detectadas: {list(df.columns)}")

    col_fecha = cols_map["FECHA"]
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
    after_notna = len(out)

    out = out.loc[out["_FECHA_DT"] >= FECHA_MIN].copy()
    after_fecha = len(out)

    out = out.loc[out["_INDICA_NORM"] == "imp"].copy()
    after_indica = len(out)

    out = out.drop(columns=["_FECHA_DT", "_INDICA_NORM"], errors="ignore").reset_index(drop=True)

    LOGGER.info(
        f"Filtro aplicado -> inicio={before} | fecha_valida={after_notna} | "
        f">={FECHA_MIN.date()}={after_fecha} | indica=Imp={after_indica}"
    )
    LOGGER.info("Muestra post-filtro (5 filas):\n" + out.head(5).to_string(index=False))
    return out


# =========================================================
# PASO 3) CRUCE CON SWIFT_COMPLETOS -> ACTUALIZAR FORMULARIO
# =========================================================
def build_com_keys(df_com: pd.DataFrame) -> pd.DataFrame:
    out = df_com.copy()
    cols_map = {str(c).strip().upper(): c for c in out.columns}

    required = ["FECHA", "DETALLE", "FORMULARIO", "DEBITO"]
    missing = [c for c in required if c not in cols_map]
    if missing:
        raise KeyError(f"En COM faltan columnas requeridas: {missing}. Detectadas: {list(out.columns)}")

    c_fecha = cols_map["FECHA"]
    c_det = cols_map["DETALLE"]
    c_form = cols_map["FORMULARIO"]
    c_deb = cols_map["DEBITO"]

    out["_fecha_dt"] = _parse_fecha_excel_series(out[c_fecha])
    out["fecha_key"] = out["_fecha_dt"].dt.strftime("%Y-%m-%d")

    out["detalle_clean"] = out[c_det].apply(_clean_detalle)

    out["debito_num"] = out[c_deb].apply(_parse_money_to_float)
    out["formulario_str"] = out[c_form].astype(str).str.strip()

    out["row_order"] = range(len(out))
    out = out.drop(columns=["_fecha_dt"], errors="ignore")
    return out


def build_swift_keys(df_swift: pd.DataFrame) -> pd.DataFrame:
    out = df_swift.copy()
    needed = ["Date", "Nombre archivo", "Amount", "id"]
    miss = [c for c in needed if c not in out.columns]
    if miss:
        raise KeyError(f"Swift_completos falta(n) columna(s) {miss}. Detectadas: {list(out.columns)}")

    out["_date_dt"] = pd.to_datetime(out["Date"], errors="coerce")
    out["fecha_key"] = out["_date_dt"].dt.strftime("%Y-%m-%d")

    out["nombre_clean"] = out["Nombre archivo"].apply(_clean_nombre_archivo)
    out["amount_num"] = out["Amount"].apply(_parse_money_to_float)

    out = out.drop(columns=["_date_dt"], errors="ignore")
    return out


def update_swift_formulario_for_sheet(df_swift_sheet: pd.DataFrame, df_com_keys: pd.DataFrame) -> pd.DataFrame:
    if df_swift_sheet.empty:
        return df_swift_sheet

    out = df_swift_sheet.copy()
    out.columns = [str(c).strip() for c in out.columns]

    if "Formulario" not in out.columns:
        out["Formulario"] = ""

    swift_k = build_swift_keys(out)
    com_k = build_com_keys(df_com_keys) if "detalle_clean" not in df_com_keys.columns else df_com_keys.copy()

    com_by_date = {k: v.copy() for k, v in com_k.groupby("fecha_key")}

    updated = 0
    multi_matched = 0
    multi_ok = 0
    multi_fail = 0

    for _, srow in swift_k.iterrows():
        sid = srow["id"]
        s_fecha = srow["fecha_key"]
        s_name = srow["nombre_clean"]
        s_amount = srow["amount_num"]

        if not isinstance(s_fecha, str) or not s_fecha:
            continue

        com_day = com_by_date.get(s_fecha)
        if com_day is None or com_day.empty:
            continue

        cand = com_day.loc[com_day["detalle_clean"].apply(lambda d: _tokens_match(s_name, d))].copy()
        if cand.empty:
            continue

        if len(cand) == 1:
            form_val = str(cand.iloc[0]["formulario_str"]).strip()
            if form_val and form_val.lower() != "none":
                out.loc[out["id"] == sid, "Formulario"] = form_val
                updated += 1
            continue

        multi_matched += 1
        deb_sum = cand["debito_num"].sum(skipna=True)

        if pd.notna(deb_sum) and pd.notna(s_amount) and abs(deb_sum - s_amount) <= AMOUNT_TOL:
            cand = cand.sort_values("row_order")
            forms = [
                str(x).strip()
                for x in cand["formulario_str"].tolist()
                if str(x).strip() != "" and str(x).strip().lower() != "none"
            ]
            if forms:
                out.loc[out["id"] == sid, "Formulario"] = "-".join(forms)
                updated += 1
                multi_ok += 1
        else:
            multi_fail += 1

    LOGGER.info(
        f"Swift actualizado Formulario: updated={updated} | multi_matched={multi_matched} | "
        f"multi_ok={multi_ok} | multi_fail={multi_fail}"
    )
    return out


# =========================================================
# PASO 4) CRUCE ORIGEN DESTINO -> LLAVE (Swift)
# =========================================================
def read_origen_destino_mapping(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"No existe origenDestino.xlsx: {path}")

    df = pd.read_excel(path, sheet_name=ORIGEN_DESTINO_SHEET)
    df.columns = [str(c).replace("\u00A0", " ").strip() for c in df.columns]

    if OD_COL_NOMBRE not in df.columns:
        raise KeyError(f"No se encontró columna '{OD_COL_NOMBRE}' en {path.name}. Columnas: {list(df.columns)}")
    if OD_COL_LLAVE not in df.columns:
        raise KeyError(f"No se encontró columna '{OD_COL_LLAVE}' en {path.name}. Columnas: {list(df.columns)}")

    out = df[[OD_COL_NOMBRE, OD_COL_LLAVE]].copy()
    out[OD_COL_NOMBRE] = out[OD_COL_NOMBRE].apply(_normalize_text_key)
    out[OD_COL_LLAVE] = out[OD_COL_LLAVE].astype(str).str.strip()

    out = out.loc[out[OD_COL_NOMBRE] != ""].copy()
    out = out.loc[out[OD_COL_LLAVE] != ""].copy()

    out = out.drop_duplicates(subset=[OD_COL_NOMBRE], keep="first").reset_index(drop=True)

    LOGGER.info(f"OrigenDestino mapping cargado: {len(out)} llaves únicas.")
    return out


def apply_llave_to_swift(df_swift_sheet: pd.DataFrame, od_map: pd.DataFrame) -> pd.DataFrame:
    if df_swift_sheet.empty:
        return df_swift_sheet

    out = df_swift_sheet.copy()
    out.columns = [str(c).strip() for c in out.columns]

    if "Nombre personalizado" not in out.columns:
        raise KeyError("Swift_completos no tiene columna 'Nombre personalizado'.")
    if "Llave" not in out.columns:
        out["Llave"] = ""

    out["_np_norm"] = out["Nombre personalizado"].apply(_normalize_text_key)
    out["_llave_cur"] = out["Llave"].fillna("").astype(str).str.strip()

    map_dict = dict(zip(od_map[OD_COL_NOMBRE], od_map[OD_COL_LLAVE]))

    before_empty = (out["_llave_cur"] == "").sum()
    out["_llave_new"] = out["_np_norm"].map(map_dict).fillna("")

    fill_mask = (out["_llave_cur"] == "") & (out["_llave_new"] != "")
    filled = int(fill_mask.sum())

    out.loc[fill_mask, "Llave"] = out.loc[fill_mask, "_llave_new"]

    after_empty = (out["Llave"].fillna("").astype(str).str.strip() == "").sum()

    LOGGER.info(f"Cruce Llave aplicado (Swift): filled={filled} | empty_before={before_empty} | empty_after={after_empty}")

    out = out.drop(columns=["_np_norm", "_llave_cur", "_llave_new"], errors="ignore")
    return out


# =========================================================
# PASO 5) FORMULARIO (Swift) -> Consecutivo (origenDestino "Origen y destino")
# =========================================================
def _extract_consecutivos_from_formulario(val) -> list:
    """
    De '12030-None-12028' -> ['12030','12028']
    - ignora None / vacíos
    - extrae solo dígitos
    """
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return []

    s = str(val).strip()
    if not s:
        return []

    parts = [p.strip() for p in s.split("-")]
    out = []
    for p in parts:
        if not p or p.lower() == "none":
            continue
        # si viene con basura, extrae solo número
        m = re.search(r"\d+", p)
        if m:
            out.append(m.group(0))
    return out


def _normalize_consecutivo_series(s: pd.Series) -> pd.Series:
    """
    Normaliza Consecutivo a string de dígitos:
    - si es numérico tipo 12029.0 -> '12029'
    - si es string -> extrae dígitos principales
    """
    def norm_one(x):
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return ""
        if isinstance(x, (int,)) and not isinstance(x, bool):
            return str(x)
        if isinstance(x, float):
            if pd.isna(x):
                return ""
            if float(x).is_integer():
                return str(int(x))
            return str(x).strip()
        sx = str(x).strip()
        m = re.search(r"\d+", sx)
        return m.group(0) if m else ""

    return s.apply(norm_one)


def update_origen_destino_llave_od(path: Path, df_swift_all: pd.DataFrame) -> None:
    """
    Actualiza origenDestino.xlsx hoja "Origen y destino":
    - Cruza: Consecutivo vs consecutivos extraídos de Swift.Formulario
    - Set: Llave Origen Destino = Swift.Llave
    - No pisa si ya tiene valor diferente (lo cuenta como conflicto)
    """
    if not path.exists():
        raise FileNotFoundError(f"No existe origenDestino.xlsx: {path}")

    if df_swift_all.empty:
        LOGGER.info("PASO 5 -> Swift vacío. No hay nada que cruzar hacia origenDestino.")
        return

    # Validaciones Swift
    for c in ("Formulario", "Llave"):
        if c not in df_swift_all.columns:
            raise KeyError(f"Swift_completos no tiene columna '{c}' requerida para PASO 5.")

    # Leer hoja Origen y destino
    df_od2 = pd.read_excel(path, sheet_name=ORIGEN_DESTINO_SHEET_2)
    df_od2.columns = [str(c).replace("\u00A0", " ").strip() for c in df_od2.columns]

    if OD2_COL_CONSECUTIVO not in df_od2.columns:
        raise KeyError(f"No existe columna '{OD2_COL_CONSECUTIVO}' en hoja '{ORIGEN_DESTINO_SHEET_2}'.")
    if OD2_COL_LLAVE_OD not in df_od2.columns:
        df_od2[OD2_COL_LLAVE_OD] = ""

    # Normalizar consecutivo en OD
    df_od2["_consec_norm"] = _normalize_consecutivo_series(df_od2[OD2_COL_CONSECUTIVO])

    # Construir mapping: consecutivo -> llave (desde Swift)
    # Si un consecutivo aparece en varias filas swift con llaves distintas, priorizamos la primera y contamos conflicto.
    consec_to_llave = {}
    conflicts_swift = 0

    for _, r in df_swift_all.iterrows():
        llave = str(r.get("Llave", "")).strip()
        if not llave:
            continue

        consecs = _extract_consecutivos_from_formulario(r.get("Formulario"))
        for c in consecs:
            if c not in consec_to_llave:
                consec_to_llave[c] = llave
            else:
                if consec_to_llave[c] != llave:
                    conflicts_swift += 1

    if not consec_to_llave:
        LOGGER.info("PASO 5 -> No se encontraron consecutivos válidos en Swift.Formulario (ignorando None).")
        return

    # Aplicar a OD
    updated = 0
    conflicts_od = 0

    def apply_row(row):
        nonlocal updated, conflicts_od
        c = row["_consec_norm"]
        if not c:
            return row

        new_llave = consec_to_llave.get(c, "")
        if not new_llave:
            return row

        cur = str(row.get(OD2_COL_LLAVE_OD, "")).strip()
        if not cur:
            row[OD2_COL_LLAVE_OD] = new_llave
            updated += 1
        else:
            if cur != new_llave:
                conflicts_od += 1
        return row

    df_od2 = df_od2.apply(apply_row, axis=1)
    df_od2 = df_od2.drop(columns=["_consec_norm"], errors="ignore")

    LOGGER.info(
        f"PASO 5 -> Origen y destino actualizado: updated={updated} | "
        f"conflicts_swift={conflicts_swift} | conflicts_od={conflicts_od}"
    )

    # Guardar SOLO reemplazando esa hoja, preservando las demás
    with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        df_od2.to_excel(writer, sheet_name=ORIGEN_DESTINO_SHEET_2, index=False)

    LOGGER.info(f"origenDestino.xlsx guardado con '{OD2_COL_LLAVE_OD}' actualizado en hoja '{ORIGEN_DESTINO_SHEET_2}'.")


# =========================================================
# EJECUCIÓN: PASO 3 + PASO 4 + PASO 5 y guardado final
# =========================================================
def run_update_swift(df_com_filtrado: pd.DataFrame) -> None:
    if df_com_filtrado.empty:
        LOGGER.warning("COM filtrada viene vacía. No se puede cruzar.")
        return

    if not SWIFT_COMPLETOS.exists():
        raise FileNotFoundError(f"No existe Swift_completos: {SWIFT_COMPLETOS}")

    # Preparar COM keys (para formulario)
    com_keys = build_com_keys(df_com_filtrado)

    # Leer Swift
    df_v1 = pd.read_excel(SWIFT_COMPLETOS, sheet_name="V1")
    df_v2 = pd.read_excel(SWIFT_COMPLETOS, sheet_name="V2")

    # PASO 3: Formulario
    LOGGER.info("Aplicando cruce (COM -> Swift) en V1...")
    df_v1_u = update_swift_formulario_for_sheet(df_v1, com_keys)

    LOGGER.info("Aplicando cruce (COM -> Swift) en V2...")
    df_v2_u = update_swift_formulario_for_sheet(df_v2, com_keys)

    # PASO 4: Llave desde origenDestino.xlsx (hoja Datos Origen Destino)
    LOGGER.info("Cargando mapping origenDestino.xlsx (Nombre personalizado -> Llave carga masiva)...")
    od_map = read_origen_destino_mapping(ORIGEN_DESTINO)

    LOGGER.info("Aplicando cruce Llave (origenDestino -> Swift) en V1...")
    df_v1_u = apply_llave_to_swift(df_v1_u, od_map)

    LOGGER.info("Aplicando cruce Llave (origenDestino -> Swift) en V2...")
    df_v2_u = apply_llave_to_swift(df_v2_u, od_map)

    # Guardar Swift actualizado UNA sola vez
    with pd.ExcelWriter(SWIFT_COMPLETOS, engine="openpyxl", mode="w") as writer:
        df_v1_u.to_excel(writer, sheet_name="V1", index=False)
        df_v2_u.to_excel(writer, sheet_name="V2", index=False)

    LOGGER.info(f"Swift_completos guardado con Formulario y Llave actualizados: {SWIFT_COMPLETOS}")

    # PASO 5: Actualizar origenDestino hoja "Origen y destino" usando Swift completo (V1+V2)
    df_swift_all = pd.concat([df_v1_u, df_v2_u], ignore_index=True)
    LOGGER.info("Aplicando PASO 5 (Swift.Formulario -> origenDestino.Consecutivo -> Llave Origen Destino)...")
    update_origen_destino_llave_od(ORIGEN_DESTINO, df_swift_all)


# =========================================================
# MAIN (PASO 1 + PASO 2 + PASO 3/4/5)
# =========================================================
if __name__ == "__main__":
    LOGGER.info("=== INICIO CRUCE FORMULARIOS + LLAVE + LLAVE OD (XLSB + origenDestino -> Swift_completos) ===")

    # Paso 1
    df_com = read_com_sheet_A_to_F(XLSB_PATH, SHEET_NAME)

    # Paso 2
    df_com_filtrado = filter_com_df(df_com)
    LOGGER.info(f"Cuenta XLSB filtrada: {len(df_com_filtrado)} filas (FECHA>=01/04/2025 e INDICA=Imp).")

    # Paso 3 + Paso 4 + Paso 5
    run_update_swift(df_com_filtrado)

    LOGGER.info("=== FIN CRUCE FORMULARIOS + LLAVE + LLAVE OD ===")