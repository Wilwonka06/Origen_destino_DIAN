# -*- coding: utf-8 -*-
"""
POST VALIDACIÓN SWIFT (SCRIPT ÚNICO)
1) Swift_manuales -> Swift_completos (APPEND por hoja V1/V2, sin duplicar id)
   - Recalcula Nombre personalizado (Proveedor + Receiver)
   - Filtra solo registros "Completo"
   - Marca Estado="Completo" a los insertados

2) Swift_completos -> Acumulado_swift (APPEND)
   - Toma TODO lo que exista en Swift_completos (V1+V2)
   - Inserta en una sola hoja "Acumulado"
   - Agrega Versión, Fecha Control, Formulario, Llave
   - Sin duplicar por id

3) Swift_completos -> Datos_Origen_Destino_V1 / V2 (PLANTILLAS BANCOLOMBIA)
   - No toca fila 1 (logo/título)
   - Encabezados en fila 2
   - Escribe desde fila 3
   - REEMPLAZA (NO APPEND)
"""

from pathlib import Path
from datetime import datetime
import pandas as pd
import logging
from openpyxl import load_workbook


# =========================================================
# LOG
# =========================================================
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
LOGGER = logging.getLogger("post_swift_full")


# =========================================================
# RUTAS
# =========================================================
# BASE del proyecto (raíz)
BASE_ROOT = Path(r"C:\Users\johangc\Desktop\Desarrollo\Origen_Destino DIAN")

# Carpeta resultados
BASE_DIR = BASE_ROOT / "resultados"

SWIFT_MANUALES = BASE_DIR / "Swift_manuales.xlsx"
SWIFT_COMPLETOS = BASE_DIR / "Swift_completos.xlsx"

ACUMULADO = BASE_DIR / "Acumulado_swift.xlsx"
ACUMULADO_SHEET = "Acumulado"

# Plantillas Bancolombia (normalmente están en raíz del proyecto)
DESTINO_V1 = BASE_ROOT / BASE_DIR / "Datos_Origen_Destino_V1.xlsx"
DESTINO_V2 = BASE_ROOT / BASE_DIR / "Datos_Origen_Destino_V2.xlsx"

CUENTA_COMPENSACION = "2190709002"


# =========================================================
# CONFIG ACUMULADO (orden esperado)
# =========================================================
ACUMULADO_COLS = [
    "Nombre archivo",
    "Receiver",
    "Date",
    "Amount",
    "Proveedor",
    "Pais",
    "Ciudad",
    "Nombre personalizado",
    "Estado",
    "Versión",
    "Fecha Control",
    "id",
    "Formulario",
    "Llave",
]


# =========================================================
# UTILIDADES
# =========================================================
def ensure_columns(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            out[c] = ""
    return out[cols]


def recalcular_nombre_personalizado(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["Proveedor"] = out.get("Proveedor", "").fillna("").astype(str).str.strip()
    out["Receiver"] = out.get("Receiver", "").fillna("").astype(str).str.strip()
    out["Nombre personalizado"] = (out["Proveedor"] + " " + out["Receiver"]).str.strip()
    return out


def es_completo(row) -> bool:
    return (
        pd.notna(row.get("Receiver")) and str(row.get("Receiver")).strip() != ""
        and pd.notna(row.get("Proveedor")) and str(row.get("Proveedor")).strip() != ""
        and pd.notna(row.get("Amount")) and str(row.get("Amount")).strip() != ""
    )


def filtrar_completos(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["Estado"] = out.apply(lambda r: "Completo" if es_completo(r) else "Incompleto", axis=1)
    return out.loc[out["Estado"] == "Completo"].copy()


def read_sheet_safe(path: Path, sheet: str) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_excel(path, sheet_name=sheet)


def write_swift_completos(df_v1: pd.DataFrame, df_v2: pd.DataFrame) -> None:
    with pd.ExcelWriter(SWIFT_COMPLETOS, engine="openpyxl", mode="w") as writer:
        df_v1.to_excel(writer, sheet_name="V1", index=False)
        df_v2.to_excel(writer, sheet_name="V2", index=False)
    LOGGER.info(f"Swift_completos actualizado: {SWIFT_COMPLETOS}")


# =========================================================
# PASO 1) Swift_manuales -> Swift_completos
# =========================================================
def paso_1_mover_manuales_a_completos() -> tuple[pd.DataFrame, pd.DataFrame]:
    if not SWIFT_MANUALES.exists():
        raise FileNotFoundError(f"No existe: {SWIFT_MANUALES}")

    man_v1 = pd.read_excel(SWIFT_MANUALES, sheet_name="V1")
    man_v2 = pd.read_excel(SWIFT_MANUALES, sheet_name="V2")

    man_v1 = recalcular_nombre_personalizado(man_v1)
    man_v2 = recalcular_nombre_personalizado(man_v2)

    man_v1_ok = filtrar_completos(man_v1)
    man_v2_ok = filtrar_completos(man_v2)

    comp_v1 = read_sheet_safe(SWIFT_COMPLETOS, "V1")
    comp_v2 = read_sheet_safe(SWIFT_COMPLETOS, "V2")

    # Validaciones ID
    for df_name, df in [("Swift_manuales V1", man_v1_ok), ("Swift_manuales V2", man_v2_ok)]:
        if not df.empty and "id" not in df.columns:
            raise KeyError(f"{df_name} no tiene columna 'id'. Es obligatoria para no duplicar.")

    if not comp_v1.empty and "id" not in comp_v1.columns:
        raise KeyError("Swift_completos (V1) no tiene columna 'id'.")
    if not comp_v2.empty and "id" not in comp_v2.columns:
        raise KeyError("Swift_completos (V2) no tiene columna 'id'.")

    # Si no hay nuevos completos, igual devolvemos la foto actual (para que pasos siguientes no queden con data vieja)
    if man_v1_ok.empty and man_v2_ok.empty:
        LOGGER.info("No hay registros completos nuevos en Swift_manuales. Swift_completos queda igual.")
        return comp_v1, comp_v2

    ids_v1 = set(comp_v1["id"].astype(str)) if not comp_v1.empty else set()
    ids_v2 = set(comp_v2["id"].astype(str)) if not comp_v2.empty else set()

    add_v1 = man_v1_ok.loc[~man_v1_ok["id"].astype(str).isin(ids_v1)].copy()
    add_v2 = man_v2_ok.loc[~man_v2_ok["id"].astype(str).isin(ids_v2)].copy()

    if not add_v1.empty:
        add_v1["Estado"] = "Completo"
    if not add_v2.empty:
        add_v2["Estado"] = "Completo"

    new_comp_v1 = pd.concat([comp_v1, add_v1], ignore_index=True) if not add_v1.empty else comp_v1
    new_comp_v2 = pd.concat([comp_v2, add_v2], ignore_index=True) if not add_v2.empty else comp_v2

    write_swift_completos(new_comp_v1, new_comp_v2)

    total_added = len(add_v1) + len(add_v2)
    LOGGER.info(f"PASO 1 OK -> Agregados a Swift_completos: V1={len(add_v1)} | V2={len(add_v2)} | Total={total_added}")

    # DEVOLVEMOS LA BASE YA ACTUALIZADA (clave para el paso 3)
    return new_comp_v1, new_comp_v2


# =========================================================
# PASO 2) Swift_completos -> Acumulado_swift
# =========================================================
def construir_df_para_acumulado(df_comp_v1: pd.DataFrame, df_comp_v2: pd.DataFrame) -> pd.DataFrame:
    df_v1 = df_comp_v1.copy()
    df_v2 = df_comp_v2.copy()

    df_v1["Versión"] = "V1"
    df_v2["Versión"] = "V2"

    df_all = pd.concat([df_v1, df_v2], ignore_index=True)

    ahora = datetime.now().strftime("%Y-%m-%d %H:%M")
    df_all["Fecha Control"] = ahora
    df_all["Formulario"] = ""
    df_all["Llave"] = ""

    if "id" not in df_all.columns:
        raise KeyError("Swift_completos no tiene columna 'id'. Es obligatoria para el acumulado.")

    df_all.columns = [str(c).strip() for c in df_all.columns]
    df_all = ensure_columns(df_all, ACUMULADO_COLS)
    return df_all


def append_acumulado(df_new: pd.DataFrame) -> None:
    if ACUMULADO.exists():
        df_old = pd.read_excel(ACUMULADO, sheet_name=ACUMULADO_SHEET)
        df_old.columns = [str(c).strip() for c in df_old.columns]
        df_old = ensure_columns(df_old, ACUMULADO_COLS)

        ids_old = set(df_old["id"].astype(str))
        df_to_add = df_new.loc[~df_new["id"].astype(str).isin(ids_old)].copy()

        if df_to_add.empty:
            LOGGER.info("PASO 2 -> Acumulado: no hay nuevos registros (todos los id ya existen).")
            return

        df_final = pd.concat([df_old, df_to_add], ignore_index=True)
        LOGGER.info(f"PASO 2 OK -> Acumulado agregados={len(df_to_add)} | total={len(df_final)}")
    else:
        df_final = df_new.copy()
        LOGGER.info(f"PASO 2 -> Acumulado no existe. Se crea nuevo con registros={len(df_final)}")

    with pd.ExcelWriter(ACUMULADO, engine="openpyxl", mode="w") as writer:
        df_final.to_excel(writer, sheet_name=ACUMULADO_SHEET, index=False)

    LOGGER.info(f"Acumulado actualizado: {ACUMULADO}")


# =========================================================
# PASO 3) Swift_completos -> Plantillas Bancolombia (REEMPLAZA SIEMPRE)
# =========================================================
def write_bancolombia_template_replace(df_source: pd.DataFrame, template_path: Path) -> None:
    """
    REEMPLAZA la data del plano:
    - No toca fila 1
    - Encabezados en fila 2
    - Limpia desde fila 3 hacia abajo
    - Escribe desde fila 3
    - No toca Observaciones
    """
    if not template_path.exists():
        raise FileNotFoundError(f"No existe plantilla destino: {template_path}")

    wb = load_workbook(template_path)
    ws = wb.active

    # Mapear headers (fila 2)
    header_map = {}
    for cell in ws[2]:
        val = str(cell.value).strip() if cell.value is not None else ""
        if val:
            header_map[val] = cell.column

    required = ["Cuenta compensación", "Nombre / Razón social", "País", "Ciudad", "Nombre personalizado"]
    missing = [c for c in required if c not in header_map]
    if missing:
        raise KeyError(f"Plantilla {template_path.name} no tiene encabezados requeridos en fila 2: {missing}")

    start_row = 3

    # ✅ LIMPIAR SOLO DATOS DESDE FILA 3 (REEMPLAZO TOTAL)
    if ws.max_row >= start_row:
        ws.delete_rows(start_row, ws.max_row - start_row + 1)

    # Escribir desde fila 3
    current_row = start_row

    # Si no hay data, igual dejamos el plano limpio (solo header)
    if df_source.empty:
        wb.save(template_path)
        LOGGER.info(f"PASO 3 OK -> Plano limpio (sin registros): {template_path}")
        return

    for _, r in df_source.iterrows():
        ws.cell(row=current_row, column=header_map["Cuenta compensación"]).value = CUENTA_COMPENSACION
        ws.cell(row=current_row, column=header_map["Nombre / Razón social"]).value = r.get("Proveedor", "")
        ws.cell(row=current_row, column=header_map["País"]).value = r.get("Pais", "")
        ws.cell(row=current_row, column=header_map["Ciudad"]).value = r.get("Ciudad", "")
        ws.cell(row=current_row, column=header_map["Nombre personalizado"]).value = r.get("Nombre personalizado", "")
        current_row += 1

    wb.save(template_path)
    LOGGER.info(f"PASO 3 OK -> Plano reemplazado: {template_path}")


def paso_3_trasladar_a_plantillas(df_comp_v1: pd.DataFrame, df_comp_v2: pd.DataFrame) -> None:
    # Por seguridad: solo completos
    df_v1 = df_comp_v1.copy()
    df_v2 = df_comp_v2.copy()

    if "Estado" in df_v1.columns:
        df_v1 = df_v1.loc[df_v1["Estado"] == "Completo"].copy()
    if "Estado" in df_v2.columns:
        df_v2 = df_v2.loc[df_v2["Estado"] == "Completo"].copy()

    write_bancolombia_template_replace(df_v1, DESTINO_V1)
    write_bancolombia_template_replace(df_v2, DESTINO_V2)


# =========================================================
# MAIN (orden exacto)
# =========================================================
def main():
    LOGGER.info("=== INICIO POST VALIDACIÓN (PASO 1 + PASO 2 + PASO 3) ===")

    # PASO 1 -> obtenemos Swift_completos ya ACTUALIZADO
    df_comp_v1, df_comp_v2 = paso_1_mover_manuales_a_completos()

    # PASO 2 -> Acumulado basado en Swift_completos actualizado
    df_acum = construir_df_para_acumulado(df_comp_v1, df_comp_v2)
    append_acumulado(df_acum)

    # PASO 3 -> Planos REEMPLAZAN usando Swift_completos actualizado
    paso_3_trasladar_a_plantillas(df_comp_v1, df_comp_v2)

    LOGGER.info("=== FIN POST VALIDACIÓN ===")


if __name__ == "__main__":
    main()