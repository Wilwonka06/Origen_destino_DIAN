# -*- coding: utf-8 -*-
"""
post_validacion_swift.py — Post validación y traslado de manuales corregidos

Pasos:
  1) Swift_manuales → Swift_completos
     Lee los registros corregidos manualmente, filtra los completos
     y los mueve a Swift_completos sin duplicar por 'id'.

  2) Swift_completos → Acumulado_swift (APPEND sin duplicar por id)
     Agrega Versión, Fecha Control. Acumula historial.

  3) Swift_completos → Plantillas Bancolombia (REEMPLAZA)
     Escribe en Datos_Origen_Destino_V1.xlsx y V2.xlsx desde fila 3,
     sin tocar fila 1 (logo) ni encabezados en fila 2.

CAMBIOS vs versión anterior:
  - Rutas y constantes → config.py
  - Logging → core.logger
  - excel_utils.write_sheets / read_sheet_safe / ensure_columns
  - Funciones públicas contar_listos_en_manuales() y ejecutar_mover_manuales()
    para ser llamadas desde main.py (modo post)
  - Lógica funcional sin cambios
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook

import config
from core.logger import get_logger
from core.excel_utils import read_sheet_safe, write_sheets, ensure_columns
from core.text_utils import build_nombre_personalizado
from core.validators import validate_input_files

LOGGER = get_logger(__name__)

# Límite de caracteres para "Nombre personalizado" en plantillas Bancolombia
NOMBRE_PERSONALIZADO_LIMITE: int = 50


# =========================================================
# UTILIDADES INTERNAS
# =========================================================
def _recalcular_nombre_personalizado(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["Nombre personalizado"] = out.apply(
        lambda r: build_nombre_personalizado(r.get("Proveedor"), r.get("Receiver")),
        axis=1,
    )
    return out


def _es_completo(row) -> bool:
    """Un registro es completo si tiene Receiver, Proveedor y Amount."""
    def _has(v) -> bool:
        if v is None:
            return False
        if isinstance(v, float) and pd.isna(v):
            return False
        return str(v).strip().lower() not in ("", "nan", "none", "nat")

    return _has(row.get("Receiver")) and _has(row.get("Proveedor")) and _has(row.get("Amount"))


def _filtrar_completos(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["Estado"] = out.apply(lambda r: "Completo" if _es_completo(r) else "Incompleto", axis=1)
    return out.loc[out["Estado"] == "Completo"].copy()


def _ensure_id_column(df: pd.DataFrame, label: str) -> None:
    """Lanza error descriptivo si el DataFrame no tiene columna 'id'."""
    if not df.empty and "id" not in df.columns:
        raise KeyError(
            f"{label} no tiene columna 'id'. "
            "Esta columna es obligatoria para evitar duplicados."
        )


# =========================================================
# FUNCIONES PÚBLICAS PARA main.py (modo post)
# =========================================================
def contar_listos_en_manuales() -> tuple[int, int]:
    """
    Lee Swift_manuales y cuenta cuántos registros están listos para mover
    (completos) y cuántos aún están incompletos.

    Retorna: (listos, pendientes)
    Usado por main.py para mostrar el resumen antes de confirmar.
    """
    if not config.SWIFT_MANUALES.exists():
        return 0, 0

    df_v1 = read_sheet_safe(config.SWIFT_MANUALES, config.SHEET_V1, context="contar_manuales")
    df_v2 = read_sheet_safe(config.SWIFT_MANUALES, config.SHEET_V2, context="contar_manuales")

    # Recalcular nombre personalizado antes de evaluar
    df_v1 = _recalcular_nombre_personalizado(df_v1) if not df_v1.empty else df_v1
    df_v2 = _recalcular_nombre_personalizado(df_v2) if not df_v2.empty else df_v2

    listos = sum([
        len(_filtrar_completos(df_v1)) if not df_v1.empty else 0,
        len(_filtrar_completos(df_v2)) if not df_v2.empty else 0,
    ])
    total = len(df_v1) + len(df_v2)
    pendientes = total - listos

    return listos, pendientes


def ejecutar_mover_manuales() -> int:
    """
    Mueve los registros completos de Swift_manuales a Swift_completos.
    Retorna el número de registros efectivamente movidos.
    """
    return _paso_1_mover_manuales()[2]  # (df_v1, df_v2, total_movidos)


# =========================================================
# PASO 1) Swift_manuales → Swift_completos
# =========================================================
def _paso_1_mover_manuales() -> tuple[pd.DataFrame, pd.DataFrame, int]:
    """
    Lee manuales, filtra completos, los agrega a Swift_completos sin duplicar.
    Retorna (df_comp_v1_actualizado, df_comp_v2_actualizado, total_movidos).
    """
    validate_input_files(config.SWIFT_MANUALES, context="post_validacion")

    man_v1 = read_sheet_safe(config.SWIFT_MANUALES, config.SHEET_V1, context="manuales")
    man_v2 = read_sheet_safe(config.SWIFT_MANUALES, config.SHEET_V2, context="manuales")

    # Recalcular Nombre personalizado (puede haber sido corregido manualmente)
    man_v1 = _recalcular_nombre_personalizado(man_v1) if not man_v1.empty else man_v1
    man_v2 = _recalcular_nombre_personalizado(man_v2) if not man_v2.empty else man_v2

    man_v1_ok = _filtrar_completos(man_v1) if not man_v1.empty else pd.DataFrame()
    man_v2_ok = _filtrar_completos(man_v2) if not man_v2.empty else pd.DataFrame()

    comp_v1 = read_sheet_safe(config.SWIFT_COMPLETOS, config.SHEET_V1, context="completos")
    comp_v2 = read_sheet_safe(config.SWIFT_COMPLETOS, config.SHEET_V2, context="completos")

    # Validar columna id
    for label, df in [
        ("Swift_manuales V1", man_v1_ok),
        ("Swift_manuales V2", man_v2_ok),
        ("Swift_completos V1", comp_v1),
        ("Swift_completos V2", comp_v2),
    ]:
        _ensure_id_column(df, label)

    # Si no hay registros nuevos completos, retornar el estado actual
    if man_v1_ok.empty and man_v2_ok.empty:
        LOGGER.info("No hay registros completos nuevos en Swift_manuales.")
        return comp_v1, comp_v2, 0

    def _merge_sin_duplicar(comp: pd.DataFrame, nuevos: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        if nuevos.empty:
            return comp, 0
        ids_existentes = set(comp["id"].astype(str)) if not comp.empty else set()
        to_add = nuevos.loc[~nuevos["id"].astype(str).isin(ids_existentes)].copy()
        if to_add.empty:
            return comp, 0
        to_add["Estado"] = "Completo"
        merged = pd.concat([comp, to_add], ignore_index=True) if not comp.empty else to_add
        return merged, len(to_add)

    new_comp_v1, added_v1 = _merge_sin_duplicar(comp_v1, man_v1_ok)
    new_comp_v2, added_v2 = _merge_sin_duplicar(comp_v2, man_v2_ok)
    total_movidos = added_v1 + added_v2

    if total_movidos > 0:
        write_sheets(
            config.SWIFT_COMPLETOS,
            {config.SHEET_V1: new_comp_v1, config.SHEET_V2: new_comp_v2},
            context="paso_1_manuales",
        )
        LOGGER.info(
            f"PASO 1 OK → movidos a Swift_completos: "
            f"V1={added_v1} | V2={added_v2} | total={total_movidos}"
        )
    else:
        LOGGER.info("PASO 1: Todos los registros completos ya existían en Swift_completos.")

    return new_comp_v1, new_comp_v2, total_movidos


# =========================================================
# PASO 2) Swift_completos → Acumulado_swift (APPEND)
# =========================================================
def _paso_2_acumulado(df_v1: pd.DataFrame, df_v2: pd.DataFrame) -> None:
    """Agrega a Acumulado_swift los registros nuevos de Swift_completos."""
    df_v1 = df_v1.copy()
    df_v2 = df_v2.copy()
    df_v1["Versión"] = "V1"
    df_v2["Versión"] = "V2"

    df_all = pd.concat([df_v1, df_v2], ignore_index=True)

    if "id" not in df_all.columns:
        raise KeyError("Swift_completos no tiene columna 'id'. Es obligatoria para el acumulado.")

    ahora = datetime.now().strftime("%Y-%m-%d %H:%M")
    df_all["Fecha Control"] = ahora

    df_all = ensure_columns(df_all, config.ACUMULADO_COLS)
    df_new = df_all[config.ACUMULADO_COLS].copy()

    if config.ACUMULADO_SWIFT.exists():
        df_old = read_sheet_safe(
            config.ACUMULADO_SWIFT, config.SHEET_ACUMULADO, context="acumulado"
        )
        df_old = ensure_columns(df_old, config.ACUMULADO_COLS)

        ids_old = set(df_old["id"].astype(str))
        df_to_add = df_new.loc[~df_new["id"].astype(str).isin(ids_old)].copy()

        if df_to_add.empty:
            LOGGER.info("PASO 2: Sin registros nuevos para acumulado (todos ya existen).")
            return

        df_final = pd.concat([df_old, df_to_add], ignore_index=True)
        LOGGER.info(
            f"PASO 2 OK → Acumulado: agregados={len(df_to_add)} | "
            f"total={len(df_final)}"
        )
    else:
        df_final = df_new.copy()
        LOGGER.info(f"PASO 2: Acumulado creado con {len(df_final)} registros.")

    write_sheets(
        config.ACUMULADO_SWIFT,
        {config.SHEET_ACUMULADO: df_final},
        context="acumulado",
    )


# =========================================================
# TRANSFORMACIONES PARA PLANTILLAS BANCOLOMBIA
# =========================================================
def _recortar_nombre_personalizado(valor, limite: int = NOMBRE_PERSONALIZADO_LIMITE):
    """
    Recorta "Nombre personalizado" al límite de caracteres preservando el código
    SWIFT (última palabra). Elimina palabras del nombre de derecha a izquierda
    hasta que el texto completo quepa dentro del límite.

    Ejemplos (limite=50):
      "EMPRESA MUY LARGA CON NOMBRE EXTENSO BOFAUS3N"
        → "EMPRESA MUY LARGA CON BOFAUS3N"   (si supera 50 chars)
      "EMPRESA CORTA BOFAUS3N"
        → sin cambio                          (ya dentro del límite)
    """
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


def _preparar_df_para_plantilla(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica las transformaciones previas a la escritura en plantilla Bancolombia:
      1. Solo registros Completos
      2. Deduplicar filas exactas
      3. Recortar "Nombre personalizado" a NOMBRE_PERSONALIZADO_LIMITE chars

    Retorna el DataFrame listo para ser escrito desde fila 3.
    """
    out = df.copy()

    # 1) Solo completos
    if "Estado" in out.columns:
        out = out.loc[out["Estado"] == "Completo"].copy()

    # 2) Deduplicar
    antes = len(out)
    out = out.drop_duplicates()
    duplicados = antes - len(out)
    if duplicados:
        LOGGER.info(f"Plantilla: {duplicados} filas duplicadas eliminadas.")

    # 3) Recortar Nombre personalizado
    if "Nombre personalizado" in out.columns:
        recortados = 0
        originales = out["Nombre personalizado"].copy()
        out["Nombre personalizado"] = out["Nombre personalizado"].apply(
            _recortar_nombre_personalizado
        )
        recortados = (out["Nombre personalizado"] != originales).sum()
        if recortados:
            LOGGER.info(
                f"Plantilla: {recortados} nombres recortados "
                f"a {NOMBRE_PERSONALIZADO_LIMITE} chars."
            )

    return out


# =========================================================
# PASO 3) Swift_completos → Plantillas Bancolombia (REEMPLAZA)
# =========================================================
def _write_bancolombia_template(df_source: pd.DataFrame, template_path: Path) -> None:
    """
    Escribe datos en la plantilla Bancolombia:
      - Fila 1: logo/título (intocable)
      - Fila 2: encabezados (intocable)
      - Fila 3+: datos (REEMPLAZA completamente)
    """
    if not template_path.exists():
        raise FileNotFoundError(
            f"No existe la plantilla Bancolombia: {template_path}\n"
            f"Verificá que la carpeta '{config.DIR_PLANTILLAS}' existe y tiene los archivos."
        )

    # Aplicar transformaciones: filtro completos + deduplicar + recortar nombres
    df_write = _preparar_df_para_plantilla(df_source)

    wb = load_workbook(template_path)
    ws = wb.active

    # Mapear encabezados de fila 2
    header_map = {
        str(cell.value).strip(): cell.column
        for cell in ws[2]
        if cell.value is not None
    }

    required_headers = [
        "Cuenta compensación",
        "Nombre / Razón social",
        "País",
        "Ciudad",
        "Nombre personalizado",
    ]
    missing = [h for h in required_headers if h not in header_map]
    if missing:
        raise KeyError(
            f"Plantilla {template_path.name} no tiene encabezados requeridos "
            f"en fila 2: {missing}"
        )

    # Limpiar datos desde fila 3
    start_row = 3
    if ws.max_row >= start_row:
        ws.delete_rows(start_row, ws.max_row - start_row + 1)

    if df_write.empty:
        wb.save(template_path)
        LOGGER.info(f"PASO 3: Plantilla limpia (sin registros): {template_path.name}")
        return

    for current_row, (_, r) in enumerate(df_write.iterrows(), start=start_row):
        ws.cell(row=current_row, column=header_map["Cuenta compensación"]).value  = config.CUENTA_COMPENSACION
        ws.cell(row=current_row, column=header_map["Nombre / Razón social"]).value = r.get("Proveedor", "")
        ws.cell(row=current_row, column=header_map["País"]).value                  = r.get("Pais", "")
        ws.cell(row=current_row, column=header_map["Ciudad"]).value                = r.get("Ciudad", "")
        ws.cell(row=current_row, column=header_map["Nombre personalizado"]).value  = r.get("Nombre personalizado", "")

    wb.save(template_path)
    LOGGER.info(
        f"PASO 3 OK → Plantilla actualizada: {template_path.name} "
        f"({len(df_write)} registros desde fila 3)"
    )


def _paso_3_plantillas(df_v1: pd.DataFrame, df_v2: pd.DataFrame) -> None:
    """Escribe las plantillas Bancolombia V1 y V2."""
    _write_bancolombia_template(df_v1, config.PLANTILLA_V1)
    _write_bancolombia_template(df_v2, config.PLANTILLA_V2)


# =========================================================
# FUNCIÓN PRINCIPAL — llamada desde main.py
# =========================================================
def run_post_validacion() -> dict:
    """
    Ejecuta los 3 pasos de post validación.

    Retorna dict con estadísticas:
        movidos, acumulado_total
    """
    LOGGER.info("=== INICIO POST VALIDACIÓN ===")

    # Paso 1: Mover manuales completos → Swift_completos
    df_comp_v1, df_comp_v2, total_movidos = _paso_1_mover_manuales()

    # Paso 2: Acumulado
    if not df_comp_v1.empty or not df_comp_v2.empty:
        _paso_2_acumulado(df_comp_v1, df_comp_v2)
    else:
        LOGGER.info("PASO 2: Swift_completos vacío, se omite acumulado.")

    # Paso 3: Plantillas Bancolombia
    try:
        _paso_3_plantillas(df_comp_v1, df_comp_v2)
    except FileNotFoundError as e:
        LOGGER.warning(f"PASO 3 omitido: {e}")

    LOGGER.info(f"=== FIN POST VALIDACIÓN ===  Movidos={total_movidos}")
    return {"movidos": total_movidos}
