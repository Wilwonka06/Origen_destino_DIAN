# -*- coding: utf-8 -*-
"""
main.py — Orquestador del pipeline Origen_Destino_DIAN

MODOS DE EJECUCIÓN:
  Todos los modos aceptan --tipo imp (default) o --tipo exp.
  La lógica es idéntica — solo cambian los archivos que se procesan.

  python main.py                        → proceso completo IMP
  python main.py --tipo exp             → proceso completo EXP
  python main.py --modo ocr             → solo extracción OCR
  python main.py --modo post_auto       → pasar manuales a completos
  python main.py --modo plantilla       → generar plantilla Bancolombia
  python main.py --modo cruces          → solo cruces
  python main.py --forzar               → ignora caché (aplica al modo ocr)
  python gui_launcher.py                → interfaz gráfica

FLUJO COMPLETO (igual para IMP y EXP, cambia solo --tipo):
  1. ocr       → extrae PDFs → Swift_completos_{tipo}
  2. post_auto → manuales → completos + acumulado
  3. plantilla → genera Datos_Origen_Destino_V1/V2_{tipo}  ← subir a Bancolombia
  4. cruces    → Formulario + Llave + Llave OD  ← requiere origenDestino descargado
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import config
from core.logger import init_logging, get_logger
from core.validators import validate_input_files, validate_output_dirs
from core.cache import PdfCache

init_logging()
LOGGER = get_logger("main")


# =========================================================
# DATACLASS DE RESULTADO
# =========================================================
@dataclass
class PipelineResult:
    modo:                str
    tipo:                str = "imp"
    inicio:              datetime = field(default_factory=datetime.now)
    fin:                 Optional[datetime] = None

    pdfs_nuevos_v1:      int = 0
    pdfs_nuevos_v2:      int = 0
    pdfs_completos:      int = 0
    pdfs_incompletos:    int = 0
    pdfs_error:          int = 0

    manuales_movidos:    int = 0
    manuales_pendientes: int = 0

    plantilla_registros:  int = 0
    formularios_cruzados: int = 0
    llaves_cruzadas:      int = 0

    errores:             list[str] = field(default_factory=list)
    advertencias:        list[str] = field(default_factory=list)
    exitoso:             bool = True

    @property
    def duracion_segundos(self) -> float:
        if self.fin:
            return (self.fin - self.inicio).total_seconds()
        return 0.0

    def resumen(self) -> str:
        tipo_label = "IMPORTACIONES" if self.tipo == "imp" else "EXPORTACIONES"
        lines = [
            "=" * 60,
            f"  RESUMEN — modo: {self.modo.upper()}  |  {tipo_label}",
            f"  Duración: {self.duracion_segundos:.1f}s",
            "=" * 60,
        ]
        if self.pdfs_nuevos_v1 + self.pdfs_nuevos_v2 > 0:
            lines += [
                f"  PDFs procesados V1:  {self.pdfs_nuevos_v1}",
                f"  PDFs procesados V2:  {self.pdfs_nuevos_v2}",
                f"  ✔ Completos:         {self.pdfs_completos}",
                f"  ✗ Incompletos:       {self.pdfs_incompletos}",
            ]
            if self.pdfs_error:
                lines.append(f"  ⚠ Con error:         {self.pdfs_error}")
        if self.manuales_movidos + self.manuales_pendientes > 0:
            lines += [
                f"  Manuales movidos:     {self.manuales_movidos}",
                f"  Manuales pendientes:  {self.manuales_pendientes}",
            ]
        if self.plantilla_registros:
            lines.append(f"  Plantilla generada:   {self.plantilla_registros} registros")
        if self.formularios_cruzados:
            lines.append(f"  Formularios cruzados: {self.formularios_cruzados}")
        if self.llaves_cruzadas:
            lines.append(f"  Llaves asignadas:     {self.llaves_cruzadas}")
        if self.advertencias:
            lines.append("  ADVERTENCIAS:")
            for w in self.advertencias:
                lines.append(f"    ⚠ {w}")
        if self.errores:
            lines.append("  ERRORES:")
            for e in self.errores:
                lines.append(f"    ✗ {e}")
        estado = "✔ EXITOSO" if self.exitoso else "✗ FALLÓ"
        lines += [f"  Estado: {estado}", "=" * 60]
        return "\n".join(lines)


# =========================================================
# PASO 1 — EXTRACCIÓN OCR
# =========================================================
def _run_ocr(result: PipelineResult, forzar: bool = False, tipo: str = "imp") -> None:
    LOGGER.info(f"── PASO 1: Extracción OCR [{tipo.upper()}] ──")

    try:
        validate_output_dirs(config.DIR_RESULTADOS)
    except FileNotFoundError as e:
        result.errores.append(str(e))
        result.exitoso = False
        return

    cache_file = config.CACHE_FILE_IMP if tipo == "imp" else config.CACHE_FILE_EXP
    cache = PdfCache(cache_file)
    if forzar:
        LOGGER.info("Modo --forzar activo: vaciando caché")
        cache.clear()

    try:
        from scripts.run_pipeline import run_pipeline_completo
        stats = run_pipeline_completo(cache=cache, debug=config.DEBUG, tipo=tipo)

        result.pdfs_nuevos_v1   = stats.get("nuevos_v1", 0)
        result.pdfs_nuevos_v2   = stats.get("nuevos_v2", 0)
        result.pdfs_completos   = stats.get("completos", 0)
        result.pdfs_incompletos = stats.get("incompletos", 0)
        result.pdfs_error       = stats.get("errores", 0)

        cache.save()
        LOGGER.info(f"── PASO 1 completado [{tipo.upper()}] ──")

    except Exception as e:
        LOGGER.error(f"Error en extracción OCR: {e}", exc_info=True)
        result.errores.append(f"Extracción OCR: {e}")
        result.exitoso = False


# =========================================================
# PASO 2 — MOVER MANUALES
# =========================================================
def _run_post_manual(
    result: PipelineResult,
    confirmar: bool = True,
    tipo: str = "imp",
) -> None:
    LOGGER.info(f"── PASO 2: Post validación [{tipo.upper()}] ──")

    # Seleccionar archivos según tipo
    swift_manuales = config.SWIFT_MANUALES_IMP if tipo == "imp" else config.SWIFT_MANUALES_EXP

    if not swift_manuales.exists():
        result.advertencias.append(
            f"Swift_manuales_{tipo}.xlsx no existe aún. Post validación omitida."
        )
        LOGGER.warning(f"Swift_manuales_{tipo}.xlsx no existe.")
        return

    try:
        from scripts.post_validacion_swift import (
            contar_listos_en_manuales,
            ejecutar_mover_manuales,
        )

        listos, pendientes = contar_listos_en_manuales(tipo=tipo)

        print("\n" + "─" * 50)
        print(f"  [{tipo.upper()}] Registros en Swift_manuales:  {listos + pendientes}")
        print(f"  ✔ Listos para mover:              {listos}")
        print(f"  ✗ Aún incompletos:                {pendientes}")
        print("─" * 50)

        if listos == 0:
            print("  → No hay registros listos para mover.\n")
            result.manuales_pendientes = pendientes
            return

        if confirmar:
            respuesta = input("  ¿Confirmar movimiento? (s/n): ").strip().lower()
            if respuesta not in ("s", "si", "sí", "y", "yes"):
                print("  → Cancelado.\n")
                return

        movidos = ejecutar_mover_manuales(tipo=tipo)
        result.manuales_movidos    = movidos
        result.manuales_pendientes = pendientes
        LOGGER.info(f"── PASO 2 completado: {movidos} movidos [{tipo.upper()}] ──")

    except Exception as e:
        LOGGER.error(f"Error en post validación: {e}", exc_info=True)
        result.errores.append(f"Post validación: {e}")
        result.exitoso = False


# =========================================================
# PASO 3 — GENERAR PLANTILLA BANCOLOMBIA
# =========================================================
def _run_plantilla(result: PipelineResult, tipo: str = "imp") -> None:
    LOGGER.info(f"── PASO 3: Generar Plantilla Bancolombia [{tipo.upper()}] ──")

    swift_completos = (
        config.SWIFT_COMPLETOS_IMP if tipo == "imp"
        else config.SWIFT_COMPLETOS_EXP
    )

    if not swift_completos.exists():
        result.advertencias.append(
            f"Swift_completos no existe. Ejecuta primero la extracción OCR."
        )
        return

    try:
        from scripts.post_validacion_swift import run_generar_plantilla
        stats = run_generar_plantilla(tipo=tipo)
        result.plantilla_registros = stats.get("registros", 0)
        LOGGER.info(
            f"── PASO 3 completado: {result.plantilla_registros} registros "
            f"en plantilla [{tipo.upper()}] ──"
        )

    except Exception as e:
        LOGGER.error(f"Error generando plantilla: {e}", exc_info=True)
        result.errores.append(f"Generar plantilla: {e}")
        result.exitoso = False


# =========================================================
# PASO 4 — CRUCES (IMP y EXP)
# =========================================================
def _run_cruces(result: PipelineResult, tipo: str = "imp") -> None:
    LOGGER.info(f"── PASO 4: Cruces [{tipo.upper()}] ──")

    swift_completos = (
        config.SWIFT_COMPLETOS_IMP if tipo == "imp"
        else config.SWIFT_COMPLETOS_EXP
    )

    try:
        validate_input_files(
            swift_completos,
            config.XLSB_CUENTA_COM,
            config.ORIGEN_DESTINO,
            context=f"cruces_{tipo}",
        )
    except FileNotFoundError as e:
        result.errores.append(str(e))
        result.exitoso = False
        return

    try:
        from scripts.run_formulario import run_cruce_completo
        stats = run_cruce_completo(tipo=tipo)
        result.formularios_cruzados = stats.get("formularios", 0)
        result.llaves_cruzadas      = stats.get("llaves", 0)
        LOGGER.info(f"── PASO 4 completado [{tipo.upper()}] ──")

    except Exception as e:
        LOGGER.error(f"Error en cruces: {e}", exc_info=True)
        result.errores.append(f"Cruces: {e}")
        result.exitoso = False


# =========================================================
# FUNCIÓN PRINCIPAL
# =========================================================
def run_pipeline(
    modo:      str  = "completo",
    forzar:    bool = False,
    confirmar: bool = True,
    tipo:      str  = "imp",
) -> PipelineResult:
    """
    Ejecuta el pipeline.

    Parámetros:
        modo      : "completo" | "ocr" | "post" | "post_auto" | "plantilla" | "cruces"
        forzar    : ignorar caché
        confirmar : pedir confirmación antes de mover manuales
        tipo      : "imp" | "exp"
    """
    tipo = tipo.lower().strip()
    result = PipelineResult(modo=modo, tipo=tipo)

    tipo_label = "IMPORTACIONES" if tipo == "imp" else "EXPORTACIONES"
    LOGGER.info(f"╔══════════════════════════════════════════╗")
    LOGGER.info(f"  INICIO PIPELINE — modo: {modo.upper()} | {tipo_label}")
    LOGGER.info(f"  Base: {config.BASE_ROOT}")
    LOGGER.info(f"╚══════════════════════════════════════════╝")

    try:
        # Paso 1: Extracción OCR
        if modo in ("completo", "ocr"):
            _run_ocr(result, forzar=forzar, tipo=tipo)

        # Paso 2: Pasar manuales a completos + acumulado
        if modo in ("completo", "post"):
            _run_post_manual(result, confirmar=confirmar, tipo=tipo)

        if modo == "post_auto":
            _run_post_manual(result, confirmar=False, tipo=tipo)

        # Paso 3: Generar plantilla Bancolombia
        if modo in ("completo", "plantilla"):
            _run_plantilla(result, tipo=tipo)

        # Paso 4: Cruces (Formulario + Llave + Llave OD)
        if modo in ("completo", "cruces"):
            swift_completos = (
                config.SWIFT_COMPLETOS_IMP if tipo == "imp"
                else config.SWIFT_COMPLETOS_EXP
            )
            if swift_completos.exists():
                _run_cruces(result, tipo=tipo)
            else:
                result.advertencias.append(
                    f"Swift_completos no existe. Cruces omitidos."
                )

    except Exception as e:
        LOGGER.error(f"Error inesperado: {e}", exc_info=True)
        result.errores.append(f"Error inesperado: {e}")
        result.exitoso = False
    finally:
        result.fin = datetime.now()

    print(result.resumen())
    return result


# =========================================================
# CLI
# =========================================================
def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pipeline Origen_Destino_DIAN",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--modo",
        choices=["completo", "ocr", "post", "post_auto", "plantilla", "cruces"],
        default="completo",
        help="Modo de ejecución (default: completo)",
    )
    parser.add_argument(
        "--tipo",
        choices=["imp", "exp"],
        default="imp",
        help="Tipo: imp=Importaciones | exp=Exportaciones (default: imp)",
    )
    parser.add_argument(
        "--forzar",
        action="store_true",
        help="Ignora caché y reprocesa todos los PDFs",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    resultado = run_pipeline(
        modo=args.modo,
        forzar=args.forzar,
        confirmar=(args.modo == "post"),
        tipo=args.tipo,
    )
    sys.exit(0 if resultado.exitoso else 1)