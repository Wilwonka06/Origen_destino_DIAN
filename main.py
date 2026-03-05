# -*- coding: utf-8 -*-
"""
main.py — Orquestador del pipeline Origen_Destino_DIAN

Punto de entrada único para ejecutar cualquier parte del flujo.

MODOS DE EJECUCIÓN:
  python main.py                    → pipeline completo
  python main.py --modo ocr         → solo extracción OCR de PDFs
  python main.py --modo cruces      → solo cruce formulario + llave
  python main.py --modo post        → mover manuales corregidos a completos
  python main.py --modo post_auto   → igual que post pero sin pedir confirmación
  python main.py --forzar           → ignora caché, reprocesa todos los PDFs

DISEÑADO PARA GUI:
  Cada modo retorna un PipelineResult con toda la información
  necesaria para que una interfaz muestre el resumen al usuario.
  
  from main import run_pipeline
  result = run_pipeline(modo="completo")
  print(result.resumen())
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

# =========================================================
# INICIALIZACIÓN TEMPRANA DE LOGGING Y CONFIG
# =========================================================
import config
from core.logger import init_logging, get_logger
from core.validators import validate_input_files, validate_output_dirs
from core.cache import PdfCache

init_logging()
LOGGER = get_logger("main")


# =========================================================
# DATACLASS DE RESULTADO (para GUI futura)
# =========================================================
@dataclass
class PipelineResult:
    """
    Resultado de una ejecución del pipeline.
    Diseñado para ser consumido por una GUI.
    """
    modo:               str
    inicio:             datetime     = field(default_factory=datetime.now)
    fin:                Optional[datetime] = None

    # OCR
    pdfs_nuevos_v1:     int = 0
    pdfs_nuevos_v2:     int = 0
    pdfs_completos:     int = 0
    pdfs_incompletos:   int = 0
    pdfs_error:         int = 0

    # Post validación
    manuales_movidos:   int = 0
    manuales_pendientes: int = 0

    # Cruces
    formularios_cruzados: int = 0
    llaves_cruzadas:      int = 0

    # General
    errores:            list[str] = field(default_factory=list)
    advertencias:       list[str] = field(default_factory=list)
    exitoso:            bool = True

    @property
    def duracion_segundos(self) -> float:
        if self.fin:
            return (self.fin - self.inicio).total_seconds()
        return 0.0

    def resumen(self) -> str:
        """Texto de resumen para mostrar en consola o GUI."""
        lines = [
            "=" * 60,
            f"  RESUMEN PIPELINE — modo: {self.modo.upper()}",
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
                f"  Manuales movidos a completos: {self.manuales_movidos}",
                f"  Manuales aún incompletos:     {self.manuales_pendientes}",
            ]

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
        lines.append(f"  Estado: {estado}")
        lines.append("=" * 60)
        return "\n".join(lines)


# =========================================================
# PASO 1 — EXTRACCIÓN OCR
# =========================================================
def _run_ocr(result: PipelineResult, forzar: bool = False) -> None:
    """Ejecuta la extracción OCR de PDFs V1 y V2."""
    LOGGER.info("── PASO 1: Extracción OCR ──")

    try:
        validate_input_files(
            config.DIR_SWIFT_RAIZ,
            config.DIR_SWIFT_RAIZ,
            config.BD_PROVEEDORES,
            config.BD_SWIFT,
            context="OCR"
        )
        validate_output_dirs(config.DIR_RESULTADOS)
    except FileNotFoundError as e:
        result.errores.append(str(e))
        result.exitoso = False
        return

    cache = PdfCache(config.CACHE_FILE)
    if forzar:
        LOGGER.info("Modo --forzar activo: vaciando caché")
        cache.clear()

    try:
        from scripts.run_pipeline import run_pipeline_completo
        stats = run_pipeline_completo(cache=cache, debug=config.DEBUG)

        result.pdfs_nuevos_v1   = stats.get("nuevos_v1", 0)
        result.pdfs_nuevos_v2   = stats.get("nuevos_v2", 0)
        result.pdfs_completos   = stats.get("completos", 0)
        result.pdfs_incompletos = stats.get("incompletos", 0)
        result.pdfs_error       = stats.get("errores", 0)

        cache.save()
        LOGGER.info("── PASO 1 completado ──")

    except Exception as e:
        LOGGER.error(f"Error en extracción OCR: {e}", exc_info=True)
        result.errores.append(f"Extracción OCR: {e}")
        result.exitoso = False


# =========================================================
# PASO 2 — MOVER MANUALES CORREGIDOS A COMPLETOS
# =========================================================
def _run_post_manual(result: PipelineResult, confirmar: bool = True) -> None:
    """
    Mueve los registros de Swift_manuales que ya están completos a Swift_completos.
    Si confirmar=True, muestra un resumen y pide confirmación antes de proceder.
    """
    LOGGER.info("── PASO 2: Post validación (manuales → completos) ──")

    if not config.SWIFT_MANUALES.exists():
        result.advertencias.append("Swift_manuales.xlsx no existe aún. Post validación omitida.")
        LOGGER.warning("Swift_manuales.xlsx no existe.")
        return

    try:
        from scripts.post_validacion_swift import (
            contar_listos_en_manuales,
            ejecutar_mover_manuales,
        )

        listos, pendientes = contar_listos_en_manuales()

        print("\n" + "─" * 50)
        print(f"  Registros en Swift_manuales:      {listos + pendientes}")
        print(f"  ✔ Listos para mover (completos):  {listos}")
        print(f"  ✗ Aún incompletos (se quedan):    {pendientes}")
        print("─" * 50)

        if listos == 0:
            print("  → No hay registros listos para mover.\n")
            result.manuales_pendientes = pendientes
            return

        if confirmar:
            respuesta = input("  ¿Confirmar movimiento? (s/n): ").strip().lower()
            if respuesta not in ("s", "si", "sí", "y", "yes"):
                print("  → Operación cancelada por el usuario.\n")
                return

        movidos = ejecutar_mover_manuales()
        result.manuales_movidos    = movidos
        result.manuales_pendientes = pendientes
        LOGGER.info(f"── PASO 2 completado: {movidos} registros movidos ──")

    except Exception as e:
        LOGGER.error(f"Error en post validación: {e}", exc_info=True)
        result.errores.append(f"Post validación: {e}")
        result.exitoso = False


# =========================================================
# PASO 3 — CRUCES (FORMULARIO + LLAVE)
# =========================================================
def _run_cruces(result: PipelineResult) -> None:
    """Ejecuta el cruce de formularios y llaves contra Swift_completos."""
    LOGGER.info("── PASO 3: Cruces (formulario + llave) ──")

    try:
        validate_input_files(
            config.SWIFT_COMPLETOS,
            config.XLSB_CUENTA_COM,
            config.ORIGEN_DESTINO,
            context="cruces",
        )
    except FileNotFoundError as e:
        result.errores.append(str(e))
        result.exitoso = False
        return

    try:
        from scripts.run_formulario import run_cruce_completo
        stats = run_cruce_completo()

        result.formularios_cruzados = stats.get("formularios", 0)
        result.llaves_cruzadas      = stats.get("llaves", 0)
        LOGGER.info("── PASO 3 completado ──")

    except Exception as e:
        LOGGER.error(f"Error en cruces: {e}", exc_info=True)
        result.errores.append(f"Cruces: {e}")
        result.exitoso = False


# =========================================================
# FUNCIÓN PRINCIPAL — run_pipeline (llamable desde GUI)
# =========================================================
def run_pipeline(
    modo:      str  = "completo",
    forzar:    bool = False,
    confirmar: bool = True,
) -> PipelineResult:
    """
    Ejecuta el pipeline en el modo indicado.

    Parámetros:
        modo:      "completo" | "ocr" | "post" | "post_auto" | "cruces"
        forzar:    True para ignorar caché y reprocesar todos los PDFs
        confirmar: True para pedir confirmación antes de mover manuales

    Retorna:
        PipelineResult con estadísticas completas de la ejecución.
        Diseñado para ser consumido por una GUI.
    """
    result = PipelineResult(modo=modo)
    LOGGER.info(f"╔══════════════════════════════════════════╗")
    LOGGER.info(f"  INICIO PIPELINE — modo: {modo.upper()}")
    LOGGER.info(f"  Base: {config.BASE_ROOT}")
    LOGGER.info(f"╚══════════════════════════════════════════╝")

    try:
        if modo in ("completo", "ocr"):
            _run_ocr(result, forzar=forzar)

        if modo in ("completo", "post"):
            _run_post_manual(result, confirmar=confirmar)

        if modo == "post_auto":
            _run_post_manual(result, confirmar=False)

        if modo in ("completo", "cruces"):
            # Solo ejecutar cruces si Swift_completos existe y tiene datos
            if config.SWIFT_COMPLETOS.exists():
                _run_cruces(result)
            else:
                result.advertencias.append(
                    "Swift_completos.xlsx no existe aún. Cruces omitidos."
                )

    except Exception as e:
        LOGGER.error(f"Error inesperado en pipeline: {e}", exc_info=True)
        result.errores.append(f"Error inesperado: {e}")
        result.exitoso = False
    finally:
        result.fin = datetime.now()

    print(result.resumen())
    return result


# =========================================================
# ENTRADA POR LÍNEA DE COMANDOS
# =========================================================
def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pipeline Origen_Destino_DIAN",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--modo",
        choices=["completo", "ocr", "post", "post_auto", "cruces"],
        default="completo",
        help=(
            "Modo de ejecución:\n"
            "  completo  → pipeline completo (default)\n"
            "  ocr       → solo extracción de PDFs\n"
            "  post      → mover manuales corregidos a completos (con confirmación)\n"
            "  post_auto → igual que post, sin confirmación\n"
            "  cruces    → solo cruce formulario + llave"
        ),
    )
    parser.add_argument(
        "--forzar",
        action="store_true",
        help="Ignora el caché y reprocesa todos los PDFs desde cero",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    resultado = run_pipeline(
        modo=args.modo,
        forzar=args.forzar,
        confirmar=(args.modo == "post"),   # solo pide confirmación en modo "post"
    )
    sys.exit(0 if resultado.exitoso else 1)
