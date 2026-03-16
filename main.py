# -*- coding: utf-8 -*-
"""
main.py — Orquestador del pipeline Origen_Destino_DIAN

MODOS DE EJECUCIÓN:
  python main.py                              → pipeline completo IMP
  python main.py --tipo exp                   → pipeline completo EXP
  python main.py --tipo gto                   → pipeline completo GTO (desde Facturas.xlsx)
  python main.py --modo cruces --tipo imp     → solo cruces IMP
  python main.py --modo ocr    --tipo exp     → solo OCR EXP
  python main.py --modo ocr    --tipo gto     → solo lectura correos GTO
  python main.py --forzar                     → ignora caché
  python gui_launcher.py                      → interfaz gráfica
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
        tipo_labels = {"imp": "IMPORTACIONES", "exp": "EXPORTACIONES", "gto": "GASTOS"}
        tipo_label = tipo_labels.get(self.tipo, self.tipo.upper())
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
def _run_correos_gto(result: PipelineResult) -> None:
    """Paso 1 GTO: extrae Facturas.xlsx → Swift_manuales_gto.xlsx"""
    LOGGER.info("── PASO 1: Lectura correos GTO ──")
    try:
        from scripts.reader_correos_gto import run_lector_correos_gto
        stats = run_lector_correos_gto()
        LOGGER.info(
            f"── PASO 1 completado [GTO] ── "
            f"Total={stats['total']} | Completos={stats['completos']} | "
            f"Incompletos={stats['incompletos']}"
        )
        if stats["incompletos"] > 0:
            result.advertencias.append(
                f"GTO: {stats['incompletos']} registro(s) incompletos en Swift_manuales_gto.xlsx. "
                "Revisá y completá antes de continuar."
            )
    except FileNotFoundError as e:
        result.errores.append(str(e))
        result.exitoso = False
        LOGGER.error(f"PASO 1 GTO: {e}")
    except Exception as e:
        result.errores.append(f"PASO 1 GTO inesperado: {e}")
        result.exitoso = False
        LOGGER.error(f"PASO 1 GTO: {e}", exc_info=True)


# =========================================================
# PASO 2 — MOVER MANUALES
# =========================================================
def _run_post_manual(
    result: PipelineResult,
    confirmar: bool = True,
    tipo: str = "imp",
) -> None:
    LOGGER.info(f"── PASO 2: Post validación [{tipo.upper()}] ──")

    # GTO: la plantilla se genera directo desde Swift_completos.
    # Swift_manuales solo existe si hubo registros incompletos.
    if tipo == "gto":
        try:
            from scripts.post_validacion_swift import run_post_validacion
            stats = run_post_validacion(tipo="gto")
            result.manuales_movidos = stats.get("movidos", 0)
            LOGGER.info(f"── PASO 2 completado [GTO] ── movidos={result.manuales_movidos}")
        except Exception as e:
            LOGGER.error(f"Error en post validación GTO: {e}", exc_info=True)
            result.errores.append(f"Post validación GTO: {e}")
            result.exitoso = False
        return

    # IMP / EXP: flujo original con Swift_manuales
    if tipo == "imp":
        swift_manuales = config.SWIFT_MANUALES_IMP
    else:
        swift_manuales = config.SWIFT_MANUALES_EXP

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
# PASO 3 — CRUCES (solo IMP por ahora)
# =========================================================
def _run_cruces(result: PipelineResult, tipo: str = "imp") -> None:
    LOGGER.info(f"── PASO 3: Cruces [{tipo.upper()}] ──")

    if tipo == "imp":
        swift_completos = config.SWIFT_COMPLETOS_IMP
    elif tipo == "exp":
        swift_completos = config.SWIFT_COMPLETOS_EXP
    else:  # gto
        swift_completos = config.SWIFT_COMPLETOS_GTO

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
        LOGGER.info(f"── PASO 3 completado [{tipo.upper()}] ──")

    except Exception as e:
        LOGGER.error(f"Error en cruces [{tipo.upper()}]: {e}", exc_info=True)
        result.errores.append(f"Cruces [{tipo.upper()}]: {e}")
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
        modo      : "completo" | "ocr" | "post" | "post_auto" | "cruces"
        forzar    : ignorar caché (solo IMP/EXP con PDFs)
        confirmar : pedir confirmación antes de mover manuales
        tipo      : "imp" | "exp" | "gto"
    """
    tipo = tipo.lower().strip()
    modo = modo.lower().strip()
    # "plantilla" es alias de "post_auto" — la GUI lo envía como "plantilla"
    if modo == "plantilla":
        modo = "post_auto"
    result = PipelineResult(modo=modo, tipo=tipo)

    tipo_labels = {"imp": "IMPORTACIONES", "exp": "EXPORTACIONES", "gto": "GASTOS"}
    tipo_label = tipo_labels.get(tipo, tipo.upper())
    LOGGER.info(f"╔══════════════════════════════════════════╗")
    LOGGER.info(f"  INICIO PIPELINE — modo: {modo.upper()} | {tipo_label}")
    LOGGER.info(f"  Base: {config.BASE_ROOT}")
    LOGGER.info(f"╚══════════════════════════════════════════╝")

    try:
        # PASO 1: extracción
        if modo in ("completo", "ocr"):
            if tipo == "gto":
                # GTO no usa OCR de PDFs — lee Facturas.xlsx generado desde Outlook
                _run_correos_gto(result)
            else:
                _run_ocr(result, forzar=forzar, tipo=tipo)

        # PASO 2: post validación (mover manuales completos → completos)
        if modo in ("completo", "post"):
            _run_post_manual(result, confirmar=confirmar, tipo=tipo)

        if modo == "post_auto":
            _run_post_manual(result, confirmar=False, tipo=tipo)

        # PASO 3: cruces Formulario + Llave
        if modo in ("completo", "cruces"):
            if tipo == "imp":
                swift_completos = config.SWIFT_COMPLETOS_IMP
            elif tipo == "exp":
                swift_completos = config.SWIFT_COMPLETOS_EXP
            else:  # gto
                swift_completos = config.SWIFT_COMPLETOS_GTO

            if swift_completos.exists():
                _run_cruces(result, tipo=tipo)
            else:
                result.advertencias.append(
                    f"Swift_completos_{tipo}.xlsx no existe. Cruces omitidos."
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
        choices=["imp", "exp", "gto"],
        default="imp",
        help="Tipo: imp=Importaciones | exp=Exportaciones | gto=Gastos (default: imp)",
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