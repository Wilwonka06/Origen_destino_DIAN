# -*- coding: utf-8 -*-
"""
config.py — Configuración centralizada del proyecto Origen_Destino_DIAN

Soporta dos tipos de operación:
  - "imp"  Importaciones (COMODIN/CONTROL DE PAGOS)
  - "exp"  Exportaciones (EXPORTACIONES/SWIFT 2019-2026/COMODIN)
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

# =========================================================
# BASE ROOT
# =========================================================
def _resolve_base_root() -> Path:
    env_root = os.environ.get("ORIGEN_DESTINO_ROOT")
    if env_root:
        p = Path(env_root)
        if p.exists():
            return p.resolve()
        raise FileNotFoundError(
            f"ORIGEN_DESTINO_ROOT apunta a una ruta que no existe: {env_root}"
        )
    return Path(__file__).resolve().parent


BASE_ROOT: Path = _resolve_base_root()

# =========================================================
# CARPETAS PRINCIPALES
# =========================================================
DIR_RESULTADOS: Path = BASE_ROOT / "resultados"
DIR_DBS:        Path = BASE_ROOT / "Dbs"
DIR_LOGS:       Path = BASE_ROOT / "logs"
DIR_PLANTILLAS: Path = BASE_ROOT / "plantillas"

# =========================================================
# AÑO Y CORTE V1/V2 (compartido IMP y EXP)
# =========================================================
SWIFT_AÑO:      int  = 2025
SWIFT_CORTE_V2: date = date(SWIFT_AÑO, 11, 26)   # 26 noviembre → empieza V2

# =========================================================
# IMP — Importaciones
# =========================================================
DIR_SWIFT_RAIZ_IMP: Path = (
    Path(r"O:\Comercio Exterior\Importaciones\CONTROL DE PAGOS")
    / f"CONTROL DE PAGOS {SWIFT_AÑO}"
    / "SWIFT"
    / "COMODIN"
)
SWIFT_FECHA_DESDE_IMP: date = date(2025, 4, 1)

# Fallback local (sin red)
DIR_PDFS_V1_IMP: Path = BASE_ROOT / "pdfs V1"
DIR_PDFS_V2_IMP: Path = BASE_ROOT / "pdfs V2"

# Salidas IMP
SWIFT_COMPLETOS_IMP: Path = DIR_RESULTADOS / "Swift_completos.xlsx"
SWIFT_MANUALES_IMP:  Path = DIR_RESULTADOS / "Swift_manuales.xlsx"
CACHE_FILE_IMP:      Path = DIR_RESULTADOS / ".procesados_cache.json"

# Plantilla Bancolombia IMP (V1 + V2 unificados)
PLANTILLA_IMP: Path = DIR_PLANTILLAS / "Plantilla_imp.xlsx"

# ── Alias legacy (compatibilidad con scripts que importan el nombre corto) ──
DIR_SWIFT_RAIZ    = DIR_SWIFT_RAIZ_IMP
SWIFT_FECHA_DESDE = SWIFT_FECHA_DESDE_IMP
DIR_PDFS_V1       = DIR_PDFS_V1_IMP
DIR_PDFS_V2       = DIR_PDFS_V2_IMP
SWIFT_COMPLETOS   = SWIFT_COMPLETOS_IMP
SWIFT_MANUALES    = SWIFT_MANUALES_IMP
ACUMULADO_SWIFT   = DIR_RESULTADOS / "Acumulado_swift.xlsx"
CACHE_FILE        = CACHE_FILE_IMP
PLANTILLA         = PLANTILLA_IMP  # alias legacy

# =========================================================
# EXP — Exportaciones
# =========================================================
DIR_SWIFT_RAIZ_EXP: Path = (
    Path(r"O:\Finanzas\Info Bancos\Pagos Internacionales\VARIOS - CUENTAS DE COMPENSACION")
    / "EXPORTACIONES"
    / "SWIFT 2019-2026"
    / "COMODIN"
    / str(SWIFT_AÑO) #cambiar por SWIFT_AÑO
)
SWIFT_FECHA_DESDE_EXP: date = date(2025, 4, 1)

# Fallback local (sin red)
DIR_PDFS_V1_EXP: Path = BASE_ROOT / "pdfs exp V1"
DIR_PDFS_V2_EXP: Path = BASE_ROOT / "pdfs exp V2"

# Salidas EXP
SWIFT_COMPLETOS_EXP: Path = DIR_RESULTADOS / "Swift_completos_exp.xlsx"
SWIFT_MANUALES_EXP:  Path = DIR_RESULTADOS / "Swift_manuales_exp.xlsx"
ACUMULADO_SWIFT_EXP: Path = DIR_RESULTADOS / "Acumulado_swift_exp.xlsx"
CACHE_FILE_EXP:      Path = DIR_RESULTADOS / ".procesados_cache_exp.json"

# Plantilla Bancolombia EXP (V1 + V2 unificados)
PLANTILLA_EXP: Path = DIR_PLANTILLAS / "Plantilla_exp.xlsx"

# =========================================================
# EXP — Servicios
# =========================================================
DIR_SWIFT_RAIZ_EXP: Path = (
    Path(r"O:\Finanzas\Info Bancos\Pagos Internacionales\VARIOS - CUENTAS DE COMPENSACION")
    / "EXPORTACIONES"
    / "SWIFT 2019-2026"
    / "COMODIN"
    / str(SWIFT_AÑO) #cambiar por SWIFT_AÑO
)
SWIFT_FECHA_DESDE_GTO: date = date(2025, 4, 1)

# Fallback local (sin red)
DIR_PDFS_V1_GTO: Path = BASE_ROOT / "pdfs Gto V1"
DIR_PDFS_V2_GTO: Path = BASE_ROOT / "pdfs Gto V2"

# Salidas GTO
SWIFT_COMPLETOS_GTO: Path = DIR_RESULTADOS / "Swift_completos_gto.xlsx"
SWIFT_MANUALES_GTO:  Path = DIR_RESULTADOS / "Swift_manuales_gto.xlsx"
ACUMULADO_SWIFT_GTO: Path = DIR_RESULTADOS / "Acumulado_swift_gto.xlsx"
CACHE_FILE_GTO:      Path = DIR_RESULTADOS / ".procesados_cache_gto.json"

# Plantilla Bancolombia GTO (V1 + V2 unificados)
PLANTILLA_GTO: Path = DIR_PLANTILLAS / "Plantilla_gto.xlsx"

# =========================================================
# ARCHIVOS DE ENTRADA (Bases de datos) — compartidos
# =========================================================
BD_PROVEEDORES:  Path = DIR_DBS / "Bd Proveedores.xlsx"
BD_SWIFT:        Path = DIR_DBS / "Bd Swift.xlsx"
XLSB_CUENTA_COM: Path = DIR_DBS / "Cuenta compensacion.xlsb"
ORIGEN_DESTINO:  Path = DIR_DBS / "origenDestino.xlsx"

# =========================================================
# NOMBRES DE HOJAS EXCEL
# =========================================================
SHEET_V1           = "V1"
SHEET_V2           = "V2"
SHEET_ACUMULADO    = "Acumulado"
SHEET_COM          = "COM"
SHEET_OD_DATOS     = "Datos Origen Destino"
SHEET_OD_ORIGEN    = "Origen y destino"

# =========================================================
# COLUMNAS DE BASES DE DATOS
# =========================================================
BD_PROV_COL_NOMBRE  = "DB Nombre o razon social del beneficiario"

BD_SWIFT_COL_CODIGO = "CODIGO DE LOS SWIFT"
BD_SWIFT_COL_PAIS   = "PAIS"
BD_SWIFT_COL_CIUDAD = "CIUDAD"

OD_COL_NOMBRE       = "Nombre personalizado"
OD_COL_LLAVE        = "Llave carga masiva"
OD2_COL_CONSECUTIVO = "Consecutivo"
OD2_COL_LLAVE_OD    = "Llave Origen Destino"

# =========================================================
# PARÁMETROS DE EXTRACCIÓN / OCR
# =========================================================
OCR_LANG   = "eng"
OCR_CONFIG = r"--oem 3 --psm 6"
OCR_DPI    = 300
OCR_MIN_NATIVE_CHARS: int = 99999   # fuerza siempre OCR

# =========================================================
# PARÁMETROS DE MATCHING Y VALIDACIÓN
# =========================================================
FUZZY_THRESHOLD   = 85
TOKEN_MIN_RATIO   = 0.60
TOKEN_MIN_OVERLAP = 2

# =========================================================
# PARÁMETROS FINANCIEROS
# =========================================================
CUENTA_COMPENSACION = "2190709002"
AMOUNT_TOL          = 0.01
FECHA_MIN_XLSB      = "2025-04-01"

# =========================================================
# COLUMNAS FINALES (orden canónico de exportación)
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
# DEBUG
# =========================================================
DEBUG = True