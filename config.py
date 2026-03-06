# -*- coding: utf-8 -*-
"""
config.py — Configuración centralizada del proyecto Origen_Destino_DIAN

Única fuente de verdad para rutas, constantes y parámetros.
Todos los scripts deben importar desde aquí; ninguno define rutas propias.

Resolución de BASE_ROOT (en orden de prioridad):
  1. Variable de entorno: ORIGEN_DESTINO_ROOT
  2. Ruta relativa al propio config.py (portabilidad entre equipos)
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

# =========================================================
# BASE ROOT — raíz del proyecto
# =========================================================
def _resolve_base_root() -> Path:
    """
    Resuelve la raíz del proyecto de forma portable.
    Prioridad:
      1. Variable de entorno ORIGEN_DESTINO_ROOT (ideal para múltiples equipos)
      2. Directorio donde vive este config.py (raíz del proyecto)
    """
    env_root = os.environ.get("ORIGEN_DESTINO_ROOT")
    if env_root:
        p = Path(env_root)
        if p.exists():
            return p.resolve()
        raise FileNotFoundError(
            f"ORIGEN_DESTINO_ROOT apunta a una ruta que no existe: {env_root}"
        )
    # Relativo a este archivo → funciona sin importar en qué equipo/carpeta esté
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
# FUENTE DE PDFs SWIFT (red corporativa)

SWIFT_AÑO: int = 2025 #date.today().year

DIR_SWIFT_RAIZ: Path = Path(r"O:\Comercio Exterior\Importaciones\CONTROL DE PAGOS") / f"CONTROL DE PAGOS {SWIFT_AÑO}" / "SWIFT" / "COMODIN"

SWIFT_FECHA_DESDE: date = date(2025, 4, 1)        # 1 de abril 2025

SWIFT_CORTE_V2: date = date(SWIFT_AÑO, 11, 26)   # 26 de noviembre  ← FIX

# Carpetas locales opcionales: se usan si DIR_SWIFT_RAIZ no existe
# (útil para pruebas offline o ejecución sin red).
DIR_PDFS_V1: Path = BASE_ROOT / "pdfs V1"
DIR_PDFS_V2: Path = BASE_ROOT / "pdfs V2"

# =========================================================
# ARCHIVOS DE ENTRADA (Bases de datos)
# =========================================================
BD_PROVEEDORES:  Path = DIR_DBS / "Bd Proveedores.xlsx"
BD_SWIFT:        Path = DIR_DBS / "Bd Swift.xlsx"
XLSB_CUENTA_COM: Path = DIR_DBS / "Cuenta compensacion.xlsb"
ORIGEN_DESTINO:  Path = DIR_DBS / "origenDestino.xlsx"

# =========================================================
# ARCHIVOS DE SALIDA (resultados)
# =========================================================
SWIFT_COMPLETOS: Path = DIR_RESULTADOS / "Swift_completos.xlsx"
SWIFT_MANUALES:  Path = DIR_RESULTADOS / "Swift_manuales.xlsx"
ACUMULADO_SWIFT: Path = DIR_RESULTADOS / "Acumulado_swift.xlsx"
CACHE_FILE:      Path = DIR_RESULTADOS / ".procesados_cache.json"

# =========================================================
# PLANTILLAS BANCOLOMBIA
# =========================================================
PLANTILLA_V1: Path = DIR_PLANTILLAS / "Datos_Origen_Destino_V1.xlsx"
PLANTILLA_V2: Path = DIR_PLANTILLAS / "Datos_Origen_Destino_V2.xlsx"

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
OCR_MIN_NATIVE_CHARS: int = 99999  # ← FIX: fuerza siempre OCR

# =========================================================
# PARÁMETROS DE MATCHING Y VALIDACIÓN
# =========================================================
FUZZY_THRESHOLD  = 85    # umbral mínimo para match fuzzy de proveedores
TOKEN_MIN_RATIO  = 0.60  # mínimo 60% de tokens coincidentes en cruce formulario
TOKEN_MIN_OVERLAP = 2    # mínimo 2 tokens en común

# =========================================================
# PARÁMETROS FINANCIEROS
# =========================================================
CUENTA_COMPENSACION = "2190709002"
AMOUNT_TOL          = 0.01   # tolerancia para comparar montos numéricos
FECHA_MIN_XLSB      = "2025-04-01"  # filtro mínimo de fecha en XLSB

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
DEBUG = False  # True activa logs detallados de OCR y matching