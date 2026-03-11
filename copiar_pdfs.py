"""
copiar_pdfs.py — Copia PDFs desde la red a carpetas locales V1 / V2

Uso:
    python copiar_pdfs.py

Lógica:
  - Lee la estructura mes/día desde DIR_SWIFT_RAIZ (red corporativa)
  - PDFs en carpetas con fecha < SWIFT_CORTE_V2  → DIR_PDFS_V1
  - PDFs en carpetas con fecha >= SWIFT_CORTE_V2 → DIR_PDFS_V2
  - Rango: desde SWIFT_FECHA_DESDE hasta fin de año
  - Si el archivo ya existe en destino, lo omite (no sobreescribe)

Estructura de carpetas esperada:
    RAIZ / "11. NOVIEMBRE" / "26 NOVIEMBRE" / *.pdf
    RAIZ / "12. DICIEMBRE" / "03 DICIEMBRE" / *.pdf
"""

from __future__ import annotations

import re
import shutil
from datetime import date
from pathlib import Path
from typing import Optional

import config



# =========================================================
# MAPA DE MESES
# =========================================================
_MESES_ES = {
    "ENERO": 1, "FEBRERO": 2, "MARZO": 3, "ABRIL": 4,
    "MAYO": 5, "JUNIO": 6, "JULIO": 7, "AGOSTO": 8,
    "SEPTIEMBRE": 9, "SETIEMBRE": 9, "OCTUBRE": 10,
    "NOVIEMBRE": 11, "DICIEMBRE": 12,
}


def _parse_mes(nombre: str) -> Optional[int]:
    """Extrae número de mes de carpetas tipo '11. NOVIEMBRE'."""
    nombre = nombre.strip().upper()
    for mes_nombre, mes_num in _MESES_ES.items():
        if mes_nombre in nombre:
            return mes_num
    return None


def _parse_dia(nombre: str, anio: int) -> Optional[date]:
    """
    Parsea fecha de carpetas tipo '26 NOVIEMBRE' o '06 NOVIEMBRE'.
    El nombre del día incluye el mes escrito, extrae día y mes directamente.
    """
    nombre = nombre.strip().upper()
    m = re.match(r"^(\d{1,2})\s+([A-ZÁÉÍÓÚÑ]+)$", nombre)
    if not m:
        return None
    dia_str = m.group(1)
    mes_str = m.group(2)
    mes_num = _MESES_ES.get(mes_str)
    if not mes_num:
        return None
    try:
        return date(anio, mes_num, int(dia_str))
    except ValueError:
        return None


# =========================================================
# COPIA
# =========================================================
def copiar_pdfs() -> None:
    raiz        = config.DIR_SWIFT_RAIZ
    destino_v1  = config.DIR_PDFS_V1
    destino_v2  = config.DIR_PDFS_V2
    corte_v2    = config.SWIFT_CORTE_V2
    fecha_desde = config.SWIFT_FECHA_DESDE
    anio        = config.SWIFT_AÑO

    if not raiz.exists():
        print(f"[ERROR] No se puede acceder a la ruta de red: {raiz}")
        return

    destino_v1.mkdir(parents=True, exist_ok=True)
    destino_v2.mkdir(parents=True, exist_ok=True)

    print(f"Raíz red  : {raiz}")
    print(f"Destino V1: {destino_v1}")
    print(f"Destino V2: {destino_v2}")
    print(f"Corte V2  : {corte_v2}  (>= este día → V2)")
    print(f"Desde     : {fecha_desde}")
    print("=" * 60)

    copiados_v1 = 0
    copiados_v2 = 0
    omitidos    = 0
    sin_parsear = []

    for carpeta_mes in sorted(raiz.iterdir()):
        if not carpeta_mes.is_dir():
            continue
        mes_num = _parse_mes(carpeta_mes.name)
        if mes_num is None:
            continue

        for carpeta_dia in sorted(carpeta_mes.iterdir()):
            if not carpeta_dia.is_dir():
                continue

            fecha = _parse_dia(carpeta_dia.name, anio)

            if fecha is None:
                sin_parsear.append(f"{carpeta_mes.name}/{carpeta_dia.name}")
                print(f"  [SIN PARSEAR] {carpeta_mes.name} / {carpeta_dia.name}")
                continue

            # Ignorar fechas anteriores a fecha_desde
            if fecha < fecha_desde:
                continue

            # Determinar versión por corte
            if fecha >= corte_v2:
                destino = destino_v2
                version = "V2"
            else:
                destino = destino_v1
                version = "V1"

            pdfs = sorted(carpeta_dia.glob("*.pdf"))
            if not pdfs:
                print(f"  [VACÍO]    {version} | {fecha} | {carpeta_dia.name}")
                continue

            for pdf in pdfs:
                archivo_destino = destino / pdf.name

                if archivo_destino.exists():
                    print(f"  [OMITIDO]  {version} | {fecha} | {pdf.name}")
                    omitidos += 1
                    continue

                shutil.copy2(pdf, archivo_destino)
                print(f"  [COPIADO]  {version} | {fecha} | {pdf.name}")

                if version == "V1":
                    copiados_v1 += 1
                else:
                    copiados_v2 += 1

    # ── Resumen ───────────────────────────────────────────────
    print("\n" + "=" * 60)
    print(f"  Copiados V1 : {copiados_v1}")
    print(f"  Copiados V2 : {copiados_v2}")
    print(f"  Omitidos    : {omitidos}  (ya existían en destino)")
    if sin_parsear:
        print(f"  Sin parsear : {len(sin_parsear)} carpetas")
        for sp in sin_parsear:
            print(f"    → {sp}")
    print("=" * 60)


if __name__ == "__main__":
    copiar_pdfs()