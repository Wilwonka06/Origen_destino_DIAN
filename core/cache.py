# -*- coding: utf-8 -*-
"""
core/cache.py — Caché de PDFs procesados

Evita reprocesar PDFs que ya fueron extraídos exitosamente en ejecuciones anteriores.
Especialmente útil para el ciclo semanal: solo procesa los PDFs nuevos.

El caché se guarda en: resultados/.procesados_cache.json

Formato interno:
{
    "<md5_hash>": {
        "file":          "SWIFT_2025_001.pdf",
        "version":       "V1",
        "procesado_en":  "2025-04-10T14:32:00",
        "estado":        "Completo"   | "Incompleto" | "Error"
    },
    ...
}

Uso:
    from core.cache import PdfCache
    cache = PdfCache(config.CACHE_FILE)

    if cache.is_processed(pdf_path):
        continue  # saltar

    # ... procesar ...
    cache.mark(pdf_path, version="V1", estado="Completo")
    cache.save()
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from core.logger import get_logger

LOGGER = get_logger(__name__)


class PdfCache:
    """
    Registro persistente de PDFs ya procesados.

    Identificación por hash MD5 del contenido del archivo:
      - Si el PDF no cambia, su hash es el mismo → se omite en la siguiente ejecución
      - Si el PDF fue reemplazado (diferente contenido), tiene otro hash → se reprocesa
    """

    def __init__(self, cache_path: Path) -> None:
        self.cache_path = cache_path
        self._data: dict = {}
        self._load()

    # ----------------------------------------------------------
    # Carga y guardado
    # ----------------------------------------------------------
    def _load(self) -> None:
        if self.cache_path.exists():
            try:
                with open(self.cache_path, "r", encoding="utf-8") as f:
                    self._data = json.load(f)
                LOGGER.debug(f"Caché cargado: {len(self._data)} PDFs registrados ({self.cache_path.name})")
            except (json.JSONDecodeError, IOError) as e:
                LOGGER.warning(f"No se pudo leer el caché, se inicia vacío: {e}")
                self._data = {}
        else:
            LOGGER.debug("Caché no existe aún, se creará al primer guardado.")
            self._data = {}

    def save(self) -> None:
        """Persiste el caché en disco."""
        try:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.cache_path, "w", encoding="utf-8") as f:
                json.dump(self._data, f, ensure_ascii=False, indent=2)
            LOGGER.debug(f"Caché guardado: {len(self._data)} entradas en {self.cache_path.name}")
        except IOError as e:
            LOGGER.warning(f"No se pudo guardar el caché: {e}")

    # ----------------------------------------------------------
    # Operaciones principales
    # ----------------------------------------------------------
    @staticmethod
    def _hash(pdf_path: Path) -> str:
        """Calcula el hash MD5 del contenido del PDF."""
        md5 = hashlib.md5()
        with open(pdf_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                md5.update(chunk)
        return md5.hexdigest()

    def is_processed(self, pdf_path: Path) -> bool:
        """
        Retorna True si el PDF ya fue procesado exitosamente en una ejecución anterior.
        PDFs con estado "Error" NO se consideran procesados (se reintentarán).
        """
        try:
            h = self._hash(pdf_path)
            entry = self._data.get(h)
            if entry and entry.get("estado") not in ("Error", None):
                return True
            return False
        except IOError:
            return False

    def mark(
        self,
        pdf_path: Path,
        version:  str,
        estado:   str = "Completo",
    ) -> None:
        """
        Registra un PDF como procesado.

        estado: "Completo" | "Incompleto" | "Error"
        """
        try:
            h = self._hash(pdf_path)
            self._data[h] = {
                "file":         pdf_path.name,
                "version":      version,
                "procesado_en": datetime.now().isoformat(timespec="seconds"),
                "estado":       estado,
            }
        except IOError as e:
            LOGGER.warning(f"No se pudo registrar en caché {pdf_path.name}: {e}")

    def remove(self, pdf_path: Path) -> None:
        """Elimina un PDF del caché (fuerza reprocesamiento)."""
        try:
            h = self._hash(pdf_path)
            if h in self._data:
                del self._data[h]
                LOGGER.debug(f"Eliminado del caché: {pdf_path.name}")
        except IOError:
            pass

    def clear(self) -> None:
        """Vacía el caché completo (para modo --forzar)."""
        self._data = {}
        LOGGER.info("Caché vaciado completamente.")

    # ----------------------------------------------------------
    # Estadísticas
    # ----------------------------------------------------------
    def stats(self) -> dict:
        """Retorna estadísticas del caché para el reporte final."""
        total      = len(self._data)
        completos  = sum(1 for e in self._data.values() if e.get("estado") == "Completo")
        incompletos = sum(1 for e in self._data.values() if e.get("estado") == "Incompleto")
        errores    = sum(1 for e in self._data.values() if e.get("estado") == "Error")
        return {
            "total_en_cache": total,
            "completos":      completos,
            "incompletos":    incompletos,
            "errores":        errores,
        }

    def pending_files(self, folder: Path, version: str) -> list[Path]:
        """
        Retorna la lista de PDFs en 'folder' que aún NO están en el caché.
        Son los que deben procesarse en esta ejecución.
        """
        if not folder.exists():
            return []

        all_pdfs = sorted(folder.glob("*.pdf"))
        pending  = [p for p in all_pdfs if not self.is_processed(p)]

        total   = len(all_pdfs)
        skipped = total - len(pending)

        LOGGER.info(
            f"[{version}] PDFs en carpeta: {total} | "
            f"Ya procesados (caché): {skipped} | "
            f"Pendientes: {len(pending)}"
        )
        return pending
