# -*- coding: utf-8 -*-
"""
core/ocr_engine.py — Motor OCR centralizado para el proyecto

Reemplaza la función _resolve_tesseract_cmd() duplicada en reader_pdf_V1.py
y reader_pdf_V2.py, y centraliza toda la configuración de Tesseract y pdfplumber.

Uso en cualquier script:
    from core.ocr_engine import get_ocr_engine
    ocr = get_ocr_engine()
    texto = ocr.extract_text_from_pdf(path_pdf, debug=False)
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Optional

import pdfplumber
import pytesseract
from PIL import Image

from core.logger import get_logger

LOGGER = get_logger(__name__)


# =========================================================
# RESOLUCIÓN DE TESSERACT (única implementación del proyecto)
# =========================================================
def _resolve_tesseract_cmd() -> str:
    """
    Busca el ejecutable tesseract.exe en orden de prioridad:
      1. Variable de entorno TESSERACT_CMD
      2. PATH del sistema (shutil.which)
      3. Rutas de instalación estándar en Windows
    """
    # 1. Variable de entorno
    env_cmd = os.environ.get("TESSERACT_CMD")
    if env_cmd and Path(env_cmd).exists():
        return env_cmd

    # 2. PATH del sistema
    which = shutil.which("tesseract")
    if which:
        return which

    # 3. Rutas estándar de Windows
    candidates = [
        r"C:\Program Files\Tesseract-OCR\tesseract.exe",
        r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
        os.path.join(
            os.environ.get("USERPROFILE", ""),
            r"AppData\Local\Programs\Tesseract-OCR\tesseract.exe"
        ),
        os.path.join(
            os.environ.get("LOCALAPPDATA", ""),
            r"Programs\Tesseract-OCR\tesseract.exe"
        ),
    ]
    for c in candidates:
        if c and Path(c).exists():
            return c

    raise FileNotFoundError(
        "No se encontró tesseract.exe.\n"
        "Opciones:\n"
        "  1. Instalar Tesseract OCR desde: https://github.com/UB-Mannheim/tesseract/wiki\n"
        "  2. Definir la variable de entorno TESSERACT_CMD con la ruta completa al ejecutable."
    )


# =========================================================
# CLASE OCR ENGINE
# =========================================================
class OcrEngine:
    """
    Motor OCR reutilizable. Se instancia una sola vez (singleton).

    Encapsula:
      - Configuración de Tesseract (lang, config, DPI)
      - Extracción de texto desde PDF (pdfplumber primero, OCR como fallback)
      - Renderizado de páginas a imagen para OCR

    NOTA sobre min_native_chars:
      Controla el umbral de caracteres nativos (pdfplumber) por debajo del cual
      se activa el OCR. Todos los regex del proyecto fueron calibrados contra
      salida de Tesseract, por lo que se recomienda mantener este valor alto
      (config.OCR_MIN_NATIVE_CHARS = 99999) para forzar siempre OCR.
      Solo reducirlo si los regex se revalidan contra texto nativo de pdfplumber.
    """

    def __init__(self, lang: str, config: str, dpi: int, min_native_chars: int = 99999) -> None:
        self.lang             = lang
        self.config           = config
        self.dpi              = dpi
        self.min_native_chars = min_native_chars   # FIX: configurable desde config.py
        self._tesseract_cmd   = _resolve_tesseract_cmd()
        pytesseract.pytesseract.tesseract_cmd = self._tesseract_cmd
        LOGGER.info(f"Tesseract encontrado: {self._tesseract_cmd}")
        LOGGER.info(f"OCR engine: lang={lang} | dpi={dpi} | min_native_chars={min_native_chars}")

    # ----------------------------------------------------------
    # Extracción principal: texto nativo → OCR como fallback
    # ----------------------------------------------------------
    def extract_text_from_pdf(
        self,
        pdf_path: Path,
        debug: bool = False,
    ) -> list[str]:
        """
        Extrae texto de cada página de un PDF.
        Retorna lista de strings, uno por página.

        Estrategia por página:
          1. pdfplumber (texto nativo) → si supera min_native_chars, lo usa
          2. OCR con Tesseract (fallback para PDFs escaneados o cuando
             min_native_chars es muy alto, que es el caso por defecto)
        """
        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF no encontrado: {pdf_path}")

        page_texts: list[str] = []

        try:
            with pdfplumber.open(str(pdf_path)) as pdf:
                for i, page in enumerate(pdf.pages):
                    text = self._extract_page_text(page, page_num=i, debug=debug)
                    page_texts.append(text or "")
        except Exception as e:
            LOGGER.error(f"Error al abrir PDF {pdf_path.name}: {e}")
            raise

        if debug:
            total_chars = sum(len(t) for t in page_texts)
            LOGGER.debug(f"{pdf_path.name}: {len(page_texts)} páginas, {total_chars} chars totales")

        return page_texts

    # ----------------------------------------------------------
    # Extracción de una sola página
    # ----------------------------------------------------------
    def _extract_page_text(
        self,
        page,
        page_num: int,
        debug: bool = False,
    ) -> Optional[str]:
        """
        Extrae texto de una página individual.

        Prioriza texto nativo solo si supera self.min_native_chars.
        Con el valor por defecto (99999), siempre usa OCR — comportamiento
        equivalente a las versiones originales de reader_pdf_V1 y reader_pdf_V2.
        """
        native_text = page.extract_text() or ""
        if len(native_text.strip()) >= self.min_native_chars:
            if debug:
                LOGGER.debug(f"  Página {page_num + 1}: texto nativo ({len(native_text)} chars)")
            return native_text

        # OCR: cubre tanto PDFs escaneados como digitales (cuando min_native_chars es alto)
        if debug:
            LOGGER.debug(
                f"  Página {page_num + 1}: texto nativo insuficiente "
                f"({len(native_text.strip())} chars < {self.min_native_chars}) → OCR"
            )

        return self._ocr_page(page, page_num, debug)

    def _ocr_page(self, page, page_num: int, debug: bool = False) -> Optional[str]:
        """Renderiza la página como imagen y aplica Tesseract OCR."""
        try:
            img: Image.Image = page.to_image(resolution=self.dpi).original
            text = pytesseract.image_to_string(img, lang=self.lang, config=self.config)
            if debug:
                LOGGER.debug(f"  Página {page_num + 1}: OCR completado ({len(text)} chars)")
            return text
        except Exception as e:
            LOGGER.warning(f"  Página {page_num + 1}: falló OCR — {e}")
            return None

    # ----------------------------------------------------------
    # Utilidad: texto completo concatenado
    # ----------------------------------------------------------
    def full_text(self, pdf_path: Path, debug: bool = False) -> str:
        """Retorna todo el texto del PDF como un único string."""
        pages = self.extract_text_from_pdf(pdf_path, debug=debug)
        return "\n".join(pages)


# =========================================================
# SINGLETON — una sola instancia por proceso
# =========================================================
_ocr_engine_instance: Optional[OcrEngine] = None


def get_ocr_engine() -> OcrEngine:
    """
    Retorna la instancia única del motor OCR.
    La crea la primera vez usando los parámetros de config.py.

    Uso:
        from core.ocr_engine import get_ocr_engine
        ocr = get_ocr_engine()
        pages = ocr.extract_text_from_pdf(Path("mi_swift.pdf"))
    """
    global _ocr_engine_instance
    if _ocr_engine_instance is None:
        try:
            import config
            _ocr_engine_instance = OcrEngine(
                lang=config.OCR_LANG,
                config=config.OCR_CONFIG,
                dpi=config.OCR_DPI,
                min_native_chars=config.OCR_MIN_NATIVE_CHARS,  # FIX: lee desde config
            )
        except ImportError:
            # Fallback con valores por defecto — fuerza OCR igual que el comportamiento original
            _ocr_engine_instance = OcrEngine(
                lang="eng",
                config=r"--oem 3 --psm 6",
                dpi=300,
                min_native_chars=99999,  # FIX: fuerza siempre OCR
            )
    return _ocr_engine_instance