# -*- coding: utf-8 -*-
"""
core/logger.py — Logging centralizado para todo el proyecto

Uso en cualquier script:
    from core.logger import get_logger
    LOGGER = get_logger(__name__)

Características:
  - Un solo setup compartido (evita handlers duplicados en ejecuciones repetidas)
  - Salida simultánea a consola y archivo de log con rotación diaria
  - Archivo de log: BASE_ROOT/logs/pipeline_YYYY-MM-DD.log
  - Nivel configurable (INFO por defecto, DEBUG si config.DEBUG = True)
"""

from __future__ import annotations

import logging
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

# Importación diferida para evitar circularidad
_CONFIGURED = False
_ROOT_LOGGER_NAME = "origen_destino_dian"


def _setup_logging(log_dir: Path, debug: bool = False) -> None:
    """
    Configura el logger raíz del proyecto una única vez.
    Llamadas posteriores son ignoradas (idempotente).
    """
    global _CONFIGURED
    if _CONFIGURED:
        return

    level = logging.DEBUG if debug else logging.INFO
    log_format = "[%(levelname)s] %(name)s | %(message)s"
    formatter = logging.Formatter(log_format)

    root = logging.getLogger(_ROOT_LOGGER_NAME)
    root.setLevel(level)

    # Evitar duplicar handlers si por alguna razón se llama varias veces
    if root.handlers:
        root.handlers.clear()

    # — Handler de consola
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    root.addHandler(console_handler)

    # — Handler de archivo con rotación diaria
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "pipeline.log"
        file_handler = TimedRotatingFileHandler(
            filename=str(log_file),
            when="midnight",
            interval=1,
            backupCount=30,        # conserva los últimos 30 días
            encoding="utf-8",
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        file_handler.suffix = "%Y-%m-%d"
        root.addHandler(file_handler)
    except Exception as e:
        # Si no se puede escribir el log en disco, continúa solo con consola
        root.warning(f"No se pudo crear el archivo de log en {log_dir}: {e}")

    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """
    Retorna un logger hijo del logger raíz del proyecto.

    Ejemplo:
        LOGGER = get_logger(__name__)
        LOGGER.info("Iniciando procesamiento...")

    Si el sistema de logging aún no fue inicializado, lo inicializa
    automáticamente con los valores de config.py.
    """
    if not _CONFIGURED:
        # Inicialización lazy: importa config solo cuando se necesita
        try:
            import config
            _setup_logging(log_dir=config.DIR_LOGS, debug=config.DEBUG)
        except ImportError:
            # Fallback si config no está disponible (tests unitarios, etc.)
            _setup_logging(log_dir=Path("logs"), debug=False)

    return logging.getLogger(f"{_ROOT_LOGGER_NAME}.{name}")


def init_logging(log_dir: Path | None = None, debug: bool | None = None) -> None:
    """
    Inicialización explícita del logging (llamar desde main.py al inicio).
    Permite sobreescribir los valores de config.py si se necesita.

    Ejemplo en main.py:
        from core.logger import init_logging
        init_logging()
    """
    try:
        import config
        _log_dir = log_dir or config.DIR_LOGS
        _debug   = debug if debug is not None else config.DEBUG
    except ImportError:
        _log_dir = log_dir or Path("logs")
        _debug   = debug or False

    _setup_logging(log_dir=_log_dir, debug=_debug)
