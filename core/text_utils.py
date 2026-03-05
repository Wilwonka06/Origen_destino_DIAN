# -*- coding: utf-8 -*-
"""
core/text_utils.py — Utilidades de texto compartidas

Consolida funciones dispersas en run_pipeline.py y run_formulario.py:
  - normalize_text()          → normalización general (tildes, mayúsculas)
  - clean_amount_value()      → limpieza de montos (formato EU/US)
  - normalize_swift_11()      → normalización de códigos BIC a 11 chars
  - build_nombre_personalizado() → "Proveedor + Receiver"
  - ProveedorMatcher          → clase para fuzzy matching configurable
  - TokenMatcher              → clase para matching por tokens (cruce formulario)
"""

from __future__ import annotations

import re
import unicodedata
from typing import Optional

from core.logger import get_logger

LOGGER = get_logger(__name__)


# =========================================================
# NORMALIZACIÓN DE TEXTO GENERAL
# =========================================================

def normalize_text(s: str | None) -> str:
    """
    Normaliza un string para comparaciones:
      - Elimina tildes y caracteres diacríticos
      - Convierte a mayúsculas
      - Elimina espacios extra al inicio/fin
      - Reemplaza espacios múltiples por uno solo
      - Reemplaza \u00A0 (non-breaking space) por espacio normal
    """
    if not s:
        return ""
    s = str(s)
    s = s.replace("\u00A0", " ")
    # Descomponer caracteres Unicode y eliminar diacríticos
    nfkd = unicodedata.normalize("NFKD", s)
    ascii_str = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", ascii_str).strip().upper()


def normalize_text_key(s: str | None) -> str:
    """
    Versión para usar como clave de cruce:
    elimina puntuación adicional además de la normalización base.
    """
    base = normalize_text(s)
    # Eliminar caracteres no alfanuméricos ni espacio
    return re.sub(r"[^A-Z0-9 ]", "", base).strip()


# =========================================================
# LIMPIEZA DE MONTOS (formato EU y US)
# =========================================================

def clean_amount_value(v) -> str:
    """
    Limpia y normaliza un valor de monto a string numérico con 2 decimales.

    Maneja formatos:
      - Americano:  1,234.56  → "1234.56"
      - Europeo:    1.234,56  → "1234.56"
      - Solo entero: 1234     → "1234.00"
      - Con espacios o símbolos: "USD 1 234,56" → "1234.56"

    Retorna "" si el valor no es parseable.
    """
    if v is None:
        return ""

    import pandas as pd
    if isinstance(v, float) and pd.isna(v):
        return ""

    s = str(v).strip()
    if not s:
        return ""

    # Eliminar espacios invisibles y caracteres no numéricos (excepto . y ,)
    s = s.replace("\u00A0", "").replace("\u2007", "").replace("\u202F", "")
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[^0-9.,]", "", s)

    if not s:
        return ""

    # Detectar formato EU vs US
    dot_count   = s.count(".")
    comma_count = s.count(",")

    if dot_count == 0 and comma_count == 0:
        # Solo dígitos enteros
        normalized = s

    elif dot_count > 1 and comma_count == 0:
        # Puntos como separadores de miles: "1.234.567" → "1234567"
        normalized = s.replace(".", "")

    elif comma_count > 1 and dot_count == 0:
        # Comas como separadores de miles: "1,234,567" → "1234567"
        normalized = s.replace(",", "")

    elif dot_count == 1 and comma_count == 0:
        # Punto como decimal: "1234.56"
        normalized = s

    elif comma_count == 1 and dot_count == 0:
        # Coma como decimal: "1234,56" → "1234.56"
        normalized = s.replace(",", ".")

    elif dot_count == 1 and comma_count >= 1:
        last_dot   = s.rfind(".")
        last_comma = s.rfind(",")
        if last_comma > last_dot:
            # Formato EU: "1.234,56" → punto=miles, coma=decimal
            normalized = s.replace(".", "").replace(",", ".")
        else:
            # Formato US: "1,234.56" → coma=miles, punto=decimal
            normalized = s.replace(",", "")

    elif comma_count == 1 and dot_count >= 1:
        last_dot   = s.rfind(".")
        last_comma = s.rfind(",")
        if last_dot > last_comma:
            # Formato US: "1,234.56"
            normalized = s.replace(",", "")
        else:
            # Formato EU: "1.234,56"
            normalized = s.replace(".", "").replace(",", ".")
    else:
        # Caso ambiguo: tomar el último separador como decimal
        last_dot   = s.rfind(".")
        last_comma = s.rfind(",")
        sep_pos    = max(last_dot, last_comma)
        if sep_pos == last_dot:
            normalized = s.replace(",", "")
        else:
            normalized = s.replace(".", "").replace(",", ".")

    # Asegurar exactamente 2 decimales
    try:
        num = float(normalized)
        return f"{num:.2f}"
    except ValueError:
        LOGGER.debug(f"clean_amount_value: no se pudo parsear '{v}' → '{normalized}'")
        return ""


# =========================================================
# NORMALIZACIÓN DE CÓDIGOS SWIFT
# =========================================================

def normalize_swift_11(code: str | None) -> str:
    """
    Normaliza un código SWIFT/BIC a 11 caracteres (agrega 'XXX' si tiene 8).
    Retorna "" si el código no tiene longitud válida (8 u 11).
    """
    if not code:
        return ""
    c = str(code).strip().upper()
    if len(c) == 8:
        return c + "XXX"
    if len(c) == 11:
        return c
    return ""


# =========================================================
# NOMBRE PERSONALIZADO
# =========================================================

def build_nombre_personalizado(proveedor: str | None, receiver: str | None) -> str:
    """
    Construye el campo 'Nombre personalizado' = "Proveedor Receiver".
    Retorna "" si ambos están vacíos.
    """
    p = str(proveedor).strip() if proveedor and str(proveedor).strip().lower() not in ("nan", "none", "") else ""
    r = str(receiver).strip()  if receiver  and str(receiver).strip().lower()  not in ("nan", "none", "") else ""
    result = f"{p} {r}".strip()
    return result if result else ""


# =========================================================
# FUZZY MATCHING — clase ProveedorMatcher
# =========================================================

class ProveedorMatcher:
    """
    Encapsula el fuzzy matching de nombres de proveedores.

    Uso:
        matcher = ProveedorMatcher(nombres_bd, threshold=85)
        mejor = matcher.match("SAMSUNG ELECTRONICS CO LTD")
        # → "SAMSUNG ELECTRONICS" o None si no supera el threshold
    """

    def __init__(self, nombres_bd: list[str], threshold: int = 85) -> None:
        try:
            from thefuzz import fuzz
            self._fuzz = fuzz
        except ImportError:
            raise ImportError(
                "Se requiere 'thefuzz' para fuzzy matching.\n"
                "Instalar con: pip install thefuzz python-Levenshtein"
            )
        self.nombres_bd  = [str(n).strip() for n in nombres_bd if n and str(n).strip()]
        self.threshold   = threshold
        self._normalized = {n: normalize_text(n) for n in self.nombres_bd}

    def match(self, query: str | None) -> Optional[str]:
        """
        Busca el mejor match en la BD de proveedores.
        Retorna el nombre original (no normalizado) o None si no supera el threshold.
        """
        if not query or not str(query).strip():
            return None

        q_norm = normalize_text(query)
        best_score = 0
        best_name  = None

        for original, norm in self._normalized.items():
            score = self._fuzz.token_set_ratio(q_norm, norm)
            if score > best_score:
                best_score = score
                best_name  = original

        if best_score >= self.threshold:
            return best_name

        return None

    def match_with_score(self, query: str | None) -> tuple[Optional[str], int]:
        """
        Igual que match() pero retorna también el score.
        Útil para debug.
        """
        if not query or not str(query).strip():
            return None, 0

        q_norm = normalize_text(query)
        best_score = 0
        best_name  = None

        for original, norm in self._normalized.items():
            score = self._fuzz.token_set_ratio(q_norm, norm)
            if score > best_score:
                best_score = score
                best_name  = original

        return (best_name if best_score >= self.threshold else None), best_score


# =========================================================
# TOKEN MATCHING — clase TokenMatcher (para cruce formulario)
# =========================================================

class TokenMatcher:
    """
    Matching por tokens para el cruce de formularios (COM → Swift).

    Extraído de run_formulario.py para ser reutilizable y testeable.

    Lógica:
      - Exige coincidencia de las primeras N palabras
      - Exige ratio de tokens coincidentes >= min_ratio
      - Exige mínimo min_overlap tokens en común
    """

    def __init__(
        self,
        min_ratio:   float = 0.60,
        min_overlap: int   = 2,
        first_words: int   = 2,
    ) -> None:
        self.min_ratio   = min_ratio
        self.min_overlap = min_overlap
        self.first_words = first_words

    def matches(self, source: str | None, target: str | None) -> bool:
        """
        Retorna True si 'source' hace match con 'target' según las reglas de tokens.

        source = Nombre archivo Swift (limpio, sin códigos iniciales ni .pdf)
        target = DETALLE_COM (limpio desde #)
        """
        if not source or not target:
            return False

        src_tokens = normalize_text(source).split()
        tgt_tokens = normalize_text(target).split()

        if not src_tokens or not tgt_tokens:
            return False

        # Regla 1: primeras N palabras deben coincidir
        n = min(self.first_words, len(src_tokens), len(tgt_tokens))
        if src_tokens[:n] != tgt_tokens[:n]:
            return False

        # Regla 2: ratio de tokens del source presentes en target
        src_set = set(src_tokens)
        tgt_set = set(tgt_tokens)
        overlap = len(src_set & tgt_set)

        if overlap < self.min_overlap:
            return False

        ratio = overlap / len(src_set) if src_set else 0
        return ratio >= self.min_ratio

    def score(self, source: str | None, target: str | None) -> float:
        """Retorna el ratio de coincidencia (0.0 a 1.0) sin aplicar threshold."""
        if not source or not target:
            return 0.0
        src_set = set(normalize_text(source).split())
        tgt_set = set(normalize_text(target).split())
        if not src_set:
            return 0.0
        overlap = len(src_set & tgt_set)
        return overlap / len(src_set)
