"""
Shared helper utilities used across all scrapers.
"""

import random
import re
import time
from typing import Optional

from config.settings import DEFAULT_PAGE_DELAY, USER_AGENTS


# ── Timing ────────────────────────────────────────────────────────────────────

def random_delay(min_s: float | None = None, max_s: float | None = None) -> None:
    """Sleep a random amount within [min_s, max_s]."""
    lo = min_s if min_s is not None else DEFAULT_PAGE_DELAY[0]
    hi = max_s if max_s is not None else DEFAULT_PAGE_DELAY[1]
    time.sleep(random.uniform(lo, hi))


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def rotate_user_agent() -> str:
    """Return a randomly chosen User-Agent string."""
    return random.choice(USER_AGENTS)


# ── Price parsing ─────────────────────────────────────────────────────────────

_PRICE_RE = re.compile(r"[\d,]+(?:\.\d+)?")

def parse_price(raw: str | None) -> Optional[float]:
    """
    Extract the first numeric value from a raw price string.

    Examples
    --------
    >>> parse_price("Rs. 1,234.50")
    1234.5
    >>> parse_price("PKR250")
    250.0
    """
    if not raw:
        return None
    raw = raw.replace(",", "")
    m = _PRICE_RE.search(raw)
    return float(m.group()) if m else None


# ── Unit / size normalization ─────────────────────────────────────────────────

_UNIT_MAP = {
    # Volume
    "ltr":   "L",  "litre":  "L",  "liter": "L",  "litres": "L",
    "liters": "L", "l":       "L",
    "ml":    "ml", "millilitre": "ml", "milliliter": "ml",
    # Weight
    "kg":    "kg", "kilogram": "kg", "kilograms": "kg",
    "g":     "g",  "gram":    "g",  "grams":    "g",   "gm": "g",
    "mg":    "mg",
    # Count
    "pcs":   "pcs", "pieces": "pcs", "piece": "pcs", "pc": "pcs",
    "pack":  "pack", "pkt":   "pack", "packet": "pack",
    "dozen": "doz",
}

_SIZE_RE = re.compile(
    r"(\d+(?:\.\d+)?)\s*"
    r"(ltr|litre|liter|litres|liters|l|ml|millilitre|milliliter"
    r"|kg|kilogram|kilograms|g|gram|grams|gm|mg"
    r"|pcs|pieces|piece|pc|pack|pkt|packet|dozen|doz)\b",
    re.IGNORECASE,
)

def normalize_unit(text: str | None) -> tuple[Optional[float], Optional[str]]:
    """
    Extract (quantity, standard_unit) from a product name or weight string.

    Returns
    -------
    (quantity, unit) or (None, None) if not found.

    Examples
    --------
    >>> normalize_unit("Milk 1.5 Ltr")
    (1.5, 'L')
    >>> normalize_unit("Sugar 5KG")
    (5.0, 'kg')
    """
    if not text:
        return None, None
    m = _SIZE_RE.search(text)
    if not m:
        return None, None
    qty  = float(m.group(1))
    unit = _UNIT_MAP.get(m.group(2).lower(), m.group(2).lower())
    # Normalise mL → L if qty >= 1000
    if unit == "ml" and qty >= 1000:
        return qty / 1000, "L"
    return qty, unit


def compute_price_per_unit(price: Optional[float], qty: Optional[float], unit: Optional[str]) -> Optional[float]:
    """Return price per base unit (per 100 g / per 100 ml / per piece)."""
    if price is None or qty is None or qty == 0:
        return None
    if unit in ("g", "ml"):
        return round(price / qty * 100, 4)   # per 100 g or 100 ml
    if unit in ("kg", "L"):
        return round(price / qty, 4)          # per kg or per litre
    if unit in ("pcs", "pack", "doz"):
        return round(price / qty, 4)          # per piece / pack
    return round(price / qty, 4)


# ── Text cleaning ─────────────────────────────────────────────────────────────

_BRAND_NOISE = re.compile(
    r"\b(ltd|limited|pvt|pakistan|pk|co|corp|inc|llc|(pvt\.?\s*ltd\.?))\b",
    re.IGNORECASE,
)

def clean_text(text: str | None) -> str:
    """Strip extra whitespace and normalise encoding."""
    if not text:
        return ""
    return " ".join(text.split()).strip()


def slugify(text: str) -> str:
    """Lowercase, replace non-alphanumeric with underscores."""
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")
