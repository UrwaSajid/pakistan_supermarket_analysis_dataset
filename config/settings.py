"""
Global configuration for the supermarket scraping pipeline.
Adjust timeouts, rate limits, storage paths, and store URLs here.
"""

import os
from pathlib import Path

# ── Project root ──────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent

# ── Storage paths ─────────────────────────────────────────────────────────────
DATA_DIR       = BASE_DIR / "data"
RAW_DIR        = DATA_DIR / "raw"
PROCESSED_DIR  = DATA_DIR / "processed"
MATCHED_DIR    = DATA_DIR / "matched"
LOGS_DIR       = BASE_DIR / "logs"

for _p in [RAW_DIR, PROCESSED_DIR, MATCHED_DIR, LOGS_DIR]:
    _p.mkdir(parents=True, exist_ok=True)

# ── HTTP / Scraping defaults ──────────────────────────────────────────────────
REQUEST_TIMEOUT      = 30          # seconds per request
MAX_RETRIES          = 5
BACKOFF_BASE         = 2.0         # exponential backoff base (seconds)
BACKOFF_MAX          = 120         # cap retry delay at 2 minutes
RATE_LIMIT_CALLS     = 5           # max calls per window
RATE_LIMIT_PERIOD    = 1.0         # window in seconds
DEFAULT_PAGE_DELAY   = (1.0, 3.0)  # (min, max) random delay between pages

# ── Worker pool ───────────────────────────────────────────────────────────────
MAX_WORKERS          = 8           # concurrent worker threads per scraper
MAX_GLOBAL_WORKERS   = 16          # across all scrapers

# ── Output formats ────────────────────────────────────────────────────────────
SAVE_FORMAT          = "parquet"   # "parquet" | "csv"
COMPRESSION          = "snappy"    # parquet compression

# ── User-agent rotation pool ─────────────────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
]

# ── Common request headers ────────────────────────────────────────────────────
BASE_HEADERS = {
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
    "DNT":             "1",
}

# ── Store configurations ──────────────────────────────────────────────────────
STORES = {
    "metro": {
        "name":         "Metro Cash & Carry",
        "base_url":     "https://www.metro.com.pk",
        "api_base":     "https://www.metro.com.pk/api",
        "cities":       ["karachi", "lahore", "islamabad", "rawalpindi", "faisalabad",
                         "multan", "hyderabad", "peshawar"],
        "enabled":      True,
    },
    "imtiaz": {
        "name":         "Imtiaz Supermarket",
        "base_url":     "https://shop.imtiaz.com.pk",
        "api_base":     "https://shop.imtiaz.com.pk",
        "cities":       ["karachi", "lahore"],
        "enabled":      True,
    },
    "naheed": {
        "name":         "Naheed Supermarket",
        "base_url":     "https://www.naheed.pk",
        "api_base":     "https://www.naheed.pk",
        "cities":       ["karachi", "lahore"],
        "enabled":      True,
    },
    "alfatah": {
        "name":         "Al-Fatah",
        "base_url":     "https://alfatah.pk",
        "api_base":     "https://alfatah.pk",
        "cities":       ["lahore", "islamabad"],
        "enabled":      True,
    },
    "chaseup": {
        "name":         "Chase Up",
        "base_url":     "https://www.chaseupgrocery.com",
        "api_base":     "https://www.chaseupgrocery.com",
        "cities":       ["karachi", "lahore"],
        "enabled":      True,
    },
    "carrefour": {
        "name":         "Carrefour Pakistan",
        "base_url":     "https://www.carrefour.com.pk",
        "api_base":     "https://www.carrefour.com.pk/api",
        "cities":       ["karachi", "lahore", "islamabad"],
        "enabled":      True,
    },
    "springs": {
        "name":         "Springs",
        "base_url":     "https://springs.com.pk",
        "api_base":     "https://springs.com.pk",
        "cities":       ["lahore"],
        "enabled":      True,
    },
    "pandamart": {
        "name":         "Pandamart (Foodpanda)",
        "base_url":     "https://www.foodpanda.pk",
        "api_base":     "https://www.foodpanda.pk/api/v5",
        "cities":       ["karachi"],
        "enabled":      True,
    },
}

# ── Validation thresholds ─────────────────────────────────────────────────────
MAX_MISSING_PCT      = 0.30     # flag column if >30 % missing
PRICE_MIN_PKR        = 1.0      # below this is suspect
PRICE_MAX_PKR        = 500_000  # above this is suspect
ZSCORE_OUTLIER_THRESH = 3.0
IQR_MULTIPLIER       = 1.5

# ── Matching thresholds ───────────────────────────────────────────────────────
FUZZY_MATCH_THRESHOLD   = 85   # token_set_ratio score (0-100)
EXACT_MATCH_BONUS       = 10   # extra score for same brand + size
