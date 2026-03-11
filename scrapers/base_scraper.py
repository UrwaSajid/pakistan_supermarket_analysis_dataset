"""
BaseScraper
===========
Abstract base class inherited by every store scraper.

Provides:
- Session management with automatic User-Agent rotation
- Exponential-backoff retry logic
- Token-bucket rate limiting
- Structured logging (per-store log file)
- API-first → requests/BS4 → Playwright fallback strategy
- Pagination helpers
- Raw data persistence (Parquet / CSV)
"""

from __future__ import annotations

import abc
import json
import random
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config.settings import (
    BACKOFF_BASE,
    BACKOFF_MAX,
    BASE_HEADERS,
    COMPRESSION,
    MAX_RETRIES,
    RAW_DIR,
    RATE_LIMIT_CALLS,
    RATE_LIMIT_PERIOD,
    REQUEST_TIMEOUT,
    SAVE_FORMAT,
    USER_AGENTS,
)
from utils.logger import get_logger
from utils.rate_limiter import RateLimiter
from utils.helpers import random_delay, clean_text


class BaseScraper(abc.ABC):
    """
    Abstract base for all store scrapers.

    Subclasses must implement:
    - ``store_name``   (class attribute / property)
    - ``base_url``     (class attribute / property)
    - ``scrape()``     (full scrape returning list[dict])
    """

    store_name: str = "base"
    base_url:   str = ""

    # ── Initialisation ────────────────────────────────────────────────────────

    def __init__(
        self,
        city: str = "karachi",
        max_workers: int = 4,
        rate_calls: int  = RATE_LIMIT_CALLS,
        rate_period: float = RATE_LIMIT_PERIOD,
    ) -> None:
        self.city        = city
        self.max_workers = max_workers
        self.logger      = get_logger(f"{self.store_name}.{city}")
        self.rate_limiter = RateLimiter(calls=rate_calls, period=rate_period)
        self._session: Optional[requests.Session] = None
        self._playwright_available = self._check_playwright()
        self.scraped_at = datetime.now(timezone.utc).isoformat()

        self.logger.info(
            "Initialised scraper | store=%s | city=%s | playwright=%s",
            self.store_name, city, self._playwright_available,
        )

    # ── Session management ────────────────────────────────────────────────────

    def _build_session(self) -> requests.Session:
        """Build a requests Session with retry adapter."""
        session = requests.Session()
        # urllib3 retry on connection/read errors (not HTTP status retries –
        # we handle those ourselves for backoff control)
        retry = Retry(
            total=3,
            connect=3,
            read=3,
            backoff_factor=0.5,
            status_forcelist=[],   # we do status-based retry manually
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://",  adapter)
        session.headers.update(BASE_HEADERS)
        session.headers["User-Agent"] = random.choice(USER_AGENTS)
        return session

    @property
    def session(self) -> requests.Session:
        if self._session is None:
            self._session = self._build_session()
        return self._session

    def rotate_ua(self) -> None:
        """Rotate User-Agent on the active session."""
        self.session.headers["User-Agent"] = random.choice(USER_AGENTS)

    def close(self) -> None:
        """Release the HTTP session."""
        if self._session:
            self._session.close()
            self._session = None

    # ── Retry-aware HTTP GET ──────────────────────────────────────────────────

    def get(
        self,
        url: str,
        params: dict | None = None,
        headers: dict | None = None,
        json_response: bool = False,
        timeout: int = REQUEST_TIMEOUT,
    ) -> requests.Response | dict | None:
        """
        Rate-limited GET with exponential backoff on 429 / 5xx.

        Returns
        -------
        - ``dict``  if ``json_response=True`` and parse succeeds
        - ``requests.Response`` otherwise
        - ``None`` if all retries exhausted
        """
        extra_headers = headers or {}
        attempt = 0

        while attempt <= MAX_RETRIES:
            try:
                with self.rate_limiter:
                    self.rotate_ua()
                    resp = self.session.get(
                        url,
                        params=params,
                        headers=extra_headers,
                        timeout=timeout,
                    )

                if resp.status_code == 200:
                    self.logger.debug("GET %s → 200", url)
                    if json_response:
                        try:
                            return resp.json()
                        except json.JSONDecodeError:
                            self.logger.warning("JSON decode failed for %s", url)
                            return None
                    return resp

                if resp.status_code == 429:
                    wait = self._backoff(attempt)
                    self.logger.warning("429 rate-limited %s | waiting %.1f s", url, wait)
                    time.sleep(wait)

                elif resp.status_code in (503, 502, 500):
                    wait = self._backoff(attempt)
                    self.logger.warning("HTTP %d for %s | retry in %.1f s", resp.status_code, url, wait)
                    time.sleep(wait)

                elif resp.status_code == 404:
                    self.logger.debug("404 – skipping %s", url)
                    return None

                else:
                    self.logger.warning("HTTP %d for %s", resp.status_code, url)
                    return None

            except requests.exceptions.ConnectionError as exc:
                wait = self._backoff(attempt)
                self.logger.warning("ConnectionError %s | retry in %.1f s | %s", url, wait, exc)
                time.sleep(wait)

            except requests.exceptions.Timeout:
                wait = self._backoff(attempt)
                self.logger.warning("Timeout %s | retry in %.1f s", url, wait)
                time.sleep(wait)

            except Exception:
                self.logger.error("Unexpected error for %s:\n%s", url, traceback.format_exc())
                return None

            attempt += 1

        self.logger.error("All %d retries exhausted for %s", MAX_RETRIES, url)
        return None

    # ── JSON / API convenience ────────────────────────────────────────────────

    def get_json(self, url: str, params: dict | None = None, headers: dict | None = None) -> dict | list | None:
        """Shorthand for ``get(..., json_response=True)``."""
        return self.get(url, params=params, headers=headers, json_response=True)

    def post_json(
        self,
        url: str,
        payload: dict | None = None,
        headers: dict | None = None,
        timeout: int = REQUEST_TIMEOUT,
    ) -> dict | list | None:
        """POST JSON payload with rate limiting and retry."""
        extra_headers = {"Content-Type": "application/json", **(headers or {})}
        attempt = 0

        while attempt <= MAX_RETRIES:
            try:
                with self.rate_limiter:
                    self.rotate_ua()
                    resp = self.session.post(
                        url,
                        json=payload,
                        headers=extra_headers,
                        timeout=timeout,
                    )
                if resp.status_code == 200:
                    try:
                        return resp.json()
                    except json.JSONDecodeError:
                        return None
                if resp.status_code in (429, 503, 502, 500):
                    time.sleep(self._backoff(attempt))
                else:
                    return None
            except Exception:
                self.logger.error("POST error %s:\n%s", url, traceback.format_exc())
                return None
            attempt += 1
        return None

    # ── Pagination helpers ────────────────────────────────────────────────────

    def paginate_offset(
        self,
        url: str,
        page_size: int = 24,
        offset_param: str = "offset",
        size_param: str = "limit",
        extra_params: dict | None = None,
        max_pages: int = 2000,
    ) -> Generator[dict | list, None, None]:
        """
        Yield JSON responses using offset-based pagination.
        Stops when the response contains fewer items than ``page_size``
        or when ``max_pages`` is reached.
        """
        params = dict(extra_params or {})
        params[size_param] = page_size
        offset = 0

        for page in range(max_pages):
            params[offset_param] = offset
            self.logger.debug("Paginating %s | offset=%d", url, offset)
            data = self.get_json(url, params=params)
            if data is None:
                self.logger.warning("No data at offset=%d – stopping pagination", offset)
                break
            yield data
            # Detect end of results
            items = data if isinstance(data, list) else self._extract_items(data)
            if len(items) < page_size:
                self.logger.info("Last page at offset=%d (got %d items)", offset, len(items))
                break
            offset += page_size
            random_delay()

    def paginate_page(
        self,
        url: str,
        page_size: int = 24,
        page_param: str = "page",
        size_param: str = "per_page",
        extra_params: dict | None = None,
        max_pages: int = 2000,
    ) -> Generator[dict | list, None, None]:
        """Yield JSON responses using 1-indexed page-based pagination."""
        params = dict(extra_params or {})
        params[size_param] = page_size

        for page_num in range(1, max_pages + 1):
            params[page_param] = page_num
            self.logger.debug("Paginating %s | page=%d", url, page_num)
            data = self.get_json(url, params=params)
            if data is None:
                break
            yield data
            items = data if isinstance(data, list) else self._extract_items(data)
            if len(items) < page_size:
                break
            random_delay()

    def _extract_items(self, data: dict | list) -> list:
        """
        Try common keys to find a list of items inside a JSON response.
        Override in subclasses for store-specific response shapes.
        """
        if isinstance(data, list):
            return data
        for key in ("data", "items", "products", "results", "hits", "docs"):
            if isinstance(data.get(key), list):
                return data[key]
        return []

    # ── Playwright fallback ───────────────────────────────────────────────────

    @staticmethod
    def _check_playwright() -> bool:
        try:
            import playwright  # noqa: F401
            return True
        except ImportError:
            return False

    def get_with_playwright(self, url: str, wait_selector: str = "body") -> str | None:
        """
        Render a page with Playwright (Chromium) and return the HTML source.
        Used as last-resort fallback when requests/BS4 fails.
        """
        if not self._playwright_available:
            self.logger.warning("Playwright not installed – cannot render %s", url)
            return None
        try:
            from playwright.sync_api import sync_playwright

            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                ctx     = browser.new_context(
                    user_agent=random.choice(USER_AGENTS),
                    viewport={"width": 1280, "height": 800},
                )
                page = ctx.new_page()
                page.goto(url, timeout=60_000)
                try:
                    page.wait_for_selector(wait_selector, timeout=15_000)
                except Exception:
                    pass
                html = page.content()
                browser.close()
            return html
        except Exception:
            self.logger.error("Playwright scrape failed for %s:\n%s", url, traceback.format_exc())
            return None

    # ── Abstract interface ────────────────────────────────────────────────────

    @abc.abstractmethod
    def scrape(self) -> list[dict[str, Any]]:
        """
        Run the full scrape for this store + city.
        Must return a list of raw product dicts.
        """
        ...

    def scrape_categories(self) -> list[str]:
        """
        Return a list of category URLs/identifiers to iterate over.
        Default: empty list (subclasses override if needed).
        """
        return []

    # ── Data saving ───────────────────────────────────────────────────────────

    def save_raw(self, records: list[dict[str, Any]]) -> Path:
        """
        Persist raw scraped records to the raw data layer.

        File naming: ``<store>_<city>_<timestamp>.<format>``
        """
        if not records:
            self.logger.warning("No records to save for %s/%s", self.store_name, self.city)
            return Path()

        df = pd.DataFrame(records)
        # Attach metadata columns
        df["_store"]      = self.store_name
        df["_city"]       = self.city
        df["_scraped_at"] = self.scraped_at

        ts       = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"{self.store_name}_{self.city}_{ts}"

        # Always save Parquet (fast, compressed) AND CSV (human-readable)
        parquet_path = RAW_DIR / f"{filename}.parquet"
        csv_path     = RAW_DIR / f"{filename}.csv"

        df.to_parquet(parquet_path, compression=COMPRESSION, index=False)
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")

        self.logger.info(
            "Saved %d raw records → %s  (+.csv)", len(df), parquet_path
        )
        return parquet_path

    # ── Utilities ─────────────────────────────────────────────────────────────

    @staticmethod
    def _backoff(attempt: int) -> float:
        """Compute capped exponential back-off delay."""
        return min(BACKOFF_BASE * (2 ** attempt) + random.uniform(0, 1), BACKOFF_MAX)

    def build_product_record(self, **kwargs) -> dict[str, Any]:
        """
        Construct a canonical product record dict.
        All fields default to None if not provided.
        """
        defaults = {
            "store":          self.store_name,
            "city":           self.city,
            "product_id":     None,
            "name":           None,
            "brand":          None,
            "category":       None,
            "subcategory":    None,
            "price":          None,
            "sale_price":     None,
            "currency":       "PKR",
            "quantity":       None,
            "unit":           None,
            "price_per_unit": None,
            "in_stock":       None,
            "image_url":      None,
            "product_url":    None,
            "barcode":        None,
            "description":    None,
            "scraped_at":     self.scraped_at,
        }
        defaults.update({k: clean_text(v) if isinstance(v, str) else v for k, v in kwargs.items()})
        return defaults

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} store={self.store_name!r} city={self.city!r}>"
