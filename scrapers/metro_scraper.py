"""
Metro Online Pakistan – API-first Scraper
==========================================
Target  : https://www.metro-online.pk
API     : https://admin.metro-online.pk/api/read/

Discovered endpoints (no browser/Playwright needed):
  GET /Stores
      → list of all stores with id, city_name, location
  GET /Categories?&filter=storeId&filterValue={id}
      → flat list of all ~233 categories for a store
  GET /Products?type=Products_nd_associated_Brands
                &filter=||tier1Id&filterValue=||{catId}
                &filter=||tier2Id&filterValue=||{catId}
                &filter=||tier3Id&filterValue=||{catId}
                &filter=||tier4Id&filterValue=||{catId}
                &offset={offset}&limit={limit}
                &filter=active&filterValue=true
                &filter=storeId&filterValue={storeId}
                &filter=!url&filterValue=!null
                &filter=Op.available_stock&filterValue=Op.gt__0
                &order=product_scoring__DESC
      → paginated product list

Known stores (from /Stores, verified 2026-03-11):
  id=10  Lahore         Canal Store
  id=11  Islamabad      Capital Store
  id=12  Karachi        Safari Park Store
  id=15  Faisalabad     Lyalpur Store
  id=16  Lahore         DHA Store
  id=21  Multan         Multan Store
  id=22  Karachi        Manghopir Site Store
  id=23  Karachi        Star Gate Store
  id=25  Lahore         Ravi Store
  id=26  Lahore         Model Town Store
  id=28  Lahore         Chand Bhag Store

Product key fields:
  id, product_name, brand_name, price, sell_price, sale_price, mrp_price,
  weight, unit_type, product_code_app, available_stock, active, storeId,
  tier1Id, tier2Id, tier3Id, tier4Id, url (image), seo_url_slug
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from config.settings import STORES
from scrapers.base_scraper import BaseScraper
from utils.helpers import (
    normalize_unit,
    parse_price,
    compute_price_per_unit,
    clean_text,
)
from utils.logger import get_logger

# ── API base ──────────────────────────────────────────────────────────────────
_ADMIN      = "https://admin.metro-online.pk"
_FRONTEND   = "https://www.metro-online.pk"
_STORES_API = f"{_ADMIN}/api/read/Stores"
_CATS_API   = f"{_ADMIN}/api/read/Categories"
_PRODS_API  = f"{_ADMIN}/api/read/Products"

# ── Pagination ────────────────────────────────────────────────────────────────
_PAGE_LIMIT = 100   # max products per API call


class MetroScraper(BaseScraper):
    """
    Scrapes Metro Online Pakistan via admin REST API.

    For each city:
      1. Fetch /Stores → match city → get store IDs (multiple per city)
      2. For each store → fetch /Categories → flat list of ~233 categories
      3. For each category → paginate /Products until dry
      4. Deduplicate by product_id across categories in the same store
    """

    store_name = "metro"
    base_url   = _FRONTEND

    def __init__(
        self,
        city: str = "karachi",
        max_workers: int = 8,
        store_id: int | None = None,
    ) -> None:
        super().__init__(city=city, max_workers=max_workers, rate_calls=10, rate_period=1.0)
        self._forced_store_id = store_id   # allow single-store override

    # =========================================================================
    # Public entry point
    # =========================================================================

    def scrape(self) -> list[dict[str, Any]]:
        self.logger.info("[Metro/%s] ▶ Starting API scrape …", self.city)

        # Resolve stores for this city
        if self._forced_store_id:
            stores = [{"id": self._forced_store_id,
                       "city_name": self.city, "location": "forced"}]
        else:
            all_stores = self._fetch_all_stores()
            stores = self._filter_by_city(all_stores, self.city)
            if not stores:
                self.logger.warning("[Metro] No store found for city='%s'", self.city)
                return []

        self.logger.info("[Metro/%s] Stores: %s", self.city,
                         [(s["id"], s["location"]) for s in stores])

        all_records: list[dict] = []
        for store in stores:
            recs = self._scrape_one_store(store)
            all_records.extend(recs)
            self.logger.info(
                "[Metro/%s] Store %-20s → %5d records  (running total: %d)",
                self.city, store["location"], len(recs), len(all_records),
            )

        self.logger.info("[Metro/%s] ■ Done – %d total records", self.city, len(all_records))
        if all_records:
            self.save_raw(all_records)
        return all_records

    # =========================================================================
    # Store discovery
    # =========================================================================

    def _fetch_all_stores(self) -> list[dict]:
        resp = self.get(_STORES_API)
        if not resp:
            self.logger.error("[Metro] Failed to fetch /Stores")
            return []
        try:
            return resp.json().get("data", [])
        except Exception as exc:
            self.logger.error("[Metro] /Stores JSON parse error: %s", exc)
            return []

    @staticmethod
    def _filter_by_city(stores: list[dict], city: str) -> list[dict]:
        """Return all stores whose city_name contains the requested city."""
        city_l = city.lower().strip()
        # Map common aliases
        aliases = {
            "islamabad": ["islamabad", "rawalpindi", "islamabad-rawalpindi"],
            "rawalpindi": ["islamabad", "rawalpindi"],
        }
        tokens = aliases.get(city_l, [city_l]) + city_l.replace("-", " ").split()
        tokens = list(dict.fromkeys(tokens))  # deduplicate

        matched = [
            s for s in stores
            if any(t in s.get("city_name", "").lower() for t in tokens)
        ]
        return matched

    # =========================================================================
    # Per-store scrape
    # =========================================================================

    def _scrape_one_store(self, store: dict) -> list[dict]:
        store_id   = int(store["id"])
        store_name = clean_text(f"{store.get('city_name','')} {store.get('location','')}")

        categories = self._fetch_categories(store_id)
        self.logger.info("[Metro] Store %d (%s): %d categories", store_id, store_name, len(categories))

        # Scrape categories in parallel
        seen_pid: set[str] = set()
        records: list[dict] = []
        lock = __import__("threading").Lock()

        def scrape_cat(cat: dict) -> list[dict]:
            return self._scrape_category(
                cat_id    = int(cat["id"]),
                cat_name  = clean_text(cat.get("category_name", str(cat["id"]))),
                store_id  = store_id,
                store_loc = store_name,
            )

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {pool.submit(scrape_cat, cat): cat for cat in categories}
            for future in as_completed(futures):
                cat = futures[future]
                try:
                    batch = future.result()
                    added = 0
                    with lock:
                        for rec in batch:
                            pid = rec.get("product_id", "")
                            if pid and pid in seen_pid:
                                continue
                            if pid:
                                seen_pid.add(pid)
                            records.append(rec)
                            added += 1
                    self.logger.info(
                        "[Metro] %-30s → %4d products  (%d new, store=%d)",
                        cat.get("category_name", "?"), len(batch), added, store_id,
                    )
                except Exception as exc:
                    self.logger.warning("[Metro] Category %s error: %s",
                                        cat.get("category_name"), exc)

        return records

    # =========================================================================
    # Category API
    # =========================================================================

    def _fetch_categories(self, store_id: int) -> list[dict]:
        url  = f"{_CATS_API}?&filter=storeId&filterValue={store_id}"
        resp = self.get(url)
        if not resp:
            return []
        try:
            data = resp.json().get("data", [])
            return data
        except Exception:
            return []

    # =========================================================================
    # Product API – per category
    # =========================================================================

    def _scrape_category(
        self,
        cat_id: int,
        cat_name: str,
        store_id: int,
        store_loc: str = "",
    ) -> list[dict]:
        """Paginate through /Products for one category."""
        records: list[dict] = []
        offset  = 0

        while True:
            batch = self._fetch_product_page(cat_id, store_id, offset)
            if not batch:
                break
            for p in batch:
                rec = self._parse_product(p, cat_name, store_id)
                if rec:
                    records.append(rec)
            if len(batch) < _PAGE_LIMIT:
                break
            offset += _PAGE_LIMIT
            time.sleep(0.1)   # gentle pacing

        return records

    def _fetch_product_page(
        self, cat_id: int, store_id: int, offset: int
    ) -> list[dict]:
        url = (
            f"{_PRODS_API}"
            f"?type=Products_nd_associated_Brands"
            f"&order=product_scoring__DESC"
            f"&filter=||tier1Id&filterValue=||{cat_id}"
            f"&filter=||tier2Id&filterValue=||{cat_id}"
            f"&filter=||tier3Id&filterValue=||{cat_id}"
            f"&filter=||tier4Id&filterValue=||{cat_id}"
            f"&offset={offset}&limit={_PAGE_LIMIT}"
            f"&filter=active&filterValue=true"
            f"&filter=storeId&filterValue={store_id}"
            f"&filter=!url&filterValue=!null"
            f"&filter=Op.available_stock&filterValue=Op.gt__0"
        )
        resp = self.get(url)
        if not resp:
            return []
        try:
            return resp.json().get("data", [])
        except Exception:
            return []

    # =========================================================================
    # Product parser
    # =========================================================================

    def _parse_product(self, p: dict, cat_name: str, store_id: int) -> dict | None:
        """Map Metro API product dict → canonical schema."""
        try:
            name = clean_text(str(p.get("product_name") or ""))
            if not name:
                return None

            # ── Prices (PKR, as returned by API – no division needed) ───────
            price      = parse_price(str(p.get("price")      or 0))
            sell_price = parse_price(str(p.get("sell_price") or 0))
            sale_price = parse_price(str(p.get("sale_price") or 0))
            mrp_price  = parse_price(str(p.get("mrp_price")  or 0))

            # Determine actual current price (prefer sell_price, then sale_price)
            actual   = sell_price or sale_price or price or mrp_price
            original = price if price and price != actual else None

            if actual is None or actual <= 0:
                return None   # skip unpriceable items

            # ── Weight / quantity ──────────────────────────────────────────
            weight_raw = str(p.get("weight") or "")
            unit_raw   = str(p.get("unit_type") or "")
            combined   = f"{weight_raw} {unit_raw}".strip()
            qty, unit  = normalize_unit(combined or name)
            ppu        = compute_price_per_unit(actual, qty, unit)

            # ── IDs & URLs ─────────────────────────────────────────────────
            pid      = str(p.get("id") or "")
            barcode  = str(p.get("product_code_app") or "")
            img_url  = str(p.get("url") or "")
            slug     = str(p.get("seo_url_slug") or p.get("id") or "")
            prod_url = (
                f"{_FRONTEND}/product/{slug}" if slug else ""
            )

            brand = clean_text(str(p.get("brand_name") or ""))

            return self.build_product_record(
                product_id     = pid,
                name           = name,
                brand          = brand,
                category       = cat_name,
                price          = actual,
                sale_price     = original,
                quantity       = qty,
                unit           = unit,
                price_per_unit = ppu,
                in_stock       = True,      # filtered: available_stock > 0
                image_url      = img_url,
                product_url    = prod_url,
                barcode        = barcode,
                extra          = {
                    "store_id":        store_id,
                    "available_stock": p.get("available_stock"),
                    "vat_perc":        p.get("vat_perc"),
                    "mrp_price":       mrp_price,
                    "article_mgb":     p.get("article_mgb"),
                },
            )
        except Exception as exc:
            self.logger.debug("[Metro] Product parse error: %s | id=%s", exc, p.get("id"))
            return None

    # =========================================================================
    # Multi-city convenience
    # =========================================================================

    @classmethod
    def scrape_all_cities(cls, max_workers_per_store: int = 8) -> list[dict]:
        """Scrape all cities defined in config in parallel."""
        from config.settings import STORES as _STORES
        cities  = _STORES["metro"]["cities"]
        all_rec: list[dict] = []
        _log    = get_logger("metro.all_cities")

        with ThreadPoolExecutor(max_workers=len(cities)) as pool:
            futures = {
                pool.submit(
                    cls(city=c, max_workers=max_workers_per_store).scrape
                ): c
                for c in cities
            }
            for future in as_completed(futures):
                city = futures[future]
                try:
                    recs = future.result()
                    all_rec.extend(recs)
                    _log.info("City %-15s → %d records", city, len(recs))
                except Exception as exc:
                    _log.warning("City %s failed: %s", city, exc)

        _log.info("All-cities done: %d total records", len(all_rec))
        return all_rec

    # =========================================================================
    # Class-level helper: list all stores
    # =========================================================================

    @classmethod
    def list_stores(cls) -> list[dict]:
        """Fetch and return all Metro stores (id, city_name, location)."""
        import requests
        r = requests.get(_STORES_API, timeout=20)
        return r.json().get("data", [])
