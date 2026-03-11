"""
Springs Superstore Pakistan – Scraper (Complete Rewrite)
=========================================================
ROOT CAUSE OF OLD BUG:
  - Used URL: springs.com.pk/grocery.html?p=X  (Magento pattern) – does NOT exist
  - Springs runs on SHOPIFY, not Magento
  - Shopify uses /collections/<handle>/products.json API (unauthenticated, public)

STRATEGY (3-pass):
  Pass 1  - Shopify JSON API per collection handle
            /collections/<handle>/products.json?page=X&limit=250
            Also scrapes /products.json (global catalog).

  Pass 2  - Sitemap discovery
            /sitemap.xml -> find additional collection handles not in our list.

  Pass 3  - Playwright fallback
            Visits key collection pages with network interception.
            Only runs if total < 500 after Passes 1+2.

DEDUPLICATION: by Shopify variant_id (globally unique per store).
"""

from __future__ import annotations

import re
import time
from typing import Any

from config.settings import STORES
from scrapers.base_scraper import BaseScraper
from utils.helpers import normalize_unit, parse_price, compute_price_per_unit, clean_text

_CFG = STORES["springs"]

# Active base URL (confirmed 200 OK + Shopify)
_BASE = "https://springs.com.pk"

# All known + likely Shopify collection handles for Springs
_COLLECTION_HANDLES: list[str] = [
    # Shopify "all products" – always present
    "all",
    # Confirmed / high-confidence
    "trending-products",
    "new-arrivals",
    "featured-products",
    "best-sellers",
    "sale",
    "deals",
    # Grocery staples
    "grocery", "groceries", "food", "food-grocery",
    "bakery", "bakery-breads", "bread",
    "rice", "rice-grains", "rice-pulses", "grains",
    "flour", "atta-flour", "wheat-flour",
    "sugar-salt", "sugar", "salt",
    "cooking-oil", "oil", "ghee", "cooking-oil-ghee",
    "spices", "masala", "spices-condiments",
    "condiments", "sauces", "ketchup-sauces",
    "pickles", "jam-honey",
    "pulses", "lentils", "daal",
    "pasta-noodles", "noodles", "pasta",
    "cereals", "breakfast-cereals", "oats",
    # Dairy
    "dairy", "dairy-eggs", "milk", "uht-milk",
    "yogurt", "butter", "cheese", "cream", "eggs",
    # Beverages
    "beverages", "drinks", "cold-drinks", "soft-drinks",
    "juice", "juices", "water", "mineral-water",
    "tea", "tea-coffee", "coffee", "green-tea",
    "energy-drinks", "squash",
    # Snacks
    "snacks", "biscuits", "cookies", "chips", "crisps",
    "confectionery", "chocolate", "candy", "sweets",
    # Personal care
    "personal-care", "hair-care", "shampoo",
    "skin-care", "soaps", "soap", "hand-wash",
    "body-wash", "lotion", "face-wash",
    "dental-care", "toothpaste", "toothbrush",
    "sanitizer", "dettol",
    # Household / cleaning
    "household", "home-care", "cleaning",
    "detergent", "washing-powder",
    "dishwash", "floor-cleaner", "bleach",
    "tissue", "tissues", "toilet-paper",
    "air-freshener",
    # Baby
    "baby", "baby-care", "baby-food", "diapers",
    "formula", "baby-products",
    # Frozen / Fresh
    "frozen", "frozen-foods", "meat", "chicken",
    "fresh", "fresh-produce", "vegetables", "fruits",
    # Health
    "health", "vitamins", "supplements",
]

# Playwright collection paths for fallback
_PW_PATHS: list[str] = [
    "/collections/all",
    "/collections/trending-products",
    "/collections/grocery",
    "/collections/groceries",
    "/collections/beverages",
    "/collections/dairy",
    "/collections/snacks",
    "/collections/personal-care",
    "/collections/household",
    "/",
]

_PAGE_SIZE = 250


class SpringsScraper(BaseScraper):
    """Shopify-based scraper for springs.com.pk (3-pass strategy)."""

    store_name = "springs"
    base_url   = _BASE

    def __init__(self, city: str = "lahore", max_workers: int = 4) -> None:
        super().__init__(city=city, max_workers=max_workers, rate_calls=5, rate_period=1.0)

    # ── Pass 1: Shopify JSON API ──────────────────────────────────────────────

    _JSON_HEADERS = {"Accept": "application/json, */*;q=0.8"}

    def _fetch_collection(self, handle: str) -> list[dict]:
        """Paginate /collections/<handle>/products.json. Returns [] on 404/error."""
        all_products: list[dict] = []
        page = 1
        url  = f"{_BASE}/collections/{handle}/products.json"

        while True:
            data = self.get_json(url, params={"limit": _PAGE_SIZE, "page": page},
                                 headers=self._JSON_HEADERS)
            if not data:
                break
            batch = data.get("products", [])
            if not batch:
                break
            all_products.extend(batch)
            self.logger.debug(
                "[Springs] collection=%s page=%d got=%d total=%d",
                handle, page, len(batch), len(all_products),
            )
            if len(batch) < _PAGE_SIZE:
                break
            page += 1
            time.sleep(0.35)

        return all_products

    def _fetch_global_catalog(self) -> list[dict]:
        """Paginate /products.json (full Shopify catalog endpoint)."""
        all_products: list[dict] = []
        page = 1
        url  = f"{_BASE}/products.json"

        while True:
            data = self.get_json(url, params={"limit": _PAGE_SIZE, "page": page},
                                 headers=self._JSON_HEADERS)
            if not data:
                break
            batch = data.get("products", [])
            if not batch:
                break
            all_products.extend(batch)
            self.logger.info(
                "[Springs] global catalog page=%d got=%d total=%d",
                page, len(batch), len(all_products),
            )
            if len(batch) < _PAGE_SIZE:
                break
            page += 1
            time.sleep(0.35)

        return all_products

    def _parse_product(self, prod: dict, collection_handle: str) -> list[dict]:
        """One Shopify product dict -> list of build_product_record dicts (one per variant)."""
        title = clean_text(prod.get("title") or "")
        if not title or len(title) < 3:
            return []

        brand    = clean_text(prod.get("vendor") or "")
        category = clean_text(prod.get("product_type") or collection_handle)
        handle   = prod.get("handle") or ""
        imgs     = prod.get("images") or []
        img      = imgs[0].get("src", "") if imgs else ""
        prod_url = f"{_BASE}/products/{handle}" if handle else _BASE

        records: list[dict] = []
        for variant in (prod.get("variants") or []):
            v_title = clean_text(variant.get("title") or "")
            if v_title.lower() in ("default title", ""):
                full_name = title
            else:
                full_name = f"{title} - {v_title}"

            price = parse_price(str(variant.get("price") or "0"))
            if price is None or price <= 0:
                price = parse_price(str(variant.get("compare_at_price") or "0"))
            if price is None or price <= 0:
                continue

            qty, unit = normalize_unit(full_name)
            ppu       = compute_price_per_unit(price, qty, unit)

            records.append(self.build_product_record(
                product_id     = str(variant.get("id") or ""),
                name           = full_name,
                brand          = brand,
                category       = category,
                price          = price,
                quantity       = qty,
                unit           = unit,
                price_per_unit = ppu,
                in_stock       = variant.get("available", True),
                image_url      = img,
                product_url    = prod_url,
                barcode        = clean_text(str(variant.get("barcode") or "")),
            ))
        return records

    def _run_pass1(self) -> tuple[list[dict], set[str]]:
        """Scrape global catalog + all collection handles. Returns (records, seen_variant_ids)."""
        records: list[dict] = []
        seen_ids: set[str]  = set()

        # 1a – Global catalog first (catches every product in one sweep)
        self.logger.info("[Springs] Pass 1a: /products.json global catalog ...")
        for prod in self._fetch_global_catalog():
            for rec in self._parse_product(prod, prod.get("product_type") or "general"):
                vid = str(rec.get("product_id") or "")
                if vid and vid not in seen_ids:
                    seen_ids.add(vid)
                    records.append(rec)
        self.logger.info("[Springs] After global catalog: %d records", len(records))

        # 1b – Per-collection to enrich category labels
        self.logger.info("[Springs] Pass 1b: %d collection handles ...", len(_COLLECTION_HANDLES))
        for handle in _COLLECTION_HANDLES:
            raw = self._fetch_collection(handle)
            if not raw:
                continue
            added = 0
            for prod in raw:
                for rec in self._parse_product(prod, handle):
                    vid = str(rec.get("product_id") or "")
                    if vid and vid not in seen_ids:
                        seen_ids.add(vid)
                        records.append(rec)
                        added += 1
            if added:
                self.logger.info(
                    "[Springs] collection='%s' +%d new | total=%d",
                    handle, added, len(records),
                )
            time.sleep(0.3)

        self.logger.info(
            "[Springs] Pass 1 complete: %d records", len(records)
        )
        return records, seen_ids

    # ── Pass 2: Sitemap discovery ─────────────────────────────────────────────

    def _discover_handles_from_sitemap(self, known_handles: set[str]) -> list[str]:
        """Parse /sitemap.xml to find collection handles not already in known_handles."""
        new_handles: list[str] = []
        try:
            resp = self.get(f"{_BASE}/sitemap.xml")
            if not resp or resp.status_code != 200:
                self.logger.warning("[Springs] sitemap.xml not accessible (HTTP %s)",
                                    getattr(resp, "status_code", "N/A"))
                return []

            text = resp.text
            # Shopify often has a sitemap index entry for collections
            coll_sitemaps = re.findall(
                r"<loc>(https://springs\.com\.pk/sitemap_collections[^<]+)</loc>",
                text,
            )

            if coll_sitemaps:
                for sub_url in coll_sitemaps:
                    sub = self.get(sub_url.strip())
                    if sub and sub.status_code == 200:
                        for _, h in re.findall(
                            r"<loc>(https://springs\.com\.pk/collections/([^<\"?/]+))</loc>",
                            sub.text,
                        ):
                            h = h.strip()
                            if h and h not in known_handles:
                                new_handles.append(h)
                    time.sleep(0.3)
            else:
                for _, h in re.findall(
                    r"<loc>(https://springs\.com\.pk/collections/([^<\"?]+))</loc>",
                    text,
                ):
                    h = h.strip().strip("/")
                    if h and h not in known_handles:
                        new_handles.append(h)

        except Exception as exc:
            self.logger.warning("[Springs] Sitemap discovery error: %s", exc)

        new_handles = list(dict.fromkeys(new_handles))
        self.logger.info("[Springs] Pass 2 sitemap: %d new handles", len(new_handles))
        return new_handles

    def _run_pass2(
        self, records: list[dict], seen_ids: set[str]
    ) -> tuple[list[dict], set[str]]:
        known = set(_COLLECTION_HANDLES) | {"all"}
        new_handles = self._discover_handles_from_sitemap(known)
        added_total = 0
        for idx, handle in enumerate(new_handles, 1):
            raw = self._fetch_collection(handle)
            for prod in raw:
                for rec in self._parse_product(prod, handle):
                    vid = str(rec.get("product_id") or "")
                    if vid and vid not in seen_ids:
                        seen_ids.add(vid)
                        records.append(rec)
                        added_total += 1
            if raw:
                time.sleep(0.3)
            # Log progress every 50 handles
            if idx % 50 == 0 or idx == len(new_handles):
                self.logger.info(
                    "[Springs] Pass 2 progress: %d/%d handles | +%d new | total=%d",
                    idx, len(new_handles), added_total, len(records),
                )
        self.logger.info(
            "[Springs] Pass 2 complete: +%d records | total=%d",
            added_total, len(records),
        )
        return records, seen_ids

    # ── Pass 3: Playwright fallback ───────────────────────────────────────────

    def _run_pass3(self, records: list[dict], seen_ids: set[str]) -> list[dict]:
        """Visit key pages with a headless browser; intercept JSON product responses."""
        if not self._playwright_available:
            self.logger.warning("[Springs] Playwright unavailable – skipping Pass 3")
            return records

        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout  # type: ignore

        pw_added = 0

        def on_response(response):
            nonlocal pw_added
            try:
                if response.status != 200:
                    return
                if "json" not in response.headers.get("content-type", ""):
                    return
                url = response.url
                if not any(k in url for k in ("products.json", "collections", "/products")):
                    return

                data = response.json()
                products: list[dict] = []
                if isinstance(data, dict):
                    products = (
                        data.get("products")
                        or data.get("items")
                        or data.get("data")
                        or []
                    )
                elif isinstance(data, list):
                    products = data

                m = re.search(r"/collections/([^/?]+)", url)
                coll = m.group(1) if m else "pw-fallback"

                for prod in products:
                    if not isinstance(prod, dict):
                        continue
                    for rec in self._parse_product(prod, coll):
                        vid = str(rec.get("product_id") or "")
                        if vid and vid not in seen_ids:
                            seen_ids.add(vid)
                            records.append(rec)
                            pw_added += 1
            except Exception:
                pass

        self.logger.info("[Springs] Pass 3: Playwright fallback starting ...")
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-dev-shm-usage",
                          "--disable-blink-features=AutomationControlled"],
                )
                ctx = browser.new_context(viewport={"width": 1440, "height": 900})
                ctx.add_init_script(
                    "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
                )
                page = ctx.new_page()
                page.on("response", on_response)

                for path in _PW_PATHS:
                    try:
                        self.logger.info(
                            "[Springs] PW visiting %s | collected=%d", path, pw_added
                        )
                        page.goto(_BASE + path, wait_until="networkidle", timeout=35_000)
                        time.sleep(2)
                        for _ in range(5):
                            page.mouse.wheel(0, 2000)
                            time.sleep(0.8)
                    except PWTimeout:
                        self.logger.warning("[Springs] PW timeout: %s", path)
                    except Exception as exc:
                        self.logger.warning("[Springs] PW error %s: %s", path, exc)

                browser.close()
        except Exception as exc:
            self.logger.error("[Springs] Playwright session error: %s", exc)

        self.logger.info(
            "[Springs] Pass 3 complete: +%d records | total=%d",
            pw_added, len(records),
        )
        return records

    # ── Main entry ────────────────────────────────────────────────────────────

    def scrape(self) -> list[dict[str, Any]]:
        self.logger.info(
            "[Springs/%s] Starting scrape (Shopify collections + sitemap + PW fallback)",
            self.city,
        )

        records, seen_ids = self._run_pass1()
        records, seen_ids = self._run_pass2(records, seen_ids)

        if len(records) < 500:
            self.logger.warning(
                "[Springs] Only %d products after API passes – running Playwright fallback",
                len(records),
            )
            records = self._run_pass3(records, seen_ids)

        self.logger.info(
            "[Springs/%s] Scrape complete: %d records", self.city, len(records)
        )

        if records:
            self.save_raw(records)
        else:
            self.logger.error(
                "[Springs] 0 products from all strategies. "
                "Verify springs.com.pk is reachable and the Shopify API is responding."
            )

        return records
