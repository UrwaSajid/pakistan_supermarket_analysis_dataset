"""
alfatah_scraper.py — Al-Fatah supermarket scraper
===================================================
Strategy
--------
Pass 1 (API)  — Shopify /products.json global catalog (limit=250, page=N)
                 alfatah.pk is a Shopify store; the API returns ALL products
                 with clean per-variant prices. Stops on HTTP 400 = no more pages.

Pass 2 (PW)   — Playwright search queries (50 queries) as a coverage top-up.
                 Uses FIXED selectors confirmed from live page HTML:
                     title : .product-title a   (NOT .product-card__title)
                     price : .product-price span (NOT .product-card__price)
                 Only runs if Pass 1 returns < 500 products (safety net).

De-duplication is by Shopify variant ID (Pass 1) or product_name.lower()
(Pass 2 fallback). Both passes deduplicate across themselves.

alfatah.pk has a single national catalogue (no branch-level pricing),
so the city attribute is metadata only.
"""

from __future__ import annotations

import re
import time
from typing import Any

from scrapers.base_scraper import BaseScraper

_BASE      = "https://alfatah.pk"
_PAGE_SIZE = 250

# confirmed css selectors from live page HTML inspection
_CARD_SEL  = ".product-card"
_TITLE_SEL = ".product-title a"          # <a class="product-title-ellipsis">
_PRICE_SEL = ".product-price span"       # <span>Rs.175.00 - Rs.699.00</span>

# 50 varied queries for Playwright pass-2 coverage top-up
_QUERIES = [
    # Dairy & Beverages
    "Milk", "UHT Milk", "Flavored Milk", "Yogurt", "Butter",
    "Cream", "Cheese", "Tea", "Green Tea", "Coffee",
    # Staples
    "Rice", "Basmati Rice", "Flour", "Wheat Flour", "Sugar",
    "Salt", "Bread", "Ghee", "Cooking Oil", "Olive Oil",
    # Snacks & Drinks
    "Biscuits", "Cookies", "Chips", "Juice", "Mango Juice",
    "Soft Drinks", "Mineral Water", "Energy Drinks", "Squash", "Noodles",
    # Personal Care
    "Shampoo", "Conditioner", "Soap", "Hand Wash", "Body Wash",
    "Toothpaste", "Toothbrush", "Dettol", "Lotion", "Face Wash",
    # Household
    "Detergent", "Surf Excel", "Ariel", "Bleach", "Tissue",
    "Dishwash", "Floor Cleaner", "Air Freshener", "Eggs", "Ketchup",
    # Pantry
    "Jam", "Honey", "Cereal", "Oats", "Baby Food",
]

_JSON_HEADERS = {"Accept": "application/json, */*;q=0.8"}


class AlFatahScraper(BaseScraper):
    """Al-Fatah supermarket scraper (Shopify API primary + Playwright fallback)."""

    store_name = "alfatah"
    base_url   = _BASE

    # ── Pass 1: Shopify JSON API ───────────────────────────────────────────────

    def _fetch_page(self, page: int) -> list[dict]:
        """Fetch one page of the global Shopify product catalogue."""
        url  = f"{_BASE}/products.json"
        data = self.get_json(url, params={"limit": _PAGE_SIZE, "page": page},
                             headers=_JSON_HEADERS)
        if data is None:
            return []
        return data.get("products", [])

    def _parse_shopify_product(self, prod: dict) -> list[dict]:
        """Return one canonical record per variant (skip price <= 0)."""
        records = []
        title  = prod.get("title", "").strip()
        ptype  = prod.get("product_type", "").strip() or None
        handle = prod.get("handle", "")
        img_url = None
        if prod.get("images"):
            img_url = prod["images"][0].get("src")

        for var in prod.get("variants", []):
            try:
                price = float(var.get("price") or 0)
            except (TypeError, ValueError):
                price = 0.0
            if price <= 0:
                continue

            var_id    = var.get("id")
            var_title = var.get("title", "")
            name = title
            if var_title and var_title.lower() not in ("default title", ""):
                name = f"{title} – {var_title}"

            records.append(
                self.build_product_record(
                    product_id  = var_id,
                    name        = name,
                    brand       = None,
                    category    = ptype,
                    price       = price,
                    quantity    = var_title if var_title.lower() != "default title" else None,
                    in_stock    = var.get("available", True),
                    image_url   = img_url,
                    product_url = f"{_BASE}/products/{handle}",
                    barcode     = var.get("barcode") or None,
                )
            )
        return records

    def _run_pass1(self) -> tuple[list[dict], set]:
        """Paginate the Shopify catalogue; return (records, seen_variant_ids)."""
        records: list[dict] = []
        seen_ids: set       = set()
        page = 1

        self.logger.info("[AlFatah] Pass 1 starting – Shopify /products.json")
        while True:
            raw = self._fetch_page(page)
            if not raw:
                self.logger.info(
                    "[AlFatah] Pass 1 complete – %d records after page %d",
                    len(records), page,
                )
                break

            added = 0
            for prod in raw:
                for rec in self._parse_shopify_product(prod):
                    vid = rec.get("product_id")
                    if vid not in seen_ids:
                        seen_ids.add(vid)
                        records.append(rec)
                        added += 1

            self.logger.info(
                "[AlFatah] Pass 1 page %d: +%d variants | total=%d",
                page, added, len(records),
            )
            if len(raw) < _PAGE_SIZE:
                break
            page += 1

        return records, seen_ids

    # ── Pass 2: Playwright search top-up ──────────────────────────────────────

    def _extract_cards_pw(self, page_obj, url: str, query: str) -> list[dict]:
        """Extract product cards from a loaded Playwright page."""
        results = []
        try:
            cards = page_obj.query_selector_all(_CARD_SEL)
            for card in cards:
                name = price_val = None

                # title: .product-title a (confirmed from live HTML)
                try:
                    el = card.query_selector(_TITLE_SEL)
                    if el:
                        name = el.inner_text().strip()
                except Exception:
                    pass

                if not name:
                    for sel in ["h2", "h3", "h4", "[class*='title'] a",
                                "[class*='title']", "a"]:
                        try:
                            el = card.query_selector(sel)
                            if el:
                                t = el.inner_text().strip()
                                if len(t) > 3:
                                    name = t
                                    break
                        except Exception:
                            pass

                # price: .product-price span — format "Rs.175.00 - Rs.699.00"
                try:
                    el = card.query_selector(_PRICE_SEL)
                    if el:
                        m = re.search(r"([\d,]+(?:\.\d+)?)", el.inner_text())
                        if m:
                            price_val = float(m.group(1).replace(",", ""))
                except Exception:
                    pass

                if not price_val:
                    for sel in ["[class*='price'] span", "[class*='price']",
                                "[class*='amount']", ".money"]:
                        try:
                            el = card.query_selector(sel)
                            if el:
                                m = re.search(r"([\d,]+(?:\.\d+)?)",
                                              el.inner_text())
                                if m:
                                    price_val = float(m.group(1).replace(",", ""))
                                    break
                        except Exception:
                            pass

                if name and price_val and price_val > 0:
                    results.append({
                        "name":     name,
                        "price":    price_val,
                        "category": query,
                        "url":      url,
                    })
        except Exception as exc:
            self.logger.error("[AlFatah] extract_cards error: %s", exc)
        return results

    def _scrape_query_pw(self, page_obj, query: str) -> list[dict]:
        """Navigate to search URL and extract visible product cards."""
        from playwright.sync_api import TimeoutError as PWTimeout

        url = f"{_BASE}/search?q={query.replace(' ', '+')}"
        try:
            page_obj.goto(url, wait_until="networkidle", timeout=40_000)
            time.sleep(2)
        except PWTimeout:
            self.logger.warning("[AlFatah] PW timeout for query '%s'", query)
            return []
        except Exception as exc:
            self.logger.error("[AlFatah] PW error for query '%s': %s", query, exc)
            return []

        return self._extract_cards_pw(page_obj, url, query)

    def _run_pass2(self, records: list[dict], seen_ids: set) -> list[dict]:
        """Playwright search sweep; adds only name-deduplicated new records."""
        self.logger.info("[AlFatah] Pass 2 – Playwright search queries (%d)",
                         len(_QUERIES))
        seen_names: set = {r["name"].lower().strip() for r in records if r.get("name")}
        added_total = 0

        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            self.logger.warning("[AlFatah] Playwright not installed – skipping Pass 2")
            return records

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage",
                      "--disable-blink-features=AutomationControlled"],
            )
            ctx = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1440, "height": 900},
            )
            ctx.add_init_script(
                "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
            )
            page_obj = ctx.new_page()

            for idx, query in enumerate(_QUERIES, 1):
                items = self._scrape_query_pw(page_obj, query)
                added = 0
                for item in items:
                    key = item["name"].lower().strip()
                    if key not in seen_names:
                        seen_names.add(key)
                        records.append(
                            self.build_product_record(
                                name        = item["name"],
                                category    = item["category"],
                                price       = item["price"],
                                product_url = item["url"],
                            )
                        )
                        added += 1
                added_total += added

                if idx % 10 == 0 or idx == len(_QUERIES):
                    self.logger.info(
                        "[AlFatah] Pass 2: %d/%d queries | +%d new | total=%d",
                        idx, len(_QUERIES), added_total, len(records),
                    )
                time.sleep(1.0)

            browser.close()

        self.logger.info(
            "[AlFatah] Pass 2 complete: +%d records | total=%d",
            added_total, len(records),
        )
        return records

    # ── Entry point ────────────────────────────────────────────────────────────

    def scrape(self) -> list[dict[str, Any]]:
        self.logger.info(
            "[AlFatah] Starting scraper | city=%s | strategy=Shopify-API+PW-fallback",
            self.city,
        )

        # Pass 1 – Shopify product API (primary, gets full catalogue)
        records, seen_ids = self._run_pass1()

        # Pass 2 – Playwright only if API returned very little
        if len(records) < 500:
            self.logger.info(
                "[AlFatah] Pass 1 returned only %d records – running Playwright pass-2",
                len(records),
            )
            records = self._run_pass2(records, seen_ids)
        else:
            self.logger.info(
                "[AlFatah] Pass 1 returned %d records – skipping Playwright pass-2",
                len(records),
            )

        if not records:
            self.logger.error("[AlFatah] 0 products scraped.")
            return []

        self.logger.info("[AlFatah] Total unique records: %d", len(records))
        self.save_raw(records)
        return records


# Keep old name as alias for compatibility with any existing main.py dispatch
AlfatahScraper = AlFatahScraper

if __name__ == "__main__":
    AlFatahScraper(city="lahore").scrape()
