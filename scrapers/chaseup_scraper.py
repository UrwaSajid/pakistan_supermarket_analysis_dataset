"""
chaseup_scraper.py – v4 (Blink platform, Playwright DOM extraction)
======================================================================
Chase Up Grocery  https://www.chaseupgrocery.com

Platform: Blink e-commerce (blinkco.io) – NOT Shopify / WooCommerce / Foodics.

Discovery (March 2026)
  restId   = 55525
  branchId = 56249
  CDN      = g-cdn.blinkco.io/ordering-system/55525/gallery/

Strategy:
  1.  Blink API probe  — tries known Blink REST endpoints to get product data.
      If ≥ 100 products returned, skip Playwright.
  2.  Playwright crawl — navigates the /catalog/ tree:
        homepage  → .swiper-slide a[href*="/catalog/"]   → top-level categories
        cat page  → same swiper filtered by path depth   → subcategories
        leaf page → scroll to load lazy items, extract via:
                    [id^="product-item-"] h4[title]
                    [class*="price_label"] (text "Rs. X")
      Also intercepts all JSON network responses for bonus data.

City pricing:
  Karachi  → 1.000×  (real prices from site)
  Lahore   → 1.025×  (estimated projection +2.5%)
"""

from __future__ import annotations

import json
import re
import time
from typing import Any

from config.settings import STORES
from scrapers.base_scraper import BaseScraper
from utils.helpers import normalize_unit, parse_price, compute_price_per_unit, clean_text

_CFG = STORES["chaseup"]


_BASE      = "https://www.chaseupgrocery.com"
_REST_ID   = "55525"
_BRANCH_ID = "56249"

# City price multipliers (ChaseUp has one website; Lahore estimated)
_CITY_MULT: dict[str, float] = {"karachi": 1.000, "lahore": 1.025}

# Blink API header set
_API_HEADERS = {
    "Accept":             "application/json, */*",
    "Referer":            _BASE + "/",
    "Origin":             _BASE,
    "X-Requested-With":  "XMLHttpRequest",
}

# Hardcoded seed categories (fallback when homepage swiper is empty)
_SEED_CATEGORIES = [
    {"href": "/catalog/grocery--staples-30886",        "name": "Grocery & Staples"},
    {"href": "/catalog/fresh-produce-30887",           "name": "Fresh Produce"},
    {"href": "/catalog/beverages-30888",               "name": "Beverages"},
    {"href": "/catalog/dairy-eggs-30889",              "name": "Dairy & Eggs"},
    {"href": "/catalog/bakery-30890",                  "name": "Bakery"},
    {"href": "/catalog/frozen-foods-30891",            "name": "Frozen Foods"},
    {"href": "/catalog/snacks-30892",                  "name": "Snacks"},
    {"href": "/catalog/household-cleaning-30893",      "name": "Household & Cleaning"},
    {"href": "/catalog/personal-care-30894",           "name": "Personal Care"},
    {"href": "/catalog/baby-kids-30895",               "name": "Baby & Kids"},
]

# Known-good subcategory URLs discovered from live HTML inspection (March 2026)
# Used to seed the category crawler so it always covers these sections
_KNOWN_SUBCATS: dict[str, list[dict]] = {
    "/catalog/grocery--staples-30886": [
        {"href": "/catalog/grocery--staples-30886/grocery--staples-55110",
         "name": "Grocery & Staples"},
    ],
}


class ChaseUpScraper(BaseScraper):
    """Scraper for Chase Up Grocery (Blink platform)."""

    store_name = "chaseup"
    base_url   = _BASE

    def __init__(self, city: str = "karachi", max_workers: int = 2) -> None:
        super().__init__(city=city, max_workers=max_workers, rate_calls=3, rate_period=1.0)
        self._price_mult = _CITY_MULT.get(city.lower(), 1.000)

    # ─── Strategy 1: Blink catalog API probe ─────────────────────────────────

    def _try_blink_api(self) -> list[dict]:
        """
        Try known Blink API endpoint patterns.
        Returns product records if ≥ 100 products found, otherwise [].
        """
        base_params = {"restId": _REST_ID, "rest_brId": _BRANCH_ID}
        records: list[dict] = []
        seen: set[str] = set()

        # ── Step 1: fetch category tree ──────────────────────────────────────
        cat_endpoints = [
            f"{_BASE}/api/catalog",
            f"{_BASE}/api/categories",
            f"{_BASE}/api/home-menu",
            f"{_BASE}/api/menu",
            f"{_BASE}/api/v1/categories",
            f"{_BASE}/api/catalog/categories",
        ]
        categories: list[dict] = []
        for url in cat_endpoints:
            data = self.get_json(url, params=base_params, headers=_API_HEADERS)
            if not data:
                continue
            cat_list: list = []
            if isinstance(data, list):
                cat_list = data
            elif isinstance(data, dict):
                for key in ("categories", "data", "sections", "groups", "menu", "result", "items"):
                    v = data.get(key)
                    if isinstance(v, list):
                        cat_list = v
                        break
            for c in cat_list:
                if not isinstance(c, dict):
                    continue
                cid  = c.get("id") or c.get("category_id") or c.get("cat_id")
                name = c.get("name") or c.get("title") or c.get("category_name") or ""
                if cid:
                    categories.append({"id": cid, "name": str(name)})
            if categories:
                self.logger.info("[ChaseUp/%s] Blink API categories: %d from %s",
                                 self.city, len(categories), url)
                break

        # ── Step 2: fetch items per category ─────────────────────────────────
        item_endpoints = [
            f"{_BASE}/api/items",
            f"{_BASE}/api/catalog/items",
            f"{_BASE}/api/menu/items",
            f"{_BASE}/api/v1/items",
        ]
        for cat in categories:
            for url in item_endpoints:
                data = self.get_json(
                    url,
                    params={**base_params, "category_id": cat["id"],
                            "per_page": 200, "page": 1},
                    headers=_API_HEADERS,
                )
                if not data:
                    continue
                added = self._ingest_api_response(data, cat["name"], records, seen)
                if added:
                    self.logger.info("[ChaseUp/%s] Cat '%s': +%d | total=%d",
                                     self.city, cat["name"], added, len(records))
                    break
                time.sleep(0.2)

        # ── Step 3: also try a direct "all items" dump ───────────────────────
        if len(records) < 50:
            for url in [f"{_BASE}/api/items", f"{_BASE}/api/all-items",
                        f"{_BASE}/api/products"]:
                data = self.get_json(url, params={**base_params, "per_page": 500},
                                     headers=_API_HEADERS)
                if data:
                    added = self._ingest_api_response(data, "", records, seen)
                    if added:
                        self.logger.info("[ChaseUp/%s] Direct dump +%d | total=%d",
                                         self.city, added, len(records))

        return records

    def _ingest_api_response(self, data: Any, category: str,
                             records: list, seen: set) -> int:
        """Parse a JSON API response and append canonical product dicts."""
        items: list = data if isinstance(data, list) else []
        if not items and isinstance(data, dict):
            for key in ("data", "items", "products", "result"):
                v = data.get(key)
                if isinstance(v, list):
                    items = v
                    break

        added = 0
        for item in items:
            if not isinstance(item, dict):
                continue
            name = None
            for nk in ("name", "title", "item_name", "product_name", "label", "display_name"):
                v = item.get(nk)
                if v and isinstance(v, str) and len(v.strip()) > 2:
                    name = v.strip()
                    break
            if not name:
                continue

            price = None
            for pk in ("price", "sale_price", "selling_price", "base_price", "amount", "cost"):
                v = item.get(pk)
                if v is not None:
                    try:
                        p = float(re.sub(r"[^\d.]", "", str(v)))
                        if p > 0:
                            price = p
                            break
                    except (ValueError, TypeError):
                        pass
            if not price:
                continue

            key = name.lower().strip()
            if key in seen:
                continue
            seen.add(key)

            img = item.get("image") or item.get("photo") or item.get("thumbnail") or ""
            if img and not str(img).startswith("http"):
                img = f"https://g-cdn.blinkco.io/ordering-system/{_REST_ID}/gallery/{img}"

            cat = category or item.get("category_name") or item.get("category") or ""
            qty, unit = normalize_unit(name)
            ppu        = compute_price_per_unit(price, qty, unit)

            records.append(self.build_product_record(
                product_id    = str(item.get("id", "")),
                name          = name,
                category      = cat,
                price         = round(price * self._price_mult, 2),
                currency      = "PKR",
                quantity      = qty,
                unit          = unit,
                price_per_unit = ppu,
                in_stock      = True,
                image_url     = str(img),
                product_url   = _BASE,
            ))
            added += 1
        return added

    # ─── Strategy 2: Playwright catalog crawler ───────────────────────────────

    def _scrape_with_playwright(self) -> list[dict]:
        """
        Full Playwright crawl of the /catalog/ tree.

        1. Homepage  → discover top-level category links from swiper slides
        2. Cat page  → discover subcategory links (deeper swiper / MUI sidebar)
        3. Leaf page → scroll to load all lazy products, extract via DOM
        Also intercepts JSON network traffic for bonus products.
        """
        try:
            from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
        except ImportError:
            self.logger.error("[ChaseUp] playwright package not installed – pip install playwright")
            return []

        records: list[dict] = []
        seen: set[str]      = set()
        intercepted: list[dict] = []
        intercepted_seen: set[str] = set()

        def _on_response(resp):
            """Capture JSON API responses during navigation."""
            try:
                if resp.status != 200:
                    return
                ct = resp.headers.get("content-type", "")
                if "json" not in ct:
                    return
                body = resp.body()
                if len(body) < 20 or len(body) > 5_000_000:
                    return
                data = json.loads(body)
                self._ingest_api_response(data, "", intercepted, intercepted_seen)
            except Exception:
                pass

        with sync_playwright() as pw:
            browser = pw.chromium.launch(
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
            page = ctx.new_page()
            page.on("response", _on_response)

            # ── Step 1: homepage – get top-level categories ───────────────────
            # Try homepage and /catalog page; whichever gives more results wins
            top_cats = self._pw_get_categories(page, _BASE + "/", depth=1)
            if len(top_cats) < 5:
                # /catalog may show the full category tree better
                alt = self._pw_get_categories(page, _BASE + "/catalog", depth=1)
                if len(alt) > len(top_cats):
                    top_cats = alt
            if not top_cats:
                self.logger.warning("[ChaseUp] No categories from homepage swiper; using seeds")
                top_cats = _SEED_CATEGORIES
            self.logger.info("[ChaseUp/%s] Top categories: %d", self.city, len(top_cats))

            # ── Step 2: iterate categories ────────────────────────────────────
            for cat in top_cats:
                cat_url = _BASE + cat["href"] if cat["href"].startswith("/") else cat["href"]

                # Try to find subcategories on this page; also inject known seeds
                sub_cats = self._pw_get_categories(page, cat_url, depth=2)
                # Augment with any hard-coded known subcategory seeds
                known = _KNOWN_SUBCATS.get(cat["href"], [])
                known_hrefs = {s["href"] for s in sub_cats}
                for ks in known:
                    if ks["href"] not in known_hrefs:
                        sub_cats.append(ks)

                if sub_cats:
                    self.logger.info("[ChaseUp/%s] Cat '%s' → %d subcats",
                                     self.city, cat["name"], len(sub_cats))
                    for sub in sub_cats:
                        sub_url = _BASE + sub["href"] if sub["href"].startswith("/") else sub["href"]

                        # Try level-3 sub-subcategories
                        leaf_cats = self._pw_get_categories(page, sub_url, depth=3)
                        if leaf_cats:
                            for leaf in leaf_cats:
                                leaf_url = (_BASE + leaf["href"]
                                            if leaf["href"].startswith("/") else leaf["href"])
                                new_r = self._pw_extract_page(page, leaf_url, leaf["name"])
                                self._merge(new_r, records, seen)
                                self.logger.info(
                                    "[ChaseUp/%s] Leaf '%s': +%d | total=%d",
                                    self.city, leaf["name"], len(new_r), len(records))
                        else:
                            new_r = self._pw_extract_page(page, sub_url, sub["name"])
                            self._merge(new_r, records, seen)
                            self.logger.info(
                                "[ChaseUp/%s] Sub '%s': +%d | total=%d",
                                self.city, sub["name"], len(new_r), len(records))
                else:
                    new_r = self._pw_extract_page(page, cat_url, cat["name"])
                    self._merge(new_r, records, seen)
                    self.logger.info("[ChaseUp/%s] Cat '%s': +%d | total=%d",
                                     self.city, cat["name"], len(new_r), len(records))

            browser.close()

        # Merge intercept bonus
        for r in intercepted:
            key = (r.get("name") or "").lower().strip()
            if key and key not in seen:
                seen.add(key)
                records.append(r)
        if intercepted:
            self.logger.info("[ChaseUp/%s] Intercepted API bonus: +%d | total=%d",
                             self.city, len(intercepted), len(records))

        return records

    # ─── Playwright helpers ───────────────────────────────────────────────────

    def _pw_get_categories(self, page, url: str, depth: int) -> list[dict]:
        """
        Navigate *url* and return catalog links at the next depth level.
        depth=1 → find links with 1 path segment after /catalog/
        depth=2 → find links with 2 path segments
        depth=3 → find links with 3 path segments
        """
        try:
            from playwright.sync_api import TimeoutError as PWTimeout
            page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            # Wait for swiper carousel (React SPA needs extra time to hydrate)
            try:
                page.wait_for_selector(".swiper-slide, .swiper-wrapper", timeout=8_000)
            except Exception:
                pass
            time.sleep(3)
        except Exception as e:
            self.logger.debug("[ChaseUp] Nav error %s: %s", url, e)
            return []

        js = f"""
        () => {{
            const links = [];
            const seen  = new Set();
            const depth = {depth};

            // Look in swiper slides AND general nav links
            document.querySelectorAll('a[href*="/catalog/"]').forEach(a => {{
                const href = a.getAttribute('href');
                if (!href || seen.has(href)) return;

                // Count non-empty path segments after /catalog/
                const part = href.replace(/\\?.*$/, '').replace(/\\/+$/, '');
                const after = part.split('/catalog/')[1];
                if (!after) return;
                const segs = after.split('/').filter(Boolean);
                if (segs.length !== depth) return;

                seen.add(href);
                const span = a.querySelector('span');
                const name = (span ? span.textContent : a.textContent || '').trim();
                if (name) links.push({{href, name}});
            }});

            // Also try sidebar MUI list items — click each radio and capture value
            // (only for subcategory discovery at depth >= 2)
            if (depth >= 2) {{
                const currentPath = window.location.pathname.replace(/\\/+$/, '');
                document.querySelectorAll(
                    'ul.MuiList-root input[type="radio"], ul.MuiList-root a[href]'
                ).forEach(el => {{
                    if (el.tagName === 'A') {{
                        const href = el.getAttribute('href');
                        if (href && href.includes('/catalog/') && !seen.has(href)) {{
                            const after = href.replace(/\\?.*$/, '').replace(/\\/+$/, '').split('/catalog/')[1];
                            if (after) {{
                                const segs = after.split('/').filter(Boolean);
                                if (segs.length === depth) {{
                                    seen.add(href);
                                    const name = el.textContent.trim();
                                    if (name) links.push({{href, name}});
                                }}
                            }}
                        }}
                    }} else {{
                        // radio: value might be slug like "canned-food-75297" or just the ID
                        const val = el.value;
                        const label = (el.closest('label') || el.parentElement)
                            ?.querySelector('.MuiFormControlLabel-label, .MuiTypography-root');
                        const name = label ? label.textContent.trim() : '';
                        if (val && name && val !== '' && val.toString() !== '0') {{
                            // Try to construct URL: currentPath + '/' + val
                            const href = currentPath + '/' + val;
                            if (!seen.has(href)) {{
                                seen.add(href);
                                links.push({{href, name, fromRadio: true}});
                            }}
                        }}
                    }}
                }});
            }}

            return links;
        }}
        """
        try:
            cats = page.evaluate(js)
            return cats if cats else []
        except Exception as e:
            self.logger.debug("[ChaseUp] JS eval error at %s: %s", url, e)
            return []

    def _pw_extract_page(self, page, url: str, category: str) -> list[dict]:
        """
        Navigate a leaf catalog page, scroll to load all lazy products,
        extract them via confirmed Blink DOM selectors, return record list.
        """
        try:
            from playwright.sync_api import TimeoutError as PWTimeout
            page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            time.sleep(2)
        except Exception as e:
            self.logger.debug("[ChaseUp] Page nav error %s: %s", url, e)
            return []

        # Scroll loop — keep going until product count stabilises
        prev_count = 0
        for _ in range(20):
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(1.5)
            try:
                count = page.evaluate(
                    "() => document.querySelectorAll('[id^=\"product-item-\"]').length"
                )
                if count == prev_count:
                    break
                prev_count = count
            except Exception:
                break

        # DOM extraction using confirmed Blink selectors
        js_extract = r"""
        () => {
            const products = [];
            document.querySelectorAll('[id^="product-item-"]').forEach(el => {
                // Name: h4[title] attribute is the canonical product name
                const h4 = el.querySelector('h4[title]');
                if (!h4) return;
                const name = h4.getAttribute('title') || h4.textContent.trim();
                if (!name) return;

                // Price: element with "price_label" in class name, text "Rs. 275.00"
                let price = null;
                const priceEl = el.querySelector('[class*="price_label"]');
                if (priceEl) {
                    const m = (priceEl.textContent || '').match(/[\d,]+\.?\d*/);
                    if (m) price = parseFloat(m[0].replace(/,/g, ''));
                }
                if (!price) {
                    // Fallback: first span that contains "Rs" text
                    for (const s of el.querySelectorAll('span')) {
                        const txt = s.textContent || '';
                        if (txt.match(/Rs\.?\s*[\d,]/i)) {
                            const m = txt.match(/[\d,]+\.?\d*/);
                            if (m) { price = parseFloat(m[0].replace(/,/g, '')); break; }
                        }
                    }
                }

                const img = el.querySelector('img');
                const imgSrc = img
                    ? (img.getAttribute('src') || img.getAttribute('data-src') || '')
                    : '';
                const pid = el.id.replace('product-item-', '');

                products.push({ name, price, img: imgSrc, id: pid });
            });
            return products;
        }
        """
        try:
            raw = page.evaluate(js_extract)
        except Exception as e:
            self.logger.warning("[ChaseUp] JS extraction error on %s: %s", url, e)
            return []

        records = []
        for p in (raw or []):
            if not p.get("name") or not p.get("price"):
                continue
            qty, unit = normalize_unit(p["name"])
            ppu        = compute_price_per_unit(p["price"], qty, unit)
            records.append(self.build_product_record(
                product_id    = str(p.get("id", "")),
                name          = p["name"],
                category      = category,
                price         = round(float(p["price"]) * self._price_mult, 2),
                currency      = "PKR",
                quantity      = qty,
                unit          = unit,
                price_per_unit = ppu,
                in_stock      = True,
                image_url     = p.get("img", ""),
                product_url   = url,
            ))
        return records

    @staticmethod
    def _merge(new_recs: list[dict], records: list[dict], seen: set[str]) -> None:
        """Deduplicate new_recs into records by lowercase name."""
        for r in new_recs:
            key = (r.get("name") or "").lower().strip()
            if key and key not in seen:
                seen.add(key)
                records.append(r)

    # ─── Main entry ───────────────────────────────────────────────────────────

    def scrape(self) -> list[dict[str, Any]]:
        self.logger.info(
            "[ChaseUp/%s] Starting scrape – Blink platform (%s)",
            self.city, _BASE,
        )

        # Strategy 1: lightweight API probe
        records = self._try_blink_api()
        self.logger.info("[ChaseUp/%s] API probe: %d products", self.city, len(records))

        # Strategy 2: Playwright catalog crawler (always runs; deduplication handles overlap)
        self.logger.info("[ChaseUp/%s] Starting Playwright catalog crawl …", self.city)
        pw_records = self._scrape_with_playwright()
        existing = {(r.get("name") or "").lower().strip() for r in records}
        for r in pw_records:
            key = (r.get("name") or "").lower().strip()
            if key and key not in existing:
                existing.add(key)
                records.append(r)

        self.logger.info("[ChaseUp/%s] Combined total: %d products", self.city, len(records))

        if not records:
            self.logger.error("[ChaseUp/%s] 0 products scraped from all strategies", self.city)
            return []

        self.save_raw(records)
        return records
