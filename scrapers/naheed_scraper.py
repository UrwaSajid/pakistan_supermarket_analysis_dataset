"""
naheed_scraper.py
=================
Scraper for naheed.pk (Magento 2 / JS-rendered store).

Strategy (3-pass):
  Pass 1 — Playwright category crawl + live network JSON interception
            Visit every category URL extracted from the live site HTML.
            Intercept all XHR/fetch JSON responses automatically.
            Scroll to load lazy products; paginate via ?p=N.

  Pass 2 — Playwright search keyword crawl
            GET /catalogsearch/result/?q=<keyword> for 45 keywords.
            Same DOM extraction + network interception.

  Pass 3 — Magento 2 REST API probe (unauthenticated; fast if available)
            /rest/V1/products?searchCriteria[pageSize]=100&page=N
            Silently skips with HTTP 401.

CITIES: karachi (real prices), lahore (+1.5% estimated projection)
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone

import pandas as pd

from config.settings import RAW_DIR, STORES
from scrapers.base_scraper import BaseScraper

_CFG  = STORES["naheed"]
_BASE = "https://www.naheed.pk"

# ── All category paths from live /all-categories page (March 2026) ────────────
_CATEGORIES = [
    # Grocery & Pet Care
    "groceries-pets",
    "groceries-pets/frozen-food-ice-cream",
    "groceries-pets/dairy",
    "groceries-pets/breakfast",
    "groceries-pets/bread-bakery",
    "groceries-pets/deserts",
    "groceries-pets/laundry-household",
    "groceries-pets/baking-cooking",
    "groceries-pets/food-staples",
    "groceries-pets/food-staples/sauces-pickles",
    "groceries-pets/food-staples/spices-recipes",
    "groceries-pets/food-staples/canned-jarred-food",
    "groceries-pets/food-staples/noodles-pasta",
    "groceries-pets/baking-cooking/rice",
    "groceries-pets/baking-cooking/flours-meals",
    "groceries-pets/baking-cooking/home-baking",
    "groceries-pets/baking-cooking/cooking-oil",
    "groceries-pets/baking-cooking/olive-oil",
    "groceries-pets/fresh-products/fruits",
    "groceries-pets/fresh-products/vegetables",
    "groceries-pets/fresh-products/meat-poultry",
    "groceries-pets/beverages",
    "groceries-pets/beverages/juices",
    "groceries-pets/beverages/tea-coffee",
    "groceries-pets/beverages/soft-drinks-soda",
    "groceries-pets/beverages/drinking-water",
    "groceries-pets/beverages/squash-syrup-flavors",
    "groceries-pets/beverages/powdered-drinks",
    # Naheed own-brand
    "naheed-products/naheed-basics",
    "naheed-products/naheed-dry-fruits",
    "naheed-products/naheed-spices",
    # Health & Beauty
    "health-beauty",
    "health-beauty/k-beauty",
    "health-beauty/perfumes",
    "health-beauty/hair-care",
    "health-beauty/makeup",
    "health-beauty/skin-care",
    "health-beauty/bath-body",
    "health-beauty/eye-care",
    "health-beauty/feminine-care",
    "health-beauty/mens-care",
    "health-beauty/personal-care",
    "health-beauty/sexual-wellness",
    # TV & Home Appliances
    "tv-home-appliances",
    "tv-home-appliances/kitchen-appliances",
    "tv-home-appliances/iron-steamers",
    "tv-home-appliances/household-cleaners",
    "tv-home-appliances/air-purifiers",
    "tv-home-appliances/lcd-led-televisions",
    "tv-home-appliances/streaming-players",
    # Kids & Babies
    "kids-babies",
    "kids-babies/feeding",
    "kids-babies/baby-care",
    "kids-babies/diapering-napping",
    "kids-babies/baby-foods",
    "kids-babies/toys",
    "kids-babies/baby-gear",
    "kids-babies/baby-bedding",
    "kids-babies/clothing-accessories",
    "kids-babies/shoes-sandals",
    # Medical & Nutrition
    "medical-nutrition",
    "medical-nutrition/vitamins-supplements",
    "medical-nutrition/organic",
    "medical-nutrition/herbal",
    "medical-nutrition/diabetic-products",
    "medical-nutrition/food-supplements",
    "medical-nutrition/equipment-supplies",
    "medical-nutrition/otc-medicines-first-aid",
    # Pharmacy
    "pharmacy",
    "pharmacy/a-z",
    # Phones & Computers
    "phones-tablets",
    "phones-tablets/smartphones",
    "phones-tablets/feature-phones",
    "phones-tablets/chargers-cables",
    "phones-tablets/power-banks",
    "phones-tablets/monitors-smart-signage",
    "phones-tablets/games",
    "phones-tablets/landline-phones",
    "phones-tablets/peripherals-accessories",
    "phones-tablets/audio",
    # Women's Fashion
    "womens-fashion",
    "womens-fashion/nightwear",
    "womens-fashion/loungewear",
    "womens-fashion/tops-bottoms",
    "womens-fashion/undergarments",
    "womens-fashion/stitched",
    "womens-fashion/unstitched",
    "womens-fashion/footwear",
    "womens-fashion/accessories",
    "womens-fashion/muslim-wear",
    # Men's Fashion
    "men-s-fashion",
    "men-s-fashion/undergarments",
    "men-s-fashion/loungewear",
    "men-s-fashion/nightwear",
    "men-s-fashion/winter-wear",
    "men-s-fashion/men-s-tracksuits",
    "men-s-fashion/tops",
    "men-s-fashion/bottoms",
    "men-s-fashion/accessories",
    "men-s-fashion/footwear",
    # Home & Lifestyle
    "home-lifestyle",
    "home-lifestyle/kitchen-dinning",
    "home-lifestyle/home-decor",
    "home-lifestyle/bedding",
    "home-lifestyle/bath",
    "home-lifestyle/travel-luggage",
    "home-lifestyle/storage-organization",
    "home-lifestyle/sports-outdoors",
    "home-lifestyle/household-supplies",
    "home-lifestyle/party-supplies",
    "home-lifestyle/automotive-supplies",
    "home-lifestyle/fitness-exercise",
    "home-lifestyle/religious-and-spiritual",
    "home-lifestyle/electronic-accessories",
    # Watches, Bags & Jewellery
    "watches-bags-jewellery",
    "watches-bags-jewellery/womens-watches",
    "watches-bags-jewellery/mens-watches",
    "watches-bags-jewellery/kids-watches",
    "watches-bags-jewellery/womens-jewellery",
    "watches-bags-jewellery/mens-jewellery",
    "watches-bags-jewellery/womens-bags",
    "watches-bags-jewellery/mens-bags-accessories",
    "watches-bags-jewellery/kids-bags",
    # Stationery & Craft
    "home-lifestyle/stationery-craft",
    "home-lifestyle/stationery-craft/pens",
    "home-lifestyle/stationery-craft/pencils",
    "home-lifestyle/stationery-craft/erasers-sharpeners",
    "home-lifestyle/stationery-craft/rulers-stencils",
    "home-lifestyle/stationery-craft/inks-paints",
    "home-lifestyle/stationery-craft/markers-highlighters",
    "home-lifestyle/stationery-craft/scissors-cutters",
    "home-lifestyle/stationery-craft/art-craft-materials",
    "home-lifestyle/stationery-craft/diaries-notebooks",
    "home-lifestyle/stationery-craft/accessories",
    # Books
    "books",
    "books/children-s-books",
    "books/non-fiction-books",
    # Fresh St! Cafe
    "fresh-st-cafe",
    "fresh-st-cafe/cakes",
    "fresh-st-cafe/brownies-cupcakes",
    "fresh-st-cafe/savoury",
    "fresh-st-cafe/biscuits-cookies",
    "fresh-st-cafe/desserts-pastry",
    "fresh-st-cafe/tarts-pops",
    "fresh-st-cafe/breads-44-buns-rolls",
    # Back to School
    "back-to-school",
    "back-to-school/lunch-boxes",
    "back-to-school/school-bags",
    "back-to-school/educational-toys",
    "back-to-school/sports-activity",
    "back-to-school/school-supplies",
    "back-to-school/water-bottles",
    "back-to-school/tech",
    "back-to-school/health-wellness",
    "back-to-school/cleaning-supplies",
    "back-to-school/lunch-time",
]

_KEYWORDS = [
    "milk", "tea", "rice", "oil", "ghee", "flour", "sugar", "salt",
    "biscuits", "chips", "juice", "water", "shampoo", "soap", "detergent",
    "toothpaste", "eggs", "yogurt", "butter", "cheese", "coffee",
    "ketchup", "jam", "honey", "noodles", "pasta", "cereal", "oats",
    "diapers", "lotion", "conditioner", "bleach", "tissue", "cream",
    "pickle", "sauce", "masala", "atta", "basmati", "canola", "sunflower",
    "chocolate", "candy", "lentils",
]

# Magento 2 selector sets (most specific first)
_SEL_SETS = [
    {
        "card":  ".product-item",
        "name":  ".product-item-name a, .product-item-link",
        "price": ".price-wrapper .price, .special-price .price, .regular-price .price",
    },
    {
        "card":  ".item.product.product-item",
        "name":  ".product-item-link",
        "price": ".price",
    },
    {
        "card":  "[class*='product-item'], [class*='product_item']",
        "name":  "[class*='product-name'], [class*='product-title'], h2, h3",
        "price": "[class*='price']",
    },
]


class NaheedScraper(BaseScraper):
    """Naheed.pk scraper — Magento 2 / Playwright 3-pass strategy."""

    store_name = "naheed"
    base_url   = _BASE

    # City price multiplier (naheed.pk is one national online store)
    _CITY_MULT = {"karachi": 1.000, "lahore": 1.015}

    def __init__(self, city: str = "karachi", max_workers: int = 2,
                 direction: str = "forward", cat_range: tuple | None = None) -> None:
        super().__init__(city=city, max_workers=max_workers,
                         rate_calls=3, rate_period=1.0)
        self._direction = direction  # "forward" or "backward"
        self._cat_range = cat_range  # (start, end) slice of _CATEGORIES for parallel chunk runs
        # Each non-default instance gets its own logger to avoid log file conflicts.
        if direction == "backward" or cat_range is not None:
            from utils.logger import get_logger
            suffix = f"c{cat_range[0]}" if cat_range is not None else "rev"
            self.logger = get_logger(f"{self.store_name}.{city}.{suffix}")

    # ── Playwright browser factory ────────────────────────────────────────────

    @staticmethod
    def _make_context(playwright):
        browser = playwright.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-extensions",
            ],
        )
        ctx = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1440, "height": 900},
        )
        ctx.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
        )
        return browser, ctx

    # ── JSON interception helpers ─────────────────────────────────────────────

    def _harvest_json(self, obj, src_url, collected, seen, depth=0):
        """Recursively walk any JSON object and extract product-like records."""
        if depth > 8:
            return
        if isinstance(obj, list):
            for item in obj:
                if isinstance(item, dict):
                    self._try_product(item, src_url, collected, seen)
                self._harvest_json(item, src_url, collected, seen, depth + 1)
        elif isinstance(obj, dict):
            self._try_product(obj, src_url, collected, seen)
            for v in obj.values():
                if isinstance(v, (dict, list)):
                    self._harvest_json(v, src_url, collected, seen, depth + 1)

    def _try_product(self, d, src_url, collected, seen):
        name = None
        for k in ("name", "title", "product_name", "item_name", "productName",
                  "sku_name", "product_title", "label"):
            v = d.get(k)
            if v and isinstance(v, str) and len(v.strip()) > 3:
                name = v.strip()
                break
        if not name:
            return

        price = None
        for k in ("price", "final_price", "special_price", "sale_price",
                  "regular_price", "selling_price", "current_price",
                  "priceValue", "finalPrice", "base_price"):
            v = d.get(k)
            if v is not None:
                try:
                    pv = re.sub(r"[^\d.]", "", str(v)) if isinstance(v, str) else v
                    price = float(pv)
                    if price > 0:
                        break
                except (ValueError, TypeError):
                    pass
        if not price or price <= 0:
            return

        key = name.lower().strip()
        if key in seen:
            return
        seen.add(key)

        cat = next(
            (d.get(k, "").strip()
             for k in ("category", "category_name", "categoryName", "type_name")
             if d.get(k)),
            ""
        )
        brand = next(
            (d.get(k, "").strip()
             for k in ("brand", "brand_name", "brandName", "manufacturer")
             if d.get(k)),
            ""
        )
        collected.append({
            "name": name, "price": price,
            "category": cat, "brand": brand, "url": src_url,
        })

    # ── DOM extraction ────────────────────────────────────────────────────────

    def _extract_dom(self, page, url):
        results = []
        try:
            for sel in _SEL_SETS:
                cards = page.query_selector_all(sel["card"])
                if not cards:
                    continue
                for card in cards:
                    name = price_val = None
                    for ns in sel["name"].split(", "):
                        try:
                            el = card.query_selector(ns.strip())
                            if el:
                                t = el.inner_text().strip()
                                if len(t) > 3:
                                    name = t
                                    break
                        except Exception:
                            pass
                    for ps in sel["price"].split(", "):
                        try:
                            el = card.query_selector(ps.strip())
                            if el:
                                pt = el.inner_text().strip()
                                m = re.search(r"([\d,]+(?:\.\d+)?)", pt)
                                if m:
                                    price_val = float(m.group(1).replace(",", ""))
                                    if price_val > 0:
                                        break
                        except Exception:
                            pass
                    if name and price_val:
                        results.append({"name": name, "price": price_val, "url": url})
                if results:
                    break
        except Exception as e:
            self.logger.debug("DOM extraction error: %s", e)
        return results

    # ── Meet-in-middle coordination ────────────────────────────────────────────

    def _get_forward_progress(self) -> int:
        """Parse the running forward-instance log and return its latest category index.

        Returns 0 if the log cannot be read or no progress line is found.
        """
        import re as _re
        import pathlib
        # Always read the *forward* instance log (no .rev suffix)
        log_path = pathlib.Path(f"logs/naheed.{self.city}.log")
        if not log_path.exists():
            return 0
        try:
            # Read only the last 4 KB — progress lines are frequent
            with open(log_path, "r", encoding="utf-8", errors="ignore") as fh:
                fh.seek(0, 2)  # EOF
                size = fh.tell()
                fh.seek(max(0, size - 4096))
                tail = fh.read()
            # Lines look like: [51/156] kids-babies p37
            matches = _re.findall(r"\[(\d+)/156\]", tail)
            if matches:
                return int(matches[-1])
        except Exception:
            pass
        return 0

    # ── Checkpoint helper ─────────────────────────────────────────────────────

    def _save_checkpoint(self, collected: list) -> None:
        """Overwrite a per-chunk checkpoint CSV every 5 categories so partial
        progress is never lost on interruption."""
        if not collected:
            return
        mult = self._CITY_MULT.get(self.city.lower(), 1.0)
        ts   = datetime.now(timezone.utc).isoformat()
        records = [
            self.build_product_record(
                name=rec["name"],
                price=round(rec["price"] * mult, 2),
                category=rec.get("category", ""),
                brand=rec.get("brand", ""),
                product_url=rec.get("url", _BASE),
                scraped_at=ts,
            )
            for rec in collected
        ]
        suffix = f"c{self._cat_range[0]}" if self._cat_range is not None else self._direction
        ckpt = RAW_DIR / f"naheed_{self.city}_{suffix}_ckpt.csv"
        pd.DataFrame(records).to_csv(ckpt, index=False, encoding="utf-8-sig")
        self.logger.info("Checkpoint → %s (%d products)", ckpt.name, len(records))

    # ── Pass 1: category crawl + network interception ─────────────────────────

    def _pass1_categories(self, direction: str = "forward"):
        try:
            from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
        except ImportError:
            self.logger.error("Playwright not installed — skipping Pass 1")
            return [], set()

        collected, seen = [], set()

        def on_response(response):
            try:
                if response.status != 200:
                    return
                ct = response.headers.get("content-type", "")
                if "json" not in ct:
                    return
                raw = response.body()
                if len(raw) < 30 or len(raw) > 20_000_000:
                    return
                before = len(collected)
                self._harvest_json(json.loads(raw), response.url, collected, seen)
                delta = len(collected) - before
                if delta > 0:
                    self.logger.info(
                        "Intercept +%d from %s | total=%d",
                        delta, response.url[:70], len(collected)
                    )
            except Exception:
                pass

        total = len(_CATEGORIES)
        # Build iteration order based on mode
        if self._cat_range is not None:
            start, end = self._cat_range
            chunk_cats = _CATEGORIES[start:end]
            cat_iter   = [(start + j + 1, cat) for j, cat in enumerate(chunk_cats)]
            direction  = "forward"  # chunks always run forward
            self.logger.info(
                "Pass 1: %d categories [CHUNK %d-%d] …", len(cat_iter), start, end
            )
        elif direction == "backward":
            cat_iter = list(enumerate(reversed(_CATEGORIES), 1))
            # Remap so orig_idx reflects position in the ORIGINAL list
            cat_iter = [(total - i + 1, cat) for i, cat in cat_iter]
            self.logger.info(
                "Pass 1: category crawl (%d categories) [BACKWARD] …", total
            )
        else:
            cat_iter = list(enumerate(_CATEGORIES, 1))
            self.logger.info(
                "Pass 1: category crawl (%d categories) …", total
            )
        cats_done = 0  # counter for incremental checkpoint saves

        with sync_playwright() as p:
            browser, ctx = self._make_context(p)
            page = ctx.new_page()
            page.on("response", on_response)

            for i, cat in cat_iter:
                # ── Meet-in-middle: check if forward and backward have converged ──
                if direction == "backward":
                    fwd = self._get_forward_progress()
                    # i is the 1-based original index of this category
                    # stop as soon as we would process a category the
                    # forward runner has already reached or passed
                    if fwd > 0 and i <= fwd:
                        self.logger.info(
                            "[backward] Meeting point reached — "
                            "forward is at %d, backward is at %d. Stopping.",
                            fwd, i,
                        )
                        break
                cat_label = cat.split("/")[-1].replace("-", " ").title()
                pg = 1
                while True:
                    url = (f"{_BASE}/{cat}?p={pg}"
                           if pg > 1 else f"{_BASE}/{cat}")
                    try:
                        self.logger.info(
                            "[%d/%d] %s p%d | collected=%d",
                            i, total, cat, pg, len(collected)
                        )
                        page.goto(url, wait_until="networkidle", timeout=40_000)
                        time.sleep(2)
                        prev = len(collected)
                        for _ in range(6):
                            page.mouse.wheel(0, 2500)
                            time.sleep(0.6)

                        for item in self._extract_dom(page, url):
                            k = item["name"].lower().strip()
                            if k not in seen:
                                seen.add(k)
                                collected.append({
                                    "name": item["name"], "price": item["price"],
                                    "category": cat_label, "brand": "", "url": item["url"],
                                })

                        new_here = len(collected) - prev
                        self.logger.info(
                            "+%d this page | total=%d", new_here, len(collected)
                        )
                        if new_here == 0:
                            break

                        try:
                            nxt = page.query_selector(
                                ".pages-item-next a, [class*='next'] a, "
                                "a[title='Next'], .action.next"
                            )
                            if not nxt:
                                break
                        except Exception:
                            break

                        pg += 1
                        if pg > 50:
                            break
                        time.sleep(0.8)

                    except PWTimeout:
                        self.logger.warning("Timeout: %s p%d", cat, pg)
                        break
                    except Exception as e:
                        self.logger.warning("Error %s p%d: %s", cat, pg, e)
                        break

                # ── Incremental checkpoint every 5 categories ──────────────
                cats_done += 1
                if cats_done % 5 == 0:
                    self._save_checkpoint(collected)

            browser.close()
        # Final checkpoint for any remaining products
        self._save_checkpoint(collected)

        self.logger.info("Pass 1 done: %d products", len(collected))
        return collected, seen

    # ── Pass 2: keyword search crawl ─────────────────────────────────────────

    def _pass2_keywords(self, initial_seen):
        try:
            from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
        except ImportError:
            return []

        collected, seen = [], set(initial_seen)

        def on_response(response):
            try:
                if response.status != 200:
                    return
                if "json" not in response.headers.get("content-type", ""):
                    return
                raw = response.body()
                if len(raw) < 30:
                    return
                self._harvest_json(json.loads(raw), response.url, collected, seen)
            except Exception:
                pass

        self.logger.info(
            "Pass 2: keyword search (%d keywords) …", len(_KEYWORDS)
        )

        with sync_playwright() as p:
            browser, ctx = self._make_context(p)
            page = ctx.new_page()
            page.on("response", on_response)

            for i, kw in enumerate(_KEYWORDS, 1):
                url = (f"{_BASE}/catalogsearch/result/"
                       f"?q={kw.replace(' ', '+')}")
                try:
                    self.logger.info(
                        "[%d/%d] search '%s' | new so far=%d",
                        i, len(_KEYWORDS), kw, len(collected)
                    )
                    page.goto(url, wait_until="networkidle", timeout=30_000)
                    time.sleep(1.5)
                    for _ in range(4):
                        page.mouse.wheel(0, 2500)
                        time.sleep(0.5)

                    for item in self._extract_dom(page, url):
                        k = item["name"].lower().strip()
                        if k not in seen:
                            seen.add(k)
                            collected.append({
                                "name": item["name"], "price": item["price"],
                                "category": kw.title(), "brand": "", "url": item["url"],
                            })
                    time.sleep(0.5)

                except PWTimeout:
                    self.logger.warning("Search timeout: '%s'", kw)
                except Exception as e:
                    self.logger.warning("Search error '%s': %s", kw, e)

            browser.close()

        self.logger.info("Pass 2 done: %d additional products", len(collected))
        return collected

    # ── Pass 3: Magento 2 REST API probe ─────────────────────────────────────

    def _pass3_rest_api(self, initial_seen):
        """Unauthenticated Magento 2 REST product list — fails gracefully."""
        collected, seen = [], set(initial_seen)
        api = f"{_BASE}/rest/V1/products"
        headers = {"Accept": "application/json",
                   "User-Agent": "Mozilla/5.0 Chrome/122"}
        self.logger.info("Pass 3: Magento REST probe …")

        for pg in range(1, 201):
            params = {
                "searchCriteria[pageSize]": 100,
                "searchCriteria[currentPage]": pg,
                "fields": "items[id,sku,name,price,type_id]",
            }
            try:
                r = self.session.get(api, params=params,
                                     headers=headers, timeout=20)
                if r.status_code in (401, 403):
                    self.logger.info("Pass 3: REST requires auth — skipping")
                    break
                if r.status_code != 200:
                    self.logger.info("Pass 3: HTTP %d — stopping", r.status_code)
                    break
                items = r.json().get("items", [])
                if not items:
                    break
                for item in items:
                    name = item.get("name", "").strip()
                    try:
                        price = float(item.get("price", 0) or 0)
                    except (TypeError, ValueError):
                        price = 0.0
                    if not name or price <= 0:
                        continue
                    k = name.lower().strip()
                    if k in seen:
                        continue
                    seen.add(k)
                    collected.append({
                        "name": name, "price": price, "category": "",
                        "brand": "",
                        "url": f"{_BASE}/catalog/product/view/id/{item.get('id','')}",
                    })
                self.logger.info(
                    "Pass 3 p%d: +%d | total=%d",
                    pg, len(items), len(collected)
                )
                if len(items) < 100:
                    break
                time.sleep(0.3)
            except Exception as e:
                self.logger.warning("Pass 3 error p%d: %s", pg, e)
                break

        self.logger.info("Pass 3 done: %d additional products", len(collected))
        return collected

    # ── Main orchestrator ─────────────────────────────────────────────────────

    def scrape(self):
        self.logger.info(
            "NaheedScraper start | city=%s | cats=%d | keywords=%d",
            self.city, len(_CATEGORIES), len(_KEYWORDS)
        )

        # Pass 1 — category crawl + network interception
        products, seen = self._pass1_categories(direction=self._direction)

        # Pass 2 & 3 skipped for chunk runs (categories already divided; speed)
        if self._cat_range is None:
            # Pass 2 — keyword search (always, to maximise coverage)
            for p in self._pass2_keywords(seen):
                seen.add(p["name"].lower().strip())
                products.append(p)

            # Pass 3 — REST API (fast bonus; silently skips if 401)
            for p in self._pass3_rest_api(seen):
                products.append(p)

        self.logger.info(
            "Naheed total unique products: %d | city=%s",
            len(products), self.city
        )

        if not products:
            self.logger.error(
                "Naheed: 0 products scraped — check connectivity "
                "and that www.naheed.pk is reachable."
            )
            self.save_raw([])
            return []

        mult = self._CITY_MULT.get(self.city.lower(), 1.0)
        ts   = datetime.now(timezone.utc).isoformat()

        records = [
            self.build_product_record(
                name=rec["name"],
                price=round(rec["price"] * mult, 2),
                category=rec.get("category", ""),
                brand=rec.get("brand", ""),
                product_url=rec.get("url", _BASE),
                scraped_at=ts,
            )
            for rec in products
        ]

        self.save_raw(records)
        return records


# Compatibility alias
NaheedKarachiScraper = NaheedScraper
