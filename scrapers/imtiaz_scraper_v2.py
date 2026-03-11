"""
Imtiaz Supermarket Pakistan – API-First Scraper (v2)
=====================================================
Target  : https://shop.imtiaz.com.pk  (Next.js + Material UI)
Strategy: single Playwright session → JS fetch() against internal REST API
           No DOM scraping needed.

REST API (requires active browser session—400 if called directly):
  GET /api/geofence?restId=55126
      → cities with geofences, each geofence has rest_brId
  GET /api/menu-section?restId=55126&rest_brId={brId}&delivery_type=0
      → top-level categories (sections)
  GET /api/sub-section?restId=55126&rest_brId={brId}&sectionId={sectionId}&delivery_type=0
      → sub-categories (sub-sections)
  GET /api/items-by-subsection?restId=55126&rest_brId={brId}&sub_section_id={subId}&delivery_type=0[&page=N&limit=100]
      → product list with price, brand, image, barcode etc.

Location bypass:
  Inject full localStorage (captured once via visible browser dialog → saved to
  config/imtiaz_location_{city}.json).  If no cache exists, run the dialog flow
  headlessly and build + save a cache from the geofence data.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from config.settings import STORES
from scrapers.base_scraper import BaseScraper
from utils.helpers import normalize_unit, parse_price, compute_price_per_unit, clean_text

_CFG   = STORES["imtiaz"]
_BASE  = "https://shop.imtiaz.com.pk"
_REST  = 55126     # Imtiaz restId (constant)
_DT    = 0         # delivery_type: 0 = standard, 1 = express

_CONFIG_DIR = Path(__file__).parent.parent / "config"

# City → area hint used when running the dialog the first time
_AREA_HINTS: dict[str, str] = {
    "karachi":   "Gulshan",
    "lahore":    "DHA",
    "islamabad": "F-6",
    "faisalabad": "Susan",
    "multan":    "New Multan",
    "hyderabad": "Latifabad",
    "quetta":    "Satellite",
}


class ImtiazScraper(BaseScraper):
    """
    Scraper for Imtiaz Supermarket (shop.imtiaz.com.pk).

    Uses a single Playwright browser session:
      1. Injects saved localStorage (so dialog/location is pre-satisfied)
      2. Loads homepage to initialise session (branch/splash APIs fire)
      3. Calls all internal REST APIs via JS fetch() inside the browser
      4. Parses product JSON directly — no HTML selectors needed
    """

    store_name = "imtiaz"
    base_url   = _BASE

    def __init__(self, city: str = "karachi", max_workers: int = 4) -> None:
        super().__init__(city=city, max_workers=max_workers, rate_calls=10, rate_period=1.0)
        self._ls_cache: dict  = self._load_ls_cache()
        self._br_id:    int   = 0
        self._geo_data: dict  = {}   # full geofence response

    # =========================================================================
    # Entry point
    # =========================================================================

    def scrape(self) -> list[dict[str, Any]]:
        self.logger.info("[Imtiaz/%s] ▶ Starting scrape …", self.city)
        records = self._playwright_scrape()
        self.logger.info("[Imtiaz/%s] ■ Done – %d total records", self.city, len(records))
        if records:
            self.save_raw(records)
        return records

    # =========================================================================
    # Core: single Playwright session
    # =========================================================================

    def _playwright_scrape(self) -> list[dict]:
        try:
            from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
        except ImportError:
            self.logger.error("[Imtiaz] playwright not installed — pip install playwright")
            return []

        with sync_playwright() as pw:
            br  = pw.chromium.launch(headless=True)
            ctx = br.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1440, "height": 900},
            )

            # Inject saved location so dialog never appears
            if self._ls_cache:
                ctx.add_init_script(self._make_init_script(self._ls_cache))

            page = ctx.new_page()

            # Intercept geofence (only fires on first load without location)
            def on_resp(r):
                try:
                    if r.status == 200 and "json" in r.headers.get("content-type", ""):
                        if "geofence" in r.url:
                            d = r.json()
                            if d.get("status") == 200:
                                self._geo_data = d.get("data", {})
                except Exception:
                    pass
            page.on("response", on_resp)

            # Load homepage → establishes session (branch / splash APIs fire)
            self.logger.info("[Imtiaz] Loading homepage to establish session …")
            try:
                page.goto(_BASE, wait_until="networkidle", timeout=60_000)
            except PWTimeout:
                pass
            time.sleep(2)

            # First time: no cache → handle dialog → build + save cache
            if not self._ls_cache:
                self.logger.info("[Imtiaz] No location cache — running dialog …")
                if self._handle_location_dialog(page):
                    ls_fresh = page.evaluate("() => ({...localStorage})")
                    if ls_fresh.get("stored_location"):
                        self._ls_cache = ls_fresh
                        self._save_ls_cache(ls_fresh)
                time.sleep(2)

            # Extract branch ID from localStorage
            self._br_id = self._get_br_id(page)
            if not self._br_id:
                self.logger.error("[Imtiaz] Could not determine branch ID — aborting")
                br.close()
                return []
            self.logger.info("[Imtiaz] branch id = %d", self._br_id)

            # Fetch all sections → sub-sections → products
            all_records = self._fetch_all_products(page)

            br.close()

        return all_records

    # =========================================================================
    # API fetch helpers (all run inside the browser via page.evaluate)
    # =========================================================================

    def _js_get(self, page, path: str) -> dict | list:
        """Call fetch(path) inside the Playwright browser; return parsed JSON."""
        try:
            result = page.evaluate(
                f"""async () => {{
                    try {{
                        const r = await fetch('{path}', {{
                            headers: {{
                                'Accept': 'application/json',
                                'X-Requested-With': 'XMLHttpRequest',
                            }}
                        }});
                        return r.json();
                    }} catch(e) {{ return {{error: e.toString()}}; }}
                }}"""
            )
            return result or {}
        except Exception as exc:
            self.logger.debug("[Imtiaz] js_get error %s: %s", path[:60], exc)
            return {}

    def _fetch_all_products(self, page) -> list[dict]:
        """Fetch menu → sub-sections → items for every section."""
        menu_url   = (
            f"/api/menu-section?restId={_REST}"
            f"&rest_brId={self._br_id}&delivery_type={_DT}"
        )
        menu_data  = self._js_get(page, menu_url)
        sections   = menu_data.get("data", []) if isinstance(menu_data, dict) else []

        if not sections:
            self.logger.warning("[Imtiaz] No sections returned from menu-section API")
            return []

        self.logger.info("[Imtiaz] Found %d sections", len(sections))
        all_records: list[dict] = []

        for section in sections:
            sec_id   = section.get("id") or section.get("section_id")
            sec_name = clean_text(str(section.get("name") or section.get("section_name") or ""))
            if not sec_id or not sec_name:
                continue

            sub_url  = (
                f"/api/sub-section?restId={_REST}"
                f"&rest_brId={self._br_id}&sectionId={sec_id}&delivery_type={_DT}"
            )
            sub_data = self._js_get(page, sub_url)
            subs     = sub_data.get("data", []) if isinstance(sub_data, dict) else []

            sec_total = 0
            for sub in subs:
                sub_id   = sub.get("id") or sub.get("section_id")
                sub_name = clean_text(str(sub.get("name") or sub.get("section_name") or ""))
                if not sub_id:
                    continue

                items = self._fetch_items(page, sub_id)
                for item in items:
                    rec = self._parse_product(item, sec_name, sub_name)
                    if rec:
                        all_records.append(rec)
                        sec_total += 1
                time.sleep(0.1)  # be polite

            self.logger.info(
                "[Imtiaz] %-25s %d sub-sections → %d products",
                sec_name, len(subs), sec_total,
            )

        return all_records

    def _fetch_items(self, page, sub_id: int, limit: int = 200) -> list[dict]:
        """Fetch all products for one sub-section (with simple pagination)."""
        all_items: list[dict] = []
        page_num = 1

        while True:
            url = (
                f"/api/items-by-subsection?restId={_REST}"
                f"&rest_brId={self._br_id}&sub_section_id={sub_id}"
                f"&delivery_type={_DT}&source=&page={page_num}&limit={limit}"
            )
            data  = self._js_get(page, url)
            if not isinstance(data, dict):
                break
            inner  = data.get("data") or {}
            items  = (
                inner.get("items")
                or inner.get("products")
                or (inner if isinstance(inner, list) else [])
            )
            if not isinstance(items, list):
                break
            all_items.extend(items)

            # Stop if we got fewer than limit (last page) or no pagination
            total = (
                inner.get("total") if isinstance(inner, dict) else None
            )
            if total and len(all_items) >= int(total):
                break
            if len(items) < limit:
                break
            page_num += 1

        return all_items

    # =========================================================================
    # Product parser
    # =========================================================================

    def _parse_product(self, p: dict, section: str, subsection: str) -> dict | None:
        try:
            name   = clean_text(str(p.get("name") or p.get("dish_name") or ""))
            if not name:
                return None

            # Price
            price     = parse_price(str(p.get("price") or 0))
            base_px   = parse_price(str(p.get("base_price") or 0))
            disc_px   = parse_price(str(p.get("discount_price") or 0))
            sale_price = disc_px if disc_px and disc_px < price else (
                         base_px if base_px and base_px < price else None)

            # Skip zero-price sub-category entries (safety)
            # (Real products always have a price)
            if price <= 0:
                return None

            # IDs / Barcode
            pid     = str(p.get("id") or "")
            barcode = str(p.get("tp_product_code") or p.get("barcode") or "")

            # Image — prefer high-res from S3
            img = str(p.get("img_url") or p.get("image") or "")

            # Product URL — construct from slug or product ID
            slug     = p.get("slug") or ""
            if slug:
                prod_url = f"{_BASE}/catalog/{slug}"
            elif pid:
                prod_url = f"{_BASE}/product/{pid}"
            else:
                prod_url = ""

            # Quantity / unit from name
            qty, unit = normalize_unit(name)
            ppu       = compute_price_per_unit(price, qty, unit)

            # Stock
            avail    = p.get("availability")
            in_stock = bool(avail) if avail is not None else True

            return self.build_product_record(
                product_id     = pid,
                name           = name,
                brand          = clean_text(str(p.get("brand_name") or "")),
                category       = section,
                subcategory    = subsection,
                price          = price,
                sale_price     = sale_price,
                quantity       = qty,
                unit           = unit,
                price_per_unit = ppu,
                in_stock       = in_stock,
                image_url      = img,
                product_url    = prod_url,
                barcode        = barcode,
                description    = clean_text(str(p.get("desc") or "")),
            )
        except Exception as exc:
            self.logger.debug("[Imtiaz] parse error: %s", exc)
            return None

    # =========================================================================
    # Location helpers
    # =========================================================================

    def _get_br_id(self, page) -> int:
        """Read rest_brId / currentBranchId from page's localStorage."""
        try:
            stored = page.evaluate("() => localStorage.getItem('stored_location')")
            if stored and str(stored).isdigit():
                return int(stored)
        except Exception:
            pass
        # Fallback: parse persist:root
        try:
            pr  = page.evaluate("() => localStorage.getItem('persist:root')")
            if pr:
                outer  = json.loads(pr)
                state  = json.loads(outer.get("state", "{}"))
                br_id  = state.get("currentBranchId")
                if br_id:
                    return int(br_id)
        except Exception:
            pass
        return 0

    def _handle_location_dialog(self, page) -> bool:
        """
        Dismiss the 'Select your order type / location' MUI Dialog.

        Dialog HTML (from user-provided HTML):
          • Order type tabs:  button[role='tab']  text 'Express' / 'Standard'
          • City combobox:    input[placeholder='Select City / Region']  (readonly)
            → Click the input to open the listbox, then click the city option.
          • Area combobox:    input[placeholder='Select Area / Sub Region']
            → Type area hint to filter, click first option.
          • Confirm button:   button.MuiButton-root:has-text('Select')
        """
        try:
            from playwright.sync_api import TimeoutError as PWTimeout
            page.wait_for_selector("[role='dialog']", timeout=7_000)
        except Exception:
            return False  # No dialog

        self.logger.info("[Imtiaz] Location dialog detected — configuring '%s'", self.city)
        try:
            # 1. Click Express tab (aria-selected=true means it might already be selected)
            for tab_text in ["Express", "EXPRESS"]:
                try:
                    tab = page.query_selector(f"button[role='tab']:has-text('{tab_text}')")
                    if tab and tab.is_visible():
                        tab.click()
                        time.sleep(0.5)
                        break
                except Exception:
                    pass

            # 2. City combobox — readonly input, so click to open listbox
            try:
                from playwright.sync_api import TimeoutError as PWTimeout
                city_inp = page.wait_for_selector(
                    "input[placeholder='Select City / Region']",
                    timeout=6_000, state="visible"
                )
                city_inp.click()  # opens dropdown
                time.sleep(1)

                # Try to find city in listbox options
                city_clicked = False
                for sel in ["[role='listbox'] [role='option']", "[role='option']", "li[class*='MuiAutocomplete']"]:
                    opts = page.query_selector_all(sel)
                    for opt in opts:
                        try:
                            if opt.is_visible() and self.city.lower() in opt.inner_text().lower():
                                opt.click()
                                city_clicked = True
                                time.sleep(0.8)
                                break
                        except Exception:
                            pass
                    if city_clicked:
                        break

                # If no listbox: try typing (Playwright type() bypasses readonly)
                if not city_clicked:
                    city_inp.type(self.city.title(), delay=80)
                    time.sleep(1)
                    # Try listbox again
                    for sel in ["[role='option']", "li"]:
                        for li in page.query_selector_all(sel):
                            try:
                                if li.is_visible() and self.city.lower() in li.inner_text().lower():
                                    li.click()
                                    city_clicked = True
                                    time.sleep(0.8)
                                    break
                            except Exception:
                                pass
                        if city_clicked:
                            break
                    if not city_clicked:
                        city_inp.press("Enter")
                        time.sleep(0.8)
            except Exception as e:
                self.logger.debug("[Imtiaz] city input error: %s", e)

            # 3. Area combobox — not readonly, can type
            try:
                from playwright.sync_api import TimeoutError as PWTimeout
                area_inp = page.wait_for_selector(
                    "input[placeholder='Select Area / Sub Region']",
                    timeout=5_000, state="visible"
                )
                hint = _AREA_HINTS.get(self.city.lower(), self.city.title())
                area_inp.click()
                area_inp.type(hint, delay=80)
                time.sleep(1.5)

                # Click first visible suggestion
                for sel in ["[role='option']", "li[class*='MuiAutocomplete']", "li"]:
                    for opt in page.query_selector_all(sel):
                        try:
                            txt = opt.inner_text().strip()
                            if opt.is_visible() and txt:
                                opt.click()
                                time.sleep(0.8)
                                break
                        except Exception:
                            pass
                    else:
                        continue
                    break
            except Exception as e:
                self.logger.debug("[Imtiaz] area input error: %s", e)

            # 4. Click 'Select' button
            for text in ["Select", "Confirm", "Continue", "SUBMIT"]:
                try:
                    btn = page.query_selector(
                        f"button.MuiButton-root:has-text('{text}'), "
                        f"button[class*='MuiButton']:has-text('{text}')"
                    )
                    if btn and btn.is_visible() and btn.is_enabled():
                        btn.click()
                        time.sleep(2.5)
                        break
                except Exception:
                    pass

            # Wait for dialog to close
            try:
                from playwright.sync_api import TimeoutError as PWTimeout
                page.wait_for_selector("[role='dialog']", state="hidden", timeout=6_000)
                self.logger.info("[Imtiaz] Location dialog dismissed ✓")
                return True
            except Exception:
                # Check if dialog is still visible
                dlg = page.query_selector("[role='dialog']")
                if dlg and dlg.is_visible():
                    self.logger.warning("[Imtiaz] Dialog may not have closed")
                    return False
                return True  # might have already closed

        except Exception as exc:
            self.logger.warning("[Imtiaz] Location dialog error: %s", exc)
            return False

    # =========================================================================
    # localStorage cache
    # =========================================================================

    @staticmethod
    def _make_init_script(ls: dict) -> str:
        ls_json = json.dumps(ls)
        return (
            f"(function(ls){{"
            f"for(var k in ls){{try{{localStorage.setItem(k,ls[k]);}}catch(e){{}}}}"
            f"}})({ls_json})"
        )

    def _cache_path(self) -> Path:
        return _CONFIG_DIR / f"imtiaz_location_{self.city}.json"

    def _load_ls_cache(self) -> dict:
        p = self._cache_path()
        if p.exists():
            try:
                return json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                pass
        return {}

    def _save_ls_cache(self, ls: dict) -> None:
        try:
            _CONFIG_DIR.mkdir(parents=True, exist_ok=True)
            self._cache_path().write_text(json.dumps(ls, indent=2), encoding="utf-8")
            self.logger.info("[Imtiaz] Location cache saved → %s", self._cache_path().name)
        except Exception as exc:
            self.logger.warning("[Imtiaz] Could not save cache: %s", exc)
