"""
Imtiaz Supermarket – DOM-Based Scraper (v3)
============================================
Target  : https://shop.imtiaz.com.pk  (React SPA / Next.js custom server)

Architecture (reverse-engineered March 2026):
  The site uses a 3-level catalog URL structure:
    L1 /catalog/{sec_slug}-{sec_id}
    L2 /catalog/{sec_slug}-{sec_id}/{sub_slug}-{sub_id}
    L3 /catalog/{sec_slug}-{sec_id}/{sub_slug}-{sub_id}/{dish_sub_slug}-{dish_sub_id}

  Products are rendered as DOM nodes at L3:
    div[id^="product-item-{id}"]   → product ID
    h4[title]                      → product name
    [class*="price_label"] span    → "Rs. 79.00"

  The REST API (/api/sub-section) fires ONLY when React router navigates to
  an L2 URL — calling it via fetch() always returns 400.  We intercept the
  response to discover dish_sub_sections and their IDs.

Scraping flow:
  1. Sections:       hardcoded from homepage carousel (13 sections)
  2. Subsections:    extracted from DOM <a> hrefs on L1 page
  3. dish_sub_secs:  intercepted from sub-section API call on L2 page
  4. Products:       scraped from DOM on each L3 page
  5. Empty pages:    silently skipped (many dish_sub_sections have no stock)
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any
from urllib.parse import unquote, parse_qs, urlparse

from config.settings import STORES
from scrapers.base_scraper import BaseScraper
from utils.helpers import parse_price, clean_text, normalize_unit, compute_price_per_unit

_BASE       = "https://shop.imtiaz.com.pk"
_CONFIG_DIR = Path(__file__).parent.parent / "config"

# ---------------------------------------------------------------------------
# All 13 top-level sections — from homepage carousel HTML (March 2026)
# ---------------------------------------------------------------------------
_SECTIONS: list[tuple[str, int]] = [
    ("fresh",                 4093),
    ("bakery",                4108),
    ("snacks--confectionary", 4087),
    ("beverages",             4089),
    ("tea--coffee",           4091),
    ("edible-grocery",        4085),
    ("dairy",                 4095),
    ("frozen",                4104),
    ("baby-world",            4107),
    ("health--beauty",        4101),
    ("home-care",             4097),
    ("pet-essentials",        4103),
    ("pharmacy",              4099),
]

# Branch IDs per city  (brId cookie — controls which store's inventory is used)
_BRANCH_IDS: dict[str, int] = {
    "karachi":    54934,
    "lahore":     54935,
    "islamabad":  54936,
    "faisalabad": 54937,
    "multan":     54938,
}


def _slugify(name: str) -> str:
    """Convert display name → URL slug matching Imtiaz URL format.
    e.g. 'Malt Drinks' → 'malt-drinks'
    """
    slug = re.sub(r'[^a-z0-9]+', '-', name.lower())
    return slug.strip('-')


def _decode_next_img(src: str) -> str:
    """Unwrap Next.js /_next/image?url=...&w=... to the real CDN URL."""
    if src and "/_next/image" in src and "url=" in src:
        try:
            qs = parse_qs(urlparse(src).query)
            return unquote(qs["url"][0])
        except Exception:
            pass
    return src or ""


# ===========================================================================
# Scraper
# ===========================================================================

class ImtiazScraper(BaseScraper):
    """
    DOM-based Playwright scraper for shop.imtiaz.com.pk.

    Session setup:
      - Loads config/imtiaz_location_{city}.json as localStorage (pre-sets
        the selected branch / delivery area without showing the popup).
      - Optionally loads config/imtiaz_session_{city}.json as Playwright
        storage_state (cookies captured via capture_session.py).
    """

    store_name = "imtiaz"
    base_url   = _BASE

    def __init__(self, city: str = "karachi", max_workers: int = 1) -> None:
        super().__init__(city=city, max_workers=max_workers, rate_calls=5, rate_period=1.0)
        self._ls_cache: dict = self._load_ls_cache()

    # =========================================================================
    # Public entry point
    # =========================================================================

    def scrape(self) -> list[dict[str, Any]]:
        self.logger.info("[Imtiaz/%s] >> Starting scrape ...", self.city)
        records = self._run()
        self.logger.info("[Imtiaz/%s] Done - %d total records", self.city, len(records))
        if records:
            self.save_raw(records)
        return records

    # =========================================================================
    # LocalStorage / session helpers
    # =========================================================================

    def _load_ls_cache(self) -> dict:
        """Load saved localStorage JSON for this city (fallback to karachi)."""
        for fname in (
            f"imtiaz_location_{self.city}.json",
            "imtiaz_location_karachi.json",
        ):
            p = _CONFIG_DIR / fname
            if p.exists():
                return json.loads(p.read_text(encoding="utf-8"))
        return {}

    def _session_path(self) -> Path | None:
        p = _CONFIG_DIR / f"imtiaz_session_{self.city}.json"
        return p if p.exists() else None

    def _inject_ls_script(self) -> str:
        """Build a Playwright init_script to pre-populate localStorage."""
        lines: list[str] = []
        for k, v in self._ls_cache.items():
            lines.append(f"localStorage.setItem({json.dumps(k)}, {json.dumps(str(v))});")
        return "\n".join(lines)

    # =========================================================================
    # Main Playwright run
    # =========================================================================

    def _run(self) -> list[dict]:
        """Navigate the 3-level catalog tree and collect all product records."""
        from playwright.sync_api import sync_playwright

        records:  list[dict] = []
        seen_ids: set[str]   = set()

        ctx_kwargs: dict[str, Any] = {"viewport": {"width": 1280, "height": 900}}
        sess_path = self._session_path()
        if sess_path:
            ctx_kwargs["storage_state"] = str(sess_path)
            self.logger.info("[Imtiaz/%s] Session state: %s", self.city, sess_path.name)

        ls_script = self._inject_ls_script()

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            ctx     = browser.new_context(**ctx_kwargs)
            if ls_script:
                ctx.add_init_script(ls_script)
            page = ctx.new_page()
            page.set_default_timeout(30_000)

            for sec_slug, sec_id in _SECTIONS:
                sec_name = sec_slug.replace("-", " ").title()
                self.logger.info("[Imtiaz] --- Section: %s ---", sec_name)

                # ── Step 1: get subsection links ───────────────────────────
                try:
                    subsections = self._get_subsections(page, sec_slug, sec_id)
                except Exception as exc:
                    self.logger.warning("[Imtiaz] Section %s failed: %s", sec_name, exc)
                    continue

                if not subsections:
                    self.logger.info("[Imtiaz]   No subsections found for %s", sec_name)
                    continue

                self.logger.info("[Imtiaz]   %d subsections", len(subsections))

                for sub_slug, sub_id in subsections:
                    sub_name = sub_slug.replace("-", " ").title()

                    # ── Step 2: get dish_sub_sections ──────────────────────
                    try:
                        dish_subs = self._get_dish_subs(
                            page, sec_slug, sec_id, sub_slug, sub_id
                        )
                    except Exception as exc:
                        self.logger.warning(
                            "[Imtiaz]   Subsection %s failed: %s", sub_name, exc
                        )
                        continue

                    if not dish_subs:
                        continue

                    # ── Step 3: scrape each dish_sub ───────────────────────
                    for ds_slug, ds_id, ds_name in dish_subs:
                        level3 = (
                            f"{_BASE}/catalog/{sec_slug}-{sec_id}"
                            f"/{sub_slug}-{sub_id}"
                            f"/{ds_slug}-{ds_id}"
                        )
                        try:
                            prods = self._scrape_product_page(
                                page, level3,
                                section=sec_name,
                                subsection=sub_name,
                                dish_sub=ds_name,
                            )
                        except Exception as exc:
                            self.logger.warning(
                                "[Imtiaz]     %s/%s/%s error: %s",
                                sec_name, sub_name, ds_name, exc,
                            )
                            prods = []

                        new_count = 0
                        for rec in prods:
                            pid = rec.get("product_id")
                            if pid and pid not in seen_ids:
                                seen_ids.add(pid)
                                records.append(rec)
                                new_count += 1

                        if new_count:
                            self.logger.info(
                                "[Imtiaz]     %-35s -> %d new  (total=%d)",
                                ds_name[:35], new_count, len(records),
                            )

                        time.sleep(0.4)   # polite delay

            browser.close()

        return records

    # =========================================================================
    # Step 1 — Extract subsection links from a section page
    # =========================================================================

    def _get_subsections(self, page, sec_slug: str, sec_id: int) -> list[tuple[str, int]]:
        """
        Navigate to L1 /catalog/{sec_slug}-{sec_id}.
        Extract all unique <a href> links that go one level deeper.
        Returns list of (sub_slug, sub_id).
        """
        page.goto(
            f"{_BASE}/catalog/{sec_slug}-{sec_id}",
            wait_until="domcontentloaded",
            timeout=30_000,
        )
        page.wait_for_timeout(2_000)

        prefix = f"/catalog/{sec_slug}-{sec_id}/"

        hrefs: list[str] = page.eval_on_selector_all(
            "a[href]",
            f"""(els) => {{
                const prefix = {json.dumps(prefix)};
                const seen   = new Set();
                const out    = [];
                for (const a of els) {{
                    const h = a.getAttribute('href');
                    if (h && h.startsWith(prefix) && h.length > prefix.length && !seen.has(h)) {{
                        seen.add(h);
                        out.push(h);
                    }}
                }}
                return out;
            }}"""
        ) or []

        results: list[tuple[str, int]] = []
        for href in hrefs:
            last = href.rstrip("/").rsplit("/", 1)[-1]   # "carbonated-soft-drinks-40540"
            m = re.match(r'^(.+)-(\d+)$', last)
            if m:
                results.append((m.group(1), int(m.group(2))))
        return results

    # =========================================================================
    # Step 2 — Get dish_sub_sections from the sub-section API (L2 navigation)
    # =========================================================================

    def _get_dish_subs(
        self,
        page,
        sec_slug: str,
        sec_id: int,
        sub_slug: str,
        sub_id: int,
    ) -> list[tuple[str, int, str]]:
        """
        Navigate to L2 /catalog/{sec_slug}-{sec_id}/{sub_slug}-{sub_id}.
        Intercept the sub-section API response to get dish_sub_sections.

        Falls back to using the subsection itself if:
          - API returned nothing
          - But the page already shows product cards (single dish_sub pages)

        Returns list of (dish_sub_slug, dish_sub_id, dish_sub_name).
        """
        captured: list[dict] = []

        def _on_response(resp):
            if "sub-section" in resp.url and resp.status == 200:
                try:
                    data = resp.json()
                    if isinstance(data, list):
                        captured.extend(data)
                    elif isinstance(data, dict):
                        items = (
                            data.get("data")
                            or data.get("dish_sections")
                            or [data]
                        )
                        if isinstance(items, list):
                            captured.extend(items)
                except Exception:
                    pass

        page.on("response", _on_response)
        page.goto(
            f"{_BASE}/catalog/{sec_slug}-{sec_id}/{sub_slug}-{sub_id}",
            wait_until="domcontentloaded",
            timeout=30_000,
        )
        page.wait_for_timeout(2_500)
        page.remove_listener("response", _on_response)

        results: list[tuple[str, int, str]] = []
        seen: set[int] = set()

        for sub in captured:
            for ds in sub.get("dish_sub_sections") or []:
                ds_id   = ds.get("id")
                ds_name = clean_text(str(ds.get("name", "")))
                if not ds_id or not ds_name or ds_id in seen:
                    continue
                seen.add(int(ds_id))
                results.append((_slugify(ds_name), int(ds_id), ds_name))

        # Fallback: if no dish_subs from API but products are already rendered
        if not results:
            count = (
                page.eval_on_selector_all(
                    "div[id^='product-item-']",
                    "els => els.length",
                )
                or 0
            )
            if count:
                fb_name = sub_slug.replace("-", " ").title()
                results.append((sub_slug, sub_id, fb_name))

        return results

    # =========================================================================
    # Step 3 — Scrape product DOM cards from an L3 page
    # =========================================================================

    def _scrape_product_page(
        self,
        page,
        url: str,
        section: str,
        subsection: str,
        dish_sub: str,
    ) -> list[dict]:
        """
        Navigate to url (L3 or fallback L2).
        Extract all product cards, return canonical records.
        Returns [] for empty subcategories (normal — many have no products).
        """
        page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_timeout(2_000)

        raw: list[dict] = page.eval_on_selector_all(
            "div[id^='product-item-']",
            """(els) => els.map(el => {
                const id      = el.id.replace('product-item-', '');
                const nameEl  = el.querySelector('h4');
                const priceEl = el.querySelector(
                    '[class*="product_item_price_label"] span, ' +
                    '[class*="price_label"] span'
                );
                const imgEl   = el.querySelector('img');
                const imgSrc  = imgEl
                    ? (imgEl.src || imgEl.getAttribute('data-src') || '')
                    : '';
                return {
                    id:    id,
                    name:  nameEl  ? (nameEl.getAttribute('title') || nameEl.textContent.trim()) : '',
                    price: priceEl ? priceEl.textContent.trim() : '',
                    img:   imgSrc,
                };
            })"""
        ) or []

        records: list[dict] = []
        for p in raw:
            pid       = str(p.get("id", "")).strip()
            name      = clean_text(str(p.get("name", "")))
            price_str = str(p.get("price", ""))
            img_raw   = str(p.get("img",   ""))

            if not pid or not name:
                continue

            # "Rs. 79.00" → 79.0
            price_num_str = re.sub(r'[Rr]s\.?\s*', '', price_str).replace(",", "").strip()
            price         = parse_price(price_num_str)
            if not price or price <= 0:
                continue

            qty, unit = normalize_unit(name)
            ppu       = compute_price_per_unit(price, qty, unit)
            prod_url  = f"{_BASE}/product/{_slugify(name)}-{pid}"
            img_url   = _decode_next_img(img_raw)

            records.append(self.build_product_record(
                product_id     = pid,
                name           = name,
                category       = section,
                subcategory    = f"{subsection} > {dish_sub}",
                price          = price,
                quantity       = qty,
                unit           = unit,
                price_per_unit = ppu,
                in_stock       = True,
                image_url      = img_url,
                product_url    = prod_url,
            ))

        return records
