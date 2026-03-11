"""
Comprehensive Imtiaz approach test:
1. Try JS fetch with Referer/Origin headers
2. Try DOM extraction after navigating to category pages
3. Try intercepting API calls made by the React app when clicking on categories
"""
import json, time, re
from pathlib import Path

SESSION_FILE = Path("config/imtiaz_session_karachi.json")
session_state = json.loads(SESSION_FILE.read_text("utf-8"))

_BASE = "https://shop.imtiaz.com.pk"
_REST = 55126
_BR   = 54934
_DT   = 0

from playwright.sync_api import sync_playwright

with sync_playwright() as pw:
    br  = pw.chromium.launch(headless=True)
    ctx = br.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        storage_state=session_state,
        viewport={"width": 1440, "height": 900},
    )
    page = ctx.new_page()

    sections = []
    intercepted_items = []

    def on_resp(r):
        try:
            if r.status != 200:
                return
            u = r.url
            if "menu-section" in u and not sections:
                d = r.json()
                sections.extend(d.get("data") or [])
            elif "sub-section" in u or "items-by-subsection" in u:
                print(f"  [INTERCEPT HIT] {u[:100]}")
                d = r.json()
                intercepted_items.append({"url": u, "data": d})
        except:
            pass
    page.on("response", on_resp)

    # Load homepage
    print("Loading homepage...")
    try:
        page.goto(_BASE, wait_until="networkidle", timeout=60_000)
    except: pass
    time.sleep(2)
    print(f"Sections intercepted: {len(sections)}")

    # -------------------------------------------------------------------------
    # TEST 1: JS fetch with Referer header
    # -------------------------------------------------------------------------
    print("\n=== TEST 1: JS fetch with Referer header ===")
    if sections:
        sec_id = sections[0].get("id")
        path = f"/api/sub-section?restId={_REST}&rest_brId={_BR}&sectionId={sec_id}&delivery_type={_DT}&source="
        r = page.evaluate("""async (args) => {
            try {
                const r = await fetch(args.path, {
                    credentials: 'include',
                    headers: {
                        'Accept': 'application/json',
                        'Referer': 'https://shop.imtiaz.com.pk/',
                        'Origin': 'https://shop.imtiaz.com.pk',
                        'X-Requested-With': 'XMLHttpRequest',
                    }
                });
                const txt = await r.text();
                return {status: r.status, body: txt.substring(0, 200)};
            } catch(e) { return {error: String(e)}; }
        }""", {"path": path})
        print(f"  With Referer: {r}")

    # -------------------------------------------------------------------------
    # TEST 2: Navigate to category page + wait for DOM products
    # -------------------------------------------------------------------------
    print("\n=== TEST 2: DOM extraction from /shop/fresh ===")
    try:
        page.goto(f"{_BASE}/shop/fresh", wait_until="networkidle", timeout=30_000)
    except: pass
    time.sleep(3)

    # Try various product card selectors
    selectors = [
        "div[id^='product-item-']",
        "[data-testid='product-card']",
        ".product-card",
        "div[class*='ProductCard']",
        "div[class*='product-item']",
        "div[class*='MenuItemCard']",
        "div[class*='menu-item']",
        "article",
    ]
    for sel in selectors:
        els = page.query_selector_all(sel)
        if els:
            print(f"  Found {len(els)} elements with: {sel}")
            # Check first element
            first = els[0]
            txt = first.inner_text()[:100].replace('\n', ' ')
            print(f"    First text: {txt}")
            inner_html = first.inner_html()[:200]
            print(f"    HTML snippet: {inner_html[:150]}")
            break
    else:
        print("  No product cards found with any selector")
        # Print a body snippet to see what's there
        body_text = page.inner_text("body")[:500].replace('\n', ' ')[:300]
        print(f"  Page body text: {body_text}")

    # Check __NEXT_DATA__ WITH session
    nd = page.evaluate("() => { const el = document.getElementById('__NEXT_DATA__'); return el ? el.textContent : null; }")
    if nd:
        nd_obj = json.loads(nd)
        pp = nd_obj.get("props", {}).get("pageProps", {})
        print(f"  __NEXT_DATA__ pageProps keys: {list(pp.keys())}")
        for k, v in pp.items():
            if isinstance(v, (list, dict)):
                cnt = len(v)
                print(f"    {k}: {type(v).__name__}[{cnt}]", end="")
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    print(f" first keys: {list(v[0].keys())[:5]}")
                else:
                    print()
    else:
        print("  No __NEXT_DATA__")

    # Sub-section links?
    links = page.query_selector_all("a[href*='/shop/']")
    shop_links = set()
    for a in links:
        href = a.get_attribute("href") or ""
        if href.count("/") >= 3:  # /shop/section/subsection
            shop_links.add(href)
    print(f"\n  Sub-section links on page: {len(shop_links)}")
    for l in list(shop_links)[:5]:
        print(f"    {l}")

    # -------------------------------------------------------------------------
    # TEST 3: Navigate to actual sub-section page (deeper URL)
    # -------------------------------------------------------------------------
    if shop_links:
        sub_url = sorted(shop_links)[0]
        print(f"\n=== TEST 3: DOM extraction from sub-section page: {sub_url} ===")
        try:
            page.goto(f"{_BASE}{sub_url}", wait_until="networkidle", timeout=30_000)
        except: pass
        time.sleep(3)

        for sel in ["div[id^='product-item-']", "div[class*='ProductCard']",
                    "div[class*='MenuItemCard']", "[data-testid='product-card']"]:
            els = page.query_selector_all(sel)
            if els:
                print(f"  Found {len(els)} elements: {sel}")
                for i, el in enumerate(els[:3]):
                    eid = el.get_attribute("id") or ""
                    # Try to get name and price
                    try:
                        name_el = el.query_selector("h2,h3,[class*='name'],[class*='title'],[class*='Name'],[class*='Title']")
                        price_el = el.query_selector("[class*='price'],[class*='Price']")
                        name_txt = name_el.inner_text() if name_el else "?"
                        price_txt = price_el.inner_text() if price_el else "?"
                        print(f"    [{i}] id={eid} name={name_txt[:40]} price={price_txt[:20]}")
                    except:
                        pass
                break
        else:
            print("  No product cards")
            body = page.inner_text("body")[:400].replace('\n',' ')
            print(f"  Body: {body[:300]}")

    # -------------------------------------------------------------------------
    # TEST 4: Check if any sub-section API calls were intercepted during nav
    # -------------------------------------------------------------------------
    print(f"\n=== TEST 4: Intercepted API calls ===")
    print(f"  sub-section / items calls intercepted: {len(intercepted_items)}")
    for item in intercepted_items[:3]:
        print(f"  {item['url'][:100]}")

    br.close()

print("\nDone.")
