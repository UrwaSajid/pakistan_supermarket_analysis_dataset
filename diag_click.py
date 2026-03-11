"""
1. Inspect homepage DOM for category navigation (divs, not links)
2. Try clicking category items
3. Check home-features API (works client-side)
4. Try sub-section with EXACT headers copied from a menu-section request
"""
import json, time
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
    all_api_calls = {}   # url -> {status, data}

    def on_resp(r):
        try:
            if r.status != 200:
                return
            u = r.url
            if "imtiaz.com.pk/api/" in u:
                d = r.json()
                all_api_calls[u] = d
                if "menu-section" in u and not sections:
                    sections.extend(d.get("data") or [])
        except:
            pass
    page.on("response", on_resp)

    print("Loading homepage...")
    try:
        page.goto(_BASE, wait_until="networkidle", timeout=60_000)
    except: pass
    time.sleep(3)
    print(f"Sections: {len(sections)}")
    print(f"APIs fired: {[u.split('/api/')[1].split('?')[0] for u in all_api_calls]}")

    # -------------------------------------------------------------------------
    # Check home-features API result
    # -------------------------------------------------------------------------
    home_feat_url = next((u for u in all_api_calls if "home-features" in u), None)
    if home_feat_url:
        hf = all_api_calls[home_feat_url]
        features = hf.get("data") or []
        print(f"\nhome-features: {len(features)} items")
        for f in features[:3]:
            print(f"  id={f.get('id')} title={f.get('title')!r} keys={list(f.keys())[:8]}")
            # Does it have nested products?
            for k, v in f.items():
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    print(f"    {k}: list[{len(v)}] first_keys={list(v[0].keys())[:6]}")

    # -------------------------------------------------------------------------
    # Homepage DOM: find category navigation divs
    # -------------------------------------------------------------------------
    print("\n=== Homepage DOM category navigation ===")
    # Look for category items that might be clickable
    cat_selectors = [
        "[class*='categor']",
        "[class*='Category']",
        "[class*='section-item']",
        "[class*='SectionItem']",
        "[class*='menu-item']",
        "[class*='MenuItem']",
        "[class*='nav-item']",
        "[class*='NavItem']",
        "div[role='button']",
        "div[tabindex]",
    ]
    for sel in cat_selectors:
        els = page.query_selector_all(sel)
        if els:
            print(f"  [{sel}]: {len(els)} elements")
            # Show first 3
            for el in els[:3]:
                txt = el.inner_text()[:60].replace('\n', ' ')
                cls = el.get_attribute("class") or ""
                onclick = el.get_attribute("onclick") or ""
                print(f"    text={txt!r} class={cls[:50]}")
            break

    # -------------------------------------------------------------------------
    # Try clicking something that looks like "Fresh" or any food category
    # -------------------------------------------------------------------------
    print("\n=== Try clicking Fresh category ===")
    # Try text-based clicking
    fresh_candidates = []
    for text in ["Fresh", "Bakery", "Snacks", "Pharmacy", "Frozen"]:
        el = page.query_selector(f"text={text}")
        if el:
            fresh_candidates.append((text, el))
            print(f"  Found text element: {text!r}")
            break

    if fresh_candidates:
        text, el = fresh_candidates[0]
        print(f"  Clicking {text!r}...")
        all_api_calls.clear()  # reset to see new calls
        el.click()
        time.sleep(4)
        print(f"  URL after click: {page.url}")
        new_apis = [u.split('/api/')[1].split('?')[0] for u in all_api_calls if "imtiaz.com.pk/api/" in u]
        print(f"  New API calls: {new_apis}")

        # Check for sub-section data
        for u, d in all_api_calls.items():
            if "sub-section" in u:
                subs = d.get("data") or []
                print(f"  SUB-SECTION HIT! {len(subs)} sub-sections from {u[:80]}")
            if "items-by-subsection" in u:
                items_data = d.get("data") or {}
                items = items_data.get("items") if isinstance(items_data, dict) else items_data
                print(f"  ITEMS HIT! {len(items or [])} items from {u[:80]}")

        # Check DOM for products
        for sel in ["div[id^='product-item-']", "div[class*='ProductCard']",
                    "div[class*='MenuItemCard']"]:
            els = page.query_selector_all(sel)
            if els:
                print(f"  DOM products [{sel}]: {len(els)}")
                for el in els[:2]:
                    eid = el.get_attribute("id") or ""
                    txt = el.inner_text()[:80].replace('\n', ' ')
                    print(f"    id={eid} | {txt}")
                break

        # Body snippet
        body = page.inner_text("body")[:500].replace('\n', ' ')
        print(f"  Body: {body[:300]}")
    else:
        print("  No clickable category found")
        # Print all visible text elements
        all_text = page.evaluate("""() => {
            return Array.from(document.querySelectorAll('button, [role=button], nav a, nav li'))
                        .filter(e => e.innerText.trim().length > 0)
                        .map(e => e.innerText.trim().substring(0, 40))
                        .slice(0, 20);
        }""")
        print(f"  Clickable elements: {all_text}")

    # -------------------------------------------------------------------------
    # Try home-features items-by-id if available
    # -------------------------------------------------------------------------
    print("\n=== Check if home-features contains product lists ===")
    hf_slash = f"/api/home-features?restId={_REST}&rest_brId={_BR}&delivery_type={_DT}&source="
    r_hf = page.evaluate("""async (path) => {
        try {
            const r = await fetch(path, {credentials:'include', headers:{'Accept':'application/json'}});
            return await r.json();
        } catch(e) { return {error: String(e)}; }
    }""", hf_slash)
    if isinstance(r_hf, dict):
        features = r_hf.get("data") or []
        print(f"home-features: {len(features)} items")
        total_products = 0
        for f in features[:2]:
            print(f"  feature: {f.get('title')!r} keys={list(f.keys())[:10]}")
            for k, v in f.items():
                if isinstance(v, list) and v and isinstance(v[0], dict) and v[0].get("price"):
                    print(f"    {k}: {len(v)} PRODUCTS! first={v[0].get('name')!r} price={v[0].get('price')}")
                    total_products += len(v)
        print(f"Total products in home-features: {total_products}")

    br.close()

print("\nDone.")
