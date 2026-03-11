"""
Next.js client-side navigation trick:
When you click a link, Next.js fetches /_next/data/{buildId}/shop/fresh.json
That JSON contains the full pageProps INCLUDING products.
"""
import json, time
from pathlib import Path

SESSION_FILE = Path("config/imtiaz_session_karachi.json")
session_state = json.loads(SESSION_FILE.read_text("utf-8"))
_BASE = "https://shop.imtiaz.com.pk"

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
    next_data_responses = []

    def on_resp(r):
        try:
            if r.status != 200:
                return
            u = r.url
            if "menu-section" in u and not sections:
                d = r.json()
                sections.extend(d.get("data") or [])
            # Capture /_next/data/ calls (client-side navigation data)
            if "/_next/data/" in u:
                ctype = r.headers.get("content-type", "")
                if "json" in ctype:
                    d = r.json()
                    next_data_responses.append({"url": u, "data": d})
                    print(f"  [_next/data] {u.split('/')[-1]}: keys={list(d.get('pageProps',{}).keys())}")
        except:
            pass
    page.on("response", on_resp)

    # Load homepage
    print("Loading homepage...")
    try:
        page.goto(_BASE, wait_until="networkidle", timeout=60_000)
    except: pass
    time.sleep(2)
    print(f"Sections: {len(sections)}")

    # Get buildId from __NEXT_DATA__
    build_id = page.evaluate("""() => {
        const el = document.getElementById('__NEXT_DATA__');
        if (!el) return null;
        try { return JSON.parse(el.textContent).buildId; } catch(e) { return null; }
    }""")
    print(f"buildId: {build_id}")

    # -------------------------------------------------------------------------
    # Test 1: Direct fetch of /_next/data/{buildId}/shop/fresh.json
    # -------------------------------------------------------------------------
    if build_id:
        print(f"\n=== Test 1: /_next/data/{build_id}/shop/fresh.json ===")
        next_url = f"/_next/data/{build_id}/shop/fresh.json"
        r = page.evaluate("""async (path) => {
            try {
                const r = await fetch(path, {
                    credentials: 'include',
                    headers: {'Accept': 'application/json', 'x-nextjs-data': '1'}
                });
                const txt = await r.text();
                return {status: r.status, len: txt.length, body: txt.substring(0,300)};
            } catch(e) { return {error: String(e)}; }
        }""", next_url)
        print(f"  result: {r}")

    # -------------------------------------------------------------------------
    # Test 2: CLICK on Fresh category link (triggers client-side nav + /_next/data/)
    # -------------------------------------------------------------------------
    print(f"\n=== Test 2: Click Fresh category link ===")
    fresh_link = page.query_selector("a[href='/shop/fresh'], a[href*='/shop/fresh']")
    if not fresh_link:
        # Try to find any category links
        all_links = page.query_selector_all("a[href*='/shop/']")
        print(f"  /shop/ links found: {len(all_links)}")
        for a in all_links[:5]:
            print(f"    {a.get_attribute('href')}")
        fresh_link = all_links[0] if all_links else None

    if fresh_link:
        href = fresh_link.get_attribute("href")
        print(f"  Clicking: {href}")
        fresh_link.click()
        time.sleep(4)
        print(f"  Current URL: {page.url}")

        # Check DOM for products
        products_found = []
        for sel in ["div[id^='product-item-']", "div[class*='ProductCard']",
                    "div[class*='MenuItemCard']", "div[class*='product']"]:
            els = page.query_selector_all(sel)
            if els:
                print(f"  DOM products [{sel}]: {len(els)}")
                for el in els[:3]:
                    eid = el.get_attribute("id") or ""
                    txt = el.inner_text()[:80].replace('\n', ' ')
                    print(f"    id={eid} | {txt}")
                products_found = els
                break

        if not products_found:
            # Print body to see what rendered
            body = page.inner_text("body")[:600].replace('\n',' ')
            print(f"  Body after click: {body[:400]}")

    # -------------------------------------------------------------------------
    # Test 3: Check /_next/data/ intercepted during navigation
    # -------------------------------------------------------------------------
    print(f"\n=== Test 3: /_next/data/ responses captured ===")
    print(f"  Count: {len(next_data_responses)}")
    for nd in next_data_responses[:3]:
        pp = nd["data"].get("pageProps", {})
        print(f"  {nd['url'].split('/')[-1]}: pageProps={list(pp.keys())}")
        for k, v in pp.items():
            if isinstance(v, list) and v:
                print(f"    {k}: list[{len(v)}] first={list(v[0].keys())[:5] if isinstance(v[0],dict) else v[0]}")

    br.close()

print("\nDone.")
