"""
Headless=False Imtiaz scraper debug.
Watch the browser to see where it gets stuck.
"""
import json, time
from pathlib import Path

CONFIG_FILE = Path("config/imtiaz_location_karachi.json")
ls_cache = json.loads(CONFIG_FILE.read_text("utf-8"))

_BASE = "https://shop.imtiaz.com.pk"
_REST = 55126
_BR   = 54934
_DT   = 0

ls_json = json.dumps(ls_cache)
init_script = (
    f"(function(ls){{"
    f"for(var k in ls){{try{{localStorage.setItem(k,ls[k]);}}catch(e){{}}}}"
    f"}})({ls_json})"
)

from playwright.sync_api import sync_playwright

print("Opening VISIBLE browser. Watch what happens...")
print("The browser will:")
print("  1. Load homepage with your stored location injected")
print("  2. Attempt to fetch sub-sections via JS fetch")
print("  3. Show you where it fails\n")

with sync_playwright() as pw:
    br  = pw.chromium.launch(headless=False, slow_mo=500)   # VISIBLE + slow
    ctx = br.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        viewport={"width": 1440, "height": 900},
    )
    ctx.add_init_script(init_script)
    page = ctx.new_page()

    sections = []
    def on_resp(r):
        if r.status == 200 and "menu-section" in r.url:
            try:
                d = r.json()
                secs = d.get("data") or []
                if secs and not sections:
                    sections.extend(secs)
                    print(f"[INTERCEPT] menu-section -> {len(secs)} sections captured")
            except: pass
    page.on("response", on_resp)

    # Step 1: Load homepage
    print("Step 1: Loading homepage...")
    try:
        page.goto(_BASE, wait_until="networkidle", timeout=60_000)
    except Exception as e:
        print(f"  Homepage error: {e}")
    time.sleep(2)

    ls_check = page.evaluate("() => localStorage.getItem('stored_location')")
    print(f"  stored_location in LS: {ls_check}")
    print(f"  Sections intercepted: {len(sections)}")

    if sections:
        sec = sections[0]
        sec_id = sec.get("id")
        print(f"\nStep 2: Trying JS fetch for sub-section (id={sec_id})...")
        
        path = f"/api/sub-section?restId={_REST}&rest_brId={_BR}&sectionId={sec_id}&delivery_type={_DT}&source="
        result = page.evaluate(
            """async (path) => {
                const r = await fetch(path, {headers: {'Accept': 'application/json'}});
                const txt = await r.text();
                return {status: r.status, body: txt.substring(0, 200)};
            }""",
            path
        )
        print(f"  sub-section result: {result}")

        if result.get("status") == 400:
            print("\n  *** API RETURNED 400 - NEED SESSION COOKIES ***")
            print("  The API requires cookies set by the Imtiaz server.")
            print("  See what cookies are currently in the browser:")
            cookies = ctx.cookies()
            print(f"  Current cookies: {[(c['name'],c['value']) for c in cookies]}")

    # Step 3: Try navigating to the shop page and check __NEXT_DATA__
    print("\nStep 3: Navigating to /shop/fresh to check page content...")
    try:
        page.goto(f"{_BASE}/shop/fresh", wait_until="domcontentloaded", timeout=30_000)
    except Exception as e:
        print(f"  {e}")
    time.sleep(3)

    nd_raw = page.evaluate("() => { const el = document.getElementById('__NEXT_DATA__'); return el ? el.textContent : null; }")
    if nd_raw:
        nd = json.loads(nd_raw)
        pp = nd.get("props", {}).get("pageProps", {})
        print(f"  __NEXT_DATA__ pageProps keys: {list(pp.keys())}")
        for k, v in pp.items():
            if isinstance(v, list):
                print(f"    {k}: list[{len(v)}]", end="")
                if v and isinstance(v[0], dict):
                    print(f" first keys: {list(v[0].keys())[:6]}")
                else:
                    print()
    else:
        print("  No __NEXT_DATA__ on /shop/fresh")

    print("\nBrowser will stay open for 30 seconds so you can inspect it...")
    print("LOOK AT:")
    print("  - Does the location dialog appear?")
    print("  - Are products visible on the /shop/fresh page?")
    print("  - Press Ctrl+C in terminal to close early")
    time.sleep(30)
    br.close()

print("Done.")
