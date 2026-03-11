"""Quick check: does __NEXT_DATA__ on category page contain product data?"""
import json, time
from pathlib import Path

ls_cache = json.loads(Path("config/imtiaz_location_karachi.json").read_text("utf-8"))
ls_json = json.dumps(ls_cache)
init_script = f"(function(ls){{for(var k in ls){{try{{localStorage.setItem(k,ls[k]);}}catch(e){{}}}}}})(  {ls_json})"

from playwright.sync_api import sync_playwright

with sync_playwright() as pw:
    br  = pw.chromium.launch(headless=True)
    ctx = br.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    ctx.add_init_script(init_script)
    page = ctx.new_page()

    # Go directly to a known sub-category page (no homepage first)
    print("Loading /shop/fresh directly...")
    try:
        page.goto("https://shop.imtiaz.com.pk/shop/fresh", wait_until="domcontentloaded", timeout=30_000)
    except Exception as e:
        print(f"  {e}")
    time.sleep(3)

    nd_raw = page.evaluate("() => { const el = document.getElementById('__NEXT_DATA__'); return el ? el.textContent : null; }")
    if nd_raw:
        nd = json.loads(nd_raw)
        pp = nd.get("props", {}).get("pageProps", {})
        print(f"pageProps keys: {list(pp.keys())}")
        for k, v in pp.items():
            if isinstance(v, list):
                print(f"  {k}: list[{len(v)}]", end="")
                if v and isinstance(v[0], dict):
                    print(f" -> first keys: {list(v[0].keys())[:8]}")
                else:
                    print()
            elif isinstance(v, dict):
                print(f"  {k}: dict{list(v.keys())[:8]}")
    else:
        print("NO __NEXT_DATA__")
        # Check cookies
        cks = ctx.cookies()
        print(f"Cookies: {[(c['name'],c['value'][:30]) for c in cks]}")

    br.close()
print("Done.")
