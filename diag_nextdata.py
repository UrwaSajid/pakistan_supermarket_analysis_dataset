"""
Check if Imtiaz category pages use Next.js SSR with __NEXT_DATA__.
If so, sub-section and items are embedded in the HTML, not fetched as XHR.
"""
import json, time
from pathlib import Path

CONFIG_FILE = Path("config/imtiaz_location_karachi.json")
ls_cache = json.loads(CONFIG_FILE.read_text("utf-8"))

_BASE = "https://shop.imtiaz.com.pk"
_REST = 55126

ls_json = json.dumps(ls_cache)
init_script = (
    f"(function(ls){{"
    f"for(var k in ls){{try{{localStorage.setItem(k,ls[k]);}}catch(e){{}}}}"
    f"}})({ls_json})"
)

from playwright.sync_api import sync_playwright

with sync_playwright() as pw:
    br  = pw.chromium.launch(headless=True)
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
                if not sections:
                    sections.extend(d.get("data") or [])
            except: pass
    page.on("response", on_resp)

    print("Loading homepage...")
    try:
        page.goto(_BASE, wait_until="networkidle", timeout=60_000)
    except: pass
    time.sleep(2)

    print(f"Sections: {len(sections)}")

    # Navigate to Fresh category
    print("\nNavigating to /shop/fresh ...")
    try:
        page.goto(f"{_BASE}/shop/fresh", wait_until="networkidle", timeout=30_000)
    except: pass
    time.sleep(2)

    # Check __NEXT_DATA__
    next_data_raw = page.evaluate("() => { const el = document.getElementById('__NEXT_DATA__'); return el ? el.textContent : null; }")
    
    if next_data_raw:
        nd = json.loads(next_data_raw)
        print(f"\n__NEXT_DATA__ found!")
        # Explore structure
        props = nd.get("props", {})
        page_props = props.get("pageProps", {})
        print(f"pageProps keys: {list(page_props.keys())[:20]}")
        
        # Look for sections/products data
        for k, v in page_props.items():
            if isinstance(v, list):
                print(f"  {k}: list of {len(v)} items")
                if v and isinstance(v[0], dict):
                    print(f"    first item keys: {list(v[0].keys())[:10]}")
            elif isinstance(v, dict):
                print(f"  {k}: dict with keys {list(v.keys())[:10]}")
            else:
                print(f"  {k}: {type(v).__name__} = {str(v)[:80]}")
        
        # Check for sub-sections
        sub_sections = page_props.get("subSections") or page_props.get("sub_sections") or page_props.get("subsections")
        if sub_sections:
            print(f"\nFound sub_sections: {len(sub_sections)}")
            if sub_sections:
                print(f"  First: {sub_sections[0]}")
        
        # Check for products/items
        products = (page_props.get("products") or page_props.get("items") or 
                    page_props.get("menuItems") or page_props.get("dishes"))
        if products:
            print(f"\nFound products: {len(products)}")
            if products:
                print(f"  First: {json.dumps(products[0], indent=2)[:300]}")
    else:
        print("\nNo __NEXT_DATA__ found on the page!")
        # Check page source for any JSON data
        content = page.content()
        if '"subSection"' in content or '"sub_section"' in content:
            print("  Found sub_section in page HTML")
        # Check for items data
        idx = content.find('"items"')
        if idx >= 0:
            print(f"  Found 'items' in HTML at pos {idx}: {content[idx:idx+100]}")

    br.close()

print("\nDone.")
