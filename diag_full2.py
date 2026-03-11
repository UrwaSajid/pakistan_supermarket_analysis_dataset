"""Imtiaz – single browser, homepage first, then navigate to leaf + capture product APIs"""
import json, time
from playwright.sync_api import sync_playwright

BASE  = "https://shop.imtiaz.com.pk"
with open(r"D:\data_science_market\config\imtiaz_location_karachi.json") as f:
    ls_data = json.load(f)

# Build init script using the same method that worked in the scraper
ls_json = json.dumps(ls_data)
LS_SCRIPT = f"(function(ls){{ for(var k in ls){{ try{{localStorage.setItem(k,ls[k]);}}catch(e){{}} }} }})({ls_json})"

with sync_playwright() as pw:
    br  = pw.chromium.launch(headless=True)
    ctx = br.new_context(
        viewport={"width":1440,"height":900},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"
    )
    ctx.add_init_script(LS_SCRIPT)

    apis = {}
    def on_resp(r):
        try:
            if r.status==200 and 'json' in r.headers.get('content-type',''):
                d = r.json()
                apis[r.url] = d
        except: pass

    pg = ctx.new_page()
    pg.on("response", on_resp)

    # Step 1: homepage to let React hydrate
    print("Loading homepage...")
    pg.goto(BASE, wait_until="networkidle", timeout=30000)
    time.sleep(3)

    # Check localStorage actually got set
    ls_check = pg.evaluate("() => ({...localStorage})")
    print(f"localStorage keys: {list(ls_check.keys())}")

    # Check for dialog
    dialog = pg.query_selector("[role='dialog']")
    print(f"Dialog on homepage: {dialog is not None}")

    # Check catalog links
    cats = pg.query_selector_all("a[href*='/catalog/']")
    print(f"Catalog links on homepage: {len(cats)}")
    for a in cats[:8]:
        print(f"  {a.get_attribute('href')!r}  {a.inner_text().strip()[:25]!r}")

    # Step 2: navigate to a leaf page in same context
    LEAF = f"{BASE}/catalog/home-care-4097/laundry-40573"
    print(f"\nNavigating to leaf: {LEAF}")
    pg.goto(LEAF, wait_until="networkidle", timeout=30000)
    time.sleep(3)

    # Explicit wait for product cards
    try:
        pg.wait_for_selector("div[id^='product-item-']", timeout=8000)
        print("Products appeared!")
    except:
        print("No product cards after wait")

    products = pg.query_selector_all("div[id^='product-item-']")
    print(f"Product items: {len(products)}")

    if products:
        print("\nSample product innerHTML:")
        print(products[0].inner_html()[:3000])

        # All links in first product
        links = products[0].query_selector_all("a")
        print(f"\nLinks in product[0]: {len(links)}")
        for a in links:
            print(f"  href={a.get_attribute('href')!r}  text={a.inner_text().strip()[:30]!r}")
    else:
        pg.screenshot(path=r"D:\data_science_market\logs\leaf_noproducts.png")
        body = pg.evaluate("document.body.innerText")[:500]
        print("Page body:", body)
        # Check dialog
        dialog2 = pg.query_selector("[role='dialog']")
        print(f"Dialog on leaf: {dialog2 is not None}")

    # All API calls
    print(f"\nAPI calls captured: {len(apis)}")
    for u, d in apis.items():
        if 'imtiaz' in u.lower() and '_next/static' not in u:
            keys = list(d.keys())[:6] if isinstance(d, dict) else f"list[{len(d)}]"
            print(f"  {u[:100]}  keys={keys}")

    # Check __NEXT_DATA__
    nd_str = pg.evaluate("() => { try{return JSON.stringify(window.__NEXT_DATA__)}catch(e){return ''} }")
    if nd_str:
        nd = json.loads(nd_str)
        print(f"\n__NEXT_DATA__ top keys: {list(nd.keys())[:8]}")
        pp = nd.get('props', {}).get('pageProps', {})
        print(f"pageProps keys: {list(pp.keys())[:12]}")
        # Look for any array with products
        def search(o, path="", d=0):
            if d>8: return
            if isinstance(o, list) and o and isinstance(o[0], dict):
                keys0 = list(o[0].keys())[:8]
                if any(k in keys0 for k in ['price','name','image','sku','barcode','slug']):
                    print(f"  [{len(o)} items] at {path}: {keys0}")
                    print(f"    sample: {json.dumps(o[0])[:200]}")
            elif isinstance(o, dict):
                for k, v in o.items():
                    search(v, f"{path}.{k}", d+1)
        search(nd)

    br.close()
