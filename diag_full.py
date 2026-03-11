"""Imtiaz – get all cities + inspect real product page structure"""
import json, time, requests
from playwright.sync_api import sync_playwright

BASE  = "https://shop.imtiaz.com.pk"
CACHE = r"D:\data_science_market\config\imtiaz_location_karachi.json"
ls_data = json.loads(open(CACHE).read())

# 1. Get all cities from geofence API (must be from browser context w/ restId)
with sync_playwright() as pw:
    br  = pw.chromium.launch(headless=True)
    ctx = br.new_context(viewport={"width":1440,"height":900})
    ls_json = json.dumps(ls_data)
    ctx.add_init_script(f"(function(ls){{for(var k in ls){{try{{localStorage.setItem(k,ls[k]);}}catch(e){{}}}}}})(JSON.parse({ls_json}))")
    pg  = ctx.new_page()

    cities_data = {}
    products_api = []
    def on_resp(r):
        try:
            if r.status==200 and 'json' in r.headers.get('content-type',''):
                u = r.url
                d = r.json()
                if 'geofence' in u:
                    cities_data.update(d)
                if isinstance(d, dict) and ('products' in str(d)[:100].lower() or 'items' in d):
                    products_api.append({'url': u[:120], 'keys': list(d.keys())[:10], 'data': d})
                elif isinstance(d, list) and d and isinstance(d[0], dict):
                    if any(k in d[0] for k in ('price','name','sku','barcode','image')):
                        products_api.append({'url': u[:120], 'type': 'list', 'sample': d[0]})
        except:
            pass
    pg.on("response", on_resp)

    pg.goto(BASE, wait_until="domcontentloaded", timeout=30000)
    time.sleep(2)

    if cities_data:
        print("=== CITIES ===")
        for city in cities_data.get("data", {}).get("cities", []):
            print(f"  {city['name']} (id={city['id']})")

    # Now visit a leaf category to see real product data
    LEAF = f"{BASE}/catalog/home-care-4097/laundry-40573"
    print(f"\nVisiting leaf: {LEAF}")
    pg.goto(LEAF, wait_until="networkidle", timeout=30000)
    time.sleep(3)

    # Try waiting for products explicitly
    try:
        pg.wait_for_selector("div[id^='product-item-']", timeout=10000)
        print("Products found with explicit wait!")
    except:
        print("Products NOT found with explicit wait")

    products = pg.query_selector_all("div[id^='product-item-']")
    print(f"Product items: {len(products)}")

    if products:
        # Print first product HTML
        print("\nSample product HTML:")
        print(products[0].inner_html()[:2000])

        # Check all links on the product card
        links = products[0].query_selector_all("a")
        print(f"\nLinks in first product card: {len(links)}")
        for a in links:
            print(f"  href={a.get_attribute('href')!r}")

    # Check all product API calls on this page
    print(f"\nProduct API calls: {len(products_api)}")
    for p in products_api[:5]:
        print(f"  {p.get('url')}")
        print(f"  keys: {p.get('keys') or list(p.get('sample',{}).keys())[:8]}")

    # Check what next data is available
    nd = pg.evaluate("() => { try { return JSON.stringify(window.__NEXT_DATA__) } catch(e) { return '' }}")
    if nd:
        obj = json.loads(nd)
        s = json.dumps(obj)
        print(f"\n__NEXT_DATA__ size: {len(s)}")
        # Find product arrays
        def find_arrays(o, path="", depth=0):
            if depth > 8: return
            if isinstance(o, list) and len(o) > 2 and isinstance(o[0], dict):
                keys = list(o[0].keys())[:6]
                print(f"  Array[{len(o)}] at {path}: keys={keys}")
            elif isinstance(o, dict):
                for k, v in o.items():
                    find_arrays(v, f"{path}.{k}", depth+1)
        find_arrays(obj)

    pg.screenshot(path=r"D:\data_science_market\logs\imtiaz_leaf_test.png")
    br.close()

print("\nDone.")
