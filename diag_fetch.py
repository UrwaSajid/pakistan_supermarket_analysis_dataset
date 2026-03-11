"""Quick debug: why JS fetch of menu-section returns empty inside browser"""
import json, time
from playwright.sync_api import sync_playwright

BASE = "https://shop.imtiaz.com.pk"
with open(r"D:\data_science_market\config\imtiaz_location_karachi.json") as f:
    ls_data = json.load(f)

ls_json   = json.dumps(ls_data)
LS_SCRIPT = f"(function(ls){{for(var k in ls){{try{{localStorage.setItem(k,ls[k]);}}catch(e){{}}}}}})(  {ls_json} )"

intercepted = {}
def on_resp(r):
    try:
        if r.status==200 and 'json' in r.headers.get('content-type',''):
            intercepted[r.url] = r.json()
    except: pass

with sync_playwright() as pw:
    br  = pw.chromium.launch(headless=True)
    ctx = br.new_context(viewport={"width":1440,"height":900},
                         user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36")
    ctx.add_init_script(LS_SCRIPT)
    pg  = ctx.new_page()
    pg.on("response", on_resp)

    pg.goto(BASE, wait_until="networkidle", timeout=30000)
    time.sleep(2)

    # What APIs fired?
    print("Intercepted APIs during homepage:")
    for u in intercepted:
        if 'imtiaz' in u and '_next' not in u and 'static' not in u:
            d = intercepted[u]
            print(f"  {u[:100]}")
            if isinstance(d, dict) and 'data' in d:
                dat = d['data']
                if isinstance(dat, list):
                    print(f"    data[{len(dat)}]  first keys: {list(dat[0].keys())[:5] if dat else 'empty'}")
                elif isinstance(dat, dict):
                    print(f"    data keys: {list(dat.keys())[:6]}")

    # Try JS fetch with different approaches
    br_id = pg.evaluate("() => localStorage.getItem('stored_location')")
    print(f"\nbr_id from localStorage: {br_id}")

    # Test 1: with source=
    r1 = pg.evaluate(f"""async () => {{
        const r = await fetch('/api/menu-section?restId=55126&rest_brId={br_id}&delivery_type=0&source=');
        return r.json();
    }}""")
    print(f"\nfetch /api/menu-section (source=): status={r1.get('status')} data_len={len(r1.get('data',[]))}")
    if r1.get('data'):
        print(f"  sections: {[s.get('name') for s in r1['data'][:5]]}")

    # Test 2: absolute URL
    r2 = pg.evaluate(f"""async () => {{
        const r = await fetch('{BASE}/api/menu-section?restId=55126&rest_brId={br_id}&delivery_type=0&source=');
        return r.json();
    }}""")
    print(f"\nfetch absolute url: status={r2.get('status')} data_len={len(r2.get('data',[]))}")

    # Test 3: copy cookies from intercepted request
    cookies = ctx.cookies()
    print(f"\ncookies: {[(c['name'],c['value'][:30]) for c in cookies]}")

    br.close()
