"""Imtiaz – dump API responses: cities, menu, sub-section, items"""
import json, time, requests
from playwright.sync_api import sync_playwright

BASE = "https://shop.imtiaz.com.pk"
with open(r"D:\data_science_market\config\imtiaz_location_karachi.json") as f:
    ls_data = json.load(f)

ls_json   = json.dumps(ls_data)
LS_SCRIPT = f"(function(ls){{for(var k in ls){{try{{localStorage.setItem(k,ls[k]);}}catch(e){{}}}}}})(  {ls_json} )"

# Extract branch info from cache
persist = json.loads(json.loads(ls_data["persist:root"])["state"])
branch_id = persist.get("currentBranchId")
city_info  = persist.get("currentCity", {})
REST_ID    = 55126
BR_ID      = branch_id
print(f"restId={REST_ID}  rest_brId={BR_ID}  city={city_info.get('name')}")

apis_captured = {}

with sync_playwright() as pw:
    br  = pw.chromium.launch(headless=True)
    ctx = br.new_context(viewport={"width":1440,"height":900},
                         user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36")
    ctx.add_init_script(LS_SCRIPT)

    def on_resp(r):
        try:
            if r.status==200 and 'json' in r.headers.get('content-type',''):
                d = r.json()
                apis_captured[r.url] = d
        except: pass

    pg = ctx.new_page()
    pg.on("response", on_resp)

    # Load homepage to hydrate session
    pg.goto(BASE, wait_until="networkidle", timeout=30000)
    time.sleep(2)

    # Navigate to a leaf to trigger items API
    pg.goto(f"{BASE}/catalog/home-care-4097/laundry-40573", wait_until="networkidle", timeout=30000)
    time.sleep(3)
    br.close()

# Dump key APIs
print("\n\n=== API: geofence ===")
geo_url = f"{BASE}/api/geofence?restId={REST_ID}"
for u, d in apis_captured.items():
    if 'geofence' in u.lower():
        cities = d.get("data",{}).get("cities",[])
        print(f"Cities count: {len(cities)}")
        for city in cities:
            gf = city.get("geofences", [])
            print(f"  {city['name']} id={city['id']}  geofences={len(gf)}")
            for g in gf[:2]:
                print(f"    area={g.get('area_name')!r}  rest_brId={g.get('rest_brId')}  brId={g.get('rest_brId')}")

print("\n=== API: menu-section ===")
for u, d in apis_captured.items():
    if 'menu-section' in u.lower():
        sections = d.get("data", [])
        print(f"Sections count: {len(sections)}")
        for s in sections[:20]:
            name = s.get('name') or s.get('section_name') or '?'
        print(f"  {name!r}  id={s.get('id')}  status={s.get('status')}")

print("\n=== API: sub-section (for laundry) ===")
for u, d in apis_captured.items():
    if 'sub-section' in u.lower():
        subs = d.get("data", [])
        print(f"Sub-sections count: {len(subs)}")
        for s in subs[:10]:
            name = s.get('name') or s.get('section_name') or '?'
            print(f"  {name!r}  id={s.get('id')}  items={s.get('total_items')}")

print("\n=== API: items-by-subsection ===")
for u, d in apis_captured.items():
    if 'items-by-subsection' in u.lower():
        items = d.get("data", {})
        if isinstance(items, dict):
            all_items = items.get("items", items.get("products", []))
            total     = items.get("total", items.get("total_items", "?"))
        elif isinstance(items, list):
            all_items = items
            total = len(items)
        else:
            all_items = []
            total = "?"
        print(f"URL: {u[:120]}")
        print(f"Total items: {total}, items in response: {len(all_items)}")
        if all_items:
            print(f"Item keys: {list(all_items[0].keys())}")
            print(f"\nSample item:")
            print(json.dumps(all_items[0], indent=2)[:1500])

# Also try direct HTTP calls (no browser session needed?)
print("\n\n=== DIRECT HTTP TEST ===")
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
           "Referer": BASE, "Origin": BASE}
for url in [
    f"{BASE}/api/geofence?restId={REST_ID}",
    f"{BASE}/api/menu-section?restId={REST_ID}&rest_brId={BR_ID}&delivery_type=0",
]:
    r = requests.get(url, headers=headers, timeout=10)
    d = r.json()
    print(f"\n{url[:80]}  → {r.status_code}")
    if r.ok:
        items = d.get("data", [])
        if isinstance(items, list):
            print(f"  data[{len(items)}]")
            for i in items[:5]:
                print(f"  {json.dumps(i)[:120]}")
        else:
            print(f"  keys: {list(d.keys())}")

# Dump to file
with open(r"D:\data_science_market\config\imtiaz_api_sample.json", "w") as f:
    sample = {k: v for k, v in list(apis_captured.items())[:10]}
    json.dump(sample, f, indent=2, default=str)
print(f"\nSaved {len(apis_captured)} captured APIs to imtiaz_api_sample.json")
