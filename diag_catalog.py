"""
Verify: navigation to /catalog/{section-slug}-{id} triggers sub-section API.
Then navigation to /catalog/{section-slug}-{id}/{subsub-slug}-{dish_sub_id} gets items.
"""
import json, time, re
from pathlib import Path

ls_cache = json.loads(Path("config/imtiaz_location_karachi.json").read_text("utf-8"))
ls_json  = json.dumps(ls_cache)
init_script = f"(function(ls){{for(var k in ls){{try{{localStorage.setItem(k,ls[k]);}}catch(e){{}}}}}})(  {ls_json} )"

_BASE = "https://shop.imtiaz.com.pk"

def slugify(text):
    s = text.lower().strip()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"[\s_]+", "-", s)
    return re.sub(r"-{2,}", "-", s).strip("-")

from playwright.sync_api import sync_playwright

with sync_playwright() as pw:
    br  = pw.chromium.launch(headless=True)
    ctx = br.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
        viewport={"width": 1440, "height": 900},
    )
    ctx.add_init_script(init_script)
    page = ctx.new_page()

    sections = []
    captured = {}

    def on_resp(r):
        try:
            if r.status != 200:
                return
            u = r.url
            if "json" not in r.headers.get("content-type", ""):
                return
            d = r.json()
            if "menu-section" in u and not sections:
                sections.extend(d.get("data") or [])
            if "sub-section" in u or "items-by-subsection" in u:
                captured[u] = d
                ep = u.split("/api/")[1].split("?")[0]
                data = d.get("data")
                cnt = len(data) if isinstance(data, list) else (len(data.get("items", [])) if isinstance(data, dict) else 0)
                print(f"  [INTERCEPTED] {ep}: {cnt} items  ({u.split('?')[1][:60]})")
        except: pass
    page.on("response", on_resp)

    print("Step 1: Homepage...")
    try:
        page.goto(_BASE, wait_until="networkidle", timeout=60_000)
    except: pass
    time.sleep(2)
    print(f"  Sections: {len(sections)}")

    if not sections:
        print("ERROR: no sections")
        br.close()
        exit()

    # Pick the "Home Care" section (we know it works from old test)
    home_care = next((s for s in sections if "home" in s.get("name","").lower()), sections[-2])
    fresh = next((s for s in sections if s.get("name","").lower() == "fresh"), sections[0])
    pharmacy = next((s for s in sections if "pharmacy" in s.get("name","").lower()), sections[-1])

    for test_sec in [home_care, fresh, pharmacy]:
        sec_id   = test_sec["id"]
        sec_name = test_sec["name"]
        sec_slug = slugify(sec_name)
        url = f"{_BASE}/catalog/{sec_slug}-{sec_id}"
        print(f"\nStep 2: Navigate to {url}")
        captured.clear()
        try:
            page.goto(url, wait_until="networkidle", timeout=30_000)
        except: pass
        time.sleep(2)

        if captured:
            print(f"  SUCCESS: captured {len(captured)} API responses")
            for u, d in captured.items():
                data = d.get("data") or []
                if isinstance(data, list):
                    print(f"  URL: {u.split('?')[1][:80]}")
                    for sub in data[:3]:
                        sub_id = sub.get("id")
                        sub_name = sub.get("name","?")
                        dish_subs = sub.get("dish_sub_sections") or []
                        print(f"    sub: {sub_name!r} id={sub_id}  dish_sub_sections={len(dish_subs)}")
                        for ds in dish_subs[:2]:
                            print(f"      dish_sub: {ds.get('name')!r} id={ds.get('id')}")
                    break
        else:
            print(f"  No API calls captured. Body: {page.inner_text('body')[:150].replace(chr(10),' ')}")

    # Test level 2: navigate to a specific dish-sub-section
    # Use Home Care (4097/home-care) -> Laundry (40573) -> Bleach (43195)
    sec = home_care
    print(f"\nStep 3: Navigate to subsection level (/catalog/home-care-{sec['id']}/laundry-40573)")
    captured.clear()
    url2 = f"{_BASE}/catalog/{slugify(sec['name'])}-{sec['id']}/laundry-40573"
    try:
        page.goto(url2, wait_until="networkidle", timeout=30_000)
    except: pass
    time.sleep(3)
    print(f"  Captured: {len(captured)} calls")
    for u, d in captured.items():
        ep = u.split("/api/")[1].split("?")[0]
        data = d.get("data")
        if isinstance(data, list):
            print(f"  {ep}: list[{len(data)}]  first keys: {list(data[0].keys())[:6] if data else []}")
            if "items" in ep.lower() or (data and data[0].get("price")):
                print(f"    First item: {data[0].get('name')!r} price={data[0].get('price')}")
        elif isinstance(data, dict):
            items = data.get("items") or []
            print(f"  {ep}: dict, items={len(items)}")

    br.close()
print("\nDone.")
