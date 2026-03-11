"""
Debug: what does _jfetch actually return for sub-section?
This mirrors exactly what the scraper does, with extra logging.
"""
import json, time
from pathlib import Path

CONFIG_FILE = Path("config/imtiaz_location_karachi.json")
ls_cache = json.loads(CONFIG_FILE.read_text("utf-8"))
print(f"Cache keys: {list(ls_cache.keys())}")

_BASE = "https://shop.imtiaz.com.pk"
_REST = 55126
_DT   = 0

# Build init script exactly like _init_script() in the scraper
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
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1440, "height": 900},
    )
    ctx.add_init_script(init_script)
    page = ctx.new_page()

    sections = []
    def on_resp(r):
        try:
            if r.status == 200 and "json" in r.headers.get("content-type",""):
                u = r.url
                d = r.json()
                if "menu-section" in u and isinstance(d, dict):
                    secs = d.get("data") or []
                    if secs:
                        sections.extend(secs)
                        print(f"  [INTERCEPT] menu-section -> {len(secs)} sections")
        except:
            pass
    page.on("response", on_resp)

    print("Loading homepage...")
    try:
        page.goto(_BASE, wait_until="networkidle", timeout=60_000)
    except Exception as e:
        print(f"  goto: {e}")
    time.sleep(2)

    # Get branch ID
    stored = page.evaluate("() => localStorage.getItem('stored_location')")
    print(f"stored_location after load: {stored}")
    pr = page.evaluate("() => localStorage.getItem('persist:root')")
    br_id = 0
    if pr:
        try:
            outer = json.loads(pr)
            state = json.loads(outer.get("state","{}"))
            br_id = int(state.get("currentBranchId") or 0)
        except:
            pass
    if stored and str(stored).strip().isdigit():
        br_id = int(stored)
    print(f"branch_id used: {br_id}")
    print(f"sections intercepted: {len(sections)}")

    if not sections:
        print("No sections. Trying manual jfetch for menu-section...")
        raw = page.evaluate(
            """async (path) => {
                const r = await fetch(path, {headers:{'Accept':'application/json'}});
                const txt = await r.text();
                return {status: r.status, text: txt.substring(0,300)};
            }""",
            f"/api/menu-section?restId={_REST}&rest_brId={br_id}&delivery_type={_DT}&source="
        )
        print(f"  manual menu-section: {raw}")
    else:
        # Test sub-section for first section
        sec = sections[0]
        sec_id = sec.get("id") or sec.get("section_id")
        print(f"\nFirst section: id={sec_id}, keys={list(sec.keys())}")

        sub_path = f"/api/sub-section?restId={_REST}&rest_brId={br_id}&sectionId={sec_id}&delivery_type={_DT}&source="
        print(f"Calling sub-section for sectionId={sec_id}...")
        
        # Raw text first
        raw = page.evaluate(
            """async (path) => {
                try {
                    const r = await fetch(path, {headers:{'Accept':'application/json'}});
                    const txt = await r.text();
                    return {status: r.status, text: txt.substring(0, 400)};
                } catch(e) { return {error: String(e)}; }
            }""",
            sub_path
        )
        print(f"  raw response: {raw}")

        if raw.get("status") == 200:
            result = page.evaluate(
                """async (path) => {
                    try {
                        const r = await fetch(path, {headers:{'Accept':'application/json'}});
                        return await r.json();
                    } catch(e) { return {error: String(e)}; }
                }""",
                sub_path
            )
            print(f"  parsed JSON keys: {list(result.keys()) if isinstance(result, dict) else type(result)}")
            if isinstance(result, dict):
                data = result.get("data")
                print(f"  data type: {type(data)}, len: {len(data) if isinstance(data,(list,dict)) else 'N/A'}")
                if isinstance(data, list) and data:
                    print(f"  first sub: {data[0]}")

    br.close()

print("\nDone.")
