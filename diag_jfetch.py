"""
Debug: why does _jfetch return empty for sub-section?
We'll load homepage, wait, then try the sub-section call and print full result.
"""
import json, time
from pathlib import Path

CONFIG_FILE = Path("config/imtiaz_location_karachi.json")

ls_cache = {}
if CONFIG_FILE.exists():
    ls_cache = json.loads(CONFIG_FILE.read_text("utf-8"))
    print(f"Cache loaded: {list(ls_cache.keys())}")

# Build init script
init_js = "() => {\n"
for k, v in ls_cache.items():
    safe_v = json.dumps(v)
    init_js += f"  localStorage.setItem({json.dumps(k)}, {safe_v});\n"
init_js += "}"

_BASE = "https://shop.imtiaz.com.pk"
_REST = 55126
_BR   = 54934   # Karachi Gulshan (from cache stored_location)
_DT   = 0

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
    ctx.add_init_script(init_js)
    page = ctx.new_page()

    # List responses for debugging
    responses = []
    def on_resp(r):
        u = r.url
        if any(k in u for k in ["menu-section", "sub-section", "geofence", "splash"]):
            responses.append((r.status, u[:100]))
    page.on("response", on_resp)

    print("Loading homepage...")
    try:
        page.goto(_BASE, wait_until="networkidle", timeout=60_000)
    except Exception as e:
        print(f"  goto: {e}")
    time.sleep(2)

    print("\nCaptured responses:")
    for status, url in responses:
        print(f"  [{status}] {url}")

    # Check current URL and page title
    print(f"\nCurrent URL: {page.url}")
    print(f"Page title: {page.title()}")

    # Check localStorage
    ls = page.evaluate("() => ({...localStorage})")
    print(f"\nLocalStorage keys: {list(ls.keys())}")
    print(f"  stored_location: {ls.get('stored_location')}")

    # Test _jfetch for menu-section
    print("\n--- Testing menu-section ---")
    path = f"/api/menu-section?restId={_REST}&rest_brId={_BR}&delivery_type={_DT}&source="
    result = page.evaluate(
        """async (path) => {
            try {
                const r = await fetch(path, {
                    headers: {
                        'Accept': 'application/json',
                        'X-Requested-With': 'XMLHttpRequest',
                    }
                });
                const txt = await r.text();
                return {status: r.status, body_len: txt.length, body_start: txt.substring(0,200)};
            } catch(e) {
                return {error: e.toString()};
            }
        }""",
        path
    )
    print(f"menu-section result: {result}")

    # Test _jfetch for sub-section (use first section ID = 1 as guess)
    # First get sections
    sec_result = page.evaluate(
        """async (path) => {
            try {
                const r = await fetch(path, {'headers': {'Accept': 'application/json'}});
                return await r.json();
            } catch(e) { return {error: String(e)}; }
        }""",
        path
    )
    sections = sec_result.get("data", []) if isinstance(sec_result, dict) else []
    print(f"\nSections found: {len(sections)}")
    if sections:
        s = sections[0]
        sec_id = s.get("id") or s.get("section_id")
        print(f"  First section: id={sec_id}, name={s.get('name')}")

        sub_path = f"/api/sub-section?restId={_REST}&rest_brId={_BR}&sectionId={sec_id}&delivery_type={_DT}&source="
        print(f"\n--- Testing sub-section (raw text) ---")
        sub_raw = page.evaluate(
            """async (path) => {
                try {
                    const r = await fetch(path, {headers: {'Accept': 'application/json'}});
                    const txt = await r.text();
                    return {status: r.status, body_len: txt.length, body_start: txt.substring(0, 300)};
                } catch(e) { return {error: String(e)}; }
            }""",
            sub_path
        )
        print(f"sub-section raw: {sub_raw}")

    br.close()

print("\nDone.")
