"""
Intercept ALL API calls the React app makes when navigating to a category.
We need to see the exact URL + method + headers used for sub-section and items.
"""
import json, time
from pathlib import Path

CONFIG_FILE = Path("config/imtiaz_location_karachi.json")
ls_cache = json.loads(CONFIG_FILE.read_text("utf-8"))

_BASE = "https://shop.imtiaz.com.pk"
_REST = 55126
_DT   = 0

ls_json = json.dumps(ls_cache)
init_script = (
    f"(function(ls){{"
    f"for(var k in ls){{try{{localStorage.setItem(k,ls[k]);}}catch(e){{}}}}"
    f"}})({ls_json})"
)

from playwright.sync_api import sync_playwright

api_calls = []

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
    
    def on_request(req):
        u = req.url
        if "/api/" in u and "imtiaz.com.pk" in u:
            api_calls.append({
                "method": req.method,
                "url": u,
                "headers": dict(req.headers),
            })

    def on_resp(r):
        try:
            if r.status == 200 and "json" in r.headers.get("content-type",""):
                if "menu-section" in r.url and not sections:
                    d = r.json()
                    secs = d.get("data") or []
                    if secs:
                        sections.extend(secs)
        except:
            pass

    page.on("request", on_request)
    page.on("response", on_resp)

    print("Loading homepage...")
    try:
        page.goto(_BASE, wait_until="networkidle", timeout=60_000)
    except Exception as e:
        print(f"  goto: {e}")
    time.sleep(2)

    print(f"Sections: {len(sections)}")
    if sections:
        # Navigate to first category to trigger sub-section + items API calls
        first_sec = sections[0]
        sec_name = first_sec.get("name","").lower().replace(" ","-").replace("&","-")
        cat_url = f"{_BASE}/shop/{sec_name}"
        print(f"\nNavigating to category: {cat_url}")
        try:
            page.goto(cat_url, wait_until="networkidle", timeout=30_000)
        except Exception as e:
            print(f"  goto category: {e}")
        time.sleep(3)

        # Also try clicking the Fresh section if navigation didn't trigger APIs
        print(f"\nAll API calls captured ({len(api_calls)} total):")
        for call in api_calls:
            url = call["url"]
            if any(k in url for k in ["sub-section","items-by-sub","splash","section"]):
                print(f"  [{call['method']}] {url}")
                # Print relevant headers (not all, just interesting ones)
                h = call["headers"]
                interesting = {k: v for k, v in h.items() 
                               if k.lower() in ["cookie","authorization","x-requested-with",
                                                "content-type","origin","referer","x-nextjs-data",
                                                "x-vercel-id"]}
                if interesting:
                    print(f"    headers: {interesting}")

        # Try manual jfetch with additional headers (matching what React sends)
        print("\n--- Testing sub-section with extra headers ---")
        sec_id = first_sec.get("id") or first_sec.get("section_id")
        # Get cookies
        cookies = ctx.cookies()
        cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])
        print(f"  Cookies in context: {len(cookies)}")
        
        # Try with cookie header
        sub_path = f"/api/sub-section?restId={_REST}&rest_brId=54934&sectionId={sec_id}&delivery_type={_DT}&source="
        raw = page.evaluate(
            """async (args) => {
                try {
                    const r = await fetch(args.path, {
                        method: 'GET',
                        credentials: 'include',
                        headers: {
                            'Accept': 'application/json',
                            'X-Requested-With': 'XMLHttpRequest',
                        }
                    });
                    const txt = await r.text();
                    return {status: r.status, text: txt.substring(0, 400)};
                } catch(e) { return {error: String(e)}; }
            }""",
            {"path": sub_path, "cookies": cookie_str}
        )
        print(f"  sub-section with credentials:include -> {raw}")

    br.close()

print("\nDone.")
