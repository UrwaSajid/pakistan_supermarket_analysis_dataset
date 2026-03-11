"""
Test API calls using the captured Playwright storage_state directly.
Also tests: what does the sub-section API actually require?
"""
import json, time
from pathlib import Path

SESSION_FILE = Path("config/imtiaz_session_karachi.json")
session_state = json.loads(SESSION_FILE.read_text("utf-8"))

_BASE = "https://shop.imtiaz.com.pk"
_REST = 55126
_BR   = 54934
_DT   = 0

from playwright.sync_api import sync_playwright

with sync_playwright() as pw:
    br  = pw.chromium.launch(headless=True)
    ctx = br.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        storage_state=session_state,
    )
    page = ctx.new_page()

    # Check cookies in context
    cookies = ctx.cookies()
    print(f"Cookies loaded: {len(cookies)}")
    for c in cookies:
        print(f"  {c['name']}={c['value'][:40]}  httpOnly={c['httpOnly']}")

    # Go to homepage
    print("\nLoading homepage...")
    sections = []
    def on_resp(r):
        if r.status == 200 and "menu-section" in r.url:
            try:
                d = r.json()
                if not sections:
                    sections.extend(d.get("data") or [])
            except: pass
    page.on("response", on_resp)
    try:
        page.goto(_BASE, wait_until="networkidle", timeout=60_000)
    except: pass
    time.sleep(2)

    ls_stored = page.evaluate("() => localStorage.getItem('stored_location')")
    print(f"stored_location: {ls_stored}")
    print(f"sections intercepted: {len(sections)}")

    # Test sub-section with credentials:include
    if sections:
        sec_id = sections[0].get("id")
        path = f"/api/sub-section?restId={_REST}&rest_brId={_BR}&sectionId={sec_id}&delivery_type={_DT}&source="
        
        print(f"\nTesting sub-section (id={sec_id}) with credentials:include...")
        r1 = page.evaluate("""async (path) => {
            try {
                const r = await fetch(path, {credentials: 'include', headers: {'Accept': 'application/json'}});
                const txt = await r.text();
                return {status: r.status, body: txt.substring(0, 300)};
            } catch(e) { return {error: String(e)}; }
        }""", path)
        print(f"  result: {r1}")

        # Also try with same-origin
        print(f"\nTesting sub-section with same-origin...")
        r2 = page.evaluate("""async (path) => {
            try {
                const r = await fetch(path, {credentials: 'same-origin', headers: {'Accept': 'application/json'}});
                const txt = await r.text();
                return {status: r.status, body: txt.substring(0, 300)};
            } catch(e) { return {error: String(e)}; }
        }""", path)
        print(f"  result: {r2}")

        # Check what cookies the current page context has
        print(f"\nCookies visible to page JS:")
        doc_cookies = page.evaluate("() => document.cookie")
        print(f"  {doc_cookies}")

        # Check if there's a session/auth cookie being set by server
        # Try to trigger a category navigation which would set cookies
        print(f"\nNavigating to /shop/fresh to trigger server cookies...")
        try:
            page.goto(f"{_BASE}/shop/fresh", wait_until="domcontentloaded", timeout=30_000)
        except: pass
        time.sleep(2)
        
        # Check cookies AFTER navigation
        cookies_after = ctx.cookies()
        print(f"Cookies after navigation: {len(cookies_after)}")
        for c in cookies_after:
            print(f"  {c['name']}={c['value'][:40]}  httpOnly={c['httpOnly']}")

        # NOW test sub-section again
        print(f"\nTesting sub-section AFTER /shop/fresh navigation...")
        r3 = page.evaluate("""async (path) => {
            try {
                const r = await fetch(path, {credentials: 'include', headers: {'Accept': 'application/json'}});
                const txt = await r.text();
                return {status: r.status, body: txt.substring(0, 300)};
            } catch(e) { return {error: String(e)}; }
        }""", path)
        print(f"  result: {r3}")

    br.close()
print("\nDone.")
