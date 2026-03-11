"""
capture_session.py
==================
Opens a REAL (visible) browser on shop.imtiaz.com.pk.
YOU:
  1. Watch the browser open
  2. Select location when the dialog appears:
       - Order type: Express
       - City: Karachi (or any city you want)
       - Area: Gulshan (or any area)
       - Click "Select"
  3. Wait until products are visible on screen (homepage loads)
  4. Come back here and press ENTER

The script will save ALL cookies + localStorage to:
   config/imtiaz_session_karachi.json

That file will be used by the scraper so it never needs the dialog again.
"""
import json
import time
from pathlib import Path
from playwright.sync_api import sync_playwright

CITY = "karachi"
OUT_FILE = Path(f"config/imtiaz_session_{CITY}.json")
_BASE = "https://shop.imtiaz.com.pk"

print("=" * 60)
print("Imtiaz Session Capture")
print("=" * 60)
print()
print("Opening REAL browser on shop.imtiaz.com.pk ...")
print()

with sync_playwright() as pw:
    browser = pw.chromium.launch(
        headless=False,
        slow_mo=200,
        args=["--start-maximized"]
    )
    ctx = browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        no_viewport=True,   # use full screen
    )
    page = ctx.new_page()

    print("Loading homepage...")
    try:
        page.goto(_BASE, wait_until="networkidle", timeout=60_000)
    except Exception:
        pass

    print()
    print("=" * 60)
    print("BROWSER IS OPEN")
    print()
    print("If you see a location dialog:")
    print("  1. Click 'Express'")
    print("  2. Select City = Karachi")
    print("  3. Select Area = Gulshan (or any)")
    print("  4. Click 'Select'")
    print()
    print("Wait until the homepage shows products, then come back here.")
    print("=" * 60)
    input("\nPress ENTER when the homepage is showing products >>> ")

    print("\nSaving session state...")
    state = ctx.storage_state()   # captures ALL cookies + localStorage
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUT_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")

    cookies = state.get("origins", [])
    total_cookies = sum(len(o.get("cookies", [])) for o in state.get("cookies", [[]]))
    all_cookies = state.get("cookies", [])
    ls_entries  = []
    for origin in state.get("origins", []):
        ls_entries.extend(origin.get("localStorage", []))

    print(f"  Cookies saved   : {len(all_cookies)}")
    print(f"  localStorage    : {len(ls_entries)} entries")
    print(f"  Saved to        : {OUT_FILE}")
    print()
    print("All done! You can close this script.")
    print("The scraper will automatically use this session file.")

    browser.close()
