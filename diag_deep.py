"""
Deep dive into home-features, home-featured-dishes, and DOM after clicking categories.
Find total products available and their structure.
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
        viewport={"width": 1440, "height": 900},
    )
    page = ctx.new_page()

    hf_data = {}
    sections = []

    def on_resp(r):
        try:
            if r.status != 200:
                return
            u = r.url
            if "home-features" in u:
                hf_data["data"] = r.json()
            if "menu-section" in u and not sections:
                d = r.json()
                sections.extend(d.get("data") or [])
        except:
            pass
    page.on("response", on_resp)

    print("Loading homepage...")
    try:
        page.goto(_BASE, wait_until="networkidle", timeout=60_000)
    except: pass
    time.sleep(3)

    # -------------------------------------------------------------------------
    # 1. Analyze ALL home-features and their products
    # -------------------------------------------------------------------------
    print("\n=== HOME-FEATURES FULL PRODUCT COUNT ===")
    hf_raw = hf_data.get("data", {})
    features = hf_raw.get("data") or []
    print(f"Total features: {len(features)}")

    all_featured_products = []
    for f in features:
        feat_title = f.get("title", "?")
        dishes = f.get("home_featured_dishes") or []
        print(f"  [{feat_title}] home_featured_dishes: {len(dishes)}", end="")
        products_with_price = [d for d in dishes if d.get("price") and float(str(d["price"])) > 0]
        print(f"  ({len(products_with_price)} with price > 0)")
        all_featured_products.extend(products_with_price)

    print(f"\nTotal featured products with price > 0: {len(all_featured_products)}")
    if all_featured_products:
        p = all_featured_products[0]
        print(f"Sample product keys: {list(p.keys())[:15]}")
        print(f"Sample product: name={p.get('name')!r} price={p.get('price')} id={p.get('id')} slug={p.get('slug')!r}")

    # -------------------------------------------------------------------------
    # 2. Try fetching home-featured-dishes API directly
    # -------------------------------------------------------------------------
    print("\n=== TRY home-featured-dishes API ===")
    # Maybe there's a separate API for this
    for feat in features[:3]:
        feat_id = feat.get("id")
        path = f"/api/home-featured-dishes?restId={_REST}&rest_brId={_BR}&homeFeatureId={feat_id}&delivery_type={_DT}&source="
        r = page.evaluate("""async (path) => {
            try {
                const r = await fetch(path, {credentials:'include',headers:{'Accept':'application/json'}});
                const txt = await r.text();
                return {status: r.status, len: txt.length, start: txt.substring(0,100)};
            } catch(e) { return {error: String(e)}; }
        }""", path)
        print(f"  homeFeatureId={feat_id} ({feat.get('title')}): {r}")

    # -------------------------------------------------------------------------
    # 3. Check DOM products after clicking each category
    # -------------------------------------------------------------------------
    print("\n=== DOM PRODUCTS after clicking categories ===")
    category_names = [f.get("title") for f in features[:5] if f.get("title")]
    all_dom_products = {}

    for cat_name in category_names:
        # Click the category text
        el = page.query_selector(f"text={cat_name}")
        if not el:
            print(f"  {cat_name}: element not found")
            continue

        el.click()
        time.sleep(2)

        # Extract product data from DOM
        product_els = page.query_selector_all("div[id^='product-item-']")
        if not product_els:
            # Try other selectors
            product_els = page.query_selector_all("[class*='MenuItemCard'], [class*='ProductCard'], [class*='product-card']")

        products_in_cat = []
        for pel in product_els[:50]:
            eid = pel.get_attribute("id") or ""
            try:
                # Extract price
                price_el = pel.query_selector("[class*='price'],[class*='Price'],[class*='amount'],[class*='Amount']")
                name_el  = pel.query_selector("h2,h3,h4,[class*='name'],[class*='Name'],[class*='title'],[class*='Title']")
                price_txt = price_el.inner_text().strip() if price_el else ""
                name_txt  = name_el.inner_text().strip() if name_el else ""
                if name_txt or price_txt:
                    products_in_cat.append({"id": eid, "name": name_txt, "price": price_txt})
            except:
                pass

        if products_in_cat:
            print(f"  {cat_name}: {len(product_els)} DOM elements, {len(products_in_cat)} with text")
            for p in products_in_cat[:2]:
                print(f"    {p}")
        elif product_els:
            print(f"  {cat_name}: {len(product_els)} elements but no text extracted")
            # Print raw HTML of first element
            raw = product_els[0].inner_html()[:200]
            print(f"    HTML: {raw}")
        else:
            # Get visible text on page
            body = page.inner_text("body")[:400].replace('\n',' ')
            print(f"  {cat_name}: 0 products. Body: {body[50:200]}")

    # -------------------------------------------------------------------------
    # 4. Check if section-details API works (another possible endpoint)
    # -------------------------------------------------------------------------
    print("\n=== TRY additional API endpoints ===")
    if sections:
        sec_id = sections[0].get("id")
        for path_template in [
            f"/api/section-details?restId={_REST}&rest_brId={_BR}&sectionId={sec_id}&source=",
            f"/api/menu-items?restId={_REST}&rest_brId={_BR}&sectionId={sec_id}&source=",
            f"/api/dishes?restId={_REST}&rest_brId={_BR}&sectionId={sec_id}&source=",
            f"/api/products?restId={_REST}&rest_brId={_BR}&sectionId={sec_id}&source=",
            f"/api/items?restId={_REST}&rest_brId={_BR}&sectionId={sec_id}&source=",
        ]:
            r = page.evaluate("""async (path) => {
                try {
                    const r = await fetch(path, {credentials:'include',headers:{'Accept':'application/json'}});
                    const txt = await r.text();
                    return {status: r.status, len: txt.length, start: txt.substring(0,80)};
                } catch(e) { return {error: String(e)}; }
            }""", path_template)
            ep = path_template.split("?")[0].split("/api/")[1]
            print(f"  /api/{ep}: status={r.get('status')} len={r.get('len')} start={r.get('start','')[:60]}")

    br.close()

print("\nDone.")
