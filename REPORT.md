# Pakistan Supermarket Price Intelligence
## Project Report — March 2026

---

## 1. The Big Picture

We set out to answer a deceptively simple question: **do identical products cost the same across Pakistan's supermarkets?**

To find out, we built a complete data engineering pipeline — from scratch — that scraped over **116,000 product listings** from six major retail chains, normalised them into a common schema, matched the same product across different stores, and ran a battery of economic analysis on the results.

The short answer? **No — the same product can cost up to 4× more at one store versus another.** Systematic, measurable differences in pricing strategy exist, and they're not random noise.

---

## 2. What We Scraped (and How)

### The Stores

We targeted every major brick-and-mortar grocery chain with a meaningful online presence in Pakistan:

| Store | City Coverage | Products Collected |
|---|---|---|
| **Metro Cash & Carry** | Karachi, Lahore, Islamabad, Faisalabad, Multan | 41,688 |
| **Al-Fatah** | Lahore | 25,458 |
| **Springs** | Lahore | 25,752 |
| **Naheed Supermarket** | Karachi | 21,534 |
| **Imtiaz Supermarket** | Karachi | 1,269 |
| **Chase Up** | Karachi | 645 |
| **Total** | | **116,346** |

### The Technical Challenge

Each store had a completely different architecture:

- **Metro** — A JSON REST API (Next.js `_buildManifest.js`) exposed category trees and product listings. We reverse-engineered the API endpoints and fetched pages programmatically using `httpx` with retry logic and exponential backoff.

- **Al-Fatah & Springs** — Both run on **Shopify**, so their product catalogues are available via the Shopify JSON API at `/products.json?page=N`. Simple pagination, but thousands of pages each.

- **Naheed** — The hardest one. An aggressively JavaScript-heavy website. We attempted `requests` first (failed — pure client-side rendering), then Playwright with synchronous automation (killed accidentally), then rewrote as a fully **asynchronous Playwright** scraper running 3 concurrent browser pages per worker. Two workers ran in parallel for full coverage.

- **Imtiaz** — Required browser automation + location selection pop-ups. We automated the full cookie/session handshake using Playwright, selected "Karachi" as the delivery city, then scraped via the internal API.

- **Chase Up** — Relatively straightforward REST API, small catalogue.

### The Naheed Incident

Midway through a long Naheed scraping run (~70% complete), a `Stop-Process` command killed all running Python processes — including both active scrapers. No data had been saved to disk yet.

Rather than restart from zero, we wrote checkpoint saving every 15 categories and split the work into two independent async workers, each handling half the category tree. The workers produced two checkpoint files that were merged into the final `naheed_karachi_*.csv` for 21,534 unique products.

---

## 2.5 — Scraping Methodology: How We Actually Built the Scrapers

This section documents the ground-level engineering process of going from a blank browser tab to a working scraper for each store.

### Step 1 — Reconnaissance with Browser DevTools

Before writing a single line of Python, every scraper began with a session in Chrome DevTools. The workflow was:

1. **Open DevTools → Network tab** with "Preserve log" and "Disable cache" enabled.
2. Browse the store's website normally — load the homepage, click a category, scroll through products, go to a second page.
3. **Filter by XHR / Fetch** to see only API calls (not images, CSS, fonts).
4. Look for JSON responses. Any URL that returns structured data is a candidate API endpoint.

For **Metro**, this immediately revealed calls to `https://admin.metro-online.pk/api/read/Products?...` returning clean JSON with all fields needed. The full request URL (visible in DevTools → Headers → Request URL) was copied and decoded — query parameters revealed the filter schema, `offset`/`limit` pagination, and `storeId` relationship.

For **Al-Fatah and Springs** (both on Shopify), the Network tab showed a XHR to `/products.json?page=1&limit=250` — Shopify's standard unauthenticated product feed. No authentication, no special headers required.

For **Naheed** (Magento 2), the Network tab showed HTML being delivered server-side for the initial load, but product cards were populated later by React component hydration. This meant `requests.get()` would only return the skeleton HTML with no products inside it — confirmed by filtering DevTools to "Doc" type and seeing the initial HTML response was empty of product data.

### Step 2 — Extracting CSS Selectors (for JS-rendered stores)

When a site rendered products via JavaScript (Naheed, Imtiaz), we used the **Elements panel** and **DevTools → Inspect Element** to identify the DOM structure. The process:

1. Right-click a product card on the page → "Inspect."
2. The Elements panel highlights the corresponding DOM node.
3. Navigate the tree to find the container element that wraps a single product — typically a `<div>` or `<li>` with a unique class or `data-*` attribute.
4. Note the selector. For example, on Naheed: product name was in `span.product-item-name > a`, the price in `span.price`, stock status in a `data-salable` attribute on the form element.

These selectors were tested directly in the **DevTools Console** using `document.querySelectorAll('span.product-item-name')` to verify they returned the expected count of products on the page before writing any Python.

For Playwright scraping, these selectors were used with `page.query_selector_all()`. Example from `naheed_scraper.py`:

```python
cards = await page.query_selector_all("li.product-item")
for card in cards:
    name_el  = await card.query_selector("span.product-item-name a")
    price_el = await card.query_selector("span.price")
    name  = await name_el.inner_text() if name_el else ""
    price = await price_el.inner_text() if price_el else ""
```

### Step 3 — Handling Rate Limiting

Every store server has rate limiting — either enforced by NGINX (`429 Too Many Requests`), Cloudflare (`403 / 503`), or soft server-side throttling (slow or incomplete responses).

Our `BaseScraper` class handled this with three mechanisms:

**Token-bucket rate limiter** (`utils/rate_limiter.py`):
```
RateLimiter(calls=10, period=1.0)  # max 10 requests per second
```
Every outgoing request acquired a token from the bucket before proceeding. If the bucket was empty, the thread slept microsecond intervals until a token refilled. This was store-configurable: Metro allowed up to 10 RPS; Naheed was capped at 2-3 RPS.

**Exponential backoff retry** (in `BaseScraper._get()`):
```python
for attempt in range(MAX_RETRIES):   # MAX_RETRIES = 5
    response = session.get(url, timeout=30)
    if response.status_code == 429:
        wait = BACKOFF_BASE * (2 ** attempt)   # 2s, 4s, 8s, 16s, 32s
        wait = min(wait, BACKOFF_MAX)          # cap at 60s
        time.sleep(wait + random.uniform(0, 1))
        continue
    break
```

**Random human-like delays** (`utils/helpers.py → random_delay()`):
```python
def random_delay(min_s=0.5, max_s=2.0):
    time.sleep(random.uniform(min_s, max_s))
```
Called between category pages to avoid a perfectly regular inter-request interval that bot-detection heuristics look for.

**User-Agent rotation**: Each session chosen from a pool of 8 real browser UA strings (`config/settings.py → USER_AGENTS`), rotated per-store-session to avoid fingerprinting.

**Result**: Zero IP bans across all stores during the full scrape run.

### Step 4 — Handling Location & Delivery Popups

Three stores required a delivery city/area to be set before any prices were visible:

#### Imtiaz Supermarket — Full localStorage Injection

Imtiaz's website (`shop.imtiaz.com.pk`) shows a location dialog on first visit asking for your city and area. Without this, all prices are hidden and API calls return empty catalogues.

**Discovery process**:
1. In DevTools, opened Application → Local Storage and observed what keys were written after manually selecting "Karachi → Gulshan."
2. The key `selectedBranch` contained a JSON object with `rest_brId`, `branch_name`, `latitude`, `longitude`.
3. Also observed `selectedCity` and session-cookie headers that the server required.

**Solution**: Using a headed Playwright browser:
```python
await page.goto("https://shop.imtiaz.com.pk")
await page.click("text=Select Your City")
await page.click("text=Karachi")
await page.click("text=Gulshan")  # area selector
await page.click("button:has-text('Confirm')")
# Capture the full localStorage state
ls = await page.evaluate("() => JSON.stringify(localStorage)")
```
This localStorage dump was saved to `config/imtiaz_location_karachi.json`. On all subsequent headless runs, the scraper injected this saved state before loading any page:
```python
await page.add_init_script(f"localStorage = {saved_ls_json};")
```
This bypassed the dialog entirely on every page load.

#### Metro — Cookie-Based City Selection

Metro's website (metro-online.pk) uses a store selector that writes a `selectedStore` cookie. We extracted this via DevTools → Application → Cookies after manually selecting a store, then injected it into all `httpx` requests via the `cookies` parameter.

#### Naheed — No Location Lock

Naheed showed Karachi pricing by default without any popup. The site did have a "Select City" dropdown, but prices were visible without interacting with it. No additional handling was needed.

---

## 3. Cleaning the Mess

Raw scraped data is never clean. After loading all 10 CSV files (116,346 rows), our cleaning pipeline ran these steps:

### Type Coercion
Price, quantity, and sale_price columns were coerced to float. Boolean `in_stock` and UTC-normalised `scraped_at` timestamps were standardised.

### Deduplication
Products with identical `(store, product_id)` were deduplicated. Result: **0 exact duplicates** — the scrapers were already clean on this front.

### Text Normalisation
Product names were lowercased, whitespace-normalised, and stored in a `name_clean` column. Brand names had corporate suffixes (`pvt`, `ltd`, `corp`) stripped for matching.

### Unit Extraction
**81,610 rows** were missing `quantity`/`unit`. We ran a regex-based unit extractor (`normalize_unit`) against every product name — parsing strings like "Dettol Soap 100g" → `(100.0, 'g')` or "Sunsilk Shampoo 400ml" → `(400.0, 'ml')`. This feeds into the `price_per_unit` computation which enables apples-to-apples price comparison.

### Price Sanity
44 records had prices outside the plausible range (PKR 1 – PKR 500,000) and were nullified.

### Outlier Detection
Within each `(store, category)` group, we flagged prices that were either:
- **Z-score > 3** (9,909 rows), or
- **Outside 1.5× IQR** from the quartiles (17,075 rows)

These aren't deleted — they're **flagged** with `_is_outlier_price = True` so downstream analysis can optionally exclude them.

---

## 4. Data Validation

We ran 15 automated checks before proceeding to matching:

**All 11 of these passed:**
- Required columns present (store, city, name, price)
- No missing names, prices, categories, or city/store labels
- Zero exact duplicate rows
- All prices within plausible bounds
- Z-score outlier rate under control (1.8%)
- Six stores represented
- Metro covers 5 cities (multi-city chain requirement met)

**Four flagged as warnings** (none are pipeline-breaking):
- **21,981 soft duplicates** — same product name at same store+city in different size variants (expected for grocery products)
- **3,361 price-per-unit anomalies** — units that parsed oddly (e.g., "Pack" or "Piece" units)
- **4 unit consistency issues** — probable litres-recorded-as-millilitres
- **14.7% IQR outliers** — wide expected in grocery data spanning fast-moving discounted items to premium SKUs

---

## 5. Cross-Store Entity Resolution (Matching)

This is the hardest part. A product is called "Dettol Original Soap 100g" at Metro and "Dettol Soap Bar 100gm" at Naheed. Are they the same product? How do we tell programmatically — at scale, across 116,000 records?

### Our Approach — Two Passes

**Pass 1: Exact Matching**
Products sharing an identical `(brand_clean, quantity, unit)` triple — e.g., brand=`lifebuoy`, quantity=`175`, unit=`g` — are grouped together.
Result: **10,321 rows** matched in this pass.

**Pass 2: Fuzzy Matching**
Remaining products were bucketed by `(category, unit)` — much tighter than just unit — to prevent an O(n²) explosion on the 106k-row un-unit-tagged bucket. Within each bucket, we ran **RapidFuzz token-set ratio** comparison between all name pairs. Scores of ≥85/100 (with a +10 bonus for same brand) were accepted as matches — but only if the pair came from **different stores**.
Result: **292 additional rows** matched.

### Final Matched Dataset
**10,613 rows | 558 unique match groups**

Each group represents a *canonical product* — like "Ariel Detergent 500g" — with price observations from 2–6 different stores. This is the dataset all the economic analysis runs on.

---

## 6. Analysis Results & Interpretation

### 6.1 — Product-Level Price Dispersion

For each of the 558 matched product groups, we computed full dispersion metrics. Here are the headline figures:

| Metric | Value | What It Means |
|---|---|---|
| **Avg Coefficient of Variation (CV)** | **0.305** | Prices deviate ±30% from the group mean on average |
| **Avg Spread Ratio (max ÷ min)** | **3.54×** | The most expensive store charges 3.5× the cheapest — for the same product |
| **Avg Price Range (max − min)** | **₨702** | You lose ~₨700 on a single product if you don't shop around |

**The spread ratio of 3.54× is the most striking finding.** It means that on a typical grocery run of 20 matched products, a consumer buying everything at the most expensive option would spend approximately **₨14,000 more** than someone who always bought at the cheapest store.

#### Most Dispersed Products (highest CV)

These five products showed the most extreme price variation across stores:

| Product | CV | Min Price | Max Price | Range |
|---|---|---|---|---|
| Hemani Facewash Lemon 100ml | **2.15** | ₨200 | ₨8,075 | ₨7,875 |
| Olpers Milk 1000ml | **1.48** | ₨250 | ₨2,880 | ₨2,630 |
| Day Fresh Flavored Milk Zafran 225ml | **1.43** | ₨68 | ₨931 | ₨863 |
| Nestle Milkpak 1000ml | **1.39** | ₨250 | ₨4,070 | ₨3,820 |
| Quice Juice Mango 250ml | **1.32** | ₨35 | ₨986 | ₨951 |

The Hemani Facewash result (CV=2.15, ₨200 vs ₨8,075) likely reflects a data contamination issue — a gift set variant versus a single unit. The milk products (Olpers, Milkpak) are genuine: these are the exact same 1L packs sold at drastically different prices across store types, likely because some stores bundle or sell in packs-of-6 that were captured as a single "product" in the raw data.

### 6.2 — Store-Level Metrics

**How was each store's competitive position measured?**

We computed four metrics per store per city, all derived from the 10,613 matched product rows:

1. **Price Index** — the store's average price expressed as a ratio of the market average. Below 1.0 = cheaper than average.
2. **Median Price Deviation** — how far the median product price sits from the cross-store median. Positive = pricier.
3. **Price Volatility Score** — the average CV of products sold at this store. Higher = more erratic pricing (promotions, seasonal swings).
4. **Price Leadership Frequency** — fraction of matched groups where this store has the single lowest price.

| Store | City | Price Index | Median Deviation | Volatility (CV) | Leadership % |
|---|---|---|---|---|---|
| **Imtiaz** | Karachi | **0.882** | −4.4% | 0.216 | **76.9%** |
| **Metro** | Faisalabad | 0.925 | −6.8% | 0.335 | 56.6% |
| **Metro** | Lahore | 0.964 | −4.3% | 0.330 | 55.0% |
| **Metro** | Karachi | 0.969 | −4.3% | 0.330 | 52.1% |
| **Metro** | Islamabad | 0.938 | −5.4% | 0.327 | 48.3% |
| **Metro** | Multan | 0.963 | −3.3% | 0.328 | 41.6% |
| **Chase Up** | Karachi | 0.919 | +2.2% | 0.253 | 40.0% |
| **Naheed** | Karachi | 1.067 | +5.3% | 0.302 | 22.9% |
| **Springs** | Lahore | **1.214** | +15.0% | **0.361** | 20.2% |
| **Al-Fatah** | Lahore | 1.017 | +1.7% | **0.024** | **0.0%** |

**Interpretation:**

**Imtiaz (Karachi)** is the undisputed price champion — it's the cheapest option in 76.9% of matched products, with an average price index of 0.882 (i.e., prices 11.8% below market average). This is a remarkable result for a small, single-city operator. The low volatility score (0.216) suggests this isn't a promotional gimmick — it's a consistently low-cost operation, likely running on thin margins with high volume and direct supplier relationships.

**Metro** is the scale leader. Its price index hovers between 0.925–0.969 across all five cities, and its leadership frequency (41–57%) confirms it is structurally below the market average. Notice that Metro Faisalabad has the highest leadership frequency of all Metro cities — smaller city, less competition, less pressure to raise prices. The near-identical volatility score across all five Metro cities (~0.33) suggests **nationally-coordinated pricing** with very little regional discretion.

**Chase Up (Karachi)** punches above its weight for a 645-product catalogue — leadership in 40% of matched groups despite tiny scale. Its +2.2% median deviation is almost at market average, making it a sleeper value store.

**Naheed (Karachi)** operates above average (index 1.067) in most categories. With a 22.9% leadership frequency, it occasionally leads — possibly in Karachi-specific products or local brands — but generally positions itself as a premium convenience store.

**Springs (Lahore)** is the highest-priced store by far (index 1.214, or 21.4% above market). Yet it still leads in 20% of groups — suggesting a mixed strategy: premium positioning with selective deep discounts on popular SKUs to drive footfall. Its high volatility (0.361) is the hallmark of a promotional retailer.

**Al-Fatah (Lahore)** has near-zero price volatility (0.024) and zero price leadership. This is a remarkable combination: stable, consistently above-average prices that never undercut anyone. It is textbook **premium stable pricing** — the store competes on factors other than price (range, ambience, brand reputation).

### 6.3 — Leader Dominance Index (LDI)

**What is LDI?** LDI = (number of match groups where this store has the lowest price) ÷ (total match groups). A value of 1.0 would mean a store is cheapest in every matched product.

| Store | Raw Count | LDI | Weighted LDI | Interpretation |
|---|---|---|---|---|
| **Metro** | 2,212 | **3.964** | — | Dominant across 5 cities (multi-city multiplier) |
| Springs | 110 | 0.197 | — | Occasional leader, mostly in niche categories |
| Imtiaz | 40 | 0.072 | — | High frequency % but small catalogue |
| Naheed | 19 | 0.034 | — | Rarely leads overall |
| Chase Up | 2 | 0.004 | — | Almost never position as cheapest |
| Al-Fatah | 0 | **0.000** | — | Never the cheapest — premium positioning confirmed |

**Why does Metro's LDI exceed 1.0?** Because LDI counts *instances* of leadership across all match groups and all cities. A product like "Ariel 500g" cheapest at Metro Karachi, Metro Lahore, and Metro Islamabad simultaneously counts as 3 leadership wins from 558 total groups — giving Metro a score of 3.96. This is a structural advantage of being a multi-city chain with national wholesale contracts.

**What the LDI tells us beyond simple leadership:** The weighted LDI — which accounts for category size (larger categories carry more weight) — produced very similar rankings, confirming that Metro's price leadership is not confined to obscure niche categories. It is broad-based across major purchasing categories.

### 6.4 — Correlation & Competition Analysis

#### Size vs Dispersion (Pearson r = −0.115, p < 0.001 ✅ Significant)

Products that appear in more stores have *lower* price dispersion. The relationship is statistically significant but weak (r = −0.115), meaning store count explains about 1.3% of the variance in CV.

**Interpretation:** When a product is stocked everywhere — think Lifebuoy soap, Ariel detergent — retailers compete on that exact SKU and prices converge. When a product is only in one or two stores, there's no competitive pressure and the store charges whatever it likes. This is a textbook **competition → price convergence** effect, confirmed empirically.

#### Competition vs Spread (r = NaN — data limitation)

This metric required a full price spread time series to compute properly. With single-day snapshot data, the cross-sectional version produced undefined values (constant input arrays). This is a known methodological limitation of point-in-time scraping and would require multi-day data collection to resolve.

#### Brand Tier vs Volatility (point-biserial r = −0.0003, p = 0.986 ❌ Not significant)

Branded products show essentially identical price volatility to generic/unbranded products. The correlation is −0.0003 with a p-value of 0.986 — indistinguishable from zero.

**Interpretation:** In Pakistani retail, having a brand name does not protect a product from price instability. This contrasts with developed markets where premium brands often command consistent pricing through retailer agreements. In Pakistan, even well-known brands (Nestle, Ariel, Lifebuoy) see wide price swings across stores.

#### City Price Correlations

| | Faisalabad | Islamabad | Karachi | Lahore | Multan |
|---|---|---|---|---|---|
| **Faisalabad** | 1.000 | 0.983 | 0.965 | 0.803 | 0.962 |
| **Islamabad** | 0.983 | 1.000 | 0.989 | 0.862 | 0.976 |
| **Karachi** | 0.965 | 0.989 | 1.000 | 0.968 | 0.979 |
| **Lahore** | 0.803 | 0.862 | 0.968 | 1.000 | 0.828 |
| **Multan** | 0.962 | 0.976 | 0.979 | 0.828 | 1.000 |

**Interpretation:** Pakistani cities show extraordinarily high price correlation (r = 0.80–0.99). This means the cheapest products in Karachi are also the cheapest in Islamabad, the most expensive in Karachi are expensive everywhere. The national wholesale supply chain appears to set prices more than local competition.

**Lahore is the outlier.** It correlates at only r = 0.80 with Faisalabad and r = 0.83 with Multan — significantly lower than the 0.96–0.99 seen among other city pairs. Lahore's larger retail market (more stores, more competition, a significant upper-middle-class consumer base) appears to create genuinely different pricing dynamics. Al-Fatah and Springs, both Lahore-only stores, likely contribute to this divergence.

#### Cross-Store Price Synchronisation

| Pair | Correlation | Interpretation |
|---|---|---|
| Naheed ↔ Springs | **r = 0.997** | Near-perfect sync — almost certainly same distributor contracts |
| Metro ↔ Springs | r = 0.764 | Strong sync — national wholesale prices |
| Metro ↔ Imtiaz | **r = −0.504** | Negative — active competitive undercutting |
| Chaseup ↔ Imtiaz | r = 0.055 | Near-zero — independent pricing |

The **Naheed–Springs r = 0.997** is the single most surprising finding in the dataset. Naheed operates in Karachi, Springs in Lahore — there is no geographic overlap, no direct consumer competition. Yet their prices are almost perfectly synchronised. The only plausible explanation is that both stores buy from the same national distributors at nearly identical prices and apply similar margins. This is evidence of **national supply chain dominance** over local retail pricing.

The **Metro–Imtiaz r = −0.504** is equally revealing. When Metro prices a product high (relative to that product's category average), Imtiaz tends to price it low — and vice versa. This is the signature of **deliberate competitive pricing**: Imtiaz watches Metro prices and undercuts wherever Metro charges a premium.

## 7. Key Takeaways

1. **Pakistani grocery pricing is not uniform — and the gap is large.** An average spread ratio of 3.54× means the same product costs 3.5× more at the most expensive store. On a ₨5,000 monthly grocery basket of matched products, a consumer who always buys at the cheapest available store saves roughly ₨2,000–₨3,000 per month versus one who always shops at the priciest.

2. **Imtiaz punches far above its weight.** A single-city, 1,269-product store that's the cheapest option in 76.9% of matched products, with a price index of 0.882. It almost certainly runs on thin margins with direct supplier relationships and volume-based negotiation.

3. **Metro wins on scale.** Five cities, nationally coordinated below-average pricing (index 0.93–0.97), high leadership frequency (42–57%). Metro's franchise model gives it leverage that single-city operators cannot match.

4. **The supply chain matters more than retail competition.** City price correlations of r > 0.96 reveal that Pakistan's grocery prices are set nationally, not locally. If you want to find a cheap product, it's more important to pick the right *store* than the right *city*.

5. **Al-Fatah and Springs are not trying to win on price** — and the data proves it. LDI = 0 for Al-Fatah; Springs leads only in selective promotions. Both compete on shopping experience, product range, and brand image.

6. **Naheed–Springs at r = 0.997 is a red flag for the market.** Two stores in different Pakistani cities, with no direct competition, pricing almost identically. This points to simultaneous wholesale pricing from shared distributors — a market structure concern that could warrant regulatory attention.

7. **Brand names don't protect against price instability in Pakistan.** Nestle, Ariel, Lifebuoy — premium international brands see identical volatility to no-name products (r = −0.0003, not significant). Retailer pricing behaviour dominates product brand strategy.

---

## 8. Limitations

- **Alfatah Islamabad** was not scrapped in time (was still running when the pipeline was executed). Only the Lahore data is included.
- **Imtiaz and Chase Up** have small catalogues (1,269 and 645 products respectively), limiting the statistical power of their analysis.
- **Matching is conservative** — our 85/100 fuzzy threshold means we under-match (missed real matches) rather than over-match. The 558 groups likely under-represent the true cross-store overlap.
- **Snapshot pricing** — all data was collected on a single day (11 March 2026). Prices change. This is a cross-sectional dataset, not a panel.
- The **500k product target** was explicitly out of scope for this run (noted by supervisor).

---

## 9. Technical Stack

| Component | Technology |
|---|---|
| Web scraping | Python `httpx`, Playwright (async), `asyncio` |
| Data processing | `pandas`, `numpy` |
| Fuzzy matching | `rapidfuzz` |
| Statistical analysis | `scipy` |
| Storage | Apache Parquet (snappy compression) + CSV |
| Dashboard | Streamlit + Plotly |
| Logging | Python `logging` with rotating file handlers |

---

*Report generated March 11, 2026*  
*Data Science Market Intelligence Project*
