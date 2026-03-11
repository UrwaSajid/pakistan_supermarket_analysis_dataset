# Pakistan Supermarket Price Intelligence

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://supermarket-analyser-pk.streamlit.app/)

A complete data engineering pipeline that scrapes, cleans, validates, matches, and analyses product prices across **6 major Pakistani supermarket chains** in **5 cities** — yielding 116,346 products and 558 cross-store matched product groups.

> **Key finding:** The same product costs up to **3.54× more** at one store versus another. Imtiaz (Karachi) is the cheapest chain (price index 0.882); Springs (Lahore) is the most expensive (1.214).

---

## Repository Structure

```
pakistan_supermarket_analysis/
├── scrapers/                  # One scraper per store
│   ├── base_scraper.py        # Shared: rate limiting, retries, UA rotation
│   ├── metro_scraper.py       # REST API (admin.metro-online.pk)
│   ├── alfatah_scraper.py     # Shopify /products.json
│   ├── springs_scraper.py     # Shopify /products.json
│   ├── naheed_scraper.py      # Async Playwright (JS-rendered Magento 2)
│   ├── imtiaz_scraper_v2.py   # Playwright + localStorage injection
│   └── chaseup_scraper.py     # REST API
├── pipeline/
│   ├── cleaner.py             # Type coercion, dedup, unit extraction
│   ├── validator.py           # 15 automated data quality checks
│   ├── matcher.py             # Exact + fuzzy entity resolution (RapidFuzz)
│   └── analysis.py            # CV, spread ratio, LDI, correlations
├── data/
│   ├── raw/                   # 10 raw CSVs (one per store-city pair)
│   ├── processed/             # Cleaned, normalised dataset (116,346 rows)
│   ├── matched/               # Cross-store matched groups (10,613 rows)
│   └── analysis/              # Analysis outputs (CSV + JSON)
├── dashboard.py               # Streamlit frontend
├── run_pipeline.py            # Master pipeline entry point
├── REPORT.md                  # Full human-readable project report
└── requirements.txt
```

---

## Stores & Coverage

| Store | Cities | Products |
|---|---|---|
| Metro Cash & Carry | Karachi, Lahore, Islamabad, Faisalabad, Multan | 41,688 |
| Al-Fatah | Lahore | 25,458 |
| Springs | Lahore | 25,752 |
| Naheed Supermarket | Karachi | 21,534 |
| Imtiaz Supermarket | Karachi | 1,269 |
| Chase Up | Karachi | 645 |
| **Total** | **5 cities** | **116,346** |

---

## Installation

```bash
git clone https://github.com/UrwaSajid/pakistan_supermarket_analysis.git
cd pakistan_supermarket_analysis

pip install -r requirements.txt

# Install Playwright browsers (only needed if re-scraping Naheed / Imtiaz)
playwright install chromium
```

---

## How Scraping Works

Each store required a different strategy discovered through Chrome DevTools Network tab analysis:

| Store | Method | Key Challenge |
|---|---|---|
| Metro | `httpx` → REST API (`admin.metro-online.pk/api/read/`) | Paginated JSON, multi-city store IDs |
| Al-Fatah | `requests` → Shopify `/products.json?page=N` | ~102 pages, simple pagination |
| Springs | `requests` → Shopify `/products.json?page=N` | ~103 pages |
| Naheed | Async Playwright (3 parallel pages) | JS-rendered Magento 2, no static HTML |
| Imtiaz | Playwright + localStorage injection | Location popup blocks all prices |
| Chase Up | `requests` → REST API | Small catalogue |

### Running individual scrapers

Scrapers are not meant to be run standalone from the project root — they are driven by `main.py` or the pipeline. To run a single store:

```bash
# From the project root
python main.py --store metro
python main.py --store alfatah
python main.py --store springs
python main.py --store naheed
python main.py --store imtiaz
python main.py --store chaseup
```

Raw output CSVs are saved to `data/raw/`.

### Handling location popups (Imtiaz)

Imtiaz hides all prices behind a city/area selector dialog. The scraper handles this by injecting a pre-captured `localStorage` snapshot (saved in `config/imtiaz_location_karachi.json`) into every Playwright page before loading:

```python
await page.add_init_script(f"Object.assign(localStorage, {json.dumps(saved_state)});")
```

To re-capture a location session for a new city:

```bash
python main.py --store imtiaz --capture-location
```

### Rate limiting & anti-bot

`BaseScraper` enforces:
- **Token-bucket rate limiter** — configurable RPS per store (Metro: 10, Naheed: 2)
- **Exponential backoff** on HTTP 429 / 503 — waits 2s, 4s, 8s, 16s, 32s
- **Random inter-request delays** — 0.5–2.0s between page fetches
- **User-Agent rotation** — pool of 8 real browser UA strings

---

## Running the Pipeline

The pipeline has 5 phases. Run all at once or selectively:

### Full pipeline (recommended)

```bash
python run_pipeline.py
```

Outputs:
- `data/processed/processed_all_<timestamp>.parquet` + `.csv`
- `data/matched/matched_all_<timestamp>.parquet` + `.csv`
- `data/analysis/*.csv` + `analysis_summary_<timestamp>.json`

### Skip the slow fuzzy-matching phase (reuse existing matched file)

```bash
python run_pipeline.py --skip-matching
```

### Re-run analysis only (matched data already exists)

```bash
python run_pipeline.py --analysis-only
```

### Run individual pipeline modules

```bash
# Phase 1+2 — clean raw CSVs only
python -c "from pipeline.cleaner import DataCleaner; DataCleaner().run()"

# Phase 3 — validate the processed dataset
python -c "from pipeline.validator import DataValidator; DataValidator().run()"

# Phase 4 — entity resolution / matching
python -c "from pipeline.matcher import ProductMatcher; ProductMatcher().run()"

# Phase 5 — price dispersion & competition analysis
python -c "from pipeline.analysis import PriceAnalyser; PriceAnalyser().run()"
```

---

## Running the Dashboard

```bash
python -m streamlit run dashboard.py
```

Opens at **http://localhost:8501**

The dashboard has 6 sections (sidebar navigation):

| Section | What it shows |
|---|---|
| Executive Summary | KPI cards, store/city distribution, pipeline overview, key findings |
| Store Performance | Price index, leadership %, volatility, median deviation table |
| Price Dispersion | CV & spread ratio distributions, most dispersed products, by-category CV |
| Market Competition | LDI ranking, cross-store sync heatmap, LDI by category, competition effect |
| City Analysis | Mean/median by city, violin plots, Pearson/Spearman city correlation heatmaps |
| Data Explorer | Filter by store/city/category/price — live table + histogram |

---

## Key Analysis Results

| Metric | Value |
|---|---|
| Avg coefficient of variation (CV) per product group | 0.305 |
| Avg spread ratio (max ÷ min price) | 3.54× |
| Avg price range per matched product | ₨702 |
| Cheapest store | Imtiaz, Karachi (price index 0.882) |
| Most expensive store | Springs, Lahore (price index 1.214) |
| Highest price leadership % | Imtiaz 76.9% |
| Naheed ↔ Springs cross-store correlation | r = 0.997 |
| Metro ↔ Imtiaz correlation | r = −0.504 |
| Cross-city correlation range | r = 0.80–0.99 |

See [REPORT.md](REPORT.md) for the full narrative, methodology, and interpretation.

---

## Data Dictionary

### `data/processed/` — cleaned full dataset

| Column | Type | Description |
|---|---|---|
| `store` | str | Store slug: `metro`, `springs`, `alfatah`, `naheed`, `imtiaz`, `chaseup` |
| `city` | str | `karachi`, `lahore`, `islamabad`, `faisalabad`, `multan` |
| `name` | str | Original product name |
| `name_clean` | str | Lowercased, whitespace-normalised name |
| `brand` | str | Brand name |
| `brand_clean` | str | Brand with corporate suffixes stripped |
| `category` | str | Store category label |
| `price` | float | Current selling price (PKR) |
| `quantity` | float | Extracted numeric quantity |
| `unit` | str | Extracted unit: `g`, `ml`, `l`, `kg`, `piece` … |
| `price_per_unit` | float | `price ÷ quantity` (comparable across sizes) |
| `_is_outlier_price` | bool | True if flagged by Z-score > 3 or IQR×1.5 |

### `data/matched/` — cross-store entity-resolved dataset

Inherits all processed columns plus:

| Column | Type | Description |
|---|---|---|
| `match_group_id` | int | Unique ID per canonical product |
| `match_method` | str | `exact` or `fuzzy` |
| `match_confidence` | float | RapidFuzz token-set-ratio score (fuzzy only) |
| `group_cv` | float | CV of prices within this match group |
| `group_spread_ratio` | float | max ÷ min price in group |
| `group_price_range` | float | max − min price in group |
| `group_n_stores` | int | Number of distinct stores in group |
