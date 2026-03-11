#!/usr/bin/env python3
"""
main.py – Pipeline entry point
================================
Run the full data collection pipeline:

  python main.py                          # scrape all enabled stores × all cities
  python main.py --stores metro imtiaz    # specific stores only
  python main.py --cities karachi lahore  # specific cities only
  python main.py --scrape-only            # skip cleaning + matching
  python main.py --store metro --city karachi  # single store/city
  python main.py --workers 16             # override thread count
"""

import argparse
import sys
import time
from pathlib import Path

# ── Make project root importable ─────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config.settings import STORES, MAX_GLOBAL_WORKERS
from pipeline.orchestrator import PipelineOrchestrator
from storage.data_store import layer_summary
from utils.logger import get_logger

_logger = get_logger("main")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Pakistan Supermarket Price Data Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full pipeline – all stores, all cities
  python main.py

  # Scrape Metro + Imtiaz only
  python main.py --stores metro imtiaz

  # Single store, single city (useful for testing)
  python main.py --store metro --city karachi --scrape-only

  # Use 20 workers for faster scraping
  python main.py --workers 20

  # Show what's already been collected (no scraping)
  python main.py --summary
        """,
    )
    p.add_argument("--stores",       nargs="+",  help="Store keys to scrape (default: all enabled)")
    p.add_argument("--store",                    help="Single store key")
    p.add_argument("--cities",       nargs="+",  help="City names to scrape for each store")
    p.add_argument("--city",                     help="Single city name")
    p.add_argument("--workers",      type=int,   default=MAX_GLOBAL_WORKERS,
                   help=f"Max parallel threads (default: {MAX_GLOBAL_WORKERS})")
    p.add_argument("--scrape-only",  action="store_true",
                   help="Only run Phase 1 (scraping). Skip cleaning + matching.")
    p.add_argument("--clean-only",   action="store_true",
                   help="Only run Phase 2+3 on already-scraped raw data.")
    p.add_argument("--summary",      action="store_true",
                   help="Print data layer summary and exit.")
    p.add_argument("--metro-all-cities", action="store_true",
                   help="Scrape Metro for all known cities in parallel.")
    return p.parse_args()


# ─────────────────────────────────────────────────────────────────────────────
# Store → city override builder
# ─────────────────────────────────────────────────────────────────────────────

def build_city_map(
    stores: list[str],
    cities_override: list[str] | None,
    city_override: str | None,
) -> dict[str, list[str]]:
    """
    Build the {store_key: [city, ...]} mapping respecting CLI overrides.
    """
    city_map: dict[str, list[str]] = {}
    for s in stores:
        default_cities = STORES.get(s, {}).get("cities", [])
        if city_override:
            city_map[s] = [city_override]
        elif cities_override:
            city_map[s] = cities_override
        else:
            city_map[s] = default_cities
    return city_map


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    args = parse_args()

    # ── Summary only ─────────────────────────────────────────────────────────
    if args.summary:
        summary = layer_summary()
        print("\n╔══════════════════════════════════╗")
        print("║  Data Layer Summary              ║")
        print("╠══════════════════════════════════╣")
        for layer, info in summary.items():
            print(f"║  {layer:<12}  files={info['files']:<6}  rows={info['rows']:<10}║")
        print("╚══════════════════════════════════╝\n")
        return

    # ── Resolve which stores to scrape ───────────────────────────────────────
    if args.store:
        stores = [args.store]
    elif args.stores:
        stores = args.stores
    else:
        stores = [k for k, v in STORES.items() if v.get("enabled")]

    # Validate
    unknown = [s for s in stores if s not in STORES]
    if unknown:
        _logger.error("Unknown store keys: %s  (valid: %s)", unknown, list(STORES.keys()))
        sys.exit(1)

    city_map = build_city_map(stores, args.cities, args.city)

    _logger.info("=" * 60)
    _logger.info("Pakistan Supermarket Price Pipeline")
    _logger.info("Stores  : %s", stores)
    _logger.info("Cities  : %s", {s: city_map[s] for s in stores})
    _logger.info("Workers : %d", args.workers)
    _logger.info("=" * 60)

    t0 = time.perf_counter()

    # ── Metro all-cities shortcut ─────────────────────────────────────────────
    if args.metro_all_cities:
        from scrapers.metro_scraper import MetroScraper
        recs = MetroScraper.scrape_all_cities(max_workers_per_store=4)
        _logger.info("Metro all-cities: %d records collected", len(recs))
        return

    # ── Clean-only mode ───────────────────────────────────────────────────────
    if args.clean_only:
        orch   = PipelineOrchestrator(stores=stores, cities=city_map, max_workers=args.workers)
        proc   = orch._run_cleaning()          # noqa: SLF001
        matched = orch._run_matching(proc)     # noqa: SLF001
        _logger.info("Clean+match done: %d processed, %d matched", len(proc), len(matched))
        return

    # ── Full or scrape-only run ───────────────────────────────────────────────
    orch = PipelineOrchestrator(
        stores      = stores,
        cities      = city_map,
        max_workers = args.workers,
    )

    if args.scrape_only:
        jobs = orch.run_scraping_only()
        total = sum(len(j.result) for j in jobs)
        _logger.info("Scrape-only done: %d total records in %.1f s",
                     total, time.perf_counter() - t0)
    else:
        report = orch.run()
        elapsed = time.perf_counter() - t0
        print("\n" + "═" * 60)
        print("Pipeline Complete")
        print(f"  Raw rows       : {report.get('raw_rows', 0):,}")
        print(f"  Processed rows : {report.get('processed_rows', 0):,}")
        print(f"  Matched rows   : {report.get('matched_rows', 0):,}")
        print(f"  Elapsed        : {elapsed:.1f} s")
        print("═" * 60 + "\n")


if __name__ == "__main__":
    main()
