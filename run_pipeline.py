"""
run_pipeline.py
===============
Master entry point for the full post-scraping pipeline:

  Phase 1  Load all raw CSVs from data/raw/
  Phase 2  Clean & normalise  → data/processed/
  Phase 3  Validate           → terminal report
  Phase 4  Entity resolution  → data/matched/
  Phase 5  Price dispersion & competition analysis → data/analysis/

Usage
-----
    python run_pipeline.py
    python run_pipeline.py --skip-matching     # skip slow fuzzy matching
    python run_pipeline.py --analysis-only     # reuse existing matched file
"""

from __future__ import annotations

import argparse
import glob
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# ── Project root on sys.path ──────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from config.settings import MATCHED_DIR, PROCESSED_DIR, RAW_DIR
from pipeline.cleaner   import DataCleaner
from pipeline.validator import DataValidator
from pipeline.matcher   import EntityMatcher
from pipeline.analysis  import run_analysis
from utils.logger       import get_logger

_logger = get_logger("run_pipeline")

# ── Canonical columns we expect from raw files ────────────────────────────────
_CANONICAL = [
    "store", "city", "product_id", "name", "brand", "category", "subcategory",
    "price", "sale_price", "currency", "quantity", "unit", "price_per_unit",
    "in_stock", "image_url", "product_url", "barcode", "description", "scraped_at",
]


# ── Phase 1: Load raw data ────────────────────────────────────────────────────

def load_raw() -> pd.DataFrame:
    """Load all non-checkpoint CSVs from data/raw/ into one DataFrame."""
    files = [
        f for f in glob.glob(str(RAW_DIR / "*.csv"))
        if "ckpt" not in Path(f).name
    ]
    if not files:
        _logger.error("No raw CSV files found in %s", RAW_DIR)
        return pd.DataFrame()

    dfs = []
    for fpath in sorted(files):
        try:
            df = pd.read_csv(fpath, low_memory=False)
            # Drop internal _store/_city/_scraped_at duplicates
            df.drop(columns=[c for c in df.columns if c.startswith("_")],
                    errors="ignore", inplace=True)
            dfs.append(df)
            _logger.info("Loaded %s: %d rows", Path(fpath).name, len(df))
        except Exception as e:
            _logger.warning("Failed to load %s: %s", fpath, e)

    combined = pd.concat(dfs, ignore_index=True)
    _logger.info("Raw data combined: %d total rows across %d files", len(combined), len(dfs))
    return combined


# ── Phase 2: Clean & normalise ────────────────────────────────────────────────

def run_cleaning(raw: pd.DataFrame) -> pd.DataFrame:
    _logger.info("=" * 60)
    _logger.info("Phase 2: Cleaning & Normalisation")
    _logger.info("=" * 60)
    cleaner  = DataCleaner(raw)
    clean_df = cleaner.run()
    ts       = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = PROCESSED_DIR / f"processed_all_{ts}.parquet"
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    clean_df.to_parquet(out_path, compression="snappy", index=False)
    # Also save CSV for inspection
    clean_df.to_csv(PROCESSED_DIR / f"processed_all_{ts}.csv",
                    index=False, encoding="utf-8-sig")
    _logger.info("Processed data saved → %s (%d rows)", out_path.name, len(clean_df))
    return clean_df


# ── Phase 3: Validation ───────────────────────────────────────────────────────

def run_validation(processed: pd.DataFrame) -> dict:
    _logger.info("=" * 60)
    _logger.info("Phase 3: Validation")
    _logger.info("=" * 60)
    validator = DataValidator(processed)
    report    = validator.run()
    # Pretty-print summary
    print("\n--- Validation Report ---")
    for chk in report["checks"]:
        icon = "✓" if chk["status"] == "PASS" else "✗"
        print(f"  {icon} [{chk['status']}]  {chk['check']:<45s}  {chk['detail']}")
    print(f"\nTotal: {report['total']}  Passed: {report['passed']}  Failed: {report['failed']}\n")
    return report


# ── Phase 4: Entity resolution ────────────────────────────────────────────────

def run_matching(processed: pd.DataFrame) -> pd.DataFrame:
    _logger.info("=" * 60)
    _logger.info("Phase 4: Entity Resolution (Cross-store matching)")
    _logger.info("=" * 60)
    matcher    = EntityMatcher(processed)
    matched_df = matcher.run()
    if matched_df.empty:
        _logger.warning("No matches found!")
        return matched_df
    ts       = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = MATCHED_DIR / f"matched_all_{ts}.parquet"
    MATCHED_DIR.mkdir(parents=True, exist_ok=True)
    matched_df.to_parquet(out_path, compression="snappy", index=False)
    matched_df.to_csv(MATCHED_DIR / f"matched_all_{ts}.csv",
                      index=False, encoding="utf-8-sig")
    _logger.info(
        "Matched data saved → %s | groups=%d rows=%d",
        out_path.name,
        matched_df["match_group_id"].nunique(),
        len(matched_df),
    )
    return matched_df


# ── Phase 5: Analysis ─────────────────────────────────────────────────────────

def run_analysis_phase(matched: pd.DataFrame, processed: pd.DataFrame) -> dict:
    _logger.info("=" * 60)
    _logger.info("Phase 5: Price Dispersion & Competition Analysis")
    _logger.info("=" * 60)
    results = run_analysis(matched, processed)
    # Save JSON summary
    ts       = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_json = ROOT / "data" / "analysis" / f"analysis_summary_{ts}.json"

    def _jsonify(v):
        if isinstance(v, (pd.DataFrame, pd.Series)):
            return v.to_dict()
        if isinstance(v, (np.integer, np.floating)):
            return float(v)
        if isinstance(v, dict):
            return {k: _jsonify(vv) for k, vv in v.items()}
        if isinstance(v, list):
            return [_jsonify(i) for i in v]
        return v

    import numpy as np
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(_jsonify(results), f, indent=2, ensure_ascii=False)
    _logger.info("Analysis summary saved → %s", out_json.name)
    return results


# ── Load latest existing file ─────────────────────────────────────────────────

def _latest(directory: Path, pattern: str) -> Path | None:
    files = sorted(directory.glob(pattern))
    return files[-1] if files else None


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Run the supermarket pricing pipeline")
    parser.add_argument("--skip-matching",  action="store_true",
                        help="Skip entity resolution (use existing matched file if present)")
    parser.add_argument("--analysis-only",  action="store_true",
                        help="Skip all phases except analysis (requires existing matched file)")
    parser.add_argument("--skip-analysis",  action="store_true",
                        help="Stop after matching, skip analysis phase")
    args = parser.parse_args()

    t0 = time.perf_counter()
    _logger.info("=" * 60)
    _logger.info("PIPELINE START  %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    _logger.info("=" * 60)

    if args.analysis_only:
        # Load existing matched + processed files
        m_path = _latest(MATCHED_DIR, "matched_all_*.parquet")
        p_path = _latest(PROCESSED_DIR, "processed_all_*.parquet")
        if not m_path or not p_path:
            _logger.error("No existing matched/processed files found. Run without --analysis-only first.")
            sys.exit(1)
        matched_df   = pd.read_parquet(m_path)
        processed_df = pd.read_parquet(p_path)
        _logger.info("Loaded existing matched:  %s (%d rows)", m_path.name, len(matched_df))
        _logger.info("Loaded existing processed: %s (%d rows)", p_path.name, len(processed_df))
        run_analysis_phase(matched_df, processed_df)
        _print_summary(None, None, matched_df, time.perf_counter() - t0)
        return

    # ── Phase 1
    raw_df = load_raw()
    if raw_df.empty:
        _logger.error("No data to process. Aborting.")
        sys.exit(1)
    print(f"\nLoaded {len(raw_df):,} raw rows from {RAW_DIR.name}/")
    print(f"Stores: {sorted(raw_df['store'].unique()) if 'store' in raw_df.columns else 'unknown'}\n")

    # ── Phase 2
    processed_df = run_cleaning(raw_df)

    # ── Phase 3
    val_report = run_validation(processed_df)

    # ── Phase 4
    if args.skip_matching:
        m_path = _latest(MATCHED_DIR, "matched_all_*.parquet")
        if m_path:
            matched_df = pd.read_parquet(m_path)
            _logger.info("Skipping matching — loaded existing %s (%d rows)", m_path.name, len(matched_df))
        else:
            _logger.warning("--skip-matching set but no existing matched file; running matcher anyway")
            matched_df = run_matching(processed_df)
    else:
        matched_df = run_matching(processed_df)

    # ── Phase 5
    if not args.skip_analysis:
        analysis_results = run_analysis_phase(matched_df, processed_df)
        _print_analysis_summary(analysis_results)

    _print_summary(processed_df, val_report, matched_df, time.perf_counter() - t0)


def _print_analysis_summary(results: dict) -> None:
    print("\n" + "=" * 60)
    print("ANALYSIS RESULTS SUMMARY")
    print("=" * 60)

    if "product_dispersion" in results:
        pd_info = results["product_dispersion"]
        print(f"\n  Matched product groups : {pd_info.get('unique_groups', 0):,}")
        print(f"  Total matched rows     : {pd_info.get('rows', 0):,}")

    if "store_metrics" in results and results["store_metrics"]:
        print("\n  Store-Level Metrics:")
        print(f"  {'Store':<15} {'City':<15} {'Price Leadership %':>18} {'Volatility Score':>17}")
        print("  " + "-" * 70)
        for row in results["store_metrics"]:
            lf  = f"{row.get('price_leadership_freq', 0)*100:.1f}%" if row.get('price_leadership_freq') is not None else "N/A"
            vol = f"{row.get('price_volatility_score', 0):.4f}" if row.get('price_volatility_score') is not None else "N/A"
            print(f"  {row['store']:<15} {row['city']:<15} {lf:>18} {vol:>17}")

    if "ldi" in results and results["ldi"].get("ldi"):
        print("\n  Leader Dominance Index (LDI):")
        for row in results["ldi"]["ldi"][:5]:
            print(f"    {row['store']:<20} LDI = {row.get('ldi', 0):.4f}"
                  f"  ({row.get('n_leader', 0)} products with lowest price)")

    if "correlations" in results:
        corr = results["correlations"]
        print("\n  Correlation Analysis:")
        for key in ("size_vs_dispersion", "competition_vs_spread", "brand_tier_vs_volatility"):
            if key in corr:
                print(f"    {key}: {corr[key].get('interpretation', '')}")
        if "cross_store_sync" in corr:
            print(f"    cross_store_sync: {corr['cross_store_sync'].get('interpretation', '')}")

    print("\n  Full results saved to data/analysis/\n")


def _print_summary(processed, val_report, matched, elapsed):
    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)
    if processed is not None:
        print(f"  Processed rows   : {len(processed):,}")
    if matched is not None and not matched.empty:
        print(f"  Matched rows     : {len(matched):,}")
        print(f"  Unique groups    : {matched['match_group_id'].nunique():,}")
    if val_report:
        print(f"  Validation       : {val_report['passed']}/{val_report['total']} checks passed")
    print(f"  Total time       : {elapsed:.1f}s")
    print(f"\n  Output directories:")
    print(f"    data/processed/   — cleaned data")
    print(f"    data/matched/     — cross-store matched products")
    print(f"    data/analysis/    — dispersion & competition analysis")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
