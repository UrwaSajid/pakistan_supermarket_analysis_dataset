"""
Pipeline Orchestrator
=======================
Coordinates all scrapers across stores × cities, manages the three data layers,
and produces a final pipeline report.

Usage
-----
from pipeline.orchestrator import PipelineOrchestrator

orch = PipelineOrchestrator(stores=["metro", "imtiaz", "naheed"], max_workers=12)
report = orch.run()
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from config.settings import LOGS_DIR, MATCHED_DIR, PROCESSED_DIR, RAW_DIR, STORES
from pipeline.worker_pool import ScrapeJob, ScraperWorkerPool
from scrapers import SCRAPER_REGISTRY
from storage.data_store import layer_summary, load, save
from utils.logger import get_logger

_logger = get_logger("orchestrator")


class PipelineOrchestrator:
    """
    Top-level pipeline coordinator.

    Parameters
    ----------
    stores      : List of store keys (from SCRAPER_REGISTRY). If None → all enabled.
    cities      : Optional override dict {store: [city, ...]}.  If None → use settings.
    max_workers : Total concurrent threads across all scraper jobs.
    """

    def __init__(
        self,
        stores:      list[str] | None = None,
        cities:      dict[str, list[str]] | None = None,
        max_workers: int = 12,
    ) -> None:
        self.stores      = stores or [k for k, v in STORES.items() if v.get("enabled")]
        self.cities      = cities  or {k: v["cities"] for k, v in STORES.items()}
        self.max_workers = max_workers
        self.worker_pool = ScraperWorkerPool(max_workers=max_workers)
        self._report: dict[str, Any] = {}

    # ── Public API ────────────────────────────────────────────────────────────

    def run(self) -> dict[str, Any]:
        """
        Execute the full pipeline:
        1. Scraping (raw layer)
        2. Cleaning + normalisation (processed layer)
        3. Entity resolution (matched layer)
        4. Generate report

        Returns a summary report dict.
        """
        t_start = time.perf_counter()
        _logger.info("=" * 70)
        _logger.info("Pipeline START | stores=%s", self.stores)
        _logger.info("=" * 70)

        # ── Phase 1: Scraping ─────────────────────────────────────────────────
        jobs = self._build_jobs()
        completed_jobs = self.worker_pool.run(jobs)
        raw_rows = sum(len(j.result) for j in completed_jobs)
        _logger.info("Phase 1 done | raw rows scraped: %d", raw_rows)

        # ── Phase 2: Clean & Normalise ────────────────────────────────────────
        processed_df = self._run_cleaning()
        _logger.info("Phase 2 done | processed rows: %d", len(processed_df))

        # ── Phase 3: Entity Resolution ────────────────────────────────────────
        matched_df = self._run_matching(processed_df)
        _logger.info("Phase 3 done | matched rows: %d", len(matched_df))

        elapsed = time.perf_counter() - t_start
        self._report = self._build_report(completed_jobs, processed_df, matched_df, elapsed)
        self._save_report()

        _logger.info("=" * 70)
        _logger.info("Pipeline DONE | total %.1f s", elapsed)
        _logger.info("=" * 70)
        return self._report

    def run_scraping_only(self) -> list[ScrapeJob]:
        """Run only Phase 1 (useful for incremental updates)."""
        jobs = self._build_jobs()
        return self.worker_pool.run(jobs)

    # ── Job construction ──────────────────────────────────────────────────────

    def _build_jobs(self) -> list[ScrapeJob]:
        jobs: list[ScrapeJob] = []
        for store_key in self.stores:
            scraper_cls = SCRAPER_REGISTRY.get(store_key)
            if scraper_cls is None:
                _logger.warning("No scraper registered for store '%s' – skipping", store_key)
                continue
            cities = self.cities.get(store_key, STORES.get(store_key, {}).get("cities", []))
            for city in cities:
                # Capture loop vars
                def _scrape_fn(cls=scraper_cls, c=city):
                    scraper = cls(city=c)
                    return scraper.scrape()

                jobs.append(ScrapeJob(
                    store      = store_key,
                    city       = city,
                    scraper_fn = _scrape_fn,
                ))
        _logger.info("Built %d scrape jobs across %d stores", len(jobs), len(self.stores))
        return jobs

    # ── Phase 2: Cleaning ─────────────────────────────────────────────────────

    def _run_cleaning(self) -> pd.DataFrame:
        """Load all raw files → clean → save to processed layer."""
        from pipeline.cleaner import DataCleaner  # lazy import
        raw_df = load("raw")
        if raw_df.empty:
            _logger.warning("No raw data found – skipping cleaning phase")
            return pd.DataFrame()
        cleaner   = DataCleaner(raw_df)
        clean_df  = cleaner.run()
        ts        = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        save(clean_df, "processed", f"processed_all_{ts}")
        return clean_df

    # ── Phase 3: Entity Resolution ────────────────────────────────────────────

    def _run_matching(self, processed_df: pd.DataFrame) -> pd.DataFrame:
        """Cross-store product matching → matched layer."""
        from pipeline.matcher import EntityMatcher  # lazy import
        if processed_df.empty:
            return pd.DataFrame()
        matcher    = EntityMatcher(processed_df)
        matched_df = matcher.run()
        ts         = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        save(matched_df, "matched", f"matched_all_{ts}")
        return matched_df

    # ── Report ────────────────────────────────────────────────────────────────

    def _build_report(
        self,
        jobs:        list[ScrapeJob],
        proc_df:     pd.DataFrame,
        matched_df:  pd.DataFrame,
        elapsed:     float,
    ) -> dict[str, Any]:
        job_stats = [
            {
                "store":    j.store,
                "city":     j.city,
                "records":  len(j.result),
                "success":  j.success,
                "duration": round(j.duration_s, 2),
                "error":    j.error,
            }
            for j in jobs
        ]
        return {
            "run_at":          datetime.now(timezone.utc).isoformat(),
            "elapsed_seconds": round(elapsed, 2),
            "scrape_jobs":     job_stats,
            "raw_rows":        sum(j["records"] for j in job_stats),
            "processed_rows":  len(proc_df) if proc_df is not None else 0,
            "matched_rows":    len(matched_df) if matched_df is not None else 0,
            "layer_summary":   layer_summary(),
        }

    def _save_report(self) -> None:
        path = LOGS_DIR / f"pipeline_report_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
        try:
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(self._report, fh, indent=2, default=str)
            _logger.info("Report saved → %s", path)
        except Exception as exc:
            _logger.warning("Could not save report: %s", exc)
