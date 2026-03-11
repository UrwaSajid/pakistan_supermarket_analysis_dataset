"""
Worker Pool – Thread-pool manager with progress tracking.

Provides ``ScraperWorkerPool`` which fans out scraper jobs across
(store, city) pairs using a ThreadPoolExecutor.
"""

from __future__ import annotations

import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Callable

from utils.logger import get_logger

_logger = get_logger("worker_pool")


@dataclass
class ScrapeJob:
    """Represents a single (store, city) scraping task."""
    store:      str
    city:       str
    scraper_fn: Callable[[], list[dict[str, Any]]]
    result:     list[dict] = field(default_factory=list)
    error:      str        = ""
    duration_s: float      = 0.0
    success:    bool       = False


class ScraperWorkerPool:
    """
    Runs scraper jobs concurrently across ``max_workers`` threads.

    Usage
    -----
    pool = ScraperWorkerPool(max_workers=8)
    results = pool.run(jobs)
    """

    def __init__(self, max_workers: int = 8) -> None:
        self.max_workers = max_workers

    def run(self, jobs: list[ScrapeJob]) -> list[ScrapeJob]:
        """
        Execute all jobs and return them with populated ``.result`` /
        ``.error`` / ``.success`` / ``.duration_s`` fields.
        """
        _logger.info("Starting worker pool: %d jobs across %d workers", len(jobs), self.max_workers)
        future_to_job: dict[Future, ScrapeJob] = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for job in jobs:
                future = executor.submit(self._run_job, job)
                future_to_job[future] = job

            completed = 0
            for future in as_completed(future_to_job):
                job = future_to_job[future]
                completed += 1
                try:
                    filled_job = future.result()
                    _logger.info(
                        "[%d/%d] %s/%s → %d records (%.1f s)",
                        completed, len(jobs),
                        job.store, job.city,
                        len(filled_job.result),
                        filled_job.duration_s,
                    )
                except Exception as exc:
                    job.error   = str(exc)
                    job.success = False
                    _logger.error("[Worker] %s/%s failed: %s", job.store, job.city, exc)

        succeeded = sum(1 for j in jobs if j.success)
        failed    = len(jobs) - succeeded
        total_recs = sum(len(j.result) for j in jobs)
        _logger.info(
            "Worker pool finished | jobs=%d succeeded=%d failed=%d total_records=%d",
            len(jobs), succeeded, failed, total_recs,
        )
        return jobs

    @staticmethod
    def _run_job(job: ScrapeJob) -> ScrapeJob:
        t0 = time.perf_counter()
        try:
            job.result    = job.scraper_fn()
            job.success   = True
        except Exception as exc:
            job.error     = str(exc)
            job.success   = False
        finally:
            job.duration_s = time.perf_counter() - t0
        return job
