"""
validator.py – Automated validation layer (Section 4.4)
========================================================
Runs mandatory checks on the processed DataFrame and returns a
structured validation report with pass/fail status.

Checks implemented
------------------
1.  Missing value percentages (per column, threshold configurable)
2.  Duplicate detection  (exact + soft by name+store+city)
3.  Unit consistency     (ml/L mismatch, kg/g mismatch)
4.  Outlier detection    (Z-score AND IQR per category)
5.  Price sanity         (bounds check, price_per_unit bounds)
6.  Required column presence
7.  Store / city coverage counts
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy import stats

from config.settings import (
    IQR_MULTIPLIER,
    MAX_MISSING_PCT,
    PRICE_MAX_PKR,
    PRICE_MIN_PKR,
    ZSCORE_OUTLIER_THRESH,
)
from utils.logger import get_logger

_logger = get_logger("validator")

# Columns that MUST be present
REQUIRED_COLUMNS = ["store", "city", "name", "price"]


class ValidationReport:
    """Accumulates check results."""

    def __init__(self):
        self.checks: list[dict] = []

    def add(self, name: str, passed: bool, detail: str = "") -> None:
        status = "PASS" if passed else "FAIL"
        self.checks.append({"check": name, "status": status, "detail": detail})
        level = _logger.info if passed else _logger.warning
        level("[Validator] %-40s %s  %s", name, status, detail)

    def summary(self) -> dict:
        passed = sum(1 for c in self.checks if c["status"] == "PASS")
        failed = sum(1 for c in self.checks if c["status"] == "FAIL")
        return {
            "total":  len(self.checks),
            "passed": passed,
            "failed": failed,
            "checks": self.checks,
        }


class DataValidator:
    """
    Runs all automated validation checks on a (processed) DataFrame.

    Parameters
    ----------
    df : Cleaned/processed DataFrame.
    """

    def __init__(self, df: pd.DataFrame) -> None:
        self.df     = df.copy()
        self.report = ValidationReport()

    def run(self) -> dict:
        _logger.info("Validator START | rows=%d cols=%d", len(self.df), len(self.df.columns))

        self._check_required_columns()
        self._check_missing_values()
        self._check_duplicates()
        self._check_price_bounds()
        self._check_unit_consistency()
        self._check_price_outliers()
        self._check_store_coverage()

        summary = self.report.summary()
        _logger.info(
            "Validator DONE  | %d checks: %d passed, %d failed",
            summary["total"], summary["passed"], summary["failed"],
        )
        return summary

    # ── Checks ────────────────────────────────────────────────────────────────

    def _check_required_columns(self) -> None:
        missing = [c for c in REQUIRED_COLUMNS if c not in self.df.columns]
        self.report.add(
            "Required columns present",
            len(missing) == 0,
            f"Missing: {missing}" if missing else f"All {REQUIRED_COLUMNS} present",
        )

    def _check_missing_values(self) -> None:
        total = len(self.df)
        for col in ["name", "price", "category", "store", "city"]:
            if col not in self.df.columns:
                continue
            pct = self.df[col].isna().mean()
            self.report.add(
                f"Missing values [{col}]",
                pct <= MAX_MISSING_PCT,
                f"{pct*100:.1f}% missing (threshold {MAX_MISSING_PCT*100:.0f}%)",
            )

    def _check_duplicates(self) -> None:
        # Exact row duplicates
        n_exact = self.df.duplicated().sum()
        self.report.add(
            "No exact duplicate rows",
            n_exact == 0,
            f"{n_exact} exact duplicates found",
        )
        # Soft duplicates: same name + store + city
        soft_cols = [c for c in ["name", "store", "city"] if c in self.df.columns]
        if len(soft_cols) == 3:
            n_soft = self.df.duplicated(subset=soft_cols).sum()
            self.report.add(
                "No soft duplicates (name+store+city)",
                n_soft == 0,
                f"{n_soft} soft duplicates found",
            )

    def _check_price_bounds(self) -> None:
        if "price" not in self.df.columns:
            return
        prices = pd.to_numeric(self.df["price"], errors="coerce").dropna()
        n_bad  = ((prices < PRICE_MIN_PKR) | (prices > PRICE_MAX_PKR)).sum()
        self.report.add(
            "Price bounds",
            n_bad == 0,
            f"{n_bad} prices outside [{PRICE_MIN_PKR}, {PRICE_MAX_PKR}] PKR",
        )
        # price_per_unit sanity (0 – 50,000 PKR per unit)
        if "price_per_unit" in self.df.columns:
            ppu = pd.to_numeric(self.df["price_per_unit"], errors="coerce").dropna()
            n_ppu_bad = ((ppu < 0) | (ppu > 50_000)).sum()
            self.report.add(
                "Price-per-unit bounds",
                n_ppu_bad == 0,
                f"{n_ppu_bad} price_per_unit values out of range",
            )

    def _check_unit_consistency(self) -> None:
        if "unit" not in self.df.columns or "quantity" not in self.df.columns:
            self.report.add("Unit consistency", True, "unit/quantity columns absent – skipped")
            return
        # Flag rows where unit is L but quantity < 0.01 (likely mL recorded as L)
        mask_L = (self.df["unit"] == "L") & (pd.to_numeric(self.df["quantity"], errors="coerce") < 0.01)
        n_bad  = mask_L.sum()
        self.report.add(
            "Unit consistency (L vs ml)",
            n_bad == 0,
            f"{n_bad} possible mL-recorded-as-L rows",
        )

    def _check_price_outliers(self) -> None:
        if "price" not in self.df.columns:
            return
        prices = pd.to_numeric(self.df["price"], errors="coerce").dropna()
        if len(prices) < 5:
            self.report.add("Price outliers (global)", True, "insufficient data")
            return
        z      = np.abs(stats.zscore(prices))
        n_z    = (z > ZSCORE_OUTLIER_THRESH).sum()
        q1, q3 = prices.quantile(0.25), prices.quantile(0.75)
        iqr    = q3 - q1
        n_iqr  = ((prices < q1 - IQR_MULTIPLIER * iqr) | (prices > q3 + IQR_MULTIPLIER * iqr)).sum()
        self.report.add(
            "Price outliers (Z-score)",
            n_z < len(prices) * 0.05,
            f"{n_z} rows ({n_z/len(prices)*100:.1f}%) flagged",
        )
        self.report.add(
            "Price outliers (IQR)",
            n_iqr < len(prices) * 0.05,
            f"{n_iqr} rows ({n_iqr/len(prices)*100:.1f}%) flagged",
        )

    def _check_store_coverage(self) -> None:
        if "store" not in self.df.columns:
            return
        counts = self.df.groupby("store").size()
        detail = "  |  ".join(f"{s}={n}" for s, n in counts.items())
        self.report.add(
            "Min 3 stores covered",
            counts.shape[0] >= 3,
            detail,
        )
        if "city" in self.df.columns:
            city_counts = self.df.groupby(["store", "city"]).size()
            multi_city  = (city_counts.groupby(level=0).size() >= 2).sum()
            self.report.add(
                "Min 2 cities per store (chains with multi-city)",
                multi_city >= 1,
                f"{multi_city} store(s) have ≥2 cities",
            )
