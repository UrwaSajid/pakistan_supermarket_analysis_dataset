"""
DataCleaner – Phase 2 of the pipeline
=======================================
Takes the raw combined DataFrame and applies:
- Deduplication
- Missing-value handling
- Type coercion
- Unit consistency validation
- Outlier detection (Z-score + IQR)
- Price sanity checks
- Brand / name normalisation
"""

from __future__ import annotations

import re

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
from utils.helpers import normalize_unit, compute_price_per_unit, clean_text

_logger = get_logger("cleaner")

# Canonical column order for the processed layer
PROCESSED_COLUMNS = [
    "store", "city", "product_id", "name", "name_clean",
    "brand", "brand_clean", "category", "subcategory",
    "price", "sale_price", "currency",
    "quantity", "unit", "price_per_unit",
    "in_stock", "image_url", "product_url",
    "barcode", "description", "scraped_at",
    # Quality flags
    "_is_outlier_price", "_missing_price", "_missing_qty",
]


class DataCleaner:
    """
    Cleans and normalises a raw product DataFrame.

    Parameters
    ----------
    df : Raw combined DataFrame (all stores, all cities).
    """

    def __init__(self, df: pd.DataFrame) -> None:
        self.raw = df.copy()
        self.df  = df.copy()

    def run(self) -> pd.DataFrame:
        _logger.info("Cleaner START | input rows: %d", len(self.df))

        self._coerce_types()
        self._drop_exact_duplicates()
        self._clean_text_fields()
        self._fill_missing_units()
        self._price_sanity_check()
        self._detect_outliers()
        self._report_missing()
        self._finalise_columns()

        _logger.info("Cleaner DONE  | output rows: %d", len(self.df))
        return self.df

    # ── Type coercion ─────────────────────────────────────────────────────────

    def _coerce_types(self) -> None:
        for col in ("price", "sale_price", "price_per_unit", "quantity"):
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors="coerce")
        if "in_stock" in self.df.columns:
            self.df["in_stock"] = self.df["in_stock"].map(
                lambda x: True if str(x).lower() in ("true", "1", "yes") else
                          False if str(x).lower() in ("false", "0", "no") else None
            )
        if "scraped_at" in self.df.columns:
            self.df["scraped_at"] = pd.to_datetime(self.df["scraped_at"], errors="coerce", utc=True)

    # ── Deduplication ─────────────────────────────────────────────────────────

    def _drop_exact_duplicates(self) -> None:
        before = len(self.df)
        # Drop rows that are byte-for-byte identical
        self.df.drop_duplicates(inplace=True)
        # Drop store-level duplicates on (store, product_id) if product_id exists
        if "product_id" in self.df.columns:
            sub = self.df[self.df["product_id"].notna() & (self.df["product_id"] != "")]
            sub_dedup = sub.drop_duplicates(subset=["store", "product_id"], keep="first")
            no_id = self.df[self.df["product_id"].isna() | (self.df["product_id"] == "")]
            self.df = pd.concat([sub_dedup, no_id], ignore_index=True)
        after = len(self.df)
        _logger.info("Dedup: %d → %d (dropped %d)", before, after, before - after)

    # ── Text cleaning ─────────────────────────────────────────────────────────

    _EXTRA_WS = re.compile(r"\s+")
    _NON_ASCII = re.compile(r"[^\x00-\x7F]+")

    def _clean_text_fields(self) -> None:
        # Clean name
        if "name" in self.df.columns:
            self.df["name_clean"] = (
                self.df["name"]
                .fillna("")
                .apply(lambda x: self._EXTRA_WS.sub(" ", str(x)).strip())
                .str.lower()
            )
        else:
            self.df["name_clean"] = ""

        # Clean brand
        if "brand" in self.df.columns:
            self.df["brand_clean"] = (
                self.df["brand"]
                .fillna("")
                .str.strip()
                .str.lower()
                .str.replace(r"\b(pvt|ltd|limited|co|corp)\b\.?", "", regex=True)
                .str.strip()
            )
        else:
            self.df["brand_clean"] = ""

        # Ensure category is filled
        if "category" not in self.df.columns:
            self.df["category"] = ""
        self.df["category"] = self.df["category"].fillna("").str.strip()

    # ── Unit filling ──────────────────────────────────────────────────────────

    def _fill_missing_units(self) -> None:
        """Re-extract qty/unit from name where missing."""
        mask = self.df["quantity"].isna() | self.df["unit"].isna()
        if mask.sum() == 0:
            return

        def _extract(row):
            if pd.notna(row.get("quantity")) and pd.notna(row.get("unit")):
                return row["quantity"], row["unit"]
            name = row.get("name_clean") or row.get("name", "")
            q, u = normalize_unit(name)
            return q, u

        extracted = self.df[mask].apply(_extract, axis=1)
        # Convert None → np.nan for the numeric quantity column to avoid dtype errors
        qty_vals  = [float(e[0]) if e[0] is not None else np.nan for e in extracted]
        unit_vals = [e[1] for e in extracted]
        self.df.loc[mask, "quantity"] = qty_vals
        self.df.loc[mask, "unit"]     = unit_vals

        # Recompute price_per_unit where it is missing
        ppu_mask = self.df["price_per_unit"].isna()
        if ppu_mask.sum() > 0:
            ppu_vals = self.df[ppu_mask].apply(
                lambda r: compute_price_per_unit(r.get("price"), r.get("quantity"), r.get("unit")),
                axis=1,
            )
            self.df.loc[ppu_mask, "price_per_unit"] = pd.to_numeric(ppu_vals, errors="coerce")
        _logger.info("Filled missing units for %d rows", mask.sum())

    # ── Price sanity ──────────────────────────────────────────────────────────

    def _price_sanity_check(self) -> None:
        self.df["_missing_price"] = self.df["price"].isna()
        self.df["_missing_qty"]   = self.df["quantity"].isna()

        bad_price = (self.df["price"] < PRICE_MIN_PKR) | (self.df["price"] > PRICE_MAX_PKR)
        n_bad = bad_price.sum()
        if n_bad:
            _logger.warning("Price sanity: %d rows outside [%.0f, %.0f] PKR – nullifying",
                            n_bad, PRICE_MIN_PKR, PRICE_MAX_PKR)
            self.df.loc[bad_price, "price"] = np.nan
            self.df.loc[bad_price, "_missing_price"] = True

    # ── Outlier detection ─────────────────────────────────────────────────────

    def _detect_outliers(self) -> None:
        """Flag price outliers within each (store, category) group."""
        self.df["_is_outlier_price"] = False

        for (store, cat), grp in self.df.groupby(["store", "category"]):
            prices = grp["price"].dropna()
            if len(prices) < 5:
                continue
            # Z-score method
            z        = np.abs(stats.zscore(prices))
            z_flags  = z > ZSCORE_OUTLIER_THRESH
            # IQR method
            q1, q3   = prices.quantile(0.25), prices.quantile(0.75)
            iqr      = q3 - q1
            iqr_flags = (prices < q1 - IQR_MULTIPLIER * iqr) | (prices > q3 + IQR_MULTIPLIER * iqr)
            both_flags = prices.index[z_flags | iqr_flags]
            self.df.loc[both_flags, "_is_outlier_price"] = True

        n_out = self.df["_is_outlier_price"].sum()
        _logger.info("Outlier detection: flagged %d price outliers", n_out)

    # ── Missing value report ──────────────────────────────────────────────────

    def _report_missing(self) -> None:
        total = len(self.df)
        for col in self.df.columns:
            pct = self.df[col].isna().mean()
            if pct > MAX_MISSING_PCT:
                _logger.warning("Column '%s' is %.1f%% missing (threshold %.0f%%)",
                                col, pct * 100, MAX_MISSING_PCT * 100)

    # ── Column finalisation ───────────────────────────────────────────────────

    def _finalise_columns(self) -> None:
        """Ensure all canonical columns exist (fill with None if absent)."""
        for col in PROCESSED_COLUMNS:
            if col not in self.df.columns:
                self.df[col] = None
        # Keep only canonical + any extra
        extra = [c for c in self.df.columns if c not in PROCESSED_COLUMNS]
        self.df = self.df[PROCESSED_COLUMNS + extra]
