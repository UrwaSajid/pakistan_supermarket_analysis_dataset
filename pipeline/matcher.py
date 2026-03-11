"""
EntityMatcher – Phase 3: Cross-store product identity resolution
=================================================================
Deterministic matching pipeline:
1. Exact match on (brand_clean + quantity + unit)
2. Token-set fuzzy match on name_clean within the same (brand, quantity, unit) bucket
3. Fallback: pure fuzzy match on name_clean with high threshold

Outputs a ``matched_df`` with a ``match_group_id`` column that groups
equivalent products across stores.
"""

from __future__ import annotations

import hashlib
import re
from typing import Optional

import numpy as np
import pandas as pd
from rapidfuzz import fuzz, process

from config.settings import EXACT_MATCH_BONUS, FUZZY_MATCH_THRESHOLD
from utils.logger import get_logger

_logger = get_logger("matcher")


class EntityMatcher:
    """
    Cross-store product entity resolver.

    Parameters
    ----------
    df : Cleaned / processed DataFrame.  Must have columns:
         store, city, name_clean, brand_clean, quantity, unit, price
    """

    def __init__(self, df: pd.DataFrame) -> None:
        self.df    = df.copy()
        self._gid  = 0          # rolling group id counter

    # ── Public entry point ────────────────────────────────────────────────────

    def run(self) -> pd.DataFrame:
        _logger.info("Matcher START | rows=%d", len(self.df))

        self.df["match_group_id"]   = None
        self.df["match_method"]     = None
        self.df["match_confidence"] = np.nan

        # Assign group IDs
        self._exact_match()
        self._fuzzy_match_bucketed()

        # Build matched view (only products that appear in ≥2 stores)
        matched = self._extract_cross_store_matches()
        _logger.info("Matcher DONE | unique match groups=%d | matched rows=%d",
                     matched["match_group_id"].nunique() if not matched.empty else 0,
                     len(matched))
        return matched

    # ── Step 1: Exact match ───────────────────────────────────────────────────

    def _exact_match(self) -> None:
        """
        Assign identical group ID to rows that share the same
        (brand_clean, quantity, unit) combination AND have a valid unit.
        """
        valid = (
            self.df["brand_clean"].notna() & (self.df["brand_clean"] != "") &
            self.df["quantity"].notna() &
            self.df["unit"].notna()
        )
        exact_df = self.df[valid].copy()

        for key, grp in exact_df.groupby(["brand_clean", "quantity", "unit"]):
            if len(grp["store"].unique()) < 2:
                continue   # must appear in ≥2 stores to be a match
            gid = self._new_gid()
            self.df.loc[grp.index, "match_group_id"]   = gid
            self.df.loc[grp.index, "match_method"]     = "exact"
            self.df.loc[grp.index, "match_confidence"] = 100.0

        n_exact = self.df["match_group_id"].notna().sum()
        _logger.info("Exact matches: %d rows assigned to groups", n_exact)

    # ── Step 2: Fuzzy match within buckets ────────────────────────────────────

    # Maximum rows per bucket for fuzzy matching — larger buckets are skipped
    # to prevent O(n²) explosion on the NaN-unit bucket.
    _MAX_BUCKET = 400

    def _fuzzy_match_bucketed(self) -> None:
        """
        For rows that didn't get an exact match, attempt token-set fuzzy
        matching within (category, unit) buckets.  Bucketing by category first
        prevents the huge NaN-unit catch-all group from blowing up run-time.
        """
        unmatched = self.df[self.df["match_group_id"].isna()].copy()
        _logger.info("Fuzzy matching %d unmatched rows …", len(unmatched))

        # Normalise category so NaN becomes empty string (still groups correctly)
        unmatched["_cat_norm"] = unmatched["category"].fillna("").str.lower().str.strip()

        for (cat, unit), grp in unmatched.groupby(["_cat_norm", "unit"], dropna=False):
            if len(grp) < 2:
                continue
            if len(grp) > self._MAX_BUCKET:
                # Bucket too large — skip to keep run-time reasonable
                continue
            self._fuzzy_within_group(grp)

        n_fuzzy = (self.df["match_method"] == "fuzzy").sum()
        _logger.info("Fuzzy matches: %d rows assigned to groups", n_fuzzy)

    def _fuzzy_within_group(self, grp: pd.DataFrame) -> None:
        """Run token-set ratio matching within a group."""
        names   = grp["name_clean"].fillna("").tolist()
        indices = grp.index.tolist()
        assigned: dict[int, str] = {}   # idx → group_id

        for i, (idx_i, name_i) in enumerate(zip(indices, names)):
            if not name_i or idx_i in assigned:
                continue
            for j in range(i + 1, len(names)):
                idx_j  = indices[j]
                name_j = names[j]
                if not name_j or idx_j in assigned:
                    continue
                score = fuzz.token_set_ratio(name_i, name_j)
                # Boost for same brand
                brand_i = str(grp.at[idx_i, "brand_clean"] if "brand_clean" in grp.columns else "")
                brand_j = str(grp.at[idx_j, "brand_clean"] if "brand_clean" in grp.columns else "")
                if brand_i and brand_i == brand_j:
                    score = min(score + EXACT_MATCH_BONUS, 100)
                if score >= FUZZY_MATCH_THRESHOLD:
                    # Check they come from ≥2 stores
                    store_i = self.df.at[idx_i, "store"]
                    store_j = self.df.at[idx_j, "store"]
                    if store_i == store_j:
                        continue
                    # Assign to same group
                    if idx_i not in assigned:
                        gid = self._new_gid()
                        assigned[idx_i] = gid
                        self.df.at[idx_i, "match_group_id"]   = gid
                        self.df.at[idx_i, "match_method"]     = "fuzzy"
                        self.df.at[idx_i, "match_confidence"] = score
                    else:
                        gid = assigned[idx_i]
                    assigned[idx_j] = gid
                    self.df.at[idx_j, "match_group_id"]   = gid
                    self.df.at[idx_j, "match_method"]     = "fuzzy"
                    self.df.at[idx_j, "match_confidence"] = score

    # ── Step 3: Extract cross-store matches ───────────────────────────────────

    def _extract_cross_store_matches(self) -> pd.DataFrame:
        """
        Return only rows that are in groups with ≥2 distinct stores.
        Adds aggregated columns for price dispersion analysis.
        """
        matched = self.df[self.df["match_group_id"].notna()].copy()
        if matched.empty:
            _logger.warning("No cross-store matches found!")
            return matched

        # Keep groups that span ≥2 stores
        multi_store = (
            matched.groupby("match_group_id")["store"]
            .nunique()
            .reset_index(name="n_stores")
        )
        valid_gids = multi_store.loc[multi_store["n_stores"] >= 2, "match_group_id"]
        matched = matched[matched["match_group_id"].isin(valid_gids)].copy()

        # Attach group-level price stats
        price_stats = (
            matched.groupby("match_group_id")["price"]
            .agg(
                group_mean_price  = "mean",
                group_median_price = "median",
                group_std_price   = "std",
                group_min_price   = "min",
                group_max_price   = "max",
                group_n_stores    = "count",
            )
            .reset_index()
        )
        price_stats["group_cv"] = (
            price_stats["group_std_price"] / price_stats["group_mean_price"].replace(0, np.nan)
        )
        price_stats["group_price_range"] = (
            price_stats["group_max_price"] - price_stats["group_min_price"]
        )
        price_stats["group_spread_ratio"] = (
            price_stats["group_max_price"] / price_stats["group_min_price"].replace(0, np.nan)
        )

        matched = matched.merge(price_stats, on="match_group_id", how="left")
        # Relative price position
        matched["rel_price_position"] = (
            matched["price"] / matched["group_mean_price"].replace(0, np.nan)
        )

        _logger.info(
            "Final matched dataset: %d rows | %d unique groups",
            len(matched),
            matched["match_group_id"].nunique(),
        )
        return matched

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _new_gid(self) -> str:
        self._gid += 1
        return f"G{self._gid:08d}"
