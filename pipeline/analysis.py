"""
analysis.py – Price Dispersion & Market Structure Analysis (Section 3)
=======================================================================
Implements all mandatory metrics from the assignment:

3.1  Product-level price dispersion metrics
3.2  Store-level aggregated metrics
3.3  Leader Dominance Index (LDI + weighted + category-wise)
3.4  Correlation & competition analysis

All results are returned as a nested dict and optionally saved to
data/analysis/ as CSV files.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

from config.settings import BASE_DIR
from utils.logger import get_logger

_logger       = get_logger("analysis")
ANALYSIS_DIR  = BASE_DIR / "data" / "analysis"
ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# 3.1  Product-level price dispersion
# ─────────────────────────────────────────────────────────────────────────────

def compute_product_dispersion(matched: pd.DataFrame) -> pd.DataFrame:
    """
    For every match_group_id compute:
      mean, median, std, cv, price_range, iqr, spread_ratio
    Also attach relative_price_position per row.
    """
    _logger.info("Computing product-level dispersion …")
    if matched.empty:
        return matched

    grp = (
        matched.groupby("match_group_id")["price"]
        .agg(
            mean_price   = "mean",
            median_price = "median",
            std_price    = "std",
            min_price    = "min",
            max_price    = "max",
            n_stores     = "count",
        )
        .reset_index()
    )
    grp["cv"]           = grp["std_price"] / grp["mean_price"].replace(0, np.nan)
    grp["price_range"]  = grp["max_price"] - grp["min_price"]
    grp["iqr_price"]    = (
        matched.groupby("match_group_id")["price"]
        .apply(lambda s: s.quantile(0.75) - s.quantile(0.25))
        .values
    )
    grp["spread_ratio"] = grp["max_price"] / grp["min_price"].replace(0, np.nan)

    out = matched.merge(grp, on="match_group_id", how="left", suffixes=("", "_grp"))
    # Relative price position: store_price / category_mean
    cat_mean = (
        out.groupby(["match_group_id", "store"])["price"]
        .first().reset_index()
        .merge(
            out.groupby("match_group_id")["price"].mean().rename("cat_mean").reset_index(),
            on="match_group_id"
        )
    )
    out["rel_price_position"] = out["price"] / out["mean_price"].replace(0, np.nan)

    _logger.info("Product dispersion done | %d unique groups", grp.shape[0])
    return out


# ─────────────────────────────────────────────────────────────────────────────
# 3.2  Store-level aggregated metrics
# ─────────────────────────────────────────────────────────────────────────────

def compute_store_metrics(matched: pd.DataFrame, processed: pd.DataFrame) -> pd.DataFrame:
    """
    For each store+city:
      avg_category_price_index, median_price_deviation,
      price_volatility_score (avg CV), price_leadership_frequency
    """
    _logger.info("Computing store-level metrics …")
    if matched.empty:
        return pd.DataFrame()

    # Market mean per match_group
    market_mean = (
        matched.groupby("match_group_id")["price"].mean().rename("market_mean")
    )
    m = matched.join(market_mean, on="match_group_id")
    m["price_deviation"] = (m["price"] - m["market_mean"]) / m["market_mean"].replace(0, np.nan)

    # Per store+city
    rows = []
    for (store, city), grp in m.groupby(["store", "city"]):
        # avg category price index: mean(store_price / market_mean)
        avg_cat_idx  = grp["rel_price_position"].mean() if "rel_price_position" in grp.columns else np.nan
        # median price deviation from market average
        med_dev      = grp["price_deviation"].median()
        # price volatility score = avg CV of products in this store
        vols         = grp["cv"].dropna() if "cv" in grp.columns else pd.Series(dtype=float)
        volatility   = vols.mean() if len(vols) else np.nan
        # price leadership frequency = fraction of groups where this store is lowest
        if "match_group_id" in grp.columns:
            store_min  = grp.groupby("match_group_id")["price"].min()
            global_min = matched.groupby("match_group_id")["price"].min()
            # Align on shared group ids (store only appears in a subset of groups)
            shared_idx = store_min.index.intersection(global_min.index)
            if len(shared_idx):
                is_leader = (store_min.loc[shared_idx].values == global_min.loc[shared_idx].values).mean()
            else:
                is_leader = 0.0
        else:
            is_leader = np.nan
        rows.append({
            "store":                    store,
            "city":                     city,
            "avg_category_price_index": round(float(avg_cat_idx), 4) if pd.notna(avg_cat_idx) else None,
            "median_price_deviation":   round(float(med_dev),      4) if pd.notna(med_dev) else None,
            "price_volatility_score":   round(float(volatility),    4) if pd.notna(volatility) else None,
            "price_leadership_freq":    round(float(is_leader),     4) if pd.notna(is_leader) else None,
        })

    df = pd.DataFrame(rows).sort_values("price_leadership_freq", ascending=False)
    _logger.info("Store metrics done | %d store-city pairs", len(df))
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 3.3  Leader Dominance Index
# ─────────────────────────────────────────────────────────────────────────────

def compute_ldi(matched: pd.DataFrame) -> dict:
    """
    LDI_store = products_with_lowest_price / total_matched_products

    Returns dict with:
      - ldi             : simple LDI per store
      - weighted_ldi    : LDI weighted by category size
      - category_ldi    : LDI per store per category
    """
    _logger.info("Computing Leader Dominance Index …")
    if matched.empty or "match_group_id" not in matched.columns:
        return {}

    total_groups = matched["match_group_id"].nunique()

    # For each group: which store(s) have the minimum price?
    group_min = matched.groupby("match_group_id")["price"].transform("min")
    is_leader = matched["price"] == group_min

    # Simple LDI
    leader_counts = (
        matched[is_leader]
        .groupby("store")
        .size()
        .rename("n_leader")
        .reset_index()
    )
    leader_counts["ldi"] = leader_counts["n_leader"] / total_groups
    ldi_df               = leader_counts.sort_values("ldi", ascending=False)

    # Weighted LDI (weighted by category size = # products in category)
    if "category" in matched.columns:
        cat_size = matched.groupby("category").size().rename("cat_size")
        m2       = matched.join(cat_size, on="category")
        m2_lead  = m2[m2["price"] == m2.groupby("match_group_id")["price"].transform("min")]
        w_ldi    = (
            m2_lead.groupby("store")
            .apply(lambda g: g["cat_size"].sum())
            .rename("weighted_leader_sum")
            .reset_index()
        )
        total_weight              = m2["cat_size"].sum()
        w_ldi["weighted_ldi"]     = w_ldi["weighted_leader_sum"] / total_weight
        w_ldi                     = w_ldi.sort_values("weighted_ldi", ascending=False)
    else:
        w_ldi = pd.DataFrame()

    # Category-wise LDI
    cat_ldi_rows = []
    if "category" in matched.columns:
        for (store, cat), grp in matched.groupby(["store", "category"]):
            cat_groups      = matched[matched["category"] == cat]["match_group_id"].nunique()
            if cat_groups == 0:
                continue
            cat_leader_cnt  = (
                grp[grp["price"] == grp.groupby("match_group_id")["price"].transform("min")]
                ["match_group_id"].nunique()
            )
            cat_ldi_rows.append({
                "store":        store,
                "category":     cat,
                "cat_ldi":      round(cat_leader_cnt / cat_groups, 4),
                "leader_count": cat_leader_cnt,
                "total_groups": cat_groups,
            })
    cat_ldi_df = pd.DataFrame(cat_ldi_rows).sort_values("cat_ldi", ascending=False)

    _logger.info(
        "LDI done | top leader: %s (LDI=%.3f)",
        ldi_df.iloc[0]["store"] if not ldi_df.empty else "N/A",
        ldi_df.iloc[0]["ldi"]   if not ldi_df.empty else 0,
    )
    return {
        "ldi":          ldi_df,
        "weighted_ldi": w_ldi,
        "category_ldi": cat_ldi_df,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 3.4  Correlation & competition analysis
# ─────────────────────────────────────────────────────────────────────────────

def compute_correlations(matched: pd.DataFrame, processed: pd.DataFrame) -> dict:
    """
    Computes:
    a) Correlation: product size vs price dispersion
    b) Correlation: n_competitors vs price spread
    c) Correlation: brand tier vs price volatility
    d) City-wise price correlation matrix (Pearson + Spearman)
    e) Cross-store price synchronization score
    """
    _logger.info("Computing correlation & competition analysis …")
    results = {}

    # ── a) Size vs price dispersion ──────────────────────────────────────────
    if "quantity" in matched.columns and "cv" in matched.columns:
        sub = matched[["quantity", "cv"]].dropna()
        if len(sub) > 10:
            r_pearson, p_pearson = stats.pearsonr(sub["quantity"], sub["cv"])
            r_spear,   p_spear   = stats.spearmanr(sub["quantity"], sub["cv"])
            results["size_vs_dispersion"] = {
                "pearson_r":  round(r_pearson, 4), "pearson_p":   round(p_pearson, 4),
                "spearman_r": round(r_spear,   4), "spearman_p":  round(p_spear,   4),
                "interpretation": (
                    "Larger-sized products show "
                    f"{'more' if r_pearson > 0 else 'less'} price dispersion "
                    f"(r={r_pearson:.3f}, p={p_pearson:.3f})"
                ),
            }
            _logger.info("Size vs dispersion: Pearson r=%.3f p=%.3f", r_pearson, p_pearson)

    # ── b) n_competitors vs price spread ─────────────────────────────────────
    if "match_group_id" in matched.columns:
        comp = (
            matched.groupby("match_group_id")
            .agg(n_stores=("store", "nunique"), price_range=("price", lambda s: s.max() - s.min()))
            .reset_index()
        )
        sub = comp[["n_stores", "price_range"]].dropna()
        if len(sub) > 10:
            r_p, p_p = stats.pearsonr(sub["n_stores"], sub["price_range"])
            r_s, p_s = stats.spearmanr(sub["n_stores"], sub["price_range"])
            results["competition_vs_spread"] = {
                "pearson_r":  round(r_p, 4), "pearson_p":  round(p_p, 4),
                "spearman_r": round(r_s, 4), "spearman_p": round(p_s, 4),
                "interpretation": (
                    f"Products sold in more stores show "
                    f"{'wider' if r_p > 0 else 'narrower'} price spreads "
                    f"(r={r_p:.3f}, p={p_p:.3f})"
                ),
            }
            _logger.info("Competition vs spread: Pearson r=%.3f p=%.3f", r_p, p_p)

    # ── c) Brand tier vs price volatility ─────────────────────────────────────
    # Define brand tier heuristic: any brand appearing in top-5% price
    # products = "premium"; rest = "economy"
    if "brand_clean" in processed.columns and "price" in processed.columns:
        brand_med = processed.groupby("brand_clean")["price"].median()
        threshold = brand_med.quantile(0.75)
        is_premium = processed["brand_clean"].map(
            lambda b: 1 if pd.notna(b) and b != "" and brand_med.get(b, 0) >= threshold else 0
        )
        if "match_group_id" in matched.columns and "brand_clean" in matched.columns:
            m3 = matched.copy()
            m3["is_premium"] = m3["brand_clean"].map(
                lambda b: 1 if pd.notna(b) and b != "" and brand_med.get(b, 0) >= threshold else 0
            )
            if "cv" in m3.columns:
                sub = m3[["is_premium", "cv"]].dropna()
                if len(sub) > 10:
                    r_p, p_p = stats.pointbiserialr(sub["is_premium"], sub["cv"])
                    results["brand_tier_vs_volatility"] = {
                        "point_biserial_r": round(r_p, 4),
                        "p_value":          round(p_p, 4),
                        "interpretation": (
                            f"Premium brands show {'higher' if r_p > 0 else 'lower'} price "
                            f"volatility vs economy brands "
                            f"(r={r_p:.3f}, p={p_p:.3f})"
                        ),
                    }
                    _logger.info("Brand tier vs volatility: r=%.3f p=%.3f", r_p, p_p)

    # ── d) City-wise price correlation matrix ─────────────────────────────────
    if "city" in matched.columns and "match_group_id" in matched.columns:
        pivot = (
            matched.groupby(["match_group_id", "city"])["price"]
            .mean()
            .unstack("city")
        )
        if pivot.shape[1] >= 2:
            pearson_mat  = pivot.corr(method="pearson")
            spearman_mat = pivot.corr(method="spearman")
            results["city_price_correlation"] = {
                "pearson":  pearson_mat.to_dict(),
                "spearman": spearman_mat.to_dict(),
            }
            _logger.info("City correlation matrix computed | cities: %s", list(pivot.columns))

    # ── e) Cross-store price synchronization ─────────────────────────────────
    if "store" in matched.columns and "match_group_id" in matched.columns:
        pivot_store = (
            matched.groupby(["match_group_id", "store"])["price"]
            .mean()
            .unstack("store")
        )
        if pivot_store.shape[1] >= 2:
            sync_mat = pivot_store.corr(method="pearson")
            # Overall sync score = mean of upper triangle correlations
            triu     = sync_mat.where(np.triu(np.ones(sync_mat.shape), k=1).astype(bool))
            sync_score = triu.stack().mean()
            results["cross_store_sync"] = {
                "correlation_matrix": sync_mat.to_dict(),
                "sync_score":         round(float(sync_score), 4),
                "interpretation": (
                    f"Mean cross-store price correlation = {sync_score:.3f}. "
                    f"{'High' if sync_score > 0.7 else 'Moderate' if sync_score > 0.4 else 'Low'} "
                    "synchronization across stores."
                ),
            }
            _logger.info("Cross-store sync score: %.3f", sync_score)

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Master runner
# ─────────────────────────────────────────────────────────────────────────────

def run_analysis(matched: pd.DataFrame, processed: pd.DataFrame) -> dict:
    """
    Run all analyses and save results to data/analysis/.
    Returns a nested dict with all analysis results.
    """
    _logger.info("=" * 60)
    _logger.info("Analysis START | matched=%d  processed=%d", len(matched), len(processed))
    _logger.info("=" * 60)

    results = {}

    # 3.1 Product dispersion
    matched_with_disp = compute_product_dispersion(matched)
    results["product_dispersion"] = {
        "rows":         len(matched_with_disp),
        "unique_groups": matched_with_disp["match_group_id"].nunique() if not matched_with_disp.empty else 0,
    }
    if not matched_with_disp.empty:
        matched_with_disp.to_csv(ANALYSIS_DIR / "product_dispersion.csv", index=False, encoding="utf-8-sig")
        _logger.info("Saved product_dispersion.csv")

    # 3.2 Store metrics
    store_metrics = compute_store_metrics(matched_with_disp, processed)
    results["store_metrics"] = store_metrics.to_dict(orient="records") if not store_metrics.empty else []
    if not store_metrics.empty:
        store_metrics.to_csv(ANALYSIS_DIR / "store_metrics.csv", index=False, encoding="utf-8-sig")
        _logger.info("Saved store_metrics.csv")

    # 3.3 LDI
    ldi = compute_ldi(matched_with_disp)
    results["ldi"] = {k: v.to_dict(orient="records") if isinstance(v, pd.DataFrame) else v
                      for k, v in ldi.items()}
    if ldi.get("ldi") is not None and not ldi["ldi"].empty:
        ldi["ldi"].to_csv(ANALYSIS_DIR / "ldi.csv", index=False, encoding="utf-8-sig")
    if ldi.get("category_ldi") is not None and not ldi["category_ldi"].empty:
        ldi["category_ldi"].to_csv(ANALYSIS_DIR / "ldi_by_category.csv", index=False, encoding="utf-8-sig")
        _logger.info("Saved ldi.csv + ldi_by_category.csv")

    # 3.4 Correlations
    corr = compute_correlations(matched_with_disp, processed)
    results["correlations"] = corr
    # Save city correlation matrices as CSV if available
    if "city_price_correlation" in corr:
        pd.DataFrame(corr["city_price_correlation"]["pearson"]).to_csv(
            ANALYSIS_DIR / "city_price_corr_pearson.csv", encoding="utf-8-sig"
        )
        pd.DataFrame(corr["city_price_correlation"]["spearman"]).to_csv(
            ANALYSIS_DIR / "city_price_corr_spearman.csv", encoding="utf-8-sig"
        )
    if "cross_store_sync" in corr:
        pd.DataFrame(corr["cross_store_sync"]["correlation_matrix"]).to_csv(
            ANALYSIS_DIR / "cross_store_sync.csv", encoding="utf-8-sig"
        )
        _logger.info("Saved correlation CSVs")

    _logger.info("=" * 60)
    _logger.info("Analysis DONE")
    _logger.info("=" * 60)
    return results
