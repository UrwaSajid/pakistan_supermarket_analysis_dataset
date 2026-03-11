"""
dashboard.py — Pakistan Supermarket Price Intelligence
Run:  python -m streamlit run dashboard.py
"""
from __future__ import annotations
import json
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ─── Page setup ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="PK Price Intelligence",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)
ROOT = Path(__file__).resolve().parent

# ─── Design tokens ────────────────────────────────────────────────────────────
PLOTLY_TEMPLATE = "plotly_white"
STORE_COLORS = {
    "metro":   "#3b82f6",
    "springs": "#22c55e",
    "alfatah": "#ef4444",
    "naheed":  "#a855f7",
    "imtiaz":  "#f97316",
    "chaseup": "#06b6d4",
}
CITY_COLORS = {
    "karachi":    "#ef4444",
    "lahore":     "#3b82f6",
    "islamabad":  "#10b981",
    "faisalabad": "#f59e0b",
    "multan":     "#8b5cf6",
}
ACCENT = "#3b82f6"

# ─── CSS ──────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  /* ── Base ── */
  .stApp,
  [data-testid="stAppViewContainer"],
  [data-testid="stMain"] > div:first-child {
    background-color: #eef2f7 !important;
  }
  #MainMenu, footer { visibility: hidden; }
  [data-testid="stHeader"] { background: transparent !important; }
  [data-testid="stToolbar"] { visibility: hidden; }

  /* ── Sidebar collapse/expand button — always visible ── */
  [data-testid="collapsedControl"] {
    visibility: visible !important;
    display: flex !important;
    z-index: 9999 !important;
  }
  [data-testid="collapsedControl"] button {
    background-color: #0f172a !important;
    border-radius: 0 8px 8px 0 !important;
  }
  [data-testid="collapsedControl"] svg,
  [data-testid="collapsedControl"] svg path {
    fill: #ffffff !important;
    stroke: #ffffff !important;
    color: #ffffff !important;
  }

  /* ── Sidebar ── */
  [data-testid="stSidebar"],
  [data-testid="stSidebarContent"] {
    background: #0f172a !important;
  }
  [data-testid="stSidebar"] p,
  [data-testid="stSidebar"] span,
  [data-testid="stSidebar"] div,
  [data-testid="stSidebar"] label {
    color: #cbd5e1 !important;
  }
  [data-testid="stSidebar"] hr { border-color: #1e293b !important; }

  /* ── Metric cards ── */
  [data-testid="stMetric"] {
    background: #ffffff !important;
    border: 1px solid #d1d9e6 !important;
    border-radius: 12px !important;
    padding: 1rem 1.2rem !important;
    box-shadow: 0 2px 6px rgba(0,0,0,.07) !important;
  }
  /* metric value — target the inner text no matter what tag */
  [data-testid="stMetricValue"],
  [data-testid="stMetricValue"] > *,
  [data-testid="stMetricValue"] p {
    font-size: 1.85rem !important;
    font-weight: 800 !important;
    color: #0f172a !important;
    line-height: 1.2 !important;
  }
  [data-testid="stMetricLabel"],
  [data-testid="stMetricLabel"] > *,
  [data-testid="stMetricLabel"] p {
    font-size: 0.72rem !important;
    color: #475569 !important;
    text-transform: uppercase !important;
    letter-spacing: .06em !important;
  }

  /* ── Section sub-headings ── */
  .sh {
    font-size: 1.05rem; font-weight: 700; color: #0f172a;
    border-left: 4px solid #3b82f6; padding-left: .65rem;
    margin: 1.4rem 0 .5rem;
  }

  /* ── Page banner ── */
  .page-banner {
    background: #0f172a;
    border-radius: 14px;
    padding: 1.1rem 1.5rem;
    margin-bottom: 1rem;
    display: flex;
    align-items: center;
    gap: .8rem;
  }
  .page-banner-icon  { font-size: 1.5rem; }
  .page-banner-title {
    font-size: 1.45rem; font-weight: 800;
    color: #f1f5f9; margin: 0; line-height: 1;
  }
  .page-banner-sub   {
    font-size: .8rem; color: #94a3b8;
    margin: .2rem 0 0; line-height: 1.4;
  }

  /* ── Callout boxes ── */
  .cb-blue  { background:#dbeafe; border-left:4px solid #3b82f6;
               padding:.7rem 1rem; border-radius:0 8px 8px 0; font-size:.84rem;
               color:#1e3a5f; margin:.45rem 0; line-height:1.6; }
  .cb-green { background:#dcfce7; border-left:4px solid #22c55e;
               padding:.7rem 1rem; border-radius:0 8px 8px 0; font-size:.84rem;
               color:#14532d; margin:.45rem 0; line-height:1.6; }
  .cb-amber { background:#fef9c3; border-left:4px solid #f59e0b;
               padding:.7rem 1rem; border-radius:0 8px 8px 0; font-size:.84rem;
               color:#713f12; margin:.45rem 0; line-height:1.6; }
  .cb-red   { background:#fee2e2; border-left:4px solid #ef4444;
               padding:.7rem 1rem; border-radius:0 8px 8px 0; font-size:.84rem;
               color:#7f1d1d; margin:.45rem 0; line-height:1.6; }

  /* ── KPI hero cards ── */
  .kpi-card {
    background: #ffffff; border: 1px solid #d1d9e6; border-radius: 14px;
    padding: 1.1rem 1.2rem; text-align: center;
    box-shadow: 0 2px 6px rgba(0,0,0,.07);
  }
  .kpi-num  { font-size: 1.9rem; font-weight: 800; color: #0f172a; line-height: 1; }
  .kpi-lbl  { font-size: .7rem; color: #475569; text-transform: uppercase;
               letter-spacing: .08em; margin-top: .3rem; }

  /* ── Chart wrapper card ── */
  .chart-card {
    background: #ffffff; border: 1px solid #d1d9e6; border-radius: 12px;
    padding: .8rem 1rem; box-shadow: 0 1px 4px rgba(0,0,0,.05);
    margin-bottom: .5rem;
  }

  /* ── Divider ── */
  .divider { border-top: 1.5px solid #d1d9e6; margin: 1.2rem 0; }

  /* ── Pipeline stage ── */
  .stage { border-radius: 12px; padding: 1rem; height: 138px; }

  /* ── Dataframe frame ── */
  [data-testid="stDataFrame"] { border-radius: 10px; overflow: hidden; }
</style>
""", unsafe_allow_html=True)


# ─── Data loading ─────────────────────────────────────────────────────────────
@st.cache_data(show_spinner="Loading data…")
def load_data() -> dict:
    def latest(directory: Path, pattern: str):
        files = sorted(directory.glob(pattern))
        return files[-1] if files else None

    ana = ROOT / "data" / "analysis"

    def load_df(directory: Path, parquet_pat: str, csv_pat: str) -> pd.DataFrame:
        """Load parquet if available, otherwise fall back to CSV."""
        pq = latest(directory, parquet_pat)
        if pq:
            return pd.read_parquet(pq)
        cv = latest(directory, csv_pat)
        if cv:
            return pd.read_csv(cv, low_memory=False)
        return pd.DataFrame()

    sum_f = latest(ana, "analysis_summary_*.json")

    processed = load_df(ROOT / "data" / "processed", "*.parquet", "*.csv")
    matched   = load_df(ROOT / "data" / "matched",   "*.parquet", "*.csv")

    def csv(name):
        p = ana / name
        return pd.read_csv(p) if p.exists() else pd.DataFrame()

    def csv_idx(name):
        p = ana / name
        return pd.read_csv(p, index_col=0) if p.exists() else pd.DataFrame()

    summary: dict = {}
    if sum_f:
        with open(sum_f) as fh:
            summary = json.load(fh)

    return {
        "processed":    processed,
        "matched":      matched,
        "store_metrics":csv("store_metrics.csv"),
        "ldi":          csv("ldi.csv"),
        "ldi_cat":      csv("ldi_by_category.csv"),
        "corr_pearson": csv_idx("city_price_corr_pearson.csv"),
        "corr_spearman":csv_idx("city_price_corr_spearman.csv"),
        "cross_sync":   csv_idx("cross_store_sync.csv"),
        "product_disp": pd.read_csv(ana / "product_dispersion.csv", nrows=60000)
                        if (ana / "product_dispersion.csv").exists() else pd.DataFrame(),
        "summary":      summary,
    }


D     = load_data()
proc  = D["processed"]
match = D["matched"]
s_met = D["store_metrics"]
ldi   = D["ldi"]
ldi_c = D["ldi_cat"]
cp    = D["corr_pearson"]
cs    = D["corr_spearman"]
cxs   = D["cross_sync"]
summ  = D["summary"]

stores_all = sorted(proc["store"].unique()) if not proc.empty else []
cities_all = sorted(proc["city"].unique())  if not proc.empty else []


# ─── Layout helpers ───────────────────────────────────────────────────────────
def page_banner(icon: str, title: str, subtitle: str = ""):
    sub = f'<div class="page-banner-sub">{subtitle}</div>' if subtitle else ""
    st.markdown(
        f'<div class="page-banner">'
        f'<span class="page-banner-icon">{icon}</span>'
        f'<div><div class="page-banner-title">{title}</div>{sub}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

def sh(text: str):
    st.markdown(f'<div class="sh">{text}</div>', unsafe_allow_html=True)

def cb(text: str, kind: str = "blue"):
    st.markdown(f'<div class="cb-{kind}">{text}</div>', unsafe_allow_html=True)

def divider():
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

def kpi(val: str, label: str):
    st.markdown(f'<div class="kpi-card"><div class="kpi-num">{val}</div>'
                f'<div class="kpi-lbl">{label}</div></div>', unsafe_allow_html=True)


# ─── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(
        '<div style="padding:.4rem 0 1rem">'
        '<span style="font-size:1.6rem">🛒</span>'
        '<span style="font-size:.95rem;font-weight:700;color:#f1f5f9;margin-left:.5rem">'
        'PK Price Intelligence</span></div>',
        unsafe_allow_html=True,
    )
    page = st.radio(
        "Navigation",
        ["Executive Summary", "Store Performance", "Price Dispersion",
         "Market Competition", "City Analysis", "Data Explorer"],
        label_visibility="collapsed",
    )
    st.markdown('<hr>', unsafe_allow_html=True)
    # Quick stats
    n_groups = match["match_group_id"].nunique() if not match.empty else 0
    st.markdown(
        f'<div style="font-size:.75rem;color:#475569;line-height:2">'
        f'<span style="color:#94a3b8;font-weight:600">Scraped</span> 11 Mar 2026<br>'
        f'<span style="color:#94a3b8;font-weight:600">Products</span> {len(proc):,}<br>'
        f'<span style="color:#94a3b8;font-weight:600">Stores</span> {len(stores_all)}<br>'
        f'<span style="color:#94a3b8;font-weight:600">Cities</span> {len(cities_all)}<br>'
        f'<span style="color:#94a3b8;font-weight:600">Match groups</span> {n_groups:,}</div>',
        unsafe_allow_html=True,
    )
    st.markdown('<hr>', unsafe_allow_html=True)
    for s in stores_all:
        color = STORE_COLORS.get(s, "#94a3b8")
        n = len(proc[proc["store"] == s])
        st.markdown(
            f'<div style="display:flex;align-items:center;gap:.5rem;margin:.25rem 0">'
            f'<div style="width:9px;height:9px;border-radius:50%;background:{color}"></div>'
            f'<span style="font-size:.8rem;color:#94a3b8">{s.capitalize()}</span>'
            f'<span style="font-size:.73rem;color:#475569;margin-left:auto">{n:,}</span></div>',
            unsafe_allow_html=True,
        )


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — EXECUTIVE SUMMARY
# ═══════════════════════════════════════════════════════════════════════════════
if page == "Executive Summary":
    page_banner("🛒", "Executive Summary",
                "Pakistan Supermarket Price Intelligence — 6 chains · 5 cities · March 2026")
    divider()

    # KPI strip
    kpi_data = [
        ("116,346", "Products collected"),
        ("6",       "Supermarket chains"),
        ("5",       "Cities covered"),
        ("558",     "Matched groups"),
        ("3.54×",   "Avg price spread"),
        ("97%",     "Validation pass rate"),
    ]
    cols = st.columns(6)
    for (v, l), c in zip(kpi_data, cols):
        with c:
            kpi(v, l)

    divider()

    # Charts row
    c_left, c_right = st.columns([1.1, 1], gap="large")
    with c_left:
        sh("Products per Store")
        if not proc.empty:
            sc = proc.groupby("store").size().reset_index(name="n").sort_values("n")
            fig = px.bar(sc, x="n", y="store", orientation="h",
                         color="store", color_discrete_map=STORE_COLORS,
                         text="n", template=PLOTLY_TEMPLATE,
                         labels={"n": "Products", "store": ""})
            fig.update_traces(texttemplate="%{x:,}", textposition="outside",
                              marker_line_width=0)
            fig.update_layout(showlegend=False, height=285,
                              margin=dict(l=0, r=70, t=10, b=10),
                              plot_bgcolor="#fff", paper_bgcolor="#fff",
                              yaxis_tickfont_size=12)
            st.plotly_chart(fig, use_container_width=True)

    with c_right:
        sh("City Distribution")
        if not proc.empty:
            cc = proc.groupby("city").size().reset_index(name="n")
            fig2 = px.pie(cc, names="city", values="n",
                          color="city", color_discrete_map=CITY_COLORS,
                          hole=0.55, template=PLOTLY_TEMPLATE)
            fig2.update_traces(textposition="outside",
                               textinfo="label+percent",
                               textfont_size=12,
                               marker_line_width=2,
                               marker_line_color="#f8fafc")
            fig2.update_layout(showlegend=False, height=285,
                               margin=dict(l=0, r=0, t=10, b=10),
                               paper_bgcolor="#fff")
            st.plotly_chart(fig2, use_container_width=True)

    divider()

    # Pipeline flow
    sh("Data Pipeline")
    pc1, pc2, pc3, pc4 = st.columns(4)
    stages = [
        ("🕷️", "Scraping", "116,346 rows collected\nREST API + Playwright",   "#dbeafe", "#1d4ed8"),
        ("🧹", "Cleaning", "Types · dedup · unit extraction\n81,610 units resolved","#dcfce7","#15803d"),
        ("✅", "Validation", "15 automated checks\n11 passed, 4 warnings",        "#fef9c3","#b45309"),
        ("🔗", "Matching",  "Exact + fuzzy resolution\n558 groups · 10,613 rows",  "#fce7f3","#9d174d"),
    ]
    for (icon, title, body, bg, color), col in zip(stages, [pc1, pc2, pc3, pc4]):
        with col:
            lines = body.split("\n")
            st.markdown(
                f'<div class="stage" style="background:{bg}">'
                f'<div style="font-size:1.5rem">{icon}</div>'
                f'<div style="font-weight:700;color:{color};margin:.3rem 0 .2rem;font-size:.88rem">{title}</div>'
                f'<div style="font-size:.78rem;color:#374151;line-height:1.6">'
                f'{lines[0]}<br><span style="color:#6b7280">{lines[1] if len(lines)>1 else ""}</span></div>'
                f'</div>',
                unsafe_allow_html=True,
            )

    divider()
    sh("Key Findings")
    cb("🏆 <b>Imtiaz (Karachi)</b> is Pakistan's cheapest major supermarket — price index 0.882, cheapest in <b>76.9% of matched products</b>. Average savings vs Springs: ~₨2,000–3,000/month for a typical household.", "green")
    cb("📊 <b>Same product, 3.54× price difference on average.</b> For 558 matched groups the max-store price is 3.54× higher than the min-store price. A packaged juice costs ₨38 at one store and ₨986 at another.", "red")
    cb("🔗 <b>Naheed ↔ Springs synchronisation r = 0.997.</b> Two chains in different cities move prices in near-perfect lockstep — almost certainly due to shared national distributor contracts.", "amber")
    cb("🌐 <b>Prices are nationally set, not locally.</b> Cross-city Pearson r ranges 0.80–0.99. Geographic variation in grocery prices is negligible relative to store-level variation.", "blue")
    cb("🚫 <b>Al-Fatah never undercuts (price leadership = 0%).</b> It holds a stable premium position with the lowest volatility of any store (CV = 0.024).", "amber")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — STORE PERFORMANCE
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "Store Performance":
    page_banner("🏪", "Store Performance",
                "Competitive positioning of each chain — based on 10,613 cross-store matched products")
    divider()

    if s_met.empty:
        st.info("Store metrics file not found.")
    else:
        s_met = s_met.copy()
        s_met["label"] = s_met.apply(
            lambda r: f"{r['store'].capitalize()} · {r['city'].capitalize()}", axis=1
        )

        sh("Price Index  (1.0 = market average)")
        tmp = s_met.sort_values("avg_category_price_index")
        fig = px.bar(tmp, x="avg_category_price_index", y="label",
                     orientation="h", color="store", color_discrete_map=STORE_COLORS,
                     text="avg_category_price_index", template=PLOTLY_TEMPLATE,
                     labels={"avg_category_price_index": "Price Index", "label": ""})
        fig.update_traces(texttemplate="%{x:.3f}", textposition="outside",
                          marker_line_width=0)
        fig.add_vline(x=1.0, line_dash="dot", line_color="#94a3b8",
                      annotation_text="Market Avg", annotation_position="top")
        fig.update_layout(showlegend=False, height=380,
                          margin=dict(l=0, r=90, t=10, b=10),
                          plot_bgcolor="#fff", paper_bgcolor="#fff")
        st.plotly_chart(fig, use_container_width=True)
        cb("Imtiaz (0.882) and all Metro locations (0.925–0.969) are structurally below the market average. Springs (1.214) and Naheed (1.067) occupy a clear premium tier.", "blue")

        divider()
        c1, c2 = st.columns(2, gap="large")
        with c1:
            sh("Price Leadership Frequency")
            st.caption("How often is this store the cheapest option?")
            tmp2 = s_met.sort_values("price_leadership_freq", ascending=False).copy()
            tmp2["pct"] = tmp2["price_leadership_freq"] * 100
            fig2 = px.bar(tmp2, x="label", y="pct",
                          color="store", color_discrete_map=STORE_COLORS,
                          text="pct", template=PLOTLY_TEMPLATE,
                          labels={"label": "", "pct": "Leadership %"})
            fig2.update_traces(texttemplate="%{y:.1f}%", textposition="outside",
                               marker_line_width=0)
            fig2.update_layout(showlegend=False, height=340,
                               margin=dict(l=0, r=0, t=10, b=60),
                               plot_bgcolor="#fff", paper_bgcolor="#fff",
                               xaxis=dict(tickangle=30, tickfont=dict(size=10)))
            st.plotly_chart(fig2, use_container_width=True)

        with c2:
            sh("Price Volatility  (avg CV)")
            st.caption("Higher = more erratic pricing relative to category.")
            tmp3 = s_met.sort_values("price_volatility_score", ascending=False)
            fig3 = px.bar(tmp3, x="label", y="price_volatility_score",
                          color="store", color_discrete_map=STORE_COLORS,
                          text="price_volatility_score", template=PLOTLY_TEMPLATE,
                          labels={"label": "", "price_volatility_score": "Avg CV"})
            fig3.update_traces(texttemplate="%{y:.3f}", textposition="outside",
                               marker_line_width=0)
            fig3.update_layout(showlegend=False, height=340,
                               margin=dict(l=0, r=0, t=10, b=60),
                               plot_bgcolor="#fff", paper_bgcolor="#fff",
                               xaxis=dict(tickangle=30, tickfont=dict(size=10)))
            st.plotly_chart(fig3, use_container_width=True)

        divider()
        sh("Median Price Deviation from Market")
        tmp4 = s_met.sort_values("median_price_deviation")
        fig4 = px.bar(tmp4, x="median_price_deviation", y="label", orientation="h",
                      color="median_price_deviation", template=PLOTLY_TEMPLATE,
                      color_continuous_scale=["#22c55e", "#f8fafc", "#ef4444"],
                      text="median_price_deviation",
                      labels={"median_price_deviation": "Deviation (negative = cheaper)", "label": ""})
        fig4.update_traces(texttemplate="%{x:+.3f}", textposition="outside",
                           marker_line_width=0)
        fig4.add_vline(x=0, line_color="#0f172a", line_width=1.5)
        fig4.update_layout(showlegend=False, coloraxis_showscale=False, height=340,
                           margin=dict(l=0, r=90, t=10, b=10),
                           plot_bgcolor="#fff", paper_bgcolor="#fff")
        st.plotly_chart(fig4, use_container_width=True)
        cb("Springs charges a 15% premium over the market median (+0.150). Imtiaz is 11.8% below (−0.118). This 27-percentage-point gap compounds to ₨2,000–3,000 monthly savings on a full grocery basket.", "amber")

        divider()
        sh("Summary Table")
        tbl = s_met[["store","city","avg_category_price_index","price_leadership_freq",
                     "price_volatility_score","median_price_deviation"]].copy()
        tbl.columns = ["Store","City","Price Index","Leadership %","Volatility (CV)","Median Dev"]
        tbl["Price Index"]    = tbl["Price Index"].map(lambda v: f"{v:.3f}")
        tbl["Leadership %"]   = tbl["Leadership %"].map(lambda v: f"{v:.1%}" if pd.notna(v) else "—")
        tbl["Volatility (CV)"]= tbl["Volatility (CV)"].map(lambda v: f"{v:.3f}")
        tbl["Median Dev"]     = tbl["Median Dev"].map(lambda v: f"{v:+.3f}")
        st.dataframe(tbl, use_container_width=True, hide_index=True, height=360)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — PRICE DISPERSION
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "Price Dispersion":
    page_banner("📊", "Price Dispersion",
                "How wide are price gaps for the same product across stores?")
    divider()

    if match.empty:
        st.info("Matched data not found.")
    else:
        grp = match.drop_duplicates("match_group_id").copy()

        cv_mean = grp["group_cv"].mean()              if "group_cv"           in grp.columns else np.nan
        sr_mean = grp["group_spread_ratio"].dropna().mean() if "group_spread_ratio" in grp.columns else np.nan
        pr_mean = grp["group_price_range"].mean()     if "group_price_range"  in grp.columns else np.nan

        c1, c2, c3, c4 = st.columns(4)
        with c1: st.metric("Avg CV", f"{cv_mean:.3f}" if not np.isnan(cv_mean) else "—",
                           help="Coefficient of Variation — price dispersion per group")
        with c2: st.metric("Avg Spread Ratio", f"{sr_mean:.2f}×" if not np.isnan(sr_mean) else "—",
                           help="Max price ÷ min price for the same product")
        with c3: st.metric("Avg Price Range", f"₨{pr_mean:,.0f}" if not np.isnan(pr_mean) else "—",
                           help="Max minus min price for the same product")
        with c4: st.metric("Product Groups", f"{grp['match_group_id'].nunique():,}")

        cb(f"The cheapest store charges on average <b>{sr_mean:.1f}× less</b> than the most expensive for the same product. "
           f"Typical price range per item: <b>₨{pr_mean:.0f}</b>.", "red")
        divider()

        c_l, c_r = st.columns(2, gap="large")
        with c_l:
            sh("Spread Ratio Distribution")
            if "group_spread_ratio" in grp.columns:
                sr = grp["group_spread_ratio"].dropna()
                sr = sr[sr < sr.quantile(0.97)]
                fig = px.histogram(sr, nbins=50, template=PLOTLY_TEMPLATE,
                                   color_discrete_sequence=[ACCENT],
                                   labels={"value": "Max ÷ Min Price", "count": "Groups"})
                fig.add_vline(x=1.0, line_dash="dot", line_color="#94a3b8",
                              annotation_text="No gap")
                fig.add_vline(x=sr.mean(), line_dash="dash", line_color="#ef4444",
                              annotation_text=f"Mean {sr.mean():.1f}×",
                              annotation_position="top right")
                fig.update_layout(height=310, showlegend=False,
                                  margin=dict(l=0,r=0,t=20,b=10),
                                  plot_bgcolor="#fff", paper_bgcolor="#fff")
                st.plotly_chart(fig, use_container_width=True)

        with c_r:
            sh("CV Distribution")
            if "group_cv" in grp.columns:
                cv = grp["group_cv"].dropna()
                cv = cv[cv < cv.quantile(0.97)]
                fig2 = px.histogram(cv, nbins=50, template=PLOTLY_TEMPLATE,
                                    color_discrete_sequence=["#a855f7"],
                                    labels={"value": "Coefficient of Variation", "count": "Groups"})
                fig2.add_vline(x=cv.mean(), line_dash="dash", line_color="#ef4444",
                               annotation_text=f"Mean {cv.mean():.3f}",
                               annotation_position="top right")
                fig2.update_layout(height=310, showlegend=False,
                                   margin=dict(l=0,r=0,t=20,b=10),
                                   plot_bgcolor="#fff", paper_bgcolor="#fff")
                st.plotly_chart(fig2, use_container_width=True)

        divider()
        sh("Most Price-Dispersed Products")
        if all(c in match.columns for c in ["group_cv","group_min_price","group_max_price","name"]):
            top = (
                match.drop_duplicates("match_group_id")
                .nlargest(12, "group_cv")
                [["name","category","group_cv","group_min_price","group_max_price","group_price_range"]]
                .copy()
            )
            top["Spread"]   = (top["group_max_price"] / top["group_min_price"]).map(lambda v: f"{v:.1f}×")
            top["CV"]       = top["group_cv"].map(lambda v: f"{v:.3f}")
            top["Min (₨)"]  = top["group_min_price"].map(lambda v: f"₨{v:,.0f}")
            top["Max (₨)"]  = top["group_max_price"].map(lambda v: f"₨{v:,.0f}")
            top["Range (₨)"]= top["group_price_range"].map(lambda v: f"₨{v:,.0f}")
            top = top.rename(columns={"name": "Product", "category": "Category"})
            st.dataframe(top[["Product","Category","CV","Min (₨)","Max (₨)","Range (₨)","Spread"]],
                         use_container_width=True, hide_index=True, height=420)

        divider()
        sh("Average CV by Category  (top 18)")
        if "category" in match.columns and "group_cv" in match.columns:
            cat_cv = (
                match.groupby("category")["group_cv"]
                .agg(avg_cv="mean", n="count").reset_index()
                .query("n >= 5")
                .sort_values("avg_cv", ascending=False)
                .head(18)
            )
            fig3 = px.bar(cat_cv, x="avg_cv", y="category", orientation="h",
                          color="avg_cv", color_continuous_scale="RdYlGn_r",
                          text="avg_cv", template=PLOTLY_TEMPLATE,
                          labels={"avg_cv": "Avg CV", "category": ""})
            fig3.update_traces(texttemplate="%{x:.3f}", textposition="outside",
                               marker_line_width=0)
            fig3.update_layout(showlegend=False, coloraxis_showscale=False,
                               height=530, margin=dict(l=0,r=80,t=10,b=10),
                               plot_bgcolor="#fff", paper_bgcolor="#fff",
                               yaxis_tickfont_size=11)
            st.plotly_chart(fig3, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — MARKET COMPETITION
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "Market Competition":
    page_banner("⚔️", "Market Competition",
                "Leader Dominance Index · Cross-store price synchronisation · Structural dynamics")
    divider()

    if not ldi.empty:
        sh("Leader Dominance Index (LDI)")
        st.caption("Share of matched products where this store holds the lowest price. "
                   "Metro > 1.0 reflects wins across 5 cities simultaneously.")

        c_ldi, c_rank = st.columns([1.3, 1], gap="large")
        with c_ldi:
            fig = px.bar(ldi.sort_values("ldi"),
                         x="ldi", y="store", orientation="h",
                         color="store", color_discrete_map=STORE_COLORS,
                         text="ldi", template=PLOTLY_TEMPLATE,
                         labels={"ldi": "LDI Score", "store": ""})
            fig.update_traces(texttemplate="%{x:.3f}", textposition="outside",
                              marker_line_width=0)
            fig.add_vline(x=1.0, line_dash="dot", line_color="#94a3b8",
                          annotation_text="100% wins threshold")
            fig.update_layout(showlegend=False, height=310,
                              margin=dict(l=0,r=90,t=10,b=10),
                              plot_bgcolor="#fff", paper_bgcolor="#fff")
            st.plotly_chart(fig, use_container_width=True)

        with c_rank:
            st.markdown("#### Rankings")
            for _, row in ldi.sort_values("ldi", ascending=False).iterrows():
                bar_pct = min(float(row["ldi"]) / 4.5, 1.0) * 100
                c = STORE_COLORS.get(row["store"], "#94a3b8")
                wins = int(row.get("n_leader", 0))
                st.markdown(
                    f'<div style="margin:.55rem 0">'
                    f'<div style="display:flex;justify-content:space-between;font-size:.84rem;margin-bottom:3px">'
                    f'<b>{row["store"].capitalize()}</b>'
                    f'<span style="color:#6b7280">LDI {row["ldi"]:.4f} · {wins:,} wins</span></div>'
                    f'<div style="background:#f1f5f9;border-radius:6px;height:8px">'
                    f'<div style="background:{c};width:{bar_pct:.0f}%;height:8px;border-radius:6px"></div>'
                    f'</div></div>',
                    unsafe_allow_html=True,
                )

    divider()

    if not cxs.empty:
        sh("Cross-Store Price Synchronisation")
        st.caption("Pearson r of matched product prices. Perfect sync = r=1, competitive undercutting = r<0.")
        try:
            sync = cxs.astype(float)
            for col in sync.columns:
                if col in sync.index:
                    sync.loc[col, col] = 1.0
            fig2 = px.imshow(
                sync, text_auto=".2f", aspect="auto",
                color_continuous_scale=["#ef4444", "#f8fafc", "#3b82f6"],
                zmin=-1, zmax=1, template=PLOTLY_TEMPLATE,
                labels=dict(color="Pearson r"),
            )
            fig2.update_traces(textfont_size=14)
            fig2.update_layout(height=420, margin=dict(l=0,r=0,t=10,b=10),
                               paper_bgcolor="#fff",
                               coloraxis_colorbar=dict(len=0.8, thickness=13))
            st.plotly_chart(fig2, use_container_width=True)
        except Exception:
            st.dataframe(cxs)

        cb("🔴 <b>Naheed ↔ Springs r = 0.997</b> — different cities, near-identical prices. "
           "These stores behave as one pricing entity despite geographic separation.", "red")
        cb("🔵 <b>Metro ↔ Imtiaz r = −0.504</b> — strong negative correlation. "
           "Where Metro overcharges, Imtiaz undercuts. Classic competitive price response.", "blue")

    divider()

    if not ldi_c.empty and "ldi" in ldi_c.columns and "category" in ldi_c.columns:
        sh("LDI by Category — Who dominates each aisle?")
        try:
            pivot = ldi_c.pivot_table(index="store", columns="category", values="ldi", aggfunc="first")
            top_cats = pivot.notna().sum().nlargest(20).index
            pivot = pivot.loc[:, top_cats].fillna(0)
            fig3 = px.imshow(pivot, text_auto=".2f", aspect="auto",
                             color_continuous_scale="Blues", template=PLOTLY_TEMPLATE,
                             labels=dict(color="LDI"))
            fig3.update_layout(height=340, margin=dict(l=0,r=0,t=10,b=10),
                               paper_bgcolor="#fff",
                               xaxis=dict(tickangle=40, tickfont=dict(size=10)))
            st.plotly_chart(fig3, use_container_width=True)
        except Exception:
            st.dataframe(ldi_c.head(40))

    divider()

    sh("Key Correlation: Number of Stores vs Price Dispersion")
    corr_data = summ.get("correlations", {}).get("size_vs_dispersion", {})
    if corr_data:
        r  = corr_data.get("pearson_r", 0)
        pv = corr_data.get("pearson_p", 1)
        c1, c2, c3 = st.columns(3)
        with c1: st.metric("Pearson r", f"{r:.3f}")
        with c2: st.metric("p-value", f"{pv:.4f}")
        with c3: st.metric("Significant?", "Yes ✅" if pv < 0.05 else "No ✗")
        cb(f"<b>r = {r:.3f}, p {'< 0.001' if pv < 0.001 else f'= {pv:.4f}'}</b> — "
           "Products sold in more stores have meaningfully lower price dispersion. "
           "More competition → narrower spreads.", "green")

    if not match.empty and "group_cv" in match.columns and "group_n_stores" in match.columns:
        gd = match.drop_duplicates("match_group_id").dropna(subset=["group_n_stores","group_cv"])
        means = gd.groupby("group_n_stores")["group_cv"].mean().reset_index()
        fig4 = px.strip(gd, x="group_n_stores", y="group_cv",
                        color_discrete_sequence=[ACCENT], template=PLOTLY_TEMPLATE,
                        labels={"group_n_stores": "Stores in Match Group",
                                "group_cv": "Price CV"})
        fig4.add_trace(go.Scatter(
            x=means["group_n_stores"], y=means["group_cv"],
            mode="lines+markers", name="Mean CV",
            line=dict(color="#ef4444", width=2.5, dash="dash"),
            marker=dict(size=8, color="#ef4444"),
        ))
        fig4.update_layout(height=320, showlegend=False,
                           margin=dict(l=0,r=0,t=10,b=10),
                           plot_bgcolor="#fff", paper_bgcolor="#fff")
        st.plotly_chart(fig4, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 5 — CITY ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "City Analysis":
    page_banner("🗺️", "City Analysis",
                "Are grocery prices different across Pakistan's major cities?")
    divider()

    if proc.empty:
        st.info("Processed data not found.")
    else:
        c1, c2 = st.columns(2, gap="large")
        with c1:
            sh("Average & Median Price by City")
            city_agg = proc.groupby("city")["price"].agg(mean="mean", median="median").reset_index()
            fig = go.Figure()
            for metric, color, name in [("mean","#3b82f6","Mean"),("median","#22c55e","Median")]:
                fig.add_trace(go.Bar(
                    x=city_agg["city"].str.capitalize(), y=city_agg[metric],
                    name=name, marker_color=color,
                    text=city_agg[metric].map(lambda v: f"₨{v:.0f}"),
                    textposition="outside",
                ))
            fig.update_layout(barmode="group", template=PLOTLY_TEMPLATE,
                              height=320, margin=dict(l=0,r=0,t=10,b=10),
                              plot_bgcolor="#fff", paper_bgcolor="#fff",
                              legend=dict(orientation="h", y=1.08),
                              yaxis_title="Price (PKR)")
            st.plotly_chart(fig, use_container_width=True)

        with c2:
            sh("Price Distribution by City")
            trimmed = proc[proc["price"] <= proc["price"].quantile(0.95)]
            fig2 = px.violin(trimmed, x="city", y="price", color="city",
                             color_discrete_map=CITY_COLORS, box=True, points=False,
                             template=PLOTLY_TEMPLATE,
                             labels={"city": "", "price": "Price (PKR)"})
            fig2.update_layout(showlegend=False, height=320,
                               margin=dict(l=0,r=0,t=10,b=10),
                               plot_bgcolor="#fff", paper_bgcolor="#fff")
            st.plotly_chart(fig2, use_container_width=True)

        divider()
        sh("City Price Correlation Matrix")
        st.caption("Based on overlapping Metro products — same chain, 5 cities. "
                   "High r means national distributor sets the price; local competition has little effect.")

        h1, h2 = st.columns(2, gap="large")
        for (corr_df, title), col in zip([(cp,"Pearson r"),(cs,"Spearman ρ")], [h1, h2]):
            with col:
                if not corr_df.empty:
                    try:
                        fig3 = px.imshow(
                            corr_df.astype(float), text_auto=".3f", aspect="auto",
                            color_continuous_scale=["#fef3c7","#3b82f6"],
                            zmin=0.7, zmax=1.0, template=PLOTLY_TEMPLATE,
                            labels=dict(color=title), title=title,
                        )
                        fig3.update_traces(textfont_size=13)
                        fig3.update_layout(height=370, margin=dict(l=0,r=0,t=30,b=10),
                                           paper_bgcolor="#fff",
                                           coloraxis_colorbar=dict(len=0.8, thickness=12))
                        st.plotly_chart(fig3, use_container_width=True)
                    except Exception:
                        st.dataframe(corr_df)

        cb("All city pairs have Pearson r > 0.80. The national supply chain — not local competition — "
           "drives Pakistani grocery pricing. Lahore shows the weakest inter-city correlation (r ≈ 0.80 "
           "vs Faisalabad), likely due to its larger and more diverse retail ecosystem.", "blue")

        divider()
        sh("Metro Cash & Carry — Cross-City Price Comparison")
        st.caption("Identical chain, 5 cities. Ideal natural experiment for geographic price variation.")
        metro = proc[proc["store"] == "metro"] if "store" in proc.columns else pd.DataFrame()
        if not metro.empty and "category" in metro.columns:
            cats = ["All categories"] + sorted(metro["category"].dropna().unique().tolist())
            sel_cat = st.selectbox("Category filter", cats)
            m_f = metro if sel_cat == "All categories" else metro[metro["category"] == sel_cat]
            m_city = m_f.groupby("city")["price"].agg(mean="mean", count="count").reset_index()
            m_city.columns = ["city","mean_price","n_products"]
            fig4 = px.bar(m_city, x="city", y="mean_price",
                          color="city", color_discrete_map=CITY_COLORS,
                          text="mean_price", template=PLOTLY_TEMPLATE,
                          labels={"mean_price": "Avg Price (PKR)", "city": ""})
            fig4.update_traces(texttemplate="₨%{y:.0f}", textposition="outside",
                               marker_line_width=0)
            fig4.update_layout(showlegend=False, height=320,
                               margin=dict(l=0,r=0,t=10,b=10),
                               plot_bgcolor="#fff", paper_bgcolor="#fff")
            st.plotly_chart(fig4, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 6 — DATA EXPLORER
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "Data Explorer":
    page_banner("🔍", "Data Explorer",
                "Browse and filter the full processed and matched datasets")
    divider()

    layer = st.radio(
        "Dataset",
        ["Processed products  (116,346 rows)", "Matched groups  (10,613 rows)"],
        horizontal=True,
    )
    df = proc if layer.startswith("Processed") else match
    divider()

    if df.empty:
        st.info("Dataset not loaded.")
    else:
        f1, f2, f3, f4 = st.columns(4)
        with f1:
            sel_store = st.multiselect("Store", sorted(df["store"].unique()) if "store" in df.columns else [])
        with f2:
            sel_city  = st.multiselect("City",  sorted(df["city"].unique())  if "city"  in df.columns else [])
        with f3:
            cats = sorted(df["category"].dropna().unique()) if "category" in df.columns else []
            sel_cat = st.multiselect("Category", cats)
        with f4:
            if "price" in df.columns:
                p99   = float(df["price"].quantile(0.99))
                p_max = st.slider("Max Price (PKR)", 0, int(p99), int(p99))

        df_f = df.copy()
        if sel_store: df_f = df_f[df_f["store"].isin(sel_store)]
        if sel_city:  df_f = df_f[df_f["city"].isin(sel_city)]
        if sel_cat:   df_f = df_f[df_f["category"].isin(sel_cat)]
        if "price" in df_f.columns:
            df_f = df_f[df_f["price"] <= p_max]

        divider()
        c1, c2, c3, c4 = st.columns(4)
        with c1: st.metric("Rows shown", f"{min(len(df_f),5000):,} of {len(df_f):,}")
        with c2:
            if "price" in df_f.columns and len(df_f):
                st.metric("Avg Price", f"₨{df_f['price'].mean():,.0f}")
        with c3:
            if "price" in df_f.columns and len(df_f):
                st.metric("Median Price", f"₨{df_f['price'].median():,.0f}")
        with c4:
            if "store" in df_f.columns:
                st.metric("Stores", df_f["store"].nunique())

        if "price" in df_f.columns and len(df_f) > 0:
            fig = px.histogram(df_f["price"].dropna(), nbins=60, template=PLOTLY_TEMPLATE,
                               color_discrete_sequence=[ACCENT],
                               labels={"value": "Price (PKR)", "count": "Products"})
            fig.update_layout(height=220, showlegend=False,
                              margin=dict(l=0,r=0,t=10,b=10),
                              plot_bgcolor="#fff", paper_bgcolor="#fff")
            st.plotly_chart(fig, use_container_width=True)

        keep_cols = [c for c in ["store","city","name","brand","category",
                                  "price","quantity","unit","price_per_unit",
                                  "match_group_id","match_method","match_confidence"]
                     if c in df_f.columns]
        st.dataframe(df_f[keep_cols].head(5000), use_container_width=True,
                     hide_index=True, height=460)
