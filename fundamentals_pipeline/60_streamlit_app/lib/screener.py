"""Screener data layer — one row per company from the published parquet artifacts.

Reuses :func:`lib.data.load_latest_data` (the same cached parquet loader the
detail page uses) — there is **no Databricks connection at runtime**. The wide
frame is built by pivoting the long-format `dashboard_metrics.parquet` to the
latest available fiscal year per ticker.

Universe flags (`is_favorite` / `in_sp500` / `in_r3000`), the GICS `sector`, the
Yahoo `industry` (sub-sector grouping key, schema v8), and **Market Cap** (a
`Market Cap` metric row) all come from `50_publish/51__export_dashboard_data.py`.
Older artifacts that predate a field simply yield its default — all-False flags,
``"Unknown"`` sector/industry, an empty Market Cap column — so the screener degrades
gracefully until the next publish.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from .data import load_latest_data

MARKET_CAP = "Market Cap"

# Screener default columns (display order).
DEFAULT_COLUMNS = [
    "Market Cap", "P/E", "P/S", "Net Margin %", "ROE %", "Revenue YoY %", "EV/EBITDA",
]
# Universe → flag column in the frame ("" = no filter).
UNIVERSE_FLAGS = {
    "All":          "",
    "S&P 500":      "in_sp500",
    "Russell 3000": "in_r3000",
    "Favorites":    "is_favorite",
}
_FLAG_COLS = ("is_favorite", "in_sp500", "in_r3000")

# Bucket for tickers with no/NULL/legacy sector (the app's "Unknown" bucket).
UNKNOWN_SECTOR = "Unknown"
# Same "Unknown" bucket for the Yahoo `industry` sub-sector key (NULL / pre-v8 artifacts).
UNKNOWN_INDUSTRY = "Unknown"
# No-op default for the (data-driven) industry filter — mirrors SECTORS[0] "All sectors".
ALL_INDUSTRIES = "All industries"
# 11 canonical GICS sectors (sorted) + a no-op default. Mirrors the hardcoded
# UNIVERSE_FLAGS pattern; "All sectors" is the no-op default.
SECTORS = ["All sectors"] + sorted([
    "Energy", "Materials", "Industrials", "Consumer Discretionary",
    "Consumer Staples", "Health Care", "Financials", "Information Technology",
    "Communication Services", "Utilities", "Real Estate",
])


def _as_bool(s: pd.Series) -> pd.Series:
    """Coerce a flag column to native bool, tolerant of legacy stringified values."""
    return s.map(lambda v: v is True or str(v).strip().lower() in ("true", "1")).astype(bool)


@st.cache_data(ttl=3600, show_spinner="Building screener…")
def build_screener_frame() -> tuple[pd.DataFrame, dict[str, str], list[str]]:
    """Return ``(wide, unit_map, metric_order)``.

    ``wide``: one row per ticker — columns ``ticker``, ``company``, the three
    universe flags, ``fiscal_year`` (the latest FY used) and one column per
    metric (pivoted from long format). ``unit_map``: metric → unit
    ('percent' / 'ratio' / 'usd') for display formatting. ``metric_order``:
    metrics ordered by `metrics_hierarchy.sort_order`, Market Cap first.
    """
    _data, metrics, meta = load_latest_data()

    m = metrics[metrics["period_type"] == "FY"].copy()
    # `metric` is a category dtype after _optimize_dtypes — cast to str so the
    # pivot doesn't materialise ghost columns for unused categories.
    m["metric"] = m["metric"].astype(str)

    # For each (ticker, metric) keep ITS latest available fiscal_year —
    # NOT a single year per ticker. Rows such as TTM Intrinsic Value metrics
    # (stamped on a future fiscal_year, e.g. 2026) or Market Cap (CALENDAR
    # year) appear ahead of the latest FY of core metrics; a per-ticker MAX
    # would freeze the whole row to that year and leave Net Margin/ROE/P-E…
    # as None even when they exist in the prior year (VNOM bug). This also
    # matches the semantics used by the detail page (latest value per metric).
    latest = m.groupby(["ticker", "metric"], observed=True)["fiscal_year"].transform("max")
    m = m[m["fiscal_year"] == latest]

    # unit per metric (for formatting) and order from metrics_hierarchy.
    unit_map = (
        m.dropna(subset=["unit"]).drop_duplicates("metric")
        .assign(unit=lambda d: d["unit"].astype(str))
        .set_index("metric")["unit"].to_dict()
    )
    unit_map.setdefault(MARKET_CAP, "usd")

    order = (
        m.drop_duplicates("metric")
        .assign(so=lambda d: pd.to_numeric(d["sort_order"], errors="coerce"))
        .sort_values(["so", "metric"], na_position="last")["metric"].tolist()
    )
    metric_order = [MARKET_CAP] + [x for x in order if x != MARKET_CAP]

    # long → wide (in pandas; keeps the SQL simple).
    wide = m.pivot_table(index="ticker", columns="metric", values="value", aggfunc="first")
    wide.columns.name = None
    fy = m.groupby("ticker", observed=True)["fiscal_year"].max().rename("fiscal_year")
    wide = wide.join(fy).reset_index()

    # company + universe flags + GICS sector from meta.
    info = pd.DataFrame(meta.get("tickers", []))
    if "ticker" in info.columns:
        keep = ["ticker"] + [c for c in ("company", "sector", "industry", *_FLAG_COLS) if c in info.columns]
        wide = wide.merge(info[keep], on="ticker", how="left")
    if "company" not in wide.columns:
        wide["company"] = wide["ticker"]
    wide["company"] = wide["company"].fillna(wide["ticker"])
    for c in _FLAG_COLS:
        wide[c] = _as_bool(wide[c]) if c in wide.columns else False

    # Sector: NULL / missing / legacy fixtures → "Unknown" bucket.
    if "sector" not in wide.columns:
        wide["sector"] = UNKNOWN_SECTOR
    wide["sector"] = (
        wide["sector"].astype("object").where(wide["sector"].notna(), UNKNOWN_SECTOR)
        .astype(str).str.strip().replace("", UNKNOWN_SECTOR)
    )

    # Industry (Yahoo sub-sector): NULL / missing / pre-v8 artifacts → "Unknown" bucket.
    if "industry" not in wide.columns:
        wide["industry"] = UNKNOWN_INDUSTRY
    wide["industry"] = (
        wide["industry"].astype("object").where(wide["industry"].notna(), UNKNOWN_INDUSTRY)
        .astype(str).str.strip().replace("", UNKNOWN_INDUSTRY)
    )

    # Market Cap may not exist yet (Release predating schema v3).
    if MARKET_CAP not in wide.columns:
        wide[MARKET_CAP] = pd.NA

    metric_order = [x for x in metric_order if x in wide.columns]
    return wide, unit_map, metric_order


def universe_mask(df: pd.DataFrame, universe: str) -> pd.Series:
    col = UNIVERSE_FLAGS.get(universe, "")
    if not col or col not in df.columns:
        return pd.Series(True, index=df.index)
    return df[col].astype(bool)


def sector_mask(df: pd.DataFrame, sector: str) -> pd.Series:
    """Boolean mask for one GICS sector. ``"All sectors"`` (the default) is a no-op."""
    if not sector or sector == SECTORS[0] or "sector" not in df.columns:
        return pd.Series(True, index=df.index)
    return df["sector"].astype(str) == sector


def industry_options(df: pd.DataFrame) -> list[str]:
    """Sorted industry choices for the screener filter: ``"All industries"`` (no-op default)
    + the distinct non-Unknown industries actually present in the frame. Data-driven (unlike the
    hardcoded 11 GICS ``SECTORS``) because Yahoo carries ~145 industries and the published set
    varies; an artifact with no ``industry`` column yields just the default."""
    if "industry" not in df.columns:
        return [ALL_INDUSTRIES]
    vals = sorted({
        str(x) for x in df["industry"].dropna().unique()
        if str(x).strip() and str(x) != UNKNOWN_INDUSTRY
    })
    return [ALL_INDUSTRIES] + vals


def industry_mask(df: pd.DataFrame, industry: str) -> pd.Series:
    """Boolean mask for one Yahoo industry. ``"All industries"`` (the default) is a no-op."""
    if not industry or industry == ALL_INDUSTRIES or "industry" not in df.columns:
        return pd.Series(True, index=df.index)
    return df["industry"].astype(str) == industry


# Comparison-page metric sets. Valuation multiples take their median EX-NEGATIVES (a negative
# multiple isn't a meaningful "cheapness" reading — matches the screener's ``pe[pe>0]``); the
# quality columns use a plain median (they are legitimately signed).
VALUATION_COLS = ("P/E", "P/FCF", "EV/EBITDA", "P/S", "P/B")
QUALITY_COLS = ("ROE %", "Revenue YoY %")


def industry_summary(
    wide: pd.DataFrame,
    *,
    value_cols: tuple[str, ...] = VALUATION_COLS,
    quality_cols: tuple[str, ...] = QUALITY_COLS,
    min_count: int = 3,
) -> tuple[pd.DataFrame, dict]:
    """Per-industry median valuation multiples for the comparison page.

    Returns ``(summary, info)``:

    * ``summary`` — one row per qualifying industry: ``industry``, ``sector`` (the MODAL GICS
      sector within the group — Yahoo industry does NOT strictly nest under GICS sector, so this
      label is approximate), ``n`` (company count), the median of each present ``value_col`` taken
      EX-NEGATIVES, and the plain median of each present ``quality_col``. Sorted by name.
    * ``info`` — ``{"hidden_small", "min_count", "unknown", "n_industries"}`` so the view can
      report what was dropped (no silent truncation).

    The ``"Unknown"`` industry is excluded from ``summary`` (counted in ``info["unknown"]``);
    groups with ``n < min_count`` are excluded (counted in ``info["hidden_small"]``). NaN-tolerant;
    when there is no usable ``industry`` column it returns an empty frame and zeroed info.
    """
    present_val = [c for c in value_cols if c in wide.columns]
    present_qual = [c for c in quality_cols if c in wide.columns]
    empty_cols = ["industry", "sector", "n", *present_val, *present_qual]
    base_info = {"hidden_small": 0, "min_count": min_count, "unknown": 0, "n_industries": 0}

    if "industry" not in wide.columns:
        return pd.DataFrame(columns=empty_cols), base_info

    df = wide.copy()
    df["industry"] = df["industry"].astype(str)
    unknown = int((df["industry"] == UNKNOWN_INDUSTRY).sum())
    df = df[df["industry"] != UNKNOWN_INDUSTRY]

    rows, hidden_small = [], 0
    for industry, g in df.groupby("industry", sort=True):
        n = len(g)
        if n < min_count:
            hidden_small += 1
            continue
        rec: dict = {"industry": industry, "n": n}
        if "sector" in g.columns:
            modes = g["sector"].mode()
            rec["sector"] = str(modes.iloc[0]) if not modes.empty else UNKNOWN_SECTOR
        else:
            rec["sector"] = UNKNOWN_SECTOR
        for c in present_val:
            s = pd.to_numeric(g[c], errors="coerce")
            s = s[s > 0]                       # ex-negatives: a negative multiple isn't "cheap"
            rec[c] = float(s.median()) if not s.empty else float("nan")
        for c in present_qual:
            s = pd.to_numeric(g[c], errors="coerce").dropna()
            rec[c] = float(s.median()) if not s.empty else float("nan")
        rows.append(rec)

    summary = pd.DataFrame(rows, columns=empty_cols)
    info = {"hidden_small": hidden_small, "min_count": min_count,
            "unknown": unknown, "n_industries": len(summary)}
    return summary, info


def search_mask(df: pd.DataFrame, query: str) -> pd.Series:
    q = (query or "").strip().lower()
    if not q:
        return pd.Series(True, index=df.index)
    return (
        df["ticker"].astype(str).str.lower().str.contains(q, na=False)
        | df["company"].astype(str).str.lower().str.contains(q, na=False)
    )


# ──────────────────────────────────────────────────────────────────────────────
# Fixed bucket system (replaces percentile-clamped range sliders)
# ──────────────────────────────────────────────────────────────────────────────
# Each bucket is ``(label, lo_inclusive, hi_exclusive)``; open ends use ±inf.
# Boundaries deliberately track the green/amber/red bands in ``lib.signals`` so a
# screener chip means the same thing as the detail-page health signal.
_INF = float("inf")
_NINF = float("-inf")

# Valuation multiples where "lower is cheaper" and a loss flips the sign.
_VALUATION_MULTIPLE_BAND = [
    ("Loss", _NINF, 0.0),
    ("Cheap", 0.0, 10.0),
    ("Fair", 10.0, 15.0),
    ("Full", 15.0, 25.0),
    ("Rich", 25.0, _INF),
]
# P/S and P/B sit on a smaller scale than earnings multiples.
_SALES_BOOK_BAND = [
    ("<1", _NINF, 1.0),
    ("1–2", 1.0, 2.0),
    ("2–5", 2.0, 5.0),
    (">5", 5.0, _INF),
]
# Capital-return ratios (Graham bands: 8% / 15%).
_RETURN_BAND = [
    ("Negative", _NINF, 0.0),
    ("0–8%", 0.0, 8.0),
    ("8–15%", 8.0, 15.0),
    (">15%", 15.0, _INF),
]
_ROA_BAND = [
    ("Negative", _NINF, 0.0),
    ("0–5%", 0.0, 5.0),
    ("5–10%", 5.0, 10.0),
    (">10%", 10.0, _INF),
]
_MARGIN_BAND = [
    ("Negative", _NINF, 0.0),
    ("0–10%", 0.0, 10.0),
    ("10–20%", 10.0, 20.0),
    ("20–40%", 20.0, 40.0),
    (">40%", 40.0, _INF),
]
_YOY_BAND = [
    ("Declining", _NINF, 0.0),
    ("0–15%", 0.0, 15.0),
    ("15–30%", 15.0, 30.0),
    (">30%", 30.0, _INF),
]
_YIELD_BAND = [
    ("<3%", _NINF, 3.0),
    ("3–6%", 3.0, 6.0),
    ("6–10%", 6.0, 10.0),
    (">10%", 10.0, _INF),
]
_MOS_BAND = [
    ("Overvalued", _NINF, 0.0),
    ("Slim", 0.0, 15.0),
    ("Decent", 15.0, 30.0),
    ("Wide", 30.0, _INF),
]
_DEBT_EQUITY_BAND = [
    ("Low", _NINF, 0.5),
    ("Mod", 0.5, 1.0),
    ("High", 1.0, _INF),
]
_DEBT_ASSETS_BAND = [
    ("Low", _NINF, 0.3),
    ("Mod", 0.3, 0.5),
    ("High", 0.5, _INF),
]
_CURRENT_RATIO_BAND = [
    ("Tight", _NINF, 1.0),
    ("OK", 1.0, 1.5),
    ("Healthy", 1.5, _INF),
]
# Raw-USD market-cap / enterprise-value size buckets.
_CAP_SIZE_BAND = [
    ("Micro", _NINF, 300e6),
    ("Small", 300e6, 2e9),
    ("Mid", 2e9, 10e9),
    ("Large", 10e9, 200e9),
    ("Mega", 200e9, _INF),
]

# Metric (or (FY)/(TTM)-stripped base name) → explicit bucket table.
EXPLICIT_BUCKETS: dict[str, list[tuple[str, float, float]]] = {
    "P/E": _VALUATION_MULTIPLE_BAND,
    "P/FCF": _VALUATION_MULTIPLE_BAND,
    "EV/EBITDA": _VALUATION_MULTIPLE_BAND,
    "P/S": _SALES_BOOK_BAND,
    "P/B": _SALES_BOOK_BAND,
    "ROE %": _RETURN_BAND,
    "ROIC %": _RETURN_BAND,
    "ROCE %": _RETURN_BAND,
    "CROIC %": _RETURN_BAND,
    "ROA %": _ROA_BAND,
    "Gross Margin %": _MARGIN_BAND,
    "Operating Margin %": _MARGIN_BAND,
    "Net Margin %": _MARGIN_BAND,
    "FCF Margin %": _MARGIN_BAND,
    "Op Cash Flow Margin %": _MARGIN_BAND,
    "Revenue YoY %": _YOY_BAND,
    "Net Income YoY %": _YOY_BAND,
    "Operating Cash Flow YoY %": _YOY_BAND,
    "Free Cash Flow YoY %": _YOY_BAND,
    "Op Cash Flow Yield %": _YIELD_BAND,
    "FCF Yield %": _YIELD_BAND,
    "Earnings Yield %": _YIELD_BAND,
    "Sales Yield %": _YIELD_BAND,
    "Book Yield %": _YIELD_BAND,
    "EBITDA Yield %": _YIELD_BAND,
    "Debt / Equity": _DEBT_EQUITY_BAND,
    "Debt / Assets": _DEBT_ASSETS_BAND,
    "Current Ratio": _CURRENT_RATIO_BAND,
    "Market Cap": _CAP_SIZE_BAND,
    "EV": _CAP_SIZE_BAND,
}

# Generic fallback by display unit for any metric without an explicit band.
_UNIT_BUCKETS: dict[str, list[tuple[str, float, float]]] = {
    "usd": [
        ("Negative", _NINF, 0.0),
        ("<$100M", 0.0, 100e6),
        ("$100M–1B", 100e6, 1e9),
        ("$1–10B", 1e9, 10e9),
        (">$10B", 10e9, _INF),
    ],
    "percent": [
        ("Negative", _NINF, 0.0),
        ("0–10%", 0.0, 10.0),
        ("10–25%", 10.0, 25.0),
        (">25%", 25.0, _INF),
    ],
    "ratio": [
        ("<1", _NINF, 1.0),
        ("1–2", 1.0, 2.0),
        ("2–5", 2.0, 5.0),
        (">5", 5.0, _INF),
    ],
}


def buckets_for(metric: str, unit: str | None) -> list[tuple[str, float, float]]:
    """Resolve the bucket table for a metric.

    Resolution order: exact metric name → the name with its ``(FY)``/``(TTM)``
    suffix stripped → ``MoS %`` prefix (margin-of-safety band) → unit fallback
    ('usd' / 'percent' / 'ratio'). Returns ``[]`` if nothing matches (e.g. an
    unknown unit), which the UI treats as "no filterable buckets".
    """
    if metric in EXPLICIT_BUCKETS:
        return EXPLICIT_BUCKETS[metric]
    base = metric.split(" (")[0].strip()
    if base in EXPLICIT_BUCKETS:
        return EXPLICIT_BUCKETS[base]
    if base.startswith("MoS %"):
        return _MOS_BAND
    return _UNIT_BUCKETS.get(unit or "", [])


def bucket_mask(
    series: pd.Series, selected: list[str], buckets: list[tuple[str, float, float]]
) -> pd.Series:
    """Boolean mask for the buckets selected for one metric.

    The selected buckets are OR-ed together (a company in *any* selected band
    passes). An empty selection is "no filter" → an all-True mask.

    When a filter IS active, NaN rows are EXCLUDED: a company with no value for
    this metric belongs in no bucket. This is intentionally stricter than the
    old ``range_mask``, which kept NaN rows visible at the slider extremes.
    """
    if not selected:
        return pd.Series(True, index=series.index)
    s = pd.to_numeric(series, errors="coerce")
    bounds = {label: (lo, hi) for label, lo, hi in buckets}
    mask = pd.Series(False, index=series.index)
    for label in selected:
        if label not in bounds:
            continue
        lo, hi = bounds[label]
        # NaN comparisons evaluate to False, so missing values drop out here.
        mask |= (s >= lo) & (s < hi)
    return mask
