"""Screener data layer — one row per company from the published parquet artifacts.

Reuses :func:`lib.data.load_latest_data` (the same cached parquet loader the
detail page uses) — there is **no Databricks connection at runtime**. The wide
frame is built by pivoting the long-format `dashboard_metrics.parquet` to the
latest available fiscal year per ticker.

Universe flags (`is_favorite` / `in_sp500` / `in_r3000`), the GICS `sector`, and
**Market Cap** (a `Market Cap` metric row) all come from
`50_publish/51__export_dashboard_data.py` (schema v6). Older artifacts that predate
a field simply yield its default — all-False flags, ``"Unknown"`` sector, an empty
Market Cap column — so the screener degrades gracefully until the next publish.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from .data import load_latest_data

MARKET_CAP = "Market Cap"

# Columnas por defecto del screener (orden de aparición).
DEFAULT_COLUMNS = [
    "Market Cap", "P/E", "P/S", "Net Margin %", "ROE %", "Revenue YoY %", "EV/EBITDA",
]
# Universo → columna de flag en el frame ("" = sin filtro).
UNIVERSE_FLAGS = {
    "All":          "",
    "S&P 500":      "in_sp500",
    "Russell 3000": "in_r3000",
    "Favorites":    "is_favorite",
}
_FLAG_COLS = ("is_favorite", "in_sp500", "in_r3000")

# Bucket for tickers with no/NULL/legacy sector (the app's "Unknown" bucket).
UNKNOWN_SECTOR = "Unknown"
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

    # Para cada (ticker, métrica) quédate con SU último fiscal_year disponible —
    # NO un único año por ticker. Filas como las métricas TTM de Intrinsic Value
    # (estampadas en un fiscal_year futuro, p.ej. 2026) o el Market Cap (año
    # CALENDARIO) van por delante del último FY de las métricas core; un MAX por
    # ticker congelaría toda la fila a ese año y dejaría Net Margin/ROE/P-E… en
    # None aunque sí existan en el año anterior (bug VNOM). Esta es además la
    # misma semántica que usa la página de detalle (último valor por métrica).
    latest = m.groupby(["ticker", "metric"], observed=True)["fiscal_year"].transform("max")
    m = m[m["fiscal_year"] == latest]

    # unit por métrica (para formateo) y orden por metrics_hierarchy.
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

    # long → wide (en pandas; el SQL se mantiene simple).
    wide = m.pivot_table(index="ticker", columns="metric", values="value", aggfunc="first")
    wide.columns.name = None
    fy = m.groupby("ticker", observed=True)["fiscal_year"].max().rename("fiscal_year")
    wide = wide.join(fy).reset_index()

    # company + flags de universo + sector GICS desde meta.
    info = pd.DataFrame(meta.get("tickers", []))
    if "ticker" in info.columns:
        keep = ["ticker"] + [c for c in ("company", "sector", *_FLAG_COLS) if c in info.columns]
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

    # Market Cap puede no existir aún (Release anterior a schema v3).
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
