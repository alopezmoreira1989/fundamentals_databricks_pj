"""Screener data layer — one row per company from the published parquet artifacts.

Reuses :func:`lib.data.load_latest_data` (the same cached parquet loader the
detail page uses) — there is **no Databricks connection at runtime**. The wide
frame is built by pivoting the long-format `dashboard_metrics.parquet` to the
latest available fiscal year per ticker.

Universe flags (`is_favorite` / `in_sp500` / `in_r3000`) come from
`meta.tickers` and **Market Cap** comes from a `Market Cap` metric row — both
added by `50_publish/51__export_dashboard_data.py` (schema v3). Older artifacts
that predate v3 simply yield all-False flags and an empty Market Cap column; the
screener degrades gracefully until the next publish.
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
# Métricas con slider de rango siempre visible.
RANGE_METRICS = ["Market Cap", "P/E", "Net Margin %", "ROE %", "Revenue YoY %"]
# Universo → columna de flag en el frame ("" = sin filtro).
UNIVERSE_FLAGS = {
    "Todos":        "",
    "S&P 500":      "in_sp500",
    "Russell 3000": "in_r3000",
    "Favoritos":    "is_favorite",
}
_FLAG_COLS = ("is_favorite", "in_sp500", "in_r3000")


def _as_bool(s: pd.Series) -> pd.Series:
    """Coerce a flag column to native bool, tolerant of legacy stringified values."""
    return s.map(lambda v: v is True or str(v).strip().lower() in ("true", "1")).astype(bool)


@st.cache_data(ttl=3600, show_spinner="Construyendo el screener…")
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

    # Quédate con el último fiscal_year disponible por ticker.
    latest = m.groupby("ticker", observed=True)["fiscal_year"].transform("max")
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

    # company + flags de universo desde meta.
    info = pd.DataFrame(meta.get("tickers", []))
    if "ticker" in info.columns:
        keep = ["ticker"] + [c for c in ("company", *_FLAG_COLS) if c in info.columns]
        wide = wide.merge(info[keep], on="ticker", how="left")
    if "company" not in wide.columns:
        wide["company"] = wide["ticker"]
    wide["company"] = wide["company"].fillna(wide["ticker"])
    for c in _FLAG_COLS:
        wide[c] = _as_bool(wide[c]) if c in wide.columns else False

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


def search_mask(df: pd.DataFrame, query: str) -> pd.Series:
    q = (query or "").strip().lower()
    if not q:
        return pd.Series(True, index=df.index)
    return (
        df["ticker"].astype(str).str.lower().str.contains(q, na=False)
        | df["company"].astype(str).str.lower().str.contains(q, na=False)
    )


def range_bounds(series: pd.Series) -> tuple[float, float] | None:
    """Slider bounds clamped to the 1st–99th percentile (robust to outliers)."""
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return None
    lo, hi = float(s.quantile(0.01)), float(s.quantile(0.99))
    if lo == hi:
        hi = lo + 1.0
    return lo, hi


def range_mask(
    series: pd.Series, sel_lo: float, sel_hi: float, bound_lo: float, bound_hi: float
) -> pd.Series:
    """Mask for a range slider.

    A bound left at its slider extreme is treated as open-ended, so companies
    with values beyond the clamped percentile bounds are NOT filtered out at the
    default (full-span) position. NaN never hides a company (``| isna``).
    """
    s = pd.to_numeric(series, errors="coerce")
    m = pd.Series(True, index=s.index)
    if sel_lo > bound_lo:
        m &= s >= sel_lo
    if sel_hi < bound_hi:
        m &= s <= sel_hi
    return m | s.isna()
