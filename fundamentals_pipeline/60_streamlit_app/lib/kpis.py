"""KPI strip — top-of-page financial headlines: Market Cap · Revenue · Net Income · Net Margin %.

Latest-FY value per KPI with its YoY delta. Market Cap and Net Margin % come from the
derived-metrics frame; Revenue and Net Income from the financials long-frame. Each card
degrades to its em-dash / "no prior year" state on missing data. The richer market/profile
KPIs (Price, Employees, P/E) live in the Overview tab (`views/overview.py`).
"""

from __future__ import annotations

import pandas as pd

from .format import fmt_delta, fmt_kpi, fmt_metric, is_missing


def render_kpi_strip(ticker: str, data: pd.DataFrame, metrics: pd.DataFrame) -> str:
    """Return the full KPI strip as an HTML string."""
    mcap = _metric_series(metrics, ticker, "Market Cap")
    rev = _concept_series(data, ticker, "Revenue", "Income Statement")
    ni = _concept_series(data, ticker, "Net Income", "Income Statement")
    margin = _metric_series(metrics, ticker, "Net Margin %")

    cards = [
        _card("MARKET CAP", fmt_kpi(_latest(mcap)), _yoy(mcap)),
        _card("REVENUE", fmt_kpi(_latest(rev)), _yoy(rev)),
        _card("NET INCOME", fmt_kpi(_latest(ni)), _yoy(ni)),
        _card("NET MARGIN", fmt_metric(_latest(margin), "percent"), _yoy(margin)),
    ]
    return f'<div class="kpi-strip">{"".join(cards)}</div>'


def _fy_series(df: pd.DataFrame, mask: pd.Series) -> list:
    """Ordered FY value series for the masked rows (oldest → newest). [] when empty."""
    sub = df[mask].sort_values("fiscal_year")
    return sub["value"].tolist() if not sub.empty else []


def _metric_series(metrics: pd.DataFrame, ticker: str, name: str) -> list:
    if metrics.empty:
        return []
    return _fy_series(
        metrics,
        (metrics["ticker"] == ticker) & (metrics["period_type"] == "FY") & (metrics["metric"] == name),
    )


def _concept_series(data: pd.DataFrame, ticker: str, concept: str, stmt: str) -> list:
    if data.empty:
        return []
    return _fy_series(
        data,
        (data["ticker"] == ticker)
        & (data["period_type"] == "FY")
        & (data["concept"] == concept)
        & (data["stmt"] == stmt),
    )


def _latest(values: list):
    return values[-1] if values and not is_missing(values[-1]) else None


def _yoy(values: list):
    """(latest − prior) / |prior| as a percent, or None when not computable."""
    if len(values) < 2 or is_missing(values[-1]) or is_missing(values[-2]) or values[-2] == 0:
        return None
    return (values[-1] - values[-2]) / abs(values[-2]) * 100.0


def _card(label: str, value_str: str, yoy_pct) -> str:
    delta_label, delta_cls = fmt_delta(yoy_pct)
    return (
        '<div class="kpi">'
        f'  <div class="label">{label}</div>'
        f'  <div class="value">{value_str}</div>'
        f'  <div class="delta {delta_cls}">{delta_label}</div>'
        '</div>'
    )
