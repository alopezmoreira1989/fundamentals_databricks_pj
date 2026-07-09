"""KPI strip — top-of-page financial headlines: Market Cap · Revenue · Net Income · Net Margin %.

Latest-FY value per KPI with its YoY delta. Market Cap and Net Margin % come from the
derived-metrics frame; Revenue and Net Income from the financials long-frame. Each card
degrades to its em-dash / "no prior year" state on missing data. The richer market/profile
KPIs (Price, Employees, P/E) live in the Overview tab (`views/overview.py`).
"""

from __future__ import annotations

import pandas as pd

from .currency import currency_badge, usd_lens_convert
from .format import fmt_delta, fmt_kpi, fmt_metric, is_missing


def render_kpi_strip(
    ticker: str, data: pd.DataFrame, metrics: pd.DataFrame, meta: dict | None = None,
    fx: pd.DataFrame | None = None, usd_lens: bool = False,
) -> str:
    """Return the full KPI strip as an HTML string.

    Market Cap converts to USD when the "View in USD" toggle (`usd_lens`) is on and this
    ticker's reporting_currency isn't already USD — same date-anchored conversion as the
    Overview tab's Market Cap/Price cards (lib/currency.py's usd_lens_convert), so the two
    cards always agree instead of one converting and the other staying native. When the
    toggle is off, Market Cap is badged with its native currency only (no conversion).
    Revenue/Net Income/Net Margin stay in native currency either way — out of scope, no
    per-line-item conversion built yet (see CLAUDE.md's currency-alignment convention).
    """
    mcap_rows = _fy_rows(metrics, ticker, "Market Cap")
    rev = _concept_series(data, ticker, "Revenue", "Income Statement")
    ni = _concept_series(data, ticker, "Net Income", "Income Statement")
    margin = _metric_series(metrics, ticker, "Net Margin %")

    mcap_val = None
    mcap_yoy = None
    mcap_badge = ""
    if not mcap_rows.empty:
        latest = mcap_rows.iloc[-1]
        if not is_missing(latest["value"]):
            mcap_val = latest["value"]
            if len(mcap_rows) >= 2:
                prior_val = mcap_rows.iloc[-2]["value"]
                if not is_missing(prior_val) and prior_val != 0:
                    mcap_yoy = (mcap_val - prior_val) / abs(prior_val) * 100.0

            ccy = "USD"
            if meta:
                for t in meta.get("tickers", []):
                    if isinstance(t, dict) and t.get("ticker") == ticker:
                        ccy = (t.get("reporting_currency") or "USD").upper()
                        break
            if ccy != "USD":
                if usd_lens:
                    period_end = latest["period_end"] if "period_end" in mcap_rows.columns else None
                    mcap_val, mcap_badge = usd_lens_convert(mcap_val, ccy, period_end, fx)
                else:
                    mcap_badge = currency_badge(ccy)

    cards = [
        _card("MARKET CAP", fmt_kpi(mcap_val) + mcap_badge, mcap_yoy),
        _card("REVENUE", fmt_kpi(_latest(rev)), _yoy(rev)),
        _card("NET INCOME", fmt_kpi(_latest(ni)), _yoy(ni)),
        _card("NET MARGIN", fmt_metric(_latest(margin), "percent"), _yoy(margin)),
    ]
    return f'<div class="kpi-strip">{"".join(cards)}</div>'


def _fy_rows(metrics: pd.DataFrame, ticker: str, name: str) -> pd.DataFrame:
    """Ordered FY rows (oldest → newest, full columns incl. period_end) for one metric."""
    if metrics.empty:
        return metrics
    mask = (metrics["ticker"] == ticker) & (metrics["period_type"] == "FY") & (metrics["metric"] == name)
    return metrics[mask].sort_values("fiscal_year")


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
