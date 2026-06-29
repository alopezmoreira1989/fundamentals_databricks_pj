"""KPI strip — 4-column grid: Market Cap · Price · Employees · P/E Ratio.

Market Cap and P/E come from the latest-FY rows of the derived-metrics frame; Price
from the daily price store (latest close + 1-day change); Employees from the per-ticker
meta record (latest filing). Each card degrades to `_empty_card()` when its source is
absent or non-sensible (missing data, ≤0 P/E, 0 employees).
"""

from __future__ import annotations

import pandas as pd

from .format import fmt_delta, fmt_kpi, is_missing
from .prices import _price_delta, _px, prices_for


def render_kpi_strip(
    ticker: str,
    data: pd.DataFrame,
    metrics: pd.DataFrame,
    prices: pd.DataFrame,
    meta: dict,
) -> str:
    """Return the full KPI strip as an HTML string."""
    cards_html: list[str] = [
        _market_cap_card(ticker, metrics),
        _price_card(ticker, prices),
        _employees_card(ticker, meta),
        _pe_card(ticker, metrics),
    ]
    return f'<div class="kpi-strip">{"".join(cards_html)}</div>'


def _latest_fy_metric(ticker: str, metrics: pd.DataFrame, metric: str) -> pd.DataFrame:
    """Latest-FY rows (desc by fiscal_year) for one ticker + metric. May be empty."""
    sub = metrics[
        (metrics["ticker"] == ticker)
        & (metrics["period_type"] == "FY")
        & (metrics["metric"] == metric)
    ]
    return sub.sort_values("fiscal_year", ascending=False)


def _market_cap_card(ticker: str, metrics: pd.DataFrame) -> str:
    series = _latest_fy_metric(ticker, metrics, "Market Cap")
    if series.empty:
        return _empty_card("MARKET CAP")

    latest = series.iloc[0]
    latest_val = latest["value"]
    if is_missing(latest_val):
        return _empty_card("MARKET CAP")
    latest_fy = int(latest["fiscal_year"])

    # YoY % vs prior fiscal year.
    yoy = None
    if len(series) >= 2:
        prior_val = series.iloc[1]["value"]
        if not is_missing(prior_val) and prior_val != 0:
            yoy = (latest_val - prior_val) / abs(prior_val) * 100

    delta_label, delta_cls = fmt_delta(yoy)
    return (
        '<div class="kpi">'
        '<div class="label">MARKET CAP</div>'
        f'<div class="value">{fmt_kpi(latest_val)}</div>'
        f'<div class="delta {delta_cls}">{delta_label}  ·  FY {latest_fy}</div>'
        '</div>'
    )


def _price_card(ticker: str, prices: pd.DataFrame) -> str:
    pdf = prices_for(prices, ticker)
    if pdf is None or pdf.empty:
        return _empty_card("PRICE")

    last = pdf.iloc[-1]
    price = last["close"]
    if is_missing(price):
        return _empty_card("PRICE")

    # 1-day % change vs the prior row's close.
    chg_1d = None
    if len(pdf) >= 2:
        prev_close = pdf.iloc[-2]["close"]
        if not is_missing(prev_close) and prev_close != 0:
            chg_1d = (price - prev_close) / abs(prev_close) * 100

    delta_label, delta_cls = _price_delta(chg_1d)
    return (
        '<div class="kpi">'
        '<div class="label">PRICE</div>'
        f'<div class="value">{_px(float(price))}</div>'
        f'<div class="delta {delta_cls}">{delta_label}  ·  latest close</div>'
        '</div>'
    )


def _employees_card(ticker: str, meta: dict) -> str:
    employees = None
    for t in meta.get("tickers", []):
        if isinstance(t, dict) and t.get("ticker") == ticker:
            employees = t.get("employees")
            break

    if employees is None or is_missing(employees) or int(employees) <= 0:
        return _empty_card("EMPLOYEES")

    return (
        '<div class="kpi">'
        '<div class="label">EMPLOYEES</div>'
        f'<div class="value">{int(employees):,}</div>'
        '<div class="delta flat">full-time · latest filing</div>'
        '</div>'
    )


def _pe_card(ticker: str, metrics: pd.DataFrame) -> str:
    series = _latest_fy_metric(ticker, metrics, "P/E")
    if series.empty:
        return _empty_card("P/E RATIO")

    val = series.iloc[0]["value"]
    if is_missing(val) or val <= 0:
        return _empty_card("P/E RATIO")

    return (
        '<div class="kpi">'
        '<div class="label">P/E RATIO</div>'
        f'<div class="value">{val:.1f}×</div>'
        '<div class="delta flat">trailing twelve months</div>'
        '</div>'
    )


def _empty_card(concept: str) -> str:
    label = concept.upper()
    return (
        f'<div class="kpi">'
        f'  <div class="label">{label}</div>'
        f'  <div class="value">—</div>'
        f'  <div class="delta flat">no data</div>'
        f'</div>'
    )
