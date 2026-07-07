"""Company Overview tab — headline KPIs + company profile.

Pure HTML renderers (return strings for ``st.markdown(..., unsafe_allow_html=True)``),
mirroring ``lib/kpis.py`` / ``lib/render.py``. Imported by ``views/company.py`` and
rendered as the first tab on the company page.

Data contract (already sliced to the selected ticker by company.py):
  * tmetrics — derived metrics (cols: ticker, period_type, fiscal_year, metric, unit, value)
  * prices   — daily price frame (cols: ticker, date, close, adj_close)
  * meta     — full meta artifact; this ticker's profile dict lives in meta["tickers"].
"""

from __future__ import annotations

import html
import re

import pandas as pd

from lib.currency import convert_to_usd, currency_badge, quote_currency
from lib.format import EM_DASH, fmt_delta, fmt_kpi, is_missing
from lib.news import fetch_yahoo_news
from lib.prices import _price_delta, _px, prices_for

# ──────────────────────────────────────────────────────────────────────────────
# Section 1 — KPI strip (Market Cap · Price · Employees · P/E Ratio)
#
# The richer market/profile KPIs live here in the Overview tab; the top-of-page
# strip (lib/kpis.py) carries the financial headlines (Revenue / Net Income / …).
# ──────────────────────────────────────────────────────────────────────────────


def render_kpi_strip(
    ticker: str, tmetrics: pd.DataFrame, prices: pd.DataFrame, meta: dict,
    fx: pd.DataFrame | None = None, usd_lens: bool = False,
) -> str:
    """Four headline KPIs: Market Cap · Price · Employees · P/E Ratio.

    Market Cap (reporting_currency) and Price (quote currency, from the ticker's listing
    `market`) can legitimately differ for the same ticker — each is converted/badged
    independently against its OWN currency when `usd_lens` is on, never a single
    ticker-level assumption. See lib/currency.py.
    """
    cards = [
        _market_cap_card(ticker, tmetrics, meta, fx, usd_lens),
        _price_card(ticker, prices, meta, fx, usd_lens),
        _employees_card(ticker, meta),
        _pe_card(ticker, tmetrics),
    ]
    return f'<div class="kpi-strip">{"".join(cards)}</div>'


def _latest_fy_metric(ticker: str, tmetrics: pd.DataFrame, metric: str) -> pd.DataFrame:
    """Latest-FY rows (desc by fiscal_year) for one ticker + metric. May be empty."""
    if tmetrics.empty:
        return tmetrics
    sub = tmetrics[
        (tmetrics["ticker"] == ticker)
        & (tmetrics["period_type"] == "FY")
        & (tmetrics["metric"] == metric)
    ]
    return sub.sort_values("fiscal_year", ascending=False)


def _usd_lens_convert(value: float, currency: str, as_of, fx: pd.DataFrame | None) -> tuple[float, str]:
    """Convert one value to USD if possible; else return it unchanged with its badge.

    Returns (display_value, badge_html). `currency` is already uppercased/non-empty-checked
    by the caller; this only runs the actual conversion + badge decision.
    """
    if fx is None or fx.empty or is_missing(as_of):
        return value, currency_badge(currency)
    converted, ok = convert_to_usd(pd.Series([value]), pd.Series([currency]), pd.Series([as_of]), fx)
    if bool(ok.iloc[0]):
        return float(converted.iloc[0]), ""
    return value, currency_badge(currency)


def _market_cap_card(
    ticker: str, tmetrics: pd.DataFrame, meta: dict, fx: pd.DataFrame | None, usd_lens: bool,
) -> str:
    series = _latest_fy_metric(ticker, tmetrics, "Market Cap")
    if series.empty:
        return _empty_card("MARKET CAP")

    latest = series.iloc[0]
    latest_val = latest["value"]
    if is_missing(latest_val):
        return _empty_card("MARKET CAP")
    latest_fy = int(latest["fiscal_year"])

    yoy = None
    if len(series) >= 2:
        prior_val = series.iloc[1]["value"]
        if not is_missing(prior_val) and prior_val != 0:
            yoy = (latest_val - prior_val) / abs(prior_val) * 100
    delta_label, delta_cls = fmt_delta(yoy)

    ccy = (_ticker_info(ticker, meta).get("reporting_currency") or "USD").upper()
    badge = ""
    if ccy != "USD":
        period_end = latest["period_end"] if "period_end" in series.columns else None
        if usd_lens:
            latest_val, badge = _usd_lens_convert(latest_val, ccy, period_end, fx)
        else:
            badge = currency_badge(ccy)

    return (
        '<div class="kpi">'
        '<div class="label">MARKET CAP</div>'
        f'<div class="value">{fmt_kpi(latest_val)}{badge}</div>'
        f'<div class="delta {delta_cls}">{delta_label}  ·  FY {latest_fy}</div>'
        '</div>'
    )


def _price_card(
    ticker: str, prices: pd.DataFrame, meta: dict, fx: pd.DataFrame | None, usd_lens: bool,
) -> str:
    pdf = prices_for(prices, ticker)
    if pdf is None or pdf.empty:
        return _empty_card("PRICE")

    last = pdf.iloc[-1]
    price = last["close"]
    if is_missing(price):
        return _empty_card("PRICE")

    # Ratio, so currency-invariant — compute BEFORE any USD-lens conversion of `price` below.
    chg_1d = None
    if len(pdf) >= 2:
        prev_close = pdf.iloc[-2]["close"]
        if not is_missing(prev_close) and prev_close != 0:
            chg_1d = (price - prev_close) / abs(prev_close) * 100
    delta_label, delta_cls = _price_delta(chg_1d)

    ccy = quote_currency(_ticker_info(ticker, meta).get("market"))
    badge = ""
    if ccy != "USD":
        trade_date = last["date"] if "date" in pdf.columns else None
        if usd_lens:
            price, badge = _usd_lens_convert(float(price), ccy, trade_date, fx)
        else:
            badge = currency_badge(ccy)

    return (
        '<div class="kpi">'
        '<div class="label">PRICE</div>'
        f'<div class="value">{_px(float(price))}{badge}</div>'
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


def _pe_card(ticker: str, tmetrics: pd.DataFrame) -> str:
    series = _latest_fy_metric(ticker, tmetrics, "P/E")
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


def _empty_card(label: str) -> str:
    return (
        '<div class="kpi">'
        f'  <div class="label">{label}</div>'
        '  <div class="value">—</div>'
        '  <div class="delta flat">no data</div>'
        '</div>'
    )


# ──────────────────────────────────────────────────────────────────────────────
# Section 2 — Company profile (info card + business description)
# ──────────────────────────────────────────────────────────────────────────────


def _ticker_info(ticker: str, meta: dict) -> dict:
    for t in meta.get("tickers", []):
        if isinstance(t, dict) and t.get("ticker") == ticker:
            return t
    return {}


def _website_link(url) -> str | None:
    """Sanitized external link showing the bare host; None when missing/unsafe."""
    if not isinstance(url, str) or not url.strip():
        return None
    u = url.strip()
    if not u.startswith(("http://", "https://")):
        u = "https://" + u
    if not u.startswith(("http://", "https://")):   # belt-and-braces after the prepend
        return None
    disp = re.sub(r"^https?://(www\.)?", "", u).rstrip("/")
    href = html.escape(u, quote=True)
    return f'<a href="{href}" target="_blank" rel="noopener noreferrer">{html.escape(disp)}</a>'


def _info_row(key: str, value_html: str | None) -> str:
    if not value_html:
        value_html = f'<span class="muted">{EM_DASH}</span>'
    return f'<div class="info-row"><span class="k">{key}</span><span class="v">{value_html}</span></div>'


def render_profile(ticker: str, meta: dict) -> str:
    """Two-column profile: fixed-width info card (left) + description card (right)."""
    info = _ticker_info(ticker, meta)

    sector = info.get("sector") or ""
    industry = info.get("industry") or ""
    exchange = info.get("exchange") or ""
    country = info.get("country") or ""
    employees = info.get("employees")
    founded = info.get("founded")
    website = info.get("website") or ""
    description = info.get("description") or ""

    emp_html = f"{int(employees):,}" if not is_missing(employees) else None
    founded_html = f"{int(founded)}" if not is_missing(founded) else None

    rows = [
        _info_row("Sector", html.escape(sector) if sector else None),
        _info_row("Industry", html.escape(industry) if industry else None),
        _info_row("Exchange", html.escape(exchange) if exchange else None),
        _info_row("Country", html.escape(country) if country else None),
        _info_row("Employees", emp_html),
        _info_row("Founded", founded_html),
        _info_row("Website", _website_link(website)),
    ]

    desc_inner = (
        f"<p>{html.escape(description)}</p>"
        if description
        else '<p class="muted">No business description available.</p>'
    )

    return (
        '<div class="overview-profile">'
        f'  <div class="overview-info">{"".join(rows)}</div>'
        '  <div class="overview-desc">'
        '    <div class="overview-desc-eyebrow">About</div>'
        f'    {desc_inner}'
        '  </div>'
        '</div>'
    )


# ──────────────────────────────────────────────────────────────────────────────
# Section 3 — Recent news (Yahoo Finance RSS)
# ──────────────────────────────────────────────────────────────────────────────


def render_news(items: list[dict]) -> str:
    """Headline list from `lib.news.fetch_yahoo_news`. Degrades to a muted note when empty."""
    if not items:
        body = '<p class="muted">No recent news available.</p>'
    else:
        cards = []
        for it in items:
            href = html.escape(it.get("link", ""), quote=True)
            title = html.escape(it.get("title", ""))
            pub = html.escape(it.get("published") or "")
            date_html = f'<span class="news-date">{pub}</span>' if pub else ""
            cards.append(
                f'<a class="news-item" href="{href}" target="_blank" rel="noopener noreferrer">'
                f'<span class="news-title">{title}</span>{date_html}</a>'
            )
        body = "".join(cards)
    return (
        '<div class="overview-news">'
        '  <div class="overview-news-eyebrow">Recent news · Yahoo Finance</div>'
        f'  <div class="news-list">{body}</div>'
        '</div>'
    )


# ──────────────────────────────────────────────────────────────────────────────
# Public entry point — render the whole Overview tab
# ──────────────────────────────────────────────────────────────────────────────


def render_overview(
    ticker: str, tmetrics: pd.DataFrame, prices: pd.DataFrame, meta: dict,
    fx: pd.DataFrame | None = None, usd_lens: bool = False,
) -> str:
    """Full Overview tab as one HTML string (KPI strip + profile + recent news).

    The news fetch is a cached, fully-graceful network call (empty list on any failure),
    so a Yahoo outage degrades to a "no recent news" note rather than breaking the page.
    """
    news = fetch_yahoo_news(ticker)
    return (
        render_kpi_strip(ticker, tmetrics, prices, meta, fx, usd_lens)
        + render_profile(ticker, meta)
        + render_news(news)
    )
