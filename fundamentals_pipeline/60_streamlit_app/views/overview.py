"""Company Overview tab — headline KPIs + company profile.

Pure HTML renderers (return strings for ``st.markdown(..., unsafe_allow_html=True)``),
mirroring ``lib/kpis.py`` / ``lib/render.py``. Imported by ``views/company.py`` and
rendered as the first tab on the company page.

Data contract (already sliced to the selected ticker by company.py):
  * tdata    — long-format financials (cols: ticker, period_type, fiscal_year, concept, stmt, value)
  * tmetrics — derived metrics       (cols: ticker, period_type, fiscal_year, metric, unit, value)
  * meta     — full meta artifact; this ticker's profile dict lives in meta["tickers"].
"""

from __future__ import annotations

import html
import re

import pandas as pd
from lib.format import EM_DASH, fmt_delta, fmt_kpi, fmt_metric, is_missing
from lib.news import fetch_yahoo_news

# ──────────────────────────────────────────────────────────────────────────────
# Series helpers — latest-FY value + the prior FY (for YoY), graceful on missing
# ──────────────────────────────────────────────────────────────────────────────


def _fy_values(df: pd.DataFrame, mask: pd.Series) -> list:
    """Ordered FY value series for the masked rows (oldest → newest). [] when empty."""
    sub = df[mask].sort_values("fiscal_year")
    return sub["value"].tolist() if not sub.empty else []


def _metric_values(tmetrics: pd.DataFrame, name: str) -> list:
    if tmetrics.empty:
        return []
    return _fy_values(
        tmetrics,
        (tmetrics["period_type"] == "FY") & (tmetrics["metric"] == name),
    )


def _concept_values(tdata: pd.DataFrame, concept: str, stmt: str) -> list:
    if tdata.empty:
        return []
    return _fy_values(
        tdata,
        (tdata["period_type"] == "FY") & (tdata["concept"] == concept) & (tdata["stmt"] == stmt),
    )


def _yoy_pct(values: list):
    """(latest − prior) / |prior| as a percent, or None when not computable."""
    if len(values) < 2 or is_missing(values[-1]) or is_missing(values[-2]) or values[-2] == 0:
        return None
    return (values[-1] - values[-2]) / abs(values[-2]) * 100.0


def _latest(values: list):
    return values[-1] if values and not is_missing(values[-1]) else None


# ──────────────────────────────────────────────────────────────────────────────
# Section 1 — KPI strip (Market Cap · Revenue · Net Income · Net Margin %)
# ──────────────────────────────────────────────────────────────────────────────


def _kpi_card(label: str, value_str: str, yoy_pct) -> str:
    delta_label, delta_cls = fmt_delta(yoy_pct)
    return (
        '<div class="kpi">'
        f'  <div class="label">{label}</div>'
        f'  <div class="value">{value_str}</div>'
        f'  <div class="delta {delta_cls}">{delta_label}</div>'
        '</div>'
    )


def render_kpi_strip(tdata: pd.DataFrame, tmetrics: pd.DataFrame) -> str:
    """Four headline KPIs for the latest FY, each with its YoY delta."""
    mcap = _metric_values(tmetrics, "Market Cap")
    rev = _concept_values(tdata, "Revenue", "Income Statement")
    ni = _concept_values(tdata, "Net Income", "Income Statement")
    margin = _metric_values(tmetrics, "Net Margin %")

    cards = [
        _kpi_card("MARKET CAP", fmt_kpi(_latest(mcap)), _yoy_pct(mcap)),
        _kpi_card("REVENUE", fmt_kpi(_latest(rev)), _yoy_pct(rev)),
        _kpi_card("NET INCOME", fmt_kpi(_latest(ni)), _yoy_pct(ni)),
        _kpi_card("NET MARGIN", fmt_metric(_latest(margin), "percent"), _yoy_pct(margin)),
    ]
    return f'<div class="kpi-strip">{"".join(cards)}</div>'


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


def render_overview(ticker: str, tdata: pd.DataFrame, tmetrics: pd.DataFrame, meta: dict) -> str:
    """Full Overview tab as one HTML string (KPI strip + profile + recent news).

    The news fetch is a cached, fully-graceful network call (empty list on any failure),
    so a Yahoo outage degrades to a "no recent news" note rather than breaking the page.
    """
    news = fetch_yahoo_news(ticker)
    return render_kpi_strip(tdata, tmetrics) + render_profile(ticker, meta) + render_news(news)
