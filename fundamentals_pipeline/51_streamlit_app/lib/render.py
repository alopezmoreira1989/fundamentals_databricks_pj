"""HTML renderers for the dashboard components.

Each public function returns a raw HTML string meant to be injected via
st.markdown(..., unsafe_allow_html=True).
"""

from __future__ import annotations

import html
import re
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from .colors import (
    AMBER,
    BLUE,
    CORAL,
    CREAM,
    GRAND_TOTAL_CONCEPTS,
    GRAY,
    GREEN,
    PER_SHARE_CONCEPTS,
    is_negative_concept,
    row_class as derive_row_class,
    row_color,
    section_class,
)
from .format import (
    EM_DASH,
    fmt_cagr,
    fmt_eps,
    fmt_kpi,
    fmt_metric,
    fmt_num,
    is_missing,
    short_quarter,
    short_year,
)
from .signals import signal_absolute, signal_vs_history, threshold_text
from .sparkline import mini_bars_svg, mini_sparkline_svg, sparkline_svg
from .tables import get_year_columns

# ──────────────────────────────────────────────────────────────────────────────
# CSS injection
# ──────────────────────────────────────────────────────────────────────────────

GOOGLE_FONTS_LINK = (
    '<link rel="preconnect" href="https://fonts.googleapis.com">'
    '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>'
    '<link href="https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,400;9..144,500;9..144,600'
    '&family=Inter:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">'
)


def inject_css(css_path: Path) -> None:
    """Read the CSS file and inject it + Google Fonts into the page head."""
    css = css_path.read_text(encoding="utf-8")
    st.html(GOOGLE_FONTS_LINK + f"<style>{css}</style>")


# ──────────────────────────────────────────────────────────────────────────────
# Masthead
# ──────────────────────────────────────────────────────────────────────────────

def render_masthead(ticker: str, data: pd.DataFrame, meta: dict[str, Any]) -> str:
    # Resolve company name from meta (schema v2: list of dicts) or fallback.
    company = ticker
    ticker_info_list = meta.get("tickers", [])
    if ticker_info_list and isinstance(ticker_info_list[0], dict):
        match = next((t for t in ticker_info_list if t["ticker"] == ticker), None)
        if match:
            company = match.get("company", ticker)

    # FY range for this ticker.
    fy_range = ""
    fy_ranges = meta.get("fy_ranges", [])
    for entry in fy_ranges:
        if entry.get("ticker") == ticker:
            fy_range = f"FY {entry['fy_min']} — FY {entry['fy_max']}"
            n_years = entry["fy_max"] - entry["fy_min"] + 1
            break
    else:
        n_years = 0

    build_ts = meta.get("build_timestamp", "")

    return (
        '<div class="masthead">'
        '  <div class="masthead-left">'
        '    <div class="eyebrow">Fundamentals · Annual filings</div>'
        f'    <h1>{html.escape(str(company))}</h1>'
        '    <div class="ticker-row">'
        f'      <span class="ticker-chip">{ticker}</span>'
        '    </div>'
        '  </div>'
        '  <div class="masthead-right">'
        f'    <div class="date">{fy_range}</div>'
        f'    <div>{n_years} fiscal years · USD</div>'
        f'    <div style="margin-top:6px;">main.financials.financials</div>'
        '  </div>'
        '</div>'
    )


# ──────────────────────────────────────────────────────────────────────────────
# Financial-statement table
# ──────────────────────────────────────────────────────────────────────────────

def render_table_html(
    df: pd.DataFrame,
    statement: str,
    ticker: str,
    notes: dict[str, Any],
) -> str:
    """Render IS / BS / CF / quarterly tables as full <table> HTML."""
    if df.empty:
        return '<p style="color:var(--ink-3)">No data available.</p>'

    year_cols = get_year_columns(df)
    is_quarterly = statement == "qt"
    n_cols = len(year_cols) + 3  # label + N years + trend + cagr (not in qt)
    # Quarter labels reconstructed as FY − YTD_Q3 (Q4), if the export flags them.
    derived_qs = df.attrs.get("derived_quarters", set()) if is_quarterly else set()

    # Header row. Values are stored as raw USD (fmt_num prints them at full
    # resolution), so the label is "USD" — NOT "$M"/millions.
    header_cells = ['<th class="col-label">USD</th>']
    for i, yr in enumerate(year_cols):
        is_last = i == len(year_cols) - 1
        cls = "col-num col-latest" if is_last else "col-num"
        if is_quarterly:
            # Quarter labels are already strings like "'24-Q4".
            cls_extra = " col-latest-q" if is_last else ""
            marker = '<sup class="derived-mark">ᵈ</sup>' if yr in derived_qs else ""
            header_cells.append(f'<th class="col-num{cls_extra}">{yr}{marker}</th>')
        else:
            label = short_year(yr) if isinstance(yr, (int, float)) else yr
            header_cells.append(f'<th class="{cls}">{label}</th>')
    if not is_quarterly:
        header_cells.append('<th class="col-trend">10y trend</th>')
        header_cells.append('<th class="col-cagr">CAGR</th>')

    thead = f'<thead><tr>{"".join(header_cells)}</tr></thead>'

    # Body rows.
    rows_html: list[str] = []
    prev_section: str | None = None
    prev_group: str | None = None
    stmt_value = df.iloc[0]["stmt"] if "stmt" in df.columns else ""

    for _, row in df.iterrows():
        section = row.get("section") if pd.notna(row.get("section")) else None
        group = row.get("group") if pd.notna(row.get("group")) else None
        concept = row["concept"]
        display_name = row["display_name"]
        indent = int(row.get("indent", 0))
        cls = row.get("row_class", "")

        # Emit section divider if new section.
        if section and section != prev_section:
            s_cls = section_class(stmt_value, section)
            rows_html.append(
                f'<tr class="section-row {s_cls}"><td colspan="{n_cols}">{html.escape(str(section))}</td></tr>'
            )
            prev_section = section
            prev_group = None  # Reset group tracking within new section.

        # Emit group subheader if nested group changed.
        if group and group != section and group != prev_group:
            rows_html.append(
                f'<tr class="group-row"><td colspan="{n_cols}">{html.escape(str(group))}</td></tr>'
            )
            prev_group = group

        # Build the data row.
        row_values = [row.get(c) for c in year_cols]

        # Label cell.
        indent_cls = f" indent-{indent}" if indent else ""
        label_td = f'<td class="label{indent_cls}">{html.escape(str(display_name))}</td>'

        # Number cells.
        is_per_share = concept in PER_SHARE_CONCEPTS
        num_cells: list[str] = []
        for i, v in enumerate(row_values):
            is_last = i == len(row_values) - 1
            latest_cls = " latest" if is_last else ""

            if is_missing(v):
                num_cells.append(f'<td class="num{latest_cls}">{EM_DASH}</td>')
            elif is_per_share:
                formatted = fmt_eps(v)
                muted = " muted" if v < 0 else ""
                num_cells.append(f'<td class="num{latest_cls}{muted}">{formatted}</td>')
            else:
                neg = v < 0
                formatted = fmt_num(v)
                muted = " muted" if neg else ""
                num_cells.append(f'<td class="num{latest_cls}{muted}">{formatted}</td>')

        # Trend sparkline + CAGR (not for quarterly mini-table).
        extra_cells = ""
        if not is_quarterly:
            color = row_color(stmt_value, concept)
            # Grand-total rows on dark bg use cream stroke instead.
            if cls == "grand-total":
                color = CREAM
            end_circle = cls in ("headline", "grand-total", "subtotal")
            stroke = 1.8 if cls in ("headline", "grand-total") else (1.6 if cls == "subtotal" else 1.5)
            svg = sparkline_svg(row_values, color=color, stroke=stroke, end_circle=end_circle)
            trend_td = f'<td class="trend">{svg}</td>'

            # CAGR.
            first = next((v for v in row_values if not is_missing(v)), None)
            last = next((v for v in reversed(row_values) if not is_missing(v)), None)
            n_years = sum(1 for v in row_values if not is_missing(v)) - 1
            cagr_label, cagr_cls = fmt_cagr(first, last, n_years)
            cagr_extra = ""
            if cls == "headline":
                cagr_extra = " up" if cagr_cls == "up" else ""
            else:
                cagr_extra = f" {cagr_cls}" if cagr_cls else ""
            cagr_td = f'<td class="cagr{cagr_extra}">{cagr_label}</td>'
            extra_cells = trend_td + cagr_td

        # Quarterly: add col-latest-q class to last cell for bold weight.
        if is_quarterly and num_cells:
            last_cell = num_cells[-1]
            num_cells[-1] = last_cell.replace('class="num latest"', 'class="num latest latest-q"')

        tr_cls = f' class="{cls}"' if cls else ""
        rows_html.append(f'<tr{tr_cls}>{label_td}{"".join(num_cells)}{extra_cells}</tr>')

    tbody = f'<tbody>{"".join(rows_html)}</tbody>'

    table_class = "q-table" if is_quarterly else "fs"
    table_html = f'<div class="fs-table-wrap"><table class="{table_class}">{thead}{tbody}</table></div>'

    # Footnote explaining the derived-quarter marker (only when some are present).
    derived_note = ""
    if derived_qs:
        derived_note = (
            '<div class="footnote" style="margin-top:12px;">'
            '<span style="color:var(--ink); font-weight:500;">ᵈ Derived quarter</span>'
            '<span class="pipe">|</span>Q4 is reconstructed as FY − YTD&nbsp;Q3 to capture '
            'year-end audit adjustments, so it is not a directly reported figure.'
            '</div>'
        )

    # Append notes for this ticker/statement if any.
    note_html = _render_notes(ticker, statement, notes)
    return table_html + derived_note + note_html


def _render_notes(ticker: str, statement: str, notes: dict[str, Any]) -> str:
    """Render ticker-specific footnotes below the table."""
    ticker_notes = notes.get(ticker, {})
    if not ticker_notes:
        return ""

    # Only show notes relevant to this tab's period type.
    # IS/BS/CF → FY notes; quarterly → Q notes.
    parts: list[str] = []
    for period_key, concepts in ticker_notes.items():
        is_q_note = "-Q" in period_key
        if statement == "qt" and not is_q_note:
            continue
        if statement != "qt" and is_q_note:
            continue
        if isinstance(concepts, dict):
            for concept_name, note_text in concepts.items():
                label = html.escape(f"{period_key} {concept_name}")
                parts.append(
                    f'<div class="footnote" style="margin-top:20px;">'
                    f'<strong style="color:var(--ink);font-weight:500;">Note · {label}</strong>'
                    f'<span class="pipe">|</span>{html.escape(str(note_text))}</div>'
                )
    return "".join(parts)


# ──────────────────────────────────────────────────────────────────────────────
# Revenue → Net Income waterfall
# ──────────────────────────────────────────────────────────────────────────────

_WATERFALL_ITEMS = [
    ("Revenue",          "#185FA5"),
    ("Gross Profit",     "#0F6E56"),
    ("Operating Income", "#534AB7"),
    ("Net Income",       "#185FA5"),
]


def render_waterfall(df_is: pd.DataFrame) -> str:
    """Revenue → Net Income waterfall for the latest FY."""
    if df_is.empty:
        return ""

    year_cols = get_year_columns(df_is)
    if not year_cols:
        return ""
    latest_yr = year_cols[-1]

    values: dict[str, float] = {}
    for _, row in df_is.iterrows():
        concept = row["concept"]
        val = row.get(latest_yr)
        if not is_missing(val):
            values[concept] = val

    revenue = values.get("Revenue")
    if not revenue or revenue == 0:
        return ""

    yr_label = short_year(latest_yr) if isinstance(latest_yr, (int, float)) else latest_yr
    net_income = values.get("Net Income", 0)
    rev_kpi = fmt_kpi(revenue)
    ni_kpi = fmt_kpi(net_income)

    bars_html: list[str] = []
    for concept, color in _WATERFALL_ITEMS:
        val = values.get(concept)
        if val is None:
            continue
        pct = val / revenue * 100
        val_str = fmt_kpi(val)
        # If bar fills > 40%, put value inside (white); else outside.
        if pct > 40:
            value_el = f'<div class="bar-value" style="right:10px; color:#FFFFFF;">{val_str} · {pct:.1f}%</div>'
        else:
            value_el = f'<div class="bar-value" style="left:calc({pct:.1f}% + 10px); color:var(--ink-2);">{val_str} · {pct:.1f}%</div>'
        bars_html.append(
            f'<div class="bar-row">'
            f'  <div class="bar-label">{concept}</div>'
            f'  <div class="bar-track">'
            f'    <div class="bar-fill" style="width:{pct:.1f}%; background:{color};"></div>'
            f'    {value_el}'
            f'  </div>'
            f'</div>'
        )

    return (
        '<div class="breakdown">'
        '<div class="breakdown-title">'
        f'<h3>Revenue → Net income · FY{yr_label}</h3>'
        f'<div class="sub">{rev_kpi} yields {ni_kpi}</div>'
        '</div>'
        f'{"".join(bars_html)}'
        '</div>'
    )


# ──────────────────────────────────────────────────────────────────────────────
# Derived metrics grid
# ──────────────────────────────────────────────────────────────────────────────

# Categories whose rows render as 5-year mini-bars instead of a line sparkline.
_USE_BARS = {"Profitability", "Growth"}
# Valuation price-multiples that get a min–avg–max range bar + vs-history signal.
_VALUATION_MULTIPLES = {"P/E", "P/S", "P/FCF", "P/B", "EV/EBITDA"}
_SIGNAL_COLORS = {"good": GREEN, "warn": AMBER, "bad": CORAL}


def _signal_color(sig: str | None) -> str | None:
    return _SIGNAL_COLORS.get(sig) if sig else None


def _clean_iv(name: str) -> str:
    """Strip the (FY)/(TTM) period suffix from Intrinsic Value labels for display."""
    name = re.sub(r"\s*\((FY|TTM)\)", "", name)
    name = re.sub(r",\s*(FY|TTM)\)", ")", name)   # "MoS % (Graham Number, FY)" → "(Graham Number)"
    return name


def render_metrics_grid(metrics: pd.DataFrame, ticker: str, iv_period: str = "FY") -> str:
    """Render the derived metrics cards — one per category from metrics_hierarchy.json.

    `iv_period` ("FY" | "TTM") selects which Intrinsic Value flavour to show — the
    two are distinguished only by the (FY)/(TTM) suffix in the metric/subcategory
    names, never by `period_type` (both are stored as FY rows).
    """
    sub = metrics[(metrics["ticker"] == ticker) & (metrics["period_type"] == "FY")].copy()
    if sub.empty:
        return '<p style="color:var(--ink-3)">No derived metrics available.</p>'

    # Group by category → subcategory → metric (preserving sort_order).
    sub = sub.sort_values("sort_order", na_position="last")
    categories = sub["category"].dropna().unique().tolist()

    cards: list[str] = []
    for cat in categories:
        cat_df = sub[sub["category"] == cat]
        subcategories = cat_df["subcategory"].dropna().unique().tolist()

        is_iv = cat == "Intrinsic Value"
        is_val = cat == "Valuation"
        if is_iv:
            # Keep only the FY or TTM flavour, per the toggle.
            suffix = f"({iv_period})"
            subcategories = [s for s in subcategories if s.endswith(suffix)]

        # Tag = first (visible) subcategory as a label.
        tag = _clean_iv(subcategories[0]) if (is_iv and subcategories) else (
            subcategories[0] if subcategories else ""
        )

        rows_html: list[str] = []
        first_sub = True
        for subcat in subcategories:
            sub_display = _clean_iv(subcat) if is_iv else subcat
            # Subcategory subheader (skip for the first one since the card header serves).
            if not first_sub:
                rows_html.append(
                    f'<div class="metric-row"><div class="m-subheader">{sub_display}</div></div>'
                )
            first_sub = False

            subcat_df = cat_df[cat_df["subcategory"] == subcat]
            for metric_name in subcat_df["metric"].unique():
                m_rows = subcat_df[subcat_df["metric"] == metric_name].sort_values("fiscal_year")
                unit = m_rows.iloc[0].get("unit", None)
                values = m_rows["value"].tolist()
                latest = values[-1] if values else None
                display = _clean_iv(metric_name) if is_iv else metric_name

                if is_val:
                    rows_html.append(_render_valuation_row(metric_name, display, unit, values, latest))
                    continue

                # Direction metrics (growth, margin of safety) keep the "+" sign.
                signed = ("YoY" in metric_name) or ("MoS %" in metric_name)
                formatted = fmt_metric(latest, unit, signed=signed)
                sig = signal_absolute(metric_name, latest)
                tooltip = threshold_text(metric_name)

                if cat in _USE_BARS:
                    bar_color = _signal_color(sig) or _metric_sparkline_color(cat, metric_name, latest)
                    svg = mini_bars_svg(values, color=bar_color, n=5)
                else:
                    svg = mini_sparkline_svg(values, color=_metric_sparkline_color(cat, metric_name, latest))

                row_cls = f"metric-row {('row-' + sig) if sig else ''}".strip()
                title_attr = f' title="{tooltip}"' if tooltip else ""
                rows_html.append(
                    f'<div class="{row_cls}"{title_attr}>'
                    f'  <div class="m-label">{display}</div>'
                    f'  <div class="m-value">{formatted}</div>'
                    f'  <div class="m-spark">{svg}</div>'
                    f'</div>'
                )

        cards.append(
            f'<div class="metric-card">'
            f'<div class="cat-header"><h4>{cat}</h4><div class="tag">{tag}</div></div>'
            f'{"".join(rows_html)}'
            f'</div>'
        )

    return f'<div class="metrics-grid">{"".join(cards)}</div>'


def _render_valuation_row(metric: str, display: str, unit, values, latest) -> str:
    """A Valuation card row: dot-leader + value, plus a min–avg–max range bar for multiples."""
    base = metric.split(" (")[0].strip()
    is_multiple = base in _VALUATION_MULTIPLES
    formatted = fmt_metric(latest, unit)              # multiples/yields: never signed
    tooltip = threshold_text(metric)

    if is_multiple:
        sig = signal_vs_history(latest, values)
        hist = [v for v in values if not is_missing(v)]
    else:
        # Yields (percent) use the absolute Graham bands; EV (usd) gets no signal.
        sig = signal_absolute(metric, latest) if unit == "percent" else None
        hist = []

    sig_cls = f" row-{sig}" if sig else ""
    title_attr = f' title="{tooltip}"' if tooltip else ""

    # Range bar only for multiples with enough history; otherwise a dotted leader.
    middle = '<div class="lead"></div>'
    chip = ""
    if is_multiple and len(hist) >= 3:
        mn, mx = min(hist), max(hist)
        avg = sum(hist[:-1]) / max(len(hist) - 1, 1)
        now = hist[-1]
        span = (mx - mn) or 1
        p_now = max(0.0, min(100.0, (now - mn) / span * 100))
        p_avg = max(0.0, min(100.0, (avg - mn) / span * 100))
        mk_cls = f" row-{sig}" if sig else ""
        middle = (
            f'<div class="vbar">'
            f'<div class="vbar-avg" style="left:{p_avg:.0f}%"></div>'
            f'<div class="vbar-mk{mk_cls}" style="left:{p_now:.0f}%"></div>'
            f'</div>'
        )
        if avg:
            dev = (now - avg) / abs(avg)
            chip = f'<span class="vchip">media {avg:.1f}x · {dev:+.0%}</span>'

    return (
        f'<div class="val-row{sig_cls}"{title_attr}>'
        f'  <div class="m-label">{display}</div>'
        f'  {middle}'
        f'  <div class="m-value">{formatted}</div>'
        f'  {chip}'
        f'</div>'
    )


def _metric_sparkline_color(category: str, metric: str, latest_value) -> str:
    """Pick sparkline color for a metric row."""
    if category == "Valuation":
        return BLUE
    if "YoY" in metric or "Growth" in metric:
        if not is_missing(latest_value) and latest_value < 0:
            return CORAL
        return GRAY
    if category == "Financial Health":
        if "Debt" in metric:
            return CORAL
        return GRAY
    # Profitability / Cash Flow — positive trends are green.
    return GREEN


# ──────────────────────────────────────────────────────────────────────────────
# Balance sheet equation check
# ──────────────────────────────────────────────────────────────────────────────

def render_balance_check(df_bs: pd.DataFrame) -> str:
    """Green ✓ if Total Assets == Total Liab. & Equity for all years; red ⚠️ otherwise."""
    if df_bs.empty:
        return ""

    year_cols = get_year_columns(df_bs)
    if not year_cols:
        return ""

    assets_row = df_bs[df_bs["concept"] == "Total Assets"]
    liab_eq_row = df_bs[df_bs["concept"] == "Total Liabilities & Equity"]

    if assets_row.empty or liab_eq_row.empty:
        return (
            '<div class="footnote" style="display:flex; align-items:center; gap:10px;">'
            '<span style="color:var(--ink-3);">⚠ Balance equation not verifiable — missing Total Assets or Total Liab. & Equity</span>'
            '</div>'
        )

    bad_years: list[str] = []
    for yr in year_cols:
        a = assets_row.iloc[0].get(yr)
        le = liab_eq_row.iloc[0].get(yr)
        if is_missing(a) or is_missing(le):
            continue
        if abs(a - le) > 1:
            label = short_year(yr) if isinstance(yr, (int, float)) else yr
            bad_years.append(label)

    if not bad_years:
        return (
            '<div class="footnote" style="display:flex; align-items:center; gap:10px;">'
            '<span style="color:var(--positive); font-weight:500;">✓ Balance equation verified</span>'
            '<span class="pipe">|</span>'
            f'<span>Total assets = Total liabilities &amp; equity holds for all {len(year_cols)} fiscal years</span>'
            '</div>'
        )
    else:
        yrs_str = ", ".join(bad_years)
        return (
            '<div class="footnote" style="display:flex; align-items:center; gap:10px;">'
            '<span style="color:var(--negative); font-weight:500;">⚠ Balance equation mismatch</span>'
            '<span class="pipe">|</span>'
            f'<span>Total assets ≠ Total liabilities &amp; equity in: {yrs_str}</span>'
            '</div>'
        )


# ──────────────────────────────────────────────────────────────────────────────
# Footnote bar (page bottom)
# ──────────────────────────────────────────────────────────────────────────────

def render_footnote_bar(meta: dict[str, Any], version: str = "") -> str:
    ts = meta.get("build_timestamp", "")
    ver = f'<span class="pipe">|</span> app {version}' if version else ""
    return (
        '<div class="footnote">'
        '<span style="color:var(--ink); font-weight:500;">main.financials.financials</span>'
        '<span class="pipe">|</span> period_type = \'FY\' for annual · period_type IN (\'Q1\'..\'Q4\') for quarterly'
        '<span class="pipe">|</span> concept_hierarchy.json applied for section/group/concept ordering'
        '<span class="pipe">|</span> Derived metrics from main.financials.financials_metrics'
        '<span class="pipe">|</span> Source · SEC EDGAR XBRL via 11__fetch_sec_xbrl'
        f'<span class="pipe">|</span> Built {ts}'
        f'{ver}'
        '</div>'
    )
