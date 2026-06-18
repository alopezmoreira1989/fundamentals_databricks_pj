"""Company detail — the per-ticker editorial dashboard (6 tabs).

Receives the ticker to show via ``st.session_state["ticker"]`` (the handoff
convention the screener writes). Keeps its own searchable selectbox so the page
also works standalone. `set_page_config` / CSS injection live in the router
(`app.py`), not here.
"""

from pathlib import Path

import streamlit as st
from lib.charts import render_bs_composition, render_cf_fcf, render_is_revenue_combo
from lib.data import app_version, load_latest_data, load_notes, load_prices
from lib.kpis import render_kpi_strip
from lib.prices import (
    WINDOW_DEFAULTS,
    WINDOW_SETS,
    price_chart,
    price_window_css,
    prices_for,
    render_price_kpis,
    resample_prices,
    slice_window,
)
from lib.quarterly import render_quarterly_combo
from lib.render import (
    iv_price_from_metrics,
    render_balance_check,
    render_footnote_bar,
    render_masthead,
    render_metrics_grid,
    render_table_html,
    render_valuation_football_field,
    render_waterfall,
)
from lib.tables import (
    balance_sheet_df,
    cash_flow_df,
    get_year_columns,
    income_statement_df,
    quarterly_df,
)

APP_DIR = Path(__file__).parents[1]

data, metrics, meta = load_latest_data()
prices = load_prices()   # heavy daily frame; own cache entry, empty if not yet published
notes = load_notes(APP_DIR / "notes.json")

# Build ticker list with company names for search
ticker_company = {}
for t_info in meta.get("tickers", []):
    if isinstance(t_info, dict):
        ticker_company[t_info["ticker"]] = t_info.get("company", t_info["ticker"])
tickers = sorted(data["ticker"].unique().tolist())

# Handoff: the screener writes st.session_state["ticker"]. Default/repair to a
# valid ticker so the bound selectbox always initialises on a real option.
if "ticker" not in st.session_state or st.session_state["ticker"] not in tickers:
    st.session_state["ticker"] = "AAPL" if "AAPL" in tickers else (tickers[0] if tickers else "")

# Ticker selector (bound to session_state) + back-to-screener button.
sel_col, _, back_col = st.columns([2, 3, 1])
with sel_col:
    ticker = st.selectbox(
        f"Search {len(tickers)} tickers",
        tickers,
        key="ticker",
        format_func=lambda t: f"{t} — {ticker_company.get(t, '')}",
        label_visibility="visible",
    )
with back_col:
    st.write("")
    if st.button("← Screener", width="stretch"):
        st.switch_page("views/screener.py")

# Masthead + KPI strip — single markdown block so the spacing matches the spec.
st.markdown(render_masthead(ticker, data, meta), unsafe_allow_html=True)
st.markdown(render_kpi_strip(ticker, data, metrics), unsafe_allow_html=True)

# Unit scale for the statement tables (IS / BS / CF / Quarterly). One control governs
# all four. Billion = 1e9 (US short scale). Default "Units" → unchanged full-resolution USD.
# NOTE: this control is irrelevant to the Price tab (per-share adjusted close, not money
# amounts) — the Price chart deliberately ignores it. Don't try to hide it on that tab:
# st.tabs exposes no active-tab signal.
SCALE_OPTIONS = {
    "Units":     (1,             ""),
    "Thousands": (1_000,         "thousands"),
    "Millions":  (1_000_000,     "millions"),
    "Billions":  (1_000_000_000, "billions"),
}
scale_label = st.segmented_control(
    "Units", options=list(SCALE_OPTIONS), default="Units",
    key="unit_scale",
    help="Scale all money amounts in the statement tables. Billion = 1e9 (US short scale).",
)
divisor, scale_word = SCALE_OPTIONS[scale_label or "Units"]

tab_px, tab_is, tab_bs, tab_cf, tab_dm, tab_qt = st.tabs(
    ["Price", "Income statement", "Balance sheet", "Cash flow", "Derived metrics", "Quarterly"]
)

with tab_px:
    # Header text is window/frequency-aware, so reserve its slot now and fill it once both are
    # resolved below — keeps it visually on top while computed last.
    header_slot = st.empty()
    # Frequency control + window buttons as ONE tight, left-aligned row. st.columns always
    # expands to full page width (which spreads the buttons apart), so instead wrap both controls
    # in a keyed st.container and flip its vertical block to a content-width flex row via CSS.
    st.markdown(
        "<style>"
        # .st-key-pxctrl IS the container's (already display:flex) vertical block — flip it to a row.
        ".st-key-pxctrl{flex-direction:row;flex-wrap:wrap;align-items:center;gap:6px;}"
        '.st-key-pxctrl>[data-testid="stElementContainer"]{width:auto;}'
        '.st-key-pxctrl>[data-testid="stElementContainer"]:has(>[data-testid="stMarkdownContainer"])'
        "{display:none;}"   # collapse the style-only markdown so it adds no gap in the flex row
        "</style>",
        unsafe_allow_html=True,
    )
    with st.container(key="pxctrl"):
        freq = st.segmented_control(
            "Frequency", ["Daily", "Weekly", "Monthly"], default="Daily",
            key="price_freq", label_visibility="collapsed",
        ) or "Daily"

        # Quick-range window. On a frequency switch, keep the current window if it still exists in
        # the new (narrower) set, else fall back to that frequency's default.
        window_set = WINDOW_SETS[freq]
        active_window = st.session_state.get("price_window", WINDOW_DEFAULTS[freq])
        if active_window not in window_set:
            active_window = WINDOW_DEFAULTS[freq]
        st.session_state["price_window"] = active_window

        # One st.button per option; CSS highlights the active key. Click → record + rerun so the
        # active highlight and the windowed KPIs/chart redraw.
        st.markdown(price_window_css(active_window), unsafe_allow_html=True)
        for label in window_set:
            if st.button(label, key=f"pxwin-{label}"):
                st.session_state["price_window"] = label
                st.rerun()

    header_slot.markdown(
        '<div class="panel-header"><h2>Price</h2>'
        f'<div class="meta">Adjusted close · SMA 20/50/200 · last {active_window} · {freq} '
        '· source: market_prices_daily</div></div>',
        unsafe_allow_html=True,
    )

    pdf_full = prices_for(prices, ticker)     # full daily history (+ SMAs) — anchors LATEST PRICE
    if pdf_full.empty:
        st.info("No price history available for this ticker yet. "
                "Prices publish with the next pipeline run (51 → 52).")
    else:
        pdf_win = slice_window(pdf_full, active_window)   # trailing window → KPIs + chart
        st.markdown(render_price_kpis(pdf_win), unsafe_allow_html=True)   # 4-card strip
        chart = price_chart(resample_prices(pdf_win, freq), ticker, freq)
        st.altair_chart(chart, use_container_width=True)

with tab_is:
    df_is = income_statement_df(data, ticker)
    st.markdown(
        '<div class="panel-header"><h2>Income statement</h2>'
        '<div class="meta">Up to 10 fiscal years · USD · concept_hierarchy.json</div></div>',
        unsafe_allow_html=True,
    )
    # Combo chart is fixed in $B (like the Quarterly combo) → ignores the unit-scale control.
    st.markdown(render_is_revenue_combo(df_is), unsafe_allow_html=True)
    st.markdown(render_table_html(df_is, statement="is", ticker=ticker, notes=notes, divisor=divisor, scale_word=scale_word), unsafe_allow_html=True)
    st.markdown(render_waterfall(df_is), unsafe_allow_html=True)

with tab_bs:
    df_bs = balance_sheet_df(data, ticker)
    st.markdown(
        '<div class="panel-header"><h2>Balance sheet</h2>'
        '<div class="meta">Fiscal year-end snapshots · USD</div></div>',
        unsafe_allow_html=True,
    )
    # Composition chart is fixed in $B (like the Quarterly combo) → ignores the unit-scale
    # control. Year picker defaults to the latest fiscal year; the chart redraws on change.
    bs_years = get_year_columns(df_bs)
    if bs_years:
        bs_year = st.selectbox(
            "Fiscal year", list(reversed(bs_years)), index=0,
            key="bs_year", format_func=lambda y: f"FY {y}",
        )
        st.markdown(render_bs_composition(df_bs, bs_year), unsafe_allow_html=True)
    st.markdown(render_table_html(df_bs, statement="bs", ticker=ticker, notes=notes, divisor=divisor, scale_word=scale_word), unsafe_allow_html=True)
    st.markdown(render_balance_check(df_bs), unsafe_allow_html=True)

with tab_cf:
    df_cf = cash_flow_df(data, ticker)
    st.markdown(
        '<div class="panel-header"><h2>Cash flow statement</h2>'
        '<div class="meta">Operating · Investing · Financing</div></div>',
        unsafe_allow_html=True,
    )
    # OCF/FCF chart is fixed in $B (like the Quarterly combo) → ignores the unit-scale control.
    st.markdown(render_cf_fcf(df_cf), unsafe_allow_html=True)
    st.markdown(render_table_html(df_cf, statement="cf", ticker=ticker, notes=notes, divisor=divisor, scale_word=scale_word), unsafe_allow_html=True)

with tab_dm:
    st.markdown(
        '<div class="panel-header"><h2>Derived metrics</h2>'
        '<div class="meta">From metrics_hierarchy.json</div></div>',
        unsafe_allow_html=True,
    )
    iv_period = st.segmented_control(
        "Intrinsic value",
        ["FY", "TTM"],
        default="FY",
        key="iv_period",
        label_visibility="collapsed",
    )
    # segmented_control returns None if the user deselects the active chip.
    iv_period = iv_period or "FY"
    st.markdown(render_metrics_grid(metrics, ticker, iv_period=iv_period), unsafe_allow_html=True)
    # Valuation football field — fills the gap below the grid. Price is backed out of the
    # IV methods' MoS rows (the app stores no per-share price directly). Returns "" → hidden.
    ff_price = iv_price_from_metrics(metrics, ticker, iv_period=iv_period)
    st.markdown(
        render_valuation_football_field(metrics, ticker, ff_price, iv_period=iv_period),
        unsafe_allow_html=True,
    )

with tab_qt:
    df_q = quarterly_df(data, ticker)
    st.markdown(
        '<div class="panel-header"><h2>Quarterly</h2>'
        '<div class="meta">Last 12 quarters · USD · YoY = same quarter prior year</div></div>',
        unsafe_allow_html=True,
    )
    st.markdown(render_quarterly_combo(df_q), unsafe_allow_html=True)
    st.markdown(render_table_html(df_q, statement="qt", ticker=ticker, notes=notes, divisor=divisor, scale_word=scale_word), unsafe_allow_html=True)

st.markdown(render_footnote_bar(meta, app_version()), unsafe_allow_html=True)
