"""Company detail — the per-ticker editorial dashboard (5 tabs).

Receives the ticker to show via ``st.session_state["ticker"]`` (the handoff
convention the screener writes). Keeps its own searchable selectbox so the page
also works standalone. `set_page_config` / CSS injection live in the router
(`app.py`), not here.
"""

from pathlib import Path

import streamlit as st

from lib.data import app_version, load_latest_data, load_notes
from lib.kpis import render_kpi_strip
from lib.quarterly import render_quarterly_combo
from lib.render import (
    render_balance_check,
    render_footnote_bar,
    iv_price_from_metrics,
    render_masthead,
    render_metrics_grid,
    render_table_html,
    render_valuation_football_field,
    render_waterfall,
)
from lib.tables import (
    balance_sheet_df,
    cash_flow_df,
    income_statement_df,
    quarterly_df,
)

APP_DIR = Path(__file__).parents[1]

data, metrics, meta = load_latest_data()
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
    if st.button("← Screener", use_container_width=True):
        st.switch_page("views/screener.py")

# Masthead + KPI strip — single markdown block so the spacing matches the spec.
st.markdown(render_masthead(ticker, data, meta), unsafe_allow_html=True)
st.markdown(render_kpi_strip(ticker, data, metrics), unsafe_allow_html=True)

# Unit scale for the statement tables (IS / BS / CF / Quarterly). One control governs
# all four. Billion = 1e9 (US short scale); a Spanish label would be "Miles de millones",
# NOT "Billones" (Spanish billón = 1e12). Default "Units" → unchanged full-resolution USD.
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

tab_is, tab_bs, tab_cf, tab_dm, tab_qt = st.tabs(
    ["Income statement", "Balance sheet", "Cash flow", "Derived metrics", "Quarterly"]
)

with tab_is:
    df_is = income_statement_df(data, ticker)
    st.markdown(
        '<div class="panel-header"><h2>Income statement</h2>'
        '<div class="meta">Up to 10 fiscal years · USD · concept_hierarchy.json</div></div>',
        unsafe_allow_html=True,
    )
    st.markdown(render_table_html(df_is, statement="is", ticker=ticker, notes=notes, divisor=divisor, scale_word=scale_word), unsafe_allow_html=True)
    st.markdown(render_waterfall(df_is), unsafe_allow_html=True)

with tab_bs:
    df_bs = balance_sheet_df(data, ticker)
    st.markdown(
        '<div class="panel-header"><h2>Balance sheet</h2>'
        '<div class="meta">Fiscal year-end snapshots · USD</div></div>',
        unsafe_allow_html=True,
    )
    st.markdown(render_table_html(df_bs, statement="bs", ticker=ticker, notes=notes, divisor=divisor, scale_word=scale_word), unsafe_allow_html=True)
    st.markdown(render_balance_check(df_bs), unsafe_allow_html=True)

with tab_cf:
    df_cf = cash_flow_df(data, ticker)
    st.markdown(
        '<div class="panel-header"><h2>Cash flow statement</h2>'
        '<div class="meta">Operating · Investing · Financing</div></div>',
        unsafe_allow_html=True,
    )
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
