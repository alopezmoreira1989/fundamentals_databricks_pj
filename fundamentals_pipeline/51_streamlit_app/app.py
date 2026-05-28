"""
Public financials dashboard — Streamlit Community Cloud entry point.

Reads parquet artifacts published nightly by 50_publish/ to a GitHub Release
(`latest` tag) and renders the editorial-newspaper style spec'd in
40_dashboards/templates/styles.css. No Databricks dependency at runtime.
"""

from pathlib import Path

import streamlit as st

from lib.data import app_version, load_latest_data, load_notes
from lib.kpis import render_kpi_strip
from lib.render import (
    inject_css,
    render_balance_check,
    render_footnote_bar,
    render_masthead,
    render_metrics_grid,
    render_table_html,
    render_waterfall,
)
from lib.quarterly import render_quarterly_combo
from lib.tables import (
    balance_sheet_df,
    cash_flow_df,
    income_statement_df,
    quarterly_df,
)

st.set_page_config(
    page_title="Financials Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Inject CSS + Google Fonts before any markup so the first paint is styled.
inject_css(Path(__file__).parent / "styles.css")

data, metrics, meta = load_latest_data()
notes = load_notes(Path(__file__).parent / "notes.json")

# Build ticker list with company names for search
ticker_company = {}
for t_info in meta.get("tickers", []):
    if isinstance(t_info, dict):
        ticker_company[t_info["ticker"]] = t_info.get("company", t_info["ticker"])
tickers = sorted(data["ticker"].unique().tolist())
default_idx = tickers.index("AAPL") if "AAPL" in tickers else 0

# Ticker selector with search — format_func shows "AAPL — Apple Inc."
sel_col, info_col = st.columns([2, 4])
with sel_col:
    ticker = st.selectbox(
        f"Search {len(tickers)} tickers",
        tickers,
        index=default_idx,
        format_func=lambda t: f"{t} — {ticker_company.get(t, '')}",
        label_visibility="visible",
    )

# Masthead + KPI strip — single markdown block so the spacing matches the spec.
st.markdown(render_masthead(ticker, data, meta), unsafe_allow_html=True)
st.markdown(render_kpi_strip(ticker, data, metrics), unsafe_allow_html=True)

tab_is, tab_bs, tab_cf, tab_dm, tab_qt = st.tabs(
    ["Income statement", "Balance sheet", "Cash flow", "Derived metrics", "Quarterly"]
)

with tab_is:
    df_is = income_statement_df(data, ticker)
    st.markdown(
        '<div class="panel-header"><h2>Income statement</h2>'
        '<div class="meta">Up to 10 fiscal years · USD millions · concept_hierarchy.json</div></div>',
        unsafe_allow_html=True,
    )
    st.markdown(render_table_html(df_is, statement="is", ticker=ticker, notes=notes), unsafe_allow_html=True)
    st.markdown(render_waterfall(df_is), unsafe_allow_html=True)

with tab_bs:
    df_bs = balance_sheet_df(data, ticker)
    st.markdown(
        '<div class="panel-header"><h2>Balance sheet</h2>'
        '<div class="meta">Fiscal year-end snapshots · USD millions</div></div>',
        unsafe_allow_html=True,
    )
    st.markdown(render_table_html(df_bs, statement="bs", ticker=ticker, notes=notes), unsafe_allow_html=True)
    st.markdown(render_balance_check(df_bs), unsafe_allow_html=True)

with tab_cf:
    df_cf = cash_flow_df(data, ticker)
    st.markdown(
        '<div class="panel-header"><h2>Cash flow statement</h2>'
        '<div class="meta">Operating · Investing · Financing</div></div>',
        unsafe_allow_html=True,
    )
    st.markdown(render_table_html(df_cf, statement="cf", ticker=ticker, notes=notes), unsafe_allow_html=True)

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

with tab_qt:
    df_q = quarterly_df(data, ticker)
    st.markdown(
        '<div class="panel-header"><h2>Quarterly</h2>'
        '<div class="meta">Last 12 quarters · USD billions · YoY = same quarter prior year</div></div>',
        unsafe_allow_html=True,
    )
    st.markdown(render_quarterly_combo(df_q), unsafe_allow_html=True)
    st.markdown(render_table_html(df_q, statement="qt", ticker=ticker, notes=notes), unsafe_allow_html=True)

st.markdown(render_footnote_bar(meta, app_version()), unsafe_allow_html=True)
