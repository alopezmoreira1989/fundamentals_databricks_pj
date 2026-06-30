"""
Public financials dashboard — Streamlit Community Cloud entry point (router).

Reads parquet artifacts published nightly by 50__publish/ to a GitHub Release
(`latest` tag) and renders the editorial-newspaper style spec'd in styles.css.
No Databricks dependency at runtime.

This file is the multipage router: it owns `set_page_config` + CSS injection and
declares the pages. The sidebar nav is hidden (`.streamlit/config.toml`
showSidebarNavigation=false) — navigation happens via `st.switch_page`:
  • Screener (landing): pick a company from a filterable table
  • Company: the per-ticker statements dashboard
  • Backtest: investment-archetype strategies vs SPY (degrades if not yet published)
"""

from pathlib import Path

import streamlit as st
from lib.render import inject_css

st.set_page_config(
    page_title="US Stock FA Screener",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Inject CSS + Google Fonts before any page renders so the first paint is styled.
inject_css(Path(__file__).parent / "styles.css")

screener = st.Page(
    "views/screener.py", title="Screener", icon="🔎", url_path="screener", default=True
)
compare = st.Page("views/compare.py", title="Sectors", icon="🧭", url_path="sectors")
company = st.Page("views/company.py", title="Company", icon="📊", url_path="company")
backtest = st.Page("views/backtest.py", title="Backtest", icon="📈", url_path="backtest")

st.navigation([screener, compare, company, backtest]).run()
