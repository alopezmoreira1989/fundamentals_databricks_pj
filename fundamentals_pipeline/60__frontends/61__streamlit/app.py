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

import logging
import os
from pathlib import Path

import streamlit as st
from lib.render import _logo_dev_key, inject_css

st.set_page_config(
    page_title="US Stock FA Screener",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Inject CSS + Google Fonts before any page renders so the first paint is styled.
inject_css(Path(__file__).parent / "styles.css")

# Deployment heads-up (once per session): on a deployed app — fixtures off — a missing
# Logo.dev key makes every company silently fall back to its monogram. This is exactly
# what a delete+redeploy does when the [logo_dev] secret isn't carried over (the Main
# file path is pinned, so moving the app dir forces that redeploy). Log it to the Cloud
# app logs (owner-visible) rather than an st.warning, which would nag every public visitor.
if "_logo_key_checked" not in st.session_state:
    st.session_state["_logo_key_checked"] = True
    if os.environ.get("DASHBOARD_USE_FIXTURES") != "1" and not _logo_dev_key():
        logging.getLogger(__name__).warning(
            "Logo.dev publishable_key missing from st.secrets — company logos will fall "
            "back to monograms. Re-add the [logo_dev] secret in Streamlit Cloud settings "
            "(secrets are wiped by the delete+redeploy that an app-dir move forces)."
        )

screener = st.Page(
    "views/screener.py", title="Screener", icon="🔎", url_path="screener", default=True
)
compare = st.Page("views/compare.py", title="Sectors", icon="🧭", url_path="sectors")
company = st.Page("views/company.py", title="Company", icon="📊", url_path="company")
backtest = st.Page("views/backtest.py", title="Backtest", icon="📈", url_path="backtest")

st.navigation([screener, compare, company, backtest]).run()
