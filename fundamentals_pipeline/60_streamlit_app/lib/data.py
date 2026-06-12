"""Fetch and cache the parquet artifacts published by 50_publish/."""

from __future__ import annotations

import io
import json
import os
import subprocess
from pathlib import Path
from typing import Any, NoReturn

import pandas as pd
import requests
import streamlit as st

OWNER = "alopezmoreira1989"
REPO  = "fundamentals_databricks_pj"
BASE_URL = f"https://github.com/{OWNER}/{REPO}/releases/download/latest"

DATA_FILE   = "dashboard_data.parquet"
METRIC_FILE = "dashboard_metrics.parquet"
META_FILE   = "dashboard_meta.json"
PRICE_FILE  = "dashboard_prices.parquet"

FIXTURE_DIR = Path(__file__).parent.parent / "fixtures"

# Low-cardinality text columns → category (huge RAM savings).
_CATEGORICAL_COLS = (
    "ticker", "period_type", "stmt", "section", "group",
    "concept", "display_name", "category", "subcategory", "metric", "unit",
)


def _optimize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Reduce resident RAM: repeated strings → category, numerics → smaller precision."""
    for col in _CATEGORICAL_COLS:
        if col in df.columns and df[col].dtype == object:
            df[col] = df[col].astype("category")
    if "fiscal_year" in df.columns:
        df["fiscal_year"] = pd.to_numeric(df["fiscal_year"], downcast="integer")
    # NOTE: do NOT downcast `value` to float32 — it has ~7 significant digits,
    # which corrupts large raw-USD figures (e.g. 274,515,000,000) that the
    # IS/BS/CF tables render at full resolution via fmt_num ({v:,.0f}). The
    # category conversions above already deliver the real RAM savings.
    if "sort_order" in df.columns:
        df["sort_order"] = pd.to_numeric(df["sort_order"], downcast="float")
    return df


@st.cache_data(ttl=3600, max_entries=1, show_spinner="Loading financial data…")
def load_latest_data() -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Return (financials, metrics, meta).

    Primary source is the GitHub Release `latest` (real data, picked up
    automatically when the pipeline publishes); committed `fixtures/` are the
    fallback (synthetic preview when the Release is missing).

    Dev override: set `DASHBOARD_USE_FIXTURES=1` to read the local `fixtures/`
    directly and skip the network — handy for testing freshly regenerated
    fixtures locally. Default off, so Streamlit Cloud behaviour is unchanged.
    """
    use_fixtures = os.environ.get("DASHBOARD_USE_FIXTURES") == "1"
    if use_fixtures and (FIXTURE_DIR / DATA_FILE).exists():
        data    = pd.read_parquet(FIXTURE_DIR / DATA_FILE)
        metrics = pd.read_parquet(FIXTURE_DIR / METRIC_FILE)
        meta    = json.loads((FIXTURE_DIR / META_FILE).read_text())
    else:
        try:
            data    = _fetch_parquet(DATA_FILE)
            metrics = _fetch_parquet(METRIC_FILE)
            meta    = _fetch_json(META_FILE)
        except Exception as exc:
            # Fallback to local fixtures (dev). If absent, show readable error (no traceback).
            if (FIXTURE_DIR / DATA_FILE).exists():
                data    = pd.read_parquet(FIXTURE_DIR / DATA_FILE)
                metrics = pd.read_parquet(FIXTURE_DIR / METRIC_FILE)
                meta    = json.loads((FIXTURE_DIR / META_FILE).read_text())
            else:
                _render_load_error(exc)   # st.error + st.stop(), no retorna

    # Normalize types: parquet preserves None (not NaN) for missing strings,
    # which breaks pandas groupby/pivot. Convert to proper NaN.
    for col in ("section", "group", "display_name"):
        if col in data.columns:
            data[col] = data[col].fillna("")
        if col in metrics.columns:
            metrics[col] = metrics[col].fillna("")

    data["period_end"] = pd.to_datetime(data["period_end"])
    if "period_end" in metrics.columns:
        metrics["period_end"] = pd.to_datetime(metrics["period_end"])

    data    = _optimize_dtypes(data)
    metrics = _optimize_dtypes(metrics)
    return data, metrics, meta


@st.cache_data(ttl=3600, max_entries=1, show_spinner="Loading prices…")
def load_prices() -> pd.DataFrame:
    """Daily price slice published by 51/52 (ticker, date, close, adj_close).

    MUST degrade gracefully: missing artifact (404 / not published) or absent
    fixture → return an EMPTY frame with the right columns. Never st.stop() — the
    Price tab shows a 'no data' notice and the rest of the page keeps working.

    Kept separate from load_latest_data so the heavy daily frame gets its own cache
    entry and is materialized lazily (only when the Price tab is first viewed).
    """
    empty = pd.DataFrame(columns=["ticker", "date", "close", "adj_close"])
    use_fixtures = os.environ.get("DASHBOARD_USE_FIXTURES") == "1"
    if use_fixtures:
        df = pd.read_parquet(FIXTURE_DIR / PRICE_FILE) if (FIXTURE_DIR / PRICE_FILE).exists() else empty
    else:
        try:
            df = _fetch_parquet(PRICE_FILE)
        except Exception:
            # Fall back to a local fixture if present; otherwise degrade to "no data".
            df = pd.read_parquet(FIXTURE_DIR / PRICE_FILE) if (FIXTURE_DIR / PRICE_FILE).exists() else empty

    if df.empty:
        return empty

    df["date"] = pd.to_datetime(df["date"])
    # Do NOT route through _optimize_dtypes: it would downcast nothing harmful here,
    # but close/adj_close must stay float64 (BRK.A-class prices lose cents in float32).
    df["ticker"] = df["ticker"].astype("category")
    return df


def _render_load_error(exc: Exception) -> NoReturn:
    """Show a readable st.error based on failure type and stop execution.

    Distinguishes the "data not yet published" case (missing `latest` Release → 404)
    from other network failures. `st.stop()` prevents the raw traceback from surfacing.
    """
    is_404 = (
        isinstance(exc, requests.HTTPError)
        and getattr(exc.response, "status_code", None) == 404
    )
    if is_404:
        st.error(
            "**The dashboard data has not been published yet.**\n\n"
            "The app downloads its data from the GitHub `latest` Release, which "
            "right now does not exist or is not accessible. "
            "Publish it by running in Databricks:\n\n"
            "1. `50_publish/51__export_dashboard_data`\n"
            "2. `50_publish/52__publish_to_github`\n\n"
            "Once the `latest` Release is available, reload this page."
        )
    else:
        st.error(
            "**The dashboard data could not be loaded.**\n\n"
            f"Error contacting the data source: `{type(exc).__name__}`. "
            "Retry in a few minutes; if it persists, check the repo's `latest` Release."
        )
    st.stop()


def _fetch_parquet(name: str) -> pd.DataFrame:
    r = requests.get(f"{BASE_URL}/{name}", timeout=30)
    r.raise_for_status()
    return pd.read_parquet(io.BytesIO(r.content))


def _fetch_json(name: str) -> dict[str, Any]:
    r = requests.get(f"{BASE_URL}/{name}", timeout=10)
    r.raise_for_status()
    return r.json()


@st.cache_data
def app_version() -> str:
    """Short git SHA of the deployed app code.

    Streamlit Community Cloud `git clone`s the repo, so `.git` is present at
    runtime — this lets you confirm a redeploy actually picked up your push.
    Falls back to reading .git/HEAD if the git binary is unavailable.
    """
    repo_root = Path(__file__).resolve().parents[3]
    try:
        sha = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=repo_root, capture_output=True, text=True, timeout=5,
        ).stdout.strip()
        if sha:
            return sha
    except Exception:
        pass
    try:
        head = (repo_root / ".git" / "HEAD").read_text().strip()
        if head.startswith("ref:"):
            ref = head.split(" ", 1)[1].strip()
            return (repo_root / ".git" / ref).read_text().strip()[:7]
        return head[:7]
    except Exception:
        return "unknown"


@st.cache_data
def load_notes(path: Path) -> dict[str, Any]:
    """Load ticker-specific footnotes from notes.json (keyed by ticker → period → concept)."""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return {k: v for k, v in raw.items() if not k.startswith("_")}
    except FileNotFoundError:
        return {}
