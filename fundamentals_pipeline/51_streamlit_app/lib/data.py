"""Fetch and cache the parquet artifacts published by 50_publish/."""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import streamlit as st

OWNER = "alopezmoreira1989"
REPO  = "fundamentals_databricks_pj"
BASE_URL = f"https://github.com/{OWNER}/{REPO}/releases/download/latest"

DATA_FILE   = "dashboard_data.parquet"
METRIC_FILE = "dashboard_metrics.parquet"
META_FILE   = "dashboard_meta.json"

FIXTURE_DIR = Path(__file__).parent.parent / "fixtures"


@st.cache_data(ttl=3600, show_spinner="Loading financial data…")
def load_latest_data() -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Return (financials, metrics, meta).

    Looks in `fixtures/` first (local dev), falls back to the GitHub Release URL
    so the same app works locally and on Streamlit Cloud without code changes.
    """
    try:
        data    = _fetch_parquet(DATA_FILE)
        metrics = _fetch_parquet(METRIC_FILE)
        meta    = _fetch_json(META_FILE)
    except Exception:
        if (FIXTURE_DIR / DATA_FILE).exists():
            data    = pd.read_parquet(FIXTURE_DIR / DATA_FILE)
            metrics = pd.read_parquet(FIXTURE_DIR / METRIC_FILE)
            meta    = json.loads((FIXTURE_DIR / META_FILE).read_text())
        else:
            raise

    # Normalize types: parquet preserves them, but JSON metadata round-trips dates as strings.
    data["period_end"]    = pd.to_datetime(data["period_end"])
    if "period_end" in metrics.columns:
        metrics["period_end"] = pd.to_datetime(metrics["period_end"])
    return data, metrics, meta


def _fetch_parquet(name: str) -> pd.DataFrame:
    r = requests.get(f"{BASE_URL}/{name}", timeout=30)
    r.raise_for_status()
    return pd.read_parquet(io.BytesIO(r.content))


def _fetch_json(name: str) -> dict[str, Any]:
    r = requests.get(f"{BASE_URL}/{name}", timeout=10)
    r.raise_for_status()
    return r.json()


@st.cache_data
def load_notes(path: Path) -> dict[str, Any]:
    """Load ticker-specific footnotes from notes.json (keyed by ticker → period → concept)."""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return {k: v for k, v in raw.items() if not k.startswith("_")}
    except FileNotFoundError:
        return {}