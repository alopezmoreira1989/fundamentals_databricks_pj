"""Fetch and cache the parquet artifacts published by 50_publish/."""

from __future__ import annotations

import io
import json
import subprocess
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

# Columnas de texto de baja cardinalidad → category (enorme ahorro de RAM).
_CATEGORICAL_COLS = (
    "ticker", "period_type", "stmt", "section", "group",
    "concept", "display_name", "category", "subcategory", "metric", "unit",
)


def _optimize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Reduce la RAM residente: strings repetidos → category, numéricos → menor precisión."""
    for col in _CATEGORICAL_COLS:
        if col in df.columns and df[col].dtype == object:
            df[col] = df[col].astype("category")
    if "fiscal_year" in df.columns:
        df["fiscal_year"] = pd.to_numeric(df["fiscal_year"], downcast="integer")
    for col in ("value", "sort_order"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], downcast="float")
    return df


@st.cache_data(ttl=3600, max_entries=1, show_spinner="Loading financial data…")
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