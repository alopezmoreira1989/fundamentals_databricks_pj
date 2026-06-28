"""Fetch and cache the parquet artifacts published by 50_publish/."""

from __future__ import annotations

import io
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, NoReturn, TypedDict

import pandas as pd
import requests
import streamlit as st

# Import the shared export↔app schema contract. Streamlit Cloud clones the whole repo, so
# parents[3] of this file is the repo root (.../60_streamlit_app/lib/data.py → repo root).
# _core is pure Python (no Spark/Databricks dep), so this keeps the app Databricks-free.
_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
from fundamentals_pipeline._core.schemas import (  # noqa: E402
    SchemaError,
    validate_artifact,
    validate_meta,
)

OWNER = "alopezmoreira1989"
REPO  = "fundamentals_databricks_pj"
BASE_URL = f"https://github.com/{OWNER}/{REPO}/releases/download/latest"

DATA_FILE     = "dashboard_data.parquet"
METRIC_FILE   = "dashboard_metrics.parquet"
META_FILE     = "dashboard_meta.json"
PRICE_FILE    = "dashboard_prices.parquet"
BACKTEST_FILE = "dashboard_backtest.parquet"

FIXTURE_DIR = Path(__file__).parent.parent / "fixtures"

# Low-cardinality text columns → category (huge RAM savings).
_CATEGORICAL_COLS = (
    "ticker", "period_type", "stmt", "section", "group",
    "concept", "display_name", "category", "subcategory", "metric", "unit",
)


def _resolve_data_ttl() -> int:
    """Cache TTL (seconds) for the Release-artifact loaders. Default 600 (10 min) so the
    app picks up a freshly published Release without a manual reboot. Override per
    environment via the ``DASHBOARD_DATA_TTL`` env var / Streamlit secret — e.g. a shorter
    value on staging for fast QA, a longer one on prod. Read once at import so it can
    parameterise the ``@st.cache_data`` decorators below; same code on both branches."""
    raw = os.environ.get("DASHBOARD_DATA_TTL")
    if raw is None:
        try:
            raw = st.secrets.get("DASHBOARD_DATA_TTL")   # Streamlit Cloud secret, if set
        except Exception:
            raw = None
    try:
        return max(30, int(raw)) if raw not in (None, "") else 600
    except (TypeError, ValueError):
        return 600


_DATA_TTL = _resolve_data_ttl()


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


@st.cache_data(ttl=_DATA_TTL, max_entries=1, show_spinner="Loading financial data…")
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

    # Schema contract — validate the RAW loaded frames (the contract tolerates the
    # object/date dtypes parquet yields pre-normalization). A violation means the
    # published artifacts drifted from what this app version expects → hard-stop with a
    # readable error rather than rendering a broken page (data + metrics are the core).
    violations = (
        validate_artifact("dashboard_data", data)
        + validate_artifact("dashboard_metrics", metrics)
        + validate_meta(meta)
    )
    if violations:
        _render_load_error(SchemaError("\n  - ".join(["Published data is incompatible with this app version:", *violations])))

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


class IndustryBenchmarks(TypedDict):
    # metric_name → ticker → 10y avg (NaN if < 2 data points)
    company_10y: dict[str, dict[str, float]]
    # metric_name → industry → median of latest-FY values across tickers (NaN if < 3 tickers)
    industry_ly: dict[str, dict[str, float]]


# Score metrics are levels you don't average for comps; MoS % is method-dependent + clamped.
_BENCH_EXCLUDE_METRICS = ("Piotroski F-Score", "Altman Z-Score", "Accruals Ratio")


@st.cache_data(ttl=_DATA_TTL, max_entries=1)
def compute_industry_benchmarks(metrics: pd.DataFrame, meta: dict) -> IndustryBenchmarks:
    """Per-metric benchmark context for the company-page derived-metrics grid.

    Two cached views over percent/ratio metrics (excluding MoS % and the score metrics):
      * ``company_10y`` — each ticker's mean of its last 10 FY values per metric
        (omitted when fewer than 2 points, so a single year isn't shown as an "average").
      * ``industry_ly`` — across all tickers sharing a Yahoo ``industry``, each ticker's
        latest-FY value per metric, then the MEDIAN (robust to outliers; omitted when
        fewer than 3 tickers contribute).

    Returned floats are plain Python ``float`` (Streamlit serialises the cache entry as JSON).
    """
    empty: IndustryBenchmarks = {"company_10y": {}, "industry_ly": {}}
    if metrics is None or metrics.empty:
        return empty

    df = metrics[
        (metrics["period_type"] == "FY") & (metrics["unit"].isin(["percent", "ratio"]))
    ].copy()
    # `metric` is category dtype after _optimize_dtypes — cast to str for the .str ops below.
    df["metric"] = df["metric"].astype(str)
    df = df[
        ~df["metric"].str.startswith("MoS %") & ~df["metric"].isin(_BENCH_EXCLUDE_METRICS)
    ]
    df["fiscal_year"] = pd.to_numeric(df["fiscal_year"], errors="coerce")
    df = df.dropna(subset=["fiscal_year", "value"])
    if df.empty:
        return empty

    # ── company_10y: mean of each ticker's last 10 FY values per metric (count ≥ 2) ──
    ordered = df.sort_values(["metric", "ticker", "fiscal_year"], ascending=[True, True, False])
    ordered["_rn"] = ordered.groupby(["metric", "ticker"], observed=True).cumcount()
    last10 = ordered[ordered["_rn"] < 10]
    c_agg = (
        last10.groupby(["metric", "ticker"], observed=True)["value"]
        .agg(avg="mean", cnt="count").reset_index()
    )
    c_agg = c_agg[c_agg["cnt"] >= 2]
    company_10y: dict[str, dict[str, float]] = {}
    for r in c_agg.itertuples(index=False):
        company_10y.setdefault(r.metric, {})[r.ticker] = float(r.avg)

    # ── industry_ly: median of each ticker's latest-FY value, per industry (n ≥ 3) ──
    industry_map = {
        t["ticker"]: t["industry"]
        for t in meta.get("tickers", [])
        if t.get("ticker") and t.get("industry")
    }
    industry_ly: dict[str, dict[str, float]] = {}
    if industry_map:
        di = df.copy()
        di["industry"] = di["ticker"].astype(str).map(industry_map)
        di = di.dropna(subset=["industry"])
        # One latest-FY row per (metric, industry, ticker) before taking the cross-sectional median.
        di = di.sort_values("fiscal_year").drop_duplicates(
            subset=["metric", "industry", "ticker"], keep="last"
        )
        i_agg = (
            di.groupby(["metric", "industry"], observed=True)
            .agg(med=("value", "median"), n=("ticker", "nunique")).reset_index()
        )
        i_agg = i_agg[i_agg["n"] >= 3]
        for r in i_agg.itertuples(index=False):
            industry_ly.setdefault(r.metric, {})[r.industry] = float(r.med)

    return {"company_10y": company_10y, "industry_ly": industry_ly}


@st.cache_data(ttl=_DATA_TTL, max_entries=1, show_spinner="Loading prices…")
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

    # SOFT schema check: the Price tab must never block the rest of the app, so on a
    # contract violation we log + degrade to "no data" instead of st.stop(). Validated on
    # the raw frame (date as object/date, ticker as object) — the contract accepts both.
    violations = validate_artifact("dashboard_prices", df)
    if violations:
        print("⚠️ dashboard_prices failed schema validation — degrading to no-data:\n  - " + "\n  - ".join(violations))
        return empty

    df["date"] = pd.to_datetime(df["date"])
    # Do NOT route through _optimize_dtypes: it would downcast nothing harmful here,
    # but close/adj_close must stay float64 (BRK.A-class prices lose cents in float32).
    df["ticker"] = df["ticker"].astype("category")
    return df


@st.cache_data(ttl=_DATA_TTL, max_entries=1, show_spinner="Loading backtest…")
def load_backtest() -> pd.DataFrame:
    """Backtest equity-curve series published by 51/52 (archetype, fiscal_year, returns, values).

    MUST degrade gracefully: missing artifact (404 / backtester not run) or absent fixture →
    return an EMPTY frame with the right columns. Never st.stop() — the Backtest view shows a
    'no data' notice and the rest of the app keeps working (same rule as load_prices).
    """
    cols = ["archetype", "fiscal_year", "portfolio_return", "benchmark_return",
            "portfolio_value", "benchmark_value", "n_holdings"]
    empty = pd.DataFrame(columns=cols)
    use_fixtures = os.environ.get("DASHBOARD_USE_FIXTURES") == "1"
    if use_fixtures:
        df = pd.read_parquet(FIXTURE_DIR / BACKTEST_FILE) if (FIXTURE_DIR / BACKTEST_FILE).exists() else empty
    else:
        try:
            df = _fetch_parquet(BACKTEST_FILE)
        except Exception:
            df = pd.read_parquet(FIXTURE_DIR / BACKTEST_FILE) if (FIXTURE_DIR / BACKTEST_FILE).exists() else empty

    if df.empty:
        return empty

    # SOFT schema check (same rule as load_prices — never st.stop()): degrade to no-data.
    violations = validate_artifact("dashboard_backtest", df)
    if violations:
        print("⚠️ dashboard_backtest failed schema validation — degrading to no-data:\n  - " + "\n  - ".join(violations))
        return empty

    df["archetype"] = df["archetype"].astype("category")
    return df


def _render_load_error(exc: Exception) -> NoReturn:
    """Show a readable st.error based on failure type and stop execution.

    Distinguishes the "data not yet published" case (missing `latest` Release → 404),
    an incompatible-format case (SchemaError), and other network failures. `st.stop()`
    prevents the raw traceback from surfacing.
    """
    if isinstance(exc, SchemaError):
        st.error(
            "**The published data is incompatible with this version of the app.**\n\n"
            "The data format no longer matches what the app expects — the app code and the "
            "published artifacts are out of sync. Redeploy the app from the matching commit, "
            "or re-run the publish step (`50_publish/51` → `52`).\n\n"
            f"```\n{exc}\n```"
        )
        st.stop()
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
