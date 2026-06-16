"""Tests for the export↔Streamlit schema contract (_core/schemas.py).

Validates the contract against the committed ``fixtures/`` parquet/meta (data, metrics,
meta) and a synthetic prices frame (no prices fixture is committed yet), plus negative
cases (missing column, wrong dtype, bad meta, unknown artifact name).
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from fundamentals_pipeline._core import schemas

FIXTURE_DIR = Path(__file__).resolve().parent.parent / "fundamentals_pipeline" / "60_streamlit_app" / "fixtures"


@pytest.fixture(scope="module")
def data_df() -> pd.DataFrame:
    return pd.read_parquet(FIXTURE_DIR / "dashboard_data.parquet")


@pytest.fixture(scope="module")
def metrics_df() -> pd.DataFrame:
    return pd.read_parquet(FIXTURE_DIR / "dashboard_metrics.parquet")


@pytest.fixture(scope="module")
def meta() -> dict:
    return json.loads((FIXTURE_DIR / "dashboard_meta.json").read_text(encoding="utf-8"))


def _prices_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ticker": pd.Series(["AAPL", "AAPL"], dtype="object"),
            "date": pd.to_datetime(["2024-01-02", "2024-01-03"]),
            "close": pd.Series([185.0, 184.25], dtype="float64"),
            "adj_close": pd.Series([184.5, 183.8], dtype="float64"),
        }
    )


# ── positive: committed fixtures satisfy the contract ────────────────────────────
def test_fixture_data_is_valid(data_df):
    assert schemas.validate_artifact("dashboard_data", data_df) == []
    schemas.assert_artifact("dashboard_data", data_df)  # does not raise


def test_fixture_metrics_is_valid(metrics_df):
    assert schemas.validate_artifact("dashboard_metrics", metrics_df) == []
    schemas.assert_artifact("dashboard_metrics", metrics_df)


def test_fixture_meta_is_valid(meta):
    assert schemas.validate_meta(meta) == []
    schemas.assert_meta(meta)


def test_synthetic_prices_is_valid():
    df = _prices_frame()
    assert schemas.validate_artifact("dashboard_prices", df) == []


def test_empty_typed_prices_is_valid():
    # Matches the empty-but-typed fallback that 51 writes when market_prices_daily is absent.
    empty = pd.DataFrame(
        {c: pd.Series(dtype=t) for c, t in
         {"ticker": "object", "date": "datetime64[ns]", "close": "float64", "adj_close": "float64"}.items()}
    )
    assert schemas.validate_artifact("dashboard_prices", empty) == []


def test_date_as_object_accepted_for_prices():
    # date32 reads as object pre-normalization — the contract must accept it.
    df = _prices_frame()
    df["date"] = df["date"].dt.date.astype("object")
    assert schemas.validate_artifact("dashboard_prices", df) == []


# ── negative: violations are reported ────────────────────────────────────────────
def test_missing_column_reported(data_df):
    broken = data_df.drop(columns=["value"])
    violations = schemas.validate_artifact("dashboard_data", broken)
    assert any("missing required column 'value'" in v for v in violations)
    with pytest.raises(schemas.SchemaError):
        schemas.assert_artifact("dashboard_data", broken)


def test_wrong_dtype_reported(data_df):
    broken = data_df.copy()
    broken["value"] = broken["value"].astype(str)  # numeric → string
    violations = schemas.validate_artifact("dashboard_data", broken)
    assert any("column 'value'" in v and "expected" in v for v in violations)


def test_extra_columns_are_allowed(data_df):
    extended = data_df.copy()
    extended["brand_new_column"] = 1
    assert schemas.validate_artifact("dashboard_data", extended) == []


def test_unknown_artifact_raises(data_df):
    with pytest.raises(ValueError):
        schemas.validate_artifact("not_an_artifact", data_df)


def test_meta_missing_key_reported(meta):
    broken = dict(meta)
    broken.pop("tickers")
    violations = schemas.validate_meta(broken)
    assert any("missing required key 'tickers'" in v for v in violations)


def test_meta_ticker_record_missing_company():
    bad = {
        "schema_version": 6,
        "build_timestamp": "x",
        "tickers": [{"ticker": "AAPL"}],  # no company
        "fy_ranges": [],
        "row_counts": {},
        "retention": {},
    }
    violations = schemas.validate_meta(bad)
    assert any("tickers[0]" in v for v in violations)


# ── dtype_family ─────────────────────────────────────────────────────────────────
def test_dtype_family():
    assert schemas.dtype_family(pd.Series([1, 2, 3])) == "numeric"
    assert schemas.dtype_family(pd.Series([True, False])) == "bool"
    assert schemas.dtype_family(pd.Series(pd.to_datetime(["2024-01-01"]))) == "datetime"
    assert schemas.dtype_family(pd.Series(["a", "b"])) == "string"
    assert schemas.dtype_family(pd.Series(["a", "b"], dtype="category")) == "string"
