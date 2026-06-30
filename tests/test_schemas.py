"""Tests for the export↔Streamlit schema contract (_core/schemas.py).

The contract-logic tests build **synthetic in-memory** frames/dicts that satisfy
``_core/schemas.py`` — they never read a file, so the suite is green on a clean checkout
where the ``fixtures/`` parquet/meta are NOT committed (only ``.gitkeep`` is; they are
generated at runtime by ``fetch_fixtures.py`` / ``generate_russell2000_fixtures.py``).

The real-fixture validations are kept as thin tests, each ``skipif`` the file is absent,
so they skip cleanly on a clean clone yet still run locally when fixtures exist.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from fundamentals_pipeline._core import schemas

FIXTURE_DIR = (
    Path(__file__).resolve().parent.parent
    / "fundamentals_pipeline" / "60__frontends" / "61__streamlit" / "fixtures"
)


# ── synthetic builders (satisfy the contract; no file reads) ──────────────────────
def _data_frame() -> pd.DataFrame:
    return pd.DataFrame({
        "ticker": pd.Series(["AAPL", "AAPL"], dtype="object"),
        "period_type": pd.Series(["FY", "FY"], dtype="object"),
        "period_end": pd.to_datetime(["2022-09-24", "2023-09-30"]),
        "fiscal_year": pd.Series([2022, 2023], dtype="int64"),
        "stmt": pd.Series(["Income Statement", "Income Statement"], dtype="object"),
        "section": pd.Series(["", ""], dtype="object"),
        "group": pd.Series(["", ""], dtype="object"),
        "concept": pd.Series(["Revenue", "Revenue"], dtype="object"),
        "display_name": pd.Series(["Revenue", "Revenue"], dtype="object"),
        "sort_order": pd.Series([10.0, 10.0], dtype="float64"),
        "value": pd.Series([394328000000.0, 383285000000.0], dtype="float64"),
    })


def _metrics_frame() -> pd.DataFrame:
    return pd.DataFrame({
        "ticker": pd.Series(["AAPL", "AAPL"], dtype="object"),
        "period_type": pd.Series(["FY", "FY"], dtype="object"),
        "period_end": pd.to_datetime(["2022-12-31", "2023-12-31"]),
        "fiscal_year": pd.Series([2022, 2023], dtype="int64"),
        "category": pd.Series(["Profitability", "Profitability"], dtype="object"),
        "subcategory": pd.Series(["Margins", "Margins"], dtype="object"),
        "metric": pd.Series(["Net Margin %", "Net Margin %"], dtype="object"),
        "unit": pd.Series(["percent", "percent"], dtype="object"),
        "sort_order": pd.Series([10.0, 10.0], dtype="float64"),
        "value": pd.Series([25.31, 25.06], dtype="float64"),
    })


def _meta_dict() -> dict:
    return {
        "schema_version": 6,
        "build_timestamp": "2026-06-16T00:00:00+00:00",
        "tickers": [
            {"ticker": "AAPL", "company": "Apple Inc", "sector": "Information Technology",
             "is_favorite": False, "in_sp500": True, "in_r3000": True},
        ],
        "fy_ranges": [{"ticker": "AAPL", "fy_min": 2014, "fy_max": 2023}],
        "row_counts": {"financials": 2, "metrics": 2, "prices": 0, "backtest": 0},
        "retention": {"fy_years": 10, "quarters": 12, "price_years": 10},
    }


def _prices_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ticker": pd.Series(["AAPL", "AAPL"], dtype="object"),
            "date": pd.to_datetime(["2024-01-02", "2024-01-03"]),
            "close": pd.Series([185.0, 184.25], dtype="float64"),
            "adj_close": pd.Series([184.5, 183.8], dtype="float64"),
        }
    )


def _backtest_frame() -> pd.DataFrame:
    return pd.DataFrame({
        "archetype": pd.Series(["graham_defensive", "graham_defensive"], dtype="object"),
        "fiscal_year": pd.Series([2022, 2023], dtype="int64"),
        "portfolio_return": pd.Series([0.12, -0.04], dtype="float64"),
        "benchmark_return": pd.Series([0.10, -0.05], dtype="float64"),
        "portfolio_value": pd.Series([112.0, 107.5], dtype="float64"),
        "benchmark_value": pd.Series([110.0, 104.5], dtype="float64"),
        "n_holdings": pd.Series([18, 22], dtype="int64"),
    })


# ── positive: synthetic frames satisfy the contract ──────────────────────────────
def test_synthetic_data_is_valid():
    assert schemas.validate_artifact("dashboard_data", _data_frame()) == []
    schemas.assert_artifact("dashboard_data", _data_frame())  # does not raise


def test_synthetic_metrics_is_valid():
    assert schemas.validate_artifact("dashboard_metrics", _metrics_frame()) == []
    schemas.assert_artifact("dashboard_metrics", _metrics_frame())


def test_synthetic_meta_is_valid():
    assert schemas.validate_meta(_meta_dict()) == []
    schemas.assert_meta(_meta_dict())


def test_meta_industry_is_optional_additive():
    # `industry` (schema v8) is additive/optional: present → valid; absent → still valid
    # (old artifacts must keep loading; it is NOT in TICKER_REQUIRED_KEYS).
    with_industry = _meta_dict()
    with_industry["schema_version"] = 8
    with_industry["tickers"][0]["industry"] = "Consumer Electronics"
    assert schemas.validate_meta(with_industry) == []
    assert "industry" not in schemas.TICKER_REQUIRED_KEYS
    assert schemas.validate_meta(_meta_dict()) == []  # no industry key → still valid


def test_synthetic_prices_is_valid():
    assert schemas.validate_artifact("dashboard_prices", _prices_frame()) == []


def test_synthetic_backtest_is_valid():
    assert schemas.validate_artifact("dashboard_backtest", _backtest_frame()) == []


def test_backtest_null_benchmark_still_valid():
    # SPY absent → benchmark_* all-NaN float (still numeric family).
    df = _backtest_frame()
    df["benchmark_return"] = float("nan")
    df["benchmark_value"] = float("nan")
    assert schemas.validate_artifact("dashboard_backtest", df) == []


def test_empty_typed_backtest_is_valid():
    empty = pd.DataFrame({c: pd.Series(dtype=t) for c, t in {
        "archetype": "object", "fiscal_year": "int64", "portfolio_return": "float64",
        "benchmark_return": "float64", "portfolio_value": "float64",
        "benchmark_value": "float64", "n_holdings": "int64",
    }.items()})
    assert schemas.validate_artifact("dashboard_backtest", empty) == []


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


# ── negative: violations are reported (synthetic inputs) ──────────────────────────
def test_missing_column_reported():
    broken = _data_frame().drop(columns=["value"])
    violations = schemas.validate_artifact("dashboard_data", broken)
    assert any("missing required column 'value'" in v for v in violations)
    with pytest.raises(schemas.SchemaError):
        schemas.assert_artifact("dashboard_data", broken)


def test_wrong_dtype_reported():
    broken = _data_frame()
    broken["value"] = broken["value"].astype(str)  # numeric → string
    violations = schemas.validate_artifact("dashboard_data", broken)
    assert any("column 'value'" in v and "expected" in v for v in violations)


def test_extra_columns_are_allowed():
    extended = _data_frame()
    extended["brand_new_column"] = 1
    assert schemas.validate_artifact("dashboard_data", extended) == []


def test_unknown_artifact_raises():
    with pytest.raises(ValueError):
        schemas.validate_artifact("not_an_artifact", _data_frame())


def test_meta_missing_key_reported():
    broken = _meta_dict()
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


# ── real-fixture validation: thin, skip-if-missing (fixtures are not committed) ───
@pytest.mark.skipif(not (FIXTURE_DIR / "dashboard_data.parquet").exists(),
                    reason="fixtures not committed; run fetch_fixtures.py")
def test_fixture_data_is_valid():
    df = pd.read_parquet(FIXTURE_DIR / "dashboard_data.parquet")
    assert schemas.validate_artifact("dashboard_data", df) == []


@pytest.mark.skipif(not (FIXTURE_DIR / "dashboard_metrics.parquet").exists(),
                    reason="fixtures not committed; run fetch_fixtures.py")
def test_fixture_metrics_is_valid():
    df = pd.read_parquet(FIXTURE_DIR / "dashboard_metrics.parquet")
    assert schemas.validate_artifact("dashboard_metrics", df) == []


@pytest.mark.skipif(not (FIXTURE_DIR / "dashboard_meta.json").exists(),
                    reason="fixtures not committed; run fetch_fixtures.py")
def test_fixture_meta_is_valid():
    meta = json.loads((FIXTURE_DIR / "dashboard_meta.json").read_text(encoding="utf-8"))
    assert schemas.validate_meta(meta) == []


# ── dtype_family ──────────────────────────────────────────────────────────────────
def test_dtype_family():
    assert schemas.dtype_family(pd.Series([1, 2, 3])) == "numeric"
    assert schemas.dtype_family(pd.Series([True, False])) == "bool"
    assert schemas.dtype_family(pd.Series(pd.to_datetime(["2024-01-01"]))) == "datetime"
    assert schemas.dtype_family(pd.Series(["a", "b"])) == "string"
    assert schemas.dtype_family(pd.Series(["a", "b"], dtype="category")) == "string"
