"""Tests for the infrastructure tier (storage + duckdb) against fixture artifacts.

No database and no network: the ``artifacts_from_fixtures`` fixture repoints the storage
layer at the committed Streamlit fixtures, and DuckDB queries the parquet files directly.
"""

from __future__ import annotations

import pytest
from infrastructure import duckdb as db
from infrastructure import storage


def test_meta_validates_and_lists_tickers(artifacts_from_fixtures):
    meta = storage.meta()
    assert meta["schema_version"]
    assert isinstance(meta["tickers"], list) and meta["tickers"]
    # every ticker record carries at least the contract-required keys
    assert all("ticker" in t and "company" in t for t in meta["tickers"])


def test_ensure_valid_passes_on_fixtures(artifacts_from_fixtures):
    # core artifacts (data + metrics) satisfy fundamentals_pipeline.schemas → no raise
    storage.ensure_valid()


def test_parquet_path_rejects_unknown_artifact(artifacts_from_fixtures):
    with pytest.raises(ValueError):
        storage.parquet_path("not_an_artifact")


def test_duckdb_registers_present_views_only(artifacts_from_fixtures):
    con = db.connect()
    try:
        views = {r[0] for r in con.execute(
            "SELECT view_name FROM duckdb_views() WHERE NOT internal"
        ).fetchall()}
    finally:
        con.close()
    # data + metrics always present; prices present in the fixtures; backtest fixture absent.
    assert {"financials", "metrics"} <= views
    assert "backtest" not in views


def test_query_financials_has_rows(artifacts_from_fixtures):
    rows = db.query("SELECT COUNT(*) AS n, COUNT(DISTINCT ticker) AS t FROM financials")
    assert rows[0]["n"] > 0
    assert rows[0]["t"] > 0


def test_query_uses_bound_params(artifacts_from_fixtures):
    rows = db.query(
        "SELECT DISTINCT ticker FROM metrics WHERE ticker = ?",
        ["AAPL"],
    )
    assert rows == [{"ticker": "AAPL"}]
