"""DuckDB query engine over the cached Parquet artifacts.

DuckDB reads the published parquet files directly off disk — no server, no per-request
pandas materialization of the multi-million-row frames. ``services/storage`` guarantees a
fresh local copy of each artifact; this module registers those files as named views and
runs read-only SQL against them.

Views (each backed by one parquet artifact):
    financials   ← dashboard_data.parquet       (ticker, period_type, period_end, …, value)
    metrics      ← dashboard_metrics.parquet     (ticker, …, category, metric, unit, value)
    prices       ← dashboard_prices.parquet      (ticker, date, close, adj_close)
    backtest     ← dashboard_backtest.parquet    (archetype, fiscal_year, returns, values)

No financial logic lives here — repositories build domain reads on top of these views; all
ratios/valuations come from ``fundamentals_pipeline``.
"""

from __future__ import annotations

from typing import Any

import duckdb

from services.storage import PARQUET_FILES, parquet_path

# view name → artifact key (into fundamentals_pipeline.schemas.ARTIFACT_NAMES).
VIEWS: dict[str, str] = {
    "financials": "dashboard_data",
    "metrics": "dashboard_metrics",
    "prices": "dashboard_prices",
    "backtest": "dashboard_backtest",
}


def connect() -> duckdb.DuckDBPyConnection:
    """Open an in-memory DuckDB connection with the artifact views registered.

    Each view points at the fresh local parquet path from ``services/storage``. A missing
    optional artifact (prices/backtest may be absent when the backtester didn't run) is
    skipped — its view simply won't exist, and callers degrade to "no data", matching the
    Streamlit app. Missing core artifacts raise from ``parquet_path`` (storage layer).

    Connections are cheap; open one per request/repository call and close it (or use
    :func:`query`, which manages the lifecycle for you).
    """
    con = duckdb.connect(database=":memory:")
    for view, artifact in VIEWS.items():
        if artifact not in PARQUET_FILES:
            continue
        try:
            path = parquet_path(artifact)
        except Exception:
            # Optional artifact not available → skip its view; core artifacts raise upstream.
            continue
        # DDL can't take a bound parameter, so the path is inlined. It comes from our own
        # config (never user input); escape single quotes to be safe on odd paths.
        literal = str(path).replace("'", "''")
        con.execute(f"CREATE VIEW {view} AS SELECT * FROM read_parquet('{literal}')")
    return con


def query(sql: str, params: list[Any] | tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
    """Run a read-only ``sql`` query against the artifact views and return rows as dicts.

    Uses DuckDB positional parameters (``?``) — always pass user-derived values via
    ``params``, never string-format them into ``sql``. The connection is opened and closed
    around the call.
    """
    con = connect()
    try:
        cur = con.execute(sql, list(params) if params else [])
        columns = [d[0] for d in cur.description]
        return [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]
    finally:
        con.close()
