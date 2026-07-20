"""Read-only DuckDB access over the cached fundamentals parquet artifacts.

Query the cached parquet locally with DuckDB rather than a live database or Databricks
warehouse. ``connection()`` returns a raw connection with each artifact registered as a
view, named directly after the artifact (``dashboard_data``, ``dashboard_metrics``, …) —
the typed repositories in :mod:`fundamentals_screener.repositories` build the DTO-returning
domain reads on top of these views; this module owns none of that, only the connection.
"""

from __future__ import annotations

import duckdb

from fundamentals_pipeline import schemas

from . import data_source


def connection() -> duckdb.DuckDBPyConnection:
    """A DuckDB connection with each cached artifact registered as a view."""
    con = duckdb.connect(database=":memory:")
    directory = data_source.data_dir()
    for name in schemas.ARTIFACT_NAMES:
        path = directory / f"{name}.parquet"
        if path.exists():
            # CREATE VIEW can't be a prepared statement in DuckDB, so the path is inlined —
            # safe here since it's built from our own local cache dir, never user input.
            posix_path = path.as_posix().replace("'", "''")
            con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{posix_path}')")
    return con
