"""DuckDB connection + query execution over the cached Parquet artifacts (Phase 2).

Public API:
    ``connect()`` — in-memory connection with the ``financials``/``metrics``/``prices``/
                    ``backtest`` views registered over the cached parquet files.
    ``query(sql, params)`` — run read-only SQL, return rows as dicts.
"""

from .engine import VIEWS, connect, query

__all__ = ["VIEWS", "connect", "query"]
