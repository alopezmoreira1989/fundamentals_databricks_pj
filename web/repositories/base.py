"""Base class for the read-only DuckDB-backed repositories.

Centralises the two things every analytical repository needs: the connection lifecycle and
the row → immutable-DTO mapping. Subclasses write parameterized, explicit-column SQL and map
each row straight into a frozen dataclass — callers never see a raw row, dict, or DataFrame.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from typing import Any, TypeVar

import duckdb
from infrastructure.duckdb import connect

T = TypeVar("T")


class DuckDBRepository:
    """Read-only repository over the artifact views registered by ``infrastructure.duckdb``.

    Pass a ``connection`` to reuse one across calls (tests, or a request-scoped connection);
    otherwise each query opens and closes its own. Never exposes the connection upward.
    """

    def __init__(self, connection: duckdb.DuckDBPyConnection | None = None) -> None:
        self._injected = connection

    @contextmanager
    def _connection(self) -> Iterator[duckdb.DuckDBPyConnection]:
        if self._injected is not None:
            yield self._injected
        else:
            con = connect()
            try:
                yield con
            finally:
                con.close()

    def _fetch(
        self,
        sql: str,
        params: Sequence[Any],
        factory: Callable[..., T],
    ) -> tuple[T, ...]:
        """Run a parameterized query and map each row into ``factory`` (a DTO).

        The SELECT column aliases must match ``factory``'s field names exactly. Rows are
        materialized once, straight into DTOs — no intermediate list-of-dicts is retained.
        """
        with self._connection() as con:
            cursor = con.execute(sql, list(params))
            columns = [d[0] for d in cursor.description]
            return tuple(
                factory(**dict(zip(columns, row, strict=True))) for row in cursor.fetchall()
            )
