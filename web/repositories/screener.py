"""Screener read repository — filter tickers by their latest-FY value of one metric."""

from __future__ import annotations

from typing import Any

from .base import DuckDBRepository
from .dtos import ScreenRow

# Each ticker's latest FY row for the metric (QUALIFY picks it inside DuckDB); the min/max
# bounds are applied *after* that pick, so a ticker is judged on its most recent value only.
_BASE_SQL = """
    WITH latest AS (
        SELECT ticker, fiscal_year, value
        FROM metrics
        WHERE metric = ? AND period_type = 'FY' AND value IS NOT NULL
        QUALIFY row_number() OVER (PARTITION BY ticker ORDER BY fiscal_year DESC) = 1
    )
    SELECT ticker, fiscal_year, value
    FROM latest
    WHERE TRUE
"""

# Distinct FY metric names, for the screener's metric picker.
_METRICS_SQL = "SELECT DISTINCT metric FROM metrics WHERE period_type = 'FY' ORDER BY metric"


class ScreenerRepository(DuckDBRepository):
    def available_metrics(self) -> tuple[str, ...]:
        """Every metric name that can be screened (distinct, alphabetical)."""
        return self._fetch_column(_METRICS_SQL)

    def screen(
        self,
        *,
        metric: str,
        min_value: float | None = None,
        max_value: float | None = None,
        limit: int = 50,
    ) -> tuple[ScreenRow, ...]:
        """Tickers whose latest-FY ``metric`` falls within ``[min_value, max_value]``.

        Bounds are optional and inclusive. Results are ordered by value (descending) and
        capped at ``limit`` — all filtering/ordering/limiting happens in DuckDB.
        """
        sql = _BASE_SQL
        params: list[Any] = [metric]
        if min_value is not None:
            sql += " AND value >= ?"
            params.append(min_value)
        if max_value is not None:
            sql += " AND value <= ?"
            params.append(max_value)
        sql += " ORDER BY value DESC LIMIT ?"
        params.append(limit)
        return self._fetch(sql, params, ScreenRow)
