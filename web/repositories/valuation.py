"""Valuation read repository — a ticker's Margin-of-Safety metrics (latest FY per metric).

The valuation math (Graham / DCF / Owner-Earnings, and the MoS % derived from them) is owned
by ``fundamentals_pipeline`` and precomputed into the published metrics artifact. This
repository only *reads* those values — it never recomputes a valuation.
"""

from __future__ import annotations

from .base import DuckDBRepository
from .dtos import MetricPoint

# All "MoS % (...)" metrics for the ticker, latest FY per metric (picked inside DuckDB).
_MOS_SQL = """
    SELECT ticker, metric, unit, fiscal_year, value
    FROM metrics
    WHERE ticker = ?
      AND period_type = 'FY'
      AND metric LIKE 'MoS %'
    QUALIFY row_number() OVER (PARTITION BY metric ORDER BY fiscal_year DESC) = 1
    ORDER BY metric
    LIMIT ?
"""


class ValuationRepository(DuckDBRepository):
    def margin_of_safety(self, ticker: str, *, limit: int = 100) -> tuple[MetricPoint, ...]:
        """Latest Margin-of-Safety metrics for the ticker (empty if none/unknown)."""
        return self._fetch(_MOS_SQL, [ticker, limit], MetricPoint)
