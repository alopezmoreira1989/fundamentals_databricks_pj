"""Company read repository — descriptive facts + latest-FY metrics for a ticker."""

from __future__ import annotations

from typing import Any

from infrastructure.storage import meta as load_meta

from .base import DuckDBRepository
from .dtos import CompanySummary, MetricPoint

# Latest fiscal year for a ticker, evaluated inside DuckDB (correlated subquery) so we never
# pull more than the one year we return.
_LATEST_METRICS_SQL = """
    SELECT ticker, metric, unit, fiscal_year, value
    FROM metrics
    WHERE ticker = ?
      AND period_type = 'FY'
      AND fiscal_year = (
          SELECT max(fiscal_year) FROM metrics WHERE ticker = ? AND period_type = 'FY'
      )
    ORDER BY metric
    LIMIT ?
"""


class CompanyRepository(DuckDBRepository):
    def get_summary(self, ticker: str) -> CompanySummary | None:
        """Descriptive facts from the meta artifact (``None`` if the ticker is unknown)."""
        records: list[dict[str, Any]] = load_meta().get("tickers", [])
        for rec in records:
            if rec.get("ticker") == ticker:
                return CompanySummary(
                    ticker=rec["ticker"],
                    name=rec.get("company", ""),
                    sector=rec.get("sector"),
                    industry=rec.get("industry"),
                    exchange=rec.get("exchange"),
                    country=rec.get("country"),
                )
        return None

    def latest_metrics(self, ticker: str, *, limit: int = 200) -> tuple[MetricPoint, ...]:
        """The ticker's derived metrics for its most recent fiscal year."""
        return self._fetch(_LATEST_METRICS_SQL, [ticker, ticker, limit], MetricPoint)
