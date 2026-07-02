"""Company read repository — descriptive facts + latest-FY metrics for a ticker."""

from __future__ import annotations

from typing import Any

from infrastructure.storage import meta as load_meta

from .base import DuckDBRepository
from .dtos import CompanySummary, MetricPoint

# Latest available value per metric (the most recent FY is often a partial/TTM year that only
# carries intrinsic-value metrics, so per-metric — not single-year — gives the full picture).
# The QUALIFY picks each metric's newest FY inside DuckDB; ordered by the hierarchy's
# sort_order so categories come out as contiguous blocks the template can regroup.
_LATEST_METRICS_SQL = """
    SELECT ticker, metric, unit, fiscal_year, value, category, subcategory, sort_order
    FROM metrics
    WHERE ticker = ?
      AND period_type = 'FY'
      AND category IS NOT NULL
    QUALIFY row_number() OVER (PARTITION BY metric ORDER BY fiscal_year DESC) = 1
    ORDER BY sort_order, metric
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

    def latest_metrics(self, ticker: str, *, limit: int = 400) -> tuple[MetricPoint, ...]:
        """The latest available value of each derived metric, ordered for grouped display."""
        return self._fetch(_LATEST_METRICS_SQL, [ticker, limit], MetricPoint)
