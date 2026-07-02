"""Valuation read repository — Margin-of-Safety metrics and the intrinsic-value football field.

The valuation math (Graham / DCF / Owner-Earnings, MoS %, the bear/mid/bull scenarios) is
owned by ``fundamentals_pipeline`` and precomputed into the published metrics artifact. This
repository only *reads* those values and pivots the scenario rows into range DTOs — it never
recomputes a valuation.
"""

from __future__ import annotations

import duckdb

from .base import DuckDBRepository
from .dtos import FootballBar, FootballField, MetricPoint

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

# Per-share intrinsic-value estimates on the TTM basis (the current view), latest value per
# metric. Bear/mid/bull arrive as three separate rows ("<method> (TTM)" [± " — Bear/Bull"]);
# they're pivoted into one bar per method below. The TTM filter drops the stale FY-only Graham
# Number, and requiring a full triple drops the total-dollar "Owner Earnings (TTM)".
_IV_TTM_SQL = """
    SELECT metric, fiscal_year, value
    FROM metrics
    WHERE ticker = ?
      AND period_type = 'FY'
      AND category = 'Intrinsic Value'
      AND unit = 'usd'
      AND metric LIKE '%(TTM)%'
      AND value IS NOT NULL
    QUALIFY row_number() OVER (PARTITION BY metric ORDER BY fiscal_year DESC) = 1
"""

_LATEST_PRICE_SQL = "SELECT arg_max(close, date) AS price FROM prices WHERE ticker = ?"

_SCENARIO_SEP = " — "  # " — " (em dash) separates a method from its Bear/Bull scenario


class ValuationRepository(DuckDBRepository):
    def margin_of_safety(self, ticker: str, *, limit: int = 100) -> tuple[MetricPoint, ...]:
        """Latest Margin-of-Safety metrics for the ticker (empty if none/unknown)."""
        return self._fetch(_MOS_SQL, [ticker, limit], MetricPoint)

    def intrinsic_value_field(self, ticker: str) -> FootballField:
        """Per-method TTM IV ranges (bear/mid/bull) plus the latest market price."""
        rows = self._fetch(_IV_TTM_SQL, [ticker], _IvRow)
        return FootballField(bars=_pivot_bars(rows), price=self._latest_price(ticker))

    def _latest_price(self, ticker: str) -> float | None:
        """Most recent close for the ticker, or ``None`` if the prices artifact is absent."""
        try:
            result = self._fetch_column(_LATEST_PRICE_SQL, [ticker])
        except duckdb.Error:
            return None  # optional prices view not registered → no price line
        return result[0] if result and result[0] is not None else None


class _IvRow:
    """Lightweight row holder for the IV pivot (matches the SELECT column names)."""

    def __init__(self, metric: str, fiscal_year: int, value: float) -> None:
        self.metric = metric
        self.fiscal_year = fiscal_year
        self.value = value


def _pivot_bars(rows: tuple[_IvRow, ...]) -> tuple[FootballBar, ...]:
    """Collapse bear/mid/bull scenario rows into one :class:`FootballBar` per method."""
    values: dict[str, dict[str, float]] = {}
    fiscal_years: dict[str, int] = {}
    for row in rows:
        head, sep, tail = row.metric.rpartition(_SCENARIO_SEP)
        if sep and tail in ("Bear", "Bull"):
            method, scenario = head, tail.lower()
        else:
            method, scenario = row.metric, "mid"
        values.setdefault(method, {})[scenario] = row.value
        if scenario == "mid":
            fiscal_years[method] = row.fiscal_year

    bars = [
        FootballBar(
            method=method,
            bear=v["bear"],
            mid=v["mid"],
            bull=v["bull"],
            fiscal_year=fiscal_years[method],
        )
        for method, v in values.items()
        if {"bear", "mid", "bull"} <= v.keys() and method in fiscal_years
    ]
    # Highest mid first — most optimistic valuation on top.
    return tuple(sorted(bars, key=lambda b: b.mid, reverse=True))
