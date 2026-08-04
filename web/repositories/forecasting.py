"""Forecasting read repository — 10-year cross-sectional ML scenario forecasts.

The forecast values (LightGBM quantile regression for years 1-5, blended toward each
scenario's DCF terminal-growth rate for years 6-10 — see issue #332's
``24__forecasting.py``) are precomputed by ``fundamentals_pipeline`` upstream and published
as the ``dashboard_forecast`` artifact. This repository only *reads* those values and pivots
the flat rows into chart-ready series — it never recomputes a forecast.
"""

from __future__ import annotations

import duckdb

from fundamentals_pipeline.forecasting import TARGET_METRICS

from .base import DuckDBRepository
from .dtos import ForecastPoint, ForecastSeries

# All forecast rows for one ticker (up to 3 metrics x 5 quantile levels x 10 horizons = 150
# rows) — one query, reshaped in Python, rather than one query per metric (which would open/
# close a fresh DuckDB connection per metric for no benefit).
_FORECAST_SQL = """
    SELECT metric, horizon, quantile_level, forecast_value
    FROM forecast
    WHERE ticker = ?
    ORDER BY metric, quantile_level, horizon
"""

# Display order: Revenue/Net Income/Free Cash Flow (TARGET_METRICS' own insertion order),
# each metric's quantiles ascending (Bear..Bull).
_METRIC_ORDER = {suffix: i for i, suffix in enumerate(TARGET_METRICS)}


class ForecastRepository(DuckDBRepository):
    def forecast_points(self, ticker: str) -> tuple[ForecastPoint, ...]:
        """Every raw forecast row for the ticker, or ``()`` if the ``forecast`` view is absent
        (the artifact is optional — degrades gracefully, same as ``usd_fx_rate``/
        ``get_filings`` elsewhere in this codebase) or the ticker has no published forecast.
        """
        try:
            return self._fetch(_FORECAST_SQL, [ticker], ForecastPoint)
        except duckdb.Error:
            return ()

    def forecast_series(self, ticker: str) -> tuple[ForecastSeries, ...]:
        """One :class:`ForecastSeries` per ``(metric, quantile level)`` actually present for
        this ticker (up to 15 — 3 metrics x 5 quantiles), each ordered by horizon and ready to
        plot as one line of the Forecasting fan chart. Purely a reshape of
        :meth:`forecast_points`; a ``(metric, quantile)`` combination with no rows at all is
        simply absent from the result — never a series of fabricated placeholders.
        """
        points = self.forecast_points(ticker)
        grouped: dict[tuple[str, float], list[ForecastPoint]] = {}
        for p in points:
            grouped.setdefault((p.metric, p.quantile_level), []).append(p)

        series = [
            ForecastSeries(
                metric=metric,
                quantile_level=level,
                horizons=tuple(p.horizon for p in sorted(rows, key=lambda p: p.horizon)),
                values=tuple(p.forecast_value for p in sorted(rows, key=lambda p: p.horizon)),
            )
            for (metric, level), rows in grouped.items()
        ]
        return tuple(sorted(series, key=lambda s: (_METRIC_ORDER.get(s.metric, 99), s.quantile_level)))
