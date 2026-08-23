"""Forecasting read repository — 10-year cross-sectional ML scenario forecasts.

The forecast values (LightGBM quantile regression for years 1-5, blended toward each
scenario's DCF terminal-growth rate for years 6-10, plus PV-discounted forward P/E / FCF
Yield — see the pipeline's ``24__forecasting.py``, issues #332/#334) are precomputed by
``fundamentals_pipeline`` upstream and published as the ``dashboard_forecast`` artifact. This
repository only *reads* those values and pivots the flat rows into chart-ready series — it
never recomputes a forecast or a multiple.

Queries the ``dashboard_forecast`` view directly — this package's own DuckDB views are always
named after the artifact itself (see ``repository.py``'s ``connection()``), never a short
alias.
"""

from __future__ import annotations

import duckdb

from fundamentals_pipeline.forecasting import TARGET_METRICS

from ..dtos import ForecastPoint, ForecastSeries
from .base import DuckDBRepository

# The three raw target-metric rows (Revenue/Net Income/Free Cash Flow forecasts) — explicitly
# excludes the "forward_pe"/"forward_fcf_yield" rows the pipeline appends to the SAME table
# (issue #334), which must never leak into the fan-chart data.
_TARGET_METRIC_NAMES = tuple(TARGET_METRICS)  # keys are the metric column's own values
_FORECAST_SQL = f"""
    SELECT metric, horizon, quantile_level, forecast_value
    FROM dashboard_forecast
    WHERE ticker = ? AND metric IN ({",".join("?" for _ in _TARGET_METRIC_NAMES)})
    ORDER BY metric, quantile_level, horizon
"""

# Forward P/E / FCF Yield, mid ("Crab", quantile 0.50) scenario only — the mockup's table has
# no scenario selector (issue #336 must only consume what's already published, no new
# service/model complexity for a switchable scenario).
_FORWARD_MULTIPLES_SQL = """
    SELECT metric, horizon, quantile_level, forecast_value
    FROM dashboard_forecast
    WHERE ticker = ? AND metric IN ('forward_pe', 'forward_fcf_yield') AND quantile_level = 0.50
    ORDER BY metric, horizon
"""

# Display order: Revenue/Net Income/Free Cash Flow (TARGET_METRICS' own insertion order),
# each metric's quantiles ascending (Bear..Bull).
_METRIC_ORDER = {suffix: i for i, suffix in enumerate(TARGET_METRICS)}


class ForecastRepository(DuckDBRepository):
    def forecast_points(self, ticker: str) -> tuple[ForecastPoint, ...]:
        """Every raw Revenue/Net Income/Free Cash Flow forecast row for the ticker, or ``()``
        if the ``dashboard_forecast`` view is absent (the artifact is optional — degrades
        gracefully, same as ``resolve_currency_rates``/``get_filings`` elsewhere in this
        codebase) or the ticker has no published forecast.
        """
        try:
            return self._fetch(_FORECAST_SQL, [ticker, *_TARGET_METRIC_NAMES], ForecastPoint)
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

    def forward_multiples(self, ticker: str) -> tuple[ForecastPoint, ...]:
        """The mid-scenario forward P/E / FCF Yield rows for the ticker (``metric`` is
        ``"forward_pe"``/``"forward_fcf_yield"``), or ``()`` on a missing view/no data — same
        degrade convention as :meth:`forecast_points`."""
        try:
            return self._fetch(_FORWARD_MULTIPLES_SQL, [ticker], ForecastPoint)
        except duckdb.Error:
            return ()
