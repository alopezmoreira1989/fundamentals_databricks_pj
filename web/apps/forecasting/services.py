"""Forecasting application service — coordinates the forecast repository.

The forecast values are precomputed by ``fundamentals_pipeline`` upstream (issue #332's
``24__forecasting.py``), so this use case is a straight read; when a future use case needs
live forecast math (e.g. #334's PV-discounted forward multiples), it calls
``fundamentals_pipeline`` here — never the repository.
"""

from __future__ import annotations

from repositories.dtos import ForecastSeries
from repositories.forecasting import ForecastRepository


def get_forecast_series(ticker: str) -> tuple[ForecastSeries, ...]:
    """Every ``(metric, quantile level)`` forecast path for the ticker, ready to plot — empty
    if the ticker has no published forecast or the artifact is unavailable."""
    return ForecastRepository().forecast_series(ticker)
