"""Forecasting views — a ticker's 10-year cross-sectional ML scenario forecasts.

``forecast_data`` returns the JSON read model at ``/forecasting/<ticker>/data/``. The
server-rendered HTML page (``/forecasting/<ticker>/``, the fan chart + forward-multiples
table from ``docs/mockups/forecasting_tab.html``) is added in issue #336 alongside its
template — this view has nothing to render against yet (templates/CSS are explicitly out of
scope for issue #335).
"""

from __future__ import annotations

from django.http import HttpRequest, JsonResponse

from . import services


def forecast_data(request: HttpRequest, ticker: str) -> JsonResponse:
    series = services.get_forecast_series(ticker.upper())
    return JsonResponse(
        {
            "ticker": ticker.upper(),
            "series": [
                {
                    "metric": s.metric,
                    "quantile_level": s.quantile_level,
                    "horizons": list(s.horizons),
                    "values": list(s.values),
                }
                for s in series
            ],
        }
    )
