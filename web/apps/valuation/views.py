"""Valuation view — returns a ticker's Margin-of-Safety metrics as JSON."""

from __future__ import annotations

from django.http import HttpRequest, JsonResponse

from . import services


def valuation_detail(request: HttpRequest, ticker: str) -> JsonResponse:
    points = services.get_margin_of_safety(ticker.upper())
    return JsonResponse(
        {
            "ticker": ticker.upper(),
            "count": len(points),
            "margin_of_safety": [
                {
                    "metric": m.metric,
                    "unit": m.unit,
                    "fiscal_year": m.fiscal_year,
                    "value": m.value,
                }
                for m in points
            ],
        }
    )
