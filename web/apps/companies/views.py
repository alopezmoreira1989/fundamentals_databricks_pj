"""Companies views — HTTP only. Validate input, call the service, serialize DTOs to JSON."""

from __future__ import annotations

from django.http import HttpRequest, JsonResponse

from . import services


def company_detail(request: HttpRequest, ticker: str) -> JsonResponse:
    detail = services.get_company_detail(ticker.upper())
    if detail is None:
        return JsonResponse({"error": f"unknown ticker {ticker.upper()!r}"}, status=404)
    return JsonResponse(
        {
            "ticker": detail.summary.ticker,
            "name": detail.summary.name,
            "sector": detail.summary.sector,
            "industry": detail.summary.industry,
            "exchange": detail.summary.exchange,
            "country": detail.summary.country,
            "metrics": [
                {
                    "metric": m.metric,
                    "unit": m.unit,
                    "fiscal_year": m.fiscal_year,
                    "value": m.value,
                }
                for m in detail.metrics
            ],
        }
    )
