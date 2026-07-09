"""Valuation views — a ticker's Margin-of-Safety metrics.

``valuation_page`` renders the HTML page at ``/valuation/<ticker>/``; ``valuation_data``
returns the JSON read model at ``/valuation/<ticker>/data/``. Both go through
``services.get_margin_of_safety`` (values precomputed by ``fundamentals_pipeline``).
"""

from __future__ import annotations

from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render

from apps.companies import services as company_services
from apps.companies.currency import quote_currency

from . import services
from .football import build_chart


def valuation_page(request: HttpRequest, ticker: str) -> HttpResponse:
    """Server-rendered valuation page: intrinsic-value football field + MoS table."""
    ticker = ticker.upper()
    points = services.get_margin_of_safety(ticker)
    scenarios = services.get_margin_of_safety_scenarios(ticker)
    chart = build_chart(services.get_intrinsic_value_field(ticker))
    summary = company_services.get_company_summary(ticker)
    price_currency = quote_currency(summary.market if summary else None).lower()
    return render(
        request,
        "valuation/detail.html",
        {
            "ticker": ticker,
            "points": points,
            "scenarios": scenarios,
            "chart": chart,
            "price_currency": price_currency,
        },
    )


def valuation_data(request: HttpRequest, ticker: str) -> JsonResponse:
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
