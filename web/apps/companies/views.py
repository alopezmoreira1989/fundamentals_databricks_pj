"""Companies views — HTTP only. Validate input, call the service, render/serialize DTOs.

Two representations of the same read model (both go through ``services.get_company_detail``):
``company_page`` renders the human HTML page (canonical ``/companies/<ticker>/``), and
``company_data`` returns the machine-readable JSON at ``/companies/<ticker>/data/``.
"""

from __future__ import annotations

from django.http import Http404, HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render

from . import services


def company_page(request: HttpRequest, ticker: str) -> HttpResponse:
    """Server-rendered company detail page (summary + latest-FY metrics)."""
    detail = services.get_company_detail(ticker.upper())
    if detail is None:
        raise Http404(f"unknown ticker {ticker.upper()!r}")
    return render(request, "companies/detail.html", {"detail": detail})


def company_data(request: HttpRequest, ticker: str) -> JsonResponse:
    """JSON read model for the same company detail (API surface)."""
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
                    "category": m.category,
                    "metric": m.metric,
                    "unit": m.unit,
                    "fiscal_year": m.fiscal_year,
                    "value": m.value,
                }
                for m in detail.metrics
            ],
        }
    )
