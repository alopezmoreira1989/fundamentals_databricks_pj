"""Companies views — HTTP only. Validate input, call the service, render/serialize DTOs.

Two representations of the same read model (both go through ``services.get_company_detail``):
``company_page`` renders the human HTML page (canonical ``/companies/<ticker>/``), and
``company_data`` returns the machine-readable JSON at ``/companies/<ticker>/data/``.
"""

from __future__ import annotations

from django.http import Http404, HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render

from apps.favorites import services as favorite_services
from apps.history import services as history_services
from apps.watchlists import services as watchlist_services

from . import services


def company_page(request: HttpRequest, ticker: str) -> HttpResponse:
    """Server-rendered company detail page (summary + latest-FY metrics).

    Visiting a valid company page also records it in the (login-required) browsing history.
    """
    ticker = ticker.upper()
    detail = services.get_company_detail(ticker)
    if detail is None:
        raise Http404(f"unknown ticker {ticker!r}")
    in_favorites = False
    watchlists: list = []
    ticker_watchlist_ids: set = set()
    if request.user.is_authenticated:
        watchlists = watchlist_services.list_watchlists(request.user)
        ticker_watchlist_ids = watchlist_services.lists_for_ticker(request.user, ticker)
        in_favorites = favorite_services.contains(request.user, ticker)
        history_services.record(request.user, ticker)  # side effect: mark as recently viewed
    return render(
        request,
        "companies/detail.html",
        {
            "detail": detail,
            "watchlists": watchlists,
            "ticker_watchlist_ids": ticker_watchlist_ids,
            "in_favorites": in_favorites,
        },
    )


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
