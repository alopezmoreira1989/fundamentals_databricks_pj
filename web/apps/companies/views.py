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
from apps.valuation import services as valuation_services
from apps.valuation.football import build_chart
from apps.watchlists import services as watchlist_services

from . import charts, pricechart, services
from .currency import quote_currency


def company_page(request: HttpRequest, ticker: str) -> HttpResponse:
    """Server-rendered company detail page (summary + latest-FY metrics).

    Visiting a valid company page also records it in the (login-required) browsing history.
    """
    ticker = ticker.upper()
    detail = services.get_company_detail(ticker)
    if detail is None:
        raise Http404(f"unknown ticker {ticker!r}")
    statements = services.get_company_statements(ticker)
    headline = services.headline_kpis(statements)
    price_chart = pricechart.build_price_chart(services.get_price_series(ticker))
    quarterly = services.get_quarterly(ticker)
    # Income/Cash-flow get a headline bar chart; the Balance Sheet gets a single-year
    # composition (rendered below), so it's excluded from the per-statement bar-chart map.
    _chart_for = {
        "Income Statement": charts.income_statement_chart,
        "Cash Flow": charts.cash_flow_chart,
    }
    statement_panes = [
        (st, _chart_for[st.name](st) if st.name in _chart_for else None)
        for st in statements.statements
    ]
    balance_sheet = next((s for s in statements.statements if s.name == "Balance Sheet"), None)
    bs_compositions = charts.balance_sheet_compositions(balance_sheet) if balance_sheet else ()
    quarterly_chart = charts.quarterly_chart(quarterly) if quarterly.lines else None
    # Valuation tab: intrinsic-value football field + MoS + price multiples. Intrinsic-value
    # metrics are dropped from the derived-metrics list to avoid duplicating the football field.
    derived_metrics, valuation_metrics = services.split_metrics(detail.metrics)
    iv_chart = build_chart(valuation_services.get_intrinsic_value_field(ticker))
    mos_scenarios = valuation_services.get_margin_of_safety_scenarios(ticker)
    price_currency = quote_currency(detail.summary.market).lower()
    # Only offer the toggle for a ticker that actually has something non-USD to convert —
    # mirrors the Streamlit app's same conditional (no point showing it for a USD-only name).
    reporting_currency = (detail.summary.reporting_currency or "USD").upper()
    show_usd_toggle = price_currency != "usd" or reporting_currency != "USD"
    usd_lens = show_usd_toggle and request.GET.get("usd") == "1"
    market_cap_kpi = services.get_market_cap_kpi(ticker, usd_lens=usd_lens)
    headline = (*headline, market_cap_kpi) if market_cap_kpi else headline
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
            "statements": statements.statements,
            "statement_panes": statement_panes,
            "headline": headline,
            "price_chart": price_chart,
            "price_currency": price_currency,
            "show_usd_toggle": show_usd_toggle,
            "usd_lens": usd_lens,
            "quarterly": quarterly,
            "quarterly_chart": quarterly_chart,
            "bs_compositions": bs_compositions,
            "derived_metrics": derived_metrics,
            "valuation_metrics": valuation_metrics,
            "iv_chart": iv_chart,
            "mos_scenarios": mos_scenarios,
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


def company_news(request: HttpRequest, ticker: str) -> JsonResponse:
    """Latest Yahoo Finance headlines for the ticker (JSON, fetched async by the Overview tab)."""
    news = services.get_company_news(ticker.upper())
    return JsonResponse(
        {"news": [{"title": n.title, "link": n.link, "published": n.published} for n in news]}
    )
