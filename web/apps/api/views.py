"""Read-only DRF viewsets over the read-model services.

Thin HTTP surface: validate query params, call the existing application services, serialize the
returned DTOs. No SQL and no financial logic here — the services (→ repositories → DuckDB /
fundamentals_pipeline) own all of that. Bad params raise DRF exceptions so every error goes out
through the one envelope (``apps.api.exceptions``); responses mirror ``repositories.dtos``.
"""

from __future__ import annotations

from rest_framework import viewsets
from rest_framework.exceptions import NotFound, ValidationError
from rest_framework.request import Request
from rest_framework.response import Response

from apps.companies import services as company_services
from apps.screener import services as screener_services
from apps.valuation import services as valuation_services

from .serializers import (
    CompanyDetailSerializer,
    CompanyListRowSerializer,
    FootballFieldSerializer,
    MetricPointSerializer,
    ScreenRowSerializer,
)


def _float_param(raw: str | None, name: str) -> float | None:
    """Optional float query param; ``None`` when absent, 400 when present but unparseable."""
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        raise ValidationError(f"'{name}' must be a number.") from None


def _int_param(raw: str | None, *, name: str, default: int, lo: int, hi: int) -> int:
    """Bounded int query param: absent → ``default``, non-integer → 400, else clamped to [lo, hi]."""
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ValidationError(f"'{name}' must be an integer.") from None
    return max(lo, min(hi, value))


class CompanyViewSet(viewsets.ViewSet):
    """``GET /api/v1/companies/`` — the paginated company table (optional filters narrow it);
    ``GET /api/v1/companies/<ticker>/`` — a company's summary + latest-FY metrics."""

    lookup_value_regex = "[^/]+"  # tickers carry '.'/'-' (e.g. BRK-B), not just word chars

    def list(self, request: Request, version: str | None = None) -> Response:
        params = request.query_params
        page = _int_param(params.get("page"), name="page", default=1, lo=1, hi=1_000_000)
        page_size = _int_param(params.get("page_size"), name="page_size", default=50, lo=1, hi=200)
        result = screener_services.list_companies(
            search=params.get("q", "").strip(),
            sector=params.get("sector", "").strip(),
            index=params.get("index", "").strip(),
            metric=params.get("metric", "").strip(),
            min_value=_float_param(params.get("min"), "min"),
            max_value=_float_param(params.get("max"), "max"),
            page=page,
            page_size=page_size,
        )
        return Response(
            {
                "count": result.total,
                "page": page,
                "page_size": page_size,
                "results": CompanyListRowSerializer(result.rows, many=True).data,
            }
        )

    def retrieve(self, request: Request, pk: str | None = None, version: str | None = None) -> Response:
        ticker = (pk or "").upper()
        detail = company_services.get_company_detail(ticker)
        if detail is None:
            raise NotFound(f"unknown ticker {ticker!r}")
        return Response(CompanyDetailSerializer(detail).data)


class ScreenerViewSet(viewsets.ViewSet):
    """``GET /api/v1/screener/?metric=<name>&min=&max=&limit=`` — the single-metric screen,
    latest FY, ordered by value descending."""

    def list(self, request: Request, version: str | None = None) -> Response:
        params = request.query_params
        metric = params.get("metric", "").strip()
        if not metric:
            raise ValidationError("query parameter 'metric' is required.")
        rows = screener_services.run_screen(
            metric=metric,
            min_value=_float_param(params.get("min"), "min"),
            max_value=_float_param(params.get("max"), "max"),
            limit=_int_param(params.get("limit"), name="limit", default=50, lo=1, hi=200),
        )
        return Response(
            {
                "metric": metric,
                "count": len(rows),
                "results": ScreenRowSerializer(rows, many=True).data,
            }
        )


class ValuationViewSet(viewsets.ViewSet):
    """``GET /api/v1/valuation/<ticker>/`` — a ticker's Margin-of-Safety metrics plus the
    intrinsic-value football field (per-method TTM ranges + market price)."""

    lookup_value_regex = "[^/]+"

    def retrieve(self, request: Request, pk: str | None = None, version: str | None = None) -> Response:
        ticker = (pk or "").upper()
        points = valuation_services.get_margin_of_safety(ticker)
        field = valuation_services.get_intrinsic_value_field(ticker)
        if not points and not field.bars:
            raise NotFound(f"no valuation data for {ticker!r}")
        return Response(
            {
                "ticker": ticker,
                "margin_of_safety": MetricPointSerializer(points, many=True).data,
                "football_field": FootballFieldSerializer(field).data,
            }
        )
