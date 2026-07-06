"""Read-only DRF viewsets over the read-model services.

Thin HTTP surface: validate query params, call the existing application services, serialize the
returned DTOs. No SQL and no financial logic here — the services (→ repositories → DuckDB /
fundamentals_pipeline) own all of that. Bad params raise DRF exceptions so every error goes out
through the one envelope (``apps.api.exceptions``); responses mirror ``repositories.dtos``.

Each action is annotated with ``@extend_schema`` so drf-spectacular emits an accurate OpenAPI 3
schema (query params, response envelopes, examples). The committed ``web/api-schema.yml`` is
drift-checked in the test suite.
"""

from __future__ import annotations

from drf_spectacular.types import OpenApiTypes
from drf_spectacular.utils import OpenApiExample, OpenApiParameter, extend_schema
from rest_framework import viewsets
from rest_framework.exceptions import NotFound, ValidationError
from rest_framework.request import Request
from rest_framework.response import Response

from apps.companies import services as company_services
from apps.screener import services as screener_services
from apps.valuation import services as valuation_services

from .serializers import (
    CompanyDetailSerializer,
    CompanyListResponseSerializer,
    CompanyListRowSerializer,
    ErrorEnvelopeSerializer,
    FootballFieldSerializer,
    MetricPointSerializer,
    ScreenResponseSerializer,
    ScreenRowSerializer,
    ValuationResponseSerializer,
)

# Errors everywhere share the one envelope; reuse the same 400/404 responses across actions.
_ERRORS = {400: ErrorEnvelopeSerializer, 404: ErrorEnvelopeSerializer}


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


def _q(name: str, description: str, *, required: bool = False) -> OpenApiParameter:
    return OpenApiParameter(name, OpenApiTypes.STR, OpenApiParameter.QUERY, description=description, required=required)


class CompanyViewSet(viewsets.ViewSet):
    """``GET /api/v1/companies/`` — the paginated company table (optional filters narrow it);
    ``GET /api/v1/companies/<ticker>/`` — a company's summary + latest-FY metrics."""

    lookup_value_regex = "[^/]+"  # tickers carry '.'/'-' (e.g. BRK-B), not just word chars

    @extend_schema(
        summary="List companies (paginated table)",
        parameters=[
            _q("q", "Case-insensitive substring match on ticker or company name."),
            _q("sector", "Exact sector filter."),
            _q("index", "Index membership filter: 'sp500' or 'r3000'."),
            _q("country", "Exact country filter."),
            _q("metric", "Add this metric's latest-FY value column and order rows by it (desc)."),
            _q("min", "Inclusive lower bound on the metric value (requires 'metric')."),
            _q("max", "Inclusive upper bound on the metric value (requires 'metric')."),
            OpenApiParameter("page", OpenApiTypes.INT, description="1-based page number (default 1)."),
            OpenApiParameter("page_size", OpenApiTypes.INT, description="Rows per page, 1–200 (default 50)."),
        ],
        responses={200: CompanyListResponseSerializer, **_ERRORS},
        examples=[
            OpenApiExample(
                "Page of the universe",
                value={
                    "count": 2987,
                    "page": 1,
                    "page_size": 2,
                    "results": [
                        {"ticker": "A", "name": "Agilent Technologies", "sector": "Healthcare",
                         "industry": "Diagnostics & Research", "metric_value": None, "fiscal_year": None},
                        {"ticker": "AAPL", "name": "Apple Inc.", "sector": "Technology",
                         "industry": "Consumer Electronics", "metric_value": None, "fiscal_year": None},
                    ],
                },
                response_only=True,
            )
        ],
    )
    def list(self, request: Request, version: str | None = None) -> Response:
        params = request.query_params
        page = _int_param(params.get("page"), name="page", default=1, lo=1, hi=1_000_000)
        page_size = _int_param(params.get("page_size"), name="page_size", default=50, lo=1, hi=200)
        result = screener_services.list_companies(
            search=params.get("q", "").strip(),
            sector=params.get("sector", "").strip(),
            index=params.get("index", "").strip(),
            country=params.get("country", "").strip(),
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

    @extend_schema(
        summary="Retrieve a company (summary + latest-FY metrics)",
        responses={200: CompanyDetailSerializer, **_ERRORS},
        examples=[
            OpenApiExample(
                "AAPL",
                value={
                    "summary": {"ticker": "AAPL", "name": "Apple Inc.", "sector": "Technology",
                                "industry": "Consumer Electronics", "exchange": "NMS", "country": "United States"},
                    "metrics": [
                        {"ticker": "AAPL", "metric": "Revenue", "unit": "USD", "fiscal_year": 2024,
                         "value": 391035000000.0, "category": "Income Statement",
                         "subcategory": None, "sort_order": 1.0}
                    ],
                },
                response_only=True,
            )
        ],
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

    @extend_schema(
        summary="Screen on a single metric",
        parameters=[
            _q("metric", "Metric name to screen on.", required=True),
            _q("min", "Inclusive lower bound on the metric value."),
            _q("max", "Inclusive upper bound on the metric value."),
            OpenApiParameter("limit", OpenApiTypes.INT, description="Max rows, 1–200 (default 50)."),
        ],
        responses={200: ScreenResponseSerializer, **_ERRORS},
        examples=[
            OpenApiExample(
                "Top ROE",
                value={
                    "metric": "Return on Equity %",
                    "count": 2,
                    "results": [
                        {"ticker": "AAPL", "fiscal_year": 2024, "value": 164.6},
                        {"ticker": "MCD", "fiscal_year": 2024, "value": 90.1},
                    ],
                },
                response_only=True,
            )
        ],
    )
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

    @extend_schema(
        summary="Retrieve valuation (Margin of Safety + football field)",
        responses={200: ValuationResponseSerializer, **_ERRORS},
        examples=[
            OpenApiExample(
                "AAPL",
                value={
                    "ticker": "AAPL",
                    "margin_of_safety": [
                        {"ticker": "AAPL", "metric": "MoS Graham (TTM)", "unit": "%", "fiscal_year": 2024,
                         "value": -35.2, "category": None, "subcategory": None, "sort_order": None}
                    ],
                    "football_field": {
                        "bars": [
                            {"method": "DCF (TTM)", "bear": 120.0, "mid": 165.0, "bull": 210.0, "fiscal_year": 2024}
                        ],
                        "price": 229.0,
                    },
                },
                response_only=True,
            )
        ],
    )
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
