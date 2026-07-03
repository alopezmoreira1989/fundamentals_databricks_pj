"""Read-only DRF viewsets over the read-model services.

Thin HTTP surface: parse/validate query params, call the existing application services, serialize
the returned DTOs. No SQL and no financial logic here — the services (→ repositories → DuckDB /
fundamentals_pipeline) own all of that. Mirrors the hand-rolled ``*_data`` JSON views, but as a
routed, serializer-backed API for a decoupled frontend or third parties.
"""

from __future__ import annotations

from rest_framework import status, viewsets
from rest_framework.exceptions import NotFound
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


def _parse_float(raw: str | None) -> tuple[float | None, bool]:
    """(value, ok). Absent/empty → (None, True); unparseable → (None, False)."""
    if not raw:
        return None, True
    try:
        return float(raw), True
    except ValueError:
        return None, False


def _parse_int(raw: str | None, *, default: int, lo: int, hi: int) -> int:
    """Clamped int parse; falls back to ``default`` on absent/unparseable input."""
    try:
        value = int(raw) if raw else default
    except (TypeError, ValueError):
        return default
    return max(lo, min(hi, value))


class CompanyViewSet(viewsets.ViewSet):
    """``GET /api/companies/`` — the paginated company table (optional filters narrow it);
    ``GET /api/companies/<ticker>/`` — a company's summary + latest-FY metrics."""

    lookup_value_regex = "[^/]+"  # tickers carry '.'/'-' (e.g. BRK-B), not just word chars

    def list(self, request: Request) -> Response:
        params = request.query_params
        min_value, ok_min = _parse_float(params.get("min"))
        max_value, ok_max = _parse_float(params.get("max"))
        if not (ok_min and ok_max):
            return Response(
                {"detail": "'min' and 'max' must be numbers."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        page = _parse_int(params.get("page"), default=1, lo=1, hi=1_000_000)
        page_size = _parse_int(params.get("page_size"), default=50, lo=1, hi=200)
        result = screener_services.list_companies(
            search=params.get("q", "").strip(),
            sector=params.get("sector", "").strip(),
            index=params.get("index", "").strip(),
            metric=params.get("metric", "").strip(),
            min_value=min_value,
            max_value=max_value,
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

    def retrieve(self, request: Request, pk: str | None = None) -> Response:
        ticker = (pk or "").upper()
        detail = company_services.get_company_detail(ticker)
        if detail is None:
            raise NotFound(f"unknown ticker {ticker!r}")
        return Response(CompanyDetailSerializer(detail).data)


class ScreenerViewSet(viewsets.ViewSet):
    """``GET /api/screener/?metric=<name>&min=&max=&limit=`` — the single-metric screen,
    latest FY, ordered by value descending."""

    def list(self, request: Request) -> Response:
        params = request.query_params
        metric = params.get("metric", "").strip()
        if not metric:
            return Response(
                {"detail": "query parameter 'metric' is required."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        min_value, ok_min = _parse_float(params.get("min"))
        max_value, ok_max = _parse_float(params.get("max"))
        if not (ok_min and ok_max):
            return Response(
                {"detail": "'min' and 'max' must be numbers."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        limit = _parse_int(params.get("limit"), default=50, lo=1, hi=200)
        rows = screener_services.run_screen(
            metric=metric, min_value=min_value, max_value=max_value, limit=limit
        )
        return Response(
            {
                "metric": metric,
                "count": len(rows),
                "results": ScreenRowSerializer(rows, many=True).data,
            }
        )


class ValuationViewSet(viewsets.ViewSet):
    """``GET /api/valuation/<ticker>/`` — a ticker's Margin-of-Safety metrics plus the
    intrinsic-value football field (per-method TTM ranges + market price)."""

    lookup_value_regex = "[^/]+"

    def retrieve(self, request: Request, pk: str | None = None) -> Response:
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
