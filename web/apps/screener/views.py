"""Screener view — parses/validates the query string, calls the service, returns JSON."""

from __future__ import annotations

from django.http import HttpRequest, JsonResponse

from . import services


def _parse_optional_float(raw: str | None) -> tuple[float | None, bool]:
    """(value, ok). Absent/empty → (None, True); unparseable → (None, False)."""
    if not raw:
        return None, True
    try:
        return float(raw), True
    except ValueError:
        return None, False


def _parse_limit(raw: str | None, *, default: int = 50, lo: int = 1, hi: int = 200) -> int:
    try:
        value = int(raw) if raw else default
    except ValueError:
        return default
    return max(lo, min(hi, value))


def screen(request: HttpRequest) -> JsonResponse:
    metric = request.GET.get("metric", "").strip()
    if not metric:
        return JsonResponse({"error": "query parameter 'metric' is required"}, status=400)

    min_value, ok_min = _parse_optional_float(request.GET.get("min"))
    max_value, ok_max = _parse_optional_float(request.GET.get("max"))
    if not (ok_min and ok_max):
        return JsonResponse({"error": "'min' and 'max' must be numbers"}, status=400)

    limit = _parse_limit(request.GET.get("limit"))
    rows = services.run_screen(
        metric=metric, min_value=min_value, max_value=max_value, limit=limit
    )
    return JsonResponse(
        {
            "metric": metric,
            "count": len(rows),
            "results": [
                {"ticker": r.ticker, "fiscal_year": r.fiscal_year, "value": r.value}
                for r in rows
            ],
        }
    )
