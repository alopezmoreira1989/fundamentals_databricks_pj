"""Screener views — parse/validate the query string, call the service, render/serialize.

``screen_page`` renders the HTML screener at ``/screener/``: a paginated table of the whole
company universe that the filters (text search, sector, index membership, and an optional
metric min/max) narrow. ``screen_data`` returns the single-metric JSON read model at
``/screener/data/``. Both share the parsing helpers below.
"""

from __future__ import annotations

import math
from urllib.parse import urlencode

from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render

from . import services

PAGE_SIZE = 50


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


def _parse_page(raw: str | None) -> int:
    try:
        return max(1, int(raw)) if raw else 1
    except ValueError:
        return 1


def screen_page(request: HttpRequest) -> HttpResponse:
    """HTML screener: the full company table (GET, state-in-URL) narrowed by the filters.

    With no filters it shows page 1 of every company. An unparseable metric bound shows an
    inline error rather than failing the request; the table still renders (bounds ignored).
    """
    search = request.GET.get("q", "").strip()
    sector = request.GET.get("sector", "").strip()
    index = request.GET.get("index", "").strip()
    metric = request.GET.get("metric", "").strip()
    raw_min, raw_max = request.GET.get("min", ""), request.GET.get("max", "")
    page = _parse_page(request.GET.get("page"))

    min_value, ok_min = _parse_optional_float(raw_min)
    max_value, ok_max = _parse_optional_float(raw_max)
    error = None if (ok_min and ok_max) else "'min' and 'max' must be numbers."

    result = services.list_companies(
        search=search,
        sector=sector,
        index=index,
        metric=metric,
        min_value=min_value if not error else None,
        max_value=max_value if not error else None,
        page=page,
        page_size=PAGE_SIZE,
    )

    num_pages = max(1, math.ceil(result.total / PAGE_SIZE))
    page = min(page, num_pages)
    # Query string carrying every filter except `page`, so pagination links preserve state.
    base_params = {
        k: v
        for k, v in {
            "q": search, "sector": sector, "index": index,
            "metric": metric, "min": raw_min, "max": raw_max,
        }.items()
        if v
    }
    return render(
        request,
        "screener/index.html",
        {
            "metrics": services.available_metrics(),
            "sectors": services.available_sectors(),
            "q": search,
            "sector": sector,
            "index": index,
            "selected": metric,
            "min": raw_min,
            "max": raw_max,
            "rows": result.rows,
            "total": result.total,
            "error": error,
            "page": page,
            "num_pages": num_pages,
            "has_prev": page > 1,
            "has_next": page < num_pages,
            "page_range": range(max(1, page - 2), min(num_pages, page + 2) + 1),
            "querystring": urlencode(base_params),
        },
    )


def screen_data(request: HttpRequest) -> JsonResponse:
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
