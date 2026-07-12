"""Screener views — parse/validate the query string, call the service, render/serialize.

``screen_page`` renders the HTML screener at ``/screener/``: a paginated, multi-metric table
of the whole company universe. The descriptive filters (text search, sector, index) narrow
the scope; the user picks any number of *metric columns* to display and any number of
*metric filters* (metric + min/max) to bound on; every column header sorts. All state lives
in the URL (GET), so every view is bookmarkable/shareable. ``screen_data`` returns the
single-metric JSON read model at ``/screener/data/``. Both share the parsing helpers below.
"""

from __future__ import annotations

import math
from urllib.parse import urlencode

from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render
from repositories.company_listing import MetricFilter, SortSpec

from . import services

PAGE_SIZE = 50

# Descriptive (non-metric) columns that can be sorted on, as (sort key, header label). The
# sort keys match what ``CompanyListingRepository`` whitelists for the scope table.
_DESC_COLUMNS = (("ticker", "Ticker"), ("name", "Company"), ("sector", "Sector"),
                 ("industry", "Industry"), ("country", "Country"), ("market", "Market"))
_SORT_KEYS_DESC = frozenset(k for k, _ in _DESC_COLUMNS)


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


def _parse_filters(request: HttpRequest) -> tuple[list[MetricFilter], bool]:
    """Metric filters from the parallel ``fmetric``/``fmin``/``fmax`` param lists.

    Rows with a blank metric are dropped; a row with an unparseable bound keeps the metric but
    drops that bound and flags the error. Returns ``(filters, all_bounds_ok)``.
    """
    metrics = request.GET.getlist("fmetric")
    mins = request.GET.getlist("fmin")
    maxs = request.GET.getlist("fmax")
    filters: list[MetricFilter] = []
    ok = True
    for i, metric in enumerate(metrics):
        metric = metric.strip()
        if not metric:
            continue
        lo, ok_lo = _parse_optional_float(mins[i] if i < len(mins) else "")
        hi, ok_hi = _parse_optional_float(maxs[i] if i < len(maxs) else "")
        ok = ok and ok_lo and ok_hi
        filters.append(MetricFilter(metric=metric, min_value=lo, max_value=hi))
    return filters, ok


def _legacy_single_metric(request: HttpRequest) -> tuple[list[str], list[MetricFilter], bool]:
    """Back-compat for the old single-metric URL (``metric``/``min``/``max``): map it to one
    display column plus, when a bound is given, one filter. Returns ``(cols, filters, ok)``."""
    metric = request.GET.get("metric", "").strip()
    if not metric:
        return [], [], True
    lo, ok_lo = _parse_optional_float(request.GET.get("min"))
    hi, ok_hi = _parse_optional_float(request.GET.get("max"))
    ok = ok_lo and ok_hi
    filters = [MetricFilter(metric=metric, min_value=lo, max_value=hi)] if (lo is not None or hi is not None) else []
    return [metric], filters, ok


def _sort_headers(
    keys: list[tuple[str, str, str | None]], sort_key: str, descending: bool, base_pairs: list[tuple[str, str]]
) -> list[dict[str, object]]:
    """Build header view-models with a toggle sort URL and the active-direction indicator.

    ``keys`` is ``(key, label, unit)`` (unit ``None`` for descriptive columns). ``base_pairs``
    carries every active filter except ``page``/``sort``/``dir`` so the links preserve state.
    """
    headers: list[dict[str, object]] = []
    for key, label, unit in keys:
        active = key == sort_key
        # A click toggles asc↔desc on the active column; a fresh column starts ascending.
        next_desc = active and not descending
        pairs = [*base_pairs, ("sort", key), ("dir", "desc" if next_desc else "asc")]
        headers.append({
            "key": key,
            "label": label,
            "unit": unit,
            "numeric": unit is not None,
            "sort_url": "?" + urlencode(pairs),
            "indicator": ("▼" if descending else "▲") if active else "",
        })
    return headers


def screen_page(request: HttpRequest) -> HttpResponse:
    """HTML screener: the full company table narrowed by the descriptive filters, with the
    user-chosen metric columns and metric filters, every column sortable, state in the URL.
    """
    search = request.GET.get("q", "").strip()
    sector = request.GET.get("sector", "").strip()
    index = request.GET.get("index", "").strip()
    country = request.GET.get("country", "").strip()
    market = request.GET.get("market", "").strip()
    industry = request.GET.get("industry", "").strip()
    # Scoped to the active sector (#231) — Yahoo's ~145-value industry taxonomy is too large to
    # show unscoped; mirrors the Streamlit screener's own sector-scoped industry picker.
    industries = services.available_industries(sector=sector)
    if industry not in industries:
        industry = ""
    # Only meaningful once the universe actually has non-US tickers; otherwise there's nothing
    # for it to convert, so it isn't offered (mirrors the Streamlit app's own condition).
    markets = services.available_markets()
    show_usd_toggle = "CA" in markets
    usd_lens = show_usd_toggle and request.GET.get("usd") == "1"
    page = _parse_page(request.GET.get("page"))

    # Selected display columns + metric filters, with the legacy single-metric URL folded in.
    cols = [c for c in (c.strip() for c in request.GET.getlist("col")) if c]
    filters, ok_filters = _parse_filters(request)
    legacy_cols, legacy_filters, ok_legacy = _legacy_single_metric(request)
    cols = list(dict.fromkeys([*cols, *legacy_cols]))
    filters = filters + legacy_filters
    error = None if (ok_filters and ok_legacy) else "Filter bounds must be numbers."
    if error:  # drop the unparseable bounds so the table still renders
        filters = [MetricFilter(metric=f.metric) for f in filters]

    # Display every selected column plus any filtered metric (so the user sees what they bound
    # on), filters first-seen order preserved.
    display_cols = list(dict.fromkeys([*cols, *(f.metric for f in filters)]))

    sort_key = request.GET.get("sort", "ticker").strip() or "ticker"
    descending = request.GET.get("dir", "asc").strip().lower() == "desc"
    # Only honour a sort key that is a descriptive column or a shown metric; else fall back.
    if sort_key not in _SORT_KEYS_DESC and sort_key not in display_cols:
        sort_key, descending = "ticker", False

    result = services.screen_table(
        search=search,
        sector=sector,
        index=index,
        country=country,
        market=market,
        industry=industry,
        columns=display_cols,
        filters=filters,
        sort=SortSpec(key=sort_key, descending=descending),
        page=page,
        page_size=PAGE_SIZE,
        usd_lens=usd_lens,
    )

    num_pages = max(1, math.ceil(result.total / PAGE_SIZE))
    page = min(page, num_pages)

    # State-carrying param pairs. `base_pairs` (no page/sort/dir) drives the sort-header links;
    # `state_pairs` (adds the active sort) drives the pagination links. usd_lens rides along in
    # both so toggling it survives a sort/page click, same bookmarkable-URL contract as every
    # other filter here.
    base_pairs: list[tuple[str, str]] = []
    for k, v in (
        ("q", search), ("sector", sector), ("index", index), ("country", country),
        ("market", market), ("industry", industry),
    ):
        if v:
            base_pairs.append((k, v))
    for c in cols:
        base_pairs.append(("col", c))
    for f in filters:
        base_pairs.append(("fmetric", f.metric))
        base_pairs.append(("fmin", "" if f.min_value is None else _num(f.min_value)))
        base_pairs.append(("fmax", "" if f.max_value is None else _num(f.max_value)))
    if usd_lens:
        base_pairs.append(("usd", "1"))
    state_pairs = [*base_pairs, ("sort", sort_key), ("dir", "desc" if descending else "asc")]

    desc_headers = _sort_headers(
        [(k, label, None) for k, label in _DESC_COLUMNS], sort_key, descending, base_pairs
    )
    metric_headers = _sort_headers(
        [(c.key, c.key, c.unit or "") for c in result.columns], sort_key, descending, base_pairs
    )
    # Rows as (row, aligned metric cells) so the template never indexes a mapping by key. Each
    # cell's unit comes from the ROW, not the column — Market Cap's unit is per-ticker (its own
    # native currency), so a column-wide unit would mislabel every non-USD ticker's cell.
    rows = [
        {"row": r, "cells": [(r.values.get(c.key), r.units.get(c.key) or c.unit) for c in result.columns]}
        for r in result.rows
    ]

    # Editable filter rows, rebuilt from the RAW params so the user sees exactly what they
    # typed (bad bounds included), seeded from the legacy URL and padded to a few blank rows.
    raw_m, raw_lo, raw_hi = (
        request.GET.getlist("fmetric"), request.GET.getlist("fmin"), request.GET.getlist("fmax")
    )
    filter_rows = [
        {"metric": m.strip(), "min": raw_lo[i] if i < len(raw_lo) else "",
         "max": raw_hi[i] if i < len(raw_hi) else ""}
        for i, m in enumerate(raw_m) if m.strip()
    ]
    if not filter_rows and legacy_filters:
        lf = legacy_filters[0]
        filter_rows = [{"metric": lf.metric, "min": request.GET.get("min", ""),
                        "max": request.GET.get("max", "")}]
    while len(filter_rows) < 3:
        filter_rows.append({"metric": "", "min": "", "max": ""})

    return render(
        request,
        "screener/index.html",
        {
            "metrics": services.available_metrics(),
            "sectors": services.available_sectors(),
            "countries": services.available_countries(),
            "markets": markets,
            "industries": industries,
            "q": search,
            "sector": sector,
            "index": index,
            "country": country,
            "market": market,
            "industry": industry,
            "show_usd_toggle": show_usd_toggle,
            "usd_lens": usd_lens,
            "cols": cols,
            "sort_key": sort_key,
            "sort_dir": "desc" if descending else "asc",
            "filter_rows": filter_rows,
            "desc_headers": desc_headers,
            "metric_headers": metric_headers,
            "rows": rows,
            "total": result.total,
            "error": error,
            "page": page,
            "num_pages": num_pages,
            "has_prev": page > 1,
            "has_next": page < num_pages,
            "page_range": range(max(1, page - 2), min(num_pages, page + 2) + 1),
            "querystring": urlencode(state_pairs),
        },
    )


def _num(value: float) -> str:
    """Render a float bound back into the URL without a trailing ``.0`` for whole numbers."""
    return str(int(value)) if value == int(value) else str(value)


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
