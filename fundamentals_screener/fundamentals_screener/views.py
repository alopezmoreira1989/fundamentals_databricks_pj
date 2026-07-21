"""Views — HTTP only. Validate input, call ``services``, render/serialize DTOs.

Three pages: ``screen`` (the paginated, multi-metric, filterable company table, bookmarkable
— all state lives in the URL querystring), ``company_detail`` (financial statements, derived
metrics, valuation football field, price chart — one ticker), and ``valuation`` (a standalone
Margin-of-Safety + intrinsic-value page for one ticker). Each also has a JSON sibling.
``company_news`` is a small async JSON endpoint the Overview tab polls for the latest
Yahoo Finance headlines (see ``news.py`` — cached via Django's cache framework).

Not ported from the source project this package was extracted from: the favorites/watchlist/
history personalization ``company_page`` had — it depends on login-scoped apps this package
doesn't assume the host project has. See the package README for the full list of what v1
does and doesn't cover.
"""

from __future__ import annotations

import math
from urllib.parse import urlencode

from django.http import Http404, HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render

from . import football, pricechart, services
from .charts import (
    balance_sheet_compositions,
    cash_flow_chart,
    income_statement_chart,
    price_line_chart,
    quarterly_chart,
)
from .currency import quote_currency
from .repositories.company_listing import MetricFilter, SortSpec

PAGE_SIZE = 50

# Descriptive (non-metric) columns that can be sorted on, as (sort key, header label). The
# sort keys match what CompanyListingRepository whitelists for the scope table.
_DESC_COLUMNS = (("ticker", "Ticker"), ("name", "Company"), ("sector", "Sector"),
                 ("industry", "Industry"), ("country", "Country"), ("market", "Market"))
_SORT_KEYS_DESC = frozenset(k for k, _ in _DESC_COLUMNS)

# Ticker/Company always show; these four are independently toggleable in the table (default:
# all shown, matching the table's original always-on behaviour, so old bookmarked URLs render
# unchanged). `desc_on` is a hidden marker submitted alongside `desc` — its presence is what
# distinguishes "user unchecked every optional column" (desc_on present, desc absent) from "URL
# never mentioned column visibility at all" (both absent), which plain GET checkbox semantics
# can't otherwise tell apart.
_OPTIONAL_DESC_COLUMNS = (("sector", "Sector"), ("industry", "Industry"),
                          ("country", "Country"), ("market", "Market"))


# ── screener ─────────────────────────────────────────────────────────────────────────────
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


def _num(value: float) -> str:
    """Render a float bound back into the URL without a trailing ``.0`` for whole numbers."""
    return str(int(value)) if value == int(value) else str(value)


def screen(request: HttpRequest) -> HttpResponse:
    """HTML screener: the full company table narrowed by the descriptive filters, with the
    user-chosen metric columns and metric filters, every column sortable, state in the URL.
    """
    search = request.GET.get("q", "").strip()
    sector = request.GET.get("sector", "").strip()
    index = request.GET.get("index", "").strip()
    country = request.GET.get("country", "").strip()
    market = request.GET.get("market", "").strip()
    industry = request.GET.get("industry", "").strip()
    # Scoped to the active sector — Yahoo's ~145-value industry taxonomy is too large to show
    # unscoped.
    industries = services.available_industries(sector=sector)
    if industry not in industries:
        industry = ""
    # Only meaningful once the universe actually has non-US tickers; otherwise there's nothing
    # for it to convert, so it isn't offered.
    markets = services.available_markets()
    show_usd_toggle = "CA" in markets
    usd_lens = show_usd_toggle and request.GET.get("usd") == "1"
    page = _parse_page(request.GET.get("page"))

    desc_explicit = "desc_on" in request.GET
    if desc_explicit:
        # Reorder to the canonical order rather than trusting the querystring/form-submission
        # order — a hand-edited URL could list them in any order.
        requested_desc = frozenset(request.GET.getlist("desc"))
        visible_desc = [k for k, _ in _OPTIONAL_DESC_COLUMNS if k in requested_desc]
    else:
        visible_desc = [k for k, _ in _OPTIONAL_DESC_COLUMNS]

    # Selected display columns + metric filters, with the legacy single-metric URL folded in.
    cols = [c for c in (c.strip() for c in request.GET.getlist("col")) if c]
    filters, ok_filters = _parse_filters(request)
    legacy_cols, legacy_filters, ok_legacy = _legacy_single_metric(request)
    cols = list(dict.fromkeys([*cols, *legacy_cols]))
    filters = filters + legacy_filters
    error = None if (ok_filters and ok_legacy) else "Filter bounds must be numbers."
    if error:  # drop the unparseable bounds so the table still renders
        filters = [MetricFilter(metric=f.metric) for f in filters]
    has_active_filters = bool(filters)

    # Display every selected column plus any filtered metric (so the user sees what they bound
    # on), filters first-seen order preserved.
    display_cols = list(dict.fromkeys([*cols, *(f.metric for f in filters)]))

    sort_key = request.GET.get("sort", "ticker").strip() or "ticker"
    descending = request.GET.get("dir", "asc").strip().lower() == "desc"
    # Only honour a sort key that is a descriptive column or a shown metric; else fall back.
    if sort_key not in _SORT_KEYS_DESC and sort_key not in display_cols:
        sort_key, descending = "ticker", False

    result = services.screen_table(
        search=search, sector=sector, index=index, country=country, market=market,
        industry=industry, columns=display_cols, filters=filters,
        sort=SortSpec(key=sort_key, descending=descending),
        page=page, page_size=PAGE_SIZE, usd_lens=usd_lens,
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
    # Snapshot before `desc`/`desc_on` are appended — this is state the table-columns toggle
    # (a second, small GET form near the table, see the template) replicates as hidden fields,
    # since its own checkboxes supply desc/desc_on themselves; duplicating them would conflict.
    filter_pairs = list(base_pairs)
    if desc_explicit:
        base_pairs.append(("desc_on", "1"))
        for k in visible_desc:
            base_pairs.append(("desc", k))
    state_pairs = [*base_pairs, ("sort", sort_key), ("dir", "desc" if descending else "asc")]
    # The table-columns toggle form must also carry the active sort/page forward (they aren't
    # in filter_pairs — sort/dir/page are appended separately everywhere else in this view too).
    desc_form_hidden = [*filter_pairs, ("sort", sort_key), ("dir", "desc" if descending else "asc"),
                        ("page", str(page))]

    desc_headers = _sort_headers(
        [(k, label, None) for k, label in _DESC_COLUMNS if k in ("ticker", "name") or k in visible_desc],
        sort_key, descending, base_pairs,
    )
    metric_headers = _sort_headers(
        [(c.key, c.key, c.unit or "") for c in result.columns], sort_key, descending, base_pairs
    )
    # Rows as (row, aligned metric cells) so the template never indexes a mapping by key. Each
    # cell's unit comes from the ROW, not the column — Market Cap's unit is per-ticker (its own
    # native currency), so a column-wide unit would mislabel every non-USD ticker's cell.
    rows = [
        {
            "row": r,
            "desc_cells": [getattr(r, k) for k in visible_desc],
            "cells": [(r.values.get(c.key), r.units.get(c.key) or c.unit) for c in result.columns],
        }
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

    # Auto-apply filters (see screen.html's fetch() calls): an AJAX request only wants the
    # results fragment re-rendered, not the whole page with masthead/filter form again — same
    # context, same template code path either way, just a different entry point into it.
    is_fragment = request.headers.get("X-Requested-With") == "XMLHttpRequest"
    template = "fundamentals_screener/_screen_results.html" if is_fragment else "fundamentals_screener/screen.html"
    return render(
        request,
        template,
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
            "has_active_filters": has_active_filters,
            "active_filter_count": len(filters),
            "optional_desc_columns": _OPTIONAL_DESC_COLUMNS,
            "visible_desc": visible_desc,
            "desc_explicit": desc_explicit,
            "desc_form_hidden": desc_form_hidden,
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


def screen_data(request: HttpRequest) -> JsonResponse:
    metric = request.GET.get("metric", "").strip()
    if not metric:
        return JsonResponse({"error": "query parameter 'metric' is required"}, status=400)

    min_value, ok_min = _parse_optional_float(request.GET.get("min"))
    max_value, ok_max = _parse_optional_float(request.GET.get("max"))
    if not (ok_min and ok_max):
        return JsonResponse({"error": "'min' and 'max' must be numbers"}, status=400)

    limit = _parse_limit(request.GET.get("limit"))
    rows = services.run_screen(metric=metric, min_value=min_value, max_value=max_value, limit=limit)
    return JsonResponse({
        "metric": metric,
        "count": len(rows),
        "results": [{"ticker": r.ticker, "fiscal_year": r.fiscal_year, "value": r.value} for r in rows],
    })


# ── company detail ──────────────────────────────────────────────────────────────────────
def company_detail(request: HttpRequest, ticker: str) -> HttpResponse:
    """Server-rendered company detail page: overview KPIs, financial statements, derived
    metrics, valuation football field, price chart."""
    ticker = ticker.upper()
    detail = services.get_company_detail(ticker)
    if detail is None:
        raise Http404(f"unknown ticker {ticker!r}")
    statements = services.get_company_statements(ticker)
    headline = services.headline_kpis(statements)
    price_windows = services.price_windows()
    price_window = request.GET.get("window", "").strip()
    if price_window not in price_windows:
        price_window = services.PRICE_WINDOW_DEFAULT
    price_series = services.get_price_series(ticker, window=price_window)
    price_chart = pricechart.build_price_chart(price_series)
    price_tab_chart = price_line_chart(price_series)
    quarterly = services.get_quarterly(ticker)
    # Income/Cash-flow get a headline bar chart; the Balance Sheet gets a single-year
    # composition (rendered below), so it's excluded from the per-statement bar-chart map.
    _chart_for = {
        "Income Statement": income_statement_chart,
        "Cash Flow": cash_flow_chart,
    }
    statement_panes = [
        (st, _chart_for[st.name](st) if st.name in _chart_for else None)
        for st in statements.statements
    ]
    balance_sheet = next((s for s in statements.statements if s.name == "Balance Sheet"), None)
    bs_compositions = balance_sheet_compositions(balance_sheet) if balance_sheet else ()
    quarterly_chart_svg = quarterly_chart(quarterly) if quarterly.lines else None
    # Valuation section: intrinsic-value football field + MoS + price multiples. Intrinsic-
    # value metrics are dropped from the derived-metrics list to avoid duplicating the
    # football field.
    derived_metrics, valuation_metrics = services.split_metrics(detail.metrics)
    iv_chart = football.build_chart(services.get_intrinsic_value_field(ticker))
    mos_scenarios = services.get_margin_of_safety_scenarios(ticker)
    price_currency = quote_currency(detail.summary.market).lower()
    # Only offer the toggle for a ticker that actually has something non-USD to convert.
    reporting_currency = (detail.summary.reporting_currency or "USD").upper()
    show_usd_toggle = price_currency != "usd" or reporting_currency != "USD"
    usd_lens = show_usd_toggle and request.GET.get("usd") == "1"
    market_cap_kpi = services.get_market_cap_kpi(ticker, usd_lens=usd_lens)
    headline = (*headline, market_cap_kpi) if market_cap_kpi else headline
    return render(
        request,
        "fundamentals_screener/company_detail.html",
        {
            "detail": detail,
            "statements": statements.statements,
            "statement_panes": statement_panes,
            "headline": headline,
            "price_chart": price_chart,
            "price_tab_chart": price_tab_chart,
            "price_windows": price_windows,
            "price_window": price_window,
            "price_currency": price_currency,
            "show_usd_toggle": show_usd_toggle,
            "usd_lens": usd_lens,
            "quarterly": quarterly,
            "quarterly_chart": quarterly_chart_svg,
            "bs_compositions": bs_compositions,
            "derived_metrics": derived_metrics,
            "valuation_metrics": valuation_metrics,
            "iv_chart": iv_chart,
            "mos_scenarios": mos_scenarios,
        },
    )


def company_news(request: HttpRequest, ticker: str) -> JsonResponse:
    """JSON endpoint the Overview tab's "Latest news" card polls for asynchronously."""
    news = services.get_company_news(ticker.upper())
    return JsonResponse({
        "news": [{"title": n.title, "link": n.link, "published": n.published} for n in news],
    })


def company_data(request: HttpRequest, ticker: str) -> JsonResponse:
    """JSON read model for the same company detail (API surface)."""
    detail = services.get_company_detail(ticker.upper())
    if detail is None:
        return JsonResponse({"error": f"unknown ticker {ticker.upper()!r}"}, status=404)
    return JsonResponse({
        "ticker": detail.summary.ticker,
        "name": detail.summary.name,
        "sector": detail.summary.sector,
        "industry": detail.summary.industry,
        "exchange": detail.summary.exchange,
        "country": detail.summary.country,
        "metrics": [
            {
                "category": m.category, "metric": m.metric, "unit": m.unit,
                "fiscal_year": m.fiscal_year, "value": m.value,
            }
            for m in detail.metrics
        ],
    })


# ── valuation ────────────────────────────────────────────────────────────────────────────
def valuation(request: HttpRequest, ticker: str) -> HttpResponse:
    """Server-rendered standalone valuation page: intrinsic-value football field + MoS table."""
    ticker = ticker.upper()
    points = services.get_margin_of_safety(ticker)
    scenarios = services.get_margin_of_safety_scenarios(ticker)
    chart = football.build_chart(services.get_intrinsic_value_field(ticker))
    summary = services.get_company_summary(ticker)
    price_currency = quote_currency(summary.market if summary else None).lower()
    return render(
        request,
        "fundamentals_screener/valuation.html",
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
    return JsonResponse({
        "ticker": ticker.upper(),
        "count": len(points),
        "margin_of_safety": [
            {"metric": m.metric, "unit": m.unit, "fiscal_year": m.fiscal_year, "value": m.value}
            for m in points
        ],
    })
