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
from urllib.parse import quote, urlencode

from django.http import Http404, HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render

from . import football, pricechart, services
from .charts import (
    TabChartData,
    balance_sheet_compositions,
    cash_flow_chart,
    income_statement_chart,
    price_chart_data,
    quarterly_chart,
)
from .currency import quote_currency
from .repositories.company_listing import MetricFilter, SortSpec

PAGE_SIZE = 50

# Valid ?bench= values for the Derived-metrics tab's benchmark switch. Anything else (missing,
# blank, garbage) falls back to the historic silent industry-then-sector auto-cascade.
_BENCH_MODES = ("industry", "sector", "compare")

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


_NET_NET_LEVELS = ("relaxed", "moderate", "strict")
_NET_NET_LEVEL_FIELD = {
    "relaxed": "ncav_per_share_relaxed",
    "moderate": "ncav_per_share_moderate",
    "strict": "ncav_per_share_strict",
}

# NCAV/Share Discount filter (issue #317): a small preset tier, not a freeform threshold input
# — mirrors the level pill's own "small discrete choice, not a text field" convention. "all" is
# the default/omittable value (no filter, ``None`` passed to the service); the other three map
# to ``services.get_net_net_screen``'s ``min_discount_pct``. Order matches the pill display
# order (loosest first), same convention as ``_NET_NET_LEVELS``/Investor Presets' own levels.
_NET_NET_DISCOUNT_OPTIONS: dict[str, float | None] = {"all": None, "0": 0.0, "15": 15.0, "33": 33.0}
# Display order + label for the Valuation page's Net-Net card (issue #262), which shows all
# three levels side by side rather than one user-selected level like the Net-Net Finder does.
_NET_NET_CARD_LEVELS = (("Relaxed", "relaxed"), ("Moderate", "moderate"), ("Strict", "strict"))


def _net_net_card_context(ticker: str) -> dict | None:
    """Shared by ``valuation()`` and ``company_detail()``: the Net-Net card's context, or
    ``None`` when the ticker has no NCAV data at any level at all (unknown ticker, or every
    level's NCAV/share is null) — nothing to show then, so the card doesn't render rather than
    showing three dashes. A NEGATIVE NCAV/share (common — most companies aren't net-nets) still
    renders, deliberately: the Valuation page always shows a company's own numbers, unlike the
    Net-Net Finder screener which only lists NCAV-positive eligible tickers. ``ratio``/
    ``bar_pct`` are left ``None``/0 for a non-positive NCAV/share, though — dividing price by a
    negative or zero NCAV produces a meaningless "ratio" (confirmed as a real bug during
    testing: a negative ratio like -34.4x satisfied the "classic net-net" bar-color threshold
    check, painting AAPL's deeply-negative NCAV bright green as if it were a bargain).

    ``snapshot.price`` comes from ``net_net_snapshot``'s own latest-close lookup — deliberately
    NOT the caller's football-field chart price (confirmed as a second real gap during testing:
    that price is unavailable for tickers lacking EPS/BVPS data even when a real close and real
    NCAV both exist, e.g. an unprofitable clinical-stage biotech).
    """
    snapshot = services.get_net_net_snapshot(ticker)
    if snapshot is None:
        return None
    price = snapshot.price
    levels = []
    for label, level in _NET_NET_CARD_LEVELS:
        ncav_per_share = getattr(snapshot, _NET_NET_LEVEL_FIELD[level])
        has_ratio = price is not None and ncav_per_share is not None and ncav_per_share > 0
        ratio = price / ncav_per_share if has_ratio else None
        discount_pct = (1 - ratio) * 100 if ratio is not None else None
        bar_pct = max(0.0, min(discount_pct, 100.0)) if discount_pct is not None else 0.0
        levels.append({"label": label, "ncav_per_share": ncav_per_share, "ratio": ratio,
                        "discount_pct": discount_pct, "bar_pct": bar_pct})
    if not any(lv["ncav_per_share"] is not None for lv in levels):
        return None
    return {"net_net": snapshot, "net_net_levels": levels}


def _render_mode(
    request: HttpRequest, *, page_title: str, fragment_template: str, main_template: str,
    full_template: str, context: dict,
) -> HttpResponse:
    """Renders one of three tiers for a screener mode view (``screen``/``_netnet_screen``/
    ``_presets_screen``), matching the request's AJAX intent:

    - a full page (real navigation — no special header);
    - the mode-switch fragment (``X-Mode-Switch: 1``, see screener.js's ``fetchAndSwap`` calls
      from ``_mode_nav.html``'s links) — mode-nav + the mode's whole content, swapped into
      ``#main`` when the user clicks a different tab. Carries an ``X-Page-Title`` response
      header (percent-encoded via ``urllib.parse.quote`` — Django would otherwise RFC 2047
      MIME-encode the raw em-dash into ``=?utf-8?q?...?=``, which ``fetch()``'s ``Headers.get``
      does not decode for you; the JS side undoes this with ``decodeURIComponent``) so the JS
      can sync the browser tab title, since nothing outside ``#main`` — including ``<title>``
      — is touched by an innerHTML swap otherwise;
    - the narrower within-mode fragment (``X-Requested-With: XMLHttpRequest``, unchanged since
      before mode-switching existed) — just the mode's own results/content, for an in-mode
      filter/sort/pagination/pill change.

    Checked in that priority order since a mode-switch request could in principle also carry
    the generic AJAX header; ``X-Mode-Switch`` is the more specific signal.
    """
    is_mode_switch = request.headers.get("X-Mode-Switch") == "1"
    is_fragment = request.headers.get("X-Requested-With") == "XMLHttpRequest"
    if is_mode_switch:
        template = main_template
    elif is_fragment:
        template = fragment_template
    else:
        template = full_template
    response = render(request, template, context)
    if is_mode_switch:
        response["X-Page-Title"] = quote(page_title)
    return response


def screen(request: HttpRequest) -> HttpResponse:
    """HTML screener — three modes sharing one route (``?mode=general|netnet|presets``,
    default ``general``). ``general`` is the full multi-metric company table (this
    function's original scope, below). ``netnet`` is the Net-Net Finder and ``presets`` is
    Investor Presets — both fixed-column screens delegated to their own ``_netnet_screen``/
    ``_presets_screen``, since their filters/columns/templates are almost entirely disjoint
    from the general screener's (only sector/country/market are shared — see issue #259).

    Every interaction on every mode is AJAX (screener.js): within-mode filter/sort/pagination/
    pill changes swap just that mode's own content container, and switching between modes via
    ``_mode_nav.html``'s links swaps the whole ``#main`` region — see ``_render_mode``'s
    docstring for the three response tiers each of the three view functions picks between.
    Every underlying ``<form method="get">``/``<a href>`` is still a real one, so all of this
    degrades to plain full-page navigations with JS disabled.
    """
    mode = request.GET.get("mode", "general").strip().lower()
    if mode == "netnet":
        return _netnet_screen(request)
    if mode == "presets":
        return _presets_screen(request)
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

    # Auto-apply filters / mode-switch (see screener.js's fetchAndSwap calls): see
    # _render_mode's own docstring for the three response tiers this picks between.
    return _render_mode(
        request,
        page_title="Screener — Fundamentals Screener",
        fragment_template="fundamentals_screener/_screen_results.html",
        main_template="fundamentals_screener/_screen_main.html",
        full_template="fundamentals_screener/screen.html",
        context={
            "mode": "general",
            # The mode-nav bar carries sector/index/country/market/industry across to Net-Net
            # Finder and Investor Presets (the descriptive filters all three modes share) —
            # precomputed here rather than in the template, matching this view's own
            # "querystring" key just below.
            "shared_qs": urlencode({
                k: v for k, v in (
                    ("sector", sector), ("index", index), ("country", country),
                    ("market", market), ("industry", industry),
                ) if v
            }),
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


# Net-Net Finder's table columns, (sort key, header label, unit-or-None) — unit drives the
# header's own right-alignment via `_sort_headers`' "numeric" flag; the None-unit columns
# (Ticker/Company/Sector/F-Score/Altman Zone) render left-aligned like every other descriptive
# column. Sorting itself happens in `services._sort_net_net_rows` (Python, not DuckDB) since
# `net_net_screen` already returns its whole filtered set unpaginated.
_NET_NET_HEADER_COLUMNS: list[tuple[str, str, str | None]] = [
    ("ticker", "Ticker", None),
    ("name", "Company", None),
    ("sector", "Sector", None),
    ("price", "Price", "usd"),
    ("ncav_per_share", "NCAV/Share", "usd"),
    ("discount", "Discount", "percent"),
    ("f_score", "F-Score", None),
    ("z_score", "Altman Zone", None),
    ("market_cap", "Market Cap", "usd"),
]


def _netnet_screen(request: HttpRequest) -> HttpResponse:
    """The Net-Net Finder: Graham-style deep-value screen at a user-chosen liquidation-
    conservatism level (see ``services.get_net_net_screen``), sharing the sector/index/country/
    market/industry descriptive filters with the general screener. Precomputes each row's
    price/NCAV-per-share ratio and a clamped 0-100 bar percentage here (not in the template) —
    the same "view builds a small per-row dict, template stays dumb" convention `screen()`
    itself uses for its own ``rows`` context key.

    Paginated in Python (the service has no LIMIT/OFFSET — it always returns the whole filtered
    set): confirmed by an actual smoke test that "Relaxed" alone (base NCAV, no haircut) matches
    ~1 in 3 tickers, not the "small fraction of the universe" a genuine net-net is — a plain
    positive-NCAV filter is a much weaker bar than "trading below value". Rendering ~1,000
    rows unpaginated produced a multi-megabyte response; slicing here (matching PAGE_SIZE) is
    cheap since the full set is already in memory. Column-header sorting (see
    ``_NET_NET_HEADER_COLUMNS``) reuses ``screen()``'s own ``_sort_headers`` helper, but the
    sort itself happens in the service (Python), not via a DuckDB ``ORDER BY``.
    """
    level = request.GET.get("level", "relaxed").strip().lower()
    if level not in _NET_NET_LEVELS:
        level = "relaxed"
    hide_value_traps = request.GET.get("hide_value_traps") == "1"
    discount = request.GET.get("discount", "all").strip().lower()
    if discount not in _NET_NET_DISCOUNT_OPTIONS:
        discount = "all"
    sector = request.GET.get("sector", "").strip()
    index = request.GET.get("index", "").strip()
    country = request.GET.get("country", "").strip()
    market = request.GET.get("market", "").strip()
    industry = request.GET.get("industry", "").strip()
    industries = services.available_industries(sector=sector)
    if industry not in industries:
        industry = ""
    sort_key = request.GET.get("sort", "discount").strip() or "discount"
    descending = request.GET.get("dir", "asc").strip().lower() == "desc"
    page = _parse_page(request.GET.get("page"))

    result = services.get_net_net_screen(
        level=level, hide_value_traps=hide_value_traps,
        min_discount_pct=_NET_NET_DISCOUNT_OPTIONS[discount],
        sector=sector, index=index, country=country, market=market, industry=industry,
        sort_key=sort_key, descending=descending,
    )
    total = len(result.rows)
    stats = result.stats
    num_pages = max(1, math.ceil(total / PAGE_SIZE))
    page = min(page, num_pages)
    offset = (page - 1) * PAGE_SIZE
    window = result.rows[offset : offset + PAGE_SIZE]

    field = _NET_NET_LEVEL_FIELD[level]
    rows = []
    for r in window:
        ncav_per_share = getattr(r, field)
        ratio = r.price / ncav_per_share if r.price is not None and ncav_per_share else None
        discount_pct = (1 - ratio) * 100 if ratio is not None else None
        bar_pct = max(0.0, min(discount_pct, 100.0)) if discount_pct is not None else 0.0
        rows.append({
            "row": r,
            "ncav_per_share": ncav_per_share,
            "ratio": ratio,
            "discount_pct": discount_pct,
            "bar_pct": bar_pct,
        })

    base_pairs: list[tuple[str, str]] = [
        p for p in (
            ("mode", "netnet"), ("level", level),
            ("hide_value_traps", "1" if hide_value_traps else ""),
            ("discount", discount if discount != "all" else ""),
            ("sector", sector), ("index", index), ("country", country), ("market", market),
            ("industry", industry),
        ) if p[1]
    ]
    headers = {
        h["key"]: h
        for h in _sort_headers(_NET_NET_HEADER_COLUMNS, sort_key, descending, base_pairs)
    }

    shared_qs = urlencode({
        k: v for k, v in (
            ("sector", sector), ("index", index), ("country", country), ("market", market),
            ("industry", industry),
        ) if v
    })
    # Pagination-link querystring: every current param except `page` (appended by the template,
    # matching _screen_results.html's own convention) so Prev/Next/page-N preserve state.
    nn_qs = urlencode([*base_pairs, ("sort", sort_key), ("dir", "desc" if descending else "asc")])

    # Auto-apply / mode-switch (see screener.js's fetchAndSwap calls): see _render_mode's own
    # docstring for the three response tiers this picks between.
    return _render_mode(
        request,
        page_title="Net-Net Finder — Fundamentals Screener",
        fragment_template="fundamentals_screener/_netnet_content.html",
        main_template="fundamentals_screener/_netnet_main.html",
        full_template="fundamentals_screener/netnet.html",
        context={
            "mode": "netnet",
            "shared_qs": shared_qs,
            "querystring": nn_qs,
            "level": level,
            "hide_value_traps": hide_value_traps,
            "discount": discount,
            "sector": sector,
            "index": index,
            "country": country,
            "market": market,
            "industry": industry,
            "sectors": services.available_sectors(),
            "countries": services.available_countries(),
            "markets": services.available_markets(),
            "industries": industries,
            "headers": headers,
            "rows": rows,
            "total": total,
            "stats": stats,
            "page": page,
            "num_pages": num_pages,
            "has_prev": page > 1,
            "has_next": page < num_pages,
            "page_range": range(max(1, page - 2), min(num_pages, page + 2) + 1),
        },
    )


def _presets_screen(request: HttpRequest) -> HttpResponse:
    """Investor Presets: a name-only pill selector (Graham/Buffett/Lynch) revealing that
    school's philosophy panel (portrait, tagline, criteria list) plus a company table filtered
    on that school's criteria (see ``services.get_preset_screen``) — sharing the sector/index/
    country/market/industry descriptive filters with the general screener, the same pattern as
    the Net-Net Finder. A second pill (``level`` — "strict"/"moderate"/"relaxed", mirroring the
    Net-Net Finder's own conservatism-level pill exactly) picks which threshold set the criteria
    actually use; the criteria list's label text is level-dependent too (see
    ``services.get_preset_definition``), not just the query bounds.

    Paginated AND sorted in DuckDB, unlike the Net-Net Finder's Python-side pagination/sort: a
    preset's matches aren't guaranteed to be a small fraction of the universe the way a genuine
    net-net is, so pushing LIMIT/OFFSET/ORDER BY into the query (``CompanyListingRepository.
    preset_screen``) avoids ever materializing an unbounded result set in Python. No sort-key
    whitelist needed here (unlike ``screen()``'s own): ``_order_clause`` already falls back
    safely to ``s.ticker`` for anything not a descriptive column or one of the preset's own
    display columns.
    """
    presets = services.preset_keys()
    preset = request.GET.get("preset", presets[0]).strip().lower()
    if preset not in presets:
        preset = presets[0]
    levels = services.preset_levels()
    level = request.GET.get("level", "strict").strip().lower()
    if level not in levels:
        level = "strict"
    sector = request.GET.get("sector", "").strip()
    index = request.GET.get("index", "").strip()
    country = request.GET.get("country", "").strip()
    market = request.GET.get("market", "").strip()
    industry = request.GET.get("industry", "").strip()
    industries = services.available_industries(sector=sector)
    if industry not in industries:
        industry = ""
    sort_key = request.GET.get("sort", "ticker").strip() or "ticker"
    descending = request.GET.get("dir", "asc").strip().lower() == "desc"
    page = _parse_page(request.GET.get("page"))

    result = services.get_preset_screen(
        preset, level=level, sector=sector, index=index, country=country, market=market,
        industry=industry, sort=SortSpec(key=sort_key, descending=descending), page=page,
        page_size=PAGE_SIZE,
    )
    num_pages = max(1, math.ceil(result.total / PAGE_SIZE))
    page = min(page, num_pages)
    definition = services.get_preset_definition(preset, level)

    # Rows as (row, aligned metric cells) so the template never indexes a mapping by key —
    # same convention screen_table's own `rows` context key uses.
    rows = [
        {
            "row": r,
            "cells": [(r.values.get(c.key), r.units.get(c.key) or c.unit) for c in result.columns],
        }
        for r in result.rows
    ]

    base_pairs: list[tuple[str, str]] = [
        p for p in (
            ("mode", "presets"), ("preset", preset), ("level", level),
            ("sector", sector), ("index", index), ("country", country), ("market", market),
            ("industry", industry),
        ) if p[1]
    ]
    headers = _sort_headers(
        [("ticker", "Ticker", None), ("name", "Company", None), ("sector", "Sector", None),
         *[(c.key, c.key, c.unit or "") for c in result.columns]],
        sort_key, descending, base_pairs,
    )

    shared_qs = urlencode({
        k: v for k, v in (
            ("sector", sector), ("index", index), ("country", country), ("market", market),
            ("industry", industry),
        ) if v
    })
    presets_qs = urlencode([*base_pairs, ("sort", sort_key), ("dir", "desc" if descending else "asc")])

    # Auto-apply / mode-switch (see screener.js's fetchAndSwap calls): see _render_mode's own
    # docstring for the three response tiers this picks between.
    return _render_mode(
        request,
        page_title="Investor Presets — Fundamentals Screener",
        fragment_template="fundamentals_screener/_presets_content.html",
        main_template="fundamentals_screener/_presets_main.html",
        full_template="fundamentals_screener/presets.html",
        context={
            "mode": "presets",
            "shared_qs": shared_qs,
            "querystring": presets_qs,
            "preset": preset,
            "presets": presets,
            "level": level,
            "levels": levels,
            "definition": definition,
            "portrait_path": f"fundamentals_screener/portraits/{preset}.png",
            "sector": sector,
            "index": index,
            "country": country,
            "market": market,
            "industry": industry,
            "sectors": services.available_sectors(),
            "countries": services.available_countries(),
            "markets": services.available_markets(),
            "industries": industries,
            "headers": headers,
            "columns": result.columns,
            "rows": rows,
            "total": result.total,
            "stats": result.stats,
            "page": page,
            "num_pages": num_pages,
            "has_prev": page > 1,
            "has_next": page < num_pages,
            "page_range": range(max(1, page - 2), min(num_pages, page + 2) + 1),
        },
    )


def about(request: HttpRequest) -> HttpResponse:
    """Static About page: value-investing framing + legal disclaimer. No dynamic context."""
    return render(request, "fundamentals_screener/about.html")


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
def _tab_chart_json(data: TabChartData) -> dict:
    """Plain-dict shape for a Chart.js bar/combo chart's ``json_script`` payload — one entry
    per :class:`~.charts.ChartSeries`, colors assigned client-side (see statement_charts.js)."""
    return {
        "labels": list(data.labels),
        "series": [{"name": s.name, "kind": s.kind, "values": list(s.values)} for s in data.series],
    }


def _price_chart_json(points: tuple) -> list[dict]:
    """Plain-dict shape for the Price tab's Lightweight Charts ``json_script`` payload."""
    return [
        {
            "date": p.date, "close": p.close, "adj_close": p.adj_close,
            "sma20": p.sma20, "sma50": p.sma50, "sma200": p.sma200,
        }
        for p in points
    ]


def _balance_sheet_json(compositions: tuple) -> list[dict]:
    """Plain-dict shape for the Balance Sheet composition's Chart.js ``json_script`` payload —
    every fiscal year embedded in one payload so the year selector can dataset-swap client-side
    (mirrors the previous ``bsShowYear()`` hide/show, which also embedded every year server-side)."""

    def stack_json(stack) -> dict:
        return {
            "total": stack.total,
            "segments": [
                {"name": s.name, "value": s.value, "pct": s.pct, "color": s.color, "boundary": s.boundary}
                for s in stack.segments
            ],
        }

    return [
        {"year": c.year, "assets": stack_json(c.assets), "liabilities_equity": stack_json(c.liabilities_equity)}
        for c in compositions
    ]


def company_detail(request: HttpRequest, ticker: str) -> HttpResponse:
    """Server-rendered company detail page: overview KPIs, financial statements, derived
    metrics, valuation football field, price chart."""
    ticker = ticker.upper()
    summary = services.get_company_summary(ticker)
    if summary is None:
        raise Http404(f"unknown ticker {ticker!r}")

    # Moved up from its old spot (right before market_cap_kpi) — only needs `summary`, so both
    # the fragment and full-page branches below can share it without needing statements/price
    # data first.
    price_currency = quote_currency(summary.market).lower()
    reporting_currency = (summary.reporting_currency or "USD").upper()
    show_usd_toggle = price_currency != "usd" or reporting_currency != "USD"
    usd_lens = show_usd_toggle and request.GET.get("usd") == "1"

    bench = request.GET.get("bench", "").strip().lower()
    if bench not in _BENCH_MODES:
        bench = ""
    compare = request.GET.get("compare", "").strip().upper()
    derived_metrics, bench_ctx = services.get_metric_history(ticker, years=5, bench=bench, compare=compare)

    # Benchmark-switch AJAX partial-swap (mirrors screen()'s is_fragment branch), scoped to just
    # the Derived-metrics tab. Deliberately does NOT mirror screen()'s "same context either way"
    # — unlike screen(), this view also computes statements/price/quarterly/valuation below, none
    # of which the fragment needs, so this returns before any of that work runs.
    is_fragment = request.headers.get("X-Requested-With") == "XMLHttpRequest"
    if is_fragment:
        return render(
            request,
            "fundamentals_screener/_derived_metrics.html",
            {
                "derived_metrics": derived_metrics,
                "bench_ctx": bench_ctx,
                "bench": bench,
                "compare_query": compare,
                "summary": summary,
                "usd_lens": usd_lens,
            },
        )

    detail = services.get_company_detail(ticker)
    if detail is None:  # defensive only — `summary` above already confirmed the ticker exists
        raise Http404(f"unknown ticker {ticker!r}")
    statements = services.get_company_statements(ticker)
    headline = services.headline_kpis(statements)
    price_windows = services.price_windows()
    price_window = request.GET.get("window", "").strip()
    if price_window not in price_windows:
        price_window = services.PRICE_WINDOW_DEFAULT
    price_series = services.get_price_series(ticker, window=price_window)
    price_chart = pricechart.build_price_chart(price_series)
    price_tab_points = price_chart_data(price_series)
    price_tab_data = _price_chart_json(price_tab_points) if price_tab_points else None
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
    statement_chart_data = {
        st.name: _tab_chart_json(chart) for st, chart in statement_panes if chart is not None
    }
    balance_sheet = next((s for s in statements.statements if s.name == "Balance Sheet"), None)
    bs_compositions = balance_sheet_compositions(balance_sheet) if balance_sheet else ()
    bs_compositions_data = _balance_sheet_json(bs_compositions) if bs_compositions else None
    quarterly_chart_obj = quarterly_chart(quarterly) if quarterly.lines else None
    quarterly_chart_data = _tab_chart_json(quarterly_chart_obj) if quarterly_chart_obj else None
    # Valuation section: intrinsic-value football field + MoS + price multiples. Intrinsic-
    # value metrics are dropped from the derived-metrics list to avoid duplicating the
    # football field. valuation_metrics comes from detail.metrics (unchanged, single-value
    # display) — derived_metrics/bench_ctx were already fetched above the fragment branch.
    _, valuation_metrics = services.split_metrics(detail.metrics)
    iv_chart = football.build_chart(services.get_intrinsic_value_field(ticker))
    mos_scenarios = services.get_margin_of_safety_scenarios(ticker)
    market_cap_kpi = services.get_market_cap_kpi(ticker, usd_lens=usd_lens)
    headline = (*headline, market_cap_kpi) if market_cap_kpi else headline
    compare_options = services.all_companies()
    filings = services.get_company_filings(ticker)
    forecast_chart = services.get_forecast_chart(ticker)
    forecast_chart_data = _forecast_chart_json(forecast_chart) if forecast_chart else None
    context = {
        "detail": detail,
        "statements": statements.statements,
        "statement_panes": statement_panes,
        "headline": headline,
        "price_chart": price_chart,
        "price_tab_data": price_tab_data,
        "price_windows": price_windows,
        "price_window": price_window,
        "price_currency": price_currency,
        "show_usd_toggle": show_usd_toggle,
        "usd_lens": usd_lens,
        "quarterly": quarterly,
        "quarterly_chart_data": quarterly_chart_data,
        "statement_chart_data": statement_chart_data,
        "bs_compositions": bs_compositions,
        "bs_compositions_data": bs_compositions_data,
        "derived_metrics": derived_metrics,
        "bench_ctx": bench_ctx,
        "bench": bench,
        "compare_query": compare,
        "summary": summary,
        "compare_options": compare_options,
        "valuation_metrics": valuation_metrics,
        "iv_chart": iv_chart,
        "mos_scenarios": mos_scenarios,
        "filings": filings,
        "forecast_chart": forecast_chart,
        "forecast_chart_data": forecast_chart_data,
    }
    net_net_ctx = _net_net_card_context(ticker)
    if net_net_ctx:
        context.update(net_net_ctx)
    return render(request, "fundamentals_screener/company_detail.html", context)


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
    context = {
        "ticker": ticker,
        "points": points,
        "scenarios": scenarios,
        "chart": chart,
        "price_currency": price_currency,
    }
    net_net_ctx = _net_net_card_context(ticker)
    if net_net_ctx:
        context.update(net_net_ctx)
    return render(request, "fundamentals_screener/valuation.html", context)


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


# ── forecasting ──────────────────────────────────────────────────────────────────────────
def _forecast_chart_json(chart) -> dict:
    """Plain-dict shape shared by the page's embedded ``json_script`` payload and the JSON
    sibling endpoint, so the two never drift."""
    return {
        "ticker": chart.ticker,
        "metrics": [
            {
                "metric": m.metric,
                "label": m.label,
                "unit": m.unit,
                "historical": [{"fiscal_year": h.fiscal_year, "value": h.value} for h in m.historical],
                "scenarios": [
                    {
                        "quantile_level": s.quantile_level,
                        "horizons": list(s.horizons),
                        "values": list(s.values),
                    }
                    for s in m.scenarios
                ],
            }
            for m in chart.metrics
        ],
        "forward_multiples": [
            {"metric": r.metric, "horizon": r.horizon, "value": r.value} for r in chart.forward_multiples
        ],
    }


def forecasting(request: HttpRequest, ticker: str) -> HttpResponse:
    """Server-rendered Forecasting page: 10-year cross-sectional ML scenario fan chart +
    PV-discounted forward-multiples table, per issue #336's mockup. The chart's data is
    embedded via ``json_script`` (see the template) rather than fetched separately — one
    request, no extra round trip."""
    ticker = ticker.upper()
    chart = services.get_forecast_chart(ticker)
    if chart is None:
        raise Http404(f"unknown ticker {ticker!r}")
    return render(
        request, "fundamentals_screener/forecasting.html",
        {"ticker": ticker, "chart": chart, "chart_data": _forecast_chart_json(chart)},
    )


def forecasting_data(request: HttpRequest, ticker: str) -> JsonResponse:
    """JSON sibling of :func:`forecasting` (API surface, mirrors ``valuation_data``)."""
    ticker = ticker.upper()
    chart = services.get_forecast_chart(ticker)
    if chart is None:
        return JsonResponse({"error": f"unknown ticker {ticker!r}"}, status=404)
    return JsonResponse(_forecast_chart_json(chart))
