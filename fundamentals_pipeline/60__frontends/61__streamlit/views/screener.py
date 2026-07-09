"""Screener — landing page. One row per company, filterable, click → detail.

Handoff convention (established here): the selected ticker is written to
``st.session_state["ticker"]`` and we ``st.switch_page`` to the company detail
page, which reads the same key.

Above the filters sits an editorial "fold" (Variant 2 — compact): a 4-card stat
band, a clickable sector-distribution strip (drives the sector filter), and a
featured-companies logo row (each tile → detail). All three are pure-frontend
reads off the same ``wide`` frame / published artifacts; no pipeline dependency.
"""

import html
import re
from urllib.parse import quote

import pandas as pd
import streamlit as st
from lib.currency import convert_to_usd, currency_badge, usd_lens_toggle
from lib.data import load_fx
from lib.render import _logo_dev_key, logo_dev_url
from lib.screener import (
    ALL_COUNTRIES,
    ALL_INDUSTRIES,
    DEFAULT_COLUMNS,
    MARKET_CAP,
    MARKET_LABELS,
    SECTORS,
    UNIVERSE_FLAGS,
    UNKNOWN_SECTOR,
    bucket_mask,
    buckets_for,
    build_screener_frame,
    country_mask,
    country_options,
    industry_mask,
    industry_options,
    market_mask,
    search_mask,
    sector_mask,
    universe_mask,
)
from lib.signals import signal_absolute

COMPANY_PAGE = "views/company.py"
SECTOR_KEY = "scr_sector"   # selectbox key the sector strip drives via callback
INDUSTRY_KEY = "scr_industry"   # Industry selectbox key (also seeded by the compare-page drill-through)
COUNTRY_KEY = "scr_country"   # Country selectbox key (data-driven options, same seeding pattern as Industry)

# ──────────────────────────────────────────────────────────────────────────────
# URL = single source of truth for ALL filter state.
# st.html injects into the top-level DOM (no iframe) and strips JS, so the table's
# header links are real browser navigations — a HARD reload that wipes session_state.
# To survive that, every header link carries the COMPLETE filter state (universe,
# sector, search, columns, bucket selections, sort column + direction) in the query
# string. On load each keyed widget seeds its initial value from st.query_params
# (falling back to today's defaults); because the widgets are keyed, that seed only
# matters on the first instantiation after a reload — normal in-session interaction
# keeps working via session_state, untouched.
# ──────────────────────────────────────────────────────────────────────────────


def _slug(name: str) -> str:
    """lowercase, collapse runs of non-alphanumerics to ``_``, strip leading/trailing ``_``."""
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def _encode_buckets(selections: dict[str, list[str]]) -> str:
    """Encode active bucket selections as ``<slug>:<label1>,<label2>|<slug2>:...``.

    Metrics with an empty selection are omitted, so an all-clear state encodes to "".
    Slugs are alphanumeric/underscore only and bucket labels never contain the
    ``|`` / ``:`` / ``,`` delimiters (verified), so the encoding is unambiguous.
    """
    parts = []
    for metric, labels in selections.items():
        if not labels:
            continue
        parts.append(f"{_slug(metric)}:{','.join(labels)}")
    return "|".join(parts)


def _decode_buckets(raw: str, metrics: list[str]) -> dict[str, list[str]]:
    """Inverse of :func:`_encode_buckets`, keyed by the ORIGINAL metric name.

    Slugs are resolved back to metric names by recomputing ``_slug(m)`` for each
    ``m`` in ``metrics``. Tolerant of empty/malformed input — never raises, just
    yields ``{}`` for anything it can't parse cleanly.
    """
    out: dict[str, list[str]] = {}
    if not raw:
        return out
    slug_to_metric = {_slug(m): m for m in metrics}
    for chunk in raw.split("|"):
        if ":" not in chunk:
            continue
        slug, _, labels_raw = chunk.partition(":")
        metric = slug_to_metric.get(slug)
        if metric is None:
            continue
        labels = [lbl for lbl in labels_raw.split(",") if lbl]
        if labels:
            out[metric] = labels
    return out


def _state_qs(
    universe: str, market: str, sector: str, industry: str, country: str, query: str,
    cols: list[str], selections: dict[str, list[str]], sort_col: str | None, sort_dir: str,
) -> str:
    """Build the FULL query string (no leading "?") encoding all filter state + sort.

    Each value is percent-encoded via ``quote(..., safe="")``. A key is omitted
    when its value is falsy/default (universe "All", market "All", sector "All sectors",
    industry "All industries", country "All countries", empty search, no columns, no
    buckets) to keep URLs short — but ``sort``/``dir`` are ALWAYS emitted once a sort is
    active.
    """
    parts: list[tuple[str, str]] = []
    if universe and universe != "All":
        parts.append(("u", universe))
    if market and market != "All":
        parts.append(("m", market))
    if sector and sector != SECTORS[0]:
        parts.append(("sec", sector))
    if industry and industry != ALL_INDUSTRIES:
        parts.append(("ind", industry))
    if country and country != ALL_COUNTRIES:
        parts.append(("ctry", country))
    if query:
        parts.append(("q", query))
    if cols:
        parts.append(("cols", "|".join(cols)))
    encoded_buckets = _encode_buckets(selections)
    if encoded_buckets:
        parts.append(("b", encoded_buckets))
    if sort_col:
        parts.append(("sort", sort_col))
        parts.append(("dir", sort_dir))
    return "&".join(f"{k}={quote(str(v), safe='')}" for k, v in parts)


# Resolve the Logo.dev key ONCE per run. _logo_dev_key() touches st.secrets; with no
# secrets.toml that surfaces a "No secrets found" alert, and calling it per featured tile
# (×8) spams the page AND collapses the app's injected <style>. One call here matches the
# masthead's single access; when it returns None we paint inline monograms without
# re-touching st.secrets. On deployed apps the key is present, so tiles hotlink Logo.dev.
_LOGO_KEY = _logo_dev_key()

wide, unit_map, metric_order = build_screener_frame()
fx = load_fx()

# Country (incorporation/HQ jurisdiction) — data-driven, unscoped by sector/universe (unlike
# Industry, which is sector-scoped) since it's a geographic property, not a business one.
_country_opts = country_options(wide)

# Seed the sector filter from the URL BEFORE anything reads SECTOR_KEY (the stat band,
# the sector strip and the selectbox all share it). Only fires when the key is absent —
# i.e. right after a hard reload — so in-session sector clicks are never clobbered.
if SECTOR_KEY not in st.session_state:
    _sec_qp = st.query_params.get("sec")
    st.session_state[SECTOR_KEY] = _sec_qp if _sec_qp in SECTORS else SECTORS[0]

# The active sector — resolved ONCE here and reused by the stat band, the sector strip and
# Featured. It is the session key both the sector bars (_set_sector callback) and the filter
# selectbox write; default "All sectors" (sector_mask is a no-op for that default).
active_sector = st.session_state.get(SECTOR_KEY, SECTORS[0])

# Industry filter — data-driven options, SCOPED to the active sector. Yahoo `industry` does not
# strictly nest under GICS `sector` (≈43% of industries span >1 sector), so this only SHORTENS the
# menu to industries with ≥1 member in the chosen sector — it never hides companies (the sector and
# industry masks below stay independent AND filters). With "All sectors" sector_mask is a no-op, so
# the full industry set is offered. Seeded from ?ind on a hard reload; the compare-page drill-through
# instead writes INDUSTRY_KEY into session_state before switching here (so seeding is skipped). A
# value absent from the (now sector-scoped) option list falls back to the default — so changing the
# sector resets an out-of-scope industry to "All industries".
_industry_opts = industry_options(wide[sector_mask(wide, active_sector)])
if INDUSTRY_KEY not in st.session_state:
    _ind_qp = st.query_params.get("ind")
    st.session_state[INDUSTRY_KEY] = _ind_qp if _ind_qp in _industry_opts else ALL_INDUSTRIES
if st.session_state.get(INDUSTRY_KEY) not in _industry_opts:
    st.session_state[INDUSTRY_KEY] = ALL_INDUSTRIES
active_industry = st.session_state.get(INDUSTRY_KEY, ALL_INDUSTRIES)


# ──────────────────────────────────────────────────────────────────────────────
# Handoff + filter callbacks (defined early — the fold widgets below use them)
# ──────────────────────────────────────────────────────────────────────────────
def _go_to(ticker: str) -> None:
    st.session_state["ticker"] = ticker
    st.switch_page(COMPANY_PAGE)


def _set_sector(sector: str) -> None:
    """on_click callback — runs before the sector selectbox is instantiated, so writing
    its session-state key here is the valid way to drive it from the bars."""
    st.session_state[SECTOR_KEY] = sector


# Query-param handoffs from the HTML results table (st.html anchors, no JS):
#   ?ticker=XYZ  → a row was clicked → navigate to detail via the single _go_to entry point.
#   ?sort=COL&dir=…&u=…&sec=…&q=…&cols=…&b=…  → a header was clicked → a hard reload that
#                  carries the full filter state; widgets re-seed from it (see helpers above).
# The ticker handler runs first and short-circuits the page (switch_page aborts the run).
_qp_ticker = st.query_params.get("ticker")
if _qp_ticker:
    st.query_params.clear()
    _go_to(str(_qp_ticker))

# Sort state comes straight from the URL — toggling happens when the link is BUILT
# (see _sort_href below), not when it's read, so there is nothing to clean up here.
sort_col = st.query_params.get("sort")
sort_dir = st.query_params.get("dir", "asc")


st.markdown(
    '<div class="scr-masthead">'
    '<div class="scr-kicker"><span class="scr-hair"></span>'
    '<span class="scr-kick-txt">US Equities · Fundamental Analysis</span>'
    '<span class="scr-hair"></span></div>'
    '<h1>US Stock FA Screener</h1>'
    f'<div class="scr-dateline"><span class="ticker-chip">{len(wide):,} companies</span>'
    '<span class="scr-sep">|</span><span>Latest available FY · USD</span>'
    '<span class="scr-sep">|</span><span>SEC EDGAR XBRL</span></div>'
    '</div>',
    unsafe_allow_html=True,
)


# ──────────────────────────────────────────────────────────────────────────────
# Fold "superior" — stat band + sector distribution + featured (above the filters)
# ──────────────────────────────────────────────────────────────────────────────
def _fmt_dash(value: str | None) -> str:
    return value if value else "—"


def _median_pct(src: pd.DataFrame, col: str) -> str | None:
    """Median of a numeric column as a ``%.1f%%`` string; None if the column is absent
    or all-NaN. NaN is ignored — never raises."""
    if col not in src.columns:
        return None
    s = pd.to_numeric(src[col], errors="coerce").dropna()
    return f"{s.median():.1f}%" if not s.empty else None


def _stat_band(src: pd.DataFrame, active_sector: str) -> str:
    """4-card stat band reusing the shared .kpi-strip/.kpi classes (no CSS change), computed
    over `src` — the active sector's subset, or all of `wide` for "All sectors". Companies
    counts `src`; the medians (P/E ex-negatives, ROE %, Rev YoY %) are over `src`, ignoring
    NaN and degrading to an em-dash. The Companies sub-note names the active sector when one
    is selected. Never raises."""
    companies = f"{len(src):,}"

    pe = pd.to_numeric(src["P/E"], errors="coerce") if "P/E" in src.columns else pd.Series(dtype=float)
    pe_pos = pe[pe > 0]
    median_pe = _fmt_dash(f"{pe_pos.median():.1f}x" if not pe_pos.empty else None)
    median_roe = _fmt_dash(_median_pct(src, "ROE %"))
    median_yoy = _fmt_dash(_median_pct(src, "Revenue YoY %"))

    note = ("S&amp;P 500 + Russell 3000" if active_sector == SECTORS[0]
            else f'<span style="color:var(--accent-ink)">{html.escape(active_sector)}</span>')

    return (
        '<div class="kpi-strip">'
        f'<div class="kpi"><div class="label">Companies</div><div class="value">{companies}</div>'
        f'<div class="delta flat">{note}</div></div>'
        f'<div class="kpi"><div class="label">Median P/E</div><div class="value">{median_pe}</div>'
        '<div class="delta flat">latest FY · ex-negatives</div></div>'
        f'<div class="kpi"><div class="label">Median ROE %</div><div class="value">{median_roe}</div>'
        '<div class="delta flat">latest FY</div></div>'
        f'<div class="kpi"><div class="label">Median Rev YoY %</div><div class="value">{median_yoy}</div>'
        '<div class="delta flat">latest FY</div></div>'
        '</div>'
    )


# ── Valuation tape (hero signature) ───────────────────────────────────────────
# Graham P/E bands. The labels MUST equal _VALUATION_MULTIPLE_BAND in lib.screener
# so a band click round-trips into the P/E pill. Each tuple is
# (label, lo_inclusive, hi_exclusive, axis text, fill class).
_PE_TAPE_BANDS: list[tuple[str, float, float, str, str]] = [
    ("Loss",  float("-inf"), 0.0,           "&lt; 0",   "b-loss"),
    ("Cheap", 0.0, 10.0,                     "0–10x",    "b-cheap"),
    ("Fair",  10.0, 15.0,                    "10–15x",   "b-fair"),
    ("Full",  15.0, 25.0,                    "15–25x",   "b-full"),
    ("Rich",  25.0, float("inf"),            "&gt; 25x", "b-rich"),
]


def _valuation_tape(src: pd.DataFrame, active_sector: str, active_industry: str) -> str:
    """Hero ribbon — the (sector-filtered) universe binned into Graham P/E bands.

    Each segment grows with its company count and is an anchor whose href carries
    the FULL filter state with that band toggled in the P/E selection (the same
    state-in-URL mechanism the sortable headers use). Current state is read from
    the URL so this stays self-contained at the top of the page, before the filter
    widgets are instantiated. NaN P/E rows are excluded from every band (matching
    ``bucket_mask``). Note: because the column multiselect lives in session (not the
    URL) during normal interaction, a band click resets columns to the URL/default
    set (always incl. P/E) — acceptable for a top-of-page overview lens.
    """
    pe = pd.to_numeric(src.get("P/E"), errors="coerce")
    counts = [int(((pe >= lo) & (pe < hi)).sum()) for _, lo, hi, *_ in _PE_TAPE_BANDS]
    total = int(sum(counts))
    pe_pos = pe[pe > 0]
    mpe = f"{pe_pos.median():.1f}x" if not pe_pos.empty else "—"
    scope = "All US equities" if active_sector == SECTORS[0] else active_sector

    # Current state from the URL → toggle hrefs + active-band highlighting.
    sel_top = _decode_buckets(st.query_params.get("b", ""), metric_order)
    active_pe = set(sel_top.get("P/E", []))
    universe_top = st.query_params.get("u", "All")
    if universe_top not in UNIVERSE_FLAGS:
        universe_top = "All"
    market_top = st.query_params.get("m", "All")
    if market_top not in MARKET_LABELS:
        market_top = "All"
    country_top = st.query_params.get("ctry", ALL_COUNTRIES)
    if country_top not in _country_opts:
        country_top = ALL_COUNTRIES
    query_top = st.query_params.get("q", "")
    cols_top = [c for c in st.query_params.get("cols", "").split("|") if c in metric_order] \
        or [c for c in DEFAULT_COLUMNS if c in metric_order]
    # P/E must be a column for its pill (hence the filter) to instantiate after the reload.
    cols_href = cols_top if "P/E" in cols_top else cols_top + ["P/E"]

    segs, axis = [], []
    for (label, _lo, _hi, axis_txt, cls), cnt in zip(_PE_TAPE_BANDS, counts, strict=True):
        cur = [b for b in active_pe if b != label]
        if label not in active_pe:
            cur.append(label)
        toggled = {k: v for k, v in sel_top.items() if k != "P/E"}
        toggled["P/E"] = cur
        href = html.escape("?" + _state_qs(
            universe_top, market_top, active_sector, active_industry, country_top, query_top,
            cols_href, toggled, sort_col, sort_dir))
        is_active = " is-active" if label in active_pe else ""
        segs.append(
            f'<a class="scr-band {cls}{is_active}" href="{href}" '
            f'style="flex-grow:{cnt}" '
            f'title="{html.escape(label)} · P/E {axis_txt} · {cnt:,} companies">'
            f'<span class="b-count">{cnt:,}</span>'
            f'<span class="b-label">{html.escape(label)}</span></a>'
        )
        axis.append(f'<span style="flex-grow:{cnt}">{axis_txt}</span>')

    return (
        '<div class="scr-tape-card">'
        '<div class="scr-tape-head">'
        f'<span class="t-title">{html.escape(scope)}, by valuation</span>'
        f'<span class="t-note">median P/E <b>{mpe}</b> · {total:,} companies with a P/E</span>'
        '</div>'
        '<div class="scr-tape">' + "".join(segs) + '</div>'
        '<div class="scr-tape-axis">' + "".join(axis) + '</div>'
        '</div>'
    )


def _medians_line(src: pd.DataFrame, active_sector: str) -> str:
    """Slim mono medians strip — supporting metadata under the tape (replaces the
    legacy 4-card .kpi-strip on the screener; P/E now lives in the tape header)."""
    companies = f"{len(src):,}"
    roe = _fmt_dash(_median_pct(src, "ROE %"))
    yoy = _fmt_dash(_median_pct(src, "Revenue YoY %"))
    scope = ("S&amp;P 500 + Russell 3000" if active_sector == SECTORS[0]
             else f'<span style="color:var(--accent-ink)">{html.escape(active_sector)}</span>')
    return (
        '<div class="scr-medians">'
        f'<span class="m-item"><b>{companies}</b>&nbsp;companies · {scope}</span>'
        f'<span class="m-item">Median ROE&nbsp;<b>{roe}</b></span>'
        f'<span class="m-item">Median Rev YoY&nbsp;<b>{yoy}</b></span>'
        '</div>'
    )


# Hero: the valuation tape (signature) + the slim medians line. Falls back to the
# legacy 4-card stat band only when P/E is absent from the published artifacts.
_src = wide[sector_mask(wide, active_sector) & industry_mask(wide, active_industry)]
if "P/E" in wide.columns:
    st.markdown(_valuation_tape(_src, active_sector, active_industry), unsafe_allow_html=True)
    st.markdown(_medians_line(_src, active_sector), unsafe_allow_html=True)
else:
    st.markdown(_stat_band(_src, active_sector), unsafe_allow_html=True)

dist_col, feat_col = st.columns([1.05, 1])

# --- Sector distribution (left, clickable) ---------------------------------------------
with dist_col, st.container(key="scr_sectors"):
    st.markdown(
        '<div class="scr-panel-head"><h3>Universe by sector</h3>'
        '<div class="meta">click to filter</div></div>',
        unsafe_allow_html=True,
    )
    if "sector" in wide.columns:
        counts = wide["sector"].value_counts()
        counts = counts[counts.index != UNKNOWN_SECTOR]
    else:
        counts = pd.Series(dtype=int)
    top = counts.head(5)
    max_count = int(top.max()) if not top.empty else 1

    def _barline(count: int, *, active: bool = False, resto: bool = False) -> str:
        """One flex line — proportional track + right-aligned count — sat tight under a
        sector's label. Width is normalised to the top-5 max count."""
        cls = " is-active" if active else (" resto" if resto else "")
        pct = min(100.0, count / max_count * 100)
        return (
            f'<div class="scr-barline{cls}"><div class="scr-track">'
            f'<div class="scr-fill" style="width:{pct:.0f}%"></div></div>'
            f'<span class="scr-count">{count:,}</span></div>'
        )

    # Top-5: a flat full-width button (the whole-row hit target) with its bar+count
    # directly beneath, each pair in its own container so the rhythm stays uniform.
    for sector, cnt in top.items():
        slug = str(sector).lower().replace(" ", "_")
        with st.container():
            st.button(str(sector), key=f"scr_sec_{slug}", on_click=_set_sector,
                      args=(str(sector),), width="stretch")
            st.markdown(_barline(int(cnt), active=(sector == active_sector)), unsafe_allow_html=True)

    # Highlight the active sector's button (the label pill) — one exact-key rule, injected
    # only when the active sector is among the drawn top-5 (else there's no bar to flag; the
    # KPIs and Featured still reflect it). Marker-guarded so a rerun can't stack it.
    if active_sector in top.index:
        slug = str(active_sector).lower().replace(" ", "_")
        st.html(
            f"<style>/* scr-sec-active */ .st-key-scr_sec_{slug} button{{"
            "background:var(--accent-soft)!important;color:var(--accent-ink)!important;"
            "font-weight:600!important;border-radius:4px!important;}</style>"
        )

    # "resto" — display-only muted aggregate of the remaining sectors. There is no single
    # sector to filter to, so it is intentionally NOT a button (the bar reads as an
    # aggregate via the muted --rule fill).
    rest = counts.iloc[5:]
    if not rest.empty:
        with st.container():
            st.markdown(f'<div class="scr-rest-label">+ {rest.size} more</div>', unsafe_allow_html=True)
            st.markdown(_barline(int(rest.sum()), resto=True), unsafe_allow_html=True)

    # Reset — always reachable; a no-op when already on "All sectors".
    st.button("↺ All sectors", key="scr_sec_reset", on_click=_set_sector, args=(SECTORS[0],),
              width="stretch")


# --- Featured companies (right, clickable) ---------------------------------------------
def _monogram_data_uri(name: str) -> str:
    """Inline SVG data-URI monogram for the no-key fallback — the tile background when
    Logo.dev isn't configured (local dev). Mirrors render.py's editorial monogram
    (accent-soft fill, accent-ink letter); hex literals because var() can't resolve inside
    a data-URI SVG. The SVG is URL-escaped, so no raw angle-bracket tag tokens enter the
    injected style element."""
    letter = html.escape((name or "•").strip()[:1].upper() or "•")
    svg = (
        "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'>"
        "<rect width='64' height='64' rx='6' fill='#E6F1FB'/>"
        "<text x='32' y='34' fill='#0C447C' font-size='30' font-weight='600' "
        "font-family='Fraunces,Times New Roman,serif' text-anchor='middle' "
        "dominant-baseline='central'>" + letter + "</text></svg>"
    )
    return 'url("data:image/svg+xml;utf8,' + quote(svg) + '")'


def _css_token(s: str) -> str:
    """Escape a ticker for use inside a CSS class selector — a literal dot would split
    `.st-key-scr_feat_BF.B` into two classes. Top-8 mega-caps are alnum, but be safe."""
    return s.replace("\\", "\\\\").replace(".", "\\.")


def _company_key(name: str) -> str:
    """Dedup key collapsing dual-class listings to one company: strip a trailing
    ``(Class A/B/C)`` parenthetical (e.g. 'Alphabet Inc. (Class C)' and '... (Class A)'
    map to the same key), then casefold. Keeps GOOG/GOOGL, FOX/FOXA, NWS/NWSA from each
    taking a featured slot."""
    return re.sub(r"\s*\(class\b[^)]*\)\s*$", "", str(name), flags=re.I).strip().casefold()


with feat_col, st.container(key="scr_featured"):
    # Featured follows the active sector (resolved once at the top of the page). Default
    # "All sectors" → top-12 global (sector_mask is a no-op there). Header names the sector.
    feat_title = "Featured" if active_sector == SECTORS[0] else f"Featured · {active_sector}"
    st.markdown(
        f'<div class="scr-panel-head"><h3>{html.escape(feat_title)}</h3>'
        '<div class="meta">→ detail</div></div>',
        unsafe_allow_html=True,
    )
    feat = wide[sector_mask(wide, active_sector)].copy()
    feat["_mc"] = pd.to_numeric(feat.get(MARKET_CAP), errors="coerce")
    # Top-12 by Market Cap (3 rows of 4), deduplicated by company keeping the highest cap —
    # so dual-class names (GOOG/GOOGL, FOX/FOXA) take one slot and free the next for a
    # distinct company. Row 3 reaches mid-caps where Logo.dev hits less, so expect more of
    # the CDN's own monograms there — expected, not a break.
    feat["_cokey"] = feat["company"].map(_company_key)
    feat = (
        feat.sort_values("_mc", ascending=False, na_position="last")
        .drop_duplicates("_cokey", keep="first")
        .head(12)
    )
    feat_rows = feat.to_dict("records")

    # ONE <style> block painting each tile's logo as its button background. Built once from
    # the top-8 (marker-guarded so it can't stack on rerun). Key present → Logo.dev hotlink
    # (the CDN's own monogram covers misses, since has_logo isn't carried in `wide`); key
    # absent → inline data-URI monogram. Tickers are exact per-ticker st-key classes (dots
    # escaped), so this never reaches the tab buttons or the keyless `Open` fallback.
    rules: list[str] = []
    for rec in feat_rows:
        tkr = str(rec["ticker"])
        co = str(rec.get("company") or tkr)
        bg = f"url('{logo_dev_url(tkr, _LOGO_KEY)}')" if _LOGO_KEY else _monogram_data_uri(co)
        rules.append(
            f".st-key-scr_feat_{_css_token(tkr)} button{{background-image:{bg};"
            "background-size:contain;background-position:center;background-repeat:no-repeat;}"
        )
    if feat_rows:
        st.html("<style>/* scr-feat-logos */ " + " ".join(rules) + "</style>")

    for r0 in range(0, len(feat_rows), 4):
        for cell, rec in zip(st.columns(4), feat_rows[r0:r0 + 4], strict=False):
            ticker = str(rec["ticker"])
            company = str(rec.get("company") or ticker)
            with cell:
                # The button IS the logo box (styled via .st-key-scr_feat_*); its label is
                # transparent so the artwork shows. Company name → tooltip, not a 3rd line.
                # _go_to runs in the main flow (not on_click) — matches the proven handoff
                # pattern and keeps st.switch_page out of a callback.
                if st.button(ticker, key=f"scr_feat_{ticker}", help=company, width="stretch"):
                    _go_to(ticker)
                st.markdown(
                    f'<div class="scr-tile-tkr">{html.escape(ticker)}</div>',
                    unsafe_allow_html=True,
                )

# ──────────────────────────────────────────────────────────────────────────────
# Filters (top)
# ──────────────────────────────────────────────────────────────────────────────
with st.container(border=True):
    c3b, c1, c1b, c2, c3, c4, c5 = st.columns([1.1, 0.9, 1.0, 1.2, 1.2, 1.2, 2.1])
    with c3b:
        # Incorporation/HQ jurisdiction — independent of Market (e.g. NG/NovaGold is
        # market="US" but country="Canada"). Data-driven, unscoped, same URL-seeding pattern
        # as Market (no session key needed — no drill-through writes into it). Shown first —
        # it's the broadest, most-used cut for the non-US universe.
        _country_qp = st.query_params.get("ctry")
        _country_index = _country_opts.index(_country_qp) if _country_qp in _country_opts else 0
        country = st.selectbox("Country", _country_opts, index=_country_index)
    with c1:
        # Listing market ("US"/"Canada") — independent of Universe (a dual-listed ticker can
        # be market="US" and still count toward S&P/TSX Composite). Same URL-seeding pattern.
        _market_list = list(MARKET_LABELS)
        _market_qp = st.query_params.get("m")
        _market_index = _market_list.index(_market_qp) if _market_qp in _market_list else 0
        market = st.selectbox("Market", _market_list, index=_market_index)
    with c1b:
        # Seed the universe from the URL (first instantiation only) — falls back to index 0.
        _univ_list = list(UNIVERSE_FLAGS)
        _univ_qp = st.query_params.get("u")
        _univ_index = _univ_list.index(_univ_qp) if _univ_qp in _univ_list else 0
        universe = st.selectbox("Universe", _univ_list, index=_univ_index)
    with c2:
        # Keyed so the sector strip's callbacks can drive it; SECTOR_KEY is pre-seeded from
        # the URL above, so default = that value on a fresh load, else "All sectors".
        sector = st.selectbox("Sector", SECTORS, key=SECTOR_KEY)
    with c3:
        # Keyed + pre-seeded above (from ?ind or the compare-page drill-through); options are
        # data-driven. The Sectors page links here with ?ind=<industry> to land pre-filtered.
        industry = st.selectbox("Industry", _industry_opts, key=INDUSTRY_KEY)
    with c4:
        query = st.text_input("Search (ticker or name)", st.query_params.get("q", ""))
    with c5:
        # Seed columns from the URL (filtered to ones that still exist), else today's defaults.
        _cols_qp = [c for c in st.query_params.get("cols", "").split("|") if c in metric_order]
        default_cols = _cols_qp or [c for c in DEFAULT_COLUMNS if c in metric_order]
        cols = st.multiselect("Columns (metrics)", metric_order, default=default_cols)

    if "reporting_currency" in wide.columns and (wide["reporting_currency"].astype(str).str.upper().isin(["CAD"])).any():
        usd_lens = usd_lens_toggle()
        st.caption("Converts Market Cap only, at each fiscal year's own FX close — other $ columns stay native-currency.")
    else:
        usd_lens = False

    # Filterable metrics follow the selected columns (always incl. Market Cap),
    # so adding any metric as a column makes it filterable. Buckets come from the
    # per-metric / unit-based bands in lib.screener — no percentile clamping.
    filter_metrics = [MARKET_CAP] + [c for c in cols if c != MARKET_CAP]
    filter_metrics = [m for m in filter_metrics if m in wide.columns]

    # Decode any bucket selections carried in the URL once, keyed by metric name.
    _bucket_defaults = _decode_buckets(st.query_params.get("b", ""), metric_order)

    st.caption("Range filters")
    bucket_specs: dict[str, list[tuple[str, float, float]]] = {}
    selections: dict[str, list[str]] = {}
    pcols = st.columns(3)
    for i, metric in enumerate(filter_metrics):
        buckets = buckets_for(metric, unit_map.get(metric))
        if not buckets:
            continue
        bucket_specs[metric] = buckets
        labels = [b[0] for b in buckets]
        # Seed from the URL (first instantiation only); drop any stale/invalid labels.
        seed = [lbl for lbl in _bucket_defaults.get(metric, []) if lbl in labels]
        with pcols[i % 3]:
            sel = st.pills(
                metric, labels,
                selection_mode="multi", key=f"bucket_{metric}", default=seed,
            )
        selections[metric] = sel or []

# ──────────────────────────────────────────────────────────────────────────────
# Apply filters
# ──────────────────────────────────────────────────────────────────────────────
mask = (universe_mask(wide, universe) & market_mask(wide, market) & sector_mask(wide, sector)
        & industry_mask(wide, industry) & country_mask(wide, country) & search_mask(wide, query))
for metric, sel in selections.items():
    mask &= bucket_mask(wide[metric], sel, bucket_specs[metric])
fdf = wide[mask].reset_index(drop=True)

# ──────────────────────────────────────────────────────────────────────────────
# Display table — semantic color-banded HTML (chips reuse lib.signals thresholds)
# ──────────────────────────────────────────────────────────────────────────────
# Replaces st.dataframe with an st.html() <table>: numeric cells carry green/amber/red
# chips driven by signal_absolute() (the same good/warn/bad bands as the detail page and
# the bucket pills), ticker cells link to the detail page via ?ticker=, and headers sort
# server-side via ?sort=. st.html strips JS, so all interactivity is plain anchors.
show_cols = ["ticker", "company"] + [c for c in cols if c in fdf.columns]
disp = fdf[show_cols].rename(columns={"ticker": "Ticker", "company": "Company"})

# Market Cap's native currency per ticker — badged unless/until converted below. Other
# usd-unit columns (EV, Revenue, …) aren't badged/converted here; see the toggle caption.
if "reporting_currency" in fdf.columns:
    mc_ccy = fdf["reporting_currency"].astype(object).fillna("USD").replace("", "USD").astype(str).str.upper()
else:
    mc_ccy = pd.Series("USD", index=fdf.index)

if usd_lens and MARKET_CAP in disp.columns and "market_cap_period_end" in fdf.columns:
    mc_usd, mc_converted = convert_to_usd(disp[MARKET_CAP], mc_ccy, fdf["market_cap_period_end"], fx)
    disp[MARKET_CAP] = mc_usd
    mc_ccy = mc_ccy.where(~mc_converted, "USD")

# usd metrics (incl. Market Cap) render in $B — convert once so display AND sort agree.
for c in cols:
    if c in disp.columns and (c == MARKET_CAP or unit_map.get(c, "") == "usd"):
        disp[c] = pd.to_numeric(disp[c], errors="coerce") / 1e9

metric_cols = [c for c in disp.columns if c not in ("Ticker", "Company")]

_MARKET_CAP_HELP = (
    "Calendar year-end value. P/E and other fiscal-year metrics use the company's "
    "fiscal year-end, so non-December filers (e.g. AAPL/Sep, MSFT/Jun, WMT/Jan) have "
    "a 0–11 month basis offset."
)
_SIG_CHIP = {"good": "scr-chip-g", "warn": "scr-chip-a", "bad": "scr-chip-r"}


def _header_label(col: str) -> str:
    """Friendly header text — mirrors the old column_config labels ($B suffix for USD)."""
    if col == MARKET_CAP or unit_map.get(col, "") == "usd":
        return f"{col} ($B)"
    return col


def _fmt_value(col: str, val, ccy: str = "USD") -> str:
    """Format one numeric cell to match the old column_config formats; NaN → em-dash.

    `ccy` badges Market Cap when it's not (or couldn't be converted to) USD — see the
    mc_ccy tracking above. Other usd-unit columns aren't badged (unconverted, native
    currency; scope explicitly limited to Market Cap for now).
    """
    if pd.isna(val):
        return "—"
    unit = unit_map.get(col, "")
    if col == MARKET_CAP or unit == "usd":   # already divided to $B above
        badge = currency_badge(ccy) if col == MARKET_CAP else ""
        return f"${val:.1f}B{badge}"
    if unit == "percent":
        return f"{val:.1f}%"
    if unit == "ratio":
        return f"{val:.2f}x"
    return f"{val:.2f}"


# Sort (server-side — st.html can't run JS). sort_col/sort_dir were read from the URL at
# the top of the page. Text columns sort case-insensitively; numeric columns sort on their
# coerced value. NaN sinks to the bottom either way.
if sort_col and sort_col in disp.columns:
    key = (lambda s: s.astype(str).str.lower()) if sort_col in ("Ticker", "Company") \
        else (lambda s: pd.to_numeric(s, errors="coerce"))
    disp = disp.sort_values(
        by=sort_col, ascending=(sort_dir == "asc"), key=key, na_position="last",
    )

st.caption(f"{len(disp):,} companies after filters")
if MARKET_CAP in cols:
    st.caption(
        "Market Cap is as of the calendar year-end; fiscal-year metrics (P/E, "
        "margins, …) use the fiscal year-end — a mixed basis for non-December filers."
    )


def _table_html() -> str:
    """Build the full results table as a sanitiser-safe HTML string (no JS, no inline hex)."""
    arrow = "▾" if sort_dir == "desc" else "▴"

    def _mark(col: str) -> str:
        return f'<span class="scr-arrow">{arrow}</span>' if sort_col == col else ""

    def _next_dir(col: str) -> str:
        """Direction this header should request: flip if it's the active sort, else asc."""
        if sort_col == col:
            return "desc" if sort_dir == "asc" else "asc"
        return "asc"

    def _sort_href(col: str) -> str:
        """Full href carrying ALL filter state + the requested sort (HTML-attribute safe)."""
        qs = _state_qs(universe, market, sector, industry, country, query, cols, selections, col, _next_dir(col))
        return html.escape("?" + qs)

    head = [
        f'<th class="scr-th-tkr"><a href="{_sort_href("Ticker")}">Ticker{_mark("Ticker")}</a></th>',
        f'<th class="scr-th-co"><a href="{_sort_href("Company")}">Company{_mark("Company")}</a></th>',
    ]
    for c in metric_cols:
        title = f' title="{html.escape(_MARKET_CAP_HELP)}"' if c == MARKET_CAP else ""
        head.append(
            f'<th{title}><a href="{_sort_href(c)}">'
            f'{html.escape(_header_label(c))}{_mark(c)}</a></th>'
        )

    body = []
    row_ccy = mc_ccy.reindex(disp.index).tolist()
    for rec, ccy in zip(disp.to_dict("records"), row_ccy, strict=True):
        tkr = str(rec["Ticker"])
        co = str(rec["Company"])
        cells = [
            f'<td class="scr-td-tkr"><a href="?ticker={quote(tkr)}">{html.escape(tkr)}</a></td>',
            f'<td class="scr-td-co" title="{html.escape(co)}">{html.escape(co)}</td>',
        ]
        for c in metric_cols:
            text = _fmt_value(c, rec[c], ccy)
            if text == "—":
                cells.append('<td class="scr-na">—</td>')
                continue
            chip = _SIG_CHIP.get(signal_absolute(c, rec[c]))
            cells.append(
                f'<td><span class="{chip}">{text}</span></td>' if chip else f"<td>{text}</td>"
            )
        body.append("<tr>" + "".join(cells) + "</tr>")

    return (
        '<div class="scr-tbl-wrap"><table class="scr-tbl"><thead><tr>'
        + "".join(head)
        + "</tr></thead><tbody>"
        + "".join(body)
        + "</tbody></table></div>"
    )


if hasattr(st, "html"):
    st.html(_table_html())
else:
    # Fallback for very old Streamlit without st.html — plain dataframe + a selectbox bridge
    # (kept so the page still works, just without the color chips / clickable rows).
    st.dataframe(disp, hide_index=True, width="stretch", height=720)
    fb_l, fb_r = st.columns([3, 1])
    with fb_l:
        pick = st.selectbox("Open company", disp["Ticker"].tolist())
    with fb_r:
        st.write("")
        if st.button("Open →") and pick:
            _go_to(pick)
