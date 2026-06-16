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
from lib.render import _logo_dev_key, logo_dev_url
from lib.screener import (
    DEFAULT_COLUMNS,
    MARKET_CAP,
    SECTORS,
    UNIVERSE_FLAGS,
    UNKNOWN_SECTOR,
    bucket_mask,
    buckets_for,
    build_screener_frame,
    search_mask,
    sector_mask,
    universe_mask,
)

COMPANY_PAGE = "views/company.py"
SECTOR_KEY = "scr_sector"   # selectbox key the sector strip drives via callback

# Resolve the Logo.dev key ONCE per run. _logo_dev_key() touches st.secrets; with no
# secrets.toml that surfaces a "No secrets found" alert, and calling it per featured tile
# (×8) spams the page AND collapses the app's injected <style>. One call here matches the
# masthead's single access; when it returns None we paint inline monograms without
# re-touching st.secrets. On deployed apps the key is present, so tiles hotlink Logo.dev.
_LOGO_KEY = _logo_dev_key()

# st.dataframe row selection (on_select) needs Streamlit ≥ 1.35.
_HAS_DF_SELECTION = tuple(int(p) for p in st.__version__.split(".")[:2]) >= (1, 35)

wide, unit_map, metric_order = build_screener_frame()


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


def _median_pct(col: str) -> str | None:
    """Median of a numeric column as a ``%.1f%%`` string; None if the column is absent
    or all-NaN. NaN is ignored — never raises."""
    if col not in wide.columns:
        return None
    s = pd.to_numeric(wide[col], errors="coerce").dropna()
    return f"{s.median():.1f}%" if not s.empty else None


def _stat_band() -> str:
    """4-card stat band reusing the shared .kpi-strip/.kpi classes (no CSS change). The
    'Sectors' / '% with sector' cards are dropped — both are capped by definition now that
    every row carries a sector. Every figure degrades to an em-dash when its source column
    is missing/empty; medians ignore NaN. Never raises."""
    companies = f"{len(wide):,}"

    pe = pd.to_numeric(wide["P/E"], errors="coerce") if "P/E" in wide.columns else pd.Series(dtype=float)
    pe_pos = pe[pe > 0]
    median_pe = _fmt_dash(f"{pe_pos.median():.1f}x" if not pe_pos.empty else None)
    median_roe = _fmt_dash(_median_pct("ROE %"))
    median_yoy = _fmt_dash(_median_pct("Revenue YoY %"))

    return (
        '<div class="kpi-strip">'
        f'<div class="kpi"><div class="label">Companies</div><div class="value">{companies}</div>'
        '<div class="delta flat">S&amp;P 500 + Russell 3000</div></div>'
        f'<div class="kpi"><div class="label">Median P/E</div><div class="value">{median_pe}</div>'
        '<div class="delta flat">latest FY · ex-negatives</div></div>'
        f'<div class="kpi"><div class="label">Median ROE %</div><div class="value">{median_roe}</div>'
        '<div class="delta flat">latest FY</div></div>'
        f'<div class="kpi"><div class="label">Median Rev YoY %</div><div class="value">{median_yoy}</div>'
        '<div class="delta flat">latest FY</div></div>'
        '</div>'
    )


st.markdown(_stat_band(), unsafe_allow_html=True)

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
    active_sector = st.session_state.get(SECTOR_KEY, SECTORS[0])

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
                      args=(str(sector),), use_container_width=True)
            st.markdown(_barline(int(cnt), active=(sector == active_sector)), unsafe_allow_html=True)

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
              use_container_width=True)


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
    # Featured follows the active sector — the same session key the bars (via _set_sector)
    # and the filter selectbox both write. Default "All sectors" → top-12 global, since
    # sector_mask is a no-op there (no special-casing). The header names the active sector.
    active_sector = st.session_state.get(SECTOR_KEY, SECTORS[0])
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
                if st.button(ticker, key=f"scr_feat_{ticker}", help=company, use_container_width=True):
                    _go_to(ticker)
                st.markdown(
                    f'<div class="scr-tile-tkr">{html.escape(ticker)}</div>',
                    unsafe_allow_html=True,
                )

# ──────────────────────────────────────────────────────────────────────────────
# Filters (top)
# ──────────────────────────────────────────────────────────────────────────────
with st.container(border=True):
    c1, c2, c3, c4 = st.columns([1.1, 1.4, 1.6, 3])
    with c1:
        universe = st.selectbox("Universe", list(UNIVERSE_FLAGS), index=0)
    with c2:
        # Keyed so the sector strip's callbacks can drive it; default = "All sectors".
        sector = st.selectbox("Sector", SECTORS, key=SECTOR_KEY)
    with c3:
        query = st.text_input("Search (ticker or name)", "")
    with c4:
        default_cols = [c for c in DEFAULT_COLUMNS if c in metric_order]
        cols = st.multiselect("Columns (metrics)", metric_order, default=default_cols)

    # Filterable metrics follow the selected columns (always incl. Market Cap),
    # so adding any metric as a column makes it filterable. Buckets come from the
    # per-metric / unit-based bands in lib.screener — no percentile clamping.
    filter_metrics = [MARKET_CAP] + [c for c in cols if c != MARKET_CAP]
    filter_metrics = [m for m in filter_metrics if m in wide.columns]

    st.caption("Range filters")
    bucket_specs: dict[str, list[tuple[str, float, float]]] = {}
    selections: dict[str, list[str]] = {}
    pcols = st.columns(3)
    for i, metric in enumerate(filter_metrics):
        buckets = buckets_for(metric, unit_map.get(metric))
        if not buckets:
            continue
        bucket_specs[metric] = buckets
        with pcols[i % 3]:
            sel = st.pills(
                metric, [b[0] for b in buckets],
                selection_mode="multi", key=f"bucket_{metric}",
            )
        selections[metric] = sel or []

# ──────────────────────────────────────────────────────────────────────────────
# Apply filters
# ──────────────────────────────────────────────────────────────────────────────
mask = universe_mask(wide, universe) & sector_mask(wide, sector) & search_mask(wide, query)
for metric, sel in selections.items():
    mask &= bucket_mask(wide[metric], sel, bucket_specs[metric])
fdf = wide[mask].reset_index(drop=True)

# ──────────────────────────────────────────────────────────────────────────────
# Display table (numeric values + column_config so column sorting works)
# ──────────────────────────────────────────────────────────────────────────────
show_cols = ["ticker", "company"] + [c for c in cols if c in fdf.columns]
disp = fdf[show_cols].rename(columns={"ticker": "Ticker", "company": "Company"})

colcfg = {
    "Ticker": st.column_config.TextColumn(width="small"),
    "Company": st.column_config.TextColumn(width="medium"),
}
for c in cols:
    if c not in disp.columns:
        continue
    unit = unit_map.get(c, "")
    if c == MARKET_CAP or unit == "usd":
        # Raw USD → readable $B.
        disp[c] = pd.to_numeric(disp[c], errors="coerce") / 1e9
        help_txt = None
        if c == MARKET_CAP:
            # Market Cap is calendar-year; P/E etc. are fiscal-year (see caption).
            help_txt = (
                "Calendar year-end value. P/E and other fiscal-year metrics use "
                "the company's fiscal year-end, so non-December filers (e.g. "
                "AAPL/Sep, MSFT/Jun, WMT/Jan) have a 0–11 month basis offset."
            )
        colcfg[c] = st.column_config.NumberColumn(f"{c} ($B)", format="$%.1fB", help=help_txt)
    elif unit == "percent":
        colcfg[c] = st.column_config.NumberColumn(c, format="%.1f%%")
    elif unit == "ratio":
        colcfg[c] = st.column_config.NumberColumn(c, format="%.2fx")
    else:
        colcfg[c] = st.column_config.NumberColumn(c, format="%.2f")

st.caption(f"{len(disp):,} companies after filters")
if MARKET_CAP in cols:
    st.caption(
        "Market Cap is as of the calendar year-end; fiscal-year metrics (P/E, "
        "margins, …) use the fiscal year-end — a mixed basis for non-December filers."
    )


if _HAS_DF_SELECTION:
    event = st.dataframe(
        disp,
        column_config=colcfg,
        hide_index=True,
        use_container_width=True,
        height=720,
        on_select="rerun",
        selection_mode="single-row",
        key="screener_table",
    )
    rows = getattr(event.selection, "rows", []) if hasattr(event, "selection") else []
    if rows:
        _go_to(disp.iloc[rows[0]]["Ticker"])
else:
    # Fallback for Streamlit < 1.35 (no on_select).
    st.dataframe(disp, column_config=colcfg, hide_index=True, use_container_width=True, height=720)
    fb_l, fb_r = st.columns([3, 1])
    with fb_l:
        pick = st.selectbox("Open company", disp["Ticker"].tolist())
    with fb_r:
        st.write("")
        if st.button("Open →") and pick:
            _go_to(pick)
