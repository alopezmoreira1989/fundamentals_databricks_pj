"""Screener — landing page. One row per company, filterable, click → detail.

Handoff convention (established here): the selected ticker is written to
``st.session_state["ticker"]`` and we ``st.switch_page`` to the company detail
page, which reads the same key.

Above the filters sits an editorial "fold" — a stat band, a clickable
sector-distribution strip (drives the sector filter), and a featured-companies
logo row (each tile → detail). All three are pure-frontend reads off the same
``wide`` frame / published artifacts; no pipeline dependency.
"""

import html

import pandas as pd
import streamlit as st
from lib.render import _logo_dev_key, _render_company_logo
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
# masthead's single access; when it returns None we render monograms without re-touching
# st.secrets. On deployed apps the key is present, so tiles hotlink Logo.dev as usual.
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
    f'<div class="masthead"><div class="masthead-left">'
    f'<div class="eyebrow">US Equities · Fundamental Analysis</div><h1>US Stock FA Screener</h1>'
    f'<div class="ticker-row"><span class="ticker-chip">{len(wide):,} companies</span></div>'
    f'</div><div class="masthead-right"><div>Latest available FY · USD</div>'
    f'<div style="margin-top:6px;">SEC EDGAR XBRL</div></div></div>',
    unsafe_allow_html=True,
)


# ──────────────────────────────────────────────────────────────────────────────
# Fold "superior" — stat band + sector distribution + featured (above the filters)
# ──────────────────────────────────────────────────────────────────────────────
def _fmt_dash(value: str | None) -> str:
    return value if value else "—"


def _stat_band() -> str:
    """4-card stat band reusing the shared .kpi-strip/.kpi classes. Every figure degrades
    to an em-dash when its source column is missing/empty — never raises."""
    n = len(wide)
    companies = f"{n:,}"

    sec = wide["sector"].astype(str) if "sector" in wide.columns else pd.Series(dtype=str)
    assigned = sec[sec != UNKNOWN_SECTOR]
    sectors = _fmt_dash(f"{assigned.nunique()} / 11" if not sec.empty else None)
    pct_assigned = _fmt_dash(f"{100 * len(assigned) / n:.0f}%" if n else None)

    pe = pd.to_numeric(wide["P/E"], errors="coerce") if "P/E" in wide.columns else pd.Series(dtype=float)
    pe_pos = pe[pe > 0]
    median_pe = _fmt_dash(f"{pe_pos.median():.1f}x" if not pe_pos.empty else None)

    return (
        '<div class="kpi-strip">'
        f'<div class="kpi"><div class="label">Companies</div><div class="value">{companies}</div></div>'
        f'<div class="kpi"><div class="label">Sectors</div><div class="value">{sectors}</div>'
        '<div class="delta flat">GICS</div></div>'
        f'<div class="kpi"><div class="label">Median P/E</div><div class="value">{median_pe}</div>'
        '<div class="delta flat">latest FY · ex-negatives</div></div>'
        f'<div class="kpi"><div class="label">% with sector</div><div class="value">{pct_assigned}</div>'
        '<div class="delta flat">Wikipedia GICS → IWV → favorites</div></div>'
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

    for sector, cnt in top.items():
        slug = str(sector).lower().replace(" ", "_")
        is_active = " is-active" if sector == active_sector else ""
        st.button(str(sector), key=f"scr_sec_{slug}", on_click=_set_sector, args=(str(sector),),
                  use_container_width=True)
        pct = min(100.0, int(cnt) / max_count * 100)
        st.markdown(
            f'<div class="scr-bar-wrap{is_active}"><div class="scr-bar">'
            f'<div class="scr-bar-fill" style="width:{pct:.0f}%"></div></div>'
            f'<span class="scr-bar-count">{int(cnt):,}</span></div>',
            unsafe_allow_html=True,
        )

    # "resto" — display-only muted aggregate of the remaining sectors.
    rest = counts.iloc[5:]
    if not rest.empty:
        rest_pct = min(100.0, int(rest.sum()) / max_count * 100)
        st.markdown(
            f'<div class="scr-bar-wrap resto"><span class="scr-rest-label">+ {rest.size} more</span>'
            f'<div class="scr-bar"><div class="scr-bar-fill" style="width:{rest_pct:.0f}%"></div></div>'
            f'<span class="scr-bar-count">{int(rest.sum()):,}</span></div>',
            unsafe_allow_html=True,
        )

    # Reset — always reachable; a no-op when already on "All sectors".
    st.button("↺ All sectors", key="scr_sec_reset", on_click=_set_sector, args=(SECTORS[0],),
              use_container_width=True)

# --- Featured companies (right, clickable) ---------------------------------------------
with feat_col, st.container(key="scr_featured"):
    st.markdown(
        '<div class="scr-panel-head"><h3>Featured</h3><div class="meta">→ detail</div></div>',
        unsafe_allow_html=True,
    )
    feat = wide.copy()
    feat["_mc"] = pd.to_numeric(feat.get(MARKET_CAP), errors="coerce")
    feat = feat.sort_values("_mc", ascending=False, na_position="last").head(8)

    feat_rows = feat.to_dict("records")
    for r0 in range(0, len(feat_rows), 4):
        for cell, rec in zip(st.columns(4), feat_rows[r0:r0 + 4], strict=False):
            ticker = str(rec["ticker"])
            company = str(rec.get("company") or ticker)
            with cell:
                # Key present → reuse the Logo.dev render (has_logo not carried in `wide`, so
                # pass None: the CDN covers misses). Key absent → inline editorial monogram,
                # WITHOUT re-touching st.secrets (see _LOGO_KEY). Never raises.
                if _LOGO_KEY:
                    logo = _render_company_logo(ticker, company, None)
                else:
                    letter = html.escape((company or ticker).strip()[:1].upper() or "•")
                    logo = f'<div class="company-logo is-monogram"><span class="logo-monogram">{letter}</span></div>'
                st.markdown(
                    f'<div class="scr-tile">{logo}'
                    f'<div class="scr-tile-tkr">{html.escape(ticker)}</div>'
                    f'<div class="scr-tile-co">{html.escape(company)}</div></div>',
                    unsafe_allow_html=True,
                )
                if st.button("→", key=f"scr_feat_{ticker}", use_container_width=True):
                    _go_to(ticker)

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
