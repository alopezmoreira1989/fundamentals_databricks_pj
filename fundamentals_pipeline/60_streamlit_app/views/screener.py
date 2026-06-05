"""Screener — landing page. One row per company, filterable, click → detail.

Handoff convention (established here): the selected ticker is written to
``st.session_state["ticker"]`` and we ``st.switch_page`` to the company detail
page, which reads the same key.
"""

import pandas as pd
import streamlit as st

from lib.screener import (
    DEFAULT_COLUMNS,
    MARKET_CAP,
    UNIVERSE_FLAGS,
    bucket_mask,
    buckets_for,
    build_screener_frame,
    search_mask,
    universe_mask,
)

COMPANY_PAGE = "views/company.py"

# st.dataframe row selection (on_select) needs Streamlit ≥ 1.35.
_HAS_DF_SELECTION = tuple(int(p) for p in st.__version__.split(".")[:2]) >= (1, 35)

wide, unit_map, metric_order = build_screener_frame()

st.markdown(
    f'<div class="masthead"><div class="masthead-left">'
    f'<div class="eyebrow">US Equities · Fundamental Analysis</div><h1>US Stock FA Screener</h1>'
    f'<div class="ticker-row"><span class="ticker-chip">{len(wide):,} companies</span></div>'
    f'</div><div class="masthead-right"><div>Latest available FY · USD</div>'
    f'<div style="margin-top:6px;">SEC EDGAR XBRL</div></div></div>',
    unsafe_allow_html=True,
)

# ──────────────────────────────────────────────────────────────────────────────
# Filters (top)
# ──────────────────────────────────────────────────────────────────────────────
with st.container(border=True):
    c1, c2, c3 = st.columns([1.2, 1.8, 3])
    with c1:
        universe = st.selectbox("Universe", list(UNIVERSE_FLAGS), index=0)
    with c2:
        query = st.text_input("Search (ticker or name)", "")
    with c3:
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
mask = universe_mask(wide, universe) & search_mask(wide, query)
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


def _go_to(ticker: str) -> None:
    st.session_state["ticker"] = ticker
    st.switch_page(COMPANY_PAGE)


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
