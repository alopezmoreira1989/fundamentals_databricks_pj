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
    RANGE_METRICS,
    UNIVERSE_FLAGS,
    build_screener_frame,
    range_bounds,
    range_mask,
    search_mask,
    universe_mask,
)

COMPANY_PAGE = "views/company.py"

# st.dataframe row selection (on_select) needs Streamlit ≥ 1.35.
_HAS_DF_SELECTION = tuple(int(p) for p in st.__version__.split(".")[:2]) >= (1, 35)

wide, unit_map, metric_order = build_screener_frame()

st.markdown(
    f'<div class="masthead"><div class="masthead-left">'
    f'<div class="eyebrow">Fundamentals · Screener</div><h1>Screener</h1>'
    f'<div class="ticker-row"><span class="ticker-chip">{len(wide):,} compañías</span></div>'
    f'</div><div class="masthead-right"><div>Último FY disponible · USD</div>'
    f'<div style="margin-top:6px;">main.financials.financials_metrics</div></div></div>',
    unsafe_allow_html=True,
)

# ──────────────────────────────────────────────────────────────────────────────
# Filtros (arriba)
# ──────────────────────────────────────────────────────────────────────────────
with st.container(border=True):
    c1, c2, c3 = st.columns([1.2, 1.8, 3])
    with c1:
        universe = st.selectbox("Universo", list(UNIVERSE_FLAGS), index=0)
    with c2:
        query = st.text_input("Buscar (ticker o nombre)", "")
    with c3:
        default_cols = [c for c in DEFAULT_COLUMNS if c in metric_order]
        cols = st.multiselect("Columnas (métricas)", metric_order, default=default_cols)

    range_cols = [m for m in RANGE_METRICS if m in wide.columns]
    slider_specs: dict[str, tuple[tuple[float, float], float, float]] = {}
    scols = st.columns(len(range_cols)) if range_cols else []
    for i, metric in enumerate(range_cols):
        bounds = range_bounds(wide[metric])
        with scols[i]:
            if bounds is None:
                st.caption(f"{metric}: sin datos")
                continue
            lo, hi = bounds
            step = (hi - lo) / 100 or 1.0
            sel = st.slider(metric, lo, hi, (lo, hi), step=step)
            slider_specs[metric] = (sel, lo, hi)

# ──────────────────────────────────────────────────────────────────────────────
# Aplicar filtros
# ──────────────────────────────────────────────────────────────────────────────
mask = universe_mask(wide, universe) & search_mask(wide, query)
for metric, (sel, lo, hi) in slider_specs.items():
    mask &= range_mask(wide[metric], sel[0], sel[1], lo, hi)
fdf = wide[mask].reset_index(drop=True)

# ──────────────────────────────────────────────────────────────────────────────
# Tabla de display (valores numéricos + column_config para que el orden funcione)
# ──────────────────────────────────────────────────────────────────────────────
show_cols = ["ticker", "company"] + [c for c in cols if c in fdf.columns]
disp = fdf[show_cols].rename(columns={"ticker": "Ticker", "company": "Compañía"})

colcfg = {
    "Ticker": st.column_config.TextColumn(width="small"),
    "Compañía": st.column_config.TextColumn(width="medium"),
}
for c in cols:
    if c not in disp.columns:
        continue
    unit = unit_map.get(c, "")
    if c == MARKET_CAP or unit == "usd":
        # USD crudo → $B legible.
        disp[c] = pd.to_numeric(disp[c], errors="coerce") / 1e9
        colcfg[c] = st.column_config.NumberColumn(f"{c} ($B)", format="$%.1fB")
    elif unit == "percent":
        colcfg[c] = st.column_config.NumberColumn(c, format="%.1f%%")
    elif unit == "ratio":
        colcfg[c] = st.column_config.NumberColumn(c, format="%.2fx")
    else:
        colcfg[c] = st.column_config.NumberColumn(c, format="%.2f")

st.caption(f"{len(disp):,} compañías tras filtros")


def _go_to(ticker: str) -> None:
    st.session_state["ticker"] = ticker
    st.switch_page(COMPANY_PAGE)


if _HAS_DF_SELECTION:
    event = st.dataframe(
        disp,
        column_config=colcfg,
        hide_index=True,
        use_container_width=True,
        on_select="rerun",
        selection_mode="single-row",
        key="screener_table",
    )
    rows = getattr(event.selection, "rows", []) if hasattr(event, "selection") else []
    if rows:
        _go_to(disp.iloc[rows[0]]["Ticker"])
else:
    # Fallback para Streamlit < 1.35 (sin on_select).
    st.dataframe(disp, column_config=colcfg, hide_index=True, use_container_width=True)
    fb_l, fb_r = st.columns([3, 1])
    with fb_l:
        pick = st.selectbox("Abrir compañía", disp["Ticker"].tolist())
    with fb_r:
        st.write("")
        if st.button("Abrir →") and pick:
            _go_to(pick)