"""Sectors — sub-sector valuation comparison.

Ranks Yahoo ``industry`` groups by median valuation multiple (P/E, P/FCF, EV/EBITDA, P/S, P/B)
plus a company count and two quality medians (ROE %, Revenue YoY %). Pure frontend over the
published artifacts — no Databricks. Clicking an industry hands off to the screener pre-filtered
to it, reusing the screener's own ``?param`` → ``switch_page`` pattern (the same mechanism the
results table uses for ``?ticker``), which sidesteps fragile cross-page HTML hrefs.

Real industries appear only after the pipeline publishes schema v8 (config rebuild + republish);
on older artifacts every company is "Unknown" and the page shows a short note instead of a table.
"""

import html
from urllib.parse import quote

import pandas as pd
import streamlit as st
from lib.format import fmt_metric
from lib.screener import (
    QUALITY_COLS,
    VALUATION_COLS,
    build_screener_frame,
    industry_summary,
)
from lib.signals import signal_absolute

SCREENER_PAGE = "views/screener.py"
INDUSTRY_KEY = "scr_industry"   # session key the screener's Industry selectbox reads

wide, unit_map, _metric_order = build_screener_frame()

# ── Drill-through handoff: ?ind=X → seed the screener's Industry filter, then switch page.
# Mirrors the screener's ?ticker= row-click handoff (same-page query param → Python → switch_page).
_qp_ind = st.query_params.get("ind")
if _qp_ind:
    st.query_params.clear()
    st.session_state[INDUSTRY_KEY] = _qp_ind
    st.switch_page(SCREENER_PAGE)

# ── Masthead ──────────────────────────────────────────────────────────────────
st.markdown(
    '<div class="scr-masthead">'
    '<div class="scr-kicker"><span class="scr-hair"></span>'
    '<span class="scr-kick-txt">US Equities · by sub-sector</span>'
    '<span class="scr-hair"></span></div>'
    '<h1>Sector &amp; Industry Valuation</h1>'
    '<div class="scr-dateline"><span>Median multiples by industry group · latest FY · USD</span>'
    '</div></div>',
    unsafe_allow_html=True,
)

summary, info = industry_summary(wide)

if summary.empty:
    st.info(
        "Industry data isn't in the published artifacts yet — it arrives at schema v8 after the "
        "next config rebuild and republish. Until then every company is grouped as "
        "“Unknown”, so there's nothing to compare here."
    )
    st.stop()

val_cols = [c for c in VALUATION_COLS if c in summary.columns]
qual_cols = [c for c in QUALITY_COLS if c in summary.columns]
num_cols = val_cols + qual_cols

# ── Controls ──────────────────────────────────────────────────────────────────
ctrl, _spacer = st.columns([1.4, 3])
with ctrl:
    rank_metric = st.selectbox("Rank industries by", val_cols, index=0)  # default = P/E
st.caption(
    f"{info['n_industries']:,} industries with ≥ {info['min_count']} companies · "
    f"{info['hidden_small']:,} smaller groups hidden · {info['unknown']:,} companies Unknown · "
    "medians are ex-negatives for valuation multiples · sector = modal GICS within the group "
    "(Yahoo industry doesn't strictly nest under GICS sector)"
)

# Cheapest first for a valuation multiple (lower = cheaper); NaN sinks to the bottom.
disp = summary.sort_values(rank_metric, ascending=True, na_position="last").reset_index(drop=True)

# ── Table (st.html — no JS; chips reuse lib.signals thresholds, like the screener) ─────────────
_SIG_CHIP = {"good": "scr-chip-g", "warn": "scr-chip-a", "bad": "scr-chip-r"}


def _num_cell(col: str, val) -> str:
    if pd.isna(val):
        return '<td class="scr-na">—</td>'
    text = html.escape(fmt_metric(val, unit_map.get(col, "ratio")))
    chip = _SIG_CHIP.get(signal_absolute(col, val))
    inner = f'<span class="{chip}">{text}</span>' if chip else text
    return f"<td>{inner}</td>"


def _table_html() -> str:
    head = (
        ['<th class="scr-th-co">Industry</th>', "<th>Sector</th>", "<th>#</th>"]
        + [f"<th>{html.escape(c)}</th>" for c in num_cols]
    )
    body = []
    for rec in disp.to_dict("records"):
        ind = str(rec["industry"])
        sec = str(rec.get("sector") or "")
        cells = [
            f'<td class="scr-td-co"><a href="?ind={quote(ind)}" '
            f'title="Open {html.escape(ind)} in the screener">{html.escape(ind)}</a></td>',
            f"<td>{html.escape(sec)}</td>",
            f'<td>{int(rec["n"]):,}</td>',
        ]
        cells += [_num_cell(c, rec[c]) for c in num_cols]
        body.append("<tr>" + "".join(cells) + "</tr>")
    return (
        '<div class="scr-tbl-wrap"><table class="scr-tbl"><thead><tr>'
        + "".join(head) + "</tr></thead><tbody>"
        + "".join(body) + "</tbody></table></div>"
    )


if hasattr(st, "html"):
    st.html(_table_html())
else:
    st.dataframe(disp, hide_index=True, width="stretch")
