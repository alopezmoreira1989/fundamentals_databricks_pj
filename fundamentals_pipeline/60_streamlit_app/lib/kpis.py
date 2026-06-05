"""KPI strip — 4-column grid showing latest-FY headline values + YoY + CAGR."""

from __future__ import annotations

import pandas as pd

from .format import fmt_cagr, fmt_delta, fmt_kpi, is_missing

# The four KPIs to display (concept name, source statement).
_KPI_DEFS = [
    ("Revenue",             "Income Statement"),
    ("Net Income",          "Income Statement"),
    ("Operating Cash Flow", "Cash Flow"),
    ("Total Assets",        "Balance Sheet"),
]


def render_kpi_strip(ticker: str, data: pd.DataFrame, metrics: pd.DataFrame) -> str:
    """Return the full KPI strip as an HTML string."""
    fy = data[(data["ticker"] == ticker) & (data["period_type"] == "FY")]

    cards_html: list[str] = []
    for concept, stmt in _KPI_DEFS:
        series = (
            fy[(fy["concept"] == concept) & (fy["stmt"] == stmt)]
            .sort_values("fiscal_year")
        )
        if series.empty:
            cards_html.append(_empty_card(concept))
            continue

        latest_val = series.iloc[-1]["value"]
        years = series["fiscal_year"].tolist()
        values = series["value"].tolist()

        # YoY %
        yoy = None
        if len(values) >= 2 and not is_missing(values[-1]) and not is_missing(values[-2]) and values[-2] != 0:
            yoy = (values[-1] - values[-2]) / abs(values[-2]) * 100

        # CAGR
        first_val = next((v for v in values if not is_missing(v)), None)
        last_val = latest_val
        n_years = len(values) - 1 if len(values) > 1 else 0
        cagr_label, _ = fmt_cagr(first_val, last_val, n_years)

        delta_label, delta_cls = fmt_delta(yoy)
        value_str = fmt_kpi(latest_val)
        label = concept.upper().replace("OPERATING CASH FLOW", "OPERATING CF")

        cards_html.append(
            f'<div class="kpi">'
            f'  <div class="label">{label}</div>'
            f'  <div class="value">{value_str}</div>'
            f'  <div class="delta {delta_cls}">{delta_label}  ·  {cagr_label} CAGR ({n_years}y)</div>'
            f'</div>'
        )

    return f'<div class="kpi-strip">{"".join(cards_html)}</div>'


def _empty_card(concept: str) -> str:
    label = concept.upper()
    return (
        f'<div class="kpi">'
        f'  <div class="label">{label}</div>'
        f'  <div class="value">—</div>'
        f'  <div class="delta flat">no data</div>'
        f'</div>'
    )
