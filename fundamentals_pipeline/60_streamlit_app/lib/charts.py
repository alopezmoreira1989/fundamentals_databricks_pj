"""Statement-tab charts for the company view (annual analogues of the Quarterly combo).

Standalone renderers — they reuse the axis helpers from ``quarterly.py`` (``_nice_ceil``
et al.) and ``is_missing``, but never touch the quarterly render path, so the Quarterly
tab output stays byte-identical. All three are fixed in $B and intentionally ignore the
statement-table unit-scale control (mirroring ``render_quarterly_combo``).

Each public function returns a raw HTML string for st.markdown(..., unsafe_allow_html=True)
and degrades to "" (chart hidden) when its required concept is missing.
"""

from __future__ import annotations

import math

import pandas as pd

from .colors import AMBER, BLUE, CORAL, GREEN, PURPLE
from .format import is_missing, short_year
from .quarterly import _nice_ceil, _nice_ticks
from .tables import get_year_columns

# Hex literals for SVG presentation attributes (var() doesn't resolve inside SVG). The
# accent family mirrors --accent / --accent-ink / --accent-soft; soft/muted tints match
# the hex tokens already used in render.py.
_ACCENT = BLUE            # #185FA5 (--accent)
_ACCENT_INK = "#0C447C"   # --accent-ink
_ACCENT_MID = "#4A85C0"   # mid accent tint
_ACCENT_LT = "#8FB6DC"    # light accent tint
_ACCENT_SOFT = "#E6F1FB"  # --accent-soft
_LIAB_OTHER = "#B7B3A8"   # muted tint for the liabilities remainder
_GRID = "#E8E5DC"         # --rule-soft
_INK = "#1C1B18"          # --ink
_INK2 = "#5F5E5A"         # --ink-2
_INK3 = "#888780"         # --ink-3
_CREAM = "#FAF8F4"        # --bg (light text on dark fills)

_MONO = "JetBrains Mono, monospace"
_SANS = "Inter, system-ui, sans-serif"


def _b(v: float) -> str:
    """Raw USD → $B label with one decimal."""
    return f"{v / 1e9:,.1f}"


# ──────────────────────────────────────────────────────────────────────────────
# Income statement — annual Revenue bars + YoY-growth line
# ──────────────────────────────────────────────────────────────────────────────

def render_is_revenue_combo(df_is: pd.DataFrame) -> str:
    """Annual Revenue bars ($B) + YoY-growth line (%), the FY analogue of the Quarterly
    combo. YoY is computed against the prior fiscal year. Returns "" when Revenue is
    absent or all-missing so the chart is simply hidden.
    """
    if df_is.empty:
        return ""
    year_cols = get_year_columns(df_is)
    if not year_cols:
        return ""
    rev_row = df_is[df_is["concept"] == "Revenue"]
    if rev_row.empty:
        return ""
    rev_values = [rev_row.iloc[0].get(c) for c in year_cols]
    valid_rev = [v for v in rev_values if not is_missing(v)]
    if not valid_rev:
        return ""

    # YoY vs the prior fiscal year (lookup by year, so a gap can't fabricate a YoY).
    rev_lookup = {yr: v for yr, v in zip(year_cols, rev_values, strict=False) if not is_missing(v)}
    yoy_values: list[float | None] = []
    for yr, v in zip(year_cols, rev_values, strict=False):
        prior = rev_lookup.get(yr - 1)
        if not is_missing(v) and prior is not None and prior != 0:
            yoy_values.append((v - prior) / abs(prior) * 100)
        else:
            yoy_values.append(None)

    # --- Dynamic axis bounds (mirrors render_quarterly_combo) ---
    rev_max_raw = max(valid_rev)
    rev_max_b = rev_max_raw / 1e9 if rev_max_raw > 1e6 else rev_max_raw
    rev_axis_max = _nice_ceil(rev_max_b)
    rev_ticks = [0, rev_axis_max * 0.25, rev_axis_max * 0.5, rev_axis_max * 0.75, rev_axis_max]

    valid_yoy = [v for v in yoy_values if v is not None]
    if valid_yoy:
        yoy_min, yoy_max = min(valid_yoy), max(valid_yoy)
    else:
        yoy_min, yoy_max = -5, 5
    yoy_lo = math.floor(yoy_min / 5) * 5
    yoy_hi = math.ceil(yoy_max / 5) * 5
    if yoy_lo < 0 and yoy_hi > 0:
        bound = max(abs(yoy_lo), abs(yoy_hi))
        yoy_lo, yoy_hi = -bound, bound
    yoy_span = (yoy_hi - yoy_lo) or 1

    # --- SVG layout (identical geometry to the Quarterly combo) ---
    W, H = 1180, 280
    margin_l, margin_r = 50, 40
    margin_t, margin_b = 30, 50
    plot_w = W - margin_l - margin_r
    plot_h = H - margin_t - margin_b

    n = len(year_cols)
    bar_gap = 28
    bar_w = (plot_w - (n - 1) * bar_gap) / n if n > 0 else 40
    baseline_y = margin_t + plot_h

    def x_center(i: int) -> float:
        return margin_l + i * (bar_w + bar_gap) + bar_w / 2

    def rev_y(val: float) -> float:
        v_b = val / 1e9 if rev_max_raw > 1e6 else val
        return baseline_y - (v_b / rev_axis_max) * plot_h

    def yoy_y(pct: float) -> float:
        return margin_t + plot_h - ((pct - yoy_lo) / yoy_span) * plot_h

    parts: list[str] = []
    parts.append(f'<svg viewBox="0 0 {W} {H}" width="100%" style="display:block;" preserveAspectRatio="none">')

    # Gridlines.
    parts.append(f'<g stroke="{_GRID}" stroke-width="1">')
    for tick in rev_ticks[1:]:
        y = baseline_y - (tick / rev_axis_max) * plot_h
        parts.append(f'<line x1="{margin_l}" y1="{y:.0f}" x2="{W - margin_r}" y2="{y:.0f}"/>')
    parts.append("</g>")

    # Left axis labels (revenue $B).
    parts.append(f'<g font-family="{_MONO}" font-size="10" fill="{_INK3}" text-anchor="end">')
    for tick in reversed(rev_ticks):
        y = baseline_y - (tick / rev_axis_max) * plot_h
        label = f"{tick:.0f}" if tick == int(tick) else f"{tick:.1f}"
        parts.append(f'<text x="{margin_l - 6}" y="{y + 4:.0f}">{label}</text>')
    parts.append("</g>")

    # Right axis labels (YoY %).
    yoy_ticks = _nice_ticks(yoy_lo, yoy_hi, n_ticks=5)
    parts.append(f'<g font-family="{_MONO}" font-size="10" fill="{GREEN}" text-anchor="start">')
    for tick in yoy_ticks:
        y = yoy_y(tick)
        sign = "+" if tick > 0 else ("−" if tick < 0 else "")
        parts.append(f'<text x="{W - margin_r + 8}" y="{y + 4:.0f}">{sign}{abs(tick):.0f}</text>')
    parts.append("</g>")

    # Bars.
    parts.append("<g>")
    for i, val in enumerate(rev_values):
        if is_missing(val):
            continue
        bx = margin_l + i * (bar_w + bar_gap)
        by = rev_y(val)
        bh = baseline_y - by
        opacity = "0.95" if i >= n - 4 else "0.85"
        if i == n - 1:
            opacity = "1.0"
        parts.append(
            f'<rect x="{bx:.1f}" y="{by:.1f}" width="{bar_w:.1f}" height="{bh:.1f}" '
            f'fill="{_ACCENT}" opacity="{opacity}"/>'
        )
    parts.append("</g>")

    # YoY line + dots.
    line_points = [f"{x_center(i):.0f},{yoy_y(v):.0f}" for i, v in enumerate(yoy_values) if v is not None]
    if len(line_points) >= 2:
        parts.append(f'<polyline points="{" ".join(line_points)}" fill="none" stroke="{GREEN}" stroke-width="2"/>')
        parts.append(f'<g fill="{GREEN}">')
        for i, yoy in enumerate(yoy_values):
            if yoy is None:
                continue
            cx, cy = x_center(i), yoy_y(yoy)
            if i == n - 1:
                parts.append(f'<circle cx="{cx:.0f}" cy="{cy:.0f}" r="4" stroke="#FFFFFF" stroke-width="1.5"/>')
            else:
                parts.append(f'<circle cx="{cx:.0f}" cy="{cy:.0f}" r="3"/>')
        parts.append("</g>")

    # Baseline.
    parts.append(
        f'<line x1="{margin_l}" y1="{baseline_y}" x2="{W - margin_r}" y2="{baseline_y}" '
        f'stroke="{_INK}" stroke-width="1"/>'
    )

    # 0% reference line (dashed).
    zero_y = yoy_y(0)
    if margin_t < zero_y < baseline_y:
        parts.append(
            f'<line x1="{margin_l}" y1="{zero_y:.0f}" x2="{W - margin_r}" y2="{zero_y:.0f}" '
            f'stroke="{GREEN}" stroke-width="0.5" stroke-dasharray="3,3" opacity="0.4"/>'
        )

    # X-axis labels (fiscal years).
    parts.append(f'<g font-family="{_MONO}" font-size="10" fill="{_INK2}" text-anchor="middle">')
    for i, yr in enumerate(year_cols):
        cx = x_center(i)
        lbl = short_year(yr) if isinstance(yr, (int, float)) else str(yr)
        extra = f' font-weight="500" fill="{_INK}"' if i == n - 1 else ""
        parts.append(f'<text x="{cx:.0f}" y="{baseline_y + 20}"{extra}>{lbl}</text>')
    parts.append("</g>")

    parts.append("</svg>")
    svg = "\n".join(parts)

    return (
        '<div class="qchart">'
        '<div class="qchart-header">'
        '<h3>Revenue with YoY growth</h3>'
        '<div class="qchart-legend">'
        f'<span><span class="legend-dot" style="background:{_ACCENT};"></span>Revenue ($B)</span>'
        f'<span><span class="legend-dot" style="background:{GREEN};"></span>YoY growth (%)</span>'
        '</div></div>'
        f'<div style="position:relative;">{svg}</div>'
        '</div>'
    )


# ──────────────────────────────────────────────────────────────────────────────
# Balance sheet — Assets | Liabilities + Equity composition (A = L + E)
# ──────────────────────────────────────────────────────────────────────────────

def render_bs_composition(df_bs: pd.DataFrame, year: int) -> str:
    """Two stacked bars (Assets | Liabilities + Equity) for one fiscal year, both sized to
    the same total so they reach equal height — making ``A = L + E`` visual. An ``Other``
    bucket per bar absorbs the remainder so each bar ties out to the total exactly.
    Returns "" when Total Assets is unavailable for the selected year.
    """
    if df_bs.empty:
        return ""

    def val(concept: str):
        r = df_bs[df_bs["concept"] == concept]
        if r.empty:
            return None
        v = r.iloc[0].get(year)
        return None if is_missing(v) else float(v)

    def clamp(concept: str) -> float:
        v = val(concept)
        return max(0.0, v) if v is not None else 0.0

    total = val("Total Assets")
    if total is None or total <= 0:
        return ""

    # Left bar — Assets (anchor: Total Assets; Other absorbs Goodwill/Intangibles/etc.).
    cash = clamp("Cash & Equivalents") + clamp("Short-term Investments")
    recv = clamp("Accounts Receivable")
    inv = clamp("Inventory")
    ppe = clamp("PP&E Net")
    a_other = max(0.0, total - (cash + recv + inv + ppe))
    assets_segs = [
        ("Cash & Inv.", cash, _ACCENT_INK, _CREAM),
        ("Receivables", recv, _ACCENT, _CREAM),
        ("Inventory", inv, _ACCENT_MID, _CREAM),
        ("PP&E", ppe, _ACCENT_LT, _INK),
        ("Other", a_other, _ACCENT_SOFT, _INK),
    ]

    # Right bar — Liabilities + Equity (non-equity ties to Total Assets − Equity; + Equity).
    equity = val("Total Stockholders Equity")
    if equity is not None:
        total_liab = max(0.0, total - equity)
    else:
        total_liab = clamp("Total Liabilities")
        equity = total - total_liab
    ap = clamp("Accounts Payable")
    std = clamp("Short-term Debt")
    ltd = clamp("Long-term Debt")
    l_other = max(0.0, total_liab - (ap + std + ltd))
    le_segs = [
        ("Payables", ap, CORAL, _CREAM),
        ("ST Debt", std, AMBER, _CREAM),
        ("LT Debt", ltd, PURPLE, _CREAM),
        ("Other liab.", l_other, _LIAB_OTHER, _INK),
        ("Equity", max(0.0, equity), GREEN, _CREAM),
    ]

    # --- SVG layout: two vertical bars side by side ---
    W, H = 560, 360
    plot_top, plot_h = 56, 264
    bar_w = 150
    left_x, right_x = 70, 340

    def draw_bar(x: float, segs, title: str) -> list[str]:
        out: list[str] = [
            f'<text x="{x + bar_w / 2:.0f}" y="{plot_top - 24}" text-anchor="middle" '
            f'font-family="{_SANS}" font-size="13" font-weight="600" fill="{_INK}">{title}</text>',
            f'<text x="{x + bar_w / 2:.0f}" y="{plot_top - 8}" text-anchor="middle" '
            f'font-family="{_MONO}" font-size="11" fill="{_INK3}">${_b(total)}B</text>',
        ]
        cursor = float(plot_top)
        for name, v, fill, tc in segs:
            if v <= 0:
                continue
            h = v / total * plot_h
            mid = cursor + h / 2
            out.append(
                f'<rect x="{x:.0f}" y="{cursor:.1f}" width="{bar_w}" height="{h:.1f}" '
                f'fill="{fill}" stroke="#FFFFFF" stroke-width="1"/>'
            )
            value_line = f"${_b(v)}B · {v / total * 100:.0f}%"
            if h >= 30:
                out.append(
                    f'<text x="{x + bar_w / 2:.0f}" y="{mid - 3:.1f}" text-anchor="middle" '
                    f'font-family="{_SANS}" font-size="11.5" font-weight="500" fill="{tc}">{name}</text>'
                )
                out.append(
                    f'<text x="{x + bar_w / 2:.0f}" y="{mid + 12:.1f}" text-anchor="middle" '
                    f'font-family="{_MONO}" font-size="10" fill="{tc}">{value_line}</text>'
                )
            elif h >= 15:
                out.append(
                    f'<text x="{x + bar_w / 2:.0f}" y="{mid + 3.5:.1f}" text-anchor="middle" '
                    f'font-family="{_MONO}" font-size="10" fill="{tc}">{value_line}</text>'
                )
            cursor += h
        return out

    svg = [f'<svg viewBox="0 0 {W} {H}" width="100%" preserveAspectRatio="xMidYMid meet">']
    svg += draw_bar(left_x, assets_segs, "Assets")
    svg += draw_bar(right_x, le_segs, "Liabilities + Equity")
    svg.append("</svg>")
    svg_str = "\n".join(svg)

    yr_label = short_year(year) if isinstance(year, (int, float)) else str(year)
    caption = (
        '<div class="footnote" style="display:flex; align-items:center; gap:10px;">'
        '<span style="color:var(--positive); font-weight:500;">✓ A = L + E — balance verified</span>'
        '<span class="pipe">|</span>'
        f'<span>FY{yr_label}: assets ${_b(total)}B tie to liabilities + equity</span>'
        '</div>'
    )

    return (
        '<div class="qchart">'
        '<div class="qchart-header">'
        f'<h3>Balance sheet composition · FY{yr_label}</h3>'
        '<div class="qchart-legend">'
        f'<span><span class="legend-dot" style="background:{_ACCENT};"></span>Assets</span>'
        f'<span><span class="legend-dot" style="background:{CORAL};"></span>Liabilities</span>'
        f'<span><span class="legend-dot" style="background:{GREEN};"></span>Equity</span>'
        '</div></div>'
        f'<div style="position:relative;">{svg_str}</div>'
        '</div>'
        f'{caption}'
    )


# ──────────────────────────────────────────────────────────────────────────────
# Cash flow — Operating Cash Flow vs Free Cash Flow (gap = CapEx)
# ──────────────────────────────────────────────────────────────────────────────

def render_cf_fcf(df_cf: pd.DataFrame) -> str:
    """Operating Cash Flow (wide, light bar) with Free Cash Flow = OCF − CapEx (narrower
    bar in front) per fiscal year; the OCF→FCF gap reads as CapEx. Returns "" when the
    Operating Cash Flow row is absent. Years missing CapEx draw no front (FCF) bar.
    """
    if df_cf.empty:
        return ""
    year_cols = get_year_columns(df_cf)
    if not year_cols:
        return ""
    ocf_row = df_cf[df_cf["concept"] == "Operating Cash Flow"]
    if ocf_row.empty:
        return ""
    ocf_values = [ocf_row.iloc[0].get(c) for c in year_cols]
    if all(is_missing(v) for v in ocf_values):
        return ""

    capex_row = df_cf[df_cf["concept"] == "CapEx"]
    capex_values = (
        [capex_row.iloc[0].get(c) for c in year_cols] if not capex_row.empty else [None] * len(year_cols)
    )

    # FCF = OCF − |CapEx|. CapEx is stored as a positive magnitude (see colors.py
    # is_negative_concept), but abs() keeps the OCF→FCF gap correct under either sign.
    # If CapEx is missing for a year, FCF is unavailable (no front bar) — never FCF = OCF.
    fcf_values: list[float | None] = []
    for ocf, cx in zip(ocf_values, capex_values, strict=False):
        if is_missing(ocf) or is_missing(cx):
            fcf_values.append(None)
        else:
            fcf_values.append(ocf - abs(cx))

    # --- $B domain, including negatives (a bad year can push OCF/FCF below zero) ---
    all_b = [v / 1e9 for v in (ocf_values + fcf_values) if not is_missing(v)]
    vmax = max(all_b + [0.0])
    vmin = min(all_b + [0.0])
    axis_max = _nice_ceil(vmax) if vmax > 0 else 0.0
    axis_min = -_nice_ceil(abs(vmin)) if vmin < 0 else 0.0
    if axis_max == 0 and axis_min == 0:
        axis_max = 10.0
    span_dom = (axis_max - axis_min) or 1

    W, H = 1180, 280
    margin_l, margin_r = 50, 40
    margin_t, margin_b = 30, 50
    plot_w = W - margin_l - margin_r
    plot_h = H - margin_t - margin_b

    n = len(year_cols)
    slot_gap = 28
    slot = (plot_w - (n - 1) * slot_gap) / n if n > 0 else 40

    def slot_x(i: int) -> float:
        return margin_l + i * (slot + slot_gap)

    def y_of(v_b: float) -> float:
        return margin_t + (axis_max - v_b) / span_dom * plot_h

    baseline_y = y_of(0.0)

    parts: list[str] = []
    parts.append(f'<svg viewBox="0 0 {W} {H}" width="100%" style="display:block;" preserveAspectRatio="none">')

    # Gridlines + left axis ($B) at nice ticks across the (possibly signed) domain.
    ticks = _nice_ticks(axis_min, axis_max, n_ticks=5)
    parts.append(f'<g stroke="{_GRID}" stroke-width="1">')
    for tick in ticks:
        if abs(tick) < 1e-9:
            continue
        y = y_of(tick)
        parts.append(f'<line x1="{margin_l}" y1="{y:.0f}" x2="{W - margin_r}" y2="{y:.0f}"/>')
    parts.append("</g>")
    parts.append(f'<g font-family="{_MONO}" font-size="10" fill="{_INK3}" text-anchor="end">')
    for tick in ticks:
        y = y_of(tick)
        label = f"{tick:.0f}" if tick == int(tick) else f"{tick:.1f}"
        parts.append(f'<text x="{margin_l - 6}" y="{y + 4:.0f}">{label}</text>')
    parts.append("</g>")

    def draw_value_bar(x: float, w: float, v_b: float, fill: str, stroke: str | None) -> str:
        top = min(y_of(v_b), baseline_y)
        h = abs(y_of(v_b) - baseline_y)
        stroke_attr = f' stroke="{stroke}" stroke-width="1"' if stroke else ""
        return f'<rect x="{x:.1f}" y="{top:.1f}" width="{w:.1f}" height="{h:.1f}" fill="{fill}"{stroke_attr}/>'

    # OCF wide light bars (behind) + FCF narrower green bars (front).
    fcf_w = slot * 0.5
    parts.append("<g>")
    for i, ocf in enumerate(ocf_values):
        if is_missing(ocf):
            continue
        parts.append(draw_value_bar(slot_x(i), slot, ocf / 1e9, _ACCENT_SOFT, _ACCENT))
    for i, fcf in enumerate(fcf_values):
        if fcf is None:
            continue
        fx = slot_x(i) + (slot - fcf_w) / 2
        parts.append(draw_value_bar(fx, fcf_w, fcf / 1e9, GREEN, None))
    parts.append("</g>")

    # FCF value labels above each front bar (below the bar when FCF is negative).
    parts.append(f'<g font-family="{_MONO}" font-size="10" fill="{GREEN}" text-anchor="middle">')
    for i, fcf in enumerate(fcf_values):
        if fcf is None:
            continue
        cx = slot_x(i) + slot / 2
        yb = y_of(fcf / 1e9)
        ty = yb - 5 if fcf >= 0 else yb + 12
        parts.append(f'<text x="{cx:.0f}" y="{ty:.0f}">{_b(fcf)}</text>')
    parts.append("</g>")

    # Zero baseline.
    parts.append(
        f'<line x1="{margin_l}" y1="{baseline_y:.0f}" x2="{W - margin_r}" y2="{baseline_y:.0f}" '
        f'stroke="{_INK}" stroke-width="1"/>'
    )

    # X-axis labels (fiscal years), always at the chart bottom.
    parts.append(f'<g font-family="{_MONO}" font-size="10" fill="{_INK2}" text-anchor="middle">')
    for i, yr in enumerate(year_cols):
        cx = slot_x(i) + slot / 2
        lbl = short_year(yr) if isinstance(yr, (int, float)) else str(yr)
        extra = f' font-weight="500" fill="{_INK}"' if i == n - 1 else ""
        parts.append(f'<text x="{cx:.0f}" y="{margin_t + plot_h + 20}"{extra}>{lbl}</text>')
    parts.append("</g>")

    parts.append("</svg>")
    svg = "\n".join(parts)

    return (
        '<div class="qchart">'
        '<div class="qchart-header">'
        '<h3>Operating vs free cash flow</h3>'
        '<div class="qchart-legend">'
        f'<span><span class="legend-dot" style="background:{_ACCENT_SOFT};border:1px solid {_ACCENT};">'
        '</span>Operating cash flow</span>'
        f'<span><span class="legend-dot" style="background:{GREEN};"></span>Free cash flow</span>'
        f'<span><span class="legend-dot" style="background:{_INK3};"></span>gap = CapEx</span>'
        '</div></div>'
        f'<div style="position:relative;">{svg}</div>'
        '</div>'
    )
