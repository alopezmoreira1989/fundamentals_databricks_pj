"""Quarterly combo chart — revenue bars + YoY growth line, dynamic axes."""

from __future__ import annotations

import math

import pandas as pd

from .format import is_missing


def render_quarterly_combo(df_q: pd.DataFrame) -> str:
    """Return the combo chart as a raw SVG wrapped in a .qchart container.

    `df_q` is the wide quarterly dataframe produced by tables.quarterly_df().
    Expects columns like "'22-Q1" ... "'24-Q4" in chronological order.
    """
    if df_q.empty:
        return '<div class="qchart"><p style="color:var(--ink-3)">No quarterly data.</p></div>'

    # Extract Revenue row and compute YoY growth.
    meta_cols = {"stmt", "section", "group", "concept", "display_name", "sort_order", "indent", "row_class"}
    q_cols = [c for c in df_q.columns if c not in meta_cols]

    rev_row = df_q[df_q["concept"] == "Revenue"]
    if rev_row.empty:
        return '<div class="qchart"><p style="color:var(--ink-3)">Revenue not available.</p></div>'

    rev_values = [rev_row.iloc[0].get(c) for c in q_cols]

    # Parse quarter labels to compute YoY (same quarter prior year).
    # Label format: "'YY-Qn" — so "'22-Q1" maps to year=2022, quarter=Q1.
    def parse_q_label(lbl: str) -> tuple[int, str]:
        parts = lbl.split("-")
        yr = 2000 + int(parts[0].replace("'", ""))
        qt = parts[1]
        return (yr, qt)

    q_meta = [parse_q_label(c) for c in q_cols]

    # Build a lookup: (year, quarter) → revenue value.
    rev_lookup: dict[tuple[int, str], float] = {}
    for (yr, qt), val in zip(q_meta, rev_values, strict=False):
        if not is_missing(val):
            rev_lookup[(yr, qt)] = val

    yoy_values: list[float | None] = []
    for (yr, qt), val in zip(q_meta, rev_values, strict=False):
        prior = rev_lookup.get((yr - 1, qt))
        if not is_missing(val) and prior is not None and prior != 0:
            yoy_values.append((val - prior) / abs(prior) * 100)
        else:
            yoy_values.append(None)

    # --- Dynamic axis bounds ---
    valid_rev = [v for v in rev_values if not is_missing(v)]
    if not valid_rev:
        return '<div class="qchart"><p style="color:var(--ink-3)">Revenue not available.</p></div>'

    rev_max_raw = max(valid_rev)
    # Scale to billions for the axis labels.
    rev_max_b = rev_max_raw / 1e9 if rev_max_raw > 1e6 else rev_max_raw
    # Round up to a "nice" number for the left axis.
    rev_axis_max = _nice_ceil(rev_max_b)
    # Tick marks: 0, 25%, 50%, 75%, 100% of axis_max.
    rev_ticks = [0, rev_axis_max * 0.25, rev_axis_max * 0.5, rev_axis_max * 0.75, rev_axis_max]

    valid_yoy = [v for v in yoy_values if v is not None]
    if valid_yoy:
        yoy_min = min(valid_yoy)
        yoy_max = max(valid_yoy)
    else:
        yoy_min, yoy_max = -5, 5
    # Extend to nice bounds and make symmetric around 0 if both signs present.
    yoy_lo = _nice_floor(yoy_min, step=5)
    yoy_hi = _nice_ceil_step(yoy_max, step=5)
    if yoy_lo < 0 and yoy_hi > 0:
        bound = max(abs(yoy_lo), abs(yoy_hi))
        yoy_lo, yoy_hi = -bound, bound
    yoy_span = yoy_hi - yoy_lo or 1

    # --- SVG layout ---
    W, H = 1180, 280
    margin_l, margin_r = 50, 40
    margin_t, margin_b = 30, 50
    plot_w = W - margin_l - margin_r
    plot_h = H - margin_t - margin_b

    n = len(q_cols)
    bar_gap = 24
    bar_w = (plot_w - (n - 1) * bar_gap) / n if n > 0 else 40
    baseline_y = margin_t + plot_h

    def x_center(i: int) -> float:
        return margin_l + i * (bar_w + bar_gap) + bar_w / 2

    def rev_y(val: float) -> float:
        v_b = val / 1e9 if rev_max_raw > 1e6 else val
        return baseline_y - (v_b / rev_axis_max) * plot_h

    def yoy_y(pct: float) -> float:
        return margin_t + plot_h - ((pct - yoy_lo) / yoy_span) * plot_h

    # --- Build SVG ---
    parts: list[str] = []
    parts.append(f'<svg viewBox="0 0 {W} {H}" width="100%" style="display:block;" preserveAspectRatio="none">')

    # Gridlines.
    parts.append('<g stroke="#E8E5DC" stroke-width="1">')
    for tick in rev_ticks[1:]:
        y = baseline_y - (tick / rev_axis_max) * plot_h
        parts.append(f'<line x1="{margin_l}" y1="{y:.0f}" x2="{W - margin_r}" y2="{y:.0f}"/>')
    parts.append("</g>")

    # Left axis labels (revenue $B).
    parts.append('<g font-family="JetBrains Mono, monospace" font-size="10" fill="#888780" text-anchor="end">')
    for tick in reversed(rev_ticks):
        y = baseline_y - (tick / rev_axis_max) * plot_h
        label = f"{tick:.0f}" if tick == int(tick) else f"{tick:.1f}"
        parts.append(f'<text x="{margin_l - 6}" y="{y + 4:.0f}">{label}</text>')
    parts.append("</g>")

    # Right axis labels (YoY %).
    yoy_ticks = _nice_ticks(yoy_lo, yoy_hi, n_ticks=5)
    parts.append('<g font-family="JetBrains Mono, monospace" font-size="10" fill="#0F6E56" text-anchor="start">')
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
        # Opacity: latest 4 quarters brighter.
        opacity = "0.95" if i >= n - 4 else "0.85"
        if i == n - 1:
            opacity = "1.0"
        parts.append(
            f'<rect x="{bx:.1f}" y="{by:.1f}" width="{bar_w:.1f}" height="{bh:.1f}" '
            f'fill="#185FA5" opacity="{opacity}"/>'
        )
    parts.append("</g>")

    # YoY line.
    line_points: list[str] = []
    for i, yoy in enumerate(yoy_values):
        if yoy is not None:
            line_points.append(f"{x_center(i):.0f},{yoy_y(yoy):.0f}")
    if len(line_points) >= 2:
        parts.append(
            f'<polyline points="{" ".join(line_points)}" fill="none" stroke="#0F6E56" stroke-width="2"/>'
        )
        # Dots.
        parts.append('<g fill="#0F6E56">')
        for i, yoy in enumerate(yoy_values):
            if yoy is None:
                continue
            cx = x_center(i)
            cy = yoy_y(yoy)
            if i == n - 1:
                parts.append(f'<circle cx="{cx:.0f}" cy="{cy:.0f}" r="4" stroke="#FFFFFF" stroke-width="1.5"/>')
            else:
                parts.append(f'<circle cx="{cx:.0f}" cy="{cy:.0f}" r="3"/>')
        parts.append("</g>")

    # Baseline.
    parts.append(f'<line x1="{margin_l}" y1="{baseline_y}" x2="{W - margin_r}" y2="{baseline_y}" stroke="#1C1B18" stroke-width="1"/>')

    # 0% reference line (dashed).
    zero_y = yoy_y(0)
    if margin_t < zero_y < baseline_y:
        parts.append(
            f'<line x1="{margin_l}" y1="{zero_y:.0f}" x2="{W - margin_r}" y2="{zero_y:.0f}" '
            f'stroke="#0F6E56" stroke-width="0.5" stroke-dasharray="3,3" opacity="0.4"/>'
        )

    # X-axis labels.
    parts.append('<g font-family="JetBrains Mono, monospace" font-size="10" fill="#5F5E5A" text-anchor="middle">')
    for i, lbl in enumerate(q_cols):
        cx = x_center(i)
        extra = ' font-weight="500" fill="#1C1B18"' if i == n - 1 else ""
        parts.append(f'<text x="{cx:.0f}" y="{baseline_y + 20}"{extra}>{lbl}</text>')
    parts.append("</g>")

    parts.append("</svg>")
    svg = "\n".join(parts)

    # Wrap in the chart container.
    return (
        '<div class="qchart">'
        '<div class="qchart-header">'
        '<h3>Quarterly revenue with YoY growth</h3>'
        '<div class="qchart-legend">'
        '<span><span class="legend-dot" style="background:#185FA5;"></span>Revenue ($B)</span>'
        '<span><span class="legend-dot" style="background:#0F6E56;"></span>YoY growth (%)</span>'
        '</div></div>'
        f'<div style="position:relative;">{svg}</div>'
        '</div>'
    )


def _nice_ceil(val: float) -> float:
    """Round up to a 'nice' chart bound (multiples of 10/30/50)."""
    if val <= 0:
        return 10
    magnitude = 10 ** math.floor(math.log10(val))
    residual = val / magnitude
    if residual <= 1.5:
        nice = 1.5 * magnitude
    elif residual <= 2:
        nice = 2 * magnitude
    elif residual <= 3:
        nice = 3 * magnitude
    elif residual <= 5:
        nice = 5 * magnitude
    elif residual <= 7.5:
        nice = 7.5 * magnitude
    else:
        nice = 10 * magnitude
    return nice


def _nice_ceil_step(val: float, step: float = 5) -> float:
    return math.ceil(val / step) * step


def _nice_floor(val: float, step: float = 5) -> float:
    return math.floor(val / step) * step


def _nice_ticks(lo: float, hi: float, n_ticks: int = 5) -> list[float]:
    span = hi - lo
    step = span / (n_ticks - 1) if n_ticks > 1 else span
    return [lo + i * step for i in range(n_ticks)]
