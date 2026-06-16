"""Statement-tab charts for the company view (annual analogues of the Quarterly combo).

Standalone renderers — they reuse the axis helpers from ``quarterly.py`` (``_nice_ceil``
et al.) and ``is_missing``, but never touch the quarterly render path, so the Quarterly
tab output stays byte-identical. All three are fixed in $B and intentionally ignore the
statement-table unit-scale control (mirroring ``render_quarterly_combo``).

Each public function returns a raw HTML string for st.markdown(..., unsafe_allow_html=True)
and degrades to "" (chart hidden) when its required concept is missing.
"""

from __future__ import annotations

import html
import math

import pandas as pd

from .colors import BLUE, GREEN
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
# Balance sheet — liquidity-ramp composition (family → current/non-current → line item)
# ──────────────────────────────────────────────────────────────────────────────
#
# Two equal-height stacked bars (Assets | Liabilities + Equity) on the left and a
# hierarchical legend on the right, laid out with a CSS grid (align-items:stretch lets
# the bars fill the taller legend column, so A = L + E is visual with no dead space).
# Colour encodes liquidity: a dark→light ramp per family (dark = most liquid). The bars
# are flex columns; each segment's flex-grow is its $-value, so both stacks — sharing the
# same Total Assets total — render at identical scale. See the .bs-* classes in styles.css.

# Liquidity ramp endpoints — on-family extensions of the accent / coral palette
# (dark = most liquid, light = most immobilized); equity is the solid positive green.
_ASSET_DARK, _ASSET_LIGHT = "#0E3D6B", "#C9DBED"
_LIAB_DARK, _LIAB_LIGHT = "#7A2415", "#E2B0A4"
_EQUITY = GREEN  # #0F6E56 (--positive)

# Fixed liquidity rank per subgroup: (display label, concept). Order is liquidity, never
# value — the item cap below picks the largest 5, but they stay in this order on the bar.
_ASSET_CURRENT = [
    ("Cash & equivalents", "Cash & Equivalents"),
    ("ST investments", "Short-term Investments"),
    ("Receivables", "Accounts Receivable"),
    ("Inventory", "Inventory"),
]
_ASSET_NONCURRENT = [
    ("PP&E", "PP&E Net"),
    ("Goodwill", "Goodwill"),
    ("Intangibles", "Intangible Assets"),
]
_LIAB_CURRENT = [
    ("Payables", "Accounts Payable"),
    ("ST debt", "Short-term Debt"),
]
_LIAB_NONCURRENT = [
    ("LT debt", "Long-term Debt"),
]

_SEG_CAP = 5          # max line items kept per subgroup (remainder folds into Other)
_EPS = 1e6            # $-threshold below which an Other plug is treated as zero


def _hex_to_rgb(h: str) -> tuple[int, int, int]:
    h = h.lstrip("#")
    return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)


def _lerp_hex(c0: str, c1: str, t: float) -> str:
    a, b = _hex_to_rgb(c0), _hex_to_rgb(c1)
    rgb = tuple(round(a[i] + (b[i] - a[i]) * t) for i in range(3))
    return f"#{rgb[0]:02X}{rgb[1]:02X}{rgb[2]:02X}"


def _ramp(c0: str, c1: str, n: int) -> list[str]:
    """n colours from dark c0 to light c1 (single colour → c0)."""
    if n <= 1:
        return [c0] * max(n, 0)
    return [_lerp_hex(c0, c1, i / (n - 1)) for i in range(n)]


def _ink_for(hex_color: str) -> str:
    """Dark ink on light shades, cream on dark — so in-bar labels stay readable."""
    r, g, b = _hex_to_rgb(hex_color)
    lum = (0.299 * r + 0.587 * g + 0.114 * b) / 255
    return _INK if lum > 0.6 else _CREAM


def render_bs_composition(df_bs: pd.DataFrame, year: int) -> str:
    """Liquidity-ramp balance-sheet composition for one fiscal year.

    Three-level hierarchy (family → current/non-current → line item), colour-encoding
    liquidity (dark = most liquid). Two equal-height bars (Assets | Liabilities + Equity)
    on the left, a hierarchical legend with every subtotal on the right. Each group's
    ``Other …`` = subtotal − Σ(identified line items) so every bar ties out exactly to
    Total Assets. Filers without a current/non-current split (banks, insurers) render a
    single liquidity ramp per side. Returns "" when Total Assets is missing for the year.
    """
    if df_bs.empty:
        return ""

    def val(concept: str):
        r = df_bs[df_bs["concept"] == concept]
        if r.empty:
            return None
        v = r.iloc[0].get(year)
        return None if is_missing(v) else float(v)

    total = val("Total Assets")
    if total is None or total <= 0:
        return ""

    def build_subgroup(name, items, subtotal: float) -> dict:
        """Identified line items (clamped ≥0, capped at 5 by abs value) + an Other plug so
        the subgroup sums exactly to ``subtotal``. Items stay in liquidity order."""
        subtotal = max(0.0, subtotal)
        present = [(lbl, max(0.0, v)) for lbl, c in items if (v := val(c)) is not None and v > 0]
        if len(present) > _SEG_CAP:
            keep = {lbl for lbl, _ in sorted(present, key=lambda x: x[1], reverse=True)[:_SEG_CAP]}
            present = [(lbl, v) for lbl, v in present if lbl in keep]
        segs = [{"name": lbl, "value": v} for lbl, v in present]
        other = subtotal - sum(s["value"] for s in segs)
        if other > _EPS:
            label = f"Other {name.lower()}" if name else "Other"
            segs.append({"name": label, "value": other})
        return {"name": name, "subtotal": subtotal, "segs": segs}

    def assign_ramp(subgroups: list[dict], dark: str, light: str) -> list[dict]:
        """Colour every visible segment across a family's subgroups along one dark→light
        ramp, and mark the first segment of the second subgroup as the current/non-current
        boundary (drawn 2px in CSS)."""
        flat = [s for sg in subgroups for s in sg["segs"]]
        ramp = _ramp(dark, light, len(flat))
        for color, s in zip(ramp, flat, strict=False):
            s["color"] = color
            s["ink"] = _ink_for(color)
        for sg in subgroups[1:]:
            if sg["segs"]:
                sg["segs"][0]["boundary"] = True
                break
        return flat

    tca, tcl = val("Total Current Assets"), val("Total Current Liabilities")
    classified = tca is not None and tcl is not None

    # Equity anchors the right side; non-equity = Total Assets − Equity forces the right
    # bar to tie to Total Assets exactly (so both bars are equal height).
    tse, tl = val("Total Stockholders Equity"), val("Total Liabilities")
    if tse is not None:
        equity = max(0.0, tse)
    elif tl is not None:
        equity = max(0.0, total - tl)
    else:
        equity = 0.0
    non_equity = max(0.0, total - equity)

    if classified:
        asset_groups = [
            build_subgroup("Current", _ASSET_CURRENT, tca),
            build_subgroup("Non-current", _ASSET_NONCURRENT, total - tca),
        ]
        cur_liab = min(max(0.0, tcl), non_equity)
        liab_groups = [
            build_subgroup("Current", _LIAB_CURRENT, cur_liab),
            build_subgroup("Non-current", _LIAB_NONCURRENT, non_equity - cur_liab),
        ]
    else:
        asset_groups = [build_subgroup("", _ASSET_CURRENT + _ASSET_NONCURRENT, total)]
        liab_groups = [build_subgroup("", _LIAB_CURRENT + _LIAB_NONCURRENT, non_equity)]

    assets_flat = assign_ramp(asset_groups, _ASSET_DARK, _ASSET_LIGHT)
    liab_flat = assign_ramp(liab_groups, _LIAB_DARK, _LIAB_LIGHT)
    equity_seg = {"name": "Equity", "value": equity, "color": _EQUITY, "ink": _ink_for(_EQUITY)}

    def bar_html(title: str, segments: list[dict]) -> str:
        rows = []
        for s in segments:
            if s["value"] <= 0:
                continue
            cls = "bs-seg bs-boundary" if s.get("boundary") else "bs-seg"
            pct = s["value"] / total * 100
            rows.append(
                f'<div class="{cls}" style="flex-grow:{s["value"] / 1e9:.4f};'
                f'background:{s["color"]};color:{s["ink"]};">'
                f'<span class="bs-seg-name">{html.escape(s["name"])}</span>'
                f'<span class="bs-seg-val">${_b(s["value"])}B · {pct:.0f}%</span>'
                '</div>'
            )
        return (
            '<div class="bs-bar">'
            f'<div class="bs-bar-head"><span class="bs-bar-name">{html.escape(title)}</span>'
            f'<span class="bs-bar-total">${_b(total)}B</span></div>'
            f'<div class="bs-stack">{"".join(rows)}</div>'
            '</div>'
        )

    def legend_family(name: str, fam_total: float, subgroups: list[dict], dot: str | None = None) -> str:
        dot_html = f'<span class="bs-leg-dot" style="background:{dot};"></span>' if dot else ""
        parts = [
            '<div class="bs-leg-fam"><div class="bs-leg-fam-head">'
            f'<span class="bs-leg-fam-name">{dot_html}{html.escape(name)}</span>'
            f'<span class="bs-leg-total">${_b(fam_total)}B</span></div>'
        ]
        for sg in subgroups:
            if sg["name"]:
                parts.append(
                    f'<div class="bs-leg-sub"><span>{sg["name"]}</span>'
                    f'<span>${_b(sg["subtotal"])}B</span></div>'
                )
            for s in sg["segs"]:
                if s["value"] <= 0:
                    continue
                parts.append(
                    '<div class="bs-leg-item">'
                    f'<span class="bs-leg-dot" style="background:{s["color"]};"></span>'
                    f'<span class="bs-leg-name">{html.escape(s["name"])}</span>'
                    f'<span class="bs-leg-val">${_b(s["value"])}B</span></div>'
                )
        parts.append("</div>")
        return "".join(parts)

    liab_total = sum(s["value"] for s in liab_flat)
    bars = (
        '<div class="bs-bars">'
        f'{bar_html("Assets", assets_flat)}'
        f'{bar_html("Liabilities + Equity", liab_flat + [equity_seg])}'
        '</div>'
    )
    legend = (
        '<div class="bs-legend">'
        '<div class="bs-leg-key"><span>Liquid</span>'
        f'<span class="bs-leg-grad" style="background:linear-gradient(90deg,{_ASSET_DARK},{_ASSET_LIGHT});">'
        '</span><span>Immobilized</span></div>'
        f'{legend_family("Assets", total, asset_groups)}'
        f'{legend_family("Liabilities", liab_total, liab_groups)}'
        f'{legend_family("Equity", equity, [], dot=_EQUITY)}'
        '</div>'
    )

    yr_label = short_year(year) if isinstance(year, (int, float)) else str(year)
    return (
        '<div class="qchart">'
        '<div class="qchart-header">'
        f'<h3>Balance sheet composition · FY{yr_label}</h3>'
        '<div class="qchart-legend"><span>$B · ordered by liquidity</span></div>'
        '</div>'
        f'<div class="bs-composition">{bars}{legend}</div>'
        '</div>'
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
