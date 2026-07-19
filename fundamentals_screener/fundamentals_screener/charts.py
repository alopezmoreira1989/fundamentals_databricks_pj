"""Server-side inline-SVG chart helpers for the company page.

Pure geometry, no I/O and no financial logic. Kept unit-testable, like
:mod:`fundamentals_screener.pricechart`.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass

from .dtos import PricePoint, QuarterGrid, Statement

# Editorial tokens (kept in sync with static/fundamentals_screener/css/app.css).
_ACCENT = "#0EA5B0"
_POSITIVE = "#0F6E56"
_NEGATIVE = "#B0301A"
_WARNING = "#E8702A"
_INK3 = "#7C8DA6"
_RULE = "#D7DEE8"


def sparkline_svg(
    values: Sequence[float | None],
    *,
    width: int = 84,
    height: int = 22,
    stroke: float = 1.5,
    color: str = _ACCENT,
) -> str:
    """A trend sparkline for ``values`` (chronological, oldest first) as an inline ``<svg>``.

    Missing points (``None``) are skipped — the line just jumps. Fewer than two valid points
    returns ``""`` (no sparkline). Output is numbers only (no user data), so it is safe to mark
    safe in the template.
    """
    vals = list(values)
    valid = [(i, v) for i, v in enumerate(vals) if v is not None]
    if len(valid) < 2:
        return ""
    lo = min(v for _, v in valid)
    hi = max(v for _, v in valid)
    span = (hi - lo) or 1.0
    n = len(vals)

    def px(i: int) -> float:
        return 2 + i * (width - 4) / max(n - 1, 1)

    def py(v: float) -> float:
        return (height - 2) - (height - 4) * (v - lo) / span  # higher value → higher on screen

    points = " ".join(f"{px(i):.1f},{py(v):.1f}" for i, v in valid)
    last_i, last_v = valid[-1]
    return (
        f'<svg class="sparkline" viewBox="0 0 {width} {height}" width="{width}" height="{height}" '
        f'preserveAspectRatio="none" aria-hidden="true">'
        f'<polyline points="{points}" fill="none" stroke="{color}" stroke-width="{stroke}" '
        f'stroke-linejoin="round" stroke-linecap="round"/>'
        f'<circle cx="{px(last_i):.1f}" cy="{py(last_v):.1f}" r="1.7" fill="{color}"/></svg>'
    )


# ── tab-level bar / combo charts ─────────────────────────────────────────────────────────

@dataclass(frozen=True)
class TabChart:
    """A rendered tab chart: the inline ``<svg>`` plus legend entries ``(name, color)``."""

    svg: str
    legend: tuple[tuple[str, str], ...]


def _bar_chart_svg(
    labels: Sequence[str],
    series: Sequence[tuple[str, Sequence[float | None], str]],
    *,
    line: tuple[str, Sequence[float | None], str] | None = None,
    width: int = 1000,
    height: int = 300,
) -> str:
    """Grouped vertical bars (one group per label) with an optional overlaid line, on a shared
    axis with a zero baseline. Negative values draw below the baseline. Numbers only → safe."""
    numbers = [v for _, vals, _ in series for v in vals if v is not None]
    if line:
        numbers += [v for v in line[1] if v is not None]
    if not numbers or not labels:
        return ""
    lo, hi = min(numbers + [0.0]), max(numbers + [0.0])
    span = (hi - lo) or 1.0
    pad_l, pad_r, pad_t, pad_b = 8, 8, 14, 26
    plot_w, plot_h = width - pad_l - pad_r, height - pad_t - pad_b

    def py(v: float) -> float:
        return pad_t + plot_h * (hi - v) / span

    y0 = py(0.0)
    n = len(labels)
    slot = plot_w / n
    nb = max(len(series), 1)
    group_w = slot * 0.68
    bar_w = group_w / nb
    gx0 = (slot - group_w) / 2

    parts = [f'<line x1="{pad_l}" y1="{y0:.1f}" x2="{width - pad_r}" y2="{y0:.1f}" stroke="{_RULE}" stroke-width="1"/>']
    for si, (_name, vals, color) in enumerate(series):
        for i, v in enumerate(vals):
            if v is None:
                continue
            x = pad_l + i * slot + gx0 + si * bar_w
            yv = py(v)
            parts.append(
                f'<rect x="{x:.1f}" y="{min(yv, y0):.1f}" width="{max(bar_w - 1.5, 1):.1f}" '
                f'height="{abs(yv - y0):.1f}" fill="{color}" rx="1"/>'
            )
    if line:
        pts = [
            (pad_l + i * slot + slot / 2, py(v)) for i, v in enumerate(line[1]) if v is not None
        ]
        if len(pts) >= 2:
            poly = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
            parts.append(f'<polyline points="{poly}" fill="none" stroke="{line[2]}" stroke-width="2.5" stroke-linejoin="round"/>')
            parts += [f'<circle cx="{x:.1f}" cy="{y:.1f}" r="2.6" fill="{line[2]}"/>' for x, y in pts]
    for i, lab in enumerate(labels):
        parts.append(
            f'<text x="{pad_l + i * slot + slot / 2:.1f}" y="{height - 8}" text-anchor="middle" '
            f'font-size="11" fill="{_INK3}" font-family="JetBrains Mono, monospace">{lab}</text>'
        )
    return (
        f'<svg class="tab-chart" viewBox="0 0 {width} {height}" width="100%" '
        f'preserveAspectRatio="xMidYMid meet" role="img">' + "".join(parts) + "</svg>"
    )


def _line_of(rows: Sequence, name: str) -> tuple[float | None, ...] | None:
    """The chronological (oldest-first) values of the line named ``name`` in a statement/grid."""
    for row in rows:
        if row.display_name == name:
            return tuple(reversed(row.values))
    return None


def _year_labels(years: Sequence[int]) -> tuple[str, ...]:
    return tuple(f"'{str(y)[2:]}" for y in reversed(years))


def income_statement_chart(statement: Statement) -> TabChart | None:
    """Revenue bars with a Net-Income line overlaid, across fiscal years."""
    revenue = _line_of(statement.lines, "Revenue")
    if revenue is None:
        return None
    labels = _year_labels(statement.years)
    net_income = _line_of(statement.lines, "Net Income")
    series = [("Revenue", revenue, _ACCENT)]
    line = ("Net Income", net_income, _POSITIVE) if net_income else None
    legend = (("Revenue", _ACCENT),) + ((("Net Income", _POSITIVE),) if net_income else ())
    return TabChart(_bar_chart_svg(labels, series, line=line), legend)


def cash_flow_chart(statement: Statement) -> TabChart | None:
    """Operating / Investing / Financing cash-flow grouped bars, across fiscal years."""
    specs = (
        ("Operating", "Operating CF", _POSITIVE),
        ("Investing", "Investing CF", _WARNING),
        ("Financing", "Financing CF", _NEGATIVE),
    )
    series = [(label, vals, color) for label, name, color in specs if (vals := _line_of(statement.lines, name))]
    if not series:
        return None
    legend = tuple((label, color) for label, vals, color in series)
    return TabChart(_bar_chart_svg(_year_labels(statement.years), series), legend)


def quarterly_chart(grid: QuarterGrid) -> TabChart | None:
    """Quarterly Revenue bars across recent fiscal quarters."""
    revenue = _line_of(grid.lines, "Revenue")
    if revenue is None:
        return None
    labels = tuple(reversed(grid.columns))
    return TabChart(_bar_chart_svg(labels, [("Revenue", revenue, _ACCENT)]), (("Revenue", _ACCENT),))


# ── price tab: adjusted close + SMA 20/50/200, value axis + date axis + legend ──────────────

# (label, value-getter, color, stroke-width, opacity) — Price is the primary series (heavier,
# fully opaque); the SMAs are supporting context (thinner, dimmed). Plots adj_close
# (split-safe), not raw close — a stock split would otherwise show as a fake cliff;
# PriceChart's headline stats (pricechart.py) still use raw close separately.
_PRICE_SERIES: tuple[tuple[str, Callable[[PricePoint], float | None], str, float, float], ...] = (
    ("Price",   lambda p: p.adj_close, "#0B2545",  2.2, 1.0),
    ("SMA 20",  lambda p: p.sma20,     "#E8702A",  1.3, 0.85),
    ("SMA 50",  lambda p: p.sma50,     _POSITIVE,  1.3, 0.85),
    ("SMA 200", lambda p: p.sma200,    _NEGATIVE,  1.3, 0.85),
)
_PRICE_W = 1000
_PRICE_H = 320
_PRICE_PAD = (46, 8, 10, 24)  # left, right, top, bottom — left is wider for the value-axis labels


def price_line_chart(series: Sequence[PricePoint]) -> TabChart | None:
    """Adjusted close + SMA 20/50/200 as an inline multi-series ``<svg>``, with value-axis
    gridlines/labels, a handful of date-axis ticks, and a legend.

    ``None`` if no series has at least 2 plottable points (e.g. a ticker with only a couple of
    days of price history, or with no adj_close at all).
    """
    n = len(series)
    if n < 2:
        return None

    all_vals = [v for _, get, *_ in _PRICE_SERIES for p in series if (v := get(p)) is not None]
    if len(all_vals) < 2:
        return None
    lo, hi = min(all_vals), max(all_vals)
    span = (hi - lo) or 1.0

    pad_l, pad_r, pad_t, pad_b = _PRICE_PAD
    plot_w, plot_h = _PRICE_W - pad_l - pad_r, _PRICE_H - pad_t - pad_b

    def px(i: int) -> float:
        return pad_l + i * plot_w / (n - 1)

    def py(v: float) -> float:
        return pad_t + plot_h * (hi - v) / span

    parts: list[str] = []
    for frac in (0.0, 1 / 3, 2 / 3, 1.0):
        v = lo + frac * span
        y = py(v)
        parts.append(
            f'<line x1="{pad_l}" y1="{y:.1f}" x2="{_PRICE_W - pad_r}" y2="{y:.1f}" '
            f'stroke="{_RULE}" stroke-width="1"/>'
        )
        parts.append(
            f'<text x="{pad_l - 6}" y="{y + 3:.1f}" text-anchor="end" font-size="10.5" '
            f'fill="{_INK3}" font-family="JetBrains Mono, monospace">{v:,.0f}</text>'
        )

    tick_idxs = sorted({round(i * (n - 1) / 4) for i in range(5)})
    for i in tick_idxs:
        # The first/last tick would otherwise overflow past the plot edge under "middle"
        # anchoring — anchor those two to the inside instead.
        anchor = "start" if i == 0 else "end" if i == tick_idxs[-1] else "middle"
        parts.append(
            f'<text x="{px(i):.1f}" y="{_PRICE_H - 6}" text-anchor="{anchor}" font-size="10.5" '
            f'fill="{_INK3}" font-family="JetBrains Mono, monospace">{series[i].date[:10]}</text>'
        )

    legend: list[tuple[str, str]] = []
    for name, get, color, stroke_w, opacity in _PRICE_SERIES:
        pts = [(px(i), py(v)) for i, p in enumerate(series) if (v := get(p)) is not None]
        if len(pts) < 2:
            continue
        poly = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
        parts.append(
            f'<polyline points="{poly}" fill="none" stroke="{color}" stroke-width="{stroke_w}" '
            f'stroke-opacity="{opacity}" stroke-linejoin="round" stroke-linecap="round"/>'
        )
        legend.append((name, color))

    svg = (
        f'<svg class="tab-chart" viewBox="0 0 {_PRICE_W} {_PRICE_H}" width="100%" '
        f'preserveAspectRatio="xMidYMid meet" role="img" '
        f'aria-label="Price trend with moving averages">' + "".join(parts) + "</svg>"
    )
    return TabChart(svg, tuple(legend))


# ── balance-sheet composition (single year, stacked twin bars) ───────────────────────────

# Semantic color families: assets = navy/cyan, liabilities = orange/red, equity = green. Within
# assets and liabilities the shade ramps DARK (most liquid / current, listed first) → LIGHT
# (least liquid / non-current). A black rule divides the current from the non-current block.
_BLUE_LIGHT, _BLUE_DARK = (168, 210, 214), (11, 37, 69)        # #A8D2D6 (illiquid) … #0B2545 (liquid)
_RED_LIGHT, _RED_DARK = (240, 195, 168), (122, 45, 22)         # #F0C3A8 (non-current) … #7A2D16 (current)
_EQUITY_GREEN = "#0F6E56"
_CURRENT_GROUPS = ("Current Assets", "Current Liabilities")
_LIABILITY_GROUPS = ("Current Liabilities", "Non-Current Liabilities")


def _ramp(start: tuple[int, int, int], end: tuple[int, int, int], rank: int, count: int) -> str:
    """A hex shade between ``start`` (rank 0) and ``end`` (rank count-1)."""
    t = rank / (count - 1) if count > 1 else 0.0
    r, g, b = (round(start[i] + (end[i] - start[i]) * t) for i in range(3))
    return f"#{r:02X}{g:02X}{b:02X}"


@dataclass(frozen=True)
class Segment:
    name: str
    value: float
    pct: float  # share of the stack total, 0–100
    color: str
    boundary: bool = False  # first non-current segment → draw the current/non-current divider


@dataclass(frozen=True)
class Stack:
    title: str
    total: float
    segments: tuple[Segment, ...]


@dataclass(frozen=True)
class Composition:
    """One fiscal year's balance sheet as two stacked bars (Assets | Liabilities & Equity)."""

    year: int
    assets: Stack
    liabilities_equity: Stack


def _with_other(leaves: list[tuple[str, float | None, bool]], total: float | None, other_label: str) -> list[tuple[str, float, bool]]:
    """Positive leaf segments (name, value, is_current), plus a non-current ``other`` remainder
    so the stack sums to ``total``."""
    segs = [(name, value, is_current) for name, value, is_current in leaves if value is not None and value > 0]
    if total is not None and total > 0:
        remainder = total - sum(value for _, value, _ in segs)
        if remainder > total * 0.01:  # ignore tiny / negative remainders
            segs.append((other_label, remainder, False))
    return segs


def _ramped(raw: list[tuple[str, float, bool]], total: float, start: tuple[int, int, int], end: tuple[int, int, int]) -> list[Segment]:
    """Dark→light ramped segments, flagging the first non-current one as the divider boundary."""
    out: list[Segment] = []
    previous_current: bool | None = None
    for i, (name, value, is_current) in enumerate(raw):
        boundary = previous_current is True and not is_current
        out.append(Segment(name=name, value=value, pct=value / total * 100, color=_ramp(start, end, i, len(raw)), boundary=boundary))
        previous_current = is_current
    return out


def balance_sheet_compositions(statement: Statement) -> tuple[Composition, ...]:
    """One :class:`Composition` per fiscal year (newest first) — assets and liabilities+equity
    each broken into their line items (plus an ``Other`` remainder), sized by dollar value."""
    lines = statement.lines
    compositions: list[Composition] = []
    for yi, year in enumerate(statement.years):
        def value_of(name: str, _yi: int = yi) -> float | None:
            return next((ln.values[_yi] for ln in lines if ln.display_name == name), None)

        # Assets → blue, dark (Cash, current) → light (PP&E / Other, non-current).
        asset_leaves = [
            (ln.display_name, ln.values[yi], ln.group in _CURRENT_GROUPS)
            for ln in lines
            if ln.section == "Assets" and ln.group and not ln.display_name.startswith("Total")
        ]
        total_assets = value_of("Total Assets")
        asset_raw = _with_other(asset_leaves, total_assets, "Other assets")
        assets = (
            Stack("Assets", total_assets, tuple(_ramped(asset_raw, total_assets, _BLUE_DARK, _BLUE_LIGHT)))
            if total_assets and total_assets > 0 and asset_raw
            else None
        )

        # Liabilities → red, dark (current) → light (non-current); equity → green (after).
        liab_leaves = [
            (ln.display_name, ln.values[yi], ln.group in _CURRENT_GROUPS)
            for ln in lines
            if ln.group in _LIABILITY_GROUPS and not ln.display_name.startswith("Total")
        ]
        liab_raw = _with_other(liab_leaves, value_of("Total Liabilities"), "Other liabilities")
        le_total = value_of("Total Liabilities & Equity")
        le_segments = _ramped(liab_raw, le_total, _RED_DARK, _RED_LIGHT) if le_total and le_total > 0 else []
        equity = value_of("Total Stockholders Equity")
        if equity is not None and equity > 0 and le_total:
            le_segments.append(Segment("Equity", equity, equity / le_total * 100, _EQUITY_GREEN))
        le = Stack("Liabilities & Equity", le_total, tuple(le_segments)) if le_total and le_segments else None

        if assets and le:
            compositions.append(Composition(year=year, assets=assets, liabilities_equity=le))
    return tuple(compositions)
