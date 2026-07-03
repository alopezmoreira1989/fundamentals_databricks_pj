"""Server-side inline-SVG chart helpers for the company page.

Pure geometry, no I/O and no financial logic — mirrors the Streamlit dashboard's sparklines
(``lib/sparkline.py``) so the two front-ends read the same. Kept unit-testable, like
:mod:`apps.companies.pricechart`.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from repositories.dtos import QuarterGrid, Statement

# Editorial tokens (kept in sync with static/css/app.css).
_ACCENT = "#185FA5"
_POSITIVE = "#0F6E56"
_NEGATIVE = "#993C1D"
_WARNING = "#BA7517"
_INK3 = "#888780"
_RULE = "#D3D1C7"


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

@dataclass(frozen=True, slots=True)
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


# ── balance-sheet composition (single year, stacked twin bars) ───────────────────────────

# Semantic color families for the composition: assets = blue, liabilities = red, equity = green.
# Within assets and liabilities the shade ramps light→dark with *decreasing liquidity* (the
# statement lines are already ordered most-liquid first, and the "Other" remainder lands last).
_BLUE_LIGHT, _BLUE_DARK = (157, 195, 230), (12, 68, 124)      # #9DC3E6 → #0C447C
_RED_LIGHT, _RED_DARK = (222, 163, 148), (122, 45, 22)        # #DEA394 → #7A2D16
_EQUITY_GREEN = "#0F6E56"
_LIABILITY_GROUPS = ("Current Liabilities", "Non-Current Liabilities")


def _ramp(light: tuple[int, int, int], dark: tuple[int, int, int], rank: int, count: int) -> str:
    """A hex shade between ``light`` (rank 0) and ``dark`` (rank count-1) — the liquidity ramp."""
    t = rank / (count - 1) if count > 1 else 0.4
    r, g, b = (round(light[i] + (dark[i] - light[i]) * t) for i in range(3))
    return f"#{r:02X}{g:02X}{b:02X}"


@dataclass(frozen=True, slots=True)
class Segment:
    name: str
    value: float
    pct: float  # share of the stack total, 0–100
    color: str


@dataclass(frozen=True, slots=True)
class Stack:
    title: str
    total: float
    segments: tuple[Segment, ...]


@dataclass(frozen=True, slots=True)
class Composition:
    """One fiscal year's balance sheet as two stacked bars (Assets | Liabilities & Equity)."""

    year: int
    assets: Stack
    liabilities_equity: Stack


def _with_other(leaves: list[tuple[str, float | None]], total: float | None, other_label: str) -> list[tuple[str, float]]:
    """Positive leaf segments, plus an ``other`` remainder so the stack sums to ``total``."""
    segs = [(name, value) for name, value in leaves if value is not None and value > 0]
    if total is not None and total > 0:
        remainder = total - sum(value for _, value in segs)
        if remainder > total * 0.01:  # ignore tiny / negative remainders
            segs.append((other_label, remainder))
    return segs


def _stack(title: str, total: float | None, raw: list[tuple[str, float]], colors: list[str]) -> Stack | None:
    if total is None or total <= 0 or not raw:
        return None
    segments = tuple(
        Segment(name=name, value=value, pct=value / total * 100, color=color)
        for (name, value), color in zip(raw, colors, strict=True)
    )
    return Stack(title=title, total=total, segments=segments)


def balance_sheet_compositions(statement: Statement) -> tuple[Composition, ...]:
    """One :class:`Composition` per fiscal year (newest first) — assets and liabilities+equity
    each broken into their line items (plus an ``Other`` remainder), sized by dollar value."""
    lines = statement.lines
    compositions: list[Composition] = []
    for yi, year in enumerate(statement.years):
        def value_of(name: str, _yi: int = yi) -> float | None:
            return next((ln.values[_yi] for ln in lines if ln.display_name == name), None)

        # Assets → blue, shaded light→dark as liquidity decreases (Cash first … "Other" last).
        asset_leaves = [
            (ln.display_name, ln.values[yi])
            for ln in lines
            if ln.section == "Assets" and ln.group and not ln.display_name.startswith("Total")
        ]
        asset_raw = _with_other(asset_leaves, value_of("Total Assets"), "Other assets")
        asset_colors = [_ramp(_BLUE_LIGHT, _BLUE_DARK, i, len(asset_raw)) for i in range(len(asset_raw))]
        assets = _stack("Assets", value_of("Total Assets"), asset_raw, asset_colors)

        # Liabilities → red (current before non-current = light→dark); equity → green.
        liab_leaves = [
            (ln.display_name, ln.values[yi])
            for ln in lines
            if ln.group in _LIABILITY_GROUPS and not ln.display_name.startswith("Total")
        ]
        liab_raw = _with_other(liab_leaves, value_of("Total Liabilities"), "Other liabilities")
        le_raw = list(liab_raw)
        le_colors = [_ramp(_RED_LIGHT, _RED_DARK, i, len(liab_raw)) for i in range(len(liab_raw))]
        equity = value_of("Total Stockholders Equity")
        if equity is not None and equity > 0:
            le_raw.append(("Equity", equity))
            le_colors.append(_EQUITY_GREEN)
        le = _stack("Liabilities & Equity", value_of("Total Liabilities & Equity"), le_raw, le_colors)

        if assets and le:
            compositions.append(Composition(year=year, assets=assets, liabilities_equity=le))
    return tuple(compositions)
