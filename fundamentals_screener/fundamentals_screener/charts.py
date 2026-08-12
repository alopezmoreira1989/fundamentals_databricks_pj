"""Server-side chart-data helpers for the company page.

Pure data derivation, no I/O and no financial logic. Kept unit-testable, like
:mod:`fundamentals_screener.pricechart`.

Geometry lives client-side now (Lightweight Charts for the Price tab, Chart.js for the
bar/combo/stacked charts -- see static/fundamentals_screener/js/price_chart.js,
statement_charts.js, balance_sheet_chart.js). These functions return small
dataclasses/tuples ready to pass through Django's ``json_script`` filter, not inline SVG.
Colors are read client-side from app.css's ``:root`` custom properties via
``getComputedStyle()``, never hardcoded here -- except :func:`sparkline_svg`'s, which is
untouched and out of scope for that migration (see its own docstring).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from .dtos import PricePoint, QuarterGrid, Statement

# Editorial token used only as sparkline_svg's inline-<svg> default. sparkline_svg itself is
# untouched by the Chart.js/Lightweight Charts migration -- a JS chart instance per table row
# (the Derived Metrics / statement tables can have 30+ rows) would tank page-render performance
# at the size these trend cells render (84x22px), so it stays hand-rolled SVG.
_ACCENT = "#0EA5B0"


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


# ── shared line/label derivation (unchanged by the chart-geometry migration) ────────────────

def _line_of(rows: Sequence, name: str) -> tuple[float | None, ...] | None:
    """The chronological (oldest-first) values of the line named ``name`` in a statement/grid."""
    for row in rows:
        if row.display_name == name:
            return tuple(reversed(row.values))
    return None


def _year_labels(years: Sequence[int]) -> tuple[str, ...]:
    return tuple(f"'{str(y)[2:]}" for y in reversed(years))


# ── tab-level bar / combo charts: DATA only, rendered client-side by statement_charts.js ────

@dataclass(frozen=True)
class ChartSeries:
    """One named data series for a Chart.js bar/combo chart (e.g. Revenue, Net Income, Operating
    CF). ``kind`` picks the Chart.js dataset type client-side (``"bar"`` or ``"line"``); the
    series' color is assigned client-side too (see statement_charts.js's fixed name→token
    mapping), never here."""

    name: str
    values: tuple[float | None, ...]
    kind: str = "bar"


@dataclass(frozen=True)
class TabChartData:
    """Chart-ready data for one tab's bar/combo chart: category labels (fiscal years or fiscal
    quarters, oldest-to-newest) plus each series to plot against them."""

    labels: tuple[str, ...]
    series: tuple[ChartSeries, ...]


def income_statement_chart(statement: Statement) -> TabChartData | None:
    """Revenue (bar) + Net Income (line) data, across fiscal years, oldest-to-newest."""
    revenue = _line_of(statement.lines, "Revenue")
    if revenue is None:
        return None
    labels = _year_labels(statement.years)
    net_income = _line_of(statement.lines, "Net Income")
    series = [ChartSeries("Revenue", revenue, "bar")]
    if net_income:
        series.append(ChartSeries("Net Income", net_income, "line"))
    return TabChartData(labels, tuple(series))


def cash_flow_chart(statement: Statement) -> TabChartData | None:
    """Operating Cash Flow vs Free Cash Flow (= OCF − |CapEx|) data, across fiscal years,
    oldest-to-newest — the OCF→FCF gap reads as CapEx. ``None`` when the statement has no
    Operating CF line."""
    ocf = _line_of(statement.lines, "Operating CF")
    if ocf is None or all(v is None for v in ocf):
        return None
    capex = _line_of(statement.lines, "CapEx")
    if capex is None:
        fcf: tuple[float | None, ...] = tuple(None for _ in ocf)
    else:
        fcf = tuple(None if (o is None or c is None) else o - abs(c) for o, c in zip(ocf, capex))
    series = (
        ChartSeries("Operating CF", ocf, "bar"),
        ChartSeries("Free CF (= OCF − CapEx)", fcf, "bar"),
    )
    return TabChartData(_year_labels(statement.years), series)


def quarterly_chart(grid: QuarterGrid) -> TabChartData | None:
    """Quarterly Revenue bar data across recent fiscal quarters, oldest-to-newest."""
    revenue = _line_of(grid.lines, "Revenue")
    if revenue is None:
        return None
    labels = tuple(reversed(grid.columns))
    return TabChartData(labels, (ChartSeries("Revenue", revenue, "bar"),))


# ── price tab: adjusted close + SMA 20/50/200, rendered client-side by price_chart.js ───────

def price_chart_data(series: Sequence[PricePoint]) -> tuple[PricePoint, ...] | None:
    """``series`` as-is, ready for the client-side Lightweight Charts render (adj_close + SMA
    20/50/200 lines).

    ``None`` if no series (Price or any SMA) has at least 2 plottable points combined — e.g. a
    ticker with only a couple of days of price history, or with no adj_close at all — matching
    the "no chart" guard the previous SVG builder applied.
    """
    if len(series) < 2:
        return None
    getters = (
        lambda p: p.adj_close,
        lambda p: p.sma20,
        lambda p: p.sma50,
        lambda p: p.sma200,
    )
    all_vals = [v for get in getters for p in series if (v := get(p)) is not None]
    if len(all_vals) < 2:
        return None
    return tuple(series)


# ── balance-sheet composition (single year, stacked twin bars) ───────────────────────────
# Unchanged by the chart-geometry migration -- these already return plain dataclasses (never
# built SVG), and the dark→light shade ramp is a genuine server-side computation (rank within a
# stack → interpolated color), not something a static app.css custom property could express.
# Only the template/view around this changes (Chart.js horizontal stacked bar replaces the CSS
# div-height bars); see balance_sheet_chart.js.

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
