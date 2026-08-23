"""Pure per-DTO currency conversion for the Company Detail page's currency lens.

Reuses ``fx_lens.RateKey``/``resolve_rates`` (shared with the General Screener) — this module
holds NO database access itself; the caller (``services.apply_currency_lens``) resolves rates
ONCE for the whole page and hands the resulting dict to every converter here.

Deliberate simplification vs. the General Screener: unlike the screener (which converts each
CELL independently, since a mixed-currency row would be confusing on a table showing MANY
tickers at once), the grid converters here (``convert_statement``/``convert_grid``) and
``convert_football`` are ALL-OR-NOTHING per DTO — if even one of the (few) distinct dates a
Statement/QuarterGrid/FootballField needs fails to resolve a rate, the WHOLE thing stays
native rather than partially converting. For a single-ticker page where every column is the
SAME company's own history, one table showing one currency throughout is both simpler to
implement/verify and arguably more honest UX than a per-column patchwork. Given CAD/USD FX
data covers back to 2003 (confirmed live this session), a partial-coverage gap for a company's
own recent fiscal history is a near-impossible edge case in practice — a deliberate simplicity
trade for a genuinely rare situation, not a corner cut on the common path. ``MetricPoint``s
(single values, e.g. the Valuation tab's multiples) still convert independently per point —
there's no "column" to keep consistent there.

Also fixes the same "usd"-labeled-but-really-native mislabeling bug already fixed for the
General Screener (see ``company_listing.py``'s ``_relabel_native_currency``): every dollar
metric OTHER than Market Cap / Market Cap (Live) carries a static "usd" unit literal from
``metrics_hierarchy.json`` regardless of the ticker's real reporting currency. This page's
templates already render Statement/QuarterGrid/FootballBar/NetNetRow figures via
``reporting_currency`` directly (never a per-cell unit), so those are already correctly
labeled — only ``MetricPoint`` (Valuation tab multiples, Market Cap KPI) and ``MetricSeries``
(Derived-metrics tab's dollar subset) render via their own ``unit`` field and inherit the same
bug ``relabel_metric_points``/``relabel_metric_series`` fix, unconditionally (independent of
whether a conversion is active — this is a display-correctness fix, not a conversion feature).

Never fabricates a rate, never raises to the caller: any DTO whose dates can't ALL resolve
just comes back unchanged (native currency), the same "real gap reads absent, not guessed"
convention used throughout this codebase.
"""
from __future__ import annotations

from dataclasses import replace
from datetime import date

from fundamentals_pipeline.fx import convert_price

from .dtos import FootballField, MetricPoint, MetricSeries, NetNetRow, QuarterGrid, Statement
from .repositories.fx_lens import RateKey

_MISLABELED_EXCLUDE = {"Market Cap", "Market Cap (Live)"}
_NON_CURRENCY_UNITS = {"percent", "ratio"}


def _to_date(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


def _is_currency_like(unit: str | None) -> bool:
    """True for anything ``metric_value`` would render as a currency — any non-blank unit that
    isn't ``"percent"``/``"ratio"`` (mirrors that template filter's own dispatch)."""
    u = (unit or "").lower()
    return bool(u) and u not in _NON_CURRENCY_UNITS


# ── relabel: fix the mislabeled-"usd" bug (unconditional, independent of conversion) ──────────

def relabel_metric_points(
    points: tuple[MetricPoint, ...], reporting_currency: str | None
) -> tuple[MetricPoint, ...]:
    """See module docstring. Market Cap / Market Cap (Live) excluded — already correct,
    genuine per-row currency. A point with no known reporting_currency is left as-is."""
    if not reporting_currency:
        return points
    ccy_lower = reporting_currency.lower()
    return tuple(
        replace(p, unit=ccy_lower)
        if p.metric not in _MISLABELED_EXCLUDE
        and (p.unit or "").lower() == "usd"
        and ccy_lower != "usd"
        else p
        for p in points
    )


def relabel_metric_series(
    series: tuple[MetricSeries, ...], reporting_currency: str | None
) -> tuple[MetricSeries, ...]:
    """See module docstring — the Derived-metrics tab's dollar subset (Operating Cash Flow,
    CapEx, Free Cash Flow) has the exact same mislabeling as the General Screener's own
    non-Market-Cap dollar columns."""
    if not reporting_currency:
        return series
    ccy_lower = reporting_currency.lower()
    return tuple(
        replace(s, unit=ccy_lower)
        if (s.unit or "").lower() == "usd" and ccy_lower != "usd"
        else s
        for s in series
    )


# ── MetricPoint: independent per-point conversion (Valuation tab multiples, Market Cap KPI) ───

def metric_point_keys(points: tuple[MetricPoint, ...], target_currency: str) -> frozenset[RateKey]:
    target = target_currency.upper()
    keys: set[RateKey] = set()
    for p in points:
        if not _is_currency_like(p.unit) or p.value is None:
            continue
        native = p.unit.upper()  # type: ignore[union-attr]
        if native == target:
            continue
        as_of = _to_date(p.period_end)
        if as_of is not None:
            keys.add(RateKey(native=native, as_of=as_of))
    return frozenset(keys)


def convert_metric_points(
    points: tuple[MetricPoint, ...], rates: dict[RateKey, float], target_currency: str
) -> tuple[MetricPoint, ...]:
    target = target_currency.upper()
    out = []
    for p in points:
        if not _is_currency_like(p.unit) or p.value is None:
            out.append(p)
            continue
        native = p.unit.upper()  # type: ignore[union-attr]
        if native == target:
            out.append(replace(p, unit=target.lower()) if p.unit != target.lower() else p)
            continue
        as_of = _to_date(p.period_end)
        rate = rates.get(RateKey(native=native, as_of=as_of)) if as_of is not None else None
        if rate is None:
            out.append(p)  # this one point stays native
            continue
        out.append(replace(p, value=convert_price(p.value, native, target, rate), unit=target.lower()))
    return tuple(out)


# ── MetricSeries: all-or-nothing per series (Derived-metrics tab dollar subset) ───────────────

def metric_series_keys(series: tuple[MetricSeries, ...], target_currency: str) -> frozenset[RateKey]:
    target = target_currency.upper()
    keys: set[RateKey] = set()
    for s in series:
        if not _is_currency_like(s.unit):
            continue
        native = s.unit.upper()  # type: ignore[union-attr]
        if native == target:
            continue
        for i, v in enumerate(s.values):
            if v is None:
                continue
            pe = s.period_ends[i] if i < len(s.period_ends) else None
            as_of = _to_date(pe)
            if as_of is not None:
                keys.add(RateKey(native=native, as_of=as_of))
    return frozenset(keys)


def convert_metric_series(
    series: tuple[MetricSeries, ...], rates: dict[RateKey, float], target_currency: str
) -> tuple[MetricSeries, ...]:
    target = target_currency.upper()
    out = []
    for s in series:
        if not _is_currency_like(s.unit):
            out.append(s)
            continue
        native = s.unit.upper()  # type: ignore[union-attr]
        if native == target:
            out.append(replace(s, unit=target.lower()) if s.unit != target.lower() else s)
            continue
        new_values: list[float | None] = []
        ok = True
        for i, v in enumerate(s.values):
            if v is None:
                new_values.append(None)
                continue
            pe = s.period_ends[i] if i < len(s.period_ends) else None
            as_of = _to_date(pe)
            rate = rates.get(RateKey(native=native, as_of=as_of)) if as_of is not None else None
            if rate is None:
                ok = False
                break
            new_values.append(convert_price(v, native, target, rate))
        out.append(replace(s, values=tuple(new_values), unit=target.lower()) if ok else s)
    return tuple(out)


# ── Statement / QuarterGrid: all-or-nothing per grid (Income Statement/Balance Sheet/Cash
#    Flow/Quarterly) ───────────────────────────────────────────────────────────────────────

def _grid_rate_by_column(
    period_ends: tuple[str | None, ...], native: str, target: str, rates: dict[RateKey, float],
) -> list[float | None]:
    out: list[float | None] = []
    for pe in period_ends:
        as_of = _to_date(pe)
        out.append(rates.get(RateKey(native=native, as_of=as_of)) if as_of is not None else None)
    return out


def _grid_column_has_value(lines, index: int) -> bool:
    return any(
        (line.values[index] if index < len(line.values) else None) is not None for line in lines
    )


def _convert_grid_lines(lines, period_ends, native: str, target: str, rates: dict[RateKey, float]):
    """(new_lines, success) — success=False means at least one column that actually carries a
    reported value has no resolvable rate; the caller then discards new_lines and keeps native."""
    rate_by_col = _grid_rate_by_column(period_ends, native, target, rates)
    for i, rate in enumerate(rate_by_col):
        if rate is None and _grid_column_has_value(lines, i):
            return lines, False
    new_lines = tuple(
        replace(line, values=tuple(
            convert_price(v, native, target, rate_by_col[i])
            if v is not None and i < len(rate_by_col) and rate_by_col[i] is not None
            else v
            for i, v in enumerate(line.values)
        ))
        for line in lines
    )
    return new_lines, True


def statement_keys(statement: Statement, reporting_currency: str, target_currency: str) -> frozenset[RateKey]:
    native, target = reporting_currency.upper(), target_currency.upper()
    if native == target:
        return frozenset()
    return frozenset(
        RateKey(native=native, as_of=d)
        for pe in statement.period_ends if (d := _to_date(pe)) is not None
    )


def convert_statement(
    statement: Statement, reporting_currency: str, rates: dict[RateKey, float], target_currency: str,
) -> tuple[Statement, str]:
    """Returns (possibly-converted statement, its resulting currency — target on success,
    native reporting_currency if the conversion couldn't fully resolve)."""
    native, target = reporting_currency.upper(), target_currency.upper()
    if native == target:
        return statement, native
    new_lines, ok = _convert_grid_lines(statement.lines, statement.period_ends, native, target, rates)
    if not ok:
        return statement, native
    return replace(statement, lines=new_lines), target


def grid_keys(grid: QuarterGrid, reporting_currency: str, target_currency: str) -> frozenset[RateKey]:
    native, target = reporting_currency.upper(), target_currency.upper()
    if native == target:
        return frozenset()
    return frozenset(
        RateKey(native=native, as_of=d)
        for pe in grid.period_ends if (d := _to_date(pe)) is not None
    )


def convert_grid(
    grid: QuarterGrid, reporting_currency: str, rates: dict[RateKey, float], target_currency: str,
) -> tuple[QuarterGrid, str]:
    native, target = reporting_currency.upper(), target_currency.upper()
    if native == target:
        return grid, native
    new_lines, ok = _convert_grid_lines(grid.lines, grid.period_ends, native, target, rates)
    if not ok:
        return grid, native
    return replace(grid, lines=new_lines), target


# ── FootballField: all-or-nothing across every bear/mid/bull bar ──────────────────────────────

def football_keys(field: FootballField, reporting_currency: str, target_currency: str) -> frozenset[RateKey]:
    native, target = reporting_currency.upper(), target_currency.upper()
    if native == target:
        return frozenset()
    return frozenset(
        RateKey(native=native, as_of=d)
        for b in field.bars if (d := _to_date(b.period_end)) is not None
    )


def convert_football(
    field: FootballField, reporting_currency: str, rates: dict[RateKey, float], target_currency: str,
) -> tuple[FootballField, str]:
    native, target = reporting_currency.upper(), target_currency.upper()
    if native == target or not field.bars:
        return field, native
    new_bars = []
    for b in field.bars:
        as_of = _to_date(b.period_end)
        rate = rates.get(RateKey(native=native, as_of=as_of)) if as_of is not None else None
        if rate is None:
            return field, native  # any bar failing -> whole field stays native
        new_bars.append(replace(
            b,
            bear=convert_price(b.bear, native, target, rate),
            mid=convert_price(b.mid, native, target, rate),
            bull=convert_price(b.bull, native, target, rate),
        ))
    return replace(field, bars=tuple(new_bars)), target


# ── NetNetRow: one shared "live snapshot" date (the Market Cap point's own period_end) ────────

def net_net_keys(
    row: NetNetRow | None, reporting_currency: str, target_currency: str, as_of: str | None,
) -> frozenset[RateKey]:
    if row is None:
        return frozenset()
    native, target = reporting_currency.upper(), target_currency.upper()
    if native == target:
        return frozenset()
    d = _to_date(as_of)
    return frozenset({RateKey(native=native, as_of=d)}) if d is not None else frozenset()


def convert_net_net(
    row: NetNetRow | None, reporting_currency: str, rates: dict[RateKey, float],
    target_currency: str, as_of: str | None,
) -> tuple[NetNetRow | None, str]:
    native = reporting_currency.upper()
    if row is None:
        return None, native
    target = target_currency.upper()
    if native == target:
        return row, native
    d = _to_date(as_of)
    rate = rates.get(RateKey(native=native, as_of=d)) if d is not None else None
    if rate is None:
        return row, native

    def conv(v: float | None) -> float | None:
        return convert_price(v, native, target, rate) if v is not None else None

    return replace(
        row,
        price=conv(row.price),
        market_cap=conv(row.market_cap),
        ncav_per_share_relaxed=conv(row.ncav_per_share_relaxed),
        ncav_per_share_moderate=conv(row.ncav_per_share_moderate),
        ncav_per_share_strict=conv(row.ncav_per_share_strict),
        currency=target.lower(),
    ), target
