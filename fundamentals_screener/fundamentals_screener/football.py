"""Presentation helper: turn a :class:`FootballField` DTO into chart geometry.

Pure layout math (no valuation logic, no I/O): maps each bar's bear/mid/bull and the market
price onto a shared 0–100 % axis so the template can draw CSS bars. Kept out of the view so
it stays unit-testable.
"""

from __future__ import annotations

from dataclasses import dataclass

from .dtos import FootballField


@dataclass(frozen=True)
class BarGeometry:
    """One method's bar positioned on the 0–100 % axis (percent = left offset / width)."""

    method: str
    fiscal_year: int
    bear: float
    mid: float
    bull: float
    left_pct: float
    width_pct: float
    mid_pct: float
    covers_price: bool  # price falls inside [bear, bull] → market within this method's range


@dataclass(frozen=True)
class FootballChart:
    bars: tuple[BarGeometry, ...]
    price: float | None
    price_pct: float | None
    axis_lo: float
    axis_hi: float


def build_chart(field: FootballField) -> FootballChart | None:
    """Geometry for ``field``, or ``None`` when there are no bars to plot."""
    if not field.bars:
        return None

    points = [v for b in field.bars for v in (b.bear, b.bull)]
    if field.price is not None:
        points.append(field.price)
    lo, hi = min(points), max(points)
    span = hi - lo or 1.0  # avoid /0 when everything collapses to one value

    def pct(value: float) -> float:
        return (value - lo) / span * 100.0

    bars = tuple(
        BarGeometry(
            method=b.method,
            fiscal_year=b.fiscal_year,
            bear=b.bear,
            mid=b.mid,
            bull=b.bull,
            left_pct=pct(b.bear),
            width_pct=pct(b.bull) - pct(b.bear),
            mid_pct=pct(b.mid),
            covers_price=field.price is not None and b.bear <= field.price <= b.bull,
        )
        for b in field.bars
    )
    price_pct = pct(field.price) if field.price is not None else None
    return FootballChart(bars=bars, price=field.price, price_pct=price_pct, axis_lo=lo, axis_hi=hi)
