"""Presentation helper: turn a price series into inline-SVG chart geometry.

Pure layout math (no I/O, no financial logic): maps the daily close series onto a fixed viewBox
so the template can draw an ``<svg>`` line + filled area. Kept out of the view/service so it
stays unit-testable — same split as :mod:`apps.valuation.football`.
"""

from __future__ import annotations

from dataclasses import dataclass

from repositories.dtos import PricePoint

_W = 1000  # viewBox width  (the SVG scales to its container via width=100%)
_H = 280   # viewBox height


@dataclass(frozen=True, slots=True)
class PriceChart:
    polyline: str  # "x,y x,y …" for <polyline points=…>
    area: str      # "M … Z" path for the shaded area under the line
    width: int
    height: int
    last_close: float
    min_close: float
    max_close: float
    first_date: str
    last_date: str
    up: bool  # last close >= first close → the range closed higher


def build_price_chart(series: tuple[PricePoint, ...]) -> PriceChart | None:
    """Geometry for ``series`` on a fixed viewBox, or ``None`` for fewer than two points."""
    if len(series) < 2:
        return None
    closes = [p.close for p in series]
    lo, hi = min(closes), max(closes)
    span = hi - lo or 1.0  # avoid /0 when the price is flat
    n = len(series)

    def coord(i: int, close: float) -> tuple[float, float]:
        x = i / (n - 1) * _W
        y = _H - (close - lo) / span * _H  # higher price → higher on screen (smaller y)
        return x, y

    pts = [coord(i, p.close) for i, p in enumerate(series)]
    polyline = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
    area = (
        f"M {pts[0][0]:.1f},{_H} "
        + " ".join(f"L {x:.1f},{y:.1f}" for x, y in pts)
        + f" L {pts[-1][0]:.1f},{_H} Z"
    )
    return PriceChart(
        polyline=polyline,
        area=area,
        width=_W,
        height=_H,
        last_close=series[-1].close,
        min_close=lo,
        max_close=hi,
        first_date=series[0].date,
        last_date=series[-1].date,
        up=series[-1].close >= series[0].close,
    )
