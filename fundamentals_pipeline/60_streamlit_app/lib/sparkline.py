"""SVG sparkline generator — used by table rows and the derived-metrics grid."""

from __future__ import annotations

from typing import Iterable, Optional

from .format import is_missing


def sparkline_svg(
    values: Iterable,
    color: str = "#185FA5",
    width: int = 78,
    height: int = 20,
    stroke: float = 1.5,
    end_circle: bool = False,
) -> str:
    """Render a sparkline as a raw inline SVG string.

    `values` should be in chronological order (oldest first). Missing values
    (None / NaN) are skipped — the polyline just jumps. If fewer than 2 valid
    points exist, returns an empty string.
    """
    vals = list(values)
    valid = [(i, v) for i, v in enumerate(vals) if not is_missing(v)]
    if len(valid) < 2:
        return ""

    vmin = min(v for _, v in valid)
    vmax = max(v for _, v in valid)
    span = (vmax - vmin) or 1
    n = len(vals)

    def x(i: int) -> float:
        return 2 + i * (width - 4) / max(n - 1, 1)

    def y(v: float) -> float:
        return 18 - 16 * (v - vmin) / span   # inverted: SVG y grows downward

    points = " ".join(f"{x(i):.1f},{y(v):.1f}" for i, v in valid)
    parts = [
        f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        f'<polyline points="{points}" fill="none" stroke="{color}" stroke-width="{stroke}"/>',
    ]
    if end_circle:
        last_i, last_v = valid[-1]
        parts.append(
            f'<circle cx="{x(last_i):.1f}" cy="{y(last_v):.1f}" r="2.3" fill="{color}"/>'
        )
    parts.append("</svg>")
    return "".join(parts)


def mini_sparkline_svg(
    values: Iterable,
    color: str = "#185FA5",
) -> str:
    """Smaller sparkline for the derived metrics grid (60×16)."""
    return sparkline_svg(values, color=color, width=60, height=16, stroke=1.5)


def mini_bars_svg(
    values,
    color: str = "#0F6E56",
    n: int = 5,
    width: int = 64,
    height: int = 26,
) -> str:
    """Mini bar chart for the last `n` years; last bar in color, the rest muted."""
    vals = [v for v in values if not is_missing(v)][-n:]
    if len(vals) < 2:
        return ""
    vmin, vmax = min(vals), max(vals)
    span = (vmax - vmin) or 1
    bw = (width - (len(vals) - 1) * 3) / len(vals)
    bars = []
    for i, v in enumerate(vals):
        h = 4 + (v - vmin) / span * (height - 6)
        x = i * (bw + 3)
        fill = color if i == len(vals) - 1 else "#CDD9CF"   # last bar highlighted
        bars.append(
            f'<rect x="{x:.1f}" y="{height - h:.1f}" width="{bw:.1f}" '
            f'height="{h:.1f}" rx="1" fill="{fill}"/>'
        )
    return f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}">{"".join(bars)}</svg>'