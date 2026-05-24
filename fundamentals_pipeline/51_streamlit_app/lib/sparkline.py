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