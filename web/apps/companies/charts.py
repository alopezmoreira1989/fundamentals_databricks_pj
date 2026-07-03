"""Server-side inline-SVG chart helpers for the company page.

Pure geometry, no I/O and no financial logic — mirrors the Streamlit dashboard's sparklines
(``lib/sparkline.py``) so the two front-ends read the same. Kept unit-testable, like
:mod:`apps.companies.pricechart`.
"""

from __future__ import annotations

from collections.abc import Sequence

_ACCENT = "#185FA5"  # --accent (kept in sync with the editorial theme)


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
