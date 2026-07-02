"""Presentation-only display filters for the company page.

Formatting a number for the screen is a presentation concern, so it lives here — not in a
service or repository (and never any financial logic; the values are already computed by
``fundamentals_pipeline`` upstream).
"""

from __future__ import annotations

from django import template

register = template.Library()

_EMPTY = "—"  # em dash for missing values


@register.filter
def metric_value(value: float | None, unit: str | None = None) -> str:
    """Render a metric value for display, using its ``unit`` to pick the format.

    ``percent`` → ``-43.8%``; ``usd`` → ``$177.60``; anything else → a thousands-grouped
    number. ``None`` (missing) renders as an em dash.
    """
    if value is None:
        return _EMPTY
    u = (unit or "").lower()
    if u == "percent":
        return f"{value:,.1f}%"
    if u == "usd":
        return f"${value:,.2f}"
    return f"{value:,.2f}"
