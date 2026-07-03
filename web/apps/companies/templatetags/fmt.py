"""Presentation-only display filters for the company page.

Formatting a number for the screen is a presentation concern, so it lives here — not in a
service or repository (and never any financial logic; the values are already computed by
``fundamentals_pipeline`` upstream).
"""

from __future__ import annotations

from collections.abc import Sequence

from django import template
from django.utils.safestring import SafeString, mark_safe

from apps.companies.charts import sparkline_svg

register = template.Library()

_EMPTY = "—"  # em dash for missing values


@register.filter
def sparkline(values: Sequence[float | None] | None) -> SafeString:
    """Inline-SVG trend sparkline for a row's values (newest-first → reversed to chronological).

    The SVG is generated from numbers only (no user data), so marking it safe is sound."""
    series = list(values) if values else []
    return mark_safe(sparkline_svg(list(reversed(series))))  # noqa: S308 - numeric-only SVG


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


@register.filter
def compact_money(value: float | None) -> str:
    """Compact financial-statement figure: ``$391.04B``, ``15.3M``, or ``6.11`` for small
    per-share values. ``None`` renders as an em dash. Sign is preserved."""
    if value is None:
        return _EMPTY
    magnitude = abs(value)
    if magnitude >= 1e12:
        return f"{value / 1e12:,.2f}T"
    if magnitude >= 1e9:
        return f"{value / 1e9:,.2f}B"
    if magnitude >= 1e6:
        return f"{value / 1e6:,.1f}M"
    if magnitude >= 1e3:
        return f"{value / 1e3:,.1f}K"
    return f"{value:,.2f}"


@register.filter
def sign_class(value: float | None) -> str:
    """Bootstrap text colour for a signed value: green ≥ 0, red < 0, empty for missing.

    On a Margin-of-Safety figure this reads as green = undervalued, red = overvalued.
    """
    if value is None:
        return ""
    return "text-success" if value >= 0 else "text-danger"
