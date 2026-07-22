"""Presentation-only display filters for the company page.

Formatting a number for the screen is a presentation concern, so it lives here — not in a
repository (and never any financial logic; the values are already computed upstream).
"""

from __future__ import annotations

from collections.abc import Sequence

from django import template
from django.utils.html import format_html
from django.utils.safestring import SafeString, mark_safe

from ..charts import sparkline_svg

register = template.Library()

_EMPTY = "—"  # em dash for missing values


def currency_badge(currency: str | None) -> str:
    """Small inline label for a non-USD currency; "" for USD/empty (no badge shown)."""
    if not currency or currency.upper() == "USD":
        return ""
    return format_html('<span class="ccy-badge">{}</span>', currency.upper())


@register.filter
def sparkline(values: Sequence[float | None] | None) -> SafeString:
    """Inline-SVG trend sparkline for a row's values (newest-first → reversed to chronological).

    The SVG is generated from numbers only (no user data), so marking it safe is sound."""
    series = list(values) if values else []
    return mark_safe(sparkline_svg(list(reversed(series))))  # noqa: S308 - numeric-only SVG


@register.filter
def metric_value(value: float | None, unit: str | None = None) -> str:
    """Render a metric value for display, using its ``unit`` to pick the format.

    ``percent`` → ``-43.8%``; ``usd`` → ``$177.60``; any other 3-letter code (e.g. ``cad``, the
    ticker's real native currency for Market Cap / raw price) → ``177.60 CAD`` with a currency
    badge; anything else → a thousands-grouped number. ``None`` (missing) renders as an em dash.
    """
    if value is None:
        return _EMPTY
    u = (unit or "").lower()
    if u == "percent":
        return f"{value:,.1f}%"
    if u == "usd":
        return f"${value:,.2f}"
    if len(u) == 3 and u.isalpha():
        return format_html("{} {}", f"{value:,.2f}", currency_badge(u))
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
def compact_money_ccy(value: float | None, currency: str | None = None) -> str:
    """Like :func:`compact_money`, but prefixes ``$`` only for USD/unset — any other currency
    (e.g. a Canadian ticker's native Market Cap) gets the compact figure plus a badge instead,
    never a ``$`` it doesn't actually trade in."""
    if value is None:
        return _EMPTY
    body = compact_money(value)
    ccy = (currency or "USD").upper()
    if ccy == "USD":
        return f"${body}"
    return format_html("{} {}", body, currency_badge(ccy))


@register.filter
def delta_chip(metric) -> str:
    """Small ▲/▼ chip (signed pp magnitude, e.g. "+8.3pp") next to a percent-unit metric's own
    Latest value, vs. the active benchmark. Empty for non-percent metrics (unit == "percent" is
    the exact gate — matches metric_value's own check) or when either side is missing. Reuses
    the existing text-success/text-danger convention (see sign_class below, used on this same
    page's Valuation tab) rather than introducing new color classes."""
    if (metric.unit or "").lower() != "percent" or metric.peer_median is None or not metric.values or metric.values[0] is None:
        return ""
    diff = metric.values[0] - metric.peer_median
    if abs(diff) < 0.05:
        return ""
    arrow = "▲" if diff >= 0 else "▼"
    css = "text-success" if diff >= 0 else "text-danger"
    magnitude = f"{diff:+.1f}pp"  # format the spec here — format_html escapes args to plain
    # strings before .format() runs, so a format spec like {:+.1f} on an already-escaped arg
    # would raise ValueError (numeric specs are invalid on str); pass a pre-formatted string.
    return format_html(' <span class="small {}">{} {}</span>', css, arrow, magnitude)


@register.filter
def sign_class(value: float | None) -> str:
    """Bootstrap text colour for a signed value: green ≥ 0, red < 0, empty for missing.

    On a Margin-of-Safety figure this reads as green = undervalued, red = overvalued.
    """
    if value is None:
        return ""
    return "text-success" if value >= 0 else "text-danger"
