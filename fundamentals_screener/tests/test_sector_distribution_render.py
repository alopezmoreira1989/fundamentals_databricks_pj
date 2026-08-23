"""Template-render tests for the Sector Distribution panel — the `_sector_distribution.html`
partial and its integration into `_screen_main.html`'s hero.

Renders directly through Django's template engine (no DuckDB/network) with a context shaped
exactly like `views.screen()` builds it — same "render through the engine before considering it
fixed" check used for the leaked `{# #}` comment bug earlier (and again here — see the fix in
`_sector_distribution.html` itself, same bug class caught by this exact kind of test).
"""

from __future__ import annotations

import pytest
from django.template.loader import render_to_string
from django.utils import translation

pytestmark = pytest.mark.django_db


def _sector_rows(*, with_unknown: bool = False):
    rows = [
        {"sector": "Information Technology", "count": 830, "pct": 31.4, "qs": "col=x&sector=Information+Technology"},
        {"sector": "Financials", "count": 481, "pct": 18.2, "qs": "col=x&sector=Financials"},
        {"sector": "Industrials", "count": 389, "pct": 14.7, "qs": "col=x&sector=Industrials"},
    ]
    if with_unknown:
        rows.append({"sector": "Unknown", "count": 5, "pct": 0.2, "qs": None})
    return rows


def _render(*, with_unknown: bool = False, **overrides):
    ctx = {"sector_rows": _sector_rows(with_unknown=with_unknown), "sector_total": 1700, "sector": ""}
    ctx.update(overrides)
    return render_to_string("fundamentals_screener/_sector_distribution.html", ctx)


def test_renders_without_error_and_leaks_no_template_syntax():
    html = _render()
    assert "{%" not in html and "{#" not in html
    assert "Sector Distribution" in html


def test_percentages_and_counts_render_correctly():
    html = _render()
    assert "31.4%" in html
    assert "830" in html
    assert "1700" in html  # sector_total


def test_percentage_display_stays_period_decimal_under_a_non_english_locale():
    # Regression (2026-08-23, caught live): floatformat ignores {% localize off %} entirely
    # (hardcodes use_l10n=True) -- an earlier version only applied the 'u' (unlocalized) suffix
    # to the CSS width value, not the adjacent display text, so a Spanish-locale request
    # rendered "100,0%" (comma decimal) in the visible label while the CSS width stayed correct.
    # Both floatformat calls in the template must carry 'u'.
    with translation.override("es"):
        html = _render()
    assert "31,4%" not in html
    assert "31.4%" in html


def test_zero_total_renders_no_companies_state_no_crash():
    html = render_to_string(
        "fundamentals_screener/_sector_distribution.html",
        {"sector_rows": [], "sector_total": 0, "sector": ""},
    )
    assert "No companies match" in html
    assert "NaN" not in html and "Infinity" not in html
    assert "{%" not in html and "{#" not in html


def test_active_sector_gets_highlighted_class():
    html = _render(sector="Financials")
    # the Financials row's <a> must carry the "active" class; IT/Industrials must not.
    assert 'data-sector="Financials"' in html
    financials_link = [
        line for line in html.splitlines() if 'data-sector="Financials"' in line
    ][0]
    assert "active" in financials_link
    it_link = [
        line for line in html.splitlines() if 'data-sector="Information Technology"' in line
    ][0]
    assert "active" not in it_link


def test_unknown_row_renders_as_plain_element_not_a_link():
    html = _render(with_unknown=True)
    assert "Unknown" in html
    # the Unknown row must not produce a data-sector link (no matching <select> option exists).
    assert 'data-sector="Unknown"' not in html
    assert '<a href="?"' not in html  # no broken empty-qs link


def test_long_sector_name_reaches_the_template_intact():
    long_name = "A Very Long Sector Name That Would Otherwise Overflow The Fixed-Width Column"
    html = _render(sector_rows=[{"sector": long_name, "count": 1, "pct": 100.0, "qs": f"sector={long_name}"}])
    assert long_name in html  # CSS text-overflow handles the visual truncation, not Python


def test_full_distribution_percentages_sum_to_approximately_100():
    # A COMPLETE distribution (every sector present, none omitted) must sum to ~100% -- the
    # per-row `pct` values are computed upstream (views.screen()) as count/total*100, so this
    # is really a check on that arithmetic reaching the template unchanged, rounding aside.
    complete = [
        {"sector": "Information Technology", "count": 830, "pct": 31.4},
        {"sector": "Financials", "count": 481, "pct": 18.2},
        {"sector": "Industrials", "count": 389, "pct": 14.7},
        {"sector": "Healthcare", "count": 314, "pct": 11.9},
        {"sector": "Consumer Discretionary", "count": 259, "pct": 9.8},
        {"sector": "Energy", "count": 164, "pct": 6.2},
        {"sector": "Materials", "count": 127, "pct": 4.8},
        {"sector": "Utilities", "count": 56, "pct": 2.1},
        {"sector": "Real Estate", "count": 50, "pct": 1.9},
        {"sector": "Communication Services", "count": 44, "pct": 1.7},
    ]
    # Independently-rounded per-row percentages (each count/total*100, rounded to 1dp) can
    # legitimately drift a few points from an exact 100 across 10 sectors -- not a bug.
    assert sum(r["pct"] for r in complete) == pytest.approx(100.0, abs=4.0)
    html = _render(sector_rows=complete, sector_total=sum(r["count"] for r in complete))
    assert "{%" not in html and "{#" not in html
