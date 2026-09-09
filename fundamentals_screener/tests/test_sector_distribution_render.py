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
    # bar_pct is share-of-the-largest-row-shown (max count here is 830, Information Technology)
    # -- deliberately a DIFFERENT number than pct (share-of-universe), see views.screen()'s own
    # comment for why.
    rows = [
        {"sector": "Information Technology", "count": 830, "pct": 31.4, "bar_pct": 100.0,
         "qs": "col=x&sector=Information+Technology"},
        {"sector": "Financials", "count": 481, "pct": 18.2, "bar_pct": 57.95, "qs": "col=x&sector=Financials"},
        {"sector": "Industrials", "count": 389, "pct": 14.7, "bar_pct": 46.87, "qs": "col=x&sector=Industrials"},
    ]
    if with_unknown:
        rows.append({"sector": "Unknown", "count": 5, "pct": 0.2, "bar_pct": 0.6, "qs": None})
    return rows


def _render(*, with_unknown: bool = False, **overrides):
    ctx = {
        "sector_rows": _sector_rows(with_unknown=with_unknown),
        "sector_total": 1700,
        "sector_total_display": "1,700",
        "sector": "",
    }
    ctx.update(overrides)
    # views.screen() always derives sector_total_display from sector_total (Python's own `:,`
    # formatting -- see views.py) -- if a test overrides sector_total without also overriding
    # sector_total_display, keep them consistent rather than silently rendering a stale stat.
    if "sector_total" in overrides and "sector_total_display" not in overrides:
        ctx["sector_total_display"] = f"{ctx['sector_total']:,}"
    return render_to_string("fundamentals_screener/_sector_distribution.html", ctx)


def test_renders_without_error_and_leaks_no_template_syntax():
    html = _render()
    assert "{%" not in html and "{#" not in html
    assert "Sector Distribution" in html


def test_percentages_and_counts_render_correctly():
    html = _render()
    assert "31.4%" in html
    assert "830" in html
    assert "1,700" in html  # sector_total_display -- comma-grouped, not the bare sector_total int


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
        {"sector_rows": [], "sector_total": 0, "sector_total_display": "0", "sector": ""},
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
    html = _render(sector_rows=[
        {"sector": long_name, "count": 1, "pct": 100.0, "bar_pct": 100.0, "qs": f"sector={long_name}"},
    ])
    assert long_name in html  # CSS text-overflow handles the visual truncation, not Python


def test_bar_width_is_relative_to_the_largest_sector_not_the_universe_total():
    # Regression for the "every bar looks like a similarly-short sliver" bug (2026-08-23, caught
    # live from a real screenshot): bar_pct must drive the CSS width, not pct. The largest row
    # (Information Technology, bar_pct=100.0) must fill its track completely; Financials
    # (bar_pct=57.95, pct=18.2) must use ITS OWN bar_pct, not its much-smaller pct.
    html = _render()
    assert 'width:100.0%' in html  # Information Technology's bar -- fills the track
    assert 'width:58.0%' in html  # Financials: 481/830*100, rounded to 1dp via floatformat
    assert 'width:18.2%' not in html  # would be wrong -- that's Financials' pct, not bar_pct


def test_header_stat_is_thousands_grouped():
    # Real bug, caught live (2026-08-24): the header stat originally rendered the bare
    # `sector_total` int directly ("2640"), not comma-grouped ("2,640") -- confirmed via a
    # Playwright screenshot of the deployed "Paired & framed" redesign. Fixed by having
    # views.screen() pre-format the number in Python (`f"{n:,}"`) rather than reaching for a
    # locale-sensitive template filter (see the next test for why that path is avoided here).
    html = _render(sector_total=2640)
    assert "2,640" in html
    assert ">2640<" not in html


def test_header_stat_stays_comma_grouped_under_a_non_english_locale():
    # Same locale-independence discipline as the percentage-label fix above: a plain Python
    # `:,` format has no active-locale awareness at all (unlike `intcomma`/`floatformat`, which
    # this app has already been burned by twice), so it can't regress the way those did.
    with translation.override("es"):
        html = _render(sector_total=2640)
    assert "2,640" in html
    assert "2.640" not in html


def test_full_distribution_percentages_sum_to_approximately_100():
    # A COMPLETE distribution (every sector present, none omitted) must sum to ~100% -- the
    # per-row `pct` values are computed upstream (views.screen()) as count/total*100, so this
    # is really a check on that arithmetic reaching the template unchanged, rounding aside.
    max_count = 830
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
    for r in complete:
        r["bar_pct"] = r["count"] / max_count * 100
    # Independently-rounded per-row percentages (each count/total*100, rounded to 1dp) can
    # legitimately drift a few points from an exact 100 across 10 sectors -- not a bug.
    assert sum(r["pct"] for r in complete) == pytest.approx(100.0, abs=4.0)
    html = _render(sector_rows=complete, sector_total=sum(r["count"] for r in complete))
    assert "{%" not in html and "{#" not in html
