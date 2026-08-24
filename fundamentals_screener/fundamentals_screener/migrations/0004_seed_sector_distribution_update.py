"""Seed the Updates entry documenting the Sector Distribution panel feature.

Per the Updates convention established in migrations/0003_seed_updates_journal_entry.py: a
meaningful engineering milestone leaves behind an Update alongside its code and tests. The
Sector Distribution panel (General Screener hero) shipped across several PRs (#408-#413) --
this entry documents the finished feature and its debugging arc as one retrospective account,
written after the fact once the layout bug was actually fixed, not contemporaneously with #408.
"""

from __future__ import annotations

from django.db import migrations

_SLUG = "sector-distribution-panel"

_ENTRY = {
    "title": "A reactive Sector Distribution panel, and three flexbox lessons",
    "slug": _SLUG,
    "category": "frontend",
    "published_at": "2026-08-23",
    "summary": (
        "The General Screener's hero now shows a live sector breakdown of the filtered result "
        "set, reusing the existing query and filter pipeline end to end -- plus what it took to "
        "get the layout actually right."
    ),
    "content": """\
### What changed

A horizontal-bar-chart "Sector Distribution" panel in the General Screener's hero, filling what
used to be empty space next to the icon and intro copy. It shows the sector breakdown of
whichever result set is currently filtered -- not the whole universe -- and reacts to every
filter: sector, index, industry, country, market, search, and any metric filter. Clicking a bar
applies that sector as a filter.

### Why

The hero had genuine unused space, and a sector breakdown is exactly the kind of at-a-glance
context a screener's landing view should surface -- but only if it is honest about what's
currently selected, not a static, always-the-same-numbers decoration.

### Implementation

One source of truth: `CompanyListingRepository.screen_table()` already builds the scoped
ticker set and metric-filter WHERE clause once per request to serve the results table's count
and paginated rows. The sector aggregate is one more query against those same variables, with
no `LIMIT` -- every filter recalculates it automatically, with no second filtering
implementation to keep in sync.

On the client, the panel piggybacks on the exact function every other filter already funnels
through (`applyForm()`), firing one extra small fetch alongside the existing results-table
swap. Clicking a sector bar sets the existing Sector dropdown's value and dispatches a native
`change` event -- so it reaches the results table and the panel through the very same code
path a manual dropdown selection would, with zero new client-side state.

### The layout bug, or: three ways flexbox will surprise you

Getting the panel's width right took three separate rounds, and static re-reading of the CSS
only got it two-thirds of the way -- each bug was ultimately confirmed by measuring the actual
rendered page with headless Chromium (Playwright), not by staring at the stylesheet harder.

`margin-left: auto` and `flex-grow` compete for the same free space on one element, and the
auto-margin claims it first -- removing the margin let the box actually grow. A `<span>` used as
a bar fill turned out to ignore `width`/`height` entirely: those properties don't apply to a
non-replaced *inline* element, and a flex/grid container only blockifies its own direct
children, not further-nested descendants -- an explicit `display: block` fixed it. And the
biggest one: the flex-sizing rules were on the included template's own outermost `<div>`, one
DOM level short of the actual flex item, which was its AJAX-swap wrapper one level up. With no
sizing rules of its own, that wrapper never grew past its natural content width -- so the two
earlier fixes were each correctly fixing a real bug, just not the one actually limiting the
width.

### Result

A live, reactive sector panel that fills its full available width, and a debugging pattern worth
reusing next time a flex/grid layout resists an obvious fix: measure the real rendered DOM
before trying a third static-reasoning guess.
""",
    "is_published": True,
}


def seed_entry(apps, schema_editor):
    Update = apps.get_model("fundamentals_screener", "Update")
    Update.objects.update_or_create(slug=_SLUG, defaults=_ENTRY)


def remove_entry(apps, schema_editor):
    Update = apps.get_model("fundamentals_screener", "Update")
    Update.objects.filter(slug=_SLUG).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("fundamentals_screener", "0003_seed_updates_journal_entry"),
    ]

    operations = [
        migrations.RunPython(seed_entry, remove_entry),
    ]
