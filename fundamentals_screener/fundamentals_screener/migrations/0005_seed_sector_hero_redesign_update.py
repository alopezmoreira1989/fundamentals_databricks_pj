"""Seed the Updates entry documenting the "Paired & framed" hero redesign.

Per the Updates convention (see migrations/0003_seed_updates_journal_entry.py): a meaningful
milestone leaves behind an Update alongside its code and tests. This one is contemporaneous --
written as part of the same PR that ships the redesign it describes, unlike 0004's own entry
(written after the fact, once the feature and its debugging arc were both finished).
"""

from __future__ import annotations

from django.db import migrations

_SLUG = "sector-panel-paired-and-framed"

_ENTRY = {
    "title": "Redesigning the hero: from three loose blocks to one paired composition",
    "slug": _SLUG,
    "category": "frontend",
    "published_at": "2026-08-24",
    "summary": (
        "The Sector Distribution panel and the hero's icon/intro no longer just sit next to "
        "each other -- they're grouped, height-matched, and framed as one designed unit."
    ),
    "content": """\
### What changed

The General Screener hero's icon, intro copy, and Sector Distribution panel used to be three
independent flex children in a row -- each sized by its own content, with no shared height or
baseline. The icon+intro block is now grouped into one unit and vertically centered; the sector
panel is framed as its own shaded sub-card with a headline stat ("2,640 companies") in place of
the old bottom total row. Both sides now share exactly one row height, set by whichever is
naturally taller -- almost always the sector panel, whose row count changes with the active
filters.

### Why

Fixing the panel's own width (see the previous Update) solved one problem and exposed another:
even at full width, the hero read as loosely arranged blocks that happened to occupy the same
row, not a single composition. The space beneath the (usually shorter) intro text looked like an
accident rather than a choice.

### Implementation

Three concrete layout options were mocked up first -- a full-width stacked strip, this paired
and framed version, and a smaller tuning of the existing three-column row -- each a faithful
replica of the live card's own fonts, colors, and spacing tokens, so the comparison was real
rather than a sketch. This one was picked because it fixes both complaints (drift between
blocks, dead space) without restructuring the sector panel's own row layout.

Mechanically: `.nn-hero-top` switched to `align-items: stretch`; a new `.scr-hero-left` wrapper
groups the icon and intro with `align-items: center`, so they center within whatever height the
row ends up being rather than sitting flush at the top. The sector panel's own root element
gained a background, border, and padding -- turning what used to look like unused space into
the card's own visible padding -- and its total moved from a trailing row into a header-line
stat, styled the same way the Net-Net Finder's own headline numbers already are.

### Result

One hero composition instead of three loosely related pieces, verified locally (both a
1900px-wide layout and a 760px narrow one, where the two blocks stack cleanly) before shipping.
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
        ("fundamentals_screener", "0004_seed_sector_distribution_update"),
    ]

    operations = [
        migrations.RunPython(seed_entry, remove_entry),
    ]
