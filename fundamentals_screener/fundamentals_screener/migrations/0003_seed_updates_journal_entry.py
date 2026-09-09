"""Seed the Updates section's own entry, documenting its own creation.

Unlike migrations/0002_seed_initial_updates.py's ten entries (retrospective, reconstructing
past work), this one is contemporaneous — written as part of the same PR that builds the
feature it describes, per the project's own Updates convention (a meaningful milestone is
expected to leave behind an Update alongside its code and tests). It therefore does NOT carry
the "Retrospective entry" framing the other ten use.

A separate migration rather than editing 0002 — that one documents a distinct, already-reviewed
batch of historical content; this is new content, not a correction to it.
"""

from __future__ import annotations

from django.db import migrations

_SLUG = "public-development-journal"

_ENTRY = {
    "title": "A public development journal: how this project documents itself",
    "slug": _SLUG,
    "category": "architecture",
    "published_at": "2026-08-21",
    "summary": (
        "This Updates section, and the model behind it, are themselves the newest entry: a "
        "small, deliberate exception to the package's database-free architecture."
    ),
    "content": """\
### What changed

A public Updates / development-journal section (`/updates/`, `/updates/<slug>/`,
`/updates/feed/`) — this page. Ten retrospective entries, reconstructing the project's history
from its git log, ADRs, and pipeline configuration, were seeded alongside it; each was checked
against the actual commits and configuration it describes before publishing, and a follow-up
review pass caught and corrected two inaccuracies (a mis-scoped tooling claim and a stale
cross-reference) before this went out.

### Why

This project doubles as a portfolio artifact. A git log is a complete record but not a
readable one — it doesn't explain *why* a decision was made, or that an earlier one (like the
Postgres-backed `web/` app) was deliberately reversed rather than simply superseded. The project
already had a habit of writing that reasoning down, in ADRs and phase-completion docs under
`docs/`; this makes the same kind of record visible on the public site itself, not just in the
repository.

### Implementation

One new Django model, `Update` — the first ORM/database dependency `fundamentals_screener` has
had since it was rebuilt read-only against published parquet artifacts (see "A Django web app
with Postgres — and why it was retired a month later"). Rather than quietly reopening that
decision, it's documented as a deliberate, narrow exception in ADR-0013, scoped to this feature
alone. Content is Markdown, rendered through a new `markdown` dependency; the RSS feed uses
Django's built-in syndication framework, so it added no dependency of its own.

### Result

A public `/updates/` section, and a written convention for keeping it current going forward:
a meaningful engineering milestone — a new data source, a market expansion, a significant
architectural change — is now expected to leave behind an Update alongside its code and tests,
not just a commit message.
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
        ("fundamentals_screener", "0002_seed_initial_updates"),
    ]

    operations = [
        migrations.RunPython(seed_entry, remove_entry),
    ]
