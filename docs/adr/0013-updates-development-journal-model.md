# ADR-0013: A single Django `Update` model for the public development journal

- **Status:** Accepted
- **Date:** 2026-08-18
- **Deciders:** repo owner

## Context

`fundamentals_screener` (ADR-0002, ADR-0008) is deliberately database-free: every view reads
precomputed data from published parquet artifacts via DuckDB (`repository.py`/`data_source.py`),
with no Django ORM, no migrations, no `models.py`/`admin.py`, and no database dependency at all.
ADR-0008 retired the app's predecessor (`web/`) specifically because it assumed infrastructure
(PostgreSQL, a persistent process, user accounts) the real CGI deployment target doesn't have —
and it explicitly states that a future feature wanting a real model or persisted data "should
write its own new ADR" rather than treating `web/`'s prior choices as a precedent.

A public "Updates" section — a development journal documenting the project's own engineering
history, for site visitors and as a portfolio artifact — was requested. This is qualitatively
different content from anything else in the package: a handful of maintainer-authored entries
(not financial data), edited occasionally (not published by an automated sync), where
Markdown/rich text, draft/published state, and a familiar content-editing UI (Django admin) are
the natural fit.

## Decision

We will add exactly one Django model, `Update`, backed by the host project's own database (via
migrations this package ships and the host applies with `manage.py migrate`), for this feature
only. No other feature in this package gains a database dependency as a result of this
decision, and this ADR does not reopen ADR-0008's "no ORM/PostgreSQL for financial data" choice.

`Update` is intentionally small: `title`, `slug` (unique), `summary`, `content` (Markdown),
`category`, `published_at`, `is_published`, `created_at`, `updated_at`. No `author` field — the
project has one maintainer, and the field would carry no information. Content is rendered from
Markdown via the `markdown` package (a new, single, pure-Python runtime dependency) rather than
a CMS or rich-text editor, since Django admin's plain `Textarea` is already sufficient for
maintainer-authored technical writing.

Ten initial historical entries are seeded via a data migration
(`migrations/0002_seed_initial_updates.py`), reconstructed from the project's real git history —
not invented — each marked in its own text as a retrospective entry.

## Consequences

**Easier:** the project can now publish a real changelog/dev-journal without inventing a second
content system; a future entry is a normal Django admin edit, not a code change or a template
edit.

**Harder / new obligations:**
- A host project must now run `manage.py migrate` after upgrading past this version (previously
  never required for this package). This is a real, host-visible change to the install contract
  documented in the package README.
- This package's own test suite, previously entirely Django-config-free, now has one test module
  (`tests/test_updates.py`) that needs a real Django settings module and database
  (`tests/settings.py`, `pytest-django`) — a new pattern for this codebase, isolated to that one
  file via `pytest.mark.django_db`; every other test file is untouched and still runs with zero
  Django configuration.
- `fundamentals_screener`'s "no auth, no user-data apps" README claim still holds for anything
  *depending on* auth (this feature needs none — Django admin's own auth is the host's existing
  concern, not a new dependency this app introduces) — but the README is updated to note the
  package now ships one small schema migration.

**Invariant a future change must preserve:** no other feature in this package should acquire a
database dependency by "because `Update` already has one" — each one needs its own justification
and, per ADR-0008's own instruction, its own ADR if it wants to reopen this boundary further.

## Alternatives considered

- **Hardcoded template/Markdown files, no model.** Rejected: the request explicitly asked for a
  model-backed, admin-manageable feature, and flat files would mean a content edit requires a
  code deploy — the exact friction a changelog is meant to avoid.
- **A third-party CMS / rich-text editor (Wagtail, django-ckeditor, etc.).** Rejected as
  disproportionate to ~10 maintainer-authored, long-form technical entries with no non-technical
  authors, no editorial workflow, and no need for a WYSIWYG editor.
- **Store entries as parquet, read through the existing DuckDB layer, to avoid an ORM
  dependency entirely.** Rejected: that layer exists specifically for data the Databricks
  pipeline publishes on a schedule; Updates are hand-authored, edited occasionally, and want
  Django admin's create/edit/draft workflow, which the DuckDB read-only layer has no equivalent
  of and isn't designed to grow one for.
