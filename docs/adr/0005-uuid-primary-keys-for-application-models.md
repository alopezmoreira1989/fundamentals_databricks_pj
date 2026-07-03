# ADR-0005: UUID primary keys for application models

- **Status:** Accepted
- **Date:** 2026-07-03  <!-- recorded retroactively; established with the first application models -->
- **Deciders:** repo owner

## Context

The Django app owns application data in PostgreSQL — users, watchlists, favorites, browsing
history — with more entities to come. Django's default primary key is a sequential
`BigAutoField`. Sequential integer keys are guessable and enumerable when exposed in URLs/APIs,
leak row counts and growth rate, and require a central sequence (awkward to generate keys
off-server or merge data from multiple sources). Switching a table's pk type *after* it has data
and foreign keys is a painful migration.

## Decision

Every **application** model declares an explicit UUID primary key from the start:

```python
id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
```

never `AutoField`/`BigAutoField`. `DEFAULT_AUTO_FIELD` stays `BigAutoField` because it governs
only Django's **framework** tables (auth/admin/sessions/contenttypes), which are not our
entities and are fine with integer keys.

## Consequences

- Keys are safe to expose in URLs and API responses — non-enumerable, and they don't leak table
  size or growth. `watchlist_id` in a URL reveals nothing and can't be walked.
- Keys can be generated anywhere without a central sequence, which suits a distributed system and
  makes merging/importing data across sources safe.
- No future "migrate the pk type" pain — the decision is paid once, up front, per model.
- Minor costs: a UUID pk is 16 bytes vs 8, and random UUIDv4s have worse index locality than
  monotonic integers. Negligible at this data scale; revisit (e.g. UUIDv7) only if it ever bites.
- A standing convention: every new application model must set the UUID pk explicitly. Enforced by
  review and noted in `architecture.md` → *Model conventions*.

## Alternatives considered

- **Default `BigAutoField`.** Rejected: enumerable in URLs, leaks counts, central sequence, and a
  costly pk migration if we change our mind after data exists.
- **Integer pk internally + a separate public UUID/slug column.** Rejected: two identifiers per
  row and the mapping to maintain, for no benefit over simply making the pk a UUID.
- **UUIDv7 (time-ordered) now.** Deferred: better index locality, but not needed at this scale and
  not first-class in the stack yet; UUIDv4 keeps it simple. Reconsider if index locality matters.
