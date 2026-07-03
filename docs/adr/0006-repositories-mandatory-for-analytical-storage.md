# ADR-0006: Repositories mandatory for analytical storage; trivial ORM CRUD may skip them

- **Status:** Accepted
- **Date:** 2026-07-03  <!-- recorded retroactively; refined while building users/watchlists/favorites -->
- **Deciders:** repo owner

## Context

ADR-0003 routes every persistent read through a repository. Applied literally to *everything*,
that produces pass-through repositories that only re-export the Django ORM API — e.g. a
`UserRepository.get(pk)` wrapping `User.objects.get(pk=...)`. That is indirection without value:
it adds a file and a DTO mapping that duplicate what the ORM already gives you as typed model
instances, and it makes trivial CRUD harder to read, not easier to change.

At the same time, the storage-replaceability goal (ADR-0003, ADR-0004) depends on **analytical**
storage — DuckDB/Parquet, a future Databricks SQL backend — always sitting behind a repository.
That is precisely the seam a backend swap needs; it must never be bypassed, no matter how simple
a query looks.

## Decision

The repository tier must add architectural value, judged case by case:

- **Mandatory — always behind a repository:** DuckDB, the Parquet artifacts, and any future
  analytical/external backend (Databricks SQL, Snowflake, …). Absolute, regardless of how trivial
  the query appears — these are the replaceability cases the goal protects.
- **Exception — trivial Django-ORM CRUD:** simple create/read/update/delete on ORM-backed
  application models (e.g. the custom user, watchlists) may use the ORM **directly from the
  service layer** when a repository would only mirror the ORM API.

The exception is narrow — it covers ORM CRUD only. It ends the moment the access earns a value
bullet: a non-trivial/composed query, DTO mapping across models, aggregation, caching, or hiding
a second data source. Analytical storage is never touched from a view or service.

## Consequences

- No boilerplate pass-through repositories; trivial user/watchlist/favorite CRUD lives in
  `apps/<name>/services.py` against the ORM and reads plainly.
- The replaceability guarantee is intact: analytical reads still funnel through repositories +
  `infrastructure/`, so an engine swap stays a repository-tier change.
- The rule is judgment, not mechanism — it needs a shared understanding of "trivial" and a
  willingness to promote CRUD to a repository when it grows a real query. `architecture.md` →
  *When a repository earns its place* is the reference, with the promotion triggers spelled out.

## Alternatives considered

- **Repository for everything, no exception.** Rejected: pass-through repositories over the ORM
  are indirection without value and make simple CRUD less readable.
- **No mandatory rule — repositories only "when useful".** Rejected: someone would eventually
  query DuckDB straight from a service for a "simple" read, breaking the replaceability seam
  exactly where it matters most.
- **Repositories for ORM data too, DTOs everywhere.** Rejected for application CRUD: Django model
  instances are already typed domain objects; a parallel DTO layer duplicates them for no gain.
