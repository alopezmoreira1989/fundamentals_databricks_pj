# ADR-0003: Strict one-directional layering with a repository tier

- **Status:** Accepted
- **Date:** 2026-07-03  <!-- recorded retroactively; established as the web layer was built -->
- **Deciders:** repo owner

## Context

The Django app reads analytical data from Parquet artifacts (via DuckDB today) and stores
application data in PostgreSQL. Without a rule, data access tends to spread: a view runs a quick
query, a template reaches into a DataFrame, a service hard-codes a SQL column name. That couples
the whole app to one storage engine and to physical column names, so a backend swap or a schema
change ripples everywhere.

A concrete future is in view: the analytical store may move from **DuckDB-over-Parquet** to
**Databricks SQL / Postgres / Snowflake / BigQuery**. We want that swap to be a localized change.

## Decision

We will enforce a strict, one-directional layering:

```
views  →  services  →  repositories  →  infrastructure (DuckDB / PostgreSQL)  →  fundamentals_pipeline
```

Each tier calls only the one below it. **Repositories are the only tier that touches persistent
storage**; they return immutable DTOs (frozen dataclasses), own all SQL (parameterized, no
`SELECT *`), and push filter/aggregate/LIMIT into the query engine. Views handle HTTP only;
services orchestrate use cases and call `fundamentals_pipeline` for business logic; neither ever
executes SQL or sees a storage column name.

The **litmus test:** swapping the storage engine must require changes **only** in
`repositories/` + `infrastructure/`. If a storage swap would force an edit to a view, a service,
or `fundamentals_pipeline`, that code is reaching past its layer and must be moved.

## Consequences

- Storage is replaceable in isolation; the DTOs repositories return, and everything above them,
  are insulated from the engine and from physical column names.
- Testability: services and views are tested against DTOs; repositories are the single seam to
  point at fixtures or a different backend.
- A cost in indirection and discipline — every persistent read goes through a repository and a
  DTO, even when a direct query would be shorter. ADR-0006 carves out the one narrow exception
  (trivial ORM CRUD) so the rule adds value rather than boilerplate.
- Enforced continuously (see `architecture.md` → *Data access architecture*), and reinforced by
  mypy + the web test suite.

## Alternatives considered

- **Fat views / "thin controller, fat model" Django default.** Rejected: couples HTTP and SQL,
  and there is no natural home for the storage-abstraction seam the replaceability goal needs.
- **Service layer but no repository tier (services run SQL).** Rejected: services would then know
  the storage engine and column names, breaking the litmus test on the first backend swap.
- **A generic ORM/repository over everything, including trivial CRUD.** Rejected as boilerplate;
  handled by the ADR-0006 exception.
