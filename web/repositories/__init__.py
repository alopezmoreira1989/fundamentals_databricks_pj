"""Repositories tier — the gateway to persistent data (where it earns its place).

Home for the **shared** repositories reused across apps (the DuckDB/artifact reads behind
``companies``/``screener``/``valuation``). A repository maps raw rows to domain DTOs and
hides storage details from the callers above it; it is the only place allowed to touch the
``infrastructure`` tier (the DuckDB engine / artifact storage). Services call repositories —
views never do.

Dependency rule (enforced project-wide — see ``docs/architecture.md``):
    views → services → repositories → infrastructure (DuckDB / PostgreSQL) → fundamentals_pipeline

Judgment over mechanism (do not add boilerplate):
  * **Mandatory** behind a repository — DuckDB, Parquet artifacts, a future Databricks SQL
    backend, any analytical/external storage. Never queried from a view or service directly.
  * **Exception** — trivial Django-ORM CRUD (e.g. ``CustomUser``) may use the ORM directly
    from the service layer; introduce a repository only once the access provides real value
    (complex/composed query, DTO mapping across models, aggregation, caching, second source).

A repository may call ``fundamentals_pipeline`` when a returned value needs business logic,
but it never reimplements a formula that lives there.

Populated as endpoints are built; empty in the scaffold phase.
"""
