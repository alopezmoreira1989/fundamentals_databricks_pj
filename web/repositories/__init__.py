"""Repositories tier — the single gateway to persistent data.

**Every** read/write of persistent data goes through a repository. A repository is the only
place allowed to touch the ``infrastructure`` tier (the DuckDB engine / artifact storage)
or the Django ORM; it maps raw rows to domain DTOs and hides the storage details from the
callers above it. Services call repositories — views never do, and views/services never
query DuckDB or PostgreSQL directly.

Dependency rule (enforced project-wide — see ``docs/architecture.md``):
    views → services → repositories → infrastructure (DuckDB / PostgreSQL) → fundamentals_pipeline

A repository may call ``fundamentals_pipeline`` when returning a value needs business logic,
but it never reimplements a formula that lives there.

Populated as endpoints are built; empty in the scaffold phase.
"""
