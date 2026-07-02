"""Services tier — application / use-case orchestration.

The layer views call. A service coordinates one use case: it invokes the ``repositories`` tier for analytical
data (never a storage engine directly), calls ``fundamentals_pipeline`` when business logic
is required, and returns plain DTOs/primitives to the view. It holds orchestration, not
persistence details and not financial formulas — and never executes raw SQL.

This package is for **cross-cutting** services shared by more than one app; single-app
workflow lives in ``apps/<name>/services.py``.

Dependency rule (enforced project-wide — see ``docs/architecture.md``):
    views → services → repositories → infrastructure (DuckDB / PostgreSQL) → fundamentals_pipeline

Exception: for trivial Django-ORM CRUD (e.g. ``CustomUser``), a service may use the ORM
directly rather than a pass-through repository — see ``docs/architecture.md`` (*When a
repository earns its place*). This never extends to analytical storage (DuckDB/Parquet/
Databricks), which is always behind a repository.

Populated as endpoints are built; empty in the scaffold phase.
"""
