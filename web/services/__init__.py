"""Services tier — application / use-case orchestration.

The layer views call. A service coordinates one use case: it invokes the ``repositories``
tier for data (never a storage engine or the ORM directly) and calls
``fundamentals_pipeline`` when business logic is required, then returns plain
DTOs/primitives to the view. It holds orchestration, not persistence details and not
financial formulas.

Dependency rule (enforced project-wide — see ``docs/architecture.md``):
    views → services → repositories → infrastructure (DuckDB / PostgreSQL) → fundamentals_pipeline

Populated as endpoints are built; empty in the scaffold phase.
"""
