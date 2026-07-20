"""Read-only DuckDB-backed repository tier.

Each repository owns its own parameterized SQL and maps rows straight into the frozen DTOs
in ``fundamentals_screener.dtos`` — callers above this tier never see a raw row, dict, or
DataFrame. Ported from fundamentals_databricks_pj's web/repositories/.
"""
