"""Infrastructure tier — the DuckDB / PostgreSQL access layer.

The bottom application tier of the web layer (above ``fundamentals_pipeline``): raw access
to the persistent stores. ``storage`` fetches + caches the published parquet/JSON artifacts;
``duckdb`` queries them; the Django ORM (later) is the PostgreSQL side.

**Only the repositories tier may import this package.** Views and application services must
never touch storage engines or the ORM directly — they go Views → services → repositories →
infrastructure. See ``docs/architecture.md``.
"""
