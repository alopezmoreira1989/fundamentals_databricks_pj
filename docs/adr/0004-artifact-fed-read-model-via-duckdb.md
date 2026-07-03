# ADR-0004: Artifact-fed read model via DuckDB — no Databricks at request time

- **Status:** Accepted
- **Date:** 2026-07-03  <!-- recorded retroactively; established with the web infrastructure tier -->
- **Deciders:** repo owner

## Context

The web app (ADR-0002) and the Streamlit app both need the pipeline's analytical results —
company facts, derived metrics, prices, intrinsic values, backtests. That data is produced on
**Databricks** and lives in Delta tables (`main.financials` / `main.config`). Querying Databricks
on every user request would couple the public web tier to a warehouse: latency, per-query cost, a
cluster/warehouse that must be always-on, and credentials in the request path.

The pipeline already publishes a small, schema-asserted set of Parquet/JSON artifacts to a
GitHub Release `latest` (`50__publish/51+52`), validated against `fundamentals_pipeline.schemas`.

## Decision

The web layer's analytical **read model is those published artifacts, queried locally with
DuckDB** — it **never** queries Databricks during a user request. `infrastructure/storage`
fetches, validates, and caches the artifacts on local disk (TTL + stale-while-revalidate);
`infrastructure/duckdb` registers each cached Parquet file as a view and runs read-only SQL over
it (predicate + projection pushdown, no full-frame loads into Python). PostgreSQL holds only
application data (ADR-0002); the pipeline's Databricks side is upstream and asynchronous.

## Consequences

- The web tier is cheap, fast, and independent: no warehouse in the request path, no Databricks
  credentials at runtime, and it stays up even if Databricks is down.
- The exact same artifact contract feeds Streamlit and the Django app — one source of truth, one
  schema, validated on fetch so a drifted publish fails loudly rather than rendering wrong data.
- Data is **as fresh as the last publish**, not real-time. Acceptable: fundamentals update on a
  slow (quarterly filing) cadence. Freshness is managed by the cache TTL and cache warming.
- CI must supply the artifacts to run the read-model tests (they self-skip offline); this is why
  the web CI job fetches the Release artifacts as fixtures.
- Aligns with ADR-0003: DuckDB sits in `infrastructure/`, reached only through repositories, so a
  later move to Databricks SQL / another engine is a repository-tier change.

## Alternatives considered

- **Query Databricks SQL at request time.** Rejected: latency, cost, an always-on warehouse, and
  warehouse credentials in the public request path.
- **Load the Parquet artifacts into pandas per request.** Rejected: multi-million-row frames;
  DuckDB does pushdown and reads only the needed rows/columns off disk.
- **Mirror the analytical data into PostgreSQL.** Rejected: a second copy to keep in sync and a
  schema to maintain, when the published Parquet already is the contract and DuckDB reads it
  directly. Postgres stays reserved for application data.
