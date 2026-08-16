# Phase 5.0 — Issuer/listing identity model: technical debt inventory + future re-key migration design

Companion to [ADR-0010](adr/0010-issuer-listing-identity-model.md). That ADR records the
*decision* (issuer/listing/source identity primitives, additive-not-re-key for this pass). This
document carries the fuller detail: the full bare-ticker-key inventory as named technical debt,
and the **designed but not yet executed** migration that will eventually resolve it. Nothing in
this document is implemented — it is a design reference for a future phase.

## 1. Current state: what stays bare-ticker-keyed after this pass

Confirmed by direct repo inspection (Phase 5.0 audit, 2026-08-16). `main.config.tickers` is the
only table with a real `(ticker, market)` composite identity key. Every table below keeps `ticker`
alone as its company-identity column after this pass — only `config.tickers` gains the new
`issuer_id`/`mic`/`listing_id` columns.

| Table | Current functional key | Target key (future migration) |
|---|---|---|
| `financials_raw` | bare `ticker`, `PARTITIONED BY (ticker)` | `issuer_id` (issuer-level) |
| `financials` | `(ticker, stmt, concept, fiscal_year, period_type)` | `(issuer_id, stmt, concept, fiscal_year, period_type)` |
| `financials_metrics` | `(ticker, fiscal_year, metric)` | `(issuer_id, fiscal_year, metric)` |
| `financials_intrinsic_value` | `(ticker, period_type, fiscal_year, method, scenario)` | `(issuer_id, period_type, fiscal_year, method, scenario)` |
| `market_prices_daily` | `(ticker, date)` | `(listing_id, date)` |
| `stock_splits` | `(ticker, split_date)` | `(listing_id, split_date)` |
| `market_cap_asof` | `(ticker, fiscal_year)` | `(listing_id, fiscal_year)` — priced per listing, but valued per issuer; see §3 open question |

Not migration candidates: `backtest_results`/`backtest_summary` (keyed by `archetype`, no company
identity at all) and `fx_rates_daily` (keyed by currency pair). `dashboard_*` published parquet
artifacts mirror whichever table they're exported from — they inherit the same key, and are the
same shape both frontends (`fundamentals_screener`, the Streamlit app) query by bare ticker.

## 2. Why this pass doesn't re-key them now

- **Public API contract.** `fundamentals_screener`'s URL routing (`/<ticker>/`), DTOs, and DuckDB
  repository queries are all bare-ticker-keyed and versioned as a public contract (see CLAUDE.md's
  "External consumers" section) — changing the row identity underneath them is a breaking change
  requiring its own coordinated, versioned migration, not something to fold into introducing the
  identity model.
- **The Streamlit app** independently has the same bare-ticker assumption throughout
  `lib/data.py` (session-state handoff, `company_10y`/`industry_map` dicts, price DataFrame
  grouping) — a second, separate consumer that any re-key must also cover.
- **No collision exists yet that requires it.** The four Phase 5.1 pilot issuers (Spain/France/
  Netherlands/Italy) are all confirmed collision-free against the live ~2,662-row US/CA universe
  — the re-key is not a blocking prerequisite for ingesting them, only for a future ticker that
  actually collides.
- **Live production Delta tables.** `financials`/`market_prices_daily` etc. are written to on a
  daily schedule by the production Job (job_id `736091313212283`) and read by both frontends —
  a re-key is a real, hard-to-reverse migration that needs its own reviewed plan and a dedicated
  phase, not a side effect of adding identity metadata.

## 3. Designed future migration (not executed)

**Sequencing**, once actually undertaken:

1. **Dual-key transitional period.** Add `issuer_id` (to the four issuer-level tables) and
   `listing_id` (to the three listing-level tables) as new, additive, nullable columns —
   populated at write time going forward, backfilled for existing rows via a join against
   `config.tickers` (now that it carries `issuer_id`/`mic`/`listing_id` after this Phase 5.0
   pass). Both the old bare-`ticker` key and the new key coexist; nothing downstream changes yet.
2. **Verify backfill coverage** the same way this pass's own `issuer_id` backfill on
   `config.tickers` was verified (exact counts: total rows, rows with the new key populated, rows
   without and why) — not a sample-based check.
3. **Switch MERGE `ON` clauses and `PARTITIONED BY`/`CLUSTER BY` keys** to the new column, once
   backfill coverage is confirmed complete for the live universe. This is the actual re-key step
   and the highest-risk part of the migration — needs a dedicated maintenance window and a
   verified rollback path (Delta's own time-travel, given no destructive `DROP`/`OVERWRITE` of
   the old `ticker` column is needed — it can remain as a plain, non-key attribute).
4. **Migrate consumers last, together**: `fundamentals_screener` (versioned per CLAUDE.md's
   External Consumers contract — bump `pyproject.toml`, coordinate with the consumer repo's own
   pinned install) and the Streamlit app (`lib/data.py`'s bare-ticker dicts/grouping), both in the
   same phase since they read the same published artifacts and must not observe an inconsistent
   key mid-migration.

**Open question, not resolved here**: `market_cap_asof` is priced per listing (a specific market's
close price) but the `market_cap` figure it stores is meant to represent the whole issuer's
capitalization — for a single-listing company these are the same number, but for a genuinely
dual-listed issuer (two listings, one issuer) it's not yet decided whether `market_cap_asof`
should key on `listing_id` (one row per listing, each showing that listing's own local price) or
on `issuer_id` (one consolidated row, requiring a "primary listing" selection rule). This needs a
decision at migration time, not guessed here.

## 3b. Backfill safety check: bare-ticker MERGE vs. `(ticker, market)` identity

The `issuer_id` backfill (§4 below) MERGEs `ON t.ticker = s.ticker` — bare ticker, not the
table's own real `(ticker, market)` identity key. If any ticker currently existed under more
than one `market` (e.g. a hypothetical `ABC`/US and `ABC`/CA both live), that MERGE could have
updated both rows with the same CIK-derived `issuer_id` even though `(ticker, market)` itself is
unique — silently conflating two distinct companies' identity metadata.

**Verified live against `main.config.tickers` (2026-08-16, post-backfill): zero tickers occur
under more than one distinct `market`.** `COUNT(DISTINCT ticker) = COUNT(DISTINCT
CONCAT(ticker, ':', market)) = COUNT(*) = 2,662`. The current production universe has globally
unique tickers, so the inherited bare-ticker MERGE did not have an existing cross-market
ambiguity to act on during this backfill — the result is safe as executed. This does **not**
mean the MERGE's bare-ticker key is itself correct in general (see the "inherited, not fixed"
note in §4) — only that no *existing* row was corrupted by it. The same check should be
re-run any time a new ticker is admitted that could plausibly collide (e.g. once European or
further Canadian tickers are actually added to `config.tickers` in a future phase), since a
future admission could reintroduce the exact ambiguity that happens not to exist today.

## 4. What this Phase 5.0 pass actually shipped (for cross-reference)

- `fundamentals_pipeline/identity.py`: `make_issuer_id()`, `make_listing_id()` (pure functions).
- `fundamentals_pipeline/sources/base.py`: `SourceEntity` corrected to
  `source_id`/`source_entity_id`/`issuer_id`/`name`/`ticker: str | None`.
- `main.config.tickers`: additive `issuer_id`/`mic`/`listing_id` columns. `issuer_id` backfilled
  for the whole existing US/CA universe via `11__fetch_sec_xbrl.py` (CIK-derived,
  `SEC_XBRL:<cik>`). `mic`/`listing_id` deliberately left NULL for existing rows.
- Pilot identity validated via literal test fixtures only (`tests/test_issuer_listing_identity.py`)
  — `XMAD:FCC`, `XPAR:ALO`, `XAMS:NAI`, `MTAA:FCT` — no `config.tickers` write, no adapter.
