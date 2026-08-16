# Phase 4 — European source research (`EU_CURRENT` / `filings.xbrl.org`)

Status: **research complete, no implementation** — presented for review per ADR-0009 §7 Phase 4.
Branch: `phase4-europe-research` (off `main`, post ADR-0009 Acceptance). No production code is
touched by this document. Do not begin Phase 5 (adapter implementation) until this is reviewed.

All findings below are from direct, live queries against `https://filings.xbrl.org` made during
this research pass (not assumed from the ADR's earlier, lighter-touch research) — every claim
that matters is backed by a real request/response quoted or paraphrased below.

---

## 1. What was verified

### 1.1 — The API surface (Phase 4A items 1, 15, 16, 17)

Three JSON-API resources, confirmed live:

- `GET /api/filings` — one row per filing (one company-year annual report)
- `GET /api/entities` — issuer records
- `GET /api/validation_messages` — per-filing XBRL validation errors/warnings

**Filtering**: `filter[<attribute>]=<value>` (e.g. `filter[country]=ES`). **Sorting**:
`sort=<field>` / `sort=-<field>` for descending. **Pagination**: `page[size]=N`,
`page[number]=N`, with `links.next`/`meta.count` in every response — real, standard JSON-API
pagination, not a bespoke scheme. **Relationship inclusion**: `?include=entity` embeds the
issuer record alongside each filing in one request (avoids an N+1 fetch pattern).

**Auth**: none required. **Terms**: "Access to the API is provided free of charge, but we
reserve the right to alter the API at any time, including applying rate limits or withdrawing
it completely" (`/docs/api`) — confirms ADR-0009's "no restrictions on data use" finding, but
adds a real, previously-unstated caveat: **no numeric rate limit is published, and no SLA is
offered.** This is a genuine open risk (Phase 4A item 16), not resolvable by more research —
noted honestly in §3 rather than guessed at.

### 1.2 — Facts are already tagged and structured — the single most important finding (Phase 4A items 9, 11)

The open question the ADR itself flagged as decisive: does `filings.xbrl.org` hand back
ready-to-consume facts, or only filing metadata requiring us to download and parse raw
Inline XBRL ourselves? **Confirmed: facts are already extracted, tagged, and structured** —
each filing's `json_url` (e.g.
`/95980020140005178328/2022-12-31/ESEF/ES/0/95980020140005178328-2022-12-31-es.json` for
Fomento de Construcciones y Contratas, a real Spanish ESEF filer, FY2022) returns a top-level
`"facts"` object plus a `"documentInfo"` metadata section — genuine xBRL-JSON, not a document
package requiring a local iXBRL parser. Verbatim, from that real filing:

```json
"concept": "ifrs-full:Revenue",              "value": "7705687000",   "unit": "iso4217:EUR",
  "period": "2022-01-01T00:00:00/2023-01-01T00:00:00"
"concept": "ifrs-full:ProfitLoss",            "value": "477930000",    "unit": "iso4217:EUR",
  "period": "2022-01-01T00:00:00/2023-01-01T00:00:00"
"concept": "ifrs-full:Assets",                "value": "15282541000", "unit": "iso4217:EUR",
  "period": "2023-01-01T00:00:00"
"concept": "ifrs-full:CashAndCashEquivalents", "value": "1575538000",  "unit": "iso4217:EUR",
  "period": "2023-01-01T00:00:00"
```

This resolves the fork the ADR left open:

```
filings.xbrl.org → filing metadata → download ESEF → parse Inline XBRL → extract facts   [NOT NEEDED]
filings.xbrl.org → filing metadata → JSON facts (already tagged)                          [CONFIRMED]
```

**And the tag vocabulary is the exact one this codebase already speaks.** `ifrs-full:Revenue`,
`ifrs-full:ProfitLoss`, `ifrs-full:Assets`, `ifrs-full:CashAndCashEquivalents` are the same
`ifrs-full`-namespace concept names `IFRS_FALLBACK_TAGS` (`00__config/01__tickers.py`) already
maps for SEC MJDS filers (Toyota, Vale, Infosys, per that dict's own verification comment) —
because `ifrs-full` is one global IFRS Foundation taxonomy, not a jurisdiction-specific
variant. This is a materially better starting position than a from-scratch tag-mapping effort:
the *canonical-label → tag* side of `EU_CONCEPT_TAGS` (ADR-0009 §2.5) can very plausibly reuse
`IFRS_FALLBACK_TAGS`'s existing tag strings directly, pending the verification in §1.4 below —
not a new mapping table built from zero.

### 1.3 — Period semantics map cleanly onto the existing `kind` model (Phase 4A item 7; Phase 4F)

Every fact's `period` field is an unambiguous ISO 8601 value, structurally self-describing —
no day-count heuristic needed the way SEC's `classify_period_shape_series` requires:

- **Duration** (`"2022-01-01T00:00:00/2023-01-01T00:00:00"`) → a flow concept (Revenue,
  ProfitLoss) — maps directly to this project's `flow_additive`/`flow_nonadditive` `kind`.
- **Instant** (`"2023-01-01T00:00:00"`, no `/`) → a snapshot concept (Assets,
  CashAndCashEquivalents) — maps directly to `kind="stock"`.

This is a genuinely easier period model than SEC's, not a harder one. **What's still
undetermined**: ESEF's annual reports are, by the mandate itself (ADR-0009 §4.1), annual-only —
no interim/quarterly ESEF filing exists to worry about `fp`-style classification for at all, so
the `21b__derive_quarterly.py` `fp`-vocabulary gap ADR-0009 flagged as a real blocker (§2.1 item
5) **does not need solving for this pilot** — European quarterly ingestion is out of scope by
construction, not deferred by choice.

### 1.4 — Coverage, verified by country (Phase 4A items 3, 4, 6; Phase 4B)

Live counts from the API (not the ADR's earlier, unverified-by-country claim):

| Country | Filing count (all years) | Earliest `period_end` found |
|---|---|---|
| Spain (ES) | 542 | 2020-12-31 |
| France (FR) | 1,178 | not individually queried — same index, no reason to differ |
| Netherlands (NL) | 656 | not individually queried |
| Italy (IT) | 872 | not individually queried |

2020-12-31 as Spain's earliest period is exactly consistent with ESEF's own mandate start (FY
commencing on/after 2020-01-01) — confirms the index isn't silently missing older data, it's
that older data genuinely doesn't exist under ESEF. All four pilot countries have several
hundred filings each — comfortably enough distinct issuers for a 15-20 company pilot (§2 below)
with room to be selective about presentation quality, sector, and `error_count`/
`warning_count` (both exposed per-filing — see §1.6).

**Germany and Ireland were not re-queried this pass** — the ADR's own finding (they're not
reliably discoverable through this aggregator) stands; re-confirming a negative is lower value
than the positive coverage numbers above, and the pilot doesn't need them (§2).

### 1.5 — Identity: LEI is the real entity key (Phase 4A item 19; Phase 4D)

`/docs/api`: "the index is structured by company identifier (**typically LEI**), then reporting
date, filing system and country." Confirmed directly in a real filing ID: Fomento de
Construcciones's `fxo_id` is `95980020140005178328-2022-12-31-ESEF-ES-0` — the leading
`95980020140005178328` is a real 20-character LEI. This is a materially better entity
identifier than anything SEC-side: **LEI is a global, standardized, ISO 17442 identifier**,
issued once per legal entity and stable across jurisdictions and listings — a stronger
foundation for `SourceEntity.source_entity_id` than SEC's US-only CIK, and a natural fit for
the brief's own "issuer, not ticker, is identity" principle (ADR-0009 §2.4). The `/api/entities`
resource (confirmed to exist, not yet deep-queried) is the likely place to resolve
ticker/name/country for a given LEI — not investigated further this pass (§3).

### 1.6 — Provenance and data-quality signals are already exposed (Phase 4A items 13, 14, 18; Phase 4H)

Every filing record carries `error_count`, `warning_count`, `inconsistency_count`,
`processed`/`date_added` timestamps, a `sha256` of the source document, and links to
`validation_messages` — i.e. `filings.xbrl.org` already computed something structurally
equivalent to this project's own `35__reconcile_filings.py` Tier-A linkbase-oracle concept
(independent validation of what's actually in the filing), for free, per filing. This is a
real asset: a European reconciliation-oracle equivalent (flagged as a gap in ADR-0009's
Consequences) may not need to be built from scratch — it may be enough to surface these
already-computed quality signals per ingested fact. Not decided here; flagged for Phase 7.

### 1.7 — Amendments / duplicate filings (Phase 4A item 14) — not conclusively resolved

Not confirmed this pass whether a restated/amended annual report produces a second `fxo_id` for
the same company+period (analogous to SEC's `10-K/A`), or overwrites the original. The `fxo_id`
format (`<LEI>-<period_end>-<system>-<country>-<sequence>`) has a trailing sequence number
(`-0` in every example seen), which suggests the mechanism exists (a `-1` would be a resubmission)
but no real example was found to confirm behavior. **Genuine open question — listed in §3, not
guessed at.**

---

## 2. Pilot countries and issuer-selection approach (Phase 4B)

Spain, France, Netherlands, Italy — all four confirmed present with real, substantial coverage
(§1.4). Germany and Ireland stay excluded, per the ADR's own finding, not re-added. Concrete
selection mechanism for Phase 8 (not executed here): query `/api/filings?filter[country]=<cc>
&sort=-processed&page[size]=50`, prefer filings with `error_count=0`/low `warning_count`
(a genuine, API-exposed quality signal — not a proxy), deliberately include at least one
messier-than-average filing per the brief's own stress-test intent (§2 of the earlier ADR-0009
research), targeting 3-5 companies per country across the sectors the originating brief named.

---

## 3. What remains uncertain (stated honestly, not glossed over)

- **No published rate limit.** "We reserve the right to apply rate limits... at any time" is
  the entire policy. A production adapter needs its own conservative, configurable throttle
  (mirroring `11__fetch_sec_xbrl.py`'s `MIN_REQUEST_GAP` pattern) sized defensively, not against
  a documented number that doesn't exist.
- **Amendment/restatement handling** (§1.7) — not confirmed. Needs either a direct example (a
  company known to have refiled) or a question to the maintainers' mailing list before Phase 6's
  dedup/MERGE logic can be designed with confidence.
- **`/api/entities`** was confirmed to exist but not deep-queried — LEI → name/ticker/exchange
  resolution mechanics (needed for `discover_entities`) are inferred, not verified in detail.
- **Full concept coverage beyond the four spot-checked tags** (Revenue, ProfitLoss, Assets,
  CashAndCashEquivalents) — real, but not exhaustive. Phase 6 needs a systematic pass checking
  every concept `IFRS_FALLBACK_TAGS` already lists against a handful of real filings across all
  four pilot countries, not just the one Spanish example this research pass spot-checked.
- **Whether every filing's facts are consolidated-only or sometimes include standalone/parent-
  only statements too** (the brief's own Phase 4C concern) — not checked; a real risk for
  double-counting or wrong-basis values if unaddressed in Phase 6's mapping design.
- **Historical depth beyond one earliest-date check for Spain** — not verified for the other
  three pilot countries individually (reasonable to assume consistency across one shared index,
  not confirmed).

---

## 4. Canonical mapping strategy (Phase 4C)

No canonical model change. Reuses the two-axis `MappingStatus`/`MappingType` model
(`fundamentals_pipeline/sources/mapping.py`, already built in Phase 3) unchanged. Concrete
proposal for Phase 6, grounded in §1.2/§1.3's real evidence, not invented fresh:

| Canonical concept | `ifrs-full` tag (spot-checked or highly likely, per `IFRS_FALLBACK_TAGS` precedent) | Proposed status |
|---|---|---|
| Revenue | `ifrs-full:Revenue` | ACCEPTED / DIRECT (verified live) |
| Net Income | `ifrs-full:ProfitLoss` | ACCEPTED / DIRECT (verified live) |
| Total Assets | `ifrs-full:Assets` | ACCEPTED / DIRECT (verified live) |
| Cash & Equivalents | `ifrs-full:CashAndCashEquivalents` | ACCEPTED / DIRECT (verified live) |
| Operating Income | — | **UNSUPPORTED** — no mandated IFRS "operating" definition; ADR-0009 §3.5 already flagged this, unchanged by this research |
| Gross Profit | — | **UNSUPPORTED** — many IFRS filers don't present a Cost-of-Sales split at all; same prior finding, unchanged |
| Everything else `IFRS_FALLBACK_TAGS` doesn't already cover | — | Not proposed — Phase 6 extends only what's verified, not what's convenient |

Consistent with the project's own `comparability > coverage` invariant: this table is
deliberately no larger than what's actually verified or directly inherited from an
already-verified precedent.

## 5. Identity strategy (Phase 4D)

LEI (`SourceEntity.source_entity_id`) is the right key — confirmed real, stable, globally
standardized, already embedded in every filing's `fxo_id` (§1.5). **`identity.py` is not
modified in this phase** — no concrete requirement was found that its `(ticker, market)`
model can't accommodate; a European issuer gets a `market` row (e.g. `"ES"`, not a blanket
`"EU"`, per ADR-0009 §2.4) same as any other. A LEI-based `source_entity_id` is a
`config.tickers`-adjacent concern for Phase 6/9, not an `identity.py` change.

## 6. `FundamentalsSource` contract sufficiency (Phase 4E)

**Sufficient as-is — no change proposed.** Checked each method against the real API:

- `discover_entities(tickers)` → resolvable via `/api/entities` (LEI/name lookup) — mechanics
  not fully verified (§3), but the method shape fits.
- `discover_filings(entity)` → `/api/filings?filter[entity]=<LEI>` (inferred filter key,
  consistent with the confirmed `filter[<attribute>]` pattern) returns exactly the
  `SourceFiling` shape (`source_filing_id`=`fxo_id`, `filing_type`, `filed_date`, `period_end`)
  already defined in Phase 3.
- `retrieve_facts(filing)` → fetch that filing's `json_url`, emit each matched `SourceFact` —
  a materially *simpler* implementation than `SECXBRLSource`'s own `retrieve_facts` (§1.2: no
  raw-response traversal needed, the facts are already flat).
- `detect_metadata(entity)` → `accounting_framework="ifrs-full"` is knowable outright (ESEF
  mandates it); `reporting_currency` is read directly off any fact's `unit` field (§1.2's EUR
  example) — arguably easier than SEC's own multi-namespace detection logic.

No deficiency found. This is itself a real Phase 3 validation point worth noting: a contract
designed against one source (SEC) fit a structurally very different second source without
needing to change.

## 7. Period normalization strategy (Phase 4F)

No new mechanism needed — §1.3's instant/duration distinction maps directly onto the existing
`kind` enum (`flow_additive`/`flow_nonadditive`/`stock`) with no day-count heuristic required.
Interim-period handling is out of scope by construction (§1.3). Non-calendar fiscal years,
52/53-week years, and consolidated-vs-standalone presentation (§3) are real open questions for
Phase 6, not resolved here.

## 8. Currency strategy (Phase 4G)

No change to this project's existing native-currency-only policy. Confirmed directly: every
fact carries its own `unit` (ISO 4217 code, e.g. `iso4217:EUR`) — reporting currency is
per-fact, explicit, never inferred. No FX conversion introduced by this source.

## 9. Provenance strategy (Phase 4H)

The existing `SourceFact` shape (Phase 3) already covers what's needed:
`source_id="EU_CURRENT"`, `source_entity_id`=LEI, `source_filing_id`=`fxo_id`,
`source_concept`=the `ifrs-full:*` tag, period/currency/value straight off the fact. No new
provenance fields proposed. §1.6's quality signals (`error_count`/`warning_count`/`sha256`)
are a candidate *addition* worth a decision in Phase 7 (a per-source
reconciliation/confidence signal), not a requirement to design now.

## 10. Exact files/components a Phase 5 implementation would touch (not built here)

Named once, not enumerated line-by-line, since none of this is implemented yet:

- New `EU_CURRENT` source-definition update in `fundamentals_pipeline/sources/registry.py` —
  flip `access_status` from `RESEARCH_ONLY` toward `ACTIVE` only once Phase 5 actually ships.
- A new `EU_CONCEPT_TAGS`-shaped dict in `00__config/01__tickers.py`, following
  `IFRS_FALLBACK_TAGS`'s exact shape, seeded from §4's table.
- A new adapter module implementing `FundamentalsSource` against `filings.xbrl.org`'s API
  (§6) — the natural next sibling to `SECXBRLSource`, but for a genuinely new source this time,
  not an additive proof over an existing one.
- No changes anticipated to `21__clean_and_merge.py`/`21c`-`21h`/`22__derived_metrics.py`
  (§1.3 — the existing canonical-key MERGE/dedup machinery and formula layer are already
  source-agnostic, confirmed in ADR-0009 §1 and unchanged by this research).

## 11. Proposed sequence, tests, DAG plan (Phase 4I items 11-13)

Unchanged from ADR-0009 §7's own Phase 5-9 breakdown — this research doesn't revise that
sequence, it fills in *how* Phase 5 would actually work once undertaken. Restated briefly:
Phase 5 (adapter) → Phase 6 (mapping, using §4's table as the starting point, extended only
after real per-concept verification across all four pilot countries, not just Spain) → Phase 7
(tests + resolving §3's open questions) → Phase 8 (pilot universe, §2) → Phase 9 (DAG
integration, the deferred `databricks.yml` fan-out from Phase 3). Tests would mirror Phase 3's
own pattern: pure-Python unit tests for the mapping table and any parsing logic, plus a scoped
live Databricks smoke test (one real filing, like Phase 3's AAPL smoke test) before merge.

---

## What this research does NOT do

Per the brief's explicit constraints: no European adapter code, no `EU_CONCEPT_TAGS` dict
written into `01__tickers.py`, no canonical model change, no country-specific schema, no ESAP
client (ESAP's registry entry stays `UNAVAILABLE`, untouched — no new ESAP research was
attempted this pass; ADR-0009 §4.4's finding stands). Nothing here is committed to running
code. This document is the Phase 4 deliverable; Phase 5 starts only after it's reviewed.
