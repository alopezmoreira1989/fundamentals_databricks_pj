# ADR-0009: Multi-market, multi-source fundamentals ingestion — Europe as the first non-US pilot, ESAP-ready, Canada registered but not forced

- **Status:** Accepted
- **Date:** 2026-08-16 (Proposed and revised earlier the same day — see the revision notes below;
  Accepted after Phase 3 landed)
- **Deciders:** repo owner

## Acceptance note (2026-08-16)

Accepted after Phase 3 (§7) met the bar this ADR itself set for Acceptance — proving the
`FundamentalsSource` design actually fits the existing codebase (identity → ticker universe →
source routing → SEC ingestion → source-normalized facts → canonical financials) without
requiring it to be rebuilt, not merely designed on paper:

- Phase 3 implemented (`fundamentals_pipeline/sources/`: the `FundamentalsSource` contract,
  the source registry with its access-status model, the `MappingStatus`/`MappingType` decision
  model) and merged into `main` ([PR #369](https://github.com/alopezmoreira1989/fundamentals_databricks_pj/pull/369)).
- `source_id` provenance added to `financials_raw`/`financials`, additive, following the
  existing `tag_namespace` precedent exactly.
- `SECXBRLSource` proved the contract fits SEC's real shape — not asserted, exercised: a real
  Databricks smoke test (local Databricks Connect, fixed same session) ran `11__fetch_sec_xbrl`
  → `21__clean_and_merge` end-to-end against a real SEC API call and real Unity Catalog writes
  (AAPL, forced refresh), confirmed `source_id`/`tag_namespace` populate correctly, confirmed
  AAPL's canonical values were unchanged, confirmed zero new `ingestion_failures`.
- Two real review findings from PR #369 were fixed and re-verified live before merge:
  `SECXBRLSource.retrieve_facts()` now matches on the real SEC accession number (`accn`), not
  an approximate `(form, period_end)` key; the ADR's own wording about `source_id` no longer
  overclaims a retroactive backfill that was never actually implemented.
- The existing US pipeline's local test suite (211 tests) passed unchanged throughout — no
  regression from this work.

Phase 4 (European source research) starts from here, on its own branch, following the same
research-before-implementation discipline this ADR was itself built with.

This ADR is the Phase 1–2 deliverable for the multi-market ingestion project (repository
analysis + architecture proposal). It documents a **proposal**; Phase 3 (§7) is scoped as a
plan in this same document, but no pipeline code changes accompany this revision — Phase 3
implementation itself is presented separately for review before any file is touched, per the
repo owner's explicit instruction.

**Revision note:** this ADR was first drafted with Canada/SEDAR+ as the candidate first
non-US source. Direct SEDAR+ research (still summarized in §5) found no viable compliant
automated-access path today. Rather than force Canada into the pilot slot anyway, this
revision follows the repo owner's redirected brief: **Europe is the first non-US pilot**,
explicitly designed around the July 2027 arrival of ESAP (European Single Access Point), with
Canada staying in the architecture as a registered-but-not-automated source. The generic
architecture below (§1–§3) is materially unchanged from the first draft — it was built
market/source-agnostic from the start, which is exactly why redirecting the pilot target cost
a rewrite of the *pilot section*, not the *framework*.

**Second revision note (post-review):** the repo owner's review flagged that the first
revision's ESEF language risked implying ESEF ≈ "all European financial data." §4 below has
been tightened to keep three distinct things separate — **ESEF** (a regulatory format/mandate,
scoped to issuers admitted to trading on an EU-regulated market, annual consolidated IFRS
statements only), **`filings.xbrl.org`** (one aggregator of ESEF filings, itself with real
coverage gaps), and **ESAP** (a future centralized access point, not live) — never conflated
into one "the European source" concept.

---

## Context

### 1. Current architecture assessment

The repository was inspected directly (not assumed) before writing this section — the
ingestion/config layer, the transformation/canonical-schema layer, and the
orchestration/validation/test layer were each audited in depth. Findings cite `file:line` for
every claim that matters to the decision in §2 onward.

#### 1.1 — There is exactly one real ingestion source today, used two different ways

Despite "multi-market roadmap Phase 1" language already in `CLAUDE.md`, **every ticker this
pipeline ingests — US or "Canadian" — goes through the same SEC EDGAR `companyfacts` API**, in
the same function, with no source dispatch of any kind:

- `10__ingestion/11__fetch_sec_xbrl.py`'s `process_ticker()` (line 560) runs uniformly for every
  ticker in `RUN_TICKERS`. The only `market ==` check in the whole file (line 844) scopes the
  `accounting_standard`/`reporting_currency` metadata-backfill `MERGE INTO config.tickers` to
  `market='CA'` rows — *after* the same SEC fetch has already run.
- This works only because "Canadian" tickers today are, by construction, **SEC-registered
  MJDS/40-F filers** (`00__config/02__tickers_master.py:648-657`'s own comment states this
  explicitly). The Canadian-admission gate (lines 811-902) checks a TSX-composite (XIC)
  candidate against **SEC's own bare-ticker index**
  (`_SEC_TICKER_INDEX_URL`, line 667) — a company that does *not* also register with the SEC is
  structurally inadmissible by this gate's design, not by a scoping flag.
- `extract_series()` (`11__fetch_sec_xbrl.py:286-328`) reads
  `facts["facts"][namespace][concept]["units"]` — the literal shape of an SEC `companyfacts`
  response. There is no adapter interface around this; the function *is* the SEC adapter.
- `IFRS_FALLBACK_TAGS` (`00__config/01__tickers.py:323-353`, wired in at
  `11__fetch_sec_xbrl.py:628`) is real, working IFRS-tag support — but it's IFRS-*taxonomy*
  support fetched through the *same SEC API* (MJDS filers submit `ifrs-full`-tagged XBRL to the
  SEC same as domestic filers submit `us-gaap`), not non-SEC-*source* support. (This also
  contradicts `CLAUDE.md`'s current claim that IFRS mapping is "still out of scope,
  deliberately" — flagged as a doc/code drift to fix separately, not part of this decision.)
- Two more files (`13__fetch_dimensional_10k.py`, `14__fetch_oracle_statements.py`) read
  *different SEC filing artifacts* (raw XBRL instance/linkbase XML) for gap-filling and
  reconciliation — not a second vendor or jurisdiction. **There is no existing "second source"
  pattern anywhere in this codebase to generalize from.**

#### 1.2 — Where SEC-specific assumptions live, and what's already generic

| Layer | SEC-specific coupling | Already generic |
|---|---|---|
| `01__tickers.py` | `STATEMENTS` tags are bare `us-gaap` strings (namespace is a hardcoded call-site default, `11__fetch_sec_xbrl.py:627`); `SEC_USER_AGENT`; `classify_period_shape*` day-buckets tuned to SEC's 10-Q/10-K cadence | `(label → tags, kind)` shape of `STATEMENTS` is source-agnostic in structure; `CONCEPT_SYNONYMS`/`CONCEPT_PRIORITY*` operate on canonical labels only |
| `02__tickers_master.py` | Canadian admission gate is SEC-registration-gated end to end (§1.1) | `market` column design; `identity.py` calls are already N-market-generic |
| `11__fetch_sec_xbrl.py` | Deepest coupling in the repo: CIK resolution, `companyfacts` URL, literal JSON-shape traversal, hardcoded SEC form-type list, `detect_accounting_standard_and_currency()`'s hardcoded `("ifrs-full", "us-gaap")` pair | The `market='CA'`-scoped metadata write-back is already shaped like a per-market extension point, just fed exclusively by SEC-shaped data |
| `12__fetch_market_data.py` | `QUOTE_CURRENCY_BY_MARKET = {"US": "USD", "CA": "CAD"}` (line 542); US-centric default | **Least coupled file in the repo** — pricing isn't SEC-sourced; the `.TO`-suffix translation (lines 95-98) is a single dict lookup at the network-call boundary, the best template for adding a new *market* |
| `21__clean_and_merge.py` | Annual-form allowlist is a hardcoded SEC form-type list (line 127); filters on `fp`, SEC EDGAR's own XBRL-frames vocabulary | Zero hardcoded tag names — `CONCEPT_PRIORITY*`/`CONCEPT_SYNONYMS` are config-driven; MERGE/dedup key is purely canonical |
| `21b__derive_quarterly.py` | Literal `fp == "Q1"/"Q2"/"Q3"/"FY"` filters throughout — a source with a different vocabulary silently produces FY-only rows (no crash) | The **duration-based** period-shape classifier is genuinely source-agnostic; `periods.py`'s `q4_from_fy_ytd()` takes plain numbers + a generic `kind` enum, no calendar logic |
| `21c`–`21h` | None | Operate purely on canonical `(ticker, stmt, concept, fiscal_year, period_type, period_end)` keys |
| `22__derived_metrics.py` | None in the *formula* layer — verified by grep, `accounting_standard` never appears here | Every margin/return/leverage/valuation formula runs on canonical concept columns only; `QUOTE_CURRENCY_BY_MARKET` currency-alignment only ever selects a *quote currency*, never branches a calculation |
| `schemas.py`/`artifacts.py` | None | Every artifact spec is column-name-generic; `tag_namespace STRING` on `financials_raw`/`financials` is the one deliberately generic provenance column |
| `concept_hierarchy.json` | None — zero raw SEC/XBRL tag names anywhere | Pure display/layout metadata, strictly downstream of the mapping layer (§1.3) |
| `35__reconcile_filings.py` | The one validator with a real structural US/SEC dependency — Tier-A oracle is `us-gaap`-namespace-only | — |
| `32`, `34`, `40` (analysis) | None | Already source/currency-agnostic |
| `39__external_benchmark_check.py` | Latent, not structural — raw `abs(diff)` comparison assumes same currency both sides; would produce false positives for a currency-mismatched foreign ticker | — |
| `databricks.yml` / `91a__pipeline_pre22.py` | SEC ingestion (`11`) and market-data ingestion (`12`) are two sequential `%run` cells inside **one Databricks Job Task**, one failure boundary | The DAG *can* express real parallel fan-out — `91d`/`91e`/`91f` → `91g` (`databricks.yml:90-121`) is exactly that shape, proving only the *ingestion tier* was never split this way |

#### 1.3 — Where canonical concept mapping actually lives (not scattered)

Confirmed by direct inspection: mapping lives in **exactly two dictionaries in one config
file**, consumed generically downstream:

1. **Tag → canonical resolution at ingestion** — `01__tickers.py`'s `STATEMENTS` (canonical
   label → priority-ordered `us-gaap` tags + `kind`) and `IFRS_FALLBACK_TAGS` (canonical label →
   lower-priority `ifrs-full` tags), consumed by `11__fetch_sec_xbrl.py` via plain dict
   iteration — no if/elif chain.
2. **Synonym collapse + tiebreak priority at merge time** — the same file's
   `CONCEPT_SYNONYMS`/`CONCEPT_PRIORITY*`, consumed by `21__clean_and_merge.py`/
   `21b__derive_quarterly.py` as a generic dict-driven `F.when` chain.

Everything downstream (`concept_hierarchy.json`, `statement_layout.py`,
`22__derived_metrics.py`, `schemas.py`) operates **exclusively on canonical concept labels**.
This is the plug-in point a new source's mapping needs — it already exists and is already
data-driven.

#### 1.4 — Identity, schema, and DAG limitations

- **Identity:** `identity.py`/`config.tickers`'s `market` column is sound (§2.4 keeps it), but
  `financials_raw`/`financials`/`market_prices_daily`/`market_cap_asof` and both frontends
  remain bare-`ticker`-keyed (documented already in `identity.py:26-29`) — a real constraint on
  multi-listing support this ADR does not propose fixing yet (scope note in §2.4).
- **Schema:** no gap at the canonical fact-table level — built market-agnostic already. The
  real gap is provenance: `financials_raw` has no `source_id` column, so once a second source
  exists, two sources' rows sharing a `(ticker, concept, period_end)` key are indistinguishable
  by origin. `QUOTE_CURRENCY_BY_MARKET` is independently duplicated in **four** places
  (`12__fetch_market_data.py:542`, `22__derived_metrics.py:53`, `23__intrinsic_value.py:70`,
  `61__streamlit/lib/currency.py:29`) — a real, if minor, maintenance gap a third market makes
  worse.
- **DAG:** structural, not just a code-layer concern — "a European source failure must not
  break SEC" (§31 of the originating brief, this ADR's own success criterion) is **not** true
  today and needs a `databricks.yml` topology change, not merely a new Python file (§2.6).

---

## Decision

We will build a **market-agnostic, source-agnostic ingestion framework** — a
`FundamentalsSource` adapter contract, a small source registry with an explicit access-status
model, a provenance-aware raw layer, and a canonical mapping layer with an explicit
accept/reject/NULL contract — and implement it for exactly one new source region first:
**Europe**, via ESEF/iXBRL filings today, with an explicit, documented migration path to ESAP
once it opens to the public in July 2027. Canada/SEDAR+ is registered in the same framework but
is **not** the pilot, and automated ingestion from it stays disabled pending a compliant access
path (§5). The canonical model does not change shape to accommodate any of this — foreign
sources adapt to it, never the reverse.

### 2.1 — Proposed architecture

```
                              ISSUER / TICKER UNIVERSE
                            (main.config.tickers, market column)
                                        |
                                        v
                              SOURCE ROUTING LAYER
                    (per (ticker, market): which source_id ingests it)
                                        |
              +-------------------------+-------------------------+
              |                         |                         |
              v                         v                         v
        SOURCE: SEC_XBRL          SOURCE: EU_CURRENT         SOURCE: SEDAR_PLUS
        (11__fetch_sec_xbrl,      (NEW — ESEF/iXBRL via      (registered only —
         unchanged)                filings.xbrl.org, §4)      AUTOMATION_RESTRICTED, §5)
              |                         |
              |                         v   (July 2027+, same contract)
              |                    SOURCE: ESAP
              |                    (future — §4.4)
              +-------------------------+-------------------------+
                                        |
                                        v
                       SOURCE NORMALIZATION / CANONICAL MAPPER
                (STATEMENTS + IFRS_FALLBACK_TAGS pattern, extended per-source)
                                        |
                                        v
                          CANONICAL ACCOUNTING MODEL (financials)
                         [UNCHANGED SHAPE — the whole point]
                                        |
                                        v
                          quarterly normalization (21b-21h)
                                        |
                                        v
                               derived metrics (22)
                                        |
                                        v
                                   valuation (23)
                                        |
                                        v
                            analytics / frontends (unchanged)
```

**What genuinely needs to change, scoped by what §1 found — not a rewrite:**

1. A `FundamentalsSource` adapter contract (§2.2) — `11__fetch_sec_xbrl.py` becomes its first
   implementation, unchanged in behavior (its internal shape already matches the contract).
2. A `source_id` column on `financials_raw` and `financials`, additive. New/updated rows get
   `"SEC_XBRL"` going forward; existing unchanged rows are intentionally **not** retroactively
   backfilled — the MERGE only sets it where its `UPDATE` branch already fires (a real
   value/`period_end` change), matching `tag_namespace`'s own established behavior on this same
   table (confirmed live: plenty of existing rows still show `tag_namespace=NULL` too). Not a
   gap to fix — a deliberate continuity with existing MERGE semantics.
3. A source registry (§2.3) with an explicit access-status enum (§2.3.1) — this is new relative
   to the first draft of this ADR and directly the product of the SEDAR+ vs. ESEF/filings.xbrl.org
   contrast found in research (§4/§5): "has an API" and "is legally safe to automate against"
   turned out to be genuinely independent questions, and the registry has to hold both.
4. `01__tickers.py`'s `STATEMENTS`/`IFRS_FALLBACK_TAGS` pattern extended with a European
   tag-mapping dict following the exact same shape — §4.3 shows this is a closer fit than
   Canada's case would have been, since ESEF is IFRS-taxonomy-tagged XBRL, structurally similar
   to what `IFRS_FALLBACK_TAGS` already consumes.
5. `21b__derive_quarterly.py`'s `fp` vocabulary needs to become source-aware wherever a new
   source doesn't share SEC's `Q1`/`Q2`/`Q3`/`FY` labels. ESEF filings are annual-report-only
   (interim reports are a separate, less standardized EU disclosure regime, not part of the
   ESEF mandate) — so the European Generation-1 pilot (§4) can reasonably ingest **annual data
   only** at first, which sidesteps this problem entirely for the pilot, deferring interim-period
   `fp` generalization to whenever European interim ingestion is actually scoped.
6. `databricks.yml`'s ingestion tier needs a structural split — a per-source ingestion task
   feeding a shared merge task, reusing the `91d`/`91e`/`91f` → `91g` fan-out/fan-in pattern
   already proven in this DAG (§2.6).

**What explicitly does not change:** `concept_hierarchy.json`, `statement_layout.py`,
`22__derived_metrics.py`'s formulas, `schemas.py`/`artifacts.py`'s export contracts, every
frontend, `21c`–`21h`'s dedup/prune/plausibility machinery.

### 2.2 — The `FundamentalsSource` adapter contract

A Python protocol/ABC in `fundamentals_pipeline/sources/base.py` (new, pure-Python, no
Spark/`dbutils` dependency — importable and unit-testable like `identity.py`/`fx.py`):

```python
class FundamentalsSource(Protocol):
    source_id: str  # "SEC_XBRL", "EU_CURRENT", "ESAP", "SEDAR_PLUS", ...

    def discover_entities(self, tickers: Sequence[str]) -> Sequence[SourceEntity]:
        """Resolve tickers to this source's own entity identifier (CIK, LEI, national
        registry number, ...)."""

    def discover_filings(self, entity: SourceEntity) -> Sequence[SourceFiling]:
        """List relevant filings (annual/interim, form/report type, filed date)."""

    def retrieve_facts(self, filing: SourceFiling) -> Sequence[SourceFact]:
        """Extract raw (source_concept, value, period, currency, ...) facts from one filing."""

    def detect_metadata(self, entity: SourceEntity) -> SourceEntityMetadata:
        """accounting_framework, reporting_currency — whatever this source can determine."""
```

The adapter stops **before** canonical mapping, period classification, or any financial
calculation — those stay in shared, source-agnostic layers, exactly matching the originating
brief's own §17 boundary. `SourceFact` provenance fields (trimmed to what `financials_raw`
doesn't already carry — it already has `scraped_at`/`filed`/`tag_namespace`):

```python
source_id, source_entity_id, source_filing_id, source_concept,
source_period_start, source_period_end, source_currency, source_value
```

`source_document_id`/`source_url` are deferred until a source that actually needs
document-level provenance (a PDF-based source) is implemented — ESEF/filings.xbrl.org facts are
already tagged-and-linked (§4.2), so the pilot doesn't need them yet; adding unused columns
now would be exactly the "abstraction with no practical value" the brief's own §34 warns
against.

### 2.3 — Source registry proposal

**Configuration, not a database table** (per the brief's own §16 instruction) — a static
Python dict is enough to let a new source be *registered* without touching pipeline code
elsewhere, and it's simpler to version/review than a Delta table with no current relational
consumer. A future coverage-reporting dashboard (brief §29) can read this same static registry
directly.

#### 2.3.1 — Source access-status model (adopting the brief's own §6/§37.F vocabulary exactly)

```python
class SourceAccessStatus(str, Enum):
    ACTIVE = "active"                          # in production use today
    RESEARCH_ONLY = "research_only"             # researched, no adapter built yet
    MANUAL_ONLY = "manual_only"                 # a human can retrieve data; no automated path
    AUTOMATION_RESTRICTED = "automation_restricted"  # automated access exists technically but
                                                       # is legally/contractually prohibited
    UNAVAILABLE = "unavailable"                 # no usable access at all currently
```

This is the field that makes the SEDAR+-vs.-ESEF contrast (§4 vs. §5) representable at all —
"has a technical API" and "is legally permitted to automate" are independent questions, and
collapsing them into one boolean (as an earlier draft of this ADR did) would have hidden
exactly the distinction that matters most here.

```python
# fundamentals_pipeline/sources/registry.py (new)

@dataclass(frozen=True)
class SourceDefinition:
    source_id: str
    jurisdiction: tuple[str, ...]           # markets/countries this source can cover
    accounting_frameworks: tuple[str, ...]
    source_type: str                        # "structured_api" | "document_repository" | "aggregator"
    access_status: SourceAccessStatus
    machine_readable: bool
    historical_depth: str                   # free text — what's actually verified, not assumed
    notes: str

SOURCE_REGISTRY: dict[str, SourceDefinition] = {
    "SEC_XBRL": SourceDefinition(
        source_id="SEC_XBRL", jurisdiction=("US",),
        accounting_frameworks=("us-gaap", "ifrs-full"),  # MJDS filers report ifrs-full via SEC too
        source_type="structured_api", access_status=SourceAccessStatus.ACTIVE,
        machine_readable=True, historical_depth="full companyfacts history",
        notes="10 req/s SEC fair-access policy; this pipeline caps at ~8 (11__fetch_sec_xbrl.py:82-83).",
    ),
    "EU_CURRENT": SourceDefinition(
        source_id="EU_CURRENT", jurisdiction=("ES", "FR", "NL", "IT", "AT", "BE", "..."),  # §4.2
        accounting_frameworks=("ifrs-full",),
        source_type="aggregator", access_status=SourceAccessStatus.RESEARCH_ONLY,  # until §7 Phase 5 ships
        machine_readable=True,
        historical_depth="ESEF mandate start = FY2020 annual reports (filed 2021+) — annual "
                          "consolidated IFRS statements only, no interim/quarterly data; scoped "
                          "to EU-regulated-market issuers, not all European companies (§4.1).",
        notes="Access path is filings.xbrl.org (XBRL International), an AGGREGATOR of ESEF "
              "filings, not ESEF itself — JSON-API, 'no restrictions on data use' stated in "
              "their own docs — see ADR-0009 §4.2. This aggregator's own index does NOT cover "
              "all EU countries (Germany, Ireland notably absent) — a retrieval gap, distinct "
              "from ESEF's own (broader) mandated scope.",
    ),
    "ESAP": SourceDefinition(
        source_id="ESAP", jurisdiction=("EU", "EEA"),
        accounting_frameworks=("ifrs-full", "national-gaap"),
        source_type="structured_api", access_status=SourceAccessStatus.UNAVAILABLE,  # not public yet
        machine_readable=True, historical_depth="none yet — collection starts 2026-07-10",
        notes="Public access legally mandated by 2027-07-10 (§4.4). Technical API spec not yet "
              "published as of this ADR. Re-research before Phase 10 (§7).",
    ),
    "SEDAR_PLUS": SourceDefinition(
        source_id="SEDAR_PLUS", jurisdiction=("CA",),
        accounting_frameworks=("ifrs-full", "aspe"),
        source_type="document_repository", access_status=SourceAccessStatus.AUTOMATION_RESTRICTED,
        machine_readable=False,  # PDF-mandated; XBRL voluntary-at-best and largely legacy-only
        historical_depth="n/a — not ingested",
        notes="Terms of Use explicitly prohibit bots/scraping and 'constructing a database' from "
              "the public filings — see ADR-0009 §5. Do not build an adapter against this entry "
              "without a licensed data-distribution path.",
    ),
    # EDINET, COMPANIES_HOUSE, CSRC/SSE/SZSE/HKEX, ASIC/ASX, DART, etc.: NOT added. The brief is
    # explicit (§15): these are architectural requirements, not researched sources. A registry
    # entry is only added once a source has been researched the way §4/§5 research SEDAR+/EU.
}
```

### 2.4 — Identity strategy

Unchanged from the first draft, and correctly so — nothing in the Europe pivot changes this: the
brief's §5/§26 `Issuer → Listing(market, ticker, exchange)` model is **not** built now.
`identity.py`/`config.tickers`'s `market` column already gives a safe `(ticker, market)`
compound key; re-keying every downstream table off a real multi-listing model is separate,
already-acknowledged migration work (`identity.py:26-29`), not a blocker for adding a second
source. European tickers get the same treatment: a `market` row per country
(`market="ES"`/`"FR"`/`"NL"`/... — not one blanket `"EU"` value, since exchange/currency/
national-source routing genuinely differ per country, per the brief's §14), admitted through a
**new**, EU-source-appropriate gate — not the existing SEC-registration gate, which has no
reason to apply to a European issuer at all.

What this ADR does add, unchanged from the first draft: a `source_id` notion at the
`config.tickers` row level, distinct from `market`, since (brief §4/§18) `market != source` and
a company can appear in more than one source system — a European company that happens to also
be an SEC filer should still be routable to `SEC_XBRL` if that's cheaper/already-compliant; the
router picks a source per ticker, not per market.

### 2.5 — Canonical mapping strategy — two-axis status/type model

Refined from the first draft to match the brief's own more precise vocabulary (§20): mapping
**status** (the accept/reject outcome) and mapping **type** (how an accepted mapping was
established) are kept as two separate fields, not conflated into one enum:

```python
class MappingStatus(str, Enum):
    ACCEPTED = "accepted"          # produces a canonical value
    AMBIGUOUS = "ambiguous"        # -> NULL
    INCOMPATIBLE = "incompatible"  # -> NULL
    UNSUPPORTED = "unsupported"    # source has nothing for this concept -> NULL

class MappingType(str, Enum):
    DIRECT = "direct"                          # same concept, same accounting meaning
    SEMANTIC_EQUIVALENT = "semantic_equivalent" # different tag, verified same meaning
    DERIVED = "derived"                        # computed from other accepted concepts
    REJECTED = "rejected"                      # explicit rejection record (paired with
                                                # status=INCOMPATIBLE or AMBIGUOUS)

@dataclass(frozen=True)
class MappingDecision:
    canonical_concept: str
    status: MappingStatus
    mapping_type: MappingType | None   # None when status != ACCEPTED and no type applies
    source_concept: str | None
    notes: str  # WHY — required, not optional; prevents "reject silently, forget why"
```

**Only `ACCEPTED` produces a canonical value** — everything else is `NULL`, per the brief's own
§3/§21 invariant (`comparability > coverage`, never map by label alone). This is explicitly a
**semantic-quality classification, not a statistical confidence score** — no probability field
is added, since the project has no methodology that would make one meaningful.

Where this already lives (§1.3) is where this extends: any new source contributes a
`<SOURCE>_CONCEPT_TAGS` dict of the `canonical_label → tag(s)` shape `IFRS_FALLBACK_TAGS`
already establishes, consumed the same generic way. Mapping stays **per-fact**, never a
standing global rule for a `(source_concept, canonical_concept)` pair (brief's period-awareness
requirement, §14 of the earlier brief) — already effectively true in today's code
(`extract_series_multi()` evaluates each period independently), so no new mechanism is needed
here, only the explicit status/type/notes record.

### 2.6 — DAG architecture

```
                          Ticker / Entity Universe
                                    |
                    +---------------+---------------+
                    |                               |
                    v                               v
              US Source (SEC)            European Source (EU_CURRENT)
                    |                               |
                    |                     (July 2027+: -> ESAP, same contract)
                    +---------------+---------------+
                                    |
                                    v
                          source normalization
                                    |
                                    v
                          canonical financials
                                    |
                                    v
                        quarterly normalization
                                    |
                                    v
                            derived metrics
                                    |
                                    v
                          valuation / analytics
```

Matches this repo's own already-proven fan-out/fan-in shape (`91d`/`91e`/`91f` → `91g`,
`databricks.yml:90-121`) — reused, not invented. `pipeline_pre22` (`91a`) splits into
per-source ingestion tasks (`ingest_sec`, later `ingest_eu`) feeding the existing merge/
transform chain, so a European ingestion failure structurally cannot fail SEC ingestion (the
brief's own §31 success criterion) — this is the one change in this ADR that must land in
`databricks.yml`, not just Python code, to actually be true rather than aspirational.

### 2.7 — Currency and market-price independence

Unchanged, and unaffected by the Europe pivot: no automatic conversion to USD anywhere in this
proposal; `reporting_currency` stays native (EUR for most of the pilot countries, though
Sweden/Denmark/Poland/etc. would carry their own currencies if ever added — not assumed
EUR-only just because "Europe"). Market-price sourcing (Yahoo Finance) stays fully decoupled
from fundamentals sourcing — `12__fetch_market_data.py` needs only the same kind of one-line
`QUOTE_CURRENCY_BY_MARKET`/Yahoo-suffix addition any new *market* would need (e.g. `.MC` for
Madrid, `.PA` for Paris, `.AS` for Amsterdam, `.MI` for Milan — verify per exchange when Phase 8
actually picks pilot tickers), independent of which *source* ingests the fundamentals.

---

## 3 — Downstream pipeline stays source-agnostic (brief §22, §J of the required deliverable)

Directly demonstrated, not just asserted: §1.2's table shows `22__derived_metrics.py` already
has **zero** `accounting_standard` branches in its formula layer today, verified by grep across
the whole file. Every margin/return/leverage/valuation formula runs on canonical concept
columns (`Revenue`, `Net Income`, `Total Assets`, ...) regardless of whether the row came from a
`us-gaap` or `ifrs-full` tag. This ADR's job is to keep that property true as sources are added
— a source's adapter maps its concepts to canonical labels or leaves them `NULL`; it never gets
to special-case a formula. No `if market == "CA": ... elif market == "DE": ...` pattern is
introduced anywhere by this design, and none should ever be added downstream of the canonical
mapper — that's the one hard rule a future PR touching `22__derived_metrics.py` should be
reviewed against.

---

## 4 — How Europe should work in 2026–2027 (Generation 1)

**Three distinct things, kept distinct throughout this section — not synonyms, not nested
subsets of one another in the way "the European source" might suggest:**

1. **ESEF** — a *regulatory format and mandate*, not a data source in itself. It defines *what
   must be tagged, by whom, and how* — it says nothing about how to retrieve any of it in bulk.
2. **`filings.xbrl.org`** — *one aggregator* that retrieves and re-publishes a subset of ESEF
   filings behind a single API. It is not ESEF itself, and its coverage is not the same thing as
   ESEF's mandated coverage (§4.2's Germany/Ireland gap is an aggregator limitation, not an ESEF
   scope limitation — those countries' issuers still file under ESEF, this aggregator just can't
   reliably retrieve what they filed).
3. **ESAP** — a *future, separate, centralized access point* (§4.4), not live yet, and not
   scoped to ESEF filings alone (it also ingests Prospectus Regulation and Short Selling
   Regulation information, later CRD/SFDR/CSRD disclosures — a broader remit than ESEF's annual-
   report-only scope).

### 4.1 — What ESEF actually is, and — as importantly — what it is not

The **European Single Electronic Format (ESEF)** is the mandatory electronic format for the
**annual financial reports of issuers whose securities are admitted to trading on an
EU-regulated market**, under the Transparency Directive — in force for financial years
commencing on/after 2020-01-01 (i.e., annual reports filed from 2021 onward). Within an ESEF
annual report, the **consolidated financial statements prepared under IFRS** are tagged in
**Inline XBRL (iXBRL)** against the IFRS taxonomy (extended with ESEF-specific extensions),
inside an XHTML document that serves both human readers and software.
[CoreFiling — What Is ESEF?](https://www.corefiling.com/esef/),
[ESMA — Electronic Reporting](https://www.esma.europa.eu/issuer-disclosure/electronic-reporting)

**What this deliberately does not mean, stated explicitly to avoid exactly the coverage
assumption this project's own invariants warn against:** ESEF is not "all European financial
data," and it is not "every European company." It covers only (a) issuers on **EU-regulated
markets specifically** (not every stock exchange segment — many growth/SME markets sit outside
this scope), (b) **annual** reports only (no ESEF-mandated interim/quarterly format — §2.1 item
5 already relies on this), and (c) only the **consolidated IFRS statements** within that annual
report get the iXBRL tagging requirement, not every disclosure in the filing. A European company
that is privately held, listed only on an SME growth market, or reporting under a national GAAP
outside the IFRS-consolidated-statements requirement may have **no ESEF filing to find at all**
— that is a real `UNSUPPORTED`/`NULL` case this architecture must produce cleanly (§2.5), not a
gap to paper over by reaching for a different, less comparable document type just to populate a
cell.

Storage/publication is **not centralized** — each EU/EEA country runs its own **Officially
Appointed Mechanism (OAM)** (e.g. Spain's CNMV runs its own XBRL portal at `cnmv.es/ipps/`,
confirmed directly — bulk ZIP download of selected XBRL reports, but no documented public API
found on CNMV's own site). A prior attempt at a federated access layer, the **European
Electronic Access Point (EEAP)**, was paused by ESMA's Board of Supervisors in **January 2018**
and never relaunched — ESAP (§4.4) is its formal successor.
[XBRL.org — Missing: A Single Electronic Access Point](https://www.xbrl.org/missing-a-single-electronic-access-point-for-the-european-single-electronic-format/)

### 4.2 — The Generation-1 recommendation: `filings.xbrl.org`, not per-country OAM scraping

Direct research found a real, already-existing aggregator that solves the fragmented-OAM
problem *without* waiting for ESAP and *without* scraping anything:

**`filings.xbrl.org`**, run by **XBRL International** (the standards body itself — the same
organization that governs the XBRL/iXBRL specification ESEF is built on, not a third-party
scraper), indexes ESEF (plus UK-SEF and Ukraine) filings across **28 European jurisdictions**
(`AT BE CY CZ DK EE ES FI FR GB GR HR HU IS IT LT LU LV MT NL NO PL PT RO SE SI SK UA`),
~25,675 filings at time of research, via a documented **JSON-API** at
`https://filings.xbrl.org/api/filings` (JSON-API spec), with an official Python client on PyPI
(`xbrl-filings-api`). Its own documentation states: **"At present, there are no restrictions on
the ways that the data can be used."**
[filings.xbrl.org — About](https://filings.xbrl.org/docs/about)

This is the exact opposite finding from SEDAR+ (§5): a structurally SEC-companyfacts-like
source (tagged XBRL facts, JSON API, IFRS taxonomy) with an explicitly permissive usage policy,
run by a credible, stable, non-commercial operator. It directly satisfies the brief's own §9
Generation-1 instruction ("use the best available legitimate machine-readable source, not
whichever is easiest to scrape") — this is not the easiest option (a raw HTML scrape of one
country's site might be "easier"), it's the legitimate one.

**Caveat, confirmed not assumed:** coverage is real but not complete — `filings.xbrl.org`'s own
docs name **Germany and Ireland** specifically as countries whose ESEF filings aren't reliably
discoverable/downloadable through this aggregator (their national OAMs don't expose filings in
a way the aggregator can index). This directly shapes pilot country selection (§6): **Spain,
France, Netherlands, and Italy — all four of the brief's own suggested candidates — are
confirmed present in the index; Germany is not**, so Germany should be dropped from the Phase-8
pilot list or handled via a separate, explicitly-researched national-OAM path later, not
assumed to work the same way.

### 4.3 — Why this is a closer architectural fit than Canada's case would have been

ESEF facts are IFRS-taxonomy-tagged XBRL — structurally the same *shape* of data
`IFRS_FALLBACK_TAGS` already consumes from SEC's `companyfacts` API for MJDS filers (§1.1). The
European adapter's canonical-mapping layer is therefore not new architecture, only a new
`EU_CONCEPT_TAGS`-shaped dict (§2.5) plus a new fetch/parse function pointed at
`filings.xbrl.org`'s JSON-API instead of SEC's `companyfacts` JSON — a real but bounded, well-
understood engineering task, unlike SEDAR+'s PDF-extraction problem (§5.3).

### 4.4 — ESAP: current status, verified, not assumed live

Per official/near-official sources (ESMA's own ESAP page, AMF, EUR-Lex legal summary):

- **2026-07-10:** ESAP begins **collecting** information from national "Collection Bodies" —
  Phase 1 scope is Transparency Directive, Prospectus Regulation, and Short Selling Regulation
  information. This is a collection start date, **not public access**.
- **2027-07-10:** ESAP is legally mandated to be established and operating for **public access**
  by this date.
- **2028-01-10 (Phase 2):** credit-institution (CRD) and sustainability-disclosure (SFDR, EU
  Taxonomy, CSRD/ESG) reporting joins ESAP.
- **2029-01-10 (Phase 2bis)** and **2030-01-10 (Phase 3):** further scope expansion.

[ESMA — European Single Access Point](https://esma.europa.eu/esmas-activities/data/european-single-access-point-esap),
[AMF — ESAP enters its implementation phase](https://www.amf-france.org/en/news-publications/news/european-single-access-point-financial-and-non-financial-information-european-entities-esap-enters),
[EUR-Lex — European single access point](https://eur-lex.europa.eu/EN/legal-content/summary/european-single-access-point.html)

**No technical API specification was found publicly available as of this research.** This ADR
does **not** design against an assumed ESAP API shape — the `ESAP` registry entry (§2.3) is
explicitly `access_status=UNAVAILABLE` with a note to re-research before Phase 10 (§7). Building
speculative code against an unpublished spec would be exactly the "skip to Phase 10" mistake the
brief's own Phase ordering warns against.

**Migration contract for when ESAP does open (documented now, built later):** ESAP becomes a
second, later `FundamentalsSource` implementation — `source_id="ESAP"` — emitting into the exact
same `SourceFact` shape `EU_CURRENT` already does. The canonical mapper, `21__clean_and_merge.py`
onward, and every downstream consumer need **zero** changes for this migration; only the
adapter (`discover_entities`/`discover_filings`/`retrieve_facts`) gets replaced. Whether
`EU_CURRENT` is then deprecated, kept as a fallback for ESAP-not-yet-covering-a-filer cases, or
run in parallel for cross-validation is a decision for whoever executes Phase 10/11, informed by
ESAP's actual coverage/quality once it's real — not decided speculatively here.

---

## 5 — How Canada/SEDAR+ should work (registered, not forced)

Unchanged conclusion from the first draft of this ADR, restated briefly since the pilot focus
moved to Europe but the underlying research and recommendation didn't change:

### 5.1 — What was verified

- **No official public API.** The CSA has stated an intention to enable APIs "where possible"
  in future phases — not scheduled, not something to build against.
- **Financial statements are PDF-mandated**; XBRL was only ever voluntary on legacy SEDAR and is
  not meaningfully present on the live SEDAR+ platform.
- **SEDAR+'s own Terms of Use explicitly prohibit** automated bots/scraping and "constructing a
  database" or "mass distribution" from the public filings — directly describing this project's
  actual use case as a prohibited one.
  [SEDAR+ Terms of Use](https://systems.securities-administrators.ca/terms-of-use/)
- A licensed commercial data-distribution path exists (at least one named vendor partnership
  confirmed) but reads as enterprise-tier, not self-serve for a personal/hobby-scale project.

### 5.2 — Decision, restated

`SEDAR_PLUS` is registered (§2.3) with `access_status=AUTOMATION_RESTRICTED`. No adapter is
built. The already-compliant path — Canadian MJDS/40-F filers ingested via `SEC_XBRL`, exactly
as today — continues and can keep expanding within that population; nothing about the Europe
pivot narrows it further. If a compliant SEDAR+ access path (an official future API, or a
licensed feed) becomes available later, `SEDAR_PLUS` gets promoted to `ACTIVE` and follows the
same onboarding process (§7) as any other new source — it does not get a special-cased path for
having been "the original plan."

---

## 6 — European pilot scope (brief §35, Phase 8)

Not executed by this ADR (Phase 8 is later, §7, well after Phase 3) — scoped here so Phase 5-8
have a concrete target already on record:

**Candidate countries, confirmed present in `filings.xbrl.org`'s index (§4.2):** Spain, France,
Netherlands, Italy. Germany is explicitly excluded from this list per §4.2's finding, not
merely omitted by oversight.

**Selection criteria, per the brief's own §35 intent (validate the architecture, not maximize
coverage):** 3-5 companies per country, spanning sectors already represented in the brief's own
list (financials, energy, industrials, technology, telecom, consumer) — final tickers to be
selected in Phase 8 by actually querying `filings.xbrl.org`'s index for which companies have
clean, complete, low-`error_count`/`warning_count` filings (the API exposes these quality
signals directly, per §4.2's response-shape research), deliberately including at least one
issuer with a known messier presentation (e.g. a diversified industrial with unusual segment
reporting) alongside straightforward ones — mirroring the brief's own explicit ask to stress-test
the architecture, not cherry-pick easy cases.

---

## 7 — Migration plan (brief §38, mapped onto this repo's real files)

1. **This ADR** (Phase 1-2) — done. Accepted or revised by the repo owner before code changes.
2. **`fundamentals_pipeline/sources/` package** (Phase 3): `FundamentalsSource`/`SourceEntity`/
   `SourceFiling`/`SourceFact` (§2.2), the registry (§2.3), the `MappingStatus`/`MappingType`
   model (§2.5) — new, pure-Python, unit-tested, no behavior change to existing ingestion.
   `11__fetch_sec_xbrl.py` is refactored to implement the interface (no logic change — §1.1
   already showed its structure maps onto the contract). `source_id` added to `financials_raw`
   and `financials`, additive — populated going forward on new/updated rows, not retroactively
   backfilled onto unchanged history (see item 2 above).
3. **`databricks.yml` ingestion-tier split** (also Phase 3, §2.6): `pipeline_pre22` splits into
   per-source ingestion tasks feeding the existing merge/transform chain. Delivers the brief's
   "source failures isolated" success criterion structurally, testable with just one source
   before a second exists.
4. **European source research** (Phase 4) — done, this ADR, §4.
5. **European pilot source adapter** (Phase 5): `EU_CURRENT` implementation against
   `filings.xbrl.org`'s JSON-API (§4.2/§4.3).
6. **Canonical IFRS → US-oriented mapping** (Phase 6): `EU_CONCEPT_TAGS` dict (§2.5), following
   the same conservative scope `IFRS_FALLBACK_TAGS` already established (core statement lines
   only; Operating Income/Gross Profit/granular debt stay `NULL` where IFRS presentation is
   genuinely heterogeneous — no new research needed here, the existing dict's scope already
   reflects the right level of caution).
7. **Validation + tests** (Phase 7): mapping-status test matrix (ACCEPTED → value, AMBIGUOUS/
   INCOMPATIBLE/UNSUPPORTED → NULL), a `35__reconcile_filings.py`-equivalent for `EU_CURRENT` if
   `filings.xbrl.org`'s own `error_count`/`warning_count`/validation-message fields don't already
   cover that need (check before building a duplicate), regression tests asserting the US
   pipeline is byte-identical before/after Phase 3's refactor.
8. **European pilot universe** (Phase 8): the ~15-20 companies scoped in §6.
9. **DAG integration** (Phase 9): wire `ingest_eu` into the split from Phase 3.
10. **ESAP migration design/documentation** (Phase 10): re-research ESAP's actual public API
    once available (not before — §4.4), write the follow-up ADR for the `EU_CURRENT` → `ESAP`
    adapter swap.
11. **July 2027+: reassess and implement the ESAP adapter** (Phase 11) — the actual migration,
    gated on Phase 10's findings and on ESAP's real public-access date holding.

Canada/SEDAR+ remains registered as a future source (§5) unless a compliant automated-access
method is identified — no phase in this plan schedules building one.

**How this avoids breaking the existing US pipeline:** every step through Phase 3 is additive
(`source_id` column, MERGE default) or a pure refactor with no behavior change (interface
extraction, DAG task split with identical task content). `financials`/`financials_metrics`/every
frontend is unaffected until Phase 5's `EU_CURRENT` rows actually start flowing through the
mapper, which doesn't happen in Phases 1-4.

---

## Answering the required deliverable directly (brief §37, A–K)

- **A. SEC-specific:** §1.1/§1.2 — CIK resolution, `companyfacts` URL/JSON shape, hardcoded SEC
  form-type lists, the Canadian-admission gate's SEC-registration dependency, `fp` vocabulary in
  `21b`, `35__reconcile_filings.py`'s us-gaap-only oracle.
- **B. Already source-neutral:** §1.3/§1.4 first two bullets — `identity.py`,
  `tickers_universe.py`, `fx.py`, the canonical schema (`schemas.py`/`artifacts.py`/
  `concept_hierarchy.json`), `22__derived_metrics.py`'s formula layer, `12__fetch_market_data.py`'s
  `.TO`-suffix pattern, `21c`-`21h`.
- **C. Must become source-agnostic:** §2.1's six numbered changes — adapter contract,
  `source_id` provenance column, registry, extended tag-mapping dicts, `21b`'s `fp` handling
  (deferred for the pilot per §2.1 item 5), `databricks.yml`'s ingestion-tier split.
- **D. Source registry design:** §2.3, with the access-status model (§2.3.1) as the key addition
  beyond the brief's own original field list.
- **E. Adapter contract:** §2.2.
- **F. Source-access model:** §2.3.1 (`ACTIVE`/`RESEARCH_ONLY`/`MANUAL_ONLY`/
  `AUTOMATION_RESTRICTED`/`UNAVAILABLE`), populated concretely for all four registered sources
  in §2.3's table.
- **G. Europe 2026-2027:** §4 — the ESEF mandate (annual, EU-regulated-market issuers, IFRS
  consolidated statements only — §4.1) accessed via `filings.xbrl.org` as the Generation-1
  aggregator (§4.2), with both its scope and its retrieval gaps verified and kept distinct.
- **H. Europe from July 2027:** §4.4 — ESAP migration contract, explicitly not built against an
  unpublished API.
- **I. Canada:** §5 — registered, `AUTOMATION_RESTRICTED`, not forced.
- **J. Canonical model stays unchanged / downstream is source-independent:** §3, demonstrated
  from the existing codebase's own behavior, not just asserted.
- **K. DAG changes:** §2.6.

---

## Consequences

- A source can be added by registering it (§2.3) and implementing one interface (§2.2), without
  touching `21__clean_and_merge.py` onward.
- The canonical model's shape is unchanged — `tag_namespace` (already generic) and the new
  `source_id` are the only schema additions, both additive.
- **Europe gets a real, working Generation-1 ingestion path out of this ADR's research** — a
  materially different outcome from Canada's, and the direct reason the pilot moved. This is
  not a downgrade of ambition; it's the brief's own §36 "research first" principle actually
  changing the plan based on what was found, which is the point of doing the research at all.
- Canada does not get a working pipeline from this ADR, by design (§5) — a deliberate,
  documented consequence, not a shortfall.
- The DAG gets more complex (a new ingestion-tier fan-out) before it gets more capable — paid
  for once in Phase 3, independent of how many sources eventually exist.
- Four duplicated `QUOTE_CURRENCY_BY_MARKET` copies (§1.4) become a real liability once a third
  currency is added — worth consolidating into `fx.py`/`identity.py` during Phase 3.
- `35__reconcile_filings.py`'s Tier-A oracle stays SEC/us-gaap-only until Phase 7 decides whether
  `filings.xbrl.org`'s own validation-message fields make a European equivalent unnecessary.
- `CLAUDE.md`'s "IFRS mapping still out of scope" line needs a correction (§1.1) — independent
  housekeeping this ADR surfaces but doesn't perform.
- ESAP's actual API, once published, may turn out to differ enough from this ADR's assumptions
  that Phase 10 requires real design work, not a rubber-stamp — flagged explicitly rather than
  promised away.

## Alternatives considered

- **Build country-specific pipelines (`CanadaIngestion`, `EuropeIngestion`, `financials_ca`,
  `financials_eu`, ...).** Rejected — the brief's own explicit constraint, and independently,
  §1's audit found the general version is barely more work, since the existing
  `STATEMENTS`/`IFRS_FALLBACK_TAGS`/`market` patterns already generalize almost for free.
- **Keep Canada as the first pilot, force a SEDAR+ adapter anyway.** Rejected — this is what the
  first draft of this ADR proposed avoiding, and the repo owner's own redirected brief confirms
  the same conclusion: don't let Canada's access constraints dictate or stall the architecture.
- **Wait for ESAP before doing any European work.** Rejected — ESAP's public-access date
  (2027-07-10) is roughly a year out from this ADR, and a real, legitimate, permissively-licensed
  Generation-1 path (`filings.xbrl.org`) already exists today; waiting would forfeit a year of
  real architecture validation for no compliance benefit (`filings.xbrl.org` is not
  access-restricted the way SEDAR+ is).
- **Treat `filings.xbrl.org` as good enough to skip ESAP planning entirely.** Rejected —
  coverage gaps (Germany, Ireland) and its non-governmental operator status mean it's a good
  Generation-1 bridge, not a permanent replacement for ESAP's eventual official, presumably
  more complete coverage. The two-generation adapter design (§2.1, §4.4) keeps both possible
  without betting the architecture on either being permanent.
- **Design against ESAP's API now, based on the brief's own description of it.** Rejected — no
  public technical spec was found; speculative code against an unpublished API is exactly the
  "skip to Phase 10" mistake the brief's own phase ordering warns against. The registry entry
  and migration contract (§2.3, §4.4) are the correct amount of preparation without overreach.
- **Collapse mapping status and mapping type into one enum** (as the first draft of this ADR
  did). Rejected on revision — the brief's own more precise §20 vocabulary (status vs. type as
  two axes) is cleaner and was adopted (§2.5).
