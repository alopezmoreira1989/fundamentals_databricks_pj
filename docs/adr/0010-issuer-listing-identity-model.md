# ADR-0010: Issuer/listing identity model — `issuer_id`, `listing_id`, transitional bare-ticker keys

- **Status:** Proposed
- **Date:** 2026-08-16
- **Deciders:** repo owner

## Context

ADR-0009 designed the source-agnostic `FundamentalsSource` framework and picked Europe (via
`filings.xbrl.org`/ESEF) as the first non-US pilot. Before implementing that pilot, a repo-wide
identity audit (Phase 5.0, this session) confirmed that a bare `ticker` string is treated as the
sole company identity almost everywhere: `financials_raw`, `financials`, `market_prices_daily`,
`stock_splits`, `market_cap_asof`, `financials_metrics`, `financials_intrinsic_value`, both
frontends (`fundamentals_screener` and the Streamlit app), and even the new (currently uncalled)
`sources/base.py`'s `SourceEntity.ticker` field.

Only `main.config.tickers` has a real composite `(ticker, market)` identity key, guarded by
`fundamentals_pipeline/identity.py`'s `check_no_cross_market_collision()` — and that guard's own
docstring already states the downstream tables stay bare-ticker-keyed. A bare ticker is not a
global identifier: the same symbol can legitimately belong to two unrelated companies on
different markets (Magna International `MG` on the TSX vs. Mistras Group `MG` on the NYSE,
already a real collision in production `config.tickers`), or to one company listed twice
(Brookfield `BAM` on both NYSE and TSX). Introducing a European source without a real identity
model would reproduce this exact risk at every layer that guard does not reach.

MIC (ISO 10383) verification, done live this session via ESMA/FIRDS and vendor MIC registries,
found a real ambiguity worth recording as a project-wide rule: several exchange groups have both
an *operating* MIC (the legal-entity/group code) and one or more *segment* MICs (the specific
market a security actually trades on). Madrid: `XMAD` (Equities segment) vs. `BMEX` (operating).
Milan: `MTAA` ("Euronext Milan", the cash-equities segment, per ESMA/FIRDS) vs. `XMIL` (operating,
covers derivatives/growth-market/bonds segments too). Paris (`XPAR`) and Amsterdam (`XAMS`) have
no equivalent split for their national cash-equities markets.

## Decision

We will introduce two identity primitives — **not** a general entity-resolution system:

- **`issuer_id = f"{source_id}:{source_entity_id}"`** (e.g. `SEC_XBRL:0000320193`,
  `EU_CURRENT:96950032TUYMW11FB530`) — a **source-qualified** issuer identity. It is explicitly
  NOT a claim of a universal cross-source identity: two different sources' `issuer_id` for the
  same real company do not compare equal, and no cross-source entity-resolution/matching is
  implemented or attempted here.
- **`listing_id = f"{MIC}:{TICKER}"`** (both upper-cased, e.g. `XNAS:AAPL`, `XMAD:FCC`) — the
  identity of one specific listing/security on one specific market. `mic` must be a real,
  verified ISO 10383 code for the market the security actually trades on; where an exchange
  group has both an operating and a segment MIC, **the segment MIC that identifies the specific
  cash-equities market wins**, not the parent operating MIC. Verified for the four Phase 5.1
  pilot issuers: Madrid `XMAD`, Paris `XPAR`, Amsterdam `XAMS`, **Milan `MTAA`** (not `XMIL`).

Both are pure functions (`fundamentals_pipeline.identity.make_issuer_id`/`make_listing_id`), not
classes or a registry — deliberately minimal, matching the existing shape of `identity.py`.

`SourceEntity` (`fundamentals_pipeline/sources/base.py`) is corrected to use
`source_id`/`source_entity_id`/`issuer_id` as its identity; `ticker` becomes optional
convenience/input metadata (an adapter may take tickers as discovery input), explicitly
documented as never globally unique and never the entity's actual identity.

Financials are modeled as **issuer-level** data (a filing is made once by the reporting entity
regardless of how many markets it lists on); market prices/market cap are modeled as
**listing-level** data (a price feed is inherently tied to one quoted security on one market).

**This pass is additive and transitional, not a re-key.** `issuer_id`/`mic`/`listing_id` are
added as new, nullable columns on `main.config.tickers` only — the four core financial/price
Delta tables named above keep their existing bare-`ticker` physical keys unchanged. `issuer_id`
is backfilled for the whole existing US/CA universe (a pure function of the CIK
`11__fetch_sec_xbrl.py` already resolves for every ticker). `mic`/`listing_id` are **not**
backfilled for the existing ~2,662 US/CA rows in this pass — that needs a Yahoo-exchange-mnemonic
→ real ISO 10383 MIC mapping that does not exist yet, and inventing/approximating one would
violate the same "never guess a MIC" principle this ADR itself establishes. The designed (not yet
executed) future re-key migration for the six core tables is documented separately in
`docs/phase5-identity-listing-model.md` (a living design doc, not an ADR, since the migration
itself is future work, not yet a decision to lock in).

No European rows are added to `config.tickers` in this pass, no European adapter is implemented,
and neither `fundamentals_screener` nor the Streamlit app is touched — both remain downstream
consumers of the bare-ticker-keyed published artifacts, unchanged.

## Consequences

**Easier:** a future European (or other non-US) source can populate `issuer_id`/`listing_id`
correctly from day one without inventing per-source conventions ad hoc. The ticker-collision risk
this ADR was written in response to is now representable (`XNAS:ABC` vs. `XPAR:ABC` are distinct
`listing_id`s) at the primitive level, ready for the tables that will eventually key on it. The
`SourceEntity` correction closes a real gap the Phase 5.0 audit found: the framework built in
ADR-0009 had, without this, reproduced the exact bare-ticker collision risk it exists to avoid.

**Harder / deferred, on purpose:** the core financial and price tables remain bare-ticker-keyed
until a dedicated future migration executes the re-key designed in
`docs/phase5-identity-listing-model.md` — this ADR does not claim that problem solved, only that
the correct metadata now exists to eventually solve it without another redesign. `mic`/
`listing_id` are genuinely unknown for the existing ~2,662 US/CA rows until the exchange-mnemonic
→ MIC mapping work happens; code must not assume they are populated for pre-existing rows.
`issuer_id` for two different sources cannot be compared to detect "is this the same company" —
that remains unimplemented entity-resolution work, tracked but not started.

## Alternatives considered

- **A synthetic global `issuer_id`/UUID minted at admission time**, independent of any source's
  own identifier. Rejected: adds a new ID-minting system with its own persistence/uniqueness
  concerns for no immediate benefit — the source-qualified form is legible (you can read the CIK
  or LEI straight out of it), sufficient for this pass's scope, and doesn't foreclose adding a
  true cross-source identity layer later if entity resolution is ever built.
- **Executing the full table re-key now** (financials/prices tables keyed on `issuer_id`/
  `listing_id` immediately). Rejected: these are live production Delta tables feeding a
  versioned public API contract (`fundamentals_screener`) and the Streamlit app; re-keying them
  is a breaking migration that needs its own dedicated, reviewed phase — not something to fold
  into introducing the identity model itself.
- **Backfilling `mic` for the existing universe via a best-effort Yahoo-exchange-mnemonic
  mapping now.** Rejected: several Yahoo mnemonics map ambiguously to more than one real MIC
  without further research (the same operating-vs-segment ambiguity found for Milan), and
  guessing would violate the "never invent a MIC" principle this ADR itself sets for the pilot.
- **Keeping `SourceEntity.ticker` as the identity field with `mic` merely added alongside it.**
  Rejected per repo owner feedback: this still lets `ticker` function as an implicit identity in
  practice; making `source_id`/`source_entity_id`/`issuer_id` the real fields and `ticker`
  explicitly optional/non-identity is the change that actually closes the gap.
