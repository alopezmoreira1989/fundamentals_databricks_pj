# ADR-0012: Listing identity key — `MIC:ISIN` for new listings, ticker kept as attribute

- **Status:** Proposed
- **Date:** 2026-08-16
- **Deciders:** repo owner

## Context

ADR-0010 (still Proposed) introduced `listing_id = f"{MIC}:{TICKER}"` as the identity of one
specific listing on one specific market, alongside `issuer_id = f"{SOURCE}:{SOURCE_ENTITY_ID}"`
for the reporting entity. Both are additive, nullable columns on `main.config.tickers` only —
no core financial or price table is keyed on either today.

Phase 5.2c/5.2d subsequently investigated ESMA FIRDS as the likely Generation-1 European
universe source and found its native reference-data key is `(ISIN, MIC)` — FIRDS has **no**
ticker field anywhere in its schema. Phase 5.2d's own primary-listing selection rule
(`IssrReq = true`, tie-broken by earliest `FrstTradDt`) operates entirely on ISIN/MIC-native
fields and never references a ticker. This raised the open question this ADR resolves: should
`listing_id` be `MIC:TICKER` (as ADR-0010 originally defined it) or `MIC:ISIN`, before the
European Universe & Admission Layer (Phase 5.3) is built on top of a frozen listing identity.

A full research pass — [docs/phase5-2e-listing-identity-key-research.md](../phase5-2e-listing-identity-key-research.md) —
was done to answer this with real evidence rather than intuition. Its findings, summarized:

- **A real, verified ticker-change example**: France Télécom S.A. rebranded to Orange S.A. on
  1 July 2013 (same legal entity, not a merger). The security's ISIN, `FR0000133308`, is
  confirmed unchanged across the rename by multiple independent sources; its Euronext Paris
  ticker changed from `FTE` to `ORA`. Live-reconfirmed via OpenFIGI this session: the ISIN
  today resolves to ticker `ORA`. Under `MIC:TICKER`, this listing's identity would have broken
  (`XPAR:FTE` → `XPAR:ORA`) for a security that never stopped being the same listing. Under
  `MIC:ISIN`, it would have stayed `XPAR:FR0000133308` throughout.
- **A real multi-venue test**: every one of Phase 5.1's four pilot ISINs legitimately spans many
  MICs in FIRDS (FCC 35, Alstom 45, New Amsterdam Invest 16, Fincantieri 41) — `MIC:ISIN`
  distinguishes each by construction, with zero special-case logic, matching FIRDS' own
  per-venue record grain exactly.
- **A real share-class test**: Volkswagen AG's ordinary (`DE0007664005`, ticker `VOW`) and
  preference (`DE0007664039`, ticker `VOW3`) shares, live-confirmed via OpenFIGI — both keying
  schemes distinguish this real case correctly (the two tickers happen to differ too), so it
  doesn't independently prove `MIC:TICKER` unsafe, but confirms `MIC:ISIN` works with no
  special-casing.
- **A real, code-grounded migration-cost inventory**: `ticker` is the actual physical Delta
  partition column for `financials`, a real merge/window key for `financials_metrics`,
  `financials_intrinsic_value`, `market_prices_daily`, `stock_splits`, and `market_cap_asof`,
  and the sole company-identity key in both `fundamentals_screener` (a versioned public API
  contract, ~781 real occurrences) and the Streamlit app (~336 occurrences). Against this,
  `issuer_id`/`mic`/`listing_id` are, today, populated for **zero** rows of the real
  `main.config.tickers` table — not for the pre-existing US/CA universe (ADR-0010's own
  documented gap) and not for the four EU pilots either, which the research corrected: they are
  never written to `config.tickers` at all (`16__fetch_eu_xbrl.py` explicitly does not touch
  that table). Nothing in production reads either identity column for a real decision yet.
- **A structural constraint independent of this decision**: yfinance, the sole upstream source
  for `market_prices_daily`/`stock_splits`/`market_cap_asof`, has no ISIN- or MIC-based lookup
  in its API — only ticker symbols. Whatever `listing_id` becomes, a ticker (or Yahoo-specific
  symbol) will always be required to actually fetch a price. This means ticker cannot be
  eliminated from the system either way; the question is only whether it is the *identity* or a
  *resolved attribute*.

## Decision

We will define **`listing_id = MIC:ISIN`** as the canonical listing identity for **new listings
admitted from an ISIN-native source** (i.e., anything the future FIRDS-based European admission
layer, Phase 5.3, admits) — not as a re-key of anything that exists today.

`ticker` becomes an explicit, mutable **attribute** of a listing under this model, not part of
its identity: resolved at admission time (via OpenFIGI, per ADR-0011's own chain) and subject to
re-resolution if it changes, rather than baked into an identity string that would then need to
change alongside it. `issuer_id` (ADR-0010) is unaffected by this decision.

**This decision is deliberately scoped to new listings only.** The pre-existing US/CA
`config.tickers` universe, and the physical `ticker`-partitioned/merged core tables
(`financials`, `financials_metrics`, `financials_intrinsic_value`, `market_prices_daily`,
`stock_splits`, `market_cap_asof`), keep their real, current, unchanged `MIC:TICKER`-shaped
(in practice, bare-ticker) identity. Nothing about how those tables are keyed changes as a
result of this ADR. `fundamentals_screener`'s URL routes and the Streamlit app's ticker-based
lookups are equally untouched — they may continue to use `ticker` as the routing/display key
even for a `MIC:ISIN`-identified listing underneath, since a listing's `ticker` attribute is
still available for that purpose (§12/§22 of the research doc).

The reasoning, in order of weight:

1. **Identity stability is the primary requirement**, and the real Orange test (not a
   hypothetical) shows `MIC:TICKER` fails it while `MIC:ISIN` passes.
2. **Zero-cost timing**: `issuer_id`/`listing_id` are unconsumed by any production code path
   today (verified, not assumed) — redefining their target representation now, before Phase 5.3
   starts writing real rows under whichever definition is chosen, is the cheapest point in the
   project's history to make this call. Once a real admission layer starts writing
   `MIC:TICKER`-keyed rows for European listings, correcting course would mean a real migration
   instead of a definition choice.
3. **Source alignment**: four of the five identity sources this project already integrates with
   (FIRDS, GLEIF, filings.xbrl.org, and — for its own primary key — OpenFIGI's FIGI) are
   ISIN/LEI-native; only the market-price source (yfinance) is ticker-only, and that constraint
   is preserved either way by keeping ticker as a required attribute.

## Consequences

**Easier**: Phase 5.3's admission layer can key European listings by the same `(ISIN, MIC)`
grain FIRDS itself already uses, with no translation step for identity purposes (a ticker is
still resolved for display, but its absence or a future change no longer threatens the
listing's identity). The real Orange-style failure mode this ADR was written to avoid becomes
structurally impossible for any listing admitted under this rule. Phase 5.2d's own
primary-listing selection rule requires no adjustment — it already operates entirely in
ISIN/MIC terms.

**Harder / deferred, on purpose**: `MIC:ISIN` is materially less human-readable than
`MIC:TICKER` (`XPAR:FR0010220475` vs. `XPAR:ALO`) — mitigated, not eliminated, by keeping
ticker as a queryable attribute for anywhere a human or a frontend needs it. `identity.py`'s
`make_listing_id(mic, ticker)` signature will need to change (or gain a sibling function) once
this is actually implemented — not done by this ADR. The pre-existing US/CA universe's real
migration question (should it eventually move to `MIC:ISIN` too, for consistency) is explicitly
**not** decided here — see Open questions. `market_prices_daily`/`stock_splits`/
`market_cap_asof` will need a ticker-resolution join layer in front of them for any
`MIC:ISIN`-identified listing, forever, since yfinance itself cannot be queried by ISIN — this
is a real, permanent architectural seam this decision does not remove, only relocates.

## Migration implications

None today. This ADR changes a *definition* that nothing yet reads or writes in production.
The real migration cost — re-partitioning/re-merging the six core tables listed above, and
touching `fundamentals_screener`'s versioned URL contract — is not proposed, scoped, or
scheduled by this ADR. It becomes relevant only if a future decision extends `MIC:ISIN` to the
existing US/CA universe, which is explicitly out of scope here (see Open questions).

## Non-goals

- Does not re-key `config.tickers`, `financials`, `financials_metrics`,
  `financials_intrinsic_value`, `market_prices_daily`, `stock_splits`, or `market_cap_asof`.
- Does not change `fundamentals_screener`'s URL routes, DTOs, or template contract.
- Does not change the Streamlit app.
- Does not change `identity.py`'s actual function signatures (a future implementation phase
  does that).
- Does not decide whether the pre-existing US/CA universe should ever move to `MIC:ISIN`.
- Does not begin Phase 5.3 (the European Universe & Admission Layer).
- Does not flip itself, ADR-0010, or ADR-0011 to Accepted.

## Open questions

- Whether a same-ticker-root share-class collision risk exists anywhere in the real European
  universe FIRDS would surface (only the Volkswagen case — which has genuinely distinct
  tickers — was checked; not proven absent in general).
- Whether the eventual US/CA re-key already deferred by ADR-0010 should also target `MIC:ISIN`
  once undertaken, for project-wide consistency — a separate, future decision, not this one.
- The exact mechanics of ticker re-resolution/staleness detection for `MIC:ISIN`-keyed listings
  (designed only at the level of "an attribute, resolved via OpenFIGI at admission time," not
  specified further).
- Whether ISIN reissuance on corporate restructuring (a real but rare occurrence, per ISIN
  conventions) has ever affected, or could plausibly affect, any of the four pilots or a
  near-term European candidate — not investigated.

## Alternatives considered

- **Keep `MIC:TICKER` as-is (status quo).** Rejected: the real Orange evidence shows this fails
  the identity-stability requirement the `listing_id` primitive was introduced to satisfy in
  the first place, and FIRDS' own schema has no ticker field, meaning every FIRDS-sourced
  candidate would need an extra, avoidable resolution hop before an identity could even be
  constructed.
- **A synthetic internal `listing_id` (a minted surrogate key) with `(MIC, ISIN)` as its
  defining attributes.** Rejected for the same reason ADR-0010 rejected an equivalent synthetic
  `issuer_id`: it adds a new ID-minting system with its own persistence/uniqueness concerns for
  no benefit over a legible, directly composable `MIC:ISIN` string that already carries no
  ambiguity.
- **Re-key the entire existing universe to `MIC:ISIN` now, for consistency.** Rejected as
  premature and out of scope: this would be a real, costly migration touching live production
  Delta tables and a versioned public API contract, undertaken to solve a problem (identity
  instability) that has not actually been observed in the existing US/CA data, and is a
  separate decision from what representation *new* listings should use.
- **Keep ticker as part of the identity but add ISIN as a secondary lookup column.** Rejected:
  this doesn't solve the actual stability problem (the identity string itself would still break
  on a ticker change like Orange's) — it only adds a convenience index, not a fix.

## Implementation note

[docs/phase5-3-european-universe-admission.md](../phase5-3-european-universe-admission.md)
implements `make_listing_id_from_isin()` (the `MIC:ISIN` function this ADR decided) as part of
the new European admission layer — real code, real tests, a real full-scale run against ESMA
FIRDS. This ADR's status is **not** changed by that implementation; a Proposed ADR being
implemented ahead of acceptance mirrors ADR-0010's own `make_issuer_id`/`make_listing_id`
precedent (also implemented while Proposed). Still awaiting the repo owner's review before
Accepted.
