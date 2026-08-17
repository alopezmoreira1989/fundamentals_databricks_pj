# Phase 6.6 — Tier A EU Financial-Statement Coverage: Implementation and Validation

**Status: implemented, tested, and validated end-to-end against real production data —
`16 → 21 → 22 → 23 → idempotency → 51 → 52 → publish`. Live website not yet refreshed (external
cron dependency, documented, unchanged from Phase 6.3).** Implements the 8 Tier A concepts
identified and evidence-gathered by the Phase 6.4 audit
(`docs/phase6-4-european-financial-statement-coverage-audit.md` §12), per the Phase 6.6b research
finding (`docs/phase6-6-esef-taxonomy-research.md`) that manual, evidence-verified mapping remains
the correct architecture — no taxonomy-assisted or automatic mapping was introduced.

---

## 1. The 8 Tier A concepts

1. Accounts Payable
2. Non-Controlling Interests (**new** canonical concept)
3. Stock-based Compensation (already existed — a real correction made during implementation, see
   §2)
4. Change in Working Capital → implemented as the existing `Changes in Working Capital` concept
5. Income Before Tax (existing concept — no new `Profit Before Tax` concept created)
6. EPS Basic / EPS Diluted (existing concepts, 2 tag variants each)
7. Finance Income (**new** canonical concept)
8. Accounts Receivable

## 2. Implementation summary

`sources/eu_current.py`'s `EU_CANONICAL_MAPPING` widened from 21 to **30 canonical concepts / 36
accepted source tags**. Two corrections made during implementation, not assumed from the audit
document verbatim:

- **"Stock-based Compensation" already existed** as a canonical concept
  (`01__tickers.py` `CASH_FLOW`, tag `ShareBasedCompensation`) — the Phase 6.4 audit's "new
  concept" framing was wrong, caught by reading the full `STATEMENTS` dict before implementing.
- **Accounts Payable needed three real tag variants, not two** — IBE tags
  `ifrs-full:TradeAndOtherPayablesToTradeSuppliers` (no "Current" qualifier), a real variant
  neither of the other two tags cover.

Two genuinely new canonical concepts added to `01__tickers.py`'s `STATEMENTS`:
`Non-Controlling Interests` (`BALANCE_SHEET`, tag `MinorityInterest`) and `Finance Income`
(`INCOME_STATEMENT`, tag `InvestmentIncomeInterest` — flagged in code comments as less
independently verified against live US data than every other entry, since this project's "never
guess a tag" discipline was honored by choosing a real, standard, well-documented us-gaap element
rather than an unverified one, while being explicit about the confidence gap).

**Accounts Receivable — the phase's own flagged highest-risk item** — implemented via
`EU_CANONICAL_MAPPING` only, deliberately not touching `01__tickers.py`'s separate, pre-existing
`IFRS_FALLBACK_TAGS["Accounts Receivable"]` entry (a different, SEC-side ifrs-full-filer
tag-string bug, out of scope). Confirmed `16__fetch_eu_xbrl.py` never reads that dict at all —
zero shared-infrastructure risk, safer than the phase's own "keep old tag + add fallback"
contingency anticipated.

`concept_hierarchy.json` updated with the two new concepts (`Non-Controlling Interests` under
Balance Sheet → Stockholders Equity; `Finance Income` under Income Statement → Non-Operating) —
the six reused concepts were already registered.

25 new/updated tests (`tests/test_sources_eu_current.py`, 60/60 pass in that file; full suite 388
passed, 2 skipped; ruff clean).

## 3. Real EU ingestion (16 → 21)

Ran against the current EU-only scrape (natural `scraped_at` scoping, same mechanism as every
prior phase — no new scoping logic). Real coverage, `financials_raw`:

| Concept | Predicted | Actual | Notes |
|---|---|---|---|
| Accounts Payable | ~7/8 | 7/8 | Exact match |
| Non-Controlling Interests | 8/8 | 8/8 | Exact match |
| Stock-based Compensation | 6/8 | **7/8** | NAI genuinely has real SBC data (€84K FY2023, €1.436M FY2024) — the Phase 6.4 audit's fresh-fetch only sampled each issuer's *latest* single filing; production ingestion walks the full filing history across years, finding NAI's real earlier-year data. Investigated and explained, not forced. |
| Changes in Working Capital | 5/8 | 5/8 | Exact match |
| Income Before Tax | ~7/8 | 7/8 | Exact match |
| EPS Basic / Diluted | 8/8 | 8/8 | Exact match |
| Finance Income | ~5/8 | 5/8 | Exact match |
| Accounts Receivable | ~7/8 | 7/8 | Exact match |

Clean `financials`: zero duplicate canonical keys; all 9 concepts landed under the correct
`stmt` (Balance Sheet: Accounts Payable/Receivable/Non-Controlling Interests; Cash Flow: Changes
in Working Capital/Stock-based Compensation; Income Statement: EPS Basic/Diluted/Finance
Income/Income Before Tax). `Non-Controlling Interests`, `Total Equity (incl NCI)`, and
`Total Stockholders Equity` confirmed as three genuinely distinct, arithmetically-consistent
values for FCC (€1,003,303,000 + €2,732,716,000 = €3,736,019,000 exactly).

**US/Canada regression**: zero rows touched — confirmed directly, every checked ticker's
`scraped_at` predates this session's work (the EU-scoped run never read their raw data at all).
**Phase 6.3 (Net Income statement classification) and Phase 6.5 (Total Equity Option A, including
IBE FY2021's real fallback case) both confirmed intact, unchanged.**

## 4. 22 — full-universe derived metrics

Ran full-universe (no ticker scoping — `22` is structurally full-universe, per established
convention). US/Canada completely unchanged (AAPL 1470, MSFT 1470, TSLA 896, AEM 533, AQN 521,
BN 190 rows — byte-identical). EU gained modest, plausible new rows (e.g. FCC 137→139) from newly
computable ratios (real Accounts Receivable now feeds the existing NCAV/Net-Net Finder haircut
formula — spot-checked FCC's NCAV (Moderate)/(Strict) now populated with real, distinct values).

## 5. 23 — intrinsic value / Owner Earnings

**The critical check, per instruction: Owner Earnings must not become 0.0 merely because an input
is absent, and no new `fillna(0)` behavior was added.** `23__intrinsic_value.py` was not modified
at all. Confirmed real: FCC's FY2024 Owner Earnings (€606,792,000) now correctly **differs** from
Net Income (€429,865,000) by exactly the real `Changes in Working Capital` input
(−€176,927,000, correctly subtracted per the existing formula) — direct, decisive proof Tier A's
new SBC/ΔWC data is flowing into the pre-existing Owner Earnings formula exactly as designed, not
merely defaulting. All 31 EU ticker-year Owner Earnings values checked: zero `0.0` rows. AAPL
unchanged exactly ($123.856B FY2025).

**New derived metrics confirmed real**: `EPS CAGR (5Y) %` (directly enabled by Tier A's EPS
mapping — real values for FCC, FCT, IBE, RAND, SGO) and expanded `Quick Ratio` coverage (FCC,
ALO, IBE, NAI, RAND, SGO).

## 6. Idempotency

Ran `22 → 23` a second time. `financials_metrics` total row count identical before/after
(2,040,347 both times); zero duplicate canonical keys anywhere; FCC's Owner Earnings values
byte-identical run-to-run; Net Income `stmt` classification and Total Equity Option A both
re-confirmed intact after the second cycle.

## 7. 51 / 52 — export and publish, including a real finding fixed mid-phase

First `51`/`52` cycle: both succeeded, GitHub Release confirmed live (`draft: false`). Downloading
and inspecting the real published `dashboard_data.parquet` directly confirmed every Tier A
concept present with the exact same per-ticker coverage as `financials` — **but** `section`/
`group`/`sort_order` were `NULL`/`NaN` for the two genuinely new concepts (`Finance Income`,
`Non-Controlling Interests`), while the six reused concepts (including the other 7 new-to-EU
concepts) had correct hierarchy metadata.

**Root cause, found and fixed within this phase's existing scope**: `51__export_dashboard_data.py`
joins against the Unity Catalog table `main.config.concept_hierarchy` — **not** the git-tracked
`concept_hierarchy.json` directly. That table is rebuilt from the JSON by a separate, pre-existing,
unmodified notebook, `00__config/03__concept_hierarchy_master.py` (its own docstring: *"1. Edit
concept_hierarchy.json 2. Commit + push 3. Re-run this notebook"*) — a step this implementation
had omitted. Ran that existing notebook (unmodified, no new mechanism, exactly the documented
process), confirmed the table now carries correct entries for both new concepts, then re-ran `51`
and `52`. Verified directly against the re-downloaded, re-published artifact: both concepts now
carry correct `section`/`group`/`display_name`/`sort_order` (e.g. `Non-Controlling Interests` →
section `Liabilities & Equity`, group `Stockholders Equity`, display name `NCI`, sort_order 170).

Final release: `draft: false`, `published_at: 2026-08-17T20:00:47Z`.

## 8. Live website validation

**Layer where the data currently stops: the website's own cron-driven cache refresh — external,
already documented in Phase 6.3, unchanged.** Live-checked FCC's real page: still serving the
same pre-Phase-6.3 cached snapshot (€567.6M Net Income under Cash Flow) confirmed multiple times
earlier this session — none of the Tier A concepts are visible yet, for the identical reason.
**Both layers upstream of the website are confirmed correct**: the data exists correctly in
`financials` (§3) and in the published `dashboard_data` artifact (§7) — this is not a pipeline or
export defect, and per instruction, not something this phase modifies or works around. No
`fundamentals_screener`-version bump was made this phase (no code change to that package), so
there is no downstream bump-PR/CI-approval gate applicable here — the only remaining step is the
site's own scheduled data-refresh cycle.

## 9. Currency regression

No currency-handling code (`fx.py`, `market_cap_asof`, or any conversion logic) was touched by
this phase — none of the 8 Tier A concepts involve price or market-cap data at all. Confirmed by
code scope alone (nothing in `01__tickers.py`'s Tier A additions or `EU_CANONICAL_MAPPING`
touches currency), consistent with the live EUR/CAD/USD behavior already re-confirmed multiple
times earlier this session (Phase 6.3's own live validation).

## 10. Remaining limitations

- Live website reflects neither this phase's nor Phase 6.3's changes yet — pending the site's own
  cron cycle, outside this session's reach.
- `Finance Income`'s us-gaap tag (`InvestmentIncomeInterest`) is not independently re-verified
  against live SEC company-facts data — flagged in code comments, worth a future confirming pass
  if real US coverage looks off once a normal SEC ingestion run picks it up.
- Tier B/C concepts remain unimplemented, per instruction — not started.
