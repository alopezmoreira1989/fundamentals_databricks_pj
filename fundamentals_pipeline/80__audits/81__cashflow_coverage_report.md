# Cash Flow XBRL concept coverage audit — Phase 0 (read-only)

_Live table: `main.financials.financials` · FY rows (`period_type='FY'`) · max FY = 2026 · recent window = 2024..2026._

> **Read this first.** Fill-rate alone does NOT prove a mapping bug. Many CF lines are legitimately absent for a given filer (a non-payer has no Dividends Paid; a company with no securities book has no Purchases of Investments). The signal that a concept is *mis-mapped* (not just inapplicable) is the **recovery-count on the NULL set** — how many NULL tickers carry annual data under an alternative tag. Decisions below are driven by recovery, not by raw fill-rate.

## 0.1 — Current Cash Flow mapping inventory

All Cash Flow concepts today are **single-tag** (no fallback lists in CF). The only synonym/priority entry is `Operating Cash Flow (cont ops)` → `Operating Cash Flow`.

| Display name | Current tag | kind | synonym/priority today |
|---|---|---|---|
| Net Income | `NetIncomeLoss` | flow_additive | — |
| Net Income (to common) | `NetIncomeLossAvailableToCommonStockholdersBasic` | flow_additive | → Net Income (prio 1) |
| Net Income (incl NCI) | `ProfitLoss` | flow_additive | → Net Income (prio 2) |
| Depreciation & Amortization | `DepreciationDepletionAndAmortization` | flow_additive | — |
| Stock-based Compensation | `ShareBasedCompensation` | flow_additive | — |
| Changes in Working Capital | `IncreaseDecreaseInOperatingCapital` | flow_additive | — |
| Operating Cash Flow | `NetCashProvidedByUsedInOperatingActivities` | flow_additive | canonical |
| Operating Cash Flow (cont ops) | `...OperatingActivitiesContinuingOperations` | flow_additive | → Operating Cash Flow (prio 1) |
| CapEx | `PaymentsToAcquirePropertyPlantAndEquipment` | flow_additive | — |
| Acquisitions | `PaymentsToAcquireBusinessesNetOfCashAcquired` | flow_additive | — |
| Purchases of Investments | `PaymentsToAcquireInvestments` | flow_additive | — |
| Sales of Investments | `ProceedsFromSaleOfInvestments` | flow_additive | — |
| Investing Cash Flow | `NetCashProvidedByUsedInInvestingActivities` | flow_additive | — |
| Debt Issuance | `ProceedsFromIssuanceOfLongTermDebt` | flow_additive | — |
| Debt Repayment | `RepaymentsOfLongTermDebt` | flow_additive | — |
| Dividends Paid | `PaymentsOfDividends` | flow_additive | — |
| Share Repurchases | `PaymentsForRepurchaseOfCommonStock` | flow_additive | — |
| Financing Cash Flow | `NetCashProvidedByUsedInFinancingActivities` | flow_additive | — |
| Net Change in Cash | `CashCashEquivalents…PeriodIncreaseDecreaseIncludingExchangeRateEffect` | flow_additive | — |

## 0.2 — Fill-rate per concept (worst recent first)

**Denominator anchor:** distinct `(ticker, fiscal_year)` pairs with *any* non-null Cash Flow FY concept (overall = 30,288 pairs; recent = 5,035 pairs). Because Net Income is mirrored into the Cash Flow section, this anchor ≈ "the filer reported financials that year" — the broadest honest denominator. Concepts with recent fill < 90% are flagged.

| Concept | overall fill | recent fill | flag |
|---|---:|---:|:--:|
| Sales of Investments | 0.0% (0/30,288) | 0.0% (0/5,035) | 🚩 |
| Changes in Working Capital | 2.3% (707/30,288) | 1.5% (76/5,035) | 🚩 |
| Purchases of Investments | 10.4% (3,163/30,288) | 11.3% (568/5,035) | 🚩 |
| Dividends Paid | 22.6% (6,853/30,288) | 21.4% (1,076/5,035) | 🚩 |
| Debt Issuance | 31.2% (9,435/30,288) | 25.1% (1,264/5,035) | 🚩 |
| Debt Repayment | 35.4% (10,731/30,288) | 29.9% (1,504/5,035) | 🚩 |
| Acquisitions | 44.5% (13,467/30,288) | 42.5% (2,140/5,035) | 🚩 |
| Depreciation & Amortization | 57.9% (17,547/30,288) | 61.6% (3,104/5,035) | 🚩 |
| Share Repurchases | 60.7% (18,380/30,288) | 63.3% (3,189/5,035) | 🚩 |
| CapEx | 73.0% (22,117/30,288) | 72.7% (3,658/5,035) | 🚩 |
| Stock-based Compensation | 85.0% (25,750/30,288) | 88.4% (4,451/5,035) | 🚩 |
| Net Change in Cash | 48.1% (14,579/30,288) | 88.9% (4,476/5,035) | 🚩 |
| Investing Cash Flow | 87.1% (26,392/30,288) | 98.4% (4,952/5,035) |  |
| Financing Cash Flow | 87.4% (26,480/30,288) | 98.9% (4,981/5,035) |  |
| Operating Cash Flow | 99.7% (30,202/30,288) | 99.9% (5,029/5,035) |  |
| Net Income | 99.8% (30,241/30,288) | 100.0% (5,035/5,035) |  |

Note: **Sales of Investments** returns **0 rows** — `ProceedsFromSaleOfInvestments` essentially never carries data → a pure mis-map, the strongest M1 signal. **Investing/Financing Cash Flow** recent fill is high (cont-ops gap is niche), but their all-time fill (~87%) and the RCAT trigger justify the M2 fix.

## 0.3 — Tag discovery on the NULL set (SEC company-facts)

For each flagged concept, up to 25 NULL-set tickers (recent window) were probed against the curated candidate tags. `recovered` = sampled NULL tickers whose annual 10-K carries data under that tag. **coexist** = tickers where ≥2 candidate tags both have annual data the same way (coalesce-vs-sum check).

_Fetch pool: 160 unique tickers (cap 160; 88 sampled tickers beyond the cap were not fetched — not silently dropped). 0 fetch errors._

### CapEx  ·  _M1_

Sampled NULL tickers fetched: **18**. Recovered by a non-current fallback tag: **10**. Coexisting (≥2 candidates same year): **4**.

| candidate tag (0 = current) | recovers |
|---|---:|
| `PaymentsToAcquirePropertyPlantAndEquipment` *(current)* | 5/18 |
| `PaymentsToAcquireProductiveAssets` | 9/18 |
| `PaymentsToAcquireMachineryAndEquipment` | 0/18 |
| `PaymentsForCapitalImprovements` | 3/18 |
| `PaymentsToAcquireOtherPropertyPlantAndEquipment` | 2/18 |

_Uncurated tags seen in this NULL set: `PaymentsToAcquireOilAndGasPropertyAndEquipment` (2), `PaymentsToAcquireOilAndGasProperty` (1)_

### Acquisitions  ·  _M1_

Sampled NULL tickers fetched: **19**. Recovered by a non-current fallback tag: **2**. Coexisting (≥2 candidates same year): **3**.

| candidate tag (0 = current) | recovers |
|---|---:|
| `PaymentsToAcquireBusinessesNetOfCashAcquired` *(current)* | 10/19 |
| `PaymentsToAcquireBusinessesGross` | 5/19 |
| `PaymentsToAcquireBusinessesAndInterestInAffiliates` | 0/19 |

### Purchases of Investments  ·  _M1_

Sampled NULL tickers fetched: **17**. Recovered by a non-current fallback tag: **10**. Coexisting (≥2 candidates same year): **6**.

| candidate tag (0 = current) | recovers |
|---|---:|
| `PaymentsToAcquireInvestments` *(current)* | 1/17 |
| `PaymentsToAcquireMarketableSecurities` | 6/17 |
| `PaymentsToAcquireAvailableForSaleSecuritiesDebt` | 5/17 |
| `PaymentsToAcquireShortTermInvestments` | 3/17 |
| `PaymentsToAcquireHeldToMaturitySecurities` | 4/17 |

_Uncurated tags seen in this NULL set: `PaymentsToAcquireAvailableForSaleSecurities` (4), `PaymentsToAcquireAvailableForSaleSecuritiesEquity` (2)_

### Sales of Investments  ·  _M1_

Sampled NULL tickers fetched: **18**. Recovered by a non-current fallback tag: **12**. Coexisting (≥2 candidates same year): **3**.

| candidate tag (0 = current) | recovers |
|---|---:|
| `ProceedsFromSaleOfInvestments` *(current)* | 0/18 |
| `ProceedsFromSaleMaturityAndCollectionsOfInvestments` | 4/18 |
| `ProceedsFromSaleAndMaturityOfMarketableSecurities` | 4/18 |
| `ProceedsFromSaleOfAvailableForSaleSecuritiesDebt` | 6/18 |
| `ProceedsFromSaleOfShortTermInvestments` | 2/18 |

_Uncurated tags seen in this NULL set: `ProceedsFromSaleOfPropertyPlantAndEquipment` (7), `ProceedsFromSaleOfAvailableForSaleSecurities` (7), `ProceedsFromMaturitiesPrepaymentsAndCallsOfAvailableForSaleSecurities` (5), `ProceedsFromSaleOfProductiveAssets` (4), `ProceedsFromMaturitiesPrepaymentsAndCallsOfHeldToMaturitySecurities` (3), `ProceedsFromSaleAndMaturityOfAvailableForSaleSecurities` (3)_

### Debt Issuance  ·  _M1_

Sampled NULL tickers fetched: **16**. Recovered by a non-current fallback tag: **2**. Coexisting (≥2 candidates same year): **3**.

| candidate tag (0 = current) | recovers |
|---|---:|
| `ProceedsFromIssuanceOfLongTermDebt` *(current)* | 10/16 |
| `ProceedsFromIssuanceOfDebt` | 2/16 |
| `ProceedsFromLongTermLinesOfCredit` | 0/16 |
| `ProceedsFromConvertibleDebt` | 2/16 |
| `ProceedsFromNotesPayable` | 1/16 |

_Uncurated tags seen in this NULL set: `ProceedsFromLinesOfCredit` (6), `ProceedsFromIssuanceOfSecuredDebt` (4), `ProceedsFromIssuanceOfOtherLongTermDebt` (3), `ProceedsFromRepaymentsOfLinesOfCredit` (3), `ProceedsFromIssuanceOfUnsecuredDebt` (3), `ProceedsFromSaleOfAvailableForSaleSecuritiesDebt` (3)_

### Debt Repayment  ·  _M1_

Sampled NULL tickers fetched: **16**. Recovered by a non-current fallback tag: **8**. Coexisting (≥2 candidates same year): **5**.

| candidate tag (0 = current) | recovers |
|---|---:|
| `RepaymentsOfLongTermDebt` *(current)* | 4/16 |
| `RepaymentsOfDebt` | 1/16 |
| `RepaymentsOfLongTermLinesOfCredit` | 4/16 |
| `RepaymentsOfConvertibleDebt` | 4/16 |
| `RepaymentsOfNotesPayable` | 5/16 |

_Uncurated tags seen in this NULL set: `RepaymentsOfSecuredDebt` (4), `RepaymentsOfLinesOfCredit` (4), `RepaymentsOfOtherLongTermDebt` (3), `RepaymentsOfMediumTermNotes` (2), `RepaymentsOfLongTermCapitalLeaseObligations` (2), `RepaymentsOfShortTermDebt` (2)_

### Dividends Paid  ·  _M1_

Sampled NULL tickers fetched: **17**. Recovered by a non-current fallback tag: **5**. Coexisting (≥2 candidates same year): **0**.

| candidate tag (0 = current) | recovers |
|---|---:|
| `PaymentsOfDividends` *(current)* | 2/17 |
| `PaymentsOfDividendsCommonStock` | 5/17 |

_Uncurated tags seen in this NULL set: `PaymentsOfDividendsPreferredStockAndPreferenceStock` (6), `PaymentsOfDividendsMinorityInterest` (3)_

### Share Repurchases  ·  _M1_

Sampled NULL tickers fetched: **17**. Recovered by a non-current fallback tag: **1**. Coexisting (≥2 candidates same year): **1**.

| candidate tag (0 = current) | recovers |
|---|---:|
| `PaymentsForRepurchaseOfCommonStock` *(current)* | 11/17 |
| `PaymentsForRepurchaseOfEquity` | 2/17 |

_Uncurated tags seen in this NULL set: `PaymentsForRepurchaseOfPreferredStockAndPreferenceStock` (2), `PaymentsForRepurchaseOfRedeemablePreferredStock` (2), `PaymentsForRepurchaseOfOtherEquity` (1), `PaymentsForRepurchaseOfRedeemableNoncontrollingInterest` (1)_

### Net Change in Cash  ·  _M1_

Sampled NULL tickers fetched: **16**. Recovered by a non-current fallback tag: **11**. Coexisting (≥2 candidates same year): **14**.

| candidate tag (0 = current) | recovers |
|---|---:|
| `CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect` *(current)* | 5/16 |
| `CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseExcludingExchangeRateEffect` | 15/16 |
| `CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecrease` | 0/16 |
| `CashAndCashEquivalentsPeriodIncreaseDecrease` | 14/16 |

_Uncurated tags seen in this NULL set: `CashAndCashEquivalentsPeriodIncreaseDecreaseExcludingExchangeRateEffect` (2), `CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffectContinuingOperations` (1)_

### Depreciation & Amortization  ·  _OUT_OF_SCOPE (component-sum)_

Sampled NULL tickers fetched: **17**. Recovered by a non-current fallback tag: **11**. Coexisting (≥2 candidates same year): **14**.

| candidate tag (0 = current) | recovers |
|---|---:|
| `DepreciationDepletionAndAmortization` *(current)* | 5/17 |
| `DepreciationAmortizationAndAccretionNet` | 3/17 |
| `DepreciationAndAmortization` | 9/17 |
| `Depreciation` | 11/17 |
| `AmortizationOfIntangibleAssets` | 13/17 |

_Uncurated tags seen in this NULL set: `AmortizationOfFinancingCosts` (6), `AmortizationOfFinancingCostsAndDiscounts` (4), `AmortizationOfDebtDiscountPremium` (3), `DepreciationAndAmortizationDiscontinuedOperations` (2), `AmortizationOfAboveAndBelowMarketLeases` (1), `AmortizationOfLeasedAsset` (1)_

## 0.4 — Recommended fix per concept (proposal — NOT applied)

**M1 = fallback list** (`extract_series_multi`, coalesce per period, never sums). **M2 = synonym + CONCEPT_PRIORITY** (collapse at merge, like Operating Cash Flow). **Out of scope = component-sum** (needs summation, deferred).

### M1 — fallback list (proposed priority-ordered tags, data-driven)

- **CapEx** — proposed list: ['PaymentsToAcquirePropertyPlantAndEquipment', 'PaymentsToAcquireProductiveAssets', 'PaymentsForCapitalImprovements', 'PaymentsToAcquireOtherPropertyPlantAndEquipment']
  - recovered by fallback: 10/18; ⚠ coexistence observed — confirm these are total-vs-component (coalesce OK) not two summable lines.
- **Acquisitions** — proposed list: ['PaymentsToAcquireBusinessesNetOfCashAcquired', 'PaymentsToAcquireBusinessesGross']
  - recovered by fallback: 2/19; ⚠ coexistence observed — confirm these are total-vs-component (coalesce OK) not two summable lines.
- **Purchases of Investments** — proposed list: ['PaymentsToAcquireInvestments', 'PaymentsToAcquireMarketableSecurities', 'PaymentsToAcquireAvailableForSaleSecuritiesDebt', 'PaymentsToAcquireHeldToMaturitySecurities', 'PaymentsToAcquireShortTermInvestments']
  - recovered by fallback: 10/17; ⚠ coexistence observed — confirm these are total-vs-component (coalesce OK) not two summable lines.
- **Sales of Investments** — proposed list: ['ProceedsFromSaleOfInvestments', 'ProceedsFromSaleOfAvailableForSaleSecuritiesDebt', 'ProceedsFromSaleMaturityAndCollectionsOfInvestments', 'ProceedsFromSaleAndMaturityOfMarketableSecurities', 'ProceedsFromSaleOfShortTermInvestments']
  - recovered by fallback: 12/18; ⚠ coexistence observed — confirm these are total-vs-component (coalesce OK) not two summable lines.
- **Debt Issuance** — proposed list: ['ProceedsFromIssuanceOfLongTermDebt', 'ProceedsFromIssuanceOfDebt', 'ProceedsFromConvertibleDebt', 'ProceedsFromNotesPayable']
  - recovered by fallback: 2/16; ⚠ coexistence observed — confirm these are total-vs-component (coalesce OK) not two summable lines.
- **Debt Repayment** — proposed list: ['RepaymentsOfLongTermDebt', 'RepaymentsOfNotesPayable', 'RepaymentsOfLongTermLinesOfCredit', 'RepaymentsOfConvertibleDebt', 'RepaymentsOfDebt']
  - recovered by fallback: 8/16; ⚠ coexistence observed — confirm these are total-vs-component (coalesce OK) not two summable lines.
- **Dividends Paid** — proposed list: ['PaymentsOfDividends', 'PaymentsOfDividendsCommonStock']
  - recovered by fallback: 5/17; no coexistence in sample → coalesce safe.
- **Share Repurchases** — proposed list: ['PaymentsForRepurchaseOfCommonStock', 'PaymentsForRepurchaseOfEquity']
  - recovered by fallback: 1/17; ⚠ coexistence observed — confirm these are total-vs-component (coalesce OK) not two summable lines.
- **Net Change in Cash** — proposed list: ['CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect', 'CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseExcludingExchangeRateEffect', 'CashAndCashEquivalentsPeriodIncreaseDecrease']
  - recovered by fallback: 11/16; ⚠ coexistence observed — confirm these are total-vs-component (coalesce OK) not two summable lines.

### M2 — synonym + CONCEPT_PRIORITY (continuing-operations subtotals)

- **Investing Cash Flow** — add `Investing Cash Flow (cont ops)` → `NetCashProvidedByUsedInInvestingActivitiesContinuingOperations`, synonym → `Investing Cash Flow`, priority base=0 / cont-ops=1.
- **Financing Cash Flow** — add `Financing Cash Flow (cont ops)` → `NetCashProvidedByUsedInFinancingActivitiesContinuingOperations`, synonym → `Financing Cash Flow`, priority base=0 / cont-ops=1.
- Mirrors `Operating Cash Flow (cont ops)` exactly. Recovers discontinued-ops filers (RCAT) without changing the canonical stored in the table → no dashboard/layout change.

## 0.5 — Out of scope this pass (report only)

- **Depreciation & Amortization** — recent fill 61.6% (3,104/5,035). 
- **Changes in Working Capital** — recent fill 1.5% (76/5,035). 
  - D&A aggregate-variant recovery on its NULL set (17 sampled): `DepreciationDepletionAndAmortization`→5, `DepreciationAmortizationAndAccretionNet`→3, `DepreciationAndAmortization`→9, `Depreciation`→11, `AmortizationOfIntangibleAssets`→13. Aggregate variants (`DepreciationAndAmortization`, `DepreciationAmortizationAndAccretionNet`) are M1-coverable; the true `Depreciation` + `AmortizationOfIntangibleAssets` **split** needs summation, not coalesce — separate decision.
  - **Changes in Working Capital** — `IncreaseDecreaseInOperatingCapital` is rarely tagged; the real data lives in component lines (AR, inventory, AP …). Pure structural gap, summation only. Defer.

## Trigger case — RCAT (Red Cat Holdings)

RCAT annual us-gaap tags present in the relevant sections (from company-facts):

```
AmortizationOfDebtDiscountPremium
AmortizationOfIntangibleAssets
CashAndCashEquivalentsPeriodIncreaseDecrease
CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect
CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffectDisposalGroupIncludingDiscontinuedOperations
Depreciation
DepreciationAndAmortization
DepreciationDepletionAndAmortization
NetCashProvidedByUsedInDiscontinuedOperations
NetCashProvidedByUsedInFinancingActivities
NetCashProvidedByUsedInFinancingActivitiesContinuingOperations
NetCashProvidedByUsedInInvestingActivities
NetCashProvidedByUsedInInvestingActivitiesContinuingOperations
NetCashProvidedByUsedInOperatingActivities
NetCashProvidedByUsedInOperatingActivitiesContinuingOperations
PaymentsForOtherTaxes
PaymentsOfDebtExtinguishmentCosts
PaymentsOfDebtIssuanceCosts
PaymentsOfStockIssuanceCosts
PaymentsToAcquireAssetsInvestingActivities
PaymentsToAcquireBusinessesNetOfCashAcquired
PaymentsToAcquireInvestments
PaymentsToAcquireOtherInvestments
PaymentsToAcquireProductiveAssets
PaymentsToAcquirePropertyPlantAndEquipment
ProceedsFromCollectionOfNotesReceivable
ProceedsFromContributedCapital
ProceedsFromContributionsFromParent
ProceedsFromConvertibleDebt
ProceedsFromDivestitureOfBusinesses
ProceedsFromDivestitureOfBusinessesAndInterestsInAffiliates
ProceedsFromDivestitureOfInterestInSubsidiariesAndAffiliates
ProceedsFromIssuanceInitialPublicOffering
ProceedsFromIssuanceOfCommonStock
ProceedsFromIssuanceOfDebt
ProceedsFromIssuanceOfLongTermDebt
ProceedsFromIssuanceOfPreferredStockAndPreferenceStock
ProceedsFromIssuanceOrSaleOfEquity
ProceedsFromNotesPayable
ProceedsFromOtherDebt
ProceedsFromOtherDeposits
ProceedsFromOtherShortTermDebt
ProceedsFromRelatedPartyDebt
ProceedsFromSaleAndMaturityOfMarketableSecurities
ProceedsFromSaleOfEquityMethodInvestments
ProceedsFromSaleOfOtherInvestments
ProceedsFromSaleOfPropertyPlantAndEquipment
ProceedsFromShortTermDebt
ProceedsFromStockOptionsExercised
ProceedsFromWarrantExercises
RepaymentsOfDebt
RepaymentsOfNotesPayable
RepaymentsOfRelatedPartyDebt
```

Confirms the disease: RCAT tags its capex/investing/financing lines under variants our single-tag map misses, plus the `…ContinuingOperations` subtotals.

---

## ⚠ RCAT reality-check (live table state) — added after the SEC probe

Inspecting RCAT's actual `financials` rows changes how Phase 1 validation should read.
RCAT is **not a clean single-cause case** — it stacks three diseases:

| FY (period_end) | what's present | what's missing | cause |
|---|---|---|---|
| 2025 (2025-12-31) | D&A, Net Income, OCF | Investing CF, Financing CF, **CapEx**, Net Change | **M2 cont-ops** (tags `…ContinuingOperations`) |
| 2024 (2024-04-30) | Net Change, Net Income, OCF | Investing CF, Financing CF, **CapEx** | **M2 cont-ops** |
| 2023 / 2022 (04-30) | Investing/Financing CF, OCF | **CapEx** | M1 tag gap (capex under `ProductiveAssets`) and/or FYE-change period drop |

Plus a **fy-assignment artifact**: `Purchases of Investments = 57,619,881` appears under
**both** `fiscal_year=2022` (period_end 2022-04-30) **and** `fiscal_year=2023` (period_end
**2022**-04-30) — a 2022 value mislabeled into FY2023. RCAT changed its fiscal year-end
(Apr-30 → Dec-31 by FY2025), so transition periods fall outside the 350–380d FY window and
get dropped or mis-binned by `classify_period_shape` / the `21` fy-assignment.

**Implication for Phase 1:**
- The **M2 fix directly recovers RCAT's FY2024/FY2025 Investing CF + Financing CF** (high confidence).
- The **M1 CapEx fallback** (`…ProductiveAssets`, etc.) will help RCAT's CapEx **only for the
  years whose period is a clean ~365d FY**; the FYE-change transition years may still come back
  NULL because the root cause there is fy-assignment, **not** tag mapping. The prompt's target
  CapEx values (259,139 / 2,450,213) are not currently extracted at all — confirm in Phase 1
  whether the M1 fallback alone surfaces them, and if not, treat RCAT's CapEx as a **separate
  fy-assignment investigation**, not evidence the mapping fix failed.
- **Recommendation:** validate the mapping fix on a *clean* recovered ticker (stable FYE, no
  discontinued ops) in addition to RCAT, so the tag fix is judged on a case it can actually cure.
  RCAT stays as the M2 cont-ops witness.

## Coalesce-vs-sum caveats the curated-tag `coexist` counter under-reports

The `coexist` column only checks the **curated** candidate tags against each other. Two real
summation risks live in the *uncurated* column and must be confirmed before Phase 1:

- **Dividends Paid** — `coexist=0` among curated tags, but the NULL set shows
  `PaymentsOfDividendsPreferredStockAndPreferenceStock` (6) and `…MinorityInterest` (3). If a
  filer reports common + preferred dividends as **separate** lines with no aggregate
  `PaymentsOfDividends`, a coalesce fallback that prefers common-only **undercounts** total
  dividends → Payout/FCF and Dividend Coverage understated. Recommend: keep the aggregate
  `PaymentsOfDividends` as priority 0 (coalesce only fills it where the aggregate is absent), and
  flag the common+preferred split as a **summation candidate** deferred like D&A.
- **Debt Issuance** — uncurated `ProceedsFromLinesOfCredit` (6) is a revolver draw, arguably a
  distinct line from term-debt issuance; coalescing it into "Debt Issuance" mixes revolver and
  term proceeds. Decide intent before adding it.
- **Net Change in Cash** — `coexist=14/16` but this is **variant-not-summable**: `Including`
  vs `Excluding`-exchange-rate-effect are two versions of the *same* total. Priority
  `Including` > `Excluding` > pre-2018 is correct; coalesce is safe here (do **not** sum).
