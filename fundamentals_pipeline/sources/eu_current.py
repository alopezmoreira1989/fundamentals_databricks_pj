"""fundamentals_pipeline.sources.eu_current — filings.xbrl.org (ESEF/IFRS) adapter logic.

Pure Python, no Spark/`dbutils`/network dependency, matching `sources/base.py`'s own
constraint. Real HTTP calls (the actual `EUCurrentSource` class implementing
`FundamentalsSource`) live in `10__ingestion/16__fetch_eu_xbrl.py`; everything in this module is
the fixture-testable logic that class delegates to: amendment/filing selection, consolidated-
vs-segment discrimination, current-vs-comparative period selection, and the narrow IFRS ->
canonical concept mapping for this pilot's five high-confidence concepts.

Every design decision here is grounded in real `filings.xbrl.org` API/xBRL-JSON responses
fetched live during Phase 5.1 research (2026-08), not assumed — see
`docs/phase5-1-eu-adapter.md` for the evidence each rule is based on.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Literal

from .base import SourceEntity, SourceFact, SourceFiling
from .mapping import MappingDecision, MappingStatus, MappingType

EU_SOURCE_ID = "EU_CURRENT"

_BASE_DIMENSION_KEYS = frozenset({"concept", "entity", "period", "unit"})

FilingRejectionReason = Literal["superseded", "missing_json_url", "has_errors"]


@dataclass(frozen=True)
class FilingRejection:
    """A filing that was considered for one (entity, period_end) group but not selected -- and
    why. `"superseded"` is a normal, expected amendment outcome. `"missing_json_url"`/
    `"has_errors"` mean the filing is genuinely not ingestible -- the caller must surface these
    as real `ingestion_failures` rows, never let them just vanish during selection."""

    fxo_id: str
    reason: FilingRejectionReason


def _filing_is_ingestible(filing: Mapping) -> bool:
    return filing.get("error_count", 0) == 0 and bool(filing.get("json_url"))


def select_filing_for_period(
    filings: Sequence[Mapping],
) -> tuple[dict | None, list[FilingRejection]]:
    """Pick the single filing that represents one (entity, period_end) group.

    All `filings` must already share the same `period_end` -- the caller groups by period_end
    before calling this. Among ingestible candidates (`error_count == 0` and a real `json_url`),
    the one with the latest `processed` timestamp wins; every other ingestible candidate is
    `"superseded"` (confirmed live against two real amendment cases: New Amsterdam Invest's
    `...-NL-0`/`...-NL-1` and Fincantieri's `...-IT-0`/`...-IT-1`, both period_end pairs where
    the later `processed` timestamp is the correct winner). A non-ingestible filing is always
    reported, never silently dropped: if it is the ONLY filing for this period_end, the winner
    is `None` and the caller must treat that as a real failure, not a quietly-missing fiscal
    year (confirmed live: Fincantieri's real FY2025 filing has `error_count=0` but
    `json_url=None`).
    """
    if not filings:
        return None, []

    rejections: list[FilingRejection] = []
    ingestible = []
    for f in filings:
        if _filing_is_ingestible(f):
            ingestible.append(f)
        else:
            reason: FilingRejectionReason = (
                "has_errors" if f.get("error_count", 0) != 0 else "missing_json_url"
            )
            rejections.append(FilingRejection(fxo_id=f["fxo_id"], reason=reason))

    if not ingestible:
        return None, rejections

    winner = max(ingestible, key=lambda f: f["processed"])
    rejections.extend(
        FilingRejection(fxo_id=f["fxo_id"], reason="superseded")
        for f in ingestible
        if f is not winner
    )
    return winner, rejections


def is_consolidated_fact(dimensions: Mapping[str, str]) -> bool:
    """True iff this fact's dimensions are EXACTLY `{concept, entity, period, unit}` -- no extra
    axis/member key. A fact with any additional dimension key is a segment/component/equity
    breakdown, not the consolidated top-line figure, and must be excluded from canonical mapping
    (NULL > a questionable value silently picked from the wrong breakdown). Confirmed live
    against a real dimensional fact: New Amsterdam Invest's `ifrs-full:ComponentsOfEquityAxis`
    breakdown of `ifrs-full:Equity`."""
    keys = set(dimensions.keys())
    return "concept" in keys and keys <= _BASE_DIMENSION_KEYS


def parse_unit_currency(unit: str) -> str | None:
    """`"iso4217:EUR"` -> `"EUR"`. `None` for anything not a BARE iso4217-prefixed currency
    code -- a composite unit like `"iso4217:EUR/xbrli:shares"` (a per-share metric) is not a
    plain currency and also returns `None`, not the garbled remainder."""
    if not unit or not unit.startswith("iso4217:"):
        return None
    code = unit.split(":", 1)[1]
    if not code or "/" in code:
        return None
    return code


def _period_end_component(period: str) -> str:
    """The end-of-period ISO datetime string, for both instant (`"...T00:00:00"`) and duration
    (`"start/end"`) period representations."""
    return period.split("/", 1)[-1]


def is_current_period_fact(period: str, filing_period_end: date) -> bool:
    """True iff this fact's period belongs to the filing's OWN reporting period, not a
    prior-year comparative published in the same file.

    xBRL's instant convention is exclusive-end: a balance-sheet snapshot "as of" the filing's
    `period_end` is tagged with `period = period_end + 1 day`. Confirmed live: FCC's FY2024
    filing (`period_end=2024-12-31`) tags its Assets/Cash instant facts with
    `period="2025-01-01T00:00:00"`, exactly `period_end + 1 day`. This holds for both instant
    (stock) and duration (flow) facts' END component and needs no fiscal-year-length assumption,
    so it works unchanged for a non-calendar fiscal year (e.g. Alstom's `period_end=2025-03-31`).
    Also confirmed live to correctly reject a same-file prior-year comparative: FCC's FY2024
    filing carries BOTH the current year's `ifrs-full:Revenue` fact (period ending
    `2025-01-01T00:00:00`) and the FY2023 comparative (period ending `2024-01-01T00:00:00`)
    under the identical concept -- only the former passes this check.
    """
    expected_end = (filing_period_end + timedelta(days=1)).isoformat()
    return _period_end_component(period).startswith(expected_end)


def _parse_period_start(period: str) -> str | None:
    """The ISO date (`YYYY-MM-DD`) a duration fact's period starts on, or `None` for an instant
    fact (no `"/"` separator)."""
    if "/" not in period:
        return None
    return period.split("/", 1)[0][:10]


def extract_source_facts(
    raw_json: Mapping,
    filing: SourceFiling,
    source_id: str,
) -> list[SourceFact]:
    """Convert one filing's raw xBRL-JSON facts into `SourceFact` objects -- filtered to
    consolidated, current-reporting-period facts only (see `is_consolidated_fact`/
    `is_current_period_fact`). `filing.source_entity_id` is the LEI; xBRL-JSON's own
    `entity` dimension is always `"scheme:<LEI>"` (confirmed live against every pilot filing —
    `documentInfo.namespaces.scheme` is the ISO 17442 LEI scheme URI) -- a fact whose `entity`
    doesn't match is excluded as a cheap sanity check, not evidence of anything more elaborate.

    Every returned fact's `source_currency` is populated (never silently dropped) even though
    the current `financials_raw`/`financials` schema has nowhere to persist it per-row -- see
    `docs/phase5-1-eu-adapter.md` for why that Delta-write-time drop is a documented, temporary
    limitation, not a design position that currency doesn't matter.
    """
    if filing.period_end is None:
        return []
    filing_period_end = date.fromisoformat(filing.period_end)
    expected_entity = f"scheme:{filing.source_entity_id}"

    out: list[SourceFact] = []
    for fact in raw_json.get("facts", {}).values():
        dims = fact.get("dimensions", {})
        if not is_consolidated_fact(dims):
            continue
        if dims.get("entity") != expected_entity:
            continue
        period = dims.get("period", "")
        if not period or not is_current_period_fact(period, filing_period_end):
            continue
        value = fact.get("value")
        if value is None:
            continue
        try:
            source_value = float(value)
        except (TypeError, ValueError):
            continue
        out.append(
            SourceFact(
                source_id=source_id,
                source_entity_id=filing.source_entity_id,
                source_filing_id=filing.source_filing_id,
                source_concept=dims.get("concept", ""),
                source_period_start=_parse_period_start(period),
                source_period_end=_period_end_component(period)[:10],
                source_currency=parse_unit_currency(dims.get("unit", "")),
                source_value=source_value,
            )
        )
    return out


def entity_from_pilot(source_id: str, ticker: str, lei: str, name: str) -> SourceEntity:
    """Build the `SourceEntity` for one pilot issuer from its already-verified identity --
    no fuzzy matching, no ticker-derived lookup (per ADR-0009/ADR-0010: `filings.xbrl.org`'s own
    `/api/entities` provides no ticker/exchange field, so a pilot's ticker/LEI pairing is a
    verified, curated fact, not something this adapter derives at runtime)."""
    from ..identity import make_issuer_id

    return SourceEntity(
        source_id=source_id,
        source_entity_id=lei,
        issuer_id=make_issuer_id(source_id, lei),
        name=name,
        ticker=ticker,
    )


# ── Canonical mapping ─────────────────────────────────────────────────────────────────────
# Phase 5.1 pilot: five high-confidence IFRS concepts. Phase 6.1 (docs/phase6-european-esef-
# financial-coverage.md): widened to reuse concepts already accepted for real `ifrs-full`
# filers elsewhere in this codebase (00__config/01__tickers.py's IFRS_FALLBACK_TAGS, verified
# 2026-07 against Toyota/Vale/Infosys companyfacts) -- Phase 6.0's research re-fetched real
# xBRL-JSON from filings.xbrl.org for all 8 admitted issuers and cross-checked every
# IFRS_FALLBACK_TAGS entry against it (605 real distinct consolidated, current-period concepts
# found total). Every entry below whose canonical_concept already has an IFRS_FALLBACK_TAGS
# row is a verified, direct reuse of that SAME accepted tag string -- not an independently
# invented mapping. These ifrs-full tag strings duplicate IFRS_FALLBACK_TAGS's own values --
# that file is a notebook-only `%run` global, unreachable from this importable, non-Spark
# package, so the tag strings are deliberately re-declared here (per Phase 5.1's own plan), not
# an independent taxonomy invented in parallel; 01__tickers.py remains the single source of
# truth for the SEC/Canada side.
#
# A canonical concept may now have MORE THAN ONE accepted source tag (see "Revenue" below) --
# real evidence shows the two Revenue variants are mutually exclusive per issuer within a single
# filing (no issuer tags both), so this is a plain either/or lookup, not a coalesce-with-
# priority mechanism like the SEC side's `extract_series_multi`/`CONCEPT_PRIORITY` -- deliberately
# not building that machinery here since no real EU filing has shown a need for it yet.
EU_CANONICAL_MAPPING: dict[str, tuple[MappingDecision, ...]] = {
    "Revenue": (
        MappingDecision(
            canonical_concept="Revenue",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.DIRECT,
            source_concept="ifrs-full:Revenue",
            notes="Verified live 2026-08 against FCC's real FY2024 xBRL-JSON filing (fact-105, "
            "value 9,071,416,000 EUR, consolidated, non-dimensional). Matches 01__tickers.py "
            "IFRS_FALLBACK_TAGS['Revenue']. Real coverage (Phase 6.0): FCC, IBE, RAND.",
        ),
        MappingDecision(
            canonical_concept="Revenue",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.SEMANTIC_EQUIVALENT,
            source_concept="ifrs-full:RevenueFromContractsWithCustomers",
            notes="Phase 6.1: the standard IFRS 15 top-line tag -- the direct IFRS analogue of "
            "the us-gaap ASC-606 'Revenue (contract)' synonym this codebase already accepts as "
            "Revenue (01__tickers.py CONCEPT_SYNONYMS). Verified live via Phase 6.0 research "
            "against ALO's real FY2026 filing (value 19,171,000,000 EUR, consolidated, "
            "non-dimensional). Real coverage: ALO, FCT, SGO -- confirmed as a PRODUCTION GAP "
            "before this change: main.financials.financials had zero 'Revenue' rows for these "
            "3 tickers (and for ISP/NAI, which do NOT get this tag -- see below) across every "
            "fiscal year on record. Deliberately NOT mapped for ISP (a bank; its real top-line "
            "concepts are isp:InterestIncomeAndSimilarRevenues / ifrs-full:RevenueFromDividends "
            "/ ifrs-full:InterestRevenueCalculatedUsingEffectiveInterestMethod -- an issuer "
            "extension or a fundamentally different IFRS concept, not this tag, and not "
            "economically the same as a corporate Revenue line -- NULL > a questionable value) "
            "or NAI (a real-estate investment company; its real top line is "
            "ifrs-full:RentalIncomeFromInvestmentProperty, a genuinely different accounting "
            "concept from Revenue, not merely a differently-named alias -- also left NULL). "
            "See docs/phase6-european-esef-financial-coverage.md §5c/§13/§17.",
        ),
    ),
    "Net Income": (
        MappingDecision(
            canonical_concept="Net Income",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.DIRECT,
            source_concept="ifrs-full:ProfitLossAttributableToOwnersOfParent",
            notes="Matches 01__tickers.py IFRS_FALLBACK_TAGS['Net Income'] -- the parent-"
            "attributable profit figure, the same 'Net Income' definition SEC filers use.",
        ),
    ),
    "Net Income (incl NCI)": (
        MappingDecision(
            canonical_concept="Net Income (incl NCI)",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.DIRECT,
            source_concept="ifrs-full:ProfitLoss",
            notes="Verified live 2026-08 against FCC's real FY2024 filing (fact-145, value "
            "567,584,000 EUR). Matches 01__tickers.py IFRS_FALLBACK_TAGS['Net Income (incl NCI)'].",
        ),
    ),
    "Total Assets": (
        MappingDecision(
            canonical_concept="Total Assets",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.DIRECT,
            source_concept="ifrs-full:Assets",
            notes="Verified live 2026-08 against FCC's real FY2024 filing (fact-45, value "
            "14,235,959,000 EUR). Matches 01__tickers.py IFRS_FALLBACK_TAGS['Total Assets'].",
        ),
    ),
    "Cash & Equivalents": (
        MappingDecision(
            canonical_concept="Cash & Equivalents",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.DIRECT,
            source_concept="ifrs-full:CashAndCashEquivalents",
            notes="Verified live 2026-08 against FCC's real FY2024 filing (fact-43, value "
            "1,849,617,000 EUR). Matches 01__tickers.py IFRS_FALLBACK_TAGS['Cash & Equivalents'].",
        ),
    ),
    # ── Phase 6.1 additions -- all Tier 1 (docs/phase6-european-esef-financial-coverage.md §16):
    # exact reuse of an existing IFRS_FALLBACK_TAGS tag string, each individually re-verified
    # against real Phase 6.0 xBRL-JSON evidence (ticker/value cited per entry).
    "Income Tax": (
        MappingDecision(
            canonical_concept="Income Tax",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.DIRECT,
            source_concept="ifrs-full:IncomeTaxExpenseContinuingOperations",
            notes="Matches IFRS_FALLBACK_TAGS['Income Tax']. Verified live via Phase 6.0 "
            "research against ALO's real FY2026 filing (value 199,000,000 EUR). Real coverage: "
            "8/8 issuers -- the single most universal concept found in this research.",
        ),
    ),
    "Operating Cash Flow": (
        MappingDecision(
            canonical_concept="Operating Cash Flow",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.DIRECT,
            source_concept="ifrs-full:CashFlowsFromUsedInOperatingActivities",
            notes="Matches IFRS_FALLBACK_TAGS['Operating Cash Flow']. Verified live via Phase "
            "6.0 research against ALO's real FY2026 filing (value 891,000,000 EUR). Real "
            "coverage: 8/8 issuers.",
        ),
    ),
    "Investing Cash Flow": (
        MappingDecision(
            canonical_concept="Investing Cash Flow",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.DIRECT,
            source_concept="ifrs-full:CashFlowsFromUsedInInvestingActivities",
            notes="Matches IFRS_FALLBACK_TAGS['Investing Cash Flow']. Verified live via Phase "
            "6.0 research against ALO's real FY2026 filing (value -552,000,000 EUR). Real "
            "coverage: 8/8 issuers.",
        ),
    ),
    "Financing Cash Flow": (
        MappingDecision(
            canonical_concept="Financing Cash Flow",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.DIRECT,
            source_concept="ifrs-full:CashFlowsFromUsedInFinancingActivities",
            notes="Matches IFRS_FALLBACK_TAGS['Financing Cash Flow']. Verified live via Phase "
            "6.0 research against ALO's real FY2026 filing (value -273,000,000 EUR). Real "
            "coverage: 8/8 issuers.",
        ),
    ),
    "Total Stockholders Equity": (
        MappingDecision(
            canonical_concept="Total Stockholders Equity",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.DIRECT,
            source_concept="ifrs-full:EquityAttributableToOwnersOfParent",
            notes="Matches IFRS_FALLBACK_TAGS['Total Stockholders Equity']. Verified live via "
            "Phase 6.0 research against ALO's real FY2026 filing (value 10,663,000,000 EUR). "
            "Real coverage: 7/8 issuers (all but ISP, a bank -- see NAI/ISP note on Revenue).",
        ),
    ),
    "Total Equity (incl NCI)": (
        MappingDecision(
            canonical_concept="Total Equity (incl NCI)",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.DIRECT,
            source_concept="ifrs-full:Equity",
            notes="Matches IFRS_FALLBACK_TAGS['Total Equity (incl NCI)']. Verified live via "
            "Phase 6.0 research against ALO's real FY2026 filing (value 10,784,000,000 EUR). "
            "Real coverage: 7/8 issuers (all but ISP).",
        ),
    ),
    "PP&E Net": (
        MappingDecision(
            canonical_concept="PP&E Net",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.DIRECT,
            source_concept="ifrs-full:PropertyPlantAndEquipment",
            notes="Matches IFRS_FALLBACK_TAGS['PP&E Net']. Verified live via Phase 6.0 "
            "research against ALO's real FY2026 filing (value 2,858,000,000 EUR). Real "
            "coverage: 7/8 issuers (all but ISP -- a bank's balance sheet has no comparable "
            "PP&E line at this scale).",
        ),
    ),
    "Total Current Assets": (
        MappingDecision(
            canonical_concept="Total Current Assets",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.DIRECT,
            source_concept="ifrs-full:CurrentAssets",
            notes="Matches IFRS_FALLBACK_TAGS['Total Current Assets']. Verified live via Phase "
            "6.0 research against FCC's real FY2024 filing (value 5,724,200,000 EUR). Real "
            "coverage: 5/8 issuers -- absence for the other 3 reflects those issuers not "
            "presenting a current/noncurrent split (a real IFRS presentation choice), not a "
            "mapping gap; unlocks Current Ratio / Quick Ratio / Net-Net Finder inputs (see "
            "docs/phase6-european-esef-financial-coverage.md §11).",
        ),
    ),
    "Total Current Liabilities": (
        MappingDecision(
            canonical_concept="Total Current Liabilities",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.DIRECT,
            source_concept="ifrs-full:CurrentLiabilities",
            notes="Matches IFRS_FALLBACK_TAGS['Total Current Liabilities']. Verified live via "
            "Phase 6.0 research against FCC's real FY2024 filing (value 3,528,830,000 EUR). "
            "Real coverage: 5/8 issuers, same presentation-choice caveat as Total Current Assets.",
        ),
    ),
    "Goodwill": (
        MappingDecision(
            canonical_concept="Goodwill",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.DIRECT,
            source_concept="ifrs-full:Goodwill",
            notes="Matches IFRS_FALLBACK_TAGS['Goodwill']. Verified live via Phase 6.0 research "
            "against ALO's real FY2026 filing (value 9,121,000,000 EUR). Real coverage: 5/8 "
            "issuers.",
        ),
    ),
    "Interest Expense": (
        MappingDecision(
            canonical_concept="Interest Expense",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.DIRECT,
            source_concept="ifrs-full:FinanceCosts",
            notes="Matches the first entry of IFRS_FALLBACK_TAGS['Interest Expense'] "
            "(['FinanceCosts', 'InterestExpense']) -- only FinanceCosts is mapped here: "
            "Phase 6.0 research found zero real occurrences of the bare ifrs-full:InterestExpense "
            "tag across all 8 issuers, so adding it would be an unverified guess, not a "
            "confirmed reuse. Verified live against ALO's real FY2026 filing (value "
            "204,000,000 EUR). Real coverage: 5/8 issuers.",
        ),
    ),
    "Dividends Paid": (
        MappingDecision(
            canonical_concept="Dividends Paid",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.DIRECT,
            source_concept="ifrs-full:DividendsPaid",
            notes="Matches IFRS_FALLBACK_TAGS['Dividends Paid']. Verified live via Phase 6.0 "
            "research against ALO's real FY2026 filing (value 38,000,000 EUR). Real coverage: "
            "6/8 issuers.",
        ),
    ),
    "Operating Income": (
        MappingDecision(
            canonical_concept="Operating Income",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.DIRECT,
            source_concept="ifrs-full:ProfitLossFromOperatingActivities",
            notes="Matches IFRS_FALLBACK_TAGS['Operating Income']. Verified live via Phase 6.0 "
            "research against ALO's real FY2026 filing (value 544,000,000 EUR). Real coverage: "
            "5/8 issuers -- unlocks EBITDA / Interest Coverage / ROIC / ROCE / ROTCE for those 5 "
            "(docs/phase6-european-esef-financial-coverage.md §11).",
        ),
    ),
    "Intangible Assets": (
        MappingDecision(
            canonical_concept="Intangible Assets",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.DIRECT,
            source_concept="ifrs-full:IntangibleAssetsOtherThanGoodwill",
            notes="Matches the canonical tag in IFRS_FALLBACK_TAGS['Intangible Assets']. "
            "Verified live via Phase 6.0 research against ALO's real FY2026 filing (value "
            "1,766,000,000 EUR). Real coverage: 3/8 issuers.",
        ),
    ),
    "Inventory": (
        MappingDecision(
            canonical_concept="Inventory",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.DIRECT,
            source_concept="ifrs-full:Inventories",
            notes="Matches IFRS_FALLBACK_TAGS['Inventory']. Verified live via Phase 6.0 "
            "research against ALO's real FY2026 filing (value 4,276,000,000 EUR). Real "
            "coverage: 3/8 issuers.",
        ),
    ),
    "Cost of Revenue": (
        MappingDecision(
            canonical_concept="Cost of Revenue",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.DIRECT,
            source_concept="ifrs-full:CostOfSales",
            notes="Matches IFRS_FALLBACK_TAGS['Cost of Revenue']. Verified live via Phase 6.0 "
            "research against ALO's real FY2026 filing (value 16,819,000,000 EUR). Real "
            "coverage: 3/8 issuers (ALO, RAND, SGO -- the same 3 issuers using the "
            "RevenueFromContractsWithCustomers Revenue variant above).",
        ),
    ),
    # ── Phase 6.6 (Tier A) — 8 concepts identified and evidence-gathered by the Phase 6.4 audit
    # (docs/phase6-4-european-financial-statement-coverage-audit.md §12), re-verified against a
    # fresh live re-fetch during that same phase before implementation here. Two
    # (Non-Controlling Interests, Finance Income) required a brand-new SEC-side canonical slot in
    # 01__tickers.py's STATEMENTS (documented there); the other six reuse an existing canonical
    # concept, adding real EU tags only.
    "Accounts Payable": (
        MappingDecision(
            canonical_concept="Accounts Payable",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.DIRECT,
            source_concept="ifrs-full:TradeAndOtherCurrentPayablesToTradeSuppliers",
            notes="Verified live 2026-08 (Phase 6.4 fresh fetch) against FCC's real FY2024 "
            "filing (value 1,118,620,000 EUR). Real coverage: FCC, ALO, SGO, NAI (4/8).",
        ),
        MappingDecision(
            canonical_concept="Accounts Payable",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.SEMANTIC_EQUIVALENT,
            source_concept="ifrs-full:TradeAndOtherPayablesToTradeSuppliers",
            notes="Same concept as the above, without the 'Current' qualifier -- IBE's own real "
            "tag variant (value 6,183,000,000 EUR, Phase 6.4 fresh fetch). Real coverage: IBE "
            "only (1/8).",
        ),
        MappingDecision(
            canonical_concept="Accounts Payable",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.SEMANTIC_EQUIVALENT,
            source_concept="ifrs-full:TradeAndOtherCurrentPayables",
            notes="A broader aggregate (trade + other payables, not trade-suppliers-only) used "
            "by issuers that don't split the two -- FCT (3,570,852,000 EUR) and RAND "
            "(4,217,000,000 EUR), Phase 6.4 fresh fetch. Real coverage: FCT, RAND (2/8). "
            "Combined with the two entries above: 7/8 real coverage (all but ISP, a bank).",
        ),
    ),
    "Non-Controlling Interests": (
        MappingDecision(
            canonical_concept="Non-Controlling Interests",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.DIRECT,
            source_concept="ifrs-full:NoncontrollingInterests",
            notes="Verified live 2026-08 (Phase 6.4 fresh fetch) against FCC's real FY2024 "
            "filing (value 1,003,303,000 EUR -- independently cross-checked to reconcile "
            "exactly against Total Stockholders Equity + this value = Total Equity (incl NCI), "
            "Phase 6.5's own §B4 identity test). Real coverage: 8/8 issuers -- the single most "
            "universal Tier A concept found. New canonical concept (01__tickers.py "
            "BALANCE_SHEET['Non-Controlling Interests'], distinct from 'Total Equity (incl "
            "NCI)' -- see that concept's own notes for the distinction).",
        ),
    ),
    "Stock-based Compensation": (
        MappingDecision(
            canonical_concept="Stock-based Compensation",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.DIRECT,
            source_concept="ifrs-full:IncreaseDecreaseThroughSharebasedPaymentTransactions",
            notes="Verified live 2026-08 (Phase 6.4 fresh fetch) against SGO's real filing "
            "(value 89,000,000 EUR). Real coverage: ALO, IBE, SGO, FCT, RAND, ISP (6/8). "
            "`ifrs-full:AdjustmentsForSharebasedPayments` also appears for ALO/RAND but adds no "
            "issuer this tag doesn't already cover -- deliberately not mapped, to avoid an "
            "unnecessary second source tag for zero coverage gain (the exact kind of avoidable "
            "multi-tag complexity Phase 6.5 spent real effort untangling for a different "
            "concept pair). Reuses the existing canonical concept "
            "(01__tickers.py CASH_FLOW['Stock-based Compensation'] -- already present, not new; "
            "Phase 6.4's own 'new canonical concept' framing for this one was corrected during "
            "implementation).",
        ),
    ),
    "Changes in Working Capital": (
        MappingDecision(
            canonical_concept="Changes in Working Capital",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.DIRECT,
            source_concept="ifrs-full:IncreaseDecreaseInWorkingCapital",
            notes="Verified live 2026-08 (Phase 6.4 fresh fetch) against FCT's real filing "
            "(value 730,093,000 EUR). Real coverage: FCC, ALO, FCT, NAI, RAND (5/8). Reuses the "
            "existing canonical concept -- note the canonical label is plural ('Changes', not "
            "'Change') to match 01__tickers.py CASH_FLOW's existing entry exactly.",
        ),
    ),
    "Income Before Tax": (
        MappingDecision(
            canonical_concept="Income Before Tax",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.DIRECT,
            source_concept="ifrs-full:ProfitLossBeforeTax",
            notes="Verified live 2026-08 (Phase 6.4 fresh fetch) against IBE's real filing "
            "(value 8,117,000,000 EUR). Real coverage: FCC, ALO, IBE, FCT, NAI, RAND, ISP (7/8, "
            "all but SGO). Reuses the existing canonical concept -- no new 'Profit Before Tax' "
            "concept was created, per instruction.",
        ),
    ),
    "EPS Basic": (
        MappingDecision(
            canonical_concept="EPS Basic",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.DIRECT,
            source_concept="ifrs-full:BasicEarningsLossPerShare",
            notes="Verified live 2026-08 (Phase 6.4 fresh fetch). Real coverage: FCC, ALO, SGO, "
            "FCT, NAI, RAND, ISP (7/8).",
        ),
        MappingDecision(
            canonical_concept="EPS Basic",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.SEMANTIC_EQUIVALENT,
            source_concept="ifrs-full:BasicEarningsLossPerShareFromContinuingOperations",
            notes="IBE's own real variant (the only issuer that tags this instead of the plain "
            "form; FCT tags both, with an identical value, a normal ESEF duplicate-tagging "
            "pattern per Phase 6.0 §13). Combined with the entry above: 8/8 real coverage.",
        ),
    ),
    "EPS Diluted": (
        MappingDecision(
            canonical_concept="EPS Diluted",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.DIRECT,
            source_concept="ifrs-full:DilutedEarningsLossPerShare",
            notes="Verified live 2026-08 (Phase 6.4 fresh fetch). Real coverage: FCC, ALO, SGO, "
            "FCT, NAI, RAND, ISP (7/8) -- same pattern as EPS Basic.",
        ),
        MappingDecision(
            canonical_concept="EPS Diluted",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.SEMANTIC_EQUIVALENT,
            source_concept="ifrs-full:DilutedEarningsLossPerShareFromContinuingOperations",
            notes="IBE's own real variant, same pattern as EPS Basic's own second entry. "
            "Combined: 8/8 real coverage.",
        ),
    ),
    "Finance Income": (
        MappingDecision(
            canonical_concept="Finance Income",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.DIRECT,
            source_concept="ifrs-full:FinanceIncome",
            notes="Verified live 2026-08 (Phase 6.4 fresh fetch) against IBE's real filing "
            "(value 2,377,000,000 EUR). Real coverage: FCC, ALO, IBE, FCT, RAND (5/8). "
            "`ifrs-full:FinanceIncomeCost` (SGO, NAI, ISP-adjacent) is a NET tag (income minus "
            "cost, ambiguous sign) -- deliberately excluded, consistent with this codebase's "
            "existing 'NET tags are EXCLUDED on purpose' policy for Interest Expense "
            "(01__tickers.py, CONCEPT_SYNONYMS section). ISP's own real tag is an "
            "insurance-specific extension (bank/insurer-specific), correctly excluded too. New "
            "canonical concept (01__tickers.py INCOME_STATEMENT['Finance Income'] -- see that "
            "entry's own notes on the us-gaap tag choice, which is less independently verified "
            "than every other entry here).",
        ),
    ),
    "Accounts Receivable": (
        MappingDecision(
            canonical_concept="Accounts Receivable",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.DIRECT,
            source_concept="ifrs-full:CurrentTradeReceivables",
            notes="Verified live 2026-08 (Phase 6.4 fresh fetch) against FCC's real filing "
            "(value 2,597,142,000 EUR). Real coverage: FCC, ALO, SGO, NAI (4/8). Implemented "
            "here (EU_CANONICAL_MAPPING) only -- deliberately does NOT touch "
            "01__tickers.py's IFRS_FALLBACK_TAGS['Accounts Receivable'] entry (a separate, "
            "pre-existing tag-string error affecting real ifrs-full 20-F/40-F SEC/Canada "
            "filers, 0/8 match for EU either way) -- the EU adapter never reads "
            "IFRS_FALLBACK_TAGS at all (confirmed: no reference anywhere in "
            "16__fetch_eu_xbrl.py), so this mapping carries zero shared-infrastructure risk. "
            "Fixing the separate SEC-side tag error is out of this phase's scope.",
        ),
        MappingDecision(
            canonical_concept="Accounts Receivable",
            status=MappingStatus.ACCEPTED,
            mapping_type=MappingType.SEMANTIC_EQUIVALENT,
            source_concept="ifrs-full:TradeAndOtherCurrentReceivables",
            notes="A broader aggregate variant (trade + other receivables) -- IBE "
            "(10,777,000,000 EUR), FCT, RAND, Phase 6.4 fresh fetch. Combined with the entry "
            "above: 7/8 real coverage (all but ISP, a bank).",
        ),
    ),
}

_SOURCE_CONCEPT_TO_DECISION = {
    decision.source_concept: decision
    for decisions in EU_CANONICAL_MAPPING.values()
    for decision in decisions
}


def map_source_fact_to_canonical(source_concept: str) -> MappingDecision | None:
    """Look up this source concept's canonical mapping decision. `None` (excluded outright, not
    NULL-valued-and-kept) for anything outside the accepted concepts above -- deliberately still
    a narrow, evidence-verified allow-list (36 accepted source tags across 30 canonical concepts
    as of Phase 6.6), not a giant IFRS taxonomy mapping guessed wholesale."""
    return _SOURCE_CONCEPT_TO_DECISION.get(source_concept)
