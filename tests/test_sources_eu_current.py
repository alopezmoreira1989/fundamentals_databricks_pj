"""Tests for fundamentals_pipeline.sources.eu_current -- the filings.xbrl.org adapter logic.

Fixture-only: no live network call in this suite (matches the repo-wide convention of pure,
locally-runnable tests). Fixtures use REAL values captured live against filings.xbrl.org during
Phase 5.1 research (2026-08) wherever noted -- not synthetic placeholders -- except where a
fixture is explicitly labeled constructed (e.g. Alstom's fiscal-year-boundary case, where only
the filing metadata, not actual fact values, was verified live).
"""

from __future__ import annotations

from datetime import date

import pytest

from fundamentals_pipeline.sources.base import SourceFiling
from fundamentals_pipeline.sources.eu_current import (
    EU_CANONICAL_MAPPING,
    EU_SOURCE_ID,
    FilingRejection,
    entity_from_pilot,
    extract_source_facts,
    is_consolidated_fact,
    is_current_period_fact,
    map_source_fact_to_canonical,
    parse_unit_currency,
    select_filing_for_period,
)
from fundamentals_pipeline.sources.mapping import MappingStatus, is_usable

# ── Real filing-list fixtures (captured live 2026-08 against filings.xbrl.org) ──────────────

_NAI_2025_FILINGS = [
    {
        "fxo_id": "724500JXEXUGEATP5L52-2025-12-31-ESEF-NL-0",
        "period_end": "2025-12-31",
        "processed": "2026-05-03 23:50:41",
        "error_count": 0,
        "warning_count": 14,
        "json_url": "/724500JXEXUGEATP5L52/2025-12-31/ESEF/NL/0/newamsterdaminvestnv-2025-12-31-1-en.json",
    },
    {
        "fxo_id": "724500JXEXUGEATP5L52-2025-12-31-ESEF-NL-1",
        "period_end": "2025-12-31",
        "processed": "2026-06-11 16:52:40",
        "error_count": 0,
        "warning_count": 14,
        "json_url": "/724500JXEXUGEATP5L52/2025-12-31/ESEF/NL/1/newamsterdaminvestnv-2025-12-31-1-en.json",
    },
]

_FINCANTIERI_2024_FILINGS = [
    {
        "fxo_id": "8156005BDF49128B6239-2024-12-31-ESEF-IT-0",
        "period_end": "2024-12-31",
        "processed": "2024-04-02",
        "error_count": 0,
        "warning_count": 4,
        "json_url": "/8156005BDF49128B6239/2024-12-31/ESEF/IT/0/8156005BDF49128B6239-2023-12-31-it.json",
    },
    {
        "fxo_id": "8156005BDF49128B6239-2024-12-31-ESEF-IT-1",
        "period_end": "2024-12-31",
        "processed": "2025-04-15",
        "error_count": 0,
        "warning_count": 2,
        "json_url": "/8156005BDF49128B6239/2024-12-31/ESEF/IT/1/8156005BDF49128B6239-2024-12-31-0-it.json",
    },
]

_FINCANTIERI_2025_FILING_MISSING_JSON = [
    {
        "fxo_id": "8156005BDF49128B6239-2025-12-31-ESEF-IT-0",
        "period_end": "2025-12-31",
        "processed": "2026-04-21",
        "error_count": 0,
        "warning_count": 0,
        "json_url": None,
    },
]


def test_select_filing_for_period_nai_amendment_later_processed_wins():
    winner, rejections = select_filing_for_period(_NAI_2025_FILINGS)
    assert winner["fxo_id"] == "724500JXEXUGEATP5L52-2025-12-31-ESEF-NL-1"
    assert rejections == [
        FilingRejection(fxo_id="724500JXEXUGEATP5L52-2025-12-31-ESEF-NL-0", reason="superseded")
    ]


def test_select_filing_for_period_fincantieri_amendment_later_processed_wins():
    winner, rejections = select_filing_for_period(_FINCANTIERI_2024_FILINGS)
    assert winner["fxo_id"] == "8156005BDF49128B6239-2024-12-31-ESEF-IT-1"
    assert len(rejections) == 1
    assert rejections[0].fxo_id == "8156005BDF49128B6239-2024-12-31-ESEF-IT-0"
    assert rejections[0].reason == "superseded"


def test_select_filing_for_period_missing_json_url_is_a_reported_failure_not_silent():
    """The real Fincantieri FY2025 case: a clean (0-error) filing with no json_url. Must NOT
    just vanish -- the winner is None AND the filing is explicitly reported as unselectable."""
    winner, rejections = select_filing_for_period(_FINCANTIERI_2025_FILING_MISSING_JSON)
    assert winner is None
    assert len(rejections) == 1
    assert rejections[0].fxo_id == "8156005BDF49128B6239-2025-12-31-ESEF-IT-0"
    assert rejections[0].reason == "missing_json_url"


def test_select_filing_for_period_has_errors_is_excluded_and_reported():
    filings = [
        {
            "fxo_id": "SYNTH-2024-12-31-ESEF-XX-0",
            "period_end": "2024-12-31",
            "processed": "2025-01-01",
            "error_count": 3,
            "json_url": "/some/path.json",
        }
    ]
    winner, rejections = select_filing_for_period(filings)
    assert winner is None
    assert rejections == [
        FilingRejection(fxo_id="SYNTH-2024-12-31-ESEF-XX-0", reason="has_errors")
    ]


def test_select_filing_for_period_empty_input():
    assert select_filing_for_period([]) == (None, [])


# ── Consolidated vs. dimensional fact discrimination (real NAI example) ────────────────────

def test_is_consolidated_fact_true_for_clean_four_key_dimensions():
    # Real: FCC fact-45 (ifrs-full:Assets)
    assert is_consolidated_fact(
        {"concept": "ifrs-full:Assets", "entity": "scheme:95980020140005178328",
         "period": "2025-01-01T00:00:00", "unit": "iso4217:EUR"}
    )


def test_is_consolidated_fact_false_for_dimensional_breakdown():
    # Real: New Amsterdam Invest's ComponentsOfEquityAxis breakdown fact
    assert not is_consolidated_fact(
        {"concept": "ifrs-full:Equity", "entity": "scheme:724500JXEXUGEATP5L52",
         "period": "2024-01-01T00:00:00",
         "ifrs-full:ComponentsOfEquityAxis": "ifrs-full:IssuedCapitalMember",
         "unit": "iso4217:EUR"}
    )


# ── Currency parsing ─────────────────────────────────────────────────────────────────────────

def test_parse_unit_currency_plain_eur():
    assert parse_unit_currency("iso4217:EUR") == "EUR"


def test_parse_unit_currency_non_currency_unit_returns_none():
    assert parse_unit_currency("xbrli:shares") is None


def test_parse_unit_currency_composite_per_share_unit_returns_none():
    assert parse_unit_currency("iso4217:EUR/xbrli:shares") is None


# ── Current-period vs. comparative-year discrimination (real FCC example) ──────────────────

def test_is_current_period_fact_duration_current_year():
    # Real FCC fact-105: FY2024 Revenue
    assert is_current_period_fact(
        "2024-01-01T00:00:00/2025-01-01T00:00:00", date(2024, 12, 31)
    )


def test_is_current_period_fact_duration_prior_year_comparative_excluded():
    # Real FCC fact-106: FY2023 comparative Revenue, same filing, same concept
    assert not is_current_period_fact(
        "2023-01-01T00:00:00/2024-01-01T00:00:00", date(2024, 12, 31)
    )


def test_is_current_period_fact_instant():
    # Real FCC fact-43: Cash & Equivalents as of FY2024 close
    assert is_current_period_fact("2025-01-01T00:00:00", date(2024, 12, 31))


def test_is_current_period_fact_non_calendar_fiscal_year():
    """Constructed fixture (Alstom's real fiscal-year-end date, synthetic fact value) -- proves
    the exclusive-end rule needs no fiscal-year-length assumption for a March 31 close."""
    assert is_current_period_fact("2025-04-01T00:00:00", date(2025, 3, 31))
    assert not is_current_period_fact("2024-04-01T00:00:00", date(2025, 3, 31))


# ── End-to-end fact extraction ──────────────────────────────────────────────────────────────

def _fcc_shaped_raw_json() -> dict:
    lei = "95980020140005178328"
    return {
        "documentInfo": {"namespaces": {"ifrs-full": "https://xbrl.ifrs.org/taxonomy/2022-03-24/ifrs-full"}},
        "facts": {
            "fact-105": {  # current-year Revenue -- KEEP
                "value": "9071416000.0",
                "dimensions": {"concept": "ifrs-full:Revenue", "entity": f"scheme:{lei}",
                                "period": "2024-01-01T00:00:00/2025-01-01T00:00:00", "unit": "iso4217:EUR"},
            },
            "fact-106": {  # prior-year comparative Revenue -- EXCLUDE (comparative)
                "value": "8217292000.0",
                "dimensions": {"concept": "ifrs-full:Revenue", "entity": f"scheme:{lei}",
                                "period": "2023-01-01T00:00:00/2024-01-01T00:00:00", "unit": "iso4217:EUR"},
            },
            "fact-45": {  # current Assets -- KEEP
                "value": "14235959000.0",
                "dimensions": {"concept": "ifrs-full:Assets", "entity": f"scheme:{lei}",
                                "period": "2025-01-01T00:00:00", "unit": "iso4217:EUR"},
            },
            "fact-equity-breakdown": {  # dimensional -- EXCLUDE (not consolidated)
                "value": "247000",
                "dimensions": {"concept": "ifrs-full:Equity", "entity": f"scheme:{lei}",
                                "period": "2025-01-01T00:00:00",
                                "ifrs-full:ComponentsOfEquityAxis": "ifrs-full:IssuedCapitalMember",
                                "unit": "iso4217:EUR"},
            },
            "fact-wrong-entity": {  # wrong entity -- EXCLUDE
                "value": "1.0",
                "dimensions": {"concept": "ifrs-full:Assets", "entity": "scheme:OTHERENTITY",
                                "period": "2025-01-01T00:00:00", "unit": "iso4217:EUR"},
            },
            "fact-null-value": {  # nil value -- EXCLUDE
                "value": None,
                "dimensions": {"concept": "ifrs-full:Assets", "entity": f"scheme:{lei}",
                                "period": "2025-01-01T00:00:00", "unit": "iso4217:EUR"},
            },
        },
    }


def test_extract_source_facts_filters_to_exactly_the_expected_two_facts():
    filing = SourceFiling(
        source_entity_id="95980020140005178328",
        source_filing_id="95980020140005178328-2024-12-31-ESEF-ES-0",
        filing_type="ESEF",
        filed_date="2025-05-08",
        period_end="2024-12-31",
    )
    facts = extract_source_facts(_fcc_shaped_raw_json(), filing, EU_SOURCE_ID)
    concepts = {f.source_concept for f in facts}
    assert concepts == {"ifrs-full:Revenue", "ifrs-full:Assets"}
    assert all(f.source_id == EU_SOURCE_ID for f in facts)
    assert all(f.source_currency == "EUR" for f in facts)
    revenue = next(f for f in facts if f.source_concept == "ifrs-full:Revenue")
    assert revenue.source_value == 9071416000.0
    assert revenue.source_period_start == "2024-01-01"
    assert revenue.source_period_end == "2025-01-01"


def test_extract_source_facts_no_period_end_returns_empty():
    filing = SourceFiling(
        source_entity_id="X", source_filing_id="Y", filing_type="ESEF",
        filed_date=None, period_end=None,
    )
    assert extract_source_facts(_fcc_shaped_raw_json(), filing, EU_SOURCE_ID) == []


# ── Canonical mapping ────────────────────────────────────────────────────────────────────────

def test_map_source_fact_to_canonical_known_concept():
    decision = map_source_fact_to_canonical("ifrs-full:Revenue")
    assert decision is not None
    assert decision.canonical_concept == "Revenue"
    assert is_usable(decision)


def test_map_source_fact_to_canonical_unmapped_concept_returns_none():
    assert map_source_fact_to_canonical("ifrs-full:SomeUnmappedConcept") is None


def test_eu_canonical_mapping_all_entries_are_accepted_and_usable():
    # Phase 6.1: EU_CANONICAL_MAPPING values are tuples of one-or-more MappingDecisions
    # (Revenue has two: the bare tag and the RevenueFromContractsWithCustomers variant).
    # Phase 6.6 (Tier A): widened from 21/22 to 30 canonical concepts / 36 accepted source tags.
    assert len(EU_CANONICAL_MAPPING) == 30
    all_decisions = [d for decisions in EU_CANONICAL_MAPPING.values() for d in decisions]
    assert len(all_decisions) == 36
    for decision in all_decisions:
        assert decision.status == MappingStatus.ACCEPTED
        assert is_usable(decision)
        assert decision.source_concept.startswith("ifrs-full:")


# ── Phase 6.1: Tier 1 coverage-expansion mappings ───────────────────────────────────────────


@pytest.mark.parametrize(
    "source_concept, expected_canonical",
    [
        ("ifrs-full:IncomeTaxExpenseContinuingOperations", "Income Tax"),
        ("ifrs-full:CashFlowsFromUsedInOperatingActivities", "Operating Cash Flow"),
        ("ifrs-full:CashFlowsFromUsedInInvestingActivities", "Investing Cash Flow"),
        ("ifrs-full:CashFlowsFromUsedInFinancingActivities", "Financing Cash Flow"),
        ("ifrs-full:EquityAttributableToOwnersOfParent", "Total Stockholders Equity"),
        ("ifrs-full:Equity", "Total Equity (incl NCI)"),
        ("ifrs-full:PropertyPlantAndEquipment", "PP&E Net"),
        ("ifrs-full:CurrentAssets", "Total Current Assets"),
        ("ifrs-full:CurrentLiabilities", "Total Current Liabilities"),
        ("ifrs-full:Goodwill", "Goodwill"),
        ("ifrs-full:FinanceCosts", "Interest Expense"),
        ("ifrs-full:DividendsPaid", "Dividends Paid"),
        ("ifrs-full:ProfitLossFromOperatingActivities", "Operating Income"),
        ("ifrs-full:IntangibleAssetsOtherThanGoodwill", "Intangible Assets"),
        ("ifrs-full:Inventories", "Inventory"),
        ("ifrs-full:CostOfSales", "Cost of Revenue"),
    ],
)
def test_tier1_mappings_resolve_to_the_expected_canonical_concept(source_concept, expected_canonical):
    decision = map_source_fact_to_canonical(source_concept)
    assert decision is not None
    assert decision.canonical_concept == expected_canonical
    assert is_usable(decision)


def test_revenue_maps_from_either_real_tag_variant():
    # Real evidence (Phase 6.0): FCC/IBE/RAND tag the bare concept; ALO/FCT/SGO tag the IFRS 15
    # contract-revenue concept instead -- both must route to the same canonical "Revenue".
    bare = map_source_fact_to_canonical("ifrs-full:Revenue")
    contract = map_source_fact_to_canonical("ifrs-full:RevenueFromContractsWithCustomers")
    assert bare.canonical_concept == "Revenue"
    assert contract.canonical_concept == "Revenue"
    assert bare.source_concept != contract.source_concept


def test_bare_interest_expense_tag_is_not_mapped_no_real_evidence():
    # Phase 6.0 research found zero real occurrences of the bare ifrs-full:InterestExpense tag
    # across all 8 issuers -- only FinanceCosts is backed by real evidence, so only FinanceCosts
    # is mapped. Adding InterestExpense here would be an unverified guess.
    assert map_source_fact_to_canonical("ifrs-full:InterestExpense") is None


def test_isp_bank_revenue_concepts_are_not_mapped_to_revenue():
    # ISP's real top-line concepts (a bank) are never mapped to canonical Revenue -- NULL is
    # correct, not a questionable guess (docs/phase6-european-esef-financial-coverage.md §5c).
    for isp_concept in (
        "isp:InterestIncomeAndSimilarRevenues",
        "ifrs-full:RevenueFromDividends",
        "ifrs-full:InterestRevenueCalculatedUsingEffectiveInterestMethod",
    ):
        assert map_source_fact_to_canonical(isp_concept) is None


def test_nai_real_estate_top_line_is_not_mapped_to_revenue():
    # NAI's real top-line concept (a real-estate investment company) is a genuinely different
    # accounting concept from Revenue, not an alias -- must stay unmapped.
    assert map_source_fact_to_canonical("ifrs-full:RentalIncomeFromInvestmentProperty") is None


def test_shares_diluted_has_no_eu_mapping():
    # Phase 6.0 confirmed zero usable share-count concepts across all 8 real issuers -- there
    # must be no EU_CANONICAL_MAPPING entry for "Shares Diluted" at all (NULL, not derived).
    assert "Shares Diluted" not in EU_CANONICAL_MAPPING


# ── Phase 6.6: Tier A coverage-expansion mappings ───────────────────────────────────────────
# docs/phase6-4-european-financial-statement-coverage-audit.md §12. Real values/tag strings cited
# in eu_current.py's own MappingDecision.notes for each entry below.


@pytest.mark.parametrize(
    "source_concept, expected_canonical",
    [
        ("ifrs-full:TradeAndOtherCurrentPayablesToTradeSuppliers", "Accounts Payable"),
        ("ifrs-full:TradeAndOtherPayablesToTradeSuppliers", "Accounts Payable"),
        ("ifrs-full:TradeAndOtherCurrentPayables", "Accounts Payable"),
        ("ifrs-full:NoncontrollingInterests", "Non-Controlling Interests"),
        ("ifrs-full:IncreaseDecreaseThroughSharebasedPaymentTransactions", "Stock-based Compensation"),
        ("ifrs-full:IncreaseDecreaseInWorkingCapital", "Changes in Working Capital"),
        ("ifrs-full:ProfitLossBeforeTax", "Income Before Tax"),
        ("ifrs-full:BasicEarningsLossPerShare", "EPS Basic"),
        ("ifrs-full:BasicEarningsLossPerShareFromContinuingOperations", "EPS Basic"),
        ("ifrs-full:DilutedEarningsLossPerShare", "EPS Diluted"),
        ("ifrs-full:DilutedEarningsLossPerShareFromContinuingOperations", "EPS Diluted"),
        ("ifrs-full:FinanceIncome", "Finance Income"),
        ("ifrs-full:CurrentTradeReceivables", "Accounts Receivable"),
        ("ifrs-full:TradeAndOtherCurrentReceivables", "Accounts Receivable"),
    ],
)
def test_tier_a_mappings_resolve_to_the_expected_canonical_concept(source_concept, expected_canonical):
    decision = map_source_fact_to_canonical(source_concept)
    assert decision is not None
    assert decision.canonical_concept == expected_canonical
    assert is_usable(decision)


def test_accounts_payable_maps_from_all_three_real_tag_variants():
    # Real evidence: FCC/ALO/SGO/NAI tag ...ToTradeSuppliers (Current); IBE tags the same concept
    # without the "Current" qualifier; FCT/RAND tag the broader ...CurrentPayables aggregate.
    # All three must route to the same canonical concept, combining to 7/8 real coverage.
    variants = [
        "ifrs-full:TradeAndOtherCurrentPayablesToTradeSuppliers",
        "ifrs-full:TradeAndOtherPayablesToTradeSuppliers",
        "ifrs-full:TradeAndOtherCurrentPayables",
    ]
    decisions = [map_source_fact_to_canonical(v) for v in variants]
    assert all(d is not None and d.canonical_concept == "Accounts Payable" for d in decisions)
    assert len({d.source_concept for d in decisions}) == 3  # three genuinely distinct tags


def test_accounts_receivable_new_ifrs_tags_are_additive_not_a_replacement():
    # Phase 6.6's explicit safety requirement: the new EU tags must be added WITHOUT touching
    # 01__tickers.py's IFRS_FALLBACK_TAGS["Accounts Receivable"] entry (a separate, pre-existing,
    # SEC-side tag-string error affecting real ifrs-full 20-F/40-F filers) -- the EU adapter's own
    # mapping is a fully independent mechanism, so this is a real, not merely incidental, safety
    # property. The old (wrong-for-EU, 0/8 match) "TradeReceivables" tag is deliberately NOT an
    # entry in EU_CANONICAL_MAPPING at all -- only the two real tags are.
    ar_source_concepts = {
        d.source_concept
        for d in EU_CANONICAL_MAPPING["Accounts Receivable"]
    }
    assert ar_source_concepts == {
        "ifrs-full:CurrentTradeReceivables",
        "ifrs-full:TradeAndOtherCurrentReceivables",
    }
    assert map_source_fact_to_canonical("ifrs-full:TradeReceivables") is None


def test_net_finance_income_cost_tags_are_not_mapped_to_finance_income():
    # SGO/NAI/ISP-adjacent real tags are NET (income minus cost, ambiguous sign) -- deliberately
    # excluded, consistent with this codebase's existing "NET tags EXCLUDED" policy for Interest
    # Expense (01__tickers.py). Only the real, gross ifrs-full:FinanceIncome tag is mapped.
    assert map_source_fact_to_canonical("ifrs-full:FinanceIncomeCost") is None
    assert map_source_fact_to_canonical(
        "isp:InsuranceFinanceIncomeExpensesFromInsuranceContractsIssuedRecognisedInProfitOrLoss"
    ) is None


def test_sharebased_payment_adjustments_tag_deliberately_not_mapped():
    # ifrs-full:AdjustmentsForSharebasedPayments adds no issuer coverage beyond what
    # IncreaseDecreaseThroughSharebasedPaymentTransactions already covers (real evidence: both
    # tags coexist for ALO with an identical value) -- deliberately not mapped, to avoid an
    # unnecessary second source tag for the same canonical concept with zero coverage gain.
    assert map_source_fact_to_canonical("ifrs-full:AdjustmentsForSharebasedPayments") is None


def test_phase_6_5_total_equity_and_net_income_mappings_are_unchanged():
    # Regression guard: Phase 6.6 must not touch the Phase 6.3 (Net Income) or Phase 6.5 (Total
    # Equity incl NCI) mappings -- both concepts are genuinely distinct from this phase's new
    # "Non-Controlling Interests" concept (a standalone NCI balance, not the aggregate total).
    net_income = map_source_fact_to_canonical("ifrs-full:ProfitLossAttributableToOwnersOfParent")
    total_equity_incl_nci = map_source_fact_to_canonical("ifrs-full:Equity")
    nci_balance = map_source_fact_to_canonical("ifrs-full:NoncontrollingInterests")
    assert net_income.canonical_concept == "Net Income"
    assert total_equity_incl_nci.canonical_concept == "Total Equity (incl NCI)"
    assert nci_balance.canonical_concept == "Non-Controlling Interests"
    assert len({net_income.canonical_concept, total_equity_incl_nci.canonical_concept,
                nci_balance.canonical_concept}) == 3  # three genuinely distinct concepts


# ── Entity construction ─────────────────────────────────────────────────────────────────────

def test_entity_from_pilot_builds_source_qualified_issuer_id():
    entity = entity_from_pilot(EU_SOURCE_ID, "FCC", "95980020140005178328", "Fomento de Construcciones y Contratas, S.A.")
    assert entity.source_id == EU_SOURCE_ID
    assert entity.source_entity_id == "95980020140005178328"
    assert entity.issuer_id == "EU_CURRENT:95980020140005178328"
    assert entity.ticker == "FCC"
