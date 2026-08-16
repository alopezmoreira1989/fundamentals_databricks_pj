"""Tests for fundamentals_pipeline.sources.eu_current -- the filings.xbrl.org adapter logic.

Fixture-only: no live network call in this suite (matches the repo-wide convention of pure,
locally-runnable tests). Fixtures use REAL values captured live against filings.xbrl.org during
Phase 5.1 research (2026-08) wherever noted -- not synthetic placeholders -- except where a
fixture is explicitly labeled constructed (e.g. Alstom's fiscal-year-boundary case, where only
the filing metadata, not actual fact values, was verified live).
"""

from __future__ import annotations

from datetime import date

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
    assert len(EU_CANONICAL_MAPPING) == 5
    for decision in EU_CANONICAL_MAPPING.values():
        assert decision.status == MappingStatus.ACCEPTED
        assert is_usable(decision)
        assert decision.source_concept.startswith("ifrs-full:")


# ── Entity construction ─────────────────────────────────────────────────────────────────────

def test_entity_from_pilot_builds_source_qualified_issuer_id():
    entity = entity_from_pilot(EU_SOURCE_ID, "FCC", "95980020140005178328", "Fomento de Construcciones y Contratas, S.A.")
    assert entity.source_id == EU_SOURCE_ID
    assert entity.source_entity_id == "95980020140005178328"
    assert entity.issuer_id == "EU_CURRENT:95980020140005178328"
    assert entity.ticker == "FCC"
