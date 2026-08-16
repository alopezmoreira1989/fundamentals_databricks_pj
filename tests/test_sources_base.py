"""Tests for the FundamentalsSource adapter contract (fundamentals_pipeline/sources/base.py).

Proves the Protocol's shape is genuinely satisfiable by a real (if trivial) implementation --
not just declared and never checked.
"""

from __future__ import annotations

from fundamentals_pipeline.sources.base import (
    FundamentalsSource,
    SourceEntity,
    SourceEntityMetadata,
    SourceFact,
    SourceFiling,
)


class _FakeSource:
    """A minimal fake fundamentals source -- structurally satisfies FundamentalsSource
    (PEP 544) without inheriting from it."""

    source_id = "FAKE_SOURCE"

    def discover_entities(self, tickers):
        return [
            SourceEntity(
                source_id=self.source_id,
                source_entity_id=f"FAKE-{t}",
                issuer_id=f"{self.source_id}:FAKE-{t}",
                name=t,
                ticker=t,
            )
            for t in tickers
        ]

    def discover_filings(self, entity):
        return [
            SourceFiling(
                source_entity_id=entity.source_entity_id,
                source_filing_id="FILING-1",
                filing_type="annual",
                filed_date="2026-03-01",
                period_end="2025-12-31",
            )
        ]

    def retrieve_facts(self, filing):
        return [
            SourceFact(
                source_id=self.source_id,
                source_entity_id=filing.source_entity_id,
                source_filing_id=filing.source_filing_id,
                source_concept="Revenue",
                source_period_start="2025-01-01",
                source_period_end=filing.period_end,
                source_currency="EUR",
                source_value=1_000_000.0,
            )
        ]

    def detect_metadata(self, entity):
        return SourceEntityMetadata(accounting_framework="ifrs-full", reporting_currency="EUR")


def test_fake_source_satisfies_the_protocol_structurally():
    source: FundamentalsSource = _FakeSource()
    entities = source.discover_entities(["ACME"])
    assert entities[0].ticker == "ACME"
    assert entities[0].source_id == "FAKE_SOURCE"
    assert entities[0].source_entity_id == "FAKE-ACME"
    assert entities[0].issuer_id == "FAKE_SOURCE:FAKE-ACME"

    filings = source.discover_filings(entities[0])
    assert filings[0].filing_type == "annual"

    facts = source.retrieve_facts(filings[0])
    assert facts[0].source_concept == "Revenue"
    assert facts[0].source_value == 1_000_000.0

    metadata = source.detect_metadata(entities[0])
    assert metadata.accounting_framework == "ifrs-full"
    assert metadata.reporting_currency == "EUR"


def test_source_entity_ticker_is_optional_identity_is_not_ticker():
    """A source that has no natural ticker at all (e.g. an issuer-only lookup) must still be
    able to construct a valid SourceEntity -- identity comes from source_id/source_entity_id/
    issuer_id, ticker is convenience-only input metadata."""
    entity = SourceEntity(
        source_id="EU_CURRENT",
        source_entity_id="96950032TUYMW11FB530",
        issuer_id="EU_CURRENT:96950032TUYMW11FB530",
        name="Alstom",
    )
    assert entity.ticker is None
    assert entity.issuer_id == "EU_CURRENT:96950032TUYMW11FB530"


def test_source_fact_carries_the_minimum_provenance_fields():
    fact = SourceFact(
        source_id="SEC_XBRL",
        source_entity_id="0000320193",
        source_filing_id="0000320193-25-000001",
        source_concept="Revenues",
        source_period_start="2025-01-01",
        source_period_end="2025-12-31",
        source_currency="USD",
        source_value=391_000_000_000.0,
    )
    assert fact.source_id == "SEC_XBRL"
    assert fact.source_value == 391_000_000_000.0
