"""Tests for the source registry (fundamentals_pipeline/sources/registry.py)."""

from __future__ import annotations

from fundamentals_pipeline.sources.registry import SOURCE_REGISTRY, SourceAccessStatus


def test_registry_has_exactly_the_four_researched_sources():
    assert set(SOURCE_REGISTRY) == {"SEC_XBRL", "EU_CURRENT", "ESAP", "SEDAR_PLUS"}


def test_sec_xbrl_is_active():
    assert SOURCE_REGISTRY["SEC_XBRL"].access_status == SourceAccessStatus.ACTIVE
    assert SOURCE_REGISTRY["SEC_XBRL"].jurisdiction == ("US",)


def test_sedar_plus_is_automation_restricted_not_active():
    # Regression guard: SEDAR+'s Terms of Use prohibit automated access (ADR-0009 §5) — this
    # must never silently flip to ACTIVE without a conscious, reviewed decision.
    entry = SOURCE_REGISTRY["SEDAR_PLUS"]
    assert entry.access_status == SourceAccessStatus.AUTOMATION_RESTRICTED
    assert entry.machine_readable is False


def test_eu_current_is_active_after_the_phase51_adapter_shipped():
    # Phase 5.1: EUCurrentSource shipped and was validated via a real Databricks smoke test
    # (docs/phase5-1-eu-adapter.md) -- flipped from RESEARCH_ONLY to ACTIVE, per the registry's
    # own prior comment stating that was the trigger condition.
    entry = SOURCE_REGISTRY["EU_CURRENT"]
    assert entry.access_status == SourceAccessStatus.ACTIVE
    assert entry.machine_readable is True
    assert "ES" in entry.jurisdiction and "FR" in entry.jurisdiction


def test_esap_is_unavailable_until_public_access_opens():
    entry = SOURCE_REGISTRY["ESAP"]
    assert entry.access_status == SourceAccessStatus.UNAVAILABLE


def test_every_entry_carries_notes_and_a_source_type():
    for source_id, entry in SOURCE_REGISTRY.items():
        assert entry.source_id == source_id
        assert entry.notes, f"{source_id} has no notes"
        assert entry.source_type in ("structured_api", "document_repository", "aggregator")
