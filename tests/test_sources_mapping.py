"""Tests for the canonical concept-mapping decision model (fundamentals_pipeline/sources/mapping.py)."""

from __future__ import annotations

import pytest

from fundamentals_pipeline.sources.mapping import (
    MappingDecision,
    MappingStatus,
    MappingType,
    is_usable,
)


def test_accepted_direct_mapping_is_usable():
    decision = MappingDecision(
        canonical_concept="Revenue",
        status=MappingStatus.ACCEPTED,
        mapping_type=MappingType.DIRECT,
        source_concept="ifrs-full:Revenue",
        notes="Direct IFRS <-> canonical equivalent, core statement line.",
    )
    assert is_usable(decision) is True


@pytest.mark.parametrize("status", [MappingStatus.AMBIGUOUS, MappingStatus.INCOMPATIBLE, MappingStatus.UNSUPPORTED])
def test_only_accepted_status_is_usable(status):
    decision = MappingDecision(
        canonical_concept="Operating Income",
        status=status,
        mapping_type=MappingType.REJECTED if status != MappingStatus.UNSUPPORTED else None,
        source_concept=None,
        notes="IFRS has no mandated definition of 'operating' -- heterogeneous presentation.",
    )
    assert is_usable(decision) is False


def test_notes_are_required_never_reject_silently():
    with pytest.raises(ValueError):
        MappingDecision(
            canonical_concept="Gross Profit",
            status=MappingStatus.INCOMPATIBLE,
            mapping_type=MappingType.REJECTED,
            source_concept=None,
            notes="",
        )


def test_semantic_equivalent_and_derived_types_are_distinct_from_direct():
    assert MappingType.DIRECT != MappingType.SEMANTIC_EQUIVALENT != MappingType.DERIVED
