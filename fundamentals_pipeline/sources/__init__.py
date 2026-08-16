"""fundamentals_pipeline.sources — the source-agnostic ingestion framework (ADR-0009).

Pure Python, no Spark/`dbutils`/Streamlit/Django dependency, following the same convention
as every other importable module in `fundamentals_pipeline/__init__.py`. Nothing in the
pipeline calls into this package yet — it is the Phase 3 "minimal source-agnostic refactor"
proving the abstraction fits the existing SEC ingestion shape, ahead of a second source
(Phase 5) actually needing it. See `docs/adr/0009-multi-market-fundamentals-ingestion-framework.md`.
"""

from .base import (
    FundamentalsSource,
    SourceEntity,
    SourceEntityMetadata,
    SourceFact,
    SourceFiling,
)
from .eu_admission import (
    EU_ADMISSION_SOURCE,
    AdmissionCandidate,
    AdmissionStatus,
    FirdsVenueRecord,
    PrimaryListingResult,
    RejectionReason,
    apply_esef_eligibility,
    apply_ticker_enrichment,
    build_admission_candidate,
    is_active_venue_record,
    is_equity_cfi,
    make_eu_issuer_id,
    select_primary_listing,
)
from .eu_current import (
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
from .mapping import MappingDecision, MappingStatus, MappingType, is_usable
from .registry import SOURCE_REGISTRY, SourceAccessStatus, SourceDefinition

__all__ = [
    "FundamentalsSource",
    "SourceEntity",
    "SourceEntityMetadata",
    "SourceFact",
    "SourceFiling",
    "MappingDecision",
    "MappingStatus",
    "MappingType",
    "is_usable",
    "SOURCE_REGISTRY",
    "SourceAccessStatus",
    "SourceDefinition",
    "EU_CANONICAL_MAPPING",
    "EU_SOURCE_ID",
    "FilingRejection",
    "entity_from_pilot",
    "extract_source_facts",
    "is_consolidated_fact",
    "is_current_period_fact",
    "map_source_fact_to_canonical",
    "parse_unit_currency",
    "select_filing_for_period",
    "EU_ADMISSION_SOURCE",
    "AdmissionCandidate",
    "AdmissionStatus",
    "FirdsVenueRecord",
    "PrimaryListingResult",
    "RejectionReason",
    "apply_esef_eligibility",
    "apply_ticker_enrichment",
    "build_admission_candidate",
    "is_active_venue_record",
    "is_equity_cfi",
    "make_eu_issuer_id",
    "select_primary_listing",
]
