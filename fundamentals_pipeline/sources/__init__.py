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
]
