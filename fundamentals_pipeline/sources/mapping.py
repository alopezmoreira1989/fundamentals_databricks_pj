"""fundamentals_pipeline.sources.mapping — the canonical concept-mapping decision model
(ADR-0009 §2.5).

Two-axis model: ``MappingStatus`` is the accept/reject outcome — only ``ACCEPTED`` produces a
canonical value, everything else is NULL (see ``is_usable``); ``MappingType`` records HOW an
accepted mapping was established. This is a semantic-quality classification, not a statistical
confidence score — the project has no methodology that would make a probability meaningful
here.

Pure Python, no Spark/`dbutils`/network dependency.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class MappingStatus(str, Enum):
    ACCEPTED = "accepted"  # produces a canonical value
    AMBIGUOUS = "ambiguous"  # -> NULL
    INCOMPATIBLE = "incompatible"  # -> NULL
    UNSUPPORTED = "unsupported"  # source has nothing for this concept -> NULL


class MappingType(str, Enum):
    DIRECT = "direct"  # same concept, same accounting meaning
    SEMANTIC_EQUIVALENT = "semantic_equivalent"  # different tag, verified same meaning
    DERIVED = "derived"  # computed from other accepted concepts
    REJECTED = "rejected"  # explicit rejection record (pairs with INCOMPATIBLE/AMBIGUOUS)


@dataclass(frozen=True)
class MappingDecision:
    """One source concept's mapping decision for one canonical concept.

    ``notes`` is required, not optional — a rejection with no reason on record is exactly what
    this model exists to prevent (never "reject silently, forget why").
    """

    canonical_concept: str
    status: MappingStatus
    mapping_type: MappingType | None
    source_concept: str | None
    notes: str

    def __post_init__(self) -> None:
        if not self.notes:
            raise ValueError(
                "MappingDecision.notes is required — never record a mapping decision without a reason"
            )


def is_usable(decision: MappingDecision) -> bool:
    """Only an ACCEPTED-status decision produces a canonical value — everything else is NULL."""
    return decision.status == MappingStatus.ACCEPTED
