"""fundamentals_pipeline.sources.registry — the source registry (ADR-0009 §2.3).

Configuration, not a database table: a static, git-versioned record of every fundamentals
source this project knows about, whether or not an adapter has been built for it yet. A future
coverage-reporting consumer can read this same static registry directly rather than needing it
duplicated into a relational store. Pure Python, no Spark/`dbutils`/network dependency.

Do NOT add a source here that hasn't been researched the way ADR-0009 §4/§5 research
EU_CURRENT/SEDAR_PLUS — this registry is meant to make future sources addable, not to invent
sources ahead of that research (see the project's own "do not over-engineer" invariant).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class SourceAccessStatus(str, Enum):
    """Whether a source can actually be automated against today — a distinct question from
    whether it has a technical API at all (ADR-0009 §2.3.1). ``SEDAR_PLUS`` has no compliant
    automated path despite existing; ``EU_CURRENT`` does."""

    ACTIVE = "active"  # in production use today
    RESEARCH_ONLY = "research_only"  # researched, no adapter built yet
    MANUAL_ONLY = "manual_only"  # a human can retrieve data; no automated path
    AUTOMATION_RESTRICTED = "automation_restricted"  # technically possible, contractually/legally prohibited
    UNAVAILABLE = "unavailable"  # no usable access at all currently


@dataclass(frozen=True)
class SourceDefinition:
    source_id: str
    jurisdiction: tuple[str, ...]
    accounting_frameworks: tuple[str, ...]
    source_type: str  # "structured_api" | "document_repository" | "aggregator"
    access_status: SourceAccessStatus
    machine_readable: bool
    historical_depth: str  # free text — what's actually verified, not assumed
    notes: str


SOURCE_REGISTRY: dict[str, SourceDefinition] = {
    "SEC_XBRL": SourceDefinition(
        source_id="SEC_XBRL",
        jurisdiction=("US",),
        accounting_frameworks=("us-gaap", "ifrs-full"),
        source_type="structured_api",
        access_status=SourceAccessStatus.ACTIVE,
        machine_readable=True,
        historical_depth="full companyfacts history",
        notes="10 req/s SEC fair-access policy; this pipeline caps at ~8 "
              "(10__ingestion/11__fetch_sec_xbrl.py).",
    ),
    "EU_CURRENT": SourceDefinition(
        source_id="EU_CURRENT",
        jurisdiction=("ES", "FR", "NL", "IT"),
        accounting_frameworks=("ifrs-full",),
        source_type="aggregator",
        access_status=SourceAccessStatus.RESEARCH_ONLY,  # until ADR-0009 §7 Phase 5 ships an adapter
        machine_readable=True,
        historical_depth="ESEF mandate start = FY2020 annual reports (filed 2021+) — annual "
                          "consolidated IFRS statements only, EU-regulated-market issuers only, "
                          "not all European companies.",
        notes="Access path is filings.xbrl.org (XBRL International), an AGGREGATOR of ESEF "
              "filings, not ESEF itself — JSON-API, 'no restrictions on data use' stated in "
              "their own docs. Does not cover all EU countries (Germany, Ireland notably "
              "absent from this aggregator's index — a retrieval gap, not an ESEF scope gap). "
              "See ADR-0009 §4.",
    ),
    "ESAP": SourceDefinition(
        source_id="ESAP",
        jurisdiction=("EU", "EEA"),
        accounting_frameworks=("ifrs-full", "national-gaap"),
        source_type="structured_api",
        access_status=SourceAccessStatus.UNAVAILABLE,  # not public yet
        machine_readable=True,
        historical_depth="none yet — collection starts 2026-07-10",
        notes="Public access legally mandated by 2027-07-10. Technical API spec not yet "
              "published as of this registry entry — re-research before Phase 10, do not "
              "design an adapter against an assumed shape. See ADR-0009 §4.4.",
    ),
    "SEDAR_PLUS": SourceDefinition(
        source_id="SEDAR_PLUS",
        jurisdiction=("CA",),
        accounting_frameworks=("ifrs-full", "aspe"),
        source_type="document_repository",
        access_status=SourceAccessStatus.AUTOMATION_RESTRICTED,
        machine_readable=False,  # PDF-mandated; XBRL voluntary-at-best and largely legacy-only
        historical_depth="n/a — not ingested",
        notes="Terms of Use explicitly prohibit bots/scraping and 'constructing a database' "
              "from the public filings. Do not build an adapter against this entry without a "
              "licensed data-distribution path. See ADR-0009 §5.",
    ),
    # EDINET, COMPANIES_HOUSE, CSRC/SSE/SZSE/HKEX, ASIC/ASX, DART, etc.: not added — architectural
    # requirements only until researched the same way (ADR-0009 §15 / originating brief §15).
}
