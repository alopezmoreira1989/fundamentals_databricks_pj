"""fundamentals_pipeline.sources.base — the FundamentalsSource adapter contract.

Pure Python, no Spark/`dbutils`/network dependency (ADR-0009 §2.2). Every fundamentals source
(SEC EDGAR today; a future European/Canadian/etc. source) implements this same shape so the
canonical mapper and everything downstream of it never needs to know which source a fact came
from.

The adapter stops BEFORE canonical mapping, period classification, or any financial
calculation — those stay in shared, source-agnostic layers (see ``mapping.py`` and the existing
``01__tickers.py``/``21__clean_and_merge.py`` machinery this package does not replace).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@dataclass(frozen=True)
class SourceEntity:
    """One issuer as this source identifies it — e.g. an SEC CIK, a future SEDAR+ issuer
    number, or an LEI. ``source_entity_id`` is opaque outside the adapter that produced it.

    Identity here is ``source_id`` + ``source_entity_id`` + ``issuer_id`` — NOT ``ticker``.
    ``issuer_id`` (built via ``fundamentals_pipeline.identity.make_issuer_id``) is a
    source-qualified identity (``f"{source_id}:{source_entity_id}"``): it does not claim to be
    a universal cross-source issuer identity, and two different sources' `issuer_id` for the
    same real company do not compare equal (see ADR-0010).

    ``ticker`` is optional and exists only because a concrete adapter (`SECXBRLSource`) happens
    to take tickers as its discovery input — it is convenience/input metadata, never treated as
    this entity's identity, and it is NOT globally unique (see `fundamentals_pipeline.identity`
    for why a bare ticker can legitimately collide across markets)."""

    source_id: str
    source_entity_id: str
    issuer_id: str
    name: str
    ticker: str | None = None


@dataclass(frozen=True)
class SourceFiling:
    """One filing/report this source has for an entity — annual or interim, in whatever
    vocabulary this source uses for that (an SEC form type, an ESEF report, ...)."""

    source_entity_id: str
    source_filing_id: str
    filing_type: str
    filed_date: str | None
    period_end: str | None


@dataclass(frozen=True)
class SourceFact:
    """One raw fact extracted from a filing — a source-native concept name and value, not yet
    mapped to a canonical concept. Provenance fields trimmed to what the existing
    ``financials_raw`` schema doesn't already carry (it already has ``scraped_at``/``filed``/
    ``tag_namespace``) — see ADR-0009 §2.2/§10."""

    source_id: str
    source_entity_id: str
    source_filing_id: str
    source_concept: str
    source_period_start: str | None
    source_period_end: str
    source_currency: str | None
    source_value: float | None


@dataclass(frozen=True)
class SourceEntityMetadata:
    """Whatever a source can determine about an entity's own reporting basis."""

    accounting_framework: str | None
    reporting_currency: str | None


@runtime_checkable
class FundamentalsSource(Protocol):
    """The common contract every fundamentals source adapter implements.

    Structural typing (PEP 544) — an adapter satisfies this by having the right methods, not
    by inheriting from anything. See ``SECXBRLSource`` in
    ``10__ingestion/11__fetch_sec_xbrl.py`` for the first (additive, currently uncalled)
    implementation proof, and ``EUCurrentSource`` in
    ``10__ingestion/16__fetch_eu_xbrl.py`` for the first real, called one (Phase 5.1).

    ``@runtime_checkable`` makes ``isinstance(adapter, FundamentalsSource)`` a real, executable
    check (method/attribute presence only — it does not check method signatures) rather than a
    purely static one — see ``tests/test_eu_adapter_protocol_conformance.py``, which uses this
    to prove ``EUCurrentSource`` genuinely satisfies the contract, not just that it was written
    with the same method names by convention.
    """

    source_id: str

    def discover_entities(self, tickers: Sequence[str]) -> Sequence[SourceEntity]:
        """Resolve tickers to this source's own entity identifier."""
        ...

    def discover_filings(self, entity: SourceEntity) -> Sequence[SourceFiling]:
        """List relevant filings for an entity."""
        ...

    def retrieve_facts(self, filing: SourceFiling) -> Sequence[SourceFact]:
        """Extract raw facts from one filing."""
        ...

    def detect_metadata(self, entity: SourceEntity) -> SourceEntityMetadata:
        """``accounting_framework``/``reporting_currency`` for an entity, if determinable."""
        ...
