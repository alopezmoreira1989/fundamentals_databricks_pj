"""fundamentals_pipeline.sources.eu_current — filings.xbrl.org (ESEF/IFRS) adapter logic.

Pure Python, no Spark/`dbutils`/network dependency, matching `sources/base.py`'s own
constraint. Real HTTP calls (the actual `EUCurrentSource` class implementing
`FundamentalsSource`) live in `10__ingestion/16__fetch_eu_xbrl.py`; everything in this module is
the fixture-testable logic that class delegates to: amendment/filing selection, consolidated-
vs-segment discrimination, current-vs-comparative period selection, and the narrow IFRS ->
canonical concept mapping for this pilot's five high-confidence concepts.

Every design decision here is grounded in real `filings.xbrl.org` API/xBRL-JSON responses
fetched live during Phase 5.1 research (2026-08), not assumed — see
`docs/phase5-1-eu-adapter.md` for the evidence each rule is based on.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Literal

from .base import SourceEntity, SourceFact, SourceFiling
from .mapping import MappingDecision, MappingStatus, MappingType

EU_SOURCE_ID = "EU_CURRENT"

_BASE_DIMENSION_KEYS = frozenset({"concept", "entity", "period", "unit"})

FilingRejectionReason = Literal["superseded", "missing_json_url", "has_errors"]


@dataclass(frozen=True)
class FilingRejection:
    """A filing that was considered for one (entity, period_end) group but not selected -- and
    why. `"superseded"` is a normal, expected amendment outcome. `"missing_json_url"`/
    `"has_errors"` mean the filing is genuinely not ingestible -- the caller must surface these
    as real `ingestion_failures` rows, never let them just vanish during selection."""

    fxo_id: str
    reason: FilingRejectionReason


def _filing_is_ingestible(filing: Mapping) -> bool:
    return filing.get("error_count", 0) == 0 and bool(filing.get("json_url"))


def select_filing_for_period(
    filings: Sequence[Mapping],
) -> tuple[dict | None, list[FilingRejection]]:
    """Pick the single filing that represents one (entity, period_end) group.

    All `filings` must already share the same `period_end` -- the caller groups by period_end
    before calling this. Among ingestible candidates (`error_count == 0` and a real `json_url`),
    the one with the latest `processed` timestamp wins; every other ingestible candidate is
    `"superseded"` (confirmed live against two real amendment cases: New Amsterdam Invest's
    `...-NL-0`/`...-NL-1` and Fincantieri's `...-IT-0`/`...-IT-1`, both period_end pairs where
    the later `processed` timestamp is the correct winner). A non-ingestible filing is always
    reported, never silently dropped: if it is the ONLY filing for this period_end, the winner
    is `None` and the caller must treat that as a real failure, not a quietly-missing fiscal
    year (confirmed live: Fincantieri's real FY2025 filing has `error_count=0` but
    `json_url=None`).
    """
    if not filings:
        return None, []

    rejections: list[FilingRejection] = []
    ingestible = []
    for f in filings:
        if _filing_is_ingestible(f):
            ingestible.append(f)
        else:
            reason: FilingRejectionReason = (
                "has_errors" if f.get("error_count", 0) != 0 else "missing_json_url"
            )
            rejections.append(FilingRejection(fxo_id=f["fxo_id"], reason=reason))

    if not ingestible:
        return None, rejections

    winner = max(ingestible, key=lambda f: f["processed"])
    rejections.extend(
        FilingRejection(fxo_id=f["fxo_id"], reason="superseded")
        for f in ingestible
        if f is not winner
    )
    return winner, rejections


def is_consolidated_fact(dimensions: Mapping[str, str]) -> bool:
    """True iff this fact's dimensions are EXACTLY `{concept, entity, period, unit}` -- no extra
    axis/member key. A fact with any additional dimension key is a segment/component/equity
    breakdown, not the consolidated top-line figure, and must be excluded from canonical mapping
    (NULL > a questionable value silently picked from the wrong breakdown). Confirmed live
    against a real dimensional fact: New Amsterdam Invest's `ifrs-full:ComponentsOfEquityAxis`
    breakdown of `ifrs-full:Equity`."""
    keys = set(dimensions.keys())
    return "concept" in keys and keys <= _BASE_DIMENSION_KEYS


def parse_unit_currency(unit: str) -> str | None:
    """`"iso4217:EUR"` -> `"EUR"`. `None` for anything not a BARE iso4217-prefixed currency
    code -- a composite unit like `"iso4217:EUR/xbrli:shares"` (a per-share metric) is not a
    plain currency and also returns `None`, not the garbled remainder."""
    if not unit or not unit.startswith("iso4217:"):
        return None
    code = unit.split(":", 1)[1]
    if not code or "/" in code:
        return None
    return code


def _period_end_component(period: str) -> str:
    """The end-of-period ISO datetime string, for both instant (`"...T00:00:00"`) and duration
    (`"start/end"`) period representations."""
    return period.split("/", 1)[-1]


def is_current_period_fact(period: str, filing_period_end: date) -> bool:
    """True iff this fact's period belongs to the filing's OWN reporting period, not a
    prior-year comparative published in the same file.

    xBRL's instant convention is exclusive-end: a balance-sheet snapshot "as of" the filing's
    `period_end` is tagged with `period = period_end + 1 day`. Confirmed live: FCC's FY2024
    filing (`period_end=2024-12-31`) tags its Assets/Cash instant facts with
    `period="2025-01-01T00:00:00"`, exactly `period_end + 1 day`. This holds for both instant
    (stock) and duration (flow) facts' END component and needs no fiscal-year-length assumption,
    so it works unchanged for a non-calendar fiscal year (e.g. Alstom's `period_end=2025-03-31`).
    Also confirmed live to correctly reject a same-file prior-year comparative: FCC's FY2024
    filing carries BOTH the current year's `ifrs-full:Revenue` fact (period ending
    `2025-01-01T00:00:00`) and the FY2023 comparative (period ending `2024-01-01T00:00:00`)
    under the identical concept -- only the former passes this check.
    """
    expected_end = (filing_period_end + timedelta(days=1)).isoformat()
    return _period_end_component(period).startswith(expected_end)


def _parse_period_start(period: str) -> str | None:
    """The ISO date (`YYYY-MM-DD`) a duration fact's period starts on, or `None` for an instant
    fact (no `"/"` separator)."""
    if "/" not in period:
        return None
    return period.split("/", 1)[0][:10]


def extract_source_facts(
    raw_json: Mapping,
    filing: SourceFiling,
    source_id: str,
) -> list[SourceFact]:
    """Convert one filing's raw xBRL-JSON facts into `SourceFact` objects -- filtered to
    consolidated, current-reporting-period facts only (see `is_consolidated_fact`/
    `is_current_period_fact`). `filing.source_entity_id` is the LEI; xBRL-JSON's own
    `entity` dimension is always `"scheme:<LEI>"` (confirmed live against every pilot filing —
    `documentInfo.namespaces.scheme` is the ISO 17442 LEI scheme URI) -- a fact whose `entity`
    doesn't match is excluded as a cheap sanity check, not evidence of anything more elaborate.

    Every returned fact's `source_currency` is populated (never silently dropped) even though
    the current `financials_raw`/`financials` schema has nowhere to persist it per-row -- see
    `docs/phase5-1-eu-adapter.md` for why that Delta-write-time drop is a documented, temporary
    limitation, not a design position that currency doesn't matter.
    """
    if filing.period_end is None:
        return []
    filing_period_end = date.fromisoformat(filing.period_end)
    expected_entity = f"scheme:{filing.source_entity_id}"

    out: list[SourceFact] = []
    for fact in raw_json.get("facts", {}).values():
        dims = fact.get("dimensions", {})
        if not is_consolidated_fact(dims):
            continue
        if dims.get("entity") != expected_entity:
            continue
        period = dims.get("period", "")
        if not period or not is_current_period_fact(period, filing_period_end):
            continue
        value = fact.get("value")
        if value is None:
            continue
        try:
            source_value = float(value)
        except (TypeError, ValueError):
            continue
        out.append(
            SourceFact(
                source_id=source_id,
                source_entity_id=filing.source_entity_id,
                source_filing_id=filing.source_filing_id,
                source_concept=dims.get("concept", ""),
                source_period_start=_parse_period_start(period),
                source_period_end=_period_end_component(period)[:10],
                source_currency=parse_unit_currency(dims.get("unit", "")),
                source_value=source_value,
            )
        )
    return out


def entity_from_pilot(source_id: str, ticker: str, lei: str, name: str) -> SourceEntity:
    """Build the `SourceEntity` for one pilot issuer from its already-verified identity --
    no fuzzy matching, no ticker-derived lookup (per ADR-0009/ADR-0010: `filings.xbrl.org`'s own
    `/api/entities` provides no ticker/exchange field, so a pilot's ticker/LEI pairing is a
    verified, curated fact, not something this adapter derives at runtime)."""
    from ..identity import make_issuer_id

    return SourceEntity(
        source_id=source_id,
        source_entity_id=lei,
        issuer_id=make_issuer_id(source_id, lei),
        name=name,
        ticker=ticker,
    )


# ── Canonical mapping (Phase 5.1 pilot -- five high-confidence IFRS concepts only) ──────────
# These ifrs-full tag strings duplicate 00__config/01__tickers.py's IFRS_FALLBACK_TAGS values --
# that file is a notebook-only `%run` global, unreachable from this importable, non-Spark
# package. 01__tickers.py remains the single source of truth for the tag strings themselves;
# this is a small, deliberately-documented duplication (per Phase 5.1's own plan), not an
# independent taxonomy mapping invented in parallel.
EU_CANONICAL_MAPPING: dict[str, MappingDecision] = {
    "Revenue": MappingDecision(
        canonical_concept="Revenue",
        status=MappingStatus.ACCEPTED,
        mapping_type=MappingType.DIRECT,
        source_concept="ifrs-full:Revenue",
        notes="Verified live 2026-08 against FCC's real FY2024 xBRL-JSON filing (fact-105, "
        "value 9,071,416,000 EUR, consolidated, non-dimensional). Matches 01__tickers.py "
        "IFRS_FALLBACK_TAGS['Revenue'].",
    ),
    "Net Income": MappingDecision(
        canonical_concept="Net Income",
        status=MappingStatus.ACCEPTED,
        mapping_type=MappingType.DIRECT,
        source_concept="ifrs-full:ProfitLossAttributableToOwnersOfParent",
        notes="Matches 01__tickers.py IFRS_FALLBACK_TAGS['Net Income'] -- the parent-"
        "attributable profit figure, the same 'Net Income' definition SEC filers use.",
    ),
    "Net Income (incl NCI)": MappingDecision(
        canonical_concept="Net Income (incl NCI)",
        status=MappingStatus.ACCEPTED,
        mapping_type=MappingType.DIRECT,
        source_concept="ifrs-full:ProfitLoss",
        notes="Verified live 2026-08 against FCC's real FY2024 filing (fact-145, value "
        "567,584,000 EUR). Matches 01__tickers.py IFRS_FALLBACK_TAGS['Net Income (incl NCI)'].",
    ),
    "Total Assets": MappingDecision(
        canonical_concept="Total Assets",
        status=MappingStatus.ACCEPTED,
        mapping_type=MappingType.DIRECT,
        source_concept="ifrs-full:Assets",
        notes="Verified live 2026-08 against FCC's real FY2024 filing (fact-45, value "
        "14,235,959,000 EUR). Matches 01__tickers.py IFRS_FALLBACK_TAGS['Total Assets'].",
    ),
    "Cash & Equivalents": MappingDecision(
        canonical_concept="Cash & Equivalents",
        status=MappingStatus.ACCEPTED,
        mapping_type=MappingType.DIRECT,
        source_concept="ifrs-full:CashAndCashEquivalents",
        notes="Verified live 2026-08 against FCC's real FY2024 filing (fact-43, value "
        "1,849,617,000 EUR). Matches 01__tickers.py IFRS_FALLBACK_TAGS['Cash & Equivalents'].",
    ),
}

_SOURCE_CONCEPT_TO_DECISION = {d.source_concept: d for d in EU_CANONICAL_MAPPING.values()}


def map_source_fact_to_canonical(source_concept: str) -> MappingDecision | None:
    """Look up this source concept's canonical mapping decision. `None` (excluded outright, not
    NULL-valued-and-kept) for anything outside the five high-confidence pilot concepts --
    deliberately narrow, per "start with high-confidence concepts, don't build a giant IFRS
    taxonomy mapping"."""
    return _SOURCE_CONCEPT_TO_DECISION.get(source_concept)
