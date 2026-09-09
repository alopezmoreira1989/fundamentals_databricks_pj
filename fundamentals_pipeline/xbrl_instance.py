"""Pure-Python XBRL instance-document parsing (no Spark/lxml/network dependency).

Recovers XBRL facts that SEC's convenience JSON APIs (companyfacts / companyconcept) silently
DROP: per-context DIMENSIONED facts. Confirmed live against SEC EDGAR (2026-08): Workday Inc
(CIK 0001327811) reports `dei:EntityCommonStockSharesOutstanding` ONLY per-share-class
(us-gaap:StatementClassOfStockAxis, members CommonClassAMember / CommonClassBMember) since
2018-11-30 -- companyfacts has ZERO rows for this concept from that date on, even though the
filer discloses it normally every 10-K, because SEC's companyfacts/companyconcept endpoints
only expose the undimensioned default-context fact. The real data lives in the filing's own raw
XBRL instance document (`*_htm.xml`), fetched by the caller (11__fetch_sec_xbrl.py owns all
HTTP I/O; this module does none).

WDAY FY2026 10-K (accession 0001327811-26-000014), confirmed via curl:
  context c-3: StatementClassOfStockAxis=CommonClassAMember, instant 2026-03-04, value 210,000,000
  context c-4: StatementClassOfStockAxis=CommonClassBMember, instant 2026-03-04, value  47,000,000
  -> sum at latest instant = 257,000,000 (a sane real total)
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import NamedTuple

_XBRLI_NS = "http://www.xbrl.org/2003/instance"


class ClassOfStockFact(NamedTuple):
    instant: str    # ISO date string, e.g. "2026-03-04"
    axis: str       # local name of the dimension axis, e.g. "StatementClassOfStockAxis"
    member: str     # local name of the class-of-stock member, e.g. "CommonClassAMember"
    value: float


def _parse_contexts(root: ET.Element) -> dict:
    """context id -> {"instant": str|None, "dims": [(axis_localname, member_localname), ...]}.

    ElementTree, Clark-notation namespace handling. `xbrldi:explicitMember`'s `dimension`
    attribute and text are QName strings using the document's bound PREFIX (e.g.
    "us-gaap:StatementClassOfStockAxis"), not a URI -- splitting on ":" and taking the last
    segment is the correct, minimal way to read them without resolving the prefix->URI binding.
    """
    ctx = {}
    for c in root.findall(f"{{{_XBRLI_NS}}}context"):
        period = c.find(f"{{{_XBRLI_NS}}}period")
        instant_el = period.find(f"{{{_XBRLI_NS}}}instant") if period is not None else None
        dims = []
        segment = c.find(f".//{{{_XBRLI_NS}}}segment")
        if segment is not None:
            for member_el in segment:
                axis = (member_el.get("dimension") or "").split(":")[-1]
                member = (member_el.text or "").strip().split(":")[-1]
                if axis and member:
                    dims.append((axis, member))
        ctx[c.get("id")] = {
            "instant": instant_el.text if instant_el is not None else None,
            "dims": dims,
        }
    return ctx


def extract_class_of_stock_shares(
    xml_bytes: bytes,
    concept_localname: str = "EntityCommonStockSharesOutstanding",
) -> list[ClassOfStockFact]:
    """Every `dei:<concept_localname>` fact whose context has EXACTLY ONE dimension member, on
    an axis whose local name contains "classofstock" (case-insensitive -- covers both the
    standard `us-gaap:ClassOfStockAxis` and the `us-gaap:StatementClassOfStockAxis` variant
    confirmed live on WDAY, and any other filer-specific naming, without hardcoding one axis).

    Deliberately narrow: contexts with ZERO or MORE THAN ONE dimension are skipped (v1 does not
    attempt to disentangle a multi-axis context -- rare for this specific cover-page concept,
    safer to skip than guess).
    """
    root = ET.fromstring(xml_bytes)
    ctx = _parse_contexts(root)
    out: list[ClassOfStockFact] = []
    for el in root.iter():
        tag = el.tag
        if not tag.startswith("{"):
            continue
        _, _, local = tag[1:].partition("}")
        if local != concept_localname or el.text is None:
            continue
        c = ctx.get(el.get("contextRef"))
        if c is None or len(c["dims"]) != 1 or c["instant"] is None:
            continue
        axis, member = c["dims"][0]
        if "classofstock" not in axis.lower():
            continue
        try:
            value = float(el.text)
        except (TypeError, ValueError):
            continue
        out.append(ClassOfStockFact(instant=c["instant"], axis=axis, member=member, value=value))
    return out


def sum_latest_instant(facts: list[ClassOfStockFact]) -> tuple[str, float] | None:
    """Group by `instant`, keep the MOST RECENT instant only, sum every member's value at that
    instant. Returns (instant, total) or None if `facts` is empty. Single-axis-per-context is
    already enforced by `extract_class_of_stock_shares`, so this sum cannot double-count a
    member against a second, co-occurring dimension."""
    if not facts:
        return None
    latest = max(f.instant for f in facts)
    total = sum(f.value for f in facts if f.instant == latest)
    return latest, total
