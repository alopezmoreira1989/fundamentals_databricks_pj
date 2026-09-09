"""Tests for the XBRL instance-document parser (fundamentals_pipeline/xbrl_instance.py).

The fixture below reproduces the REAL structure confirmed live against SEC EDGAR (2026-08):
Workday Inc's FY2026 10-K (CIK 0001327811, accession 0001327811-26-000014) reports
`dei:EntityCommonStockSharesOutstanding` only per-share-class, dimensioned by
`us-gaap:StatementClassOfStockAxis` — context `c-3` (CommonClassAMember, 210,000,000) and
context `c-4` (CommonClassBMember, 47,000,000), both at instant 2026-03-04. Sum: 257,000,000.
"""

from __future__ import annotations

from fundamentals_pipeline.xbrl_instance import (
    extract_class_of_stock_shares,
    sum_latest_instant,
)

_NS = (
    'xmlns:xbrli="http://www.xbrl.org/2003/instance" '
    'xmlns:dei="http://xbrl.sec.gov/dei/2024" '
    'xmlns:us-gaap="http://fasb.org/us-gaap/2024" '
    'xmlns:xbrldi="http://xbrl.org/2006/xbrldi"'
)


def _context(cid: str, instant: str, axis: str | None, member: str | None) -> str:
    if axis is None:
        segment = ""
    else:
        segment = (
            "<xbrli:segment>"
            f'<xbrldi:explicitMember dimension="{axis}">{member}</xbrldi:explicitMember>'
            "</xbrli:segment>"
        )
    return f"""
    <xbrli:context id="{cid}">
        <xbrli:entity>
            <xbrli:identifier scheme="http://www.sec.gov/CIK">0001327811</xbrli:identifier>
            {segment}
        </xbrli:entity>
        <xbrli:period><xbrli:instant>{instant}</xbrli:instant></xbrli:period>
    </xbrli:context>
    """


def _fact(concept: str, context_ref: str, value: str) -> str:
    return f'<dei:{concept} contextRef="{context_ref}" unitRef="shares">{value}</dei:{concept}>'


def _doc(contexts: str, facts: str) -> bytes:
    return f'<xbrl {_NS}>{contexts}{facts}</xbrl>'.encode()


def test_real_wday_10k_structure_sums_to_the_confirmed_total():
    xml = _doc(
        contexts=(
            _context("c-3", "2026-03-04", "us-gaap:StatementClassOfStockAxis", "us-gaap:CommonClassAMember")
            + _context("c-4", "2026-03-04", "us-gaap:StatementClassOfStockAxis", "us-gaap:CommonClassBMember")
        ),
        facts=(
            _fact("EntityCommonStockSharesOutstanding", "c-3", "210000000")
            + _fact("EntityCommonStockSharesOutstanding", "c-4", "47000000")
        ),
    )
    facts = extract_class_of_stock_shares(xml)
    assert len(facts) == 2
    assert {f.member for f in facts} == {"CommonClassAMember", "CommonClassBMember"}
    assert sum_latest_instant(facts) == ("2026-03-04", 257000000.0)


def test_context_with_unrelated_dimension_is_excluded():
    xml = _doc(
        contexts=_context("c-1", "2026-03-04", "us-gaap:StatementBusinessSegmentsAxis", "us-gaap:SegmentAMember"),
        facts=_fact("EntityCommonStockSharesOutstanding", "c-1", "999000000"),
    )
    assert extract_class_of_stock_shares(xml) == []


def test_context_with_two_dimensions_is_excluded():
    contexts = """
    <xbrli:context id="c-2">
        <xbrli:entity>
            <xbrli:identifier scheme="http://www.sec.gov/CIK">0001327811</xbrli:identifier>
            <xbrli:segment>
                <xbrldi:explicitMember dimension="us-gaap:StatementClassOfStockAxis">us-gaap:CommonClassAMember</xbrldi:explicitMember>
                <xbrldi:explicitMember dimension="us-gaap:StatementBusinessSegmentsAxis">us-gaap:SegmentAMember</xbrldi:explicitMember>
            </xbrli:segment>
        </xbrli:entity>
        <xbrli:period><xbrli:instant>2026-03-04</xbrli:instant></xbrli:period>
    </xbrli:context>
    """
    xml = _doc(contexts=contexts, facts=_fact("EntityCommonStockSharesOutstanding", "c-2", "210000000"))
    assert extract_class_of_stock_shares(xml) == []


def test_no_matching_axis_returns_empty():
    xml = _doc(contexts="", facts="")
    facts = extract_class_of_stock_shares(xml)
    assert facts == []
    assert sum_latest_instant(facts) is None


def test_only_the_latest_instant_is_summed():
    xml = _doc(
        contexts=(
            _context("c-1", "2025-11-01", "us-gaap:StatementClassOfStockAxis", "us-gaap:CommonClassAMember")
            + _context("c-2", "2025-11-01", "us-gaap:StatementClassOfStockAxis", "us-gaap:CommonClassBMember")
            + _context("c-3", "2026-03-04", "us-gaap:StatementClassOfStockAxis", "us-gaap:CommonClassAMember")
            + _context("c-4", "2026-03-04", "us-gaap:StatementClassOfStockAxis", "us-gaap:CommonClassBMember")
        ),
        facts=(
            _fact("EntityCommonStockSharesOutstanding", "c-1", "205000000")
            + _fact("EntityCommonStockSharesOutstanding", "c-2", "48000000")
            + _fact("EntityCommonStockSharesOutstanding", "c-3", "210000000")
            + _fact("EntityCommonStockSharesOutstanding", "c-4", "47000000")
        ),
    )
    facts = extract_class_of_stock_shares(xml)
    assert len(facts) == 4
    assert sum_latest_instant(facts) == ("2026-03-04", 257000000.0)
