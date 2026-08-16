"""Tests for the Phase 5.0 / ADR-0010 issuer/listing identity primitives
(fundamentals_pipeline.identity.make_issuer_id / make_listing_id).

Includes real-data tests against the four verified European pilot issuers (Spain/France/
Netherlands/Italy) researched and confirmed live against filings.xbrl.org this session --
proving the model represents them correctly without writing anything into config.tickers or
building a European adapter (both explicitly out of scope for this pass).
"""

from __future__ import annotations

from fundamentals_pipeline.identity import make_issuer_id, make_listing_id

# Real, verified pilot data (Phase 5.0/5.1 research) -- not placeholders.
_PILOT_ISSUERS = {
    "FCC": {"mic": "XMAD", "lei": "95980020140005178328"},   # Fomento de Construcciones y Contratas
    "ALO": {"mic": "XPAR", "lei": "96950032TUYMW11FB530"},   # Alstom
    "NAI":  {"mic": "XAMS", "lei": "724500JXEXUGEATP5L52"},  # New Amsterdam Invest N.V.
    "FCT": {"mic": "MTAA", "lei": "8156005BDF49128B6239"},   # Fincantieri S.p.A.
}

# A representative sample of real existing US listing_ids -- collision check baseline.
_REPRESENTATIVE_US_LISTINGS = {"XNAS:AAPL", "XNAS:TSLA", "XNYS:GOOGL"}


def test_make_issuer_id_format():
    assert make_issuer_id("SEC_XBRL", "0000320193") == "SEC_XBRL:0000320193"


def test_make_listing_id_format_and_uppercasing():
    assert make_listing_id("xnas", "aapl") == "XNAS:AAPL"
    assert make_listing_id("XMAD", "FCC") == "XMAD:FCC"


def test_make_issuer_id_is_source_qualified_not_universal():
    """Two different sources' issuer_id for the same raw id string must NOT compare equal --
    issuer_id is source-qualified, not a claim of a universal cross-source identity."""
    sec_side = make_issuer_id("SEC_XBRL", "123")
    eu_side = make_issuer_id("EU_CURRENT", "123")
    assert sec_side != eu_side


def test_ticker_collision_separated_by_listing_id():
    """Test A (repo owner's spec): XNAS:ABC and XPAR:ABC must not collide -- they are distinct
    listing_ids sharing only the raw ticker string, which is exactly what listing_id exists to
    prevent from being treated as identity."""
    us_listing = make_listing_id("XNAS", "ABC")
    eu_listing = make_listing_id("XPAR", "ABC")
    assert us_listing != eu_listing
    assert {us_listing, eu_listing} == {"XNAS:ABC", "XPAR:ABC"}


def test_same_issuer_multiple_listings():
    """Test B (repo owner's spec): one issuer_id may legitimately be associated with more than
    one listing_id (e.g. a dual-listed company) -- listing_id and issuer_id are independent
    axes, not a 1:1 pair."""
    issuer_id = make_issuer_id("SEC_XBRL", "0001594805")  # hypothetical dual-listed issuer
    listing_a = make_listing_id("XNAS", "XYZ")
    listing_b = make_listing_id("XPAR", "XYZ")
    assert listing_a != listing_b
    # Both listings would carry the SAME issuer_id at the call site (this module makes no
    # claim about that association -- it only proves the two id spaces don't collide).
    assert issuer_id == "SEC_XBRL:0001594805"


def test_pilot_issuers_real_listing_ids():
    """Validates the model against the four real, verified EU pilot issuers researched this
    session -- literal fixtures only, no config.tickers write, no adapter."""
    expected = {
        "FCC": "XMAD:FCC",
        "ALO": "XPAR:ALO",
        "NAI": "XAMS:NAI",
        "FCT": "MTAA:FCT",
    }
    for ticker, data in _PILOT_ISSUERS.items():
        listing_id = make_listing_id(data["mic"], ticker)
        assert listing_id == expected[ticker]


def test_pilot_issuers_real_issuer_ids():
    for data in _PILOT_ISSUERS.values():
        issuer_id = make_issuer_id("EU_CURRENT", data["lei"])
        assert issuer_id == f"EU_CURRENT:{data['lei']}"
        assert issuer_id.startswith("EU_CURRENT:")


def test_pilot_listing_ids_do_not_collide_with_representative_us_listings():
    pilot_listing_ids = {
        make_listing_id(data["mic"], ticker) for ticker, data in _PILOT_ISSUERS.items()
    }
    assert pilot_listing_ids.isdisjoint(_REPRESENTATIVE_US_LISTINGS)
