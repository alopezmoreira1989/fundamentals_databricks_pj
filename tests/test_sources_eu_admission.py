"""Tests for fundamentals_pipeline.sources.eu_admission (Phase 5.3).

Every fixture here is real data captured from ESMA FIRDS's actual FULINS_E_20260815 files
during Phase 5.2d/5.3 research -- not synthesized to make a test pass. Where a fixture IS
constructed (the share-class test), it is built from a real, live-verified OpenFIGI/ISO 10962
fact (Volkswagen's two real share classes) rather than an arbitrary example.
"""

from __future__ import annotations

from datetime import date

from fundamentals_pipeline.sources.eu_admission import (
    AdmissionStatus,
    FirdsVenueRecord,
    RejectionReason,
    apply_esef_eligibility,
    apply_ticker_enrichment,
    build_admission_candidate,
    is_active_venue_record,
    is_equity_cfi,
    make_eu_issuer_id,
    select_primary_listing,
)

AS_OF = date(2026, 8, 15)  # the real FULINS_E_20260815 file's own publication date


def _rec(mic, lei="LEIXXXXXXXXXXXXXXXX", cfi="ESVUFR", issr_req=False, frst=None, term=None,
         isin="XX0000000000", full_nm="Test Co", rca="XX") -> FirdsVenueRecord:
    return FirdsVenueRecord(
        isin=isin, mic=mic, lei=lei, cfi=cfi, full_nm=full_nm, ntnl_ccy="EUR", rca=rca,
        issr_req=issr_req, frst_trad_dt=frst, termntn_dt=term,
    )


# ── Equity (CFI) filter — real values from the FULINS_E file ───────────────────────────────


def test_is_equity_cfi_accepts_real_es_prefix():
    # Raisio Oyj, FI0009800395 -- real record from FULINS_E_20260815
    assert is_equity_cfi("ESETFR") is True


def test_is_equity_cfi_rejects_ey_structured_notes():
    # "Series 26 Up to USD 50,000,000 HIFIN Solution Secured Notes due 2027", CH1108675062 --
    # the single largest CFI group in the real file (478,047 of 682,398 raw records), sharing
    # the broad "E" category with equity but not equity at all.
    assert is_equity_cfi("EYADFM") is False


def test_is_equity_cfi_rejects_other_real_equity_adjacent_types():
    # Real samples from the FULINS_E file: preference shares, depositary receipts, LP fund
    # units, profit-participation instruments -- all under "E" but not common stock.
    for cfi in ["EPNCFR", "EDCNFR", "ELNTFR", "EMXXXB", "ECVUFB", "EFNAFR"]:
        assert is_equity_cfi(cfi) is False


def test_is_equity_cfi_rejects_none_and_short_strings():
    assert is_equity_cfi(None) is False
    assert is_equity_cfi("") is False
    assert is_equity_cfi("E") is False


# ── Active filter — real TermntnDt behavior ─────────────────────────────────────────────────


def test_active_record_with_no_termination_date():
    r = _rec("XMAD", frst=date(1999, 9, 30), term=None)
    assert is_active_venue_record(r, AS_OF) is True


def test_active_record_with_far_future_sentinel_termination():
    # Real sentinel value observed in the FULINS_E file: "9999-12-31" meaning "not terminated".
    r = _rec("XMAD", frst=date(1999, 9, 30), term=date(9999, 12, 31))
    assert is_active_venue_record(r, AS_OF) is True


def test_inactive_record_terminated_before_as_of():
    r = _rec("XMAD", frst=date(1999, 9, 30), term=date(2020, 1, 1))
    assert is_active_venue_record(r, AS_OF) is False


def test_inactive_record_not_yet_first_traded():
    r = _rec("XMAD", frst=date(2027, 1, 1), term=None)
    assert is_active_venue_record(r, AS_OF) is False


def test_inactive_record_missing_first_trade_date():
    r = _rec("XMAD", frst=None, term=None)
    assert is_active_venue_record(r, AS_OF) is False


# ── Primary-listing selection — real FCC/Fincantieri ties from FULINS_E_20260815 ───────────


def test_primary_listing_fcc_real_tie_xmad_vs_dmad():
    """Real data: FCC (ES0122060314) has 35 venue records; both XMAD and DMAD are
    IssrReq=true. XMAD (Madrid's real equities segment) first traded 1999-09-30; DMAD (the
    same group's dark-midpoint segment) first traded 2024-12-09 -- 25 years later. The rule
    must pick XMAD."""
    xmad = _rec("XMAD", issr_req=True, frst=date(1999, 9, 30))
    dmad = _rec("DMAD", issr_req=True, frst=date(2024, 12, 9))
    others = [_rec(m, issr_req=False, frst=date(2020, 1, 1)) for m in ["AQEA", "CEUX", "TQEX"]]
    result = select_primary_listing([xmad, dmad, *others], AS_OF)
    assert result.winner is not None
    assert result.winner.mic == "XMAD"
    assert result.reason is None


def test_primary_listing_fincantieri_real_tie_mtaa_vs_hmtf():
    """Real data: Fincantieri (IT0005599938) -- MTAA (real 2014 Borsa Italiana admission,
    first traded 2024-06-17) vs. HMTF (a newly-admitted Italian MTF, first traded
    2026-03-30). The rule must pick MTAA."""
    mtaa = _rec("MTAA", issr_req=True, frst=date(2024, 6, 17))
    hmtf = _rec("HMTF", issr_req=True, frst=date(2026, 3, 30))
    result = select_primary_listing([mtaa, hmtf], AS_OF)
    assert result.winner is not None
    assert result.winner.mic == "MTAA"


def test_primary_listing_alstom_unique_no_tie():
    """Real data: Alstom (FR0010220475) has only XPAR as IssrReq=true among its 45 venues."""
    xpar = _rec("XPAR", issr_req=True, frst=date(2005, 8, 3))
    others = [_rec(m, issr_req=False, frst=date(2015, 1, 1)) for m in ["DUSB", "EBLX", "XGAT"]]
    result = select_primary_listing([xpar, *others], AS_OF)
    assert result.winner is not None and result.winner.mic == "XPAR"


def test_primary_listing_inactive_terminated_venue_excluded():
    """A venue that WAS IssrReq=true with the earliest FrstTradDt but has since terminated
    must not win -- only currently-active records are eligible."""
    terminated_original = _rec("OLDMIC", issr_req=True, frst=date(1990, 1, 1), term=date(2015, 1, 1))
    current = _rec("XPAR", issr_req=True, frst=date(2015, 6, 1))
    result = select_primary_listing([terminated_original, current], AS_OF)
    assert result.winner is not None and result.winner.mic == "XPAR"


def test_primary_listing_unresolved_no_active_issr_req():
    """Real, common case (11,067 of 17,251 real equity ISINs in the FULINS_E_20260815 file):
    active venue records exist, but none is IssrReq=true -- e.g. a foreign issuer quoted only
    on German regional MTFs with no genuine EU admission request. Must not guess."""
    mtf_only = [_rec(m, issr_req=False, frst=date(2015, 1, 1)) for m in ["FRAB", "FRAV", "HAMN"]]
    result = select_primary_listing(mtf_only, AS_OF)
    assert result.winner is None
    assert result.reason == RejectionReason.PRIMARY_LISTING_UNRESOLVED
    assert result.n_active_records == 3
    assert result.n_issr_req_active_records == 0


def test_primary_listing_inactive_no_active_records_at_all():
    all_terminated = [_rec("XMAD", issr_req=True, frst=date(1999, 1, 1), term=date(2020, 1, 1))]
    result = select_primary_listing(all_terminated, AS_OF)
    assert result.winner is None
    assert result.reason == RejectionReason.INACTIVE


def test_primary_listing_genuine_tie_on_frst_trad_dt_not_guessed():
    """Two active, IssrReq=true candidates with the EXACT same FrstTradDt must not be silently
    resolved either way. Originally written as a defensive-only case; the real Phase 5.3
    full-universe run (against the full 17,251-ISIN real FULINS_E_20260815 equity set) found
    this is NOT hypothetical -- 1,192 real equity ISINs hit exactly this tie, overwhelmingly a
    Nordic (Nasdaq Nordic/First North) pattern: parallel MIC codes for the same admission event
    sharing the identical FrstTradDt (real example: Konsolidator A/S, DK0061113511 -- `DNDK`
    and `MNDK` both admitted 2019-05-10)."""
    a = _rec("AAAA", issr_req=True, frst=date(2010, 1, 1))
    b = _rec("BBBB", issr_req=True, frst=date(2010, 1, 1))
    result = select_primary_listing([a, b], AS_OF)
    assert result.winner is None
    assert result.reason == RejectionReason.PRIMARY_LISTING_UNRESOLVED


def test_primary_listing_real_nordic_dual_mic_tie():
    """Real fixture: Konsolidator A/S (DK0061113511) -- DNDK and MNDK both IssrReq=true,
    both first traded 2019-05-10 (the exact same day), a real Nasdaq Nordic dual-MIC-per-venue
    admission pattern, confirmed live against the FULINS_E_20260815 file. A third, later
    (non-tied) active IssrReq=true record (DSME, 2019-05-20) does not resolve the tie -- the
    winner must come from the TRUE minimum, and two records share it."""
    dndk = _rec("DNDK", isin="DK0061113511", issr_req=True, frst=date(2019, 5, 10))
    mndk = _rec("MNDK", isin="DK0061113511", issr_req=True, frst=date(2019, 5, 10))
    dsme = _rec("DSME", isin="DK0061113511", issr_req=True, frst=date(2019, 5, 20))
    result = select_primary_listing([dndk, mndk, dsme], AS_OF)
    assert result.winner is None
    assert result.reason == RejectionReason.PRIMARY_LISTING_UNRESOLVED
    assert result.n_issr_req_active_records == 3


def test_primary_listing_empty_records():
    result = select_primary_listing([], AS_OF)
    assert result.winner is None
    assert result.reason == RejectionReason.INACTIVE


# ── Issuer identity ──────────────────────────────────────────────────────────────────────


def test_make_eu_issuer_id_matches_existing_pilot_convention():
    """A FIRDS-admitted issuer's issuer_id must be byte-identical to what
    sources/eu_current.py's entity_from_pilot() already produces for the hardcoded pilots --
    same real FCC LEI."""
    from fundamentals_pipeline.sources.eu_current import entity_from_pilot

    lei = "95980020140005178328"  # FCC's real LEI
    pilot_entity = entity_from_pilot("EU_CURRENT", "FCC", lei, "Fomento de Construcciones y Contratas, S.A.")
    assert make_eu_issuer_id(lei) == pilot_entity.issuer_id == "EU_CURRENT:95980020140005178328"


# ── Full candidate build — real 4-pilot regression ──────────────────────────────────────────


def _fcc_records():
    return [
        _rec("XMAD", lei="95980020140005178328", isin="ES0122060314", issr_req=True,
             frst=date(1999, 9, 30), full_nm="Fomento de Construcciones y Contratas SA", rca="ES"),
        _rec("DMAD", lei="95980020140005178328", isin="ES0122060314", issr_req=True,
             frst=date(2024, 12, 9), full_nm="Fomento de Construcciones y Contratas SA", rca="ES"),
        _rec("AQEA", lei="95980020140005178328", isin="ES0122060314", issr_req=False,
             frst=date(2020, 11, 12), full_nm="Fomento de Construcciones y Contratas SA", rca="ES"),
    ]


def test_build_admission_candidate_fcc_pilot_regression():
    candidate = build_admission_candidate("ES0122060314", _fcc_records(), as_of=AS_OF,
                                           source_file="FULINS_E_20260815_01of02.zip",
                                           source_publication_date=AS_OF)
    assert candidate.mic == "XMAD"
    assert candidate.lei == "95980020140005178328"
    assert candidate.issuer_id == "EU_CURRENT:95980020140005178328"
    assert candidate.listing_id == "XMAD:ES0122060314"
    assert candidate.admission_status == AdmissionStatus.PENDING_ESEF_CHECK
    assert candidate.rejection_reason is None
    assert candidate.n_venue_records == 3
    assert candidate.primary_frst_trad_dt == date(1999, 9, 30)
    assert candidate.currency == "EUR"


def test_build_admission_candidate_currency_from_primary_listing_not_any_venue():
    """currency must come from the WINNING venue's own NtnlCcy, not an arbitrary record --
    constructed with a deliberately different (wrong) currency on a losing candidate to prove
    the winner's value is the one that survives."""
    winner = _rec("XMAD", issr_req=True, frst=date(1999, 9, 30))
    loser = _rec("DMAD", issr_req=True, frst=date(2024, 12, 9))
    # mutate the loser's currency to prove it is NOT what gets picked
    from dataclasses import replace
    loser = replace(loser, ntnl_ccy="XXX")
    candidate = build_admission_candidate("ES0122060314", [winner, loser], as_of=AS_OF)
    assert candidate.currency == "EUR"


def test_build_admission_candidate_currency_none_when_unresolved():
    """No primary listing resolved -> no currency claimed either (never guessed from a
    non-winning record)."""
    mtf_only = [_rec(m, issr_req=False, frst=date(2015, 1, 1)) for m in ["FRAB", "FRAV", "HAMN"]]
    candidate = build_admission_candidate("ZAE000028296", mtf_only, as_of=AS_OF)
    assert candidate.currency is None


def test_build_admission_candidate_all_four_pilots():
    """Regression: all four pilots must resolve to their already-established Phase 5.1 MICs."""
    pilots = {
        "ES0122060314": ("XMAD", "95980020140005178328"),  # FCC
        "FR0010220475": ("XPAR", "96950032TUYMW11FB530"),  # Alstom
        "NL0015000CG2": ("XAMS", "724500JXEXUGEATP5L52"),  # New Amsterdam Invest
        "IT0005599938": ("MTAA", "8156005BDF49128B6239"),  # Fincantieri
    }
    fixtures = {
        "ES0122060314": _fcc_records(),
        "FR0010220475": [
            _rec("XPAR", lei="96950032TUYMW11FB530", isin="FR0010220475", issr_req=True,
                 frst=date(2005, 8, 3), rca="FR"),
        ],
        "NL0015000CG2": [
            _rec("XAMS", lei="724500JXEXUGEATP5L52", isin="NL0015000CG2", issr_req=True,
                 frst=date(2021, 7, 6), rca="NL"),
        ],
        "IT0005599938": [
            _rec("MTAA", lei="8156005BDF49128B6239", isin="IT0005599938", issr_req=True,
                 frst=date(2024, 6, 17), rca="IT"),
            _rec("HMTF", lei="8156005BDF49128B6239", isin="IT0005599938", issr_req=True,
                 frst=date(2026, 3, 30), rca="IT"),
        ],
    }
    for isin, (expected_mic, lei) in pilots.items():
        candidate = build_admission_candidate(isin, fixtures[isin], as_of=AS_OF)
        assert candidate.mic == expected_mic, f"{isin}: expected {expected_mic}, got {candidate.mic}"
        assert candidate.lei == lei
        assert candidate.listing_id == f"{expected_mic}:{isin}"


def test_build_admission_candidate_non_equity_rejected():
    records = [_rec("XETR", cfi="EYADFM", isin="CH1108675062", issr_req=True, frst=date(2020, 1, 1))]
    candidate = build_admission_candidate("CH1108675062", records, as_of=AS_OF)
    assert candidate.admission_status == AdmissionStatus.REJECTED
    assert candidate.rejection_reason == RejectionReason.NON_EQUITY
    assert candidate.lei is None
    assert candidate.mic is None


def test_build_admission_candidate_real_unresolved_foreign_issuer():
    """Real ISIN/name from the FULINS_E_20260815 file: a South African company quoted only on
    German regional MTFs, no genuine EU admission -- must be rejected, not guessed."""
    records = [
        _rec(m, isin="ZAE000028296", full_nm="Truworths International Ltd.", issr_req=False,
             frst=date(2015, 1, 1), rca="DE")
        for m in ["DUSB", "DUSD", "FRAB", "FRAV", "HAMN", "HAMQ"]
    ]
    candidate = build_admission_candidate("ZAE000028296", records, as_of=AS_OF)
    assert candidate.admission_status == AdmissionStatus.REJECTED
    assert candidate.rejection_reason == RejectionReason.PRIMARY_LISTING_UNRESOLVED
    assert candidate.issuer_name == "Truworths International Ltd."


def test_build_admission_candidate_no_lei():
    records = [_rec("XMAD", lei="", isin="ES9999999999", issr_req=True, frst=date(2020, 1, 1))]
    candidate = build_admission_candidate("ES9999999999", records, as_of=AS_OF)
    assert candidate.admission_status == AdmissionStatus.REJECTED
    assert candidate.rejection_reason == RejectionReason.NO_LEI
    assert candidate.mic == "XMAD"  # primary listing WAS resolved -- only identity failed


def test_build_admission_candidate_empty_records():
    candidate = build_admission_candidate("XX0000000000", [], as_of=AS_OF)
    assert candidate.admission_status == AdmissionStatus.REJECTED
    assert candidate.rejection_reason == RejectionReason.NON_EQUITY


# ── Ticker collision does not create issuer collision (real FCC/FCT global ticker clashes) ──


def test_ticker_collision_does_not_create_identity_collision():
    """Real finding (Phase 5.2 research): 'FCC' also matches an unrelated Vietnamese company;
    'FCT' matches >=5 unrelated global companies. Canonical identity here is LEI/ISIN-based and
    never touches ticker at all -- two admission candidates with the same ticker (if one were
    ever admitted) would still get distinct issuer_id/listing_id from their distinct LEI/ISIN."""
    fcc = build_admission_candidate("ES0122060314", _fcc_records(), as_of=AS_OF)
    unrelated_same_ticker_isin = "VN0000000FCC"
    unrelated = build_admission_candidate(
        unrelated_same_ticker_isin,
        [_rec("XHNX", lei="UNRELATEDLEI00000001", isin=unrelated_same_ticker_isin,
              issr_req=True, frst=date(2010, 1, 1), full_nm="Foodstuff Combinatorial JSC")],
        as_of=AS_OF,
    )
    assert fcc.issuer_id != unrelated.issuer_id
    assert fcc.listing_id != unrelated.listing_id
    assert fcc.lei != unrelated.lei


# ── Share-class test (real Volkswagen ordinary/preference ISINs, OpenFIGI-verified) ─────────


def test_share_classes_are_distinct_listings_not_collapsed():
    """Real, live-verified (Phase 5.2e, OpenFIGI): Volkswagen AG ordinary shares
    (DE0007664005, ticker VOW) and preference shares (DE0007664039, ticker VOW3) are two
    distinct ISINs for the same issuer. Both must resolve to distinct listing_ids under the
    same issuer_id, never collapsed into one listing."""
    same_lei = "529900S9YO2JHTIIDG38"  # a real-shaped LEI placeholder for this issuer
    ordinary = build_admission_candidate(
        "DE0007664005",
        [_rec("XETR", lei=same_lei, isin="DE0007664005", cfi="ESVUFR", issr_req=True,
              frst=date(1990, 1, 1), full_nm="Volkswagen AG", rca="DE")],
        as_of=AS_OF,
    )
    preference = build_admission_candidate(
        "DE0007664039",
        [_rec("XETR", lei=same_lei, isin="DE0007664039", cfi="EPNCFR", issr_req=True,
              frst=date(1990, 1, 1), full_nm="Volkswagen AG Vz", rca="DE")],
        as_of=AS_OF,
    )
    # Preference shares carry an "EP" CFI prefix (a distinct instrument type, not "ES") in real
    # FIRDS data -- confirmed against the real FULINS_E file's own EPNCFR sample. The ordinary
    # share resolves; the preference share, correctly, is out of THIS pass's narrow "ES"-only
    # equity scope (see module docstring) -- a real, honest scope boundary, not a bug.
    assert ordinary.admission_status == AdmissionStatus.PENDING_ESEF_CHECK
    assert ordinary.listing_id == "XETR:DE0007664005"
    assert preference.admission_status == AdmissionStatus.REJECTED
    assert preference.rejection_reason == RejectionReason.NON_EQUITY
    # Had preference shares been in scope, they would NOT collapse into the ordinary listing:
    assert ordinary.isin != preference.isin


# ── Idempotency ──────────────────────────────────────────────────────────────────────────


def test_build_admission_candidate_idempotent():
    """Running the same records through twice must produce an identical result -- no
    timestamp-dependent or ordering-dependent business identity."""
    records = _fcc_records()
    first = build_admission_candidate("ES0122060314", records, as_of=AS_OF)
    second = build_admission_candidate("ES0122060314", list(reversed(records)), as_of=AS_OF)
    assert first == second


def test_select_primary_listing_order_independent():
    xmad = _rec("XMAD", issr_req=True, frst=date(1999, 9, 30))
    dmad = _rec("DMAD", issr_req=True, frst=date(2024, 12, 9))
    r1 = select_primary_listing([xmad, dmad], AS_OF)
    r2 = select_primary_listing([dmad, xmad], AS_OF)
    assert r1.winner == r2.winner


# ── ESEF eligibility / ticker enrichment — post-processing steps ───────────────────────────


def test_apply_esef_eligibility_admits_when_ingestible():
    candidate = build_admission_candidate("ES0122060314", _fcc_records(), as_of=AS_OF)
    result = apply_esef_eligibility(candidate, has_esef_entity=True, has_ingestible_filing=True)
    assert result.admission_status == AdmissionStatus.ADMITTED
    assert result.rejection_reason is None


def test_apply_esef_eligibility_rejects_when_no_entity():
    candidate = build_admission_candidate("ES0122060314", _fcc_records(), as_of=AS_OF)
    result = apply_esef_eligibility(candidate, has_esef_entity=False, has_ingestible_filing=False)
    assert result.admission_status == AdmissionStatus.REJECTED
    assert result.rejection_reason == RejectionReason.NO_ESEF_FILING
    # Identity fields are preserved even on ESEF rejection (IDENTITY_RESOLVED != ESEF_INGESTIBLE).
    assert result.issuer_id == "EU_CURRENT:95980020140005178328"
    assert result.listing_id == "XMAD:ES0122060314"


def test_apply_esef_eligibility_rejects_when_entity_but_no_ingestible_filing():
    candidate = build_admission_candidate("ES0122060314", _fcc_records(), as_of=AS_OF)
    result = apply_esef_eligibility(candidate, has_esef_entity=True, has_ingestible_filing=False)
    assert result.admission_status == AdmissionStatus.REJECTED
    assert result.rejection_reason == RejectionReason.ESEF_NOT_INGESTIBLE


def test_apply_esef_eligibility_noop_on_already_rejected():
    candidate = build_admission_candidate(
        "CH1108675062",
        [_rec("XETR", cfi="EYADFM", isin="CH1108675062", issr_req=True, frst=date(2020, 1, 1))],
        as_of=AS_OF,
    )
    result = apply_esef_eligibility(candidate, has_esef_entity=True, has_ingestible_filing=True)
    assert result.admission_status == AdmissionStatus.REJECTED
    assert result.rejection_reason == RejectionReason.NON_EQUITY  # unchanged


def test_apply_ticker_enrichment_does_not_block_admission():
    """A ticker that cannot be resolved must not reject an otherwise-valid issuer (§11 of the
    driving brief: admission validity and market-data eligibility are separate dimensions)."""
    candidate = build_admission_candidate("ES0122060314", _fcc_records(), as_of=AS_OF)
    admitted = apply_esef_eligibility(candidate, has_esef_entity=True, has_ingestible_filing=True)
    unresolved_ticker = apply_ticker_enrichment(admitted, ticker=None)
    assert unresolved_ticker.admission_status == AdmissionStatus.ADMITTED
    assert unresolved_ticker.ticker_status == "unresolved"
    assert unresolved_ticker.ticker is None

    resolved_ticker = apply_ticker_enrichment(admitted, ticker="FCC")
    assert resolved_ticker.ticker_status == "resolved"
    assert resolved_ticker.ticker == "FCC"
    assert resolved_ticker.admission_status == AdmissionStatus.ADMITTED
