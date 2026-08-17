"""fundamentals_pipeline.sources.eu_admission — European universe/admission layer (Phase 5.3).

Pure Python, no Spark/`dbutils`/network dependency, matching `sources/base.py`'s own
constraint. Real HTTP calls and the FIRDS XML download/parse live in
`10__ingestion/17__firds_admission.py`; everything here is the fixture-testable decision logic
that notebook delegates to — equity classification, active-instrument filtering, primary-listing
selection, and the admission/rejection state machine.

Every rule here is grounded in real ESMA FIRDS data downloaded and inspected during Phase 5.2c/
5.2d/5.3 research (2026-08), not assumed:

- **Equity filter** (`is_equity_cfi`): the CFI 2-character prefix `"ES"` = Equity, Common/
  Ordinary Shares (ISO 10962; independently confirmed against ESMA's own FIRDS CFI validation
  workbook, ESMA70-145-1090 — see Phase 5.2d). Verified by direct negative example against the
  real FULINS_E file: the `"EY"` prefix (structured notes, e.g. "Series 26 ... HIFIN Solution
  Secured Notes due 2027") is by far the largest single CFI group in that file (478,047 of
  682,398 raw records) despite sharing the broad `"E"` top-level category with genuine common
  stock — a real, concrete proof that filtering to exactly `"ES"`, not any `"E*"` prefix, is
  necessary, not merely cautious. `"EP"`/`"EC"`/`"EF"`/`"EL"`/`"ED"`/`"EM"` were likewise sampled
  and are real but distinct instrument types (preference shares, depositary receipts, LP fund
  units, profit-participation instruments) — correctly excluded from a "common stock" universe.
- **Active filter** (`is_active_venue_record`): `TradgVnRltdAttrbts/TermntnDt` is real and
  present on ~60% of real venue records (confirmed: 301,785 of 500,000 in the first equity
  file); its far-future sentinel value (`9999-12-31T22:00:00Z`) needs no special-casing — a
  plain date comparison against the reference date already treats it as "not terminated."
  `FrstTradDt` must also be `<= as_of` (a venue admission that hasn't started trading yet is not
  currently active).
- **Primary-listing rule** (`select_primary_listing`): `IssrReq = true` among currently-active
  venue records, tie-broken by earliest `FrstTradDt` — the exact rule Phase 5.2d derived and
  validated against two real ties (FCC's `XMAD` vs. `DMAD`; Fincantieri's `MTAA` vs. `HMTF`).
  Phase 5.3's own full-universe run against the real 2026-08-15 file additionally found this
  rule *correctly* leaves the majority of raw equity ISINs (11,067 of 17,251, i.e. ~64%)
  without a resolvable primary listing — not a rule failure. A real sample of 10 such ISINs
  (South African, Bermudian, Chinese, Canadian, Australian, and US companies, e.g. "Truworths
  International Ltd.", "Agricultural Bank Of China Ltd", "iRadimed Corp.") are all foreign
  issuers quoted only on small German regional MTF venues (Tradegate/Gettex/Munich/Stuttgart/
  Hamburg-style codes) with no genuine EU-regulated-market admission — exactly the "foreign
  company incidentally quoted on a European venue" case this project's universe scope has
  always intended to exclude (see the target architecture's "European equity instruments that
  can be linked to European ESEF-reporting issuers", not "every ISIN FIRDS happens to mention").
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Literal

EU_ADMISSION_SOURCE = "ESMA_FIRDS"

# The fundamentals SOURCE an admitted issuer would be ingested through remains EU_CURRENT
# (filings.xbrl.org / ESEF) — FIRDS is the UNIVERSE source, not the fundamentals source.
# issuer_id must therefore stay "EU_CURRENT:<LEI>", byte-identical to what
# sources/eu_current.py's entity_from_pilot() already produces for the 4 hardcoded pilots, so a
# candidate this module admits needs no re-identification if a future phase wires it into
# EUCurrentSource.
_ISSUER_ID_SOURCE = "EU_CURRENT"


class AdmissionStatus(str, Enum):
    ADMITTED = "admitted"  # identity + instrument + ESEF all resolved
    PENDING_ESEF_CHECK = "pending_esef_check"  # identity + instrument resolved; ESEF not checked
    # this run (bulk candidates outside the bounded validation scope, see §26 of the driving brief)
    REJECTED = "rejected"


class RejectionReason(str, Enum):
    """Only codes that correspond to a real, implemented check — never a placeholder for a
    check that doesn't exist yet."""

    NON_EQUITY = "non_equity"
    INACTIVE = "inactive"
    NO_LEI = "no_lei"
    IDENTITY_UNRESOLVED = "identity_unresolved"  # LEI present but failed a GLEIF cross-check
    # (bounded-validation scope only — see §9/§25 of the driving brief)
    PRIMARY_LISTING_UNRESOLVED = "primary_listing_unresolved"
    NO_ESEF_FILING = "no_esef_filing"
    ESEF_NOT_INGESTIBLE = "esef_not_ingestible"


TickerStatus = Literal["resolved", "unresolved", "not_attempted"]


@dataclass(frozen=True)
class FirdsVenueRecord:
    """One real FIRDS `RefData` record's fields relevant to admission -- one row per
    (ISIN, MIC), the exact grain FIRDS itself reports at."""

    isin: str
    mic: str
    lei: str
    cfi: str
    full_nm: str
    ntnl_ccy: str | None
    rca: str | None  # TechAttrbts/RlvntCmptntAuthrty -- the reporting regulator's country code
    issr_req: bool
    frst_trad_dt: date | None
    termntn_dt: date | None


@dataclass(frozen=True)
class PrimaryListingResult:
    """The outcome of `select_primary_listing` for one ISIN's venue records -- always carries a
    reason, whether resolved or not (auditability: "why was this MIC chosen" / "why wasn't
    one")."""

    winner: FirdsVenueRecord | None
    reason: RejectionReason | None  # None iff winner is not None
    n_active_records: int
    n_issr_req_active_records: int


@dataclass(frozen=True)
class AdmissionCandidate:
    """One ISIN's full admission decision -- the auditable output record this module produces.
    Every field a future consumer or a human reviewer needs to answer "why is/isn't this here"
    without re-deriving it from raw FIRDS data."""

    isin: str
    lei: str | None
    mic: str | None  # primary listing MIC, if resolved
    issuer_id: str | None  # EU_CURRENT:<LEI>, if LEI resolved
    listing_id: str | None  # MIC:ISIN (ADR-0012), if primary listing resolved
    issuer_name: str | None
    country: str | None  # RCA, from the primary listing's own venue record
    currency: str | None  # NtnlCcy from the primary listing's own venue record -- real,
    # FIRDS-sourced, never guessed/defaulted (see Phase 5.6's own currency-alignment fix,
    # which reads this instead of falling through to a silent "USD" default)
    ticker: str | None
    ticker_status: TickerStatus
    admission_status: AdmissionStatus
    rejection_reason: RejectionReason | None
    n_venue_records: int
    primary_frst_trad_dt: date | None  # provenance: the field the primary-listing tie-break used
    source: str = EU_ADMISSION_SOURCE
    source_file: str | None = None
    source_publication_date: date | None = None


def is_equity_cfi(cfi: str | None) -> bool:
    """True iff `cfi` is a real CFI code classified as Equity, Common/Ordinary Shares -- the
    2-character prefix `"ES"`, exactly, not any `"E*"` prefix (see module docstring for the
    real negative-example evidence this exact-prefix requirement is based on)."""
    return bool(cfi) and cfi[:2] == "ES"


def is_active_venue_record(record: FirdsVenueRecord, as_of: date) -> bool:
    """True iff this specific (ISIN, MIC) venue admission is currently active: trading has
    started (`FrstTradDt <= as_of`) and has not terminated (`TermntnDt` absent, or
    `TermntnDt >= as_of` -- the real `9999-12-31` sentinel satisfies this with no special
    case)."""
    if record.frst_trad_dt is None or record.frst_trad_dt > as_of:
        return False
    if record.termntn_dt is not None and record.termntn_dt < as_of:
        return False
    return True


def select_primary_listing(
    records: Sequence[FirdsVenueRecord],
    as_of: date,
) -> PrimaryListingResult:
    """Pick the single currently-active, issuer-requested venue record with the earliest
    `FrstTradDt` -- Phase 5.2d's rule, applied among ACTIVE records only (a terminated venue
    admission cannot be today's primary listing, even if it once had the earliest `FrstTradDt`).

    All `records` must already share the same ISIN -- the caller groups by ISIN before calling
    this. Never guesses: an ISIN with no active records is `INACTIVE`; one with active records
    but none `IssrReq = true` is `PRIMARY_LISTING_UNRESOLVED` (the dominant real case -- 11,067
    of 17,251 real equity ISINs in the full FULINS_E_20260815 run, per the module docstring:
    mostly foreign stocks quoted on European MTFs without a genuine EU admission request); a
    genuine exact tie on the minimum `FrstTradDt` among active `IssrReq = true` candidates is
    ALSO real and non-rare -- confirmed against 1,192 real equity ISINs in that same full run
    (not the two-case sample Phase 5.2d's narrower 4-pilot check saw), overwhelmingly a Nordic
    (Nasdaq Nordic/First North) pattern of parallel MIC codes sharing one admission event's
    exact date (real example: Konsolidator A/S, DK0061113511 -- `DNDK` and `MNDK` both admitted
    2019-05-10). Handled the same way either way: `PRIMARY_LISTING_UNRESOLVED`, never a guess.
    """
    active = [r for r in records if is_active_venue_record(r, as_of)]
    if not active:
        return PrimaryListingResult(None, RejectionReason.INACTIVE, 0, 0)

    candidates = [r for r in active if r.issr_req]
    if not candidates:
        return PrimaryListingResult(
            None, RejectionReason.PRIMARY_LISTING_UNRESOLVED, len(active), 0
        )

    min_dt = min(r.frst_trad_dt for r in candidates)
    tied = [r for r in candidates if r.frst_trad_dt == min_dt]
    if len(tied) > 1:
        return PrimaryListingResult(
            None, RejectionReason.PRIMARY_LISTING_UNRESOLVED, len(active), len(candidates)
        )

    return PrimaryListingResult(tied[0], None, len(active), len(candidates))


def make_eu_issuer_id(lei: str) -> str:
    """`issuer_id` for a FIRDS-admitted issuer -- always `EU_CURRENT:<LEI>` (not
    `ESMA_FIRDS:<LEI>`), so it is byte-identical to what `sources/eu_current.py`'s
    `entity_from_pilot()` already produces for the 4 hardcoded pilots. FIRDS is the universe
    source; the fundamentals themselves would still come from `EU_CURRENT` (filings.xbrl.org)."""
    from ..identity import make_issuer_id

    return make_issuer_id(_ISSUER_ID_SOURCE, lei)


def build_admission_candidate(
    isin: str,
    records: Sequence[FirdsVenueRecord],
    *,
    as_of: date,
    source_file: str | None = None,
    source_publication_date: date | None = None,
) -> AdmissionCandidate:
    """The full per-ISIN admission decision, through identity + instrument resolution --
    stops short of the ESEF check (a separate, bounded step -- see
    `apply_esef_eligibility`/`apply_ticker_enrichment` below), which needs a network call this
    pure module deliberately does not make."""
    n_venue_records = len(records)

    if not records:
        return AdmissionCandidate(
            isin=isin, lei=None, mic=None, issuer_id=None, listing_id=None,
            issuer_name=None, country=None, currency=None, ticker=None, ticker_status="not_attempted",
            admission_status=AdmissionStatus.REJECTED, rejection_reason=RejectionReason.NON_EQUITY,
            n_venue_records=n_venue_records, primary_frst_trad_dt=None,
            source_file=source_file, source_publication_date=source_publication_date,
        )

    # A representative record for display purposes ONLY (e.g. issuer_name when no single venue
    # "wins" as primary) -- picked deterministically by MIC, never by list/iteration order.
    # Real, live data forced this: the same ISIN's venue records can carry inconsistently
    # formatted names across reporting venues (confirmed against the full FULINS_E_20260815
    # run -- e.g. "Bang and Olufsen A/S" on one venue's record vs. "BANG&OLUF DKK10 B" on
    # another's for the same ISIN), so `records[0]` depended on input order and broke
    # idempotency (found and fixed during this phase's own idempotency test).
    representative = min(records, key=lambda r: r.mic)

    if not is_equity_cfi(representative.cfi):
        return AdmissionCandidate(
            isin=isin, lei=None, mic=None, issuer_id=None, listing_id=None,
            issuer_name=None, country=None, currency=None, ticker=None, ticker_status="not_attempted",
            admission_status=AdmissionStatus.REJECTED, rejection_reason=RejectionReason.NON_EQUITY,
            n_venue_records=n_venue_records, primary_frst_trad_dt=None,
            source_file=source_file, source_publication_date=source_publication_date,
        )

    primary = select_primary_listing(records, as_of)
    if primary.winner is None:
        return AdmissionCandidate(
            isin=isin, lei=None, mic=None, issuer_id=None, listing_id=None,
            issuer_name=representative.full_nm or None, country=None, currency=None,
            ticker=None, ticker_status="not_attempted",
            admission_status=AdmissionStatus.REJECTED, rejection_reason=primary.reason,
            n_venue_records=n_venue_records, primary_frst_trad_dt=None,
            source_file=source_file, source_publication_date=source_publication_date,
        )

    winner = primary.winner
    if not winner.lei:
        return AdmissionCandidate(
            isin=isin, lei=None, mic=winner.mic, issuer_id=None, listing_id=None,
            issuer_name=winner.full_nm or None, country=winner.rca, currency=winner.ntnl_ccy,
            ticker=None, ticker_status="not_attempted",
            admission_status=AdmissionStatus.REJECTED, rejection_reason=RejectionReason.NO_LEI,
            n_venue_records=n_venue_records, primary_frst_trad_dt=winner.frst_trad_dt,
            source_file=source_file, source_publication_date=source_publication_date,
        )

    from ..identity import make_listing_id_from_isin

    return AdmissionCandidate(
        isin=isin,
        lei=winner.lei,
        mic=winner.mic,
        issuer_id=make_eu_issuer_id(winner.lei),
        listing_id=make_listing_id_from_isin(winner.mic, isin),
        issuer_name=winner.full_nm or None,
        country=winner.rca,
        currency=winner.ntnl_ccy,
        ticker=None,
        ticker_status="not_attempted",
        admission_status=AdmissionStatus.PENDING_ESEF_CHECK,
        rejection_reason=None,
        n_venue_records=n_venue_records,
        primary_frst_trad_dt=winner.frst_trad_dt,
        source_file=source_file,
        source_publication_date=source_publication_date,
    )


def apply_esef_eligibility(
    candidate: AdmissionCandidate,
    *,
    has_esef_entity: bool,
    has_ingestible_filing: bool,
) -> AdmissionCandidate:
    """Advance a `PENDING_ESEF_CHECK` candidate to `ADMITTED`/`REJECTED` once the caller (the
    notebook) has actually queried `filings.xbrl.org` for it. Deliberately a separate function
    from `build_admission_candidate` -- the ESEF check makes a real network call, which this
    module never does, and per the driving brief's §26/§22, is only performed for a bounded
    subset of candidates in this phase, not the full admitted-by-FIRDS-alone population.

    `IDENTITY_RESOLVED` (this candidate has a real issuer_id/listing_id) is distinct from
    `ESEF_INGESTIBLE` (this candidate's fundamentals are actually retrievable) -- a candidate
    that fails this check keeps its resolved identity fields; only `admission_status`/
    `rejection_reason` change, so a future source covering the same issuer could reuse the
    identity without redoing resolution (mirrors ADR-0011's own IDENTITY_RESOLVED vs.
    NOT_INGESTIBLE distinction)."""
    if candidate.admission_status != AdmissionStatus.PENDING_ESEF_CHECK:
        return candidate
    if not has_esef_entity:
        reason = RejectionReason.NO_ESEF_FILING
    elif not has_ingestible_filing:
        reason = RejectionReason.ESEF_NOT_INGESTIBLE
    else:
        reason = None
    status = AdmissionStatus.ADMITTED if reason is None else AdmissionStatus.REJECTED
    return _replace_status(candidate, admission_status=status, rejection_reason=reason)


def apply_ticker_enrichment(
    candidate: AdmissionCandidate,
    *,
    ticker: str | None,
) -> AdmissionCandidate:
    """Attach a resolved ticker (or record that resolution was attempted and failed) --
    deliberately never blocks admission (§11 of the driving brief: ticker resolvability and
    admission validity are separate dimensions). Only called for candidates already at
    `ADMITTED` or `PENDING_ESEF_CHECK` -- a rejected candidate's ticker is not worth resolving."""
    return _replace_status(
        candidate,
        ticker=ticker,
        ticker_status=("resolved" if ticker else "unresolved"),
    )


def _replace_status(candidate: AdmissionCandidate, **changes) -> AdmissionCandidate:
    from dataclasses import replace

    return replace(candidate, **changes)
