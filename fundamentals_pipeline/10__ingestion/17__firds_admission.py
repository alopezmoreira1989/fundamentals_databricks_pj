# Databricks notebook source
# MAGIC %md
# MAGIC # 10__ingestion / 17__firds_admission
# MAGIC
# MAGIC **Phase 5.3 (ADR-0009/ADR-0010/ADR-0011/ADR-0012) — the European universe & admission
# MAGIC layer.** Downloads ESMA FIRDS's real, official equity reference-data files, filters them
# MAGIC down to a trustworthy European admitted universe, and writes the auditable admission
# MAGIC decision (why each candidate is/isn't admitted) to
# MAGIC `main.config.eu_admission_candidates`. This is deliberately the layer BEFORE
# MAGIC `16__fetch_eu_xbrl.py`'s `EUCurrentSource` — it replaces the hardcoded `PILOT_EU_ENTITIES`
# MAGIC list with a reproducible, FIRDS-derived process, but does NOT itself write to
# MAGIC `financials_raw`/`financials`, does NOT modify `config.tickers`, and is NOT wired into
# MAGIC `16` or the scheduled DAG in this pass (see the module docstring in
# MAGIC `fundamentals_pipeline/sources/eu_admission.py` and `docs/phase5-3-european-universe-
# MAGIC admission.md` for the full research/design write-up).
# MAGIC
# MAGIC **Architecture:** all decision logic (equity classification, active-instrument filtering,
# MAGIC primary-listing selection, the admission/rejection state machine) is pure, fixture-tested
# MAGIC Python in `fundamentals_pipeline/sources/eu_admission.py` — this notebook only adds the
# MAGIC real FIRDS download, the real XML parse, the bounded ESEF-eligibility/ticker-enrichment
# MAGIC network calls, and the Delta write.
# MAGIC
# MAGIC **Scale note (§24/§25/§26 of the driving brief):** the raw FIRDS equity file is ~682k
# MAGIC `RefData` records with NO Spark-native reader for this exact ISO 20022 schema available
# MAGIC without a new heavy dependency this project doesn't otherwise need. The XML parse and the
# MAGIC deterministic equity/active/primary-listing filtering therefore run driver-side in plain
# MAGIC Python (`xml.etree.ElementTree.iterparse`, streaming — never the whole file materialized
# MAGIC as DOM at once) — a one-time, CPU-bound, no-external-API-call pass, not the "hundreds of
# MAGIC thousands of HTTP requests" pattern the brief explicitly prohibits. Only the resulting
# MAGIC SMALL admission-decision table (thousands of rows, not hundreds of thousands) is written
# MAGIC to Spark/Delta. External API calls (OpenFIGI, filings.xbrl.org) are made ONLY for a
# MAGIC bounded validation scope (established pilots + generalization candidates + a small
# MAGIC sample) — never once per raw FIRDS row, and never once per every resolved candidate
# MAGIC either (see §5/§6 below).

# COMMAND ----------

# MAGIC %md ## 0. Load config

# COMMAND ----------

# MAGIC %run "../00__config/01__tickers"

# COMMAND ----------

import zipfile
from dataclasses import replace
from datetime import date, datetime, timezone
from io import BytesIO
from xml.etree import ElementTree as ET

import pandas as pd
import requests

try:
    from fundamentals_pipeline.sources.eu_admission import (
        AdmissionStatus,
        FirdsVenueRecord,
        RejectionReason,
        apply_esef_eligibility,
        apply_ticker_enrichment,
        build_admission_candidate,
    )
    from fundamentals_pipeline.sources.eu_current import select_filing_for_period
except ImportError:
    import subprocess
    import sys

    subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "-e", "../.."])
    from fundamentals_pipeline.sources.eu_admission import (
        AdmissionStatus,
        FirdsVenueRecord,
        RejectionReason,
        apply_esef_eligibility,
        apply_ticker_enrichment,
        build_admission_candidate,
    )
    from fundamentals_pipeline.sources.eu_current import select_filing_for_period

# Gates the network/Spark sections below, mirroring 16__fetch_eu_xbrl.py's own RUN_EU_PILOT
# pattern — lets a local import (e.g. from tests) load this module's helper functions without
# a live FIRDS download or a Spark session.
RUN_FIRDS_ADMISSION = globals().get("RUN_FIRDS_ADMISSION", True)

FIRDS_NS = "urn:iso:std:iso:20022:tech:xsd:auth.017.001.02"
FIRDS_M2M_URL = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select"
FIRDS_HEADERS = {"User-Agent": globals().get("SEC_USER_AGENT", "unset (loaded outside Databricks)")}
OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"
FILINGS_XBRL_BASE = "https://filings.xbrl.org"

# COMMAND ----------

# MAGIC %md ## 1. FIRDS retrieval — real M2M API query, real download, provenance preserved
# MAGIC
# MAGIC Uses ESMA's own documented machine-to-machine endpoint (ESMA65-8-5014), not a
# MAGIC third-party mirror, not eFIRDS, not an undocumented endpoint (per the driving brief's
# MAGIC explicit §3 constraint). Every retrieved file's real `file_name`/`publication_date`/
# MAGIC `download_link`/`checksum` is preserved on every admission row this run produces — the
# MAGIC raw reference data stays auditable back to its exact source file.

# COMMAND ----------


def find_latest_firds_equity_files() -> list[dict]:
    """Query the real FIRDS M2M API for the latest FULINS equity (file_name containing '_E_')
    full reference-data files. Returns the raw Solr doc dicts (file_name/publication_date/
    download_link/checksum/...) -- real provenance metadata, not invented."""
    params = {"q": "file_type:fulins", "wt": "json", "rows": "50", "sort": "publication_date desc"}
    resp = requests.get(FIRDS_M2M_URL, params=params, headers=FIRDS_HEADERS, timeout=30)
    resp.raise_for_status()
    docs = resp.json().get("response", {}).get("docs", [])
    equity_docs = [d for d in docs if "_E_" in d["file_name"]]
    if not equity_docs:
        return []
    latest_date = equity_docs[0]["publication_date"]
    return [d for d in equity_docs if d["publication_date"] == latest_date]


def download_firds_file(doc: dict) -> bytes:
    resp = requests.get(doc["download_link"], headers=FIRDS_HEADERS, timeout=120)
    resp.raise_for_status()
    return resp.content


# COMMAND ----------

# MAGIC %md ## 2. XML parsing — real ISO 20022 auth.017.001.02 schema, streaming iterparse

# COMMAND ----------


def parse_firds_equity_zip(zip_bytes: bytes) -> list[FirdsVenueRecord]:
    """Stream-parses one FULINS_E zip's XML into `FirdsVenueRecord`s -- ALL RefData records
    (not pre-filtered to equity here; `build_admission_candidate` applies the equity filter),
    so a full raw-record count is still derivable for the funnel report. Uses `iterparse` +
    `elem.clear()` so peak memory never holds the full DOM -- confirmed to process the real
    ~682k-record file set in well under a minute during Phase 5.3 research."""
    out: list[FirdsVenueRecord] = []
    with zipfile.ZipFile(BytesIO(zip_bytes)) as z:
        xml_names = [n for n in z.namelist() if n.endswith(".xml")]
        for xml_name in xml_names:
            with z.open(xml_name) as f:
                for _event, elem in ET.iterparse(f, events=("end",)):
                    if elem.tag != f"{{{FIRDS_NS}}}RefData":
                        continue
                    fingnl = elem.find(f"{{{FIRDS_NS}}}FinInstrmGnlAttrbts")
                    if fingnl is None:
                        elem.clear()
                        continue
                    isin = fingnl.findtext(f"{{{FIRDS_NS}}}Id", default="")
                    cfi = fingnl.findtext(f"{{{FIRDS_NS}}}ClssfctnTp", default="")
                    full_nm = fingnl.findtext(f"{{{FIRDS_NS}}}FullNm", default="")
                    ntnl_ccy = fingnl.findtext(f"{{{FIRDS_NS}}}NtnlCcy", default="") or None
                    lei = elem.findtext(f"{{{FIRDS_NS}}}Issr", default="")
                    trad = elem.find(f"{{{FIRDS_NS}}}TradgVnRltdAttrbts")
                    mic = trad.findtext(f"{{{FIRDS_NS}}}Id", default="") if trad is not None else ""
                    issr_req = (
                        trad.findtext(f"{{{FIRDS_NS}}}IssrReq", default="false") == "true"
                        if trad is not None else False
                    )
                    frst_raw = trad.findtext(f"{{{FIRDS_NS}}}FrstTradDt", default="") if trad is not None else ""
                    term_raw = trad.findtext(f"{{{FIRDS_NS}}}TermntnDt", default="") if trad is not None else ""
                    tech = elem.find(f"{{{FIRDS_NS}}}TechAttrbts")
                    rca = tech.findtext(f"{{{FIRDS_NS}}}RlvntCmptntAuthrty", default="") if tech is not None else ""
                    out.append(FirdsVenueRecord(
                        isin=isin, mic=mic, lei=lei, cfi=cfi, full_nm=full_nm,
                        ntnl_ccy=ntnl_ccy, rca=rca or None, issr_req=issr_req,
                        frst_trad_dt=date.fromisoformat(frst_raw[:10]) if frst_raw else None,
                        termntn_dt=date.fromisoformat(term_raw[:10]) if term_raw else None,
                    ))
                    elem.clear()
    return out


# COMMAND ----------

# MAGIC %md ## 3. Admission pipeline — equity → active → primary listing → identity

# COMMAND ----------


def run_admission_pipeline(records: list[FirdsVenueRecord], as_of: date, source_file: str,
                            source_publication_date: date) -> list:
    """Groups by ISIN and applies `build_admission_candidate` per group -- the deterministic,
    no-network part of admission. Returns one `AdmissionCandidate` per unique ISIN in the raw
    file (including non-equity ones, each carrying its own real rejection reason)."""
    by_isin: dict[str, list[FirdsVenueRecord]] = {}
    for r in records:
        by_isin.setdefault(r.isin, []).append(r)

    return [
        build_admission_candidate(
            isin, recs, as_of=as_of,
            source_file=source_file, source_publication_date=source_publication_date,
        )
        for isin, recs in by_isin.items()
    ]


# COMMAND ----------

# MAGIC %md ## 4. Bounded ESEF eligibility + ticker enrichment
# MAGIC
# MAGIC Deliberately NOT run for the full resolved-candidate population (~6,000+ unique issuers)
# MAGIC in this pass — per the driving brief's §22/§23/§26, this phase proves `FIRDS → admitted
# MAGIC universe`, not `admitted universe → full fundamentals ingestion`. Candidates outside
# MAGIC `BOUNDED_VALIDATION_ISINS` stay at `PENDING_ESEF_CHECK` — a real, honest state, not a
# MAGIC silently-skipped one (see `eu_admission.AdmissionStatus`).

# COMMAND ----------

# The 4 already-shipped Phase 5.1 pilots + the 2 Phase 5.2b generalization candidates (proving
# the identity chain generalizes beyond the hardcoded pilots) + 3 new candidates chosen during
# Phase 5.3's own real full-scale run specifically to exercise real, distinct outcomes:
# SAP AG (DE0007164600) deliberately included because Germany is a known filings.xbrl.org
# coverage gap (see registry.py's EU_CURRENT notes) — expected to resolve identity via FIRDS but
# fail the ESEF check, a real demonstration of IDENTITY_RESOLVED != ESEF_INGESTIBLE. Randstad
# Holding NV (NL0000379121) and Intesa Sanpaolo SpA (IT0000072618) are real, resolved,
# well-known candidates from countries already confirmed ESEF-covered (NL/IT).
BOUNDED_VALIDATION_ISINS = {
    "ES0122060314",  # FCC
    "FR0010220475",  # Alstom
    "NL0015000CG2",  # New Amsterdam Invest
    "IT0005599938",  # Fincantieri
    "ES0144580Y14",  # Iberdrola (generalization candidate, Phase 5.2b)
    "FR0000125007",  # Saint-Gobain (generalization candidate, Phase 5.2b)
    "DE0007164600",  # SAP AG (new sample -- expected Germany/ESEF-gap case)
    "NL0000379121",  # Randstad Holding NV (new sample)
    "IT0000072618",  # Intesa Sanpaolo SpA (new sample)
}


def check_esef_eligibility(lei: str) -> tuple[bool, bool]:
    """Real `filings.xbrl.org` call -- (has_esef_entity, has_ingestible_filing). Reuses
    `eu_current.select_filing_for_period` (grouped by period_end) rather than re-implementing
    the amendment-selection rule a second time, per the driving brief's own §12 instruction."""
    resp = requests.get(f"{FILINGS_XBRL_BASE}/api/entities/{lei}/filings",
                         headers=FIRDS_HEADERS, timeout=30)
    if resp.status_code == 404:
        return False, False
    resp.raise_for_status()
    data = resp.json().get("data", [])
    if not data:
        return False, False
    by_period: dict[str, list[dict]] = {}
    for row in data:
        attrs = row.get("attributes", {})
        by_period.setdefault(attrs.get("period_end"), []).append(attrs)
    for group in by_period.values():
        winner, _rejections = select_filing_for_period(group)
        if winner is not None:
            return True, True
    return True, False  # entity exists, but no group ever produced an ingestible filing


def resolve_ticker_via_openfigi(isin: str, mic: str) -> str | None:
    """Real OpenFIGI call -- ticker enrichment only, never identity (§11/§25 of the driving
    brief). A single-job request per candidate, only for the bounded scope."""
    try:
        resp = requests.post(
            OPENFIGI_URL, json=[{"idType": "ID_ISIN", "idValue": isin, "micCode": mic}],
            headers={"Content-Type": "application/json"}, timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()[0].get("data", [])
        return data[0]["ticker"] if data else None
    except Exception:
        return None


def apply_bounded_validation(candidates: list, scope: set[str]) -> list:
    out = []
    for c in candidates:
        if c.isin not in scope or c.admission_status != AdmissionStatus.PENDING_ESEF_CHECK:
            out.append(c)
            continue
        has_entity, has_filing = check_esef_eligibility(c.lei)
        c = apply_esef_eligibility(c, has_esef_entity=has_entity, has_ingestible_filing=has_filing)
        if c.admission_status in (AdmissionStatus.ADMITTED, AdmissionStatus.PENDING_ESEF_CHECK):
            ticker = resolve_ticker_via_openfigi(c.isin, c.mic)
            c = apply_ticker_enrichment(c, ticker=ticker)
        out.append(c)
    return out


# COMMAND ----------

# MAGIC %md ## 5. Run

# COMMAND ----------

if RUN_FIRDS_ADMISSION:
    retrieved_at = datetime.now(timezone.utc)
    equity_files = find_latest_firds_equity_files()
    print(f"Found {len(equity_files)} latest FIRDS equity file(s): "
          f"{[d['file_name'] for d in equity_files]}")

    all_records: list[FirdsVenueRecord] = []
    file_provenance = []
    for doc in equity_files:
        content = download_firds_file(doc)
        recs = parse_firds_equity_zip(content)
        all_records.extend(recs)
        file_provenance.append(doc)
        print(f"  {doc['file_name']}: {len(recs)} RefData records")

    source_file = ", ".join(d["file_name"] for d in file_provenance)
    source_publication_date = (
        date.fromisoformat(file_provenance[0]["publication_date"][:10])
        if file_provenance else date.today()
    )
    # Reference date for active-instrument determination is the FILE's own publication date,
    # never "today" -- re-running against an older downloaded file must reproduce the same
    # admission decision (idempotency), matching this project's existing date-anchoring
    # discipline for FX/price rates.
    AS_OF = source_publication_date

    print(f"\nTotal raw RefData records: {len(all_records)}")

    candidates = run_admission_pipeline(all_records, AS_OF, source_file, source_publication_date)
    print(f"Unique ISINs: {len(candidates)}")

    candidates = apply_bounded_validation(candidates, BOUNDED_VALIDATION_ISINS)

    # ── Funnel report ──
    from collections import Counter

    equity_n = sum(1 for c in candidates if c.rejection_reason != RejectionReason.NON_EQUITY)
    active_n = sum(1 for c in candidates if c.rejection_reason not in
                   (RejectionReason.NON_EQUITY, RejectionReason.INACTIVE))
    primary_n = sum(1 for c in candidates if c.mic is not None)
    issuer_n = len({c.lei for c in candidates if c.lei})
    status_counts = Counter(c.admission_status for c in candidates)
    reason_counts = Counter(c.rejection_reason for c in candidates if c.rejection_reason)

    print(f"\nFunnel: raw={len(all_records)} -> unique_isin={len(candidates)} "
          f"-> equity={equity_n} -> active={active_n} -> primary_listing={primary_n} "
          f"-> unique_issuers={issuer_n}")
    print(f"Admission status: {dict(status_counts)}")
    print(f"Rejection reasons: {dict(reason_counts)}")

# COMMAND ----------

# MAGIC %md ## 6. Write `main.config.eu_admission_candidates` (new table, not wired into the DAG)

# COMMAND ----------

if RUN_FIRDS_ADMISSION:
    from pyspark.sql.types import (
        DateType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    admission_tbl = f"{CATALOG}.config.eu_admission_candidates"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {admission_tbl} (
            isin                     STRING    NOT NULL,
            lei                      STRING,
            mic                      STRING,
            issuer_id                STRING,
            listing_id               STRING,
            issuer_name              STRING,
            country                  STRING,
            ticker                   STRING,
            ticker_status            STRING    NOT NULL,
            admission_status         STRING    NOT NULL,
            rejection_reason         STRING,
            n_venue_records          INT       NOT NULL,
            primary_frst_trad_dt     DATE,
            source                   STRING    NOT NULL,
            source_file              STRING,
            source_publication_date  DATE,
            retrieved_at             TIMESTAMP NOT NULL
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact'   = 'true'
        )
    """)

    _schema = StructType([
        StructField("isin", StringType(), False),
        StructField("lei", StringType(), True),
        StructField("mic", StringType(), True),
        StructField("issuer_id", StringType(), True),
        StructField("listing_id", StringType(), True),
        StructField("issuer_name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("ticker", StringType(), True),
        StructField("ticker_status", StringType(), False),
        StructField("admission_status", StringType(), False),
        StructField("rejection_reason", StringType(), True),
        StructField("n_venue_records", IntegerType(), False),
        StructField("primary_frst_trad_dt", DateType(), True),
        StructField("source", StringType(), False),
        StructField("source_file", StringType(), True),
        StructField("source_publication_date", DateType(), True),
        StructField("retrieved_at", TimestampType(), False),
    ])

    rows = [{
        "isin": c.isin, "lei": c.lei, "mic": c.mic, "issuer_id": c.issuer_id,
        "listing_id": c.listing_id, "issuer_name": c.issuer_name, "country": c.country,
        "ticker": c.ticker, "ticker_status": c.ticker_status,
        "admission_status": c.admission_status.value,
        "rejection_reason": c.rejection_reason.value if c.rejection_reason else None,
        "n_venue_records": c.n_venue_records,
        "primary_frst_trad_dt": c.primary_frst_trad_dt,
        "source": c.source, "source_file": c.source_file,
        "source_publication_date": c.source_publication_date,
        "retrieved_at": retrieved_at,
    } for c in candidates]

    pdf = pd.DataFrame(rows)
    sdf = spark.createDataFrame(pdf, schema=_schema)

    # Full overwrite -- this table is a point-in-time snapshot of one FIRDS run's admission
    # decision, not an append-only log (re-running with the same input file must reproduce the
    # same rows, not accumulate duplicates -- idempotency, verified in tests/test_sources_
    # eu_admission.py and re-confirmed against the real file in Phase 5.3's own validation).
    sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(admission_tbl)
    print(f"✓ {len(rows)} admission candidate(s) written to {admission_tbl}")
