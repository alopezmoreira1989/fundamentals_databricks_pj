# Databricks notebook source
# MAGIC %md
# MAGIC # 10__ingestion / 16__fetch_eu_xbrl
# MAGIC
# MAGIC **Phase 5.1 (ADR-0009/ADR-0010) — the European `filings.xbrl.org` fundamentals adapter.**
# MAGIC Fetches real annual IFRS/ESEF filings and writes mapped facts into the SAME
# MAGIC `financials_raw`/`financials` tables SEC ingestion uses — `source_id="EU_CURRENT"` is the
# MAGIC only thing that distinguishes a row's origin, per ADR-0009's "no
# MAGIC `financials_raw_europe`" decision.
# MAGIC
# MAGIC **Architecture:** `EUCurrentSource` implements the same `FundamentalsSource` contract
# MAGIC (`fundamentals_pipeline/sources/base.py`) `SECXBRLSource` proved fits SEC's shape —
# MAGIC `discover_entities`/`discover_filings`/`retrieve_facts`/`detect_metadata`. Unlike
# MAGIC `SECXBRLSource` (a proof, never called), this class — and `process_eu_entity` below — are
# MAGIC the REAL, working European ingestion path. Filing selection (the amendment rule),
# MAGIC consolidated-vs-segment discrimination, and current-vs-comparative period selection all
# MAGIC live as pure, fixture-tested functions in `fundamentals_pipeline/sources/eu_current.py`;
# MAGIC this notebook only adds the network calls and the Delta write.
# MAGIC
# MAGIC **Phase 5.4 update — production input is now `main.config.eu_admission_candidates`,
# MAGIC not the hardcoded pilot list.** `PILOT_EU_ENTITIES` remains, unchanged, as the
# MAGIC test/dev fixture `EUCurrentSource()`'s no-arg default uses; the real ingestion run (§5)
# MAGIC reads `admission_status = 'admitted'` rows fresh from the Phase 5.3 admission layer via
# MAGIC `load_admitted_eu_entities()`, never a hardcoded ticker list. Still does NOT modify
# MAGIC `main.config.tickers`, still does NOT run on the scheduled DAG (standalone invocation
# MAGIC only — see `docs/phase5-1-eu-adapter.md`/`docs/phase5-4-european-vertical-slice.md` for
# MAGIC the one-line `91a` change a future phase would need to schedule this).
# MAGIC
# MAGIC **Writes to:** `{catalog}.{schema}.financials_raw` (append), `{catalog}.{schema}.ingestion_failures`.

# COMMAND ----------

# MAGIC %md ## 0. Load config

# COMMAND ----------

# MAGIC %run "../00__config/01__tickers"

# COMMAND ----------

import json
from datetime import date, datetime, timedelta

import pandas as pd
import requests

# `pyspark` is deliberately NOT imported at module level -- this repo has no Spark CI (see
# CLAUDE.md) and pyspark is not a test dependency. The only place it's actually needed is
# EU_SCHEMA_DEF/the CREATE TABLE section below, already gated behind `if RUN_EU_PILOT:`; the
# import is deferred to right before that use so tests/test_eu_adapter_protocol_conformance.py
# can load EUCurrentSource/process_eu_entity (RUN_EU_PILOT=False) without pyspark installed.

try:
    from fundamentals_pipeline.identity import make_issuer_id
    from fundamentals_pipeline.sources.base import (
        FundamentalsSource,
        SourceEntity,
        SourceEntityMetadata,
        SourceFiling,
    )
    from fundamentals_pipeline.sources.eu_current import (
        EU_SOURCE_ID,
        entity_from_pilot,
        extract_source_facts,
        map_source_fact_to_canonical,
        select_filing_for_period,
    )
except ImportError:
    import subprocess
    import sys

    subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "-e", "../.."])
    from fundamentals_pipeline.identity import make_issuer_id
    from fundamentals_pipeline.sources.base import (
        FundamentalsSource,
        SourceEntity,
        SourceEntityMetadata,
        SourceFiling,
    )
    from fundamentals_pipeline.sources.eu_current import (
        EU_SOURCE_ID,
        entity_from_pilot,
        extract_source_facts,
        map_source_fact_to_canonical,
        select_filing_for_period,
    )

# `SEC_USER_AGENT`/`STATEMENTS` are `%run`-injected notebook globals (see section 0 above) --
# absent when this file is loaded outside Databricks (e.g. the local protocol-conformance test
# in tests/test_eu_adapter_protocol_conformance.py). Guarded via globals().get(...), the same
# defensive pattern 11__fetch_sec_xbrl.py already uses for AGGREGATE_OR_SUM_CONCEPTS/
# DEI_NAMESPACE_CONCEPTS -- a real Databricks run is completely unaffected (the real values are
# always present there before this cell runs).
EU_HEADERS = {"User-Agent": globals().get("SEC_USER_AGENT", "unset (loaded outside Databricks)")}
EU_API_BASE = "https://filings.xbrl.org"

# ── Reverse lookup: canonical label -> (stmt_name, kind), reused directly from STATEMENTS
# (the same `%run`-injected config SEC ingestion uses) — no duplicated stmt/kind mapping.
#
# Deterministic priority (Phase 6.3 fix — docs/phase6-3-net-income-statement-classification.md),
# NOT a naive dict-comprehension collapse. Three labels ("Net Income", "Net Income (to common)",
# "Net Income (incl NCI)") genuinely exist in BOTH STATEMENTS["Income Statement"] and
# STATEMENTS["Cash Flow"] — correct and load-bearing for SEC ingestion, which extracts each
# statement's concept map independently (11__fetch_sec_xbrl.py's own `for stmt_name, concept_map
# in STATEMENTS.items()` loop) and so genuinely produces TWO real, separately-sourced rows per
# collision (confirmed: AAPL has 17 Income Statement + 17 Cash Flow Net Income rows, one pair per
# fiscal year — real GAAP contexts, not a duplicate). The EU adapter has no second XBRL context to
# draw on — one ESEF fact must resolve to exactly ONE statement — so a plain last-key-wins
# dict comprehension (whichever statement STATEMENTS.items() happens to yield last) silently
# picked "Cash Flow" for all three, which is wrong: every current collision is a P&L bottom-line
# concept, and a statement's Cash Flow appearance is always a RECONCILIATION reuse of a figure
# computed elsewhere (Income Statement for a flow concept; Balance Sheet for a stock concept),
# never that concept's own primary source. `_STMT_PRIORITY` encodes that general rule (primary-
# source statements before the reconciliation statement) explicitly, so the result no longer
# depends on `STATEMENTS`' own dict/insertion order, and any *future* label collision — including
# today's dormant "Net Income (to common)" — resolves the same safe way automatically, with no
# per-label override list to remember to update.
_STMT_PRIORITY = ("Income Statement", "Balance Sheet", "Cash Flow")

_LABEL_TO_STMT_KIND: dict = {}
for _stmt_name in _STMT_PRIORITY:
    for _label, (_xbrl_concept, _kind) in globals().get("STATEMENTS", {}).get(_stmt_name, {}).items():
        _LABEL_TO_STMT_KIND.setdefault(_label, (_stmt_name, _kind))

# Gates the actual table creation / network / Delta-write sections below (4-7) -- mirrors
# 11__fetch_sec_xbrl.py's own `if RUN_TICKERS:` pattern exactly. Definitions above (PILOT_EU_
# ENTITIES, EUCurrentSource, process_eu_entity) stay unconditional/always-defined, same as `11`'s
# own get_cik/extract_series/SECXBRLSource -- this is what lets
# tests/test_eu_adapter_protocol_conformance.py load the real EUCurrentSource class locally
# (with RUN_EU_PILOT=False pre-seeded) without any network call or Spark session. A real
# Databricks run is completely unaffected -- RUN_EU_PILOT defaults True there.
RUN_EU_PILOT = globals().get("RUN_EU_PILOT", True)

# COMMAND ----------

# MAGIC %md ## 1. Pilot issuers (verified identity, Phase 5.0/5.1 research — not a universe)

# COMMAND ----------

PILOT_EU_ENTITIES = [
    # (ticker, LEI, mic, company name)
    ("FCC", "95980020140005178328", "XMAD", "Fomento de Construcciones y Contratas, S.A."),
    ("ALO", "96950032TUYMW11FB530", "XPAR", "Alstom"),
    ("NAI", "724500JXEXUGEATP5L52", "XAMS", "New Amsterdam Invest N.V."),
    ("FCT", "8156005BDF49128B6239", "MTAA", "Fincantieri S.p.A."),
]

# COMMAND ----------

# MAGIC %md ## 1b. Admitted EU entities (Phase 5.4 — real production input)
# MAGIC
# MAGIC `PILOT_EU_ENTITIES` above stays exactly as it was (Phase 5.1) — it remains the fixture
# MAGIC used by tests and by `EUCurrentSource()`'s own no-arg default, deliberately kept separate
# MAGIC from the real production path per Phase 5.4's own instruction not to conflate "test
# MAGIC fixture" with "real admitted-universe input". The real, production-capable entity source
# MAGIC is `main.config.eu_admission_candidates` (Phase 5.3) — this function reads it fresh at
# MAGIC run time, filtered to `admission_status = 'admitted'` ONLY (never `pending_esef_check` or
# MAGIC `rejected` — Phase 5.3's own admission decision is authoritative, not re-derived here).
# MAGIC No ticker list is ever hardcoded in this path.

# COMMAND ----------


def load_admitted_eu_entities(spark_session, catalog: str) -> list[tuple[str, str, str, str]]:
    """Real Spark query against `{catalog}.config.eu_admission_candidates` -- returns
    `(ticker, lei, mic, name)` tuples, the same shape `PILOT_EU_ENTITIES` uses, so
    `EUCurrentSource` can consume either interchangeably. Only `admission_status = 'admitted'`
    rows qualify. A real, possible edge case (not observed in the current 8-row admitted set,
    all of which have a resolved ticker): an admitted row with `ticker IS NULL` (ticker
    resolution is explicitly non-blocking for admission, per Phase 5.3's own
    `apply_ticker_enrichment` design) -- such a row is EXCLUDED here (this ingestion path,
    like the pre-existing pilot one, is ticker-keyed downstream in `financials_raw`/
    `financials`) and must be reported, not silently dropped -- see the caller's own
    `missing_ticker` failure logging below.
    """
    rows = spark_session.sql(f"""
        SELECT isin, ticker, lei, mic, issuer_name
        FROM {catalog}.config.eu_admission_candidates
        WHERE admission_status = 'admitted'
        ORDER BY isin
    """).collect()
    missing_ticker = [r["isin"] for r in rows if not r["ticker"]]
    if missing_ticker:
        print(f"  WARNING: {len(missing_ticker)} admitted candidate(s) have no resolved "
              f"ticker, excluded from this ingestion run: {missing_ticker}")
    return [
        (r["ticker"], r["lei"], r["mic"], r["issuer_name"])
        for r in rows if r["ticker"]
    ]


# COMMAND ----------

# MAGIC %md ## 2. EUCurrentSource — FundamentalsSource adapter (real network calls)

# COMMAND ----------

class EUCurrentSource:
    """`filings.xbrl.org` (ESEF/IFRS) as a `FundamentalsSource` -- the real, called European
    ingestion path (unlike SECXBRLSource in 11__fetch_sec_xbrl.py, which remains a proof).

    `entities` (Phase 5.4): the `(ticker, lei, mic, name)` list this source resolves tickers
    against. Defaults to `PILOT_EU_ENTITIES` (preserves every existing test's no-arg
    `EUCurrentSource()` behavior unchanged) -- the real production ingestion run below
    constructs this with `entities=load_admitted_eu_entities(...)` instead. This is the "small
    additive change" Phase 5.4 makes to the source contract -- no new interface, no second
    European source class.
    """

    source_id = EU_SOURCE_ID

    def __init__(self, entities=None):
        self._entities = entities if entities is not None else PILOT_EU_ENTITIES

    def discover_entities(self, tickers):
        """Identity is already known/verified (ADR-0010) -- no fuzzy matching, no
        ticker->LEI lookup at runtime (filings.xbrl.org's own /api/entities has no ticker field,
        per Phase 4 research)."""
        by_ticker = {t: (lei, name) for t, lei, _mic, name in self._entities}
        out = []
        for t in tickers:
            if t not in by_ticker:
                continue
            lei, name = by_ticker[t]
            out.append(entity_from_pilot(self.source_id, t, lei, name))
        return out

    def discover_filings(self, entity: SourceEntity):
        resp = requests.get(
            f"{EU_API_BASE}/api/entities/{entity.source_entity_id}/filings",
            headers=EU_HEADERS, timeout=30,
        )
        resp.raise_for_status()
        payload = resp.json()
        out = []
        for row in payload.get("data", []):
            attrs = row.get("attributes", {})
            out.append(SourceFiling(
                source_entity_id=entity.source_entity_id,
                source_filing_id=attrs.get("fxo_id", row.get("id", "")),
                filing_type="ESEF",
                filed_date=attrs.get("date_added"),
                period_end=attrs.get("period_end"),
            ))
        # Attach the raw attrs dicts (error_count/json_url/processed) alongside — the pure
        # amendment-selection logic in eu_current.select_filing_for_period operates on these
        # raw dicts, not on SourceFiling itself (SourceFiling has no error/processed fields).
        self._raw_filings_by_entity = getattr(self, "_raw_filings_by_entity", {})
        self._raw_filings_by_entity[entity.source_entity_id] = [
            row.get("attributes", {}) for row in payload.get("data", [])
        ]
        return out

    def raw_filings_for(self, source_entity_id: str) -> list[dict]:
        """The raw JSON:API `attributes` dicts (error_count/json_url/processed/...) for an
        entity's filings, as returned by the last `discover_filings` call for that entity —
        `SourceFiling` itself has no error/processed fields, so `eu_current.
        select_filing_for_period` (and this class's other methods) operate on these raw dicts
        directly rather than on `SourceFiling`."""
        return getattr(self, "_raw_filings_by_entity", {}).get(source_entity_id, [])

    def retrieve_facts(self, filing: SourceFiling):
        json_url = self._filing_json_url(filing)
        if not json_url:
            return []
        resp = requests.get(f"{EU_API_BASE}{json_url}", headers=EU_HEADERS, timeout=60)
        resp.raise_for_status()
        return extract_source_facts(resp.json(), filing, self.source_id)

    def _filing_json_url(self, filing: SourceFiling) -> str | None:
        for attrs in self.raw_filings_for(filing.source_entity_id):
            if attrs.get("fxo_id") == filing.source_filing_id:
                return attrs.get("json_url")
        return None

    def detect_metadata(self, entity: SourceEntity):
        """Verified evidence, not assumed: checks the real xBRL-JSON documentInfo.namespaces of
        one filing for `ifrs-full` presence, per rule that accounting standard must be detected,
        not defaulted to 'Europe = IFRS'."""
        raw_filings = self.raw_filings_for(entity.source_entity_id)
        ingestible = [f for f in raw_filings if f.get("error_count", 0) == 0 and f.get("json_url")]
        if not ingestible:
            return SourceEntityMetadata(accounting_framework=None, reporting_currency=None)
        newest = max(ingestible, key=lambda f: f["processed"])
        resp = requests.get(f"{EU_API_BASE}{newest['json_url']}", headers=EU_HEADERS, timeout=60)
        resp.raise_for_status()
        doc_info = resp.json().get("documentInfo", {})
        namespaces = doc_info.get("namespaces", {})
        framework = "ifrs-full" if "ifrs-full" in namespaces else None
        filing = SourceFiling(
            source_entity_id=entity.source_entity_id, source_filing_id=newest["fxo_id"],
            filing_type="ESEF", filed_date=newest.get("date_added"), period_end=newest.get("period_end"),
        )
        facts = extract_source_facts(resp.json(), filing, self.source_id)
        currencies = {f.source_currency for f in facts if f.source_currency}
        reporting_currency = next(iter(currencies)) if len(currencies) == 1 else None
        return SourceEntityMetadata(accounting_framework=framework, reporting_currency=reporting_currency)


_source: FundamentalsSource = EUCurrentSource()

# COMMAND ----------

# MAGIC %md ## 3. process_eu_entity — the real ingestion entry point (analogue of `process_ticker`)

# COMMAND ----------

def process_eu_entity(ticker: str, lei: str, mic: str, name: str, scraped_at_ts: datetime) -> tuple:
    """Returns (records: list[dict], failures: list[dict]).

    failures items: {"ticker","error_type","error_message","step"} -- same shape 11's
    ingestion_failures writer already expects, reused as-is.
    """
    records: list[dict] = []
    failures: list[dict] = []

    entities = _source.discover_entities([ticker])
    if not entities:
        failures.append({"ticker": ticker, "error_type": "entity_not_found",
                          "error_message": f"No pilot entity for ticker {ticker!r}", "step": "discover_entities"})
        return records, failures
    entity = entities[0]

    try:
        filings = _source.discover_filings(entity)
    except Exception as e:
        failures.append({"ticker": ticker, "error_type": "discover_filings_error",
                          "error_message": str(e)[:500], "step": "discover_filings"})
        return records, failures

    raw_by_id = {f["fxo_id"]: f for f in _source.raw_filings_for(lei)}
    by_period: dict[str, list[dict]] = {}
    for f in filings:
        by_period.setdefault(f.period_end, []).append(raw_by_id[f.source_filing_id])

    for period_end, group in sorted(by_period.items()):
        winner_raw, rejections = select_filing_for_period(group)
        for rej in rejections:
            if rej.reason == "superseded":
                continue  # normal, expected amendment behavior -- not a failure
            failures.append({
                "ticker": ticker, "error_type": rej.reason,
                "error_message": f"filing {rej.fxo_id!r} (period_end={period_end}) is not ingestible",
                "step": "discover_filings",
            })
        if winner_raw is None:
            continue  # already reported above if it was genuinely non-ingestible

        winner_filing = SourceFiling(
            source_entity_id=lei, source_filing_id=winner_raw["fxo_id"], filing_type="ESEF",
            filed_date=winner_raw.get("date_added"), period_end=period_end,
        )
        try:
            facts = _source.retrieve_facts(winner_filing)
        except Exception as e:
            failures.append({"ticker": ticker, "error_type": "retrieve_facts_error",
                              "error_message": str(e)[:500], "step": "retrieve_facts"})
            continue

        mapped = []
        for fact in facts:
            decision = map_source_fact_to_canonical(fact.source_concept)
            if decision is None:
                continue
            mapped.append((fact, decision))
        if not mapped:
            failures.append({"ticker": ticker, "error_type": "no_mappable_facts",
                              "error_message": f"filing {winner_raw['fxo_id']!r}: 0 of {len(facts)} facts mapped",
                              "step": "map_facts"})
            continue

        currencies = {fact.source_currency for fact, _d in mapped if fact.source_currency}
        if len(currencies) > 1:
            failures.append({"ticker": ticker, "error_type": "currency_mismatch",
                              "error_message": f"filing {winner_raw['fxo_id']!r}: multiple currencies {sorted(currencies)}",
                              "step": "currency_check"})
            continue

        period_end_date = date.fromisoformat(period_end)
        for fact, decision in mapped:
            stmt_name, kind = _LABEL_TO_STMT_KIND[decision.canonical_concept]
            period_start = date.fromisoformat(fact.source_period_start) if fact.source_period_start else None
            records.append({
                "ticker": ticker,
                "company": name,
                "stmt": stmt_name,
                "concept": decision.canonical_concept,
                "kind": kind,
                "fy": period_end_date.year,
                "fp": "FY",
                "form": "ESEF",
                "period_start": period_start,
                "period_end": period_end_date,
                "period_shape": classify_period_shape(fact.source_period_start, fact.source_period_end),
                "value": fact.source_value,
                "filed": date.fromisoformat(winner_raw["date_added"][:10]) if winner_raw.get("date_added") else None,
                "scraped_at": scraped_at_ts,
                "tag_namespace": "ifrs-full",
                "source_id": EU_SOURCE_ID,
            })

    return records, failures

# COMMAND ----------

# MAGIC %md ## 4. Ensure target Delta table & schema (same table `11__fetch_sec_xbrl` writes)

# COMMAND ----------

if RUN_EU_PILOT:
    from pyspark.sql.types import (
        DateType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    raw_full = f"{CATALOG}.{SCHEMA}.{RAW_TABLE}"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {raw_full} (
            ticker        STRING    NOT NULL,
            company       STRING,
            stmt          STRING    NOT NULL,
            concept       STRING    NOT NULL,
            kind          STRING    NOT NULL,
            fy            INT,
            fp            STRING,
            form          STRING,
            period_start  DATE,
            period_end    DATE      NOT NULL,
            period_shape  STRING,
            value         DOUBLE,
            filed         DATE,
            scraped_at    TIMESTAMP,
            tag_namespace STRING,
            source_id     STRING
        )
        USING DELTA
        PARTITIONED BY (ticker)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact'   = 'true'
        )
    """)

    EU_SCHEMA_DEF = StructType([
        StructField("ticker",        StringType(),    False),
        StructField("company",       StringType(),    True),
        StructField("stmt",          StringType(),    False),
        StructField("concept",       StringType(),    False),
        StructField("kind",          StringType(),    False),
        StructField("fy",            IntegerType(),   True),
        StructField("fp",            StringType(),    True),
        StructField("form",          StringType(),    True),
        StructField("period_start",  DateType(),      True),
        StructField("period_end",    DateType(),      False),
        StructField("period_shape",  StringType(),    True),
        StructField("value",         DoubleType(),    True),
        StructField("filed",         DateType(),      True),
        StructField("scraped_at",    TimestampType(), True),
        StructField("tag_namespace", StringType(),    True),
        StructField("source_id",     StringType(),    True),
    ])

# COMMAND ----------

# MAGIC %md ## 5. Run the ingestion — Phase 5.4: driven by `eu_admission_candidates`, not the
# MAGIC hardcoded pilot list
# MAGIC
# MAGIC `EU_ENTITIES_TO_INGEST` is loaded fresh from `main.config.eu_admission_candidates`
# MAGIC (`admission_status = 'admitted'` only, per Phase 5.3's own decision — never re-derived
# MAGIC here) — never hardcoded in this section. `_source` is reassigned to an
# MAGIC `EUCurrentSource` built against that real list; `PILOT_EU_ENTITIES`/the module-level
# MAGIC no-arg `_source` above remain the test/dev default, untouched.

# COMMAND ----------

if RUN_EU_PILOT:
    EU_ENTITIES_TO_INGEST = load_admitted_eu_entities(spark, CATALOG)
    print(f"Loaded {len(EU_ENTITIES_TO_INGEST)} admitted EU entities from "
          f"{CATALOG}.config.eu_admission_candidates")
    _source = EUCurrentSource(entities=EU_ENTITIES_TO_INGEST)

    scraped_at = datetime.utcnow()
    all_records: list[dict] = []
    all_failures: list[dict] = []

    for ticker, lei, mic, name in EU_ENTITIES_TO_INGEST:
        print(f"-- {ticker} ({name}) --")
        try:
            records, failures = process_eu_entity(ticker, lei, mic, name, scraped_at)
        except Exception as e:
            # One pilot issuer's failure must not abort the other three.
            failures = [{"ticker": ticker, "error_type": "unhandled_error",
                         "error_message": str(e)[:500], "step": "process_eu_entity"}]
            records = []
        all_records.extend(records)
        all_failures.extend(failures)
        years = sorted({r["fy"] for r in records})
        print(f"   {len(records)} facts across {len(years)} fiscal year(s): {years}")
        for f in failures:
            print(f"   ⚠ [{f['error_type']}] {f['error_message'][:120]}")

    print(f"\nTotal: {len(all_records)} records, {len(all_failures)} failures across "
          f"{len(PILOT_EU_ENTITIES)} pilot issuers")

# COMMAND ----------

# MAGIC %md ## 6. Write records to `financials_raw`

# COMMAND ----------

if RUN_EU_PILOT:
    if all_records:
        pdf = pd.DataFrame(all_records)
        pdf["fy"] = pdf["fy"].astype("Int64")
        sdf = spark.createDataFrame(pdf, schema=EU_SCHEMA_DEF)
        (sdf.write.mode("append").option("mergeSchema", "true").saveAsTable(raw_full))
        print(f"✓ {len(pdf)} EU_CURRENT record(s) appended to {raw_full}")
    else:
        print("✓ No EU_CURRENT records to write this run")

# COMMAND ----------

# MAGIC %md ## 7. Write ingestion failures to Delta (same table `11` writes)

# COMMAND ----------

if RUN_EU_PILOT:
    _failures_tbl = f"{CATALOG}.{SCHEMA}.ingestion_failures"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_failures_tbl} (
            ticker         STRING    NOT NULL,
            error_type     STRING    NOT NULL,
            error_message  STRING,
            step           STRING    NOT NULL,
            scraped_at     TIMESTAMP NOT NULL
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact'   = 'true'
        )
    """)

    if all_failures:
        _fail_schema = StructType([
            StructField("ticker",        StringType(),    False),
            StructField("error_type",    StringType(),    False),
            StructField("error_message", StringType(),    True),
            StructField("step",          StringType(),    False),
            StructField("scraped_at",    TimestampType(), False),
        ])
        _fail_records = [{
            "ticker": f["ticker"], "error_type": f["error_type"],
            "error_message": f["error_message"], "step": f["step"], "scraped_at": scraped_at,
        } for f in all_failures]
        spark.createDataFrame(_fail_records, schema=_fail_schema) \
             .write.mode("append").saveAsTable(_failures_tbl)
        print(f"✓ {len(_fail_records)} failure(s) written → {_failures_tbl}")
    else:
        print("✓ No ingestion failures to record")
