# Databricks notebook source
# MAGIC %md
# MAGIC # 30_analysis / 35__reconcile_filings  ⚠️ EXTERNAL reconciliation validator
# MAGIC
# MAGIC The first **external** validator: it reconciles published values in
# MAGIC `main.financials.financials` against an **independent ground truth**, instead of the
# MAGIC internal self-consistency checks (`34__invariants_check`, `validate-quarters`,
# MAGIC `validate-concept-hierarchy`). Two tiers, on purpose:
# MAGIC
# MAGIC - **Tier B/C — value / scale / sign / period (cheap, universe-wide).** Once the app has
# MAGIC   *chosen* a tag, its value should equal the raw companyfacts fact for that (tag, period)
# MAGIC   — same XBRL. We use the already-ingested `main.financials.financials_raw` as the
# MAGIC   companyfacts proxy (no network), bridging the app's *canonical* concept to the raw
# MAGIC   *pre-synonym* label via the reverse `CONCEPT_SYNONYMS` map. Derived rows
# MAGIC   (`is_derived = true`, e.g. `Q4 = FY − YTD_Q3`) are **excluded** — by design they are
# MAGIC   not a single raw fact. Flags `VALUE_MISMATCH`, `SCALE_SIGN`, `PERIOD_MISALIGN`.
# MAGIC - **Tier A — coverage / wrong-tag (golden set, v1).** Joins
# MAGIC   `main.config.reconciliation_oracle` (built by `14__fetch_oracle_statements` from each
# MAGIC   filer's presentation linkbase) to `financials`. Flags `COVERAGE_GAP` (oracle has a
# MAGIC   statement line for a period; app shows NULL/absent) split into **fixable** (us-gaap)
# MAGIC   vs **non-mappable** (`custom`); `ORACLE_VALUE_MISMATCH` (app has a value, but it
# MAGIC   disagrees with the **highest-priority presented** us-gaap tag — the wrong-tag /
# MAGIC   value-mangle case that Tier B/C cannot see, because there the app matches its own
# MAGIC   chosen raw fact; priority is statement-aware, so the Cash Flow line correctly expects the
# MAGIC   consolidated `ProfitLoss` incl. NCI — the indirect-method start — and reconciles against it);
# MAGIC   and `ORACLE_PARSE_FAIL` (counts against the validator).
# MAGIC
# MAGIC **Writes only `main.config.reconciliation_findings`** (append-only audit, partitioned by
# MAGIC `run_id`) + the view `main.config.reconciliation_open` + a clustered parquet export.
# MAGIC **Read-only** against `main.financials.*`.
# MAGIC
# MAGIC **Severity (deterministic):** `high` if the concept feeds a valuation (the `23__intrinsic_value`
# MAGIC inputs) or the cluster spans many tickers; `med` if displayed (in `concept_hierarchy`) but
# MAGIC non-valuation; `low` otherwise. **Root-cause / fix mechanism** is a deterministic map per
# MAGIC `issue_class` (no AI in v1) into the `xbrl-concept-mapping` failure-mode library.
# MAGIC
# MAGIC > **Validate-the-validator gate:** Tier A stays golden-set-only until its false-positive
# MAGIC > rate (tracked via `ORACLE_PARSE_FAIL` + manual review) drops below the agreed threshold.
# MAGIC > Do not scale Tier A to the universe here.
# MAGIC >
# MAGIC > Databricks-only: uses `spark`, `%run`, writes Delta + a driver-local parquet.

# COMMAND ----------

# MAGIC %run "../00_config/01__tickers"

# COMMAND ----------

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

FIN          = f"{CATALOG}.{SCHEMA}.financials"
RAW          = f"{CATALOG}.{SCHEMA}.{RAW_TABLE}"
ORACLE       = f"{CATALOG}.config.reconciliation_oracle"
FINDINGS     = f"{CATALOG}.config.reconciliation_findings"
OPEN_VIEW    = f"{CATALOG}.config.reconciliation_open"
HIER_JSON    = "../00_config/concept_hierarchy.json"
GOLDEN_JSON  = "../00_config/reconciliation_golden_set.json"
EXPORT_PATH  = "/tmp/reconciliation_findings.parquet"

# ── Tolerances / thresholds ──────────────────────────────────────────────────
VALUE_TOL          = 0.005   # 0.5% rel-diff — raw facts are the same XBRL, so near-exact
SCALE_MIN_LOG      = 0.5     # |log10(app/raw)| ≥ 0.5 → treat as a 10ⁿ scale candidate (~>3.16x)
CLUSTER_HIGH       = 25      # a finding cluster spanning ≥ this many tickers is bumped to 'high'
SAMPLE_PER_CLUSTER = 5       # evidence rows per cluster in the export

checked_at     = datetime.now(timezone.utc)
checked_at_sql = checked_at.strftime("%Y-%m-%d %H:%M:%S")   # clean Spark TIMESTAMP literal (no offset/micros)
run_id         = checked_at.strftime("%Y%m%d%H%M%S")
print(f"run_id = {run_id}  ({checked_at.isoformat(timespec='seconds')})")

# COMMAND ----------

# MAGIC %md ## 1. Reference maps → temp views
# MAGIC
# MAGIC - `_syn_map` — reverse `CONCEPT_SYNONYMS`: app canonical concept → every raw source label
# MAGIC   (identity included). Bridges `financials.concept` (post-synonym) to `financials_raw.concept`
# MAGIC   (pre-synonym) for the Tier-B/C join.
# MAGIC - `_concept_lookup` — `(tag, stmt) → canonical concept` for Tier-A tag→concept mapping
# MAGIC   (keyed on `stmt` so `NetIncomeLoss` resolves correctly on both IS and CF).
# MAGIC - `_valuation_inputs` / `_displayed` — severity inputs (valuation = `23`'s `NEEDED`;
# MAGIC   displayed = concepts in `concept_hierarchy`).
# MAGIC - `_golden` — the Tier-A scope (golden set ∪ `COMBINED_FILERS`) for `ORACLE_PARSE_FAIL`.

# COMMAND ----------

def _strip_jsonc(path: str):
    raw = Path(path).read_text(encoding="utf-8")
    return json.loads("\n".join(ln for ln in raw.splitlines() if not ln.strip().startswith("//")))


# Reverse-synonym map (app canonical -> raw source label, identity included)
_syn_rows = set()
for _stmt, _cmap in STATEMENTS.items():
    for _label in _cmap:
        _syn_rows.add((CONCEPT_SYNONYMS.get(_label, _label), _label))

# (tag, stmt) -> (canonical concept, source label, priority). `priority` mirrors
# CONCEPT_PRIORITY (lower = the tag the merge prefers when synonyms coexist) so Tier A can
# reconstruct which presented tag the app SHOULD have published. If one tag maps from several
# labels (rare), keep the most-preferred (lowest-priority) source label.
_cl_map: dict[tuple, tuple] = {}
for _stmt, _cmap in STATEMENTS.items():
    for _label, (_xbrl, _kind) in _cmap.items():
        _canon = CONCEPT_SYNONYMS.get(_label, _label)
        _prio  = concept_priority(_stmt, _label)   # statement-aware (Cash Flow → ProfitLoss wins)
        for _tag in ([_xbrl] if isinstance(_xbrl, str) else _xbrl):
            _k = (_tag, _stmt)
            if _k not in _cl_map or _prio < _cl_map[_k][2]:
                _cl_map[_k] = (_canon, _label, _prio)
_cl_rows = [(t, s, c, lbl, prio) for (t, s), (c, lbl, prio) in _cl_map.items()]

# Valuation-input concepts (canonical) — mirror of NEEDED in 23__intrinsic_value.py.
VALUATION_INPUTS = {
    "Net Income", "Revenue", "Operating Income", "EPS Diluted", "Shares Diluted",
    "Total Stockholders Equity", "Total Assets", "Long-term Debt", "Short-term Debt",
    "Cash & Equivalents", "Short-term Investments", "Retained Earnings",
    "Operating Cash Flow", "CapEx", "Depreciation & Amortization",
    "Stock-based Compensation", "Changes in Working Capital",
}

# Displayed concepts = every concept node in concept_hierarchy.json
_hier = _strip_jsonc(HIER_JSON)
DISPLAYED = {
    ch["concept"]
    for stmt, nodes in _hier.items() if not stmt.startswith("_")
    for node in nodes for ch in node.get("children", []) if "concept" in ch
}

# Golden scope (golden set ∪ COMBINED_FILERS)
_golden = _strip_jsonc(GOLDEN_JSON)
_golden_rows = _golden["tickers"] if isinstance(_golden, dict) else _golden
_golden_tickers = {r["ticker"].upper() for r in _golden_rows if r.get("ticker")}
_golden_tickers |= {t.upper() for t in (COMBINED_FILERS or {})}

spark.createDataFrame(list(_syn_rows), "app_concept string, raw_concept string") \
     .createOrReplaceTempView("_syn_map")
spark.createDataFrame(_cl_rows, "tag string, stmt string, label_canonical string, source_label string, priority int") \
     .createOrReplaceTempView("_concept_lookup")
spark.createDataFrame([(c,) for c in sorted(VALUATION_INPUTS)], "concept string") \
     .createOrReplaceTempView("_valuation_inputs")
spark.createDataFrame([(c,) for c in sorted(DISPLAYED)], "concept string") \
     .createOrReplaceTempView("_displayed")
spark.createDataFrame([(t,) for t in sorted(_golden_tickers)], "ticker string") \
     .createOrReplaceTempView("_golden")

print(f"✓ syn_map rows: {len(_syn_rows)} | concept_lookup rows: {len(_cl_rows)}")
print(f"✓ valuation inputs: {len(VALUATION_INPUTS)} | displayed: {len(DISPLAYED)} | "
      f"golden tickers: {len(_golden_tickers)}")

# COMMAND ----------

# MAGIC %md ## 2. Tier B/C — value / scale / sign / period (universe-wide)
# MAGIC
# MAGIC For each published non-derived value, gather the candidate raw facts at the **same**
# MAGIC `period_end` (across the concept's synonym labels). No within-tolerance match ⇒ a finding.
# MAGIC Heavy step (joins `financials_raw`, ~80M rows) — intended for the monthly/on-demand job.

# COMMAND ----------

# Candidate raw values per published (non-derived) app row, same period_end.
_bc_flagged = spark.sql(f"""
    WITH app AS (
        SELECT ticker, stmt, concept AS app_concept, period_type, period_end, fiscal_year,
               value AS app_value
        FROM {FIN}
        WHERE value IS NOT NULL
          AND (is_derived IS NULL OR is_derived = false)
          AND period_type IN ('FY','Q1','Q2','Q3','Q4')
    )
    SELECT a.ticker, a.stmt, a.app_concept, a.period_type, a.period_end, a.fiscal_year,
           a.app_value, r.value AS raw_value,
           ABS((a.app_value - r.value) / NULLIF(r.value, 0))             AS rel,
           ABS((a.app_value + r.value) / NULLIF(r.value, 0))             AS rel_sign,
           ABS(a.app_value / NULLIF(r.value, 0))                         AS aratio
    FROM app a
    JOIN _syn_map s ON s.app_concept = a.app_concept
    JOIN {RAW} r
      ON r.ticker = a.ticker AND r.stmt = a.stmt
     AND r.concept = s.raw_concept AND r.period_end = a.period_end
    WHERE r.value IS NOT NULL
""").localCheckpoint(eager=True)
_bc_flagged.createOrReplaceTempView("_bc_flagged")
print(f"  candidate (app × raw) pairs: {_bc_flagged.count():,}")

# COMMAND ----------

# Per app-row key K = (ticker, stmt, app_concept, period_type, period_end): match/scale/sign flags.
spark.sql(f"""
    SELECT ticker, stmt, app_concept, period_type, period_end,
           MAX(fiscal_year) AS fiscal_year, MAX(app_value) AS app_value,
           MIN(rel) AS min_rel,
           -- `rel` is NULL when raw_value = 0 (NULLIF), so exact equality is added explicitly:
           -- without it app_value = raw_value = 0 (perfect agreement) would never match and would
           -- surface as a spurious VALUE_MISMATCH (abs_diff = 0, rel_diff = NULL).
           MAX(CASE WHEN rel <= {VALUE_TOL} OR app_value = raw_value THEN 1 ELSE 0 END) AS any_match,
           MAX(CASE WHEN rel_sign <= {VALUE_TOL} THEN 1 ELSE 0 END) AS any_sign,
           MAX(CASE WHEN aratio > 0 AND ABS(LOG10(aratio)) >= {SCALE_MIN_LOG}
                     AND ABS(aratio - POWER(10, ROUND(LOG10(aratio))))
                         / POWER(10, ROUND(LOG10(aratio))) <= {VALUE_TOL}
                    THEN 1 ELSE 0 END) AS any_scale
    FROM _bc_flagged
    GROUP BY ticker, stmt, app_concept, period_type, period_end
""").localCheckpoint(eager=True).createOrReplaceTempView("_bc_scored")

# Representative raw value per K = the candidate closest to the app value (evidence).
spark.sql("""
    SELECT ticker, stmt, app_concept, period_type, period_end, raw_value AS rep_raw FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY ticker, stmt, app_concept, period_type, period_end
            ORDER BY ABS(app_value - raw_value)) AS rn
        FROM _bc_flagged
    ) WHERE rn = 1
""").createOrReplaceTempView("_bc_rep")

# PERIOD_MISALIGN: among UNMATCHED app rows only, does the app value match a raw fact for the
# same concept at a DIFFERENT period_end? (the fiscal/calendar anchor signature)
spark.sql(f"""
    SELECT DISTINCT u.ticker, u.stmt, u.app_concept, u.period_type, u.period_end
    FROM (SELECT * FROM _bc_scored WHERE any_match = 0) u
    JOIN _syn_map s ON s.app_concept = u.app_concept
    JOIN {RAW} r
      ON r.ticker = u.ticker AND r.stmt = u.stmt AND r.concept = s.raw_concept
     AND r.period_end <> u.period_end AND r.value IS NOT NULL
     AND ABS((u.app_value - r.value) / NULLIF(r.value, 0)) <= {VALUE_TOL}
""").createOrReplaceTempView("_bc_pm")

# Assemble Tier-B/C findings (only unmatched rows). Precedence: SCALE_SIGN > PERIOD_MISALIGN > VALUE_MISMATCH.
df_bc = spark.sql("""
    SELECT
        'B/C'                               AS tier,
        sc.ticker, CAST(NULL AS STRING)     AS cik, CAST(NULL AS STRING) AS accession,
        CAST(NULL AS STRING)                AS form,
        sc.fiscal_year, sc.period_end, sc.period_type, sc.stmt,
        sc.app_concept                      AS concept,
        CAST(NULL AS STRING)                AS oracle_label,
        CAST(NULL AS STRING)                AS oracle_tag,
        CAST(NULL AS STRING)                AS tag_namespace,
        CASE WHEN sc.any_scale = 1 OR sc.any_sign = 1 THEN 'SCALE_SIGN'
             WHEN pm.ticker IS NOT NULL                THEN 'PERIOD_MISALIGN'
             ELSE 'VALUE_MISMATCH' END      AS issue_class,
        sc.app_value, rep.rep_raw           AS oracle_value,
        ABS(sc.app_value - rep.rep_raw)     AS abs_diff,
        sc.min_rel                          AS rel_diff
    FROM _bc_scored sc
    JOIN _bc_rep rep USING (ticker, stmt, app_concept, period_type, period_end)
    LEFT JOIN _bc_pm pm USING (ticker, stmt, app_concept, period_type, period_end)
    WHERE sc.any_match = 0
""")
print(f"  Tier B/C findings: {df_bc.count():,}")

# COMMAND ----------

# MAGIC %md ## 3. Tier A — coverage / wrong-tag (golden set)

# COMMAND ----------

def _table_exists(fqn: str) -> bool:
    try:
        spark.sql(f"SELECT 1 FROM {fqn} LIMIT 1").collect()
        return True
    except Exception:
        return False


_have_oracle = _table_exists(ORACLE)
if not _have_oracle:
    print(f"⚠ {ORACLE} not found — run 14__fetch_oracle_statements first. "
          f"Tier A (COVERAGE_GAP / ORACLE_PARSE_FAIL) skipped this run.")

# COVERAGE_GAP — oracle (FY) has a line for a period; app shows NULL/absent for the mapped concept.
if _have_oracle:
    df_cov = spark.sql(f"""
        WITH oa AS (
            SELECT o.cik, o.ticker, o.accession, o.form, o.fiscal_year, o.period_end,
                   o.period_type, o.stmt, o.oracle_tag, o.tag_namespace, o.oracle_label,
                   o.oracle_value, cl.label_canonical,
                   CASE WHEN o.tag_namespace = 'us-gaap' AND cl.label_canonical IS NOT NULL
                        THEN cl.label_canonical ELSE o.oracle_tag END AS mapped_concept,
                   (o.tag_namespace = 'us-gaap' AND cl.label_canonical IS NOT NULL) AS is_tracked
            FROM {ORACLE} o
            LEFT JOIN _concept_lookup cl ON cl.tag = o.oracle_tag AND cl.stmt = o.stmt
            WHERE o.period_type = 'FY'
        ),
        gap AS (
            SELECT oa.* FROM oa
            LEFT JOIN {FIN} f
              ON f.ticker = oa.ticker AND f.stmt = oa.stmt
             AND f.concept = oa.mapped_concept AND f.period_end = oa.period_end
             AND f.period_type = 'FY' AND f.value IS NOT NULL
            WHERE f.ticker IS NULL
        ),
        deduped AS (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY ticker, stmt, mapped_concept, period_end
                ORDER BY accession) AS rn
            FROM gap
        )
        SELECT
            'A'                          AS tier,
            ticker, cik, accession, form, fiscal_year, period_end, period_type, stmt,
            mapped_concept               AS concept,
            oracle_label, oracle_tag, tag_namespace,
            'COVERAGE_GAP'               AS issue_class,
            CAST(NULL AS DOUBLE)         AS app_value,
            oracle_value,
            CAST(NULL AS DOUBLE)         AS abs_diff,
            CAST(NULL AS DOUBLE)         AS rel_diff
        FROM deduped WHERE rn = 1
    """)
    print(f"  Tier A COVERAGE_GAP findings: {df_cov.count():,}")

    # ORACLE_PARSE_FAIL — a golden (ticker, stmt) with NO us-gaap FY oracle line (parser/fetch miss).
    df_pf = spark.sql(f"""
        WITH expected AS (
            SELECT g.ticker, st.stmt
            FROM _golden g
            CROSS JOIN (SELECT explode(array('Income Statement','Balance Sheet','Cash Flow')) AS stmt) st
        ),
        present AS (
            SELECT DISTINCT ticker, stmt FROM {ORACLE}
            WHERE tag_namespace = 'us-gaap' AND period_type = 'FY'
        )
        SELECT
            'A'                          AS tier,
            e.ticker, CAST(NULL AS STRING) AS cik, CAST(NULL AS STRING) AS accession,
            CAST(NULL AS STRING)         AS form,
            CAST(NULL AS INT)            AS fiscal_year, CAST(NULL AS DATE) AS period_end,
            'FY'                         AS period_type, e.stmt,
            CAST(NULL AS STRING)         AS concept,
            CAST(NULL AS STRING)         AS oracle_label, CAST(NULL AS STRING) AS oracle_tag,
            CAST(NULL AS STRING)         AS tag_namespace,
            'ORACLE_PARSE_FAIL'          AS issue_class,
            CAST(NULL AS DOUBLE)         AS app_value, CAST(NULL AS DOUBLE) AS oracle_value,
            CAST(NULL AS DOUBLE)         AS abs_diff, CAST(NULL AS DOUBLE) AS rel_diff
        FROM expected e
        LEFT JOIN present p ON p.ticker = e.ticker AND p.stmt = e.stmt
        WHERE p.ticker IS NULL
    """)
    print(f"  Tier A ORACLE_PARSE_FAIL findings: {df_pf.count():,}")

    # ORACLE_VALUE_MISMATCH — the app published a non-NULL FY value, but it disagrees with the
    # value carried by the **highest-priority presented us-gaap tag** for that statement line.
    # The `winner` is the tag the merge SHOULD have picked (MIN priority, then larger magnitude —
    # mirrors concept_priority() → value-desc in 21). `priority` is now STATEMENT-AWARE, so on the
    # Cash Flow line the winner is the consolidated ProfitLoss (the indirect-method start) — the
    # same tag the merge now publishes — and CF Net Income reconciles instead of being excluded.
    # Two failure modes collapse here:
    #   • wrong tag among coexisting synonyms (e.g. WMB Revenue picked `contract` over `Revenues`)
    #   • value mangled at merge for the right tag.
    # Quiet by construction when the app matches the winner. Sign flips that the linkbase explains
    # (`negated`) are excluded; oracle_value = 0 is skipped (rel_diff NULL) to avoid div-by-zero noise.
    # One concept stays carved out (see the WHERE clause): Sales of Investments on the Cash Flow
    # line (a sales-vs-maturities sibling-line ambiguity — validator mapping noise, not a value
    # mangle; see the WHERE comment for the full rationale). The former Net Income / Cash Flow
    # carve-out was REMOVED: with statement-aware priority the app and the oracle winner now agree
    # on the consolidated ProfitLoss, so the line validates cleanly.
    df_vm = spark.sql(f"""
        WITH oa AS (
            SELECT o.cik, o.ticker, o.accession, o.form, o.fiscal_year, o.period_end, o.stmt,
                   o.oracle_tag, o.tag_namespace, o.oracle_label, o.negated, o.oracle_value,
                   cl.label_canonical AS concept, cl.priority
            FROM {ORACLE} o
            JOIN _concept_lookup cl ON cl.tag = o.oracle_tag AND cl.stmt = o.stmt
            WHERE o.period_type = 'FY' AND o.tag_namespace = 'us-gaap'
        ),
        winner AS (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY ticker, stmt, concept, period_end
                ORDER BY priority ASC, ABS(oracle_value) DESC) AS rn
            FROM oa
        ),
        joined AS (
            SELECT w.*, f.value AS app_value
            FROM (SELECT * FROM winner WHERE rn = 1) w
            JOIN {FIN} f
              ON f.ticker = w.ticker AND f.stmt = w.stmt
             AND f.concept = w.concept AND f.period_end = w.period_end
             AND f.period_type = 'FY' AND f.value IS NOT NULL
        )
        SELECT
            'A'                          AS tier,
            ticker, cik, accession, form, fiscal_year, period_end, 'FY' AS period_type, stmt,
            concept, oracle_label, oracle_tag, tag_namespace,
            'ORACLE_VALUE_MISMATCH'      AS issue_class,
            app_value, oracle_value,
            ABS(app_value - oracle_value)                            AS abs_diff,
            ABS((app_value - oracle_value) / NULLIF(oracle_value, 0)) AS rel_diff
        FROM joined
        WHERE app_value <> oracle_value
          AND ABS((app_value - oracle_value) / NULLIF(oracle_value, 0)) > {VALUE_TOL}
          AND NOT (negated AND ABS((app_value + oracle_value) / NULLIF(oracle_value, 0)) <= {VALUE_TOL})
          -- (The former Net Income / Cash Flow carve-out was removed: statement-aware priority now
          -- makes the consolidated ProfitLoss the oracle winner AND the app's published value, so
          -- the indirect-method reconciliation start reconciles cleanly instead of being excluded.)
          -- Sales of Investments on the Cash Flow statement is a known sibling-line ambiguity, not a
          -- value mangle: securities-heavy filers present *sales* and *maturities/calls* of marketable
          -- securities as SEPARATE investing lines. The concept intentionally tracks only the sales-
          -- proceeds tag, but the oracle winner's magnitude tie-break (ABS(oracle_value) DESC) selects
          -- the larger maturities line — so app (sales) always trails oracle (maturities). Capturing the
          -- economic total would require summing two distinct tags (a definitional change, declined);
          -- left as-is, this is validator mapping noise. Drop it to keep the bucket clean.
          AND NOT (concept = 'Sales of Investments' AND stmt = 'Cash Flow')
          -- Short-term Debt on the Balance Sheet is intentionally a SUM of two disjoint current-debt
          -- components: ShortTermBorrowings (commercial paper / revolver) + LongTermDebtCurrent
          -- (current maturities of long-term debt). DebtCurrent is the us-gaap aggregate of both;
          -- when a filer presents only the parts (LIN, WMT, many CP issuers) the app sums them at
          -- ingestion (extract_series_aggregate_or_sum / AGGREGATE_OR_SUM_CONCEPTS in 11). The Tier-A
          -- oracle is single-line — its magnitude tie-break picks the larger component alone — so the
          -- app (the sum) ALWAYS exceeds the oracle winner by the other component. Unlike Sales of
          -- Investments, summing here was the CORRECT fix (it un-did an undercount of leverage), so
          -- the residual mismatch is structural validator noise, not a mangle. The value is still
          -- guarded by Tier B/C, which now matches the app against its own (summed) raw fact. Drop it.
          AND NOT (concept = 'Short-term Debt' AND stmt = 'Balance Sheet')
    """)
    print(f"  Tier A ORACLE_VALUE_MISMATCH findings: {df_vm.count():,}")

# COMMAND ----------

# MAGIC %md ## 4. Unify, enrich (severity / root-cause / evidence), write findings

# COMMAND ----------

_findings = df_bc
if _have_oracle:
    _findings = _findings.unionByName(df_cov).unionByName(df_vm).unionByName(df_pf)
_findings.createOrReplaceTempView("_findings_raw")

# Deterministic root-cause / fix-mechanism map + base severity, then a cluster bump to 'high'
# for any (issue_class, concept, stmt, root_cause) spanning ≥ CLUSTER_HIGH tickers.
_enriched = spark.sql(f"""
    WITH base AS (
        SELECT r.*,
            CASE
                WHEN issue_class = 'COVERAGE_GAP' AND tag_namespace = 'custom'
                     THEN 'filer custom-namespace statement line, no us-gaap equivalent'
                WHEN issue_class = 'COVERAGE_GAP' AND oracle_tag IS NOT NULL
                     AND oracle_tag = concept
                     THEN 'us-gaap statement line not in the concept map'
                WHEN issue_class = 'COVERAGE_GAP'
                     THEN 'mapped us-gaap tag not landing in financials (synonym/priority/merge gap)'
                WHEN issue_class = 'SCALE_SIGN'
                     THEN '10^n scale factor or sign flip vs the raw fact'
                WHEN issue_class = 'PERIOD_MISALIGN'
                     THEN 'app value matches the raw fact at a different period_end (fiscal/calendar anchor)'
                WHEN issue_class = 'VALUE_MISMATCH'
                     THEN 'app value diverges from the raw fact (same XBRL — precision/merge mangling)'
                WHEN issue_class = 'ORACLE_VALUE_MISMATCH'
                     THEN 'app value differs from the highest-priority presented statement line (wrong synonym tag picked, or value mangled at merge)'
                WHEN issue_class = 'ORACLE_PARSE_FAIL'
                     THEN 'linkbase oracle produced no us-gaap FY line where one was expected'
            END AS root_cause_hypothesis,
            CASE
                WHEN issue_class = 'COVERAGE_GAP' AND tag_namespace = 'custom' THEN 'custom — not mappable'
                WHEN issue_class = 'COVERAGE_GAP' AND oracle_tag = concept     THEN 'mechanism-1 tag-list'
                WHEN issue_class = 'COVERAGE_GAP'                              THEN 'synonym+priority'
                WHEN issue_class = 'SCALE_SIGN'                               THEN 'scale fix 21__'
                WHEN issue_class = 'PERIOD_MISALIGN'                          THEN 'period anchor 21__/22__'
                WHEN issue_class = 'VALUE_MISMATCH'                           THEN 'inspect 21__clean_and_merge / precision'
                WHEN issue_class = 'ORACLE_VALUE_MISMATCH'                    THEN 'synonym+priority — CONCEPT_PRIORITY / inspect 21__clean_and_merge'
                WHEN issue_class = 'ORACLE_PARSE_FAIL'                        THEN 'validator — linkbase parser'
            END AS suggested_mechanism,
            CASE
                WHEN concept IN (SELECT concept FROM _valuation_inputs) THEN 'high'
                WHEN concept IN (SELECT concept FROM _displayed)        THEN 'med'
                ELSE 'low'
            END AS severity_base,
            CASE
                WHEN accession IS NOT NULL AND cik IS NOT NULL
                     THEN CONCAT('https://www.sec.gov/Archives/edgar/data/',
                                 CAST(CAST(cik AS BIGINT) AS STRING), '/',
                                 REPLACE(accession, '-', ''), '/')
                ELSE CONCAT('https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=',
                            ticker, '&type=10-K&dateb=&owner=include&count=40')
            END AS evidence_url
        FROM _findings_raw r
    ),
    clustered AS (
        SELECT *, SIZE(COLLECT_SET(ticker) OVER (
            PARTITION BY issue_class, concept, stmt, root_cause_hypothesis)) AS cluster_tickers
        FROM base
    )
    SELECT
        '{run_id}' AS run_id, TIMESTAMP('{checked_at_sql}') AS checked_at,
        ticker, cik, accession, form, fiscal_year, period_end, period_type, stmt, concept,
        oracle_label, oracle_tag, tag_namespace, issue_class, app_value, oracle_value,
        abs_diff, rel_diff,
        CASE WHEN cluster_tickers >= {CLUSTER_HIGH} THEN 'high' ELSE severity_base END AS severity,
        root_cause_hypothesis, suggested_mechanism, evidence_url,
        'OPEN' AS status, CAST(NULL AS STRING) AS notes
    FROM clustered
""")
_enriched = _enriched.localCheckpoint(eager=True)
print(f"✓ Total findings this run: {_enriched.count():,}")
_enriched.groupBy("issue_class", "severity").count().orderBy("issue_class", "severity").show(50, truncate=False)

# COMMAND ----------

# Findings table — append-only, partitioned by run_id. Schema-drift guard (CREATE IF NOT EXISTS
# does not evolve schema): drop+recreate if columns drift (rebuildable audit table).
_DESIRED = [
    "run_id", "checked_at", "ticker", "cik", "accession", "form", "fiscal_year", "period_end",
    "period_type", "stmt", "concept", "oracle_label", "oracle_tag", "tag_namespace", "issue_class",
    "app_value", "oracle_value", "abs_diff", "rel_diff", "severity", "root_cause_hypothesis",
    "suggested_mechanism", "evidence_url", "status", "notes",
]
if _table_exists(FINDINGS):
    if [f.name for f in spark.table(FINDINGS).schema.fields] != _DESIRED:
        print(f"⚠ schema drift on {FINDINGS} — dropping and recreating.")
        spark.sql(f"DROP TABLE {FINDINGS}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {FINDINGS} (
        run_id                STRING,
        checked_at            TIMESTAMP,
        ticker                STRING,
        cik                   STRING,
        accession             STRING,
        form                  STRING,
        fiscal_year           INT,
        period_end            DATE,
        period_type           STRING,
        stmt                  STRING,
        concept               STRING,
        oracle_label          STRING,
        oracle_tag            STRING,
        tag_namespace         STRING,
        issue_class           STRING,
        app_value             DOUBLE,
        oracle_value          DOUBLE,
        abs_diff              DOUBLE,
        rel_diff              DOUBLE,
        severity              STRING,
        root_cause_hypothesis STRING,
        suggested_mechanism   STRING,
        evidence_url          STRING,
        status                STRING,
        notes                 STRING
    )
    USING DELTA
    PARTITIONED BY (run_id)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

# Idempotent re-run of the SAME run_id: replace that partition (no duplicate appends).
_enriched.select(*_DESIRED).write.mode("overwrite") \
    .option("replaceWhere", f"run_id = '{run_id}'") \
    .saveAsTable(FINDINGS)
print(f"✓ Wrote {_enriched.count():,} findings → {FINDINGS} (run_id={run_id})")

spark.sql(f"""
    CREATE OR REPLACE VIEW {OPEN_VIEW} AS
    SELECT * FROM {FINDINGS}
    WHERE run_id = (SELECT MAX(run_id) FROM {FINDINGS}) AND status = 'OPEN'
""")
print(f"✓ View refreshed: {OPEN_VIEW} (latest run_id, status=OPEN)")

# COMMAND ----------

# MAGIC %md ## 5. Export — clustered open findings → parquet (the review gate's source)
# MAGIC
# MAGIC Clustering key `(issue_class, concept, stmt, root_cause_hypothesis)` so e.g. 200 tickers
# MAGIC missing Cost of Revenue collapse to one review item. The markdown review (generated later,
# MAGIC outside this notebook) is built from this export.

# COMMAND ----------

_clusters = spark.sql(f"""
    WITH f AS (SELECT * FROM {FINDINGS} WHERE run_id = '{run_id}' AND status = 'OPEN'),
    ranked AS (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY issue_class, concept, stmt, root_cause_hypothesis
            ORDER BY severity, ticker, period_end) AS rn
        FROM f
    ),
    samples AS (
        SELECT issue_class, concept, stmt, root_cause_hypothesis,
               COLLECT_LIST(NAMED_STRUCT(
                   'ticker', ticker, 'period_end', CAST(period_end AS STRING),
                   'app_value', app_value, 'oracle_value', oracle_value,
                   'oracle_tag', oracle_tag, 'evidence_url', evidence_url)) AS sample
        FROM ranked WHERE rn <= {SAMPLE_PER_CLUSTER}
        GROUP BY issue_class, concept, stmt, root_cause_hypothesis
    ),
    agg AS (
        SELECT issue_class, concept, stmt, root_cause_hypothesis,
               MAX(suggested_mechanism) AS suggested_mechanism,
               MAX(tag_namespace)       AS tag_namespace,
               CASE WHEN MAX(CASE severity WHEN 'high' THEN 3 WHEN 'med' THEN 2 ELSE 1 END) = 3 THEN 'high'
                    WHEN MAX(CASE severity WHEN 'high' THEN 3 WHEN 'med' THEN 2 ELSE 1 END) = 2 THEN 'med'
                    ELSE 'low' END      AS severity,
               COUNT(*)                 AS n_rows,
               COUNT(DISTINCT ticker)   AS n_tickers,
               MIN(fiscal_year)         AS fy_min, MAX(fiscal_year) AS fy_max
        FROM f
        GROUP BY issue_class, concept, stmt, root_cause_hypothesis
    )
    SELECT a.issue_class, a.concept, a.stmt, a.root_cause_hypothesis, a.suggested_mechanism,
           a.tag_namespace, a.severity, a.n_rows, a.n_tickers, a.fy_min, a.fy_max, s.sample
    FROM agg a LEFT JOIN samples s USING (issue_class, concept, stmt, root_cause_hypothesis)
    ORDER BY CASE a.severity WHEN 'high' THEN 0 WHEN 'med' THEN 1 ELSE 2 END,
             a.n_tickers DESC, a.issue_class, a.concept
""").toPandas()

# Serialize the sample struct-list to a JSON string column (portable parquet, no nested schema churn).
_clusters["sample"] = _clusters["sample"].apply(
    lambda s: json.dumps([dict(r.asDict()) if hasattr(r, "asDict") else dict(r) for r in s], default=str)
    if s is not None else "[]")
_clusters["run_id"] = run_id

_clusters.attrs = {}   # clear Spark Connect PlanMetrics (see 51__export_dashboard_data)
_clusters.to_parquet(EXPORT_PATH, index=False)
print(f"✓ Exported {len(_clusters):,} cluster(s) → {EXPORT_PATH}")
print(_clusters[["issue_class", "concept", "stmt", "severity", "n_tickers", "n_rows"]]
      .head(30).to_string(index=False))

# COMMAND ----------

# MAGIC %md ## 6. Summary

# COMMAND ----------

spark.sql(f"""
    SELECT issue_class, severity, COUNT(*) AS rows, COUNT(DISTINCT ticker) AS tickers
    FROM {FINDINGS} WHERE run_id = '{run_id}'
    GROUP BY issue_class, severity
    ORDER BY issue_class, severity
""").display()
