"""Tests for Phase 5.4's admission-driven EUCurrentSource entity input
(fundamentals_pipeline/10__ingestion/16__fetch_eu_xbrl.py's `load_admitted_eu_entities()` and
`EUCurrentSource(entities=...)`).

No live network/Spark -- `load_admitted_eu_entities()` is exercised against a small fake Spark
session (a `.sql(query).collect()` stub returning fake Row-like dicts), the same "captured
fixture" discipline the rest of this test suite uses for network-touching code. Real Databricks
execution is validated separately (see docs/phase5-4-european-vertical-slice.md), not by CI.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

_NOTEBOOK_PATH = (
    Path(__file__).resolve().parent.parent
    / "fundamentals_pipeline"
    / "10__ingestion"
    / "16__fetch_eu_xbrl.py"
)


def _load_eu_adapter_module():
    spec = importlib.util.spec_from_file_location("_eu_adapter_under_test_54", _NOTEBOOK_PATH)
    module = importlib.util.module_from_spec(spec)
    module.RUN_EU_PILOT = False
    spec.loader.exec_module(module)
    return module


class _FakeRow(dict):
    """Mimics pyspark.sql.Row's `row["col"]` access -- a dict subclass is enough for this."""


class _FakeSparkSession:
    def __init__(self, rows: list[dict]):
        self._rows = [_FakeRow(r) for r in rows]
        self.last_sql: str | None = None

    def sql(self, query: str):
        self.last_sql = query
        return self

    def collect(self):
        return self._rows


# ── EUCurrentSource(entities=...) ───────────────────────────────────────────────────────────


def test_eu_current_source_no_arg_defaults_to_pilot_entities():
    """Regression: every existing test/call site that constructs EUCurrentSource() with no
    args must keep getting PILOT_EU_ENTITIES-only behavior."""
    module = _load_eu_adapter_module()
    source = module.EUCurrentSource()
    entities = source.discover_entities(["FCC", "IBE"])  # IBE not in PILOT_EU_ENTITIES
    assert len(entities) == 1
    assert entities[0].ticker == "FCC"


def test_eu_current_source_custom_entities_overrides_pilot_list():
    module = _load_eu_adapter_module()
    custom = [("IBE", "5QK37QC7NWOJ8D7WVQ45", "XMAD", "Iberdrola SA")]
    source = module.EUCurrentSource(entities=custom)

    # A pilot-only ticker must NOT resolve against a custom entity list.
    assert source.discover_entities(["FCC"]) == []

    entities = source.discover_entities(["IBE"])
    assert len(entities) == 1
    assert entities[0].source_entity_id == "5QK37QC7NWOJ8D7WVQ45"
    assert entities[0].issuer_id == "EU_CURRENT:5QK37QC7NWOJ8D7WVQ45"


def test_eu_current_source_empty_entities_list_resolves_nothing():
    module = _load_eu_adapter_module()
    source = module.EUCurrentSource(entities=[])
    assert source.discover_entities(["FCC"]) == []


# ── load_admitted_eu_entities() ─────────────────────────────────────────────────────────────


def test_load_admitted_eu_entities_returns_pilot_shaped_tuples():
    module = _load_eu_adapter_module()
    fake_spark = _FakeSparkSession([
        {"isin": "ES0122060314", "ticker": "FCC", "lei": "95980020140005178328",
         "mic": "XMAD", "issuer_name": "Fomento de Construcciones y Contratas, S.A."},
        {"isin": "FR0010220475", "ticker": "ALO", "lei": "96950032TUYMW11FB530",
         "mic": "XPAR", "issuer_name": "Alstom"},
    ])
    result = module.load_admitted_eu_entities(fake_spark, "main")
    assert result == [
        ("FCC", "95980020140005178328", "XMAD", "Fomento de Construcciones y Contratas, S.A."),
        ("ALO", "96950032TUYMW11FB530", "XPAR", "Alstom"),
    ]


def test_load_admitted_eu_entities_queries_admission_status_admitted_only():
    """The query itself must filter to admission_status = 'admitted' -- never a client-side
    filter over all rows (that would risk silently including pending_esef_check/rejected rows
    if the WHERE clause were ever dropped)."""
    module = _load_eu_adapter_module()
    fake_spark = _FakeSparkSession([])
    module.load_admitted_eu_entities(fake_spark, "main")
    assert "admission_status = 'admitted'" in fake_spark.last_sql
    assert "main.config.eu_admission_candidates" in fake_spark.last_sql


def test_load_admitted_eu_entities_excludes_null_ticker_and_warns(capsys):
    """A real, possible edge case per Phase 5.3's own design: an ADMITTED row can still have
    ticker_status='unresolved' (ticker is explicitly non-blocking for admission). Such a row
    must be excluded from ingestion (this path is ticker-keyed downstream), not silently
    included with a None ticker, and must be reported, not silently dropped."""
    module = _load_eu_adapter_module()
    fake_spark = _FakeSparkSession([
        {"isin": "ES0122060314", "ticker": "FCC", "lei": "95980020140005178328",
         "mic": "XMAD", "issuer_name": "FCC"},
        {"isin": "DE0000000000", "ticker": None, "lei": "SOMELEIVALUE0000001",
         "mic": "FRAA", "issuer_name": "No Ticker Co"},
    ])
    result = module.load_admitted_eu_entities(fake_spark, "main")
    assert len(result) == 1
    assert result[0][0] == "FCC"
    captured = capsys.readouterr()
    assert "DE0000000000" in captured.out
    assert "WARNING" in captured.out


def test_load_admitted_eu_entities_empty_result():
    module = _load_eu_adapter_module()
    fake_spark = _FakeSparkSession([])
    assert module.load_admitted_eu_entities(fake_spark, "main") == []
