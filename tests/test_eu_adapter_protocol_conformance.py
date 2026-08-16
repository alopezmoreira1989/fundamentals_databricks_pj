"""Proves EUCurrentSource (fundamentals_pipeline/10__ingestion/16__fetch_eu_xbrl.py) genuinely
satisfies the FundamentalsSource Protocol at runtime -- not just that it happens to have
methods with the right names by convention. Requested in PR #373 review: makes the Phase 3
architectural contract executable rather than implicit.

The notebook file can't be imported the normal way (its directory/filename start with digits,
and its module-level code assumes `%run`-injected Databricks globals like `spark`/`CATALOG`).
It's loaded here via importlib with RUN_EU_PILOT=False pre-seeded, which gates every network-
calling/Spark-touching section of the file (see 16__fetch_eu_xbrl.py's own "RUN_EU_PILOT" cell)
-- EUCurrentSource's class definition and process_eu_entity's function definition are always
unconditional (mirroring how 11__fetch_sec_xbrl.py's SECXBRLSource/get_cik/extract_series are
never gated either), so this test never touches the network or a Spark session.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

from fundamentals_pipeline.sources.base import FundamentalsSource

_NOTEBOOK_PATH = (
    Path(__file__).resolve().parent.parent
    / "fundamentals_pipeline"
    / "10__ingestion"
    / "16__fetch_eu_xbrl.py"
)


def _load_eu_adapter_module():
    spec = importlib.util.spec_from_file_location("_eu_adapter_under_test", _NOTEBOOK_PATH)
    module = importlib.util.module_from_spec(spec)
    module.RUN_EU_PILOT = False  # skip table creation / the real pilot loop / Delta writes
    spec.loader.exec_module(module)
    return module


def test_eu_current_source_satisfies_fundamentals_source_protocol():
    module = _load_eu_adapter_module()
    source = module.EUCurrentSource()

    # runtime_checkable isinstance check -- real, executable proof of structural conformance,
    # not just "it compiles" or "it was written with matching method names by convention".
    assert isinstance(source, FundamentalsSource)

    typed_source: FundamentalsSource = source  # static: also satisfies the type annotation
    assert typed_source.source_id == "EU_CURRENT"
    assert callable(typed_source.discover_entities)
    assert callable(typed_source.discover_filings)
    assert callable(typed_source.retrieve_facts)
    assert callable(typed_source.detect_metadata)


def test_eu_current_source_discover_entities_uses_verified_pilot_identity_no_network():
    """discover_entities() resolves purely from the hardcoded, pre-verified pilot list -- no
    network call -- so it's safe to exercise directly even with RUN_EU_PILOT=False."""
    module = _load_eu_adapter_module()
    source = module.EUCurrentSource()

    entities = source.discover_entities(["FCC", "UNKNOWN_TICKER"])

    assert len(entities) == 1
    assert entities[0].source_id == "EU_CURRENT"
    assert entities[0].source_entity_id == "95980020140005178328"
    assert entities[0].issuer_id == "EU_CURRENT:95980020140005178328"
    assert entities[0].ticker == "FCC"
