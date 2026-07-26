"""Schema contract on the export ↔ Streamlit boundary — the single source of truth.

The publish layer (``50__publish/51__export_dashboard_data.py``) writes three parquet
artifacts plus a meta JSON; the public Streamlit app (``60__frontends/61__streamlit/lib/data.py``)
reads them. This module pins the column set + dtype *family* of each artifact and the
required keys of the meta JSON, so a pipeline change to the export can never silently
ship a shape the app can't read:

  * the export side asserts BEFORE writing parquet → a bad run fails loudly;
  * the load side validates AFTER reading → an incompatible artifact yields a readable
    error (hard for the core data/metrics; soft for prices, which must never block the app).

Dtype checks are by *family* (``numeric`` / ``datetime`` / ``string`` / ``bool``), not exact
dtype, because the same column legitimately reads as ``object`` (raw date32) at export time
and as ``datetime64`` / ``category`` after the app normalizes it.

The artifact-name/meta-key constants and the meta-JSON validation (``ARTIFACT_NAMES``,
``META_REQUIRED_KEYS``, ``validate_meta``, ``SchemaError``, …) live in :mod:`artifacts`, a
pandas-free sibling module, and are re-exported here for backward compatibility — everything
that used to work as ``schemas.ARTIFACT_NAMES`` etc. still does. Only the dtype-level
DataFrame validation below (``dtype_family``/``validate_artifact``/``assert_artifact``) — the
part that actually needs pandas — lives in this module. Split out 2026-07-26: a caller that
only needs the artifact-name constants (e.g. fundamentals_screener's request-path DuckDB
connection setup) was paying pandas's ~400ms cold-import cost just to import this whole
module under a no-persistent-process (CGI) deployment, purely for a plain string tuple.
"""

from __future__ import annotations

import pandas as pd
from pandas.api import types as ptypes

from .artifacts import (
    ARTIFACT_NAMES,
    ARTIFACTS,
    META_REQUIRED_KEYS,
    TICKER_REQUIRED_KEYS,
    SchemaError,
    assert_meta,
    required_columns,
    validate_meta,
)

__all__ = [
    "SchemaError",
    "ARTIFACTS",
    "ARTIFACT_NAMES",
    "META_REQUIRED_KEYS",
    "TICKER_REQUIRED_KEYS",
    "required_columns",
    "validate_meta",
    "assert_meta",
    "dtype_family",
    "validate_artifact",
    "assert_artifact",
]


# ── dtype families ──────────────────────────────────────────────────────────────
def dtype_family(series: pd.Series) -> str:
    """Map a pandas dtype to a coarse family: bool / numeric / datetime / string / other.

    ``bool`` is checked before ``numeric`` (pandas treats bool as a numeric subtype).
    Categorical and object/string dtypes all collapse to ``string``.
    """
    dt = series.dtype
    if ptypes.is_bool_dtype(dt):
        return "bool"
    if ptypes.is_numeric_dtype(dt):
        return "numeric"
    if ptypes.is_datetime64_any_dtype(dt):
        return "datetime"
    if isinstance(dt, pd.CategoricalDtype) or ptypes.is_object_dtype(dt) or ptypes.is_string_dtype(dt):
        return "string"
    return "other"


def validate_artifact(name: str, df: pd.DataFrame) -> list[str]:
    """Return a list of human-readable contract violations for one parquet artifact.

    Empty list ⇒ the frame satisfies the contract. Columns beyond the required set are
    allowed (additive changes don't break the app). Raises ``ValueError`` for an
    unknown artifact name.
    """
    if name not in ARTIFACTS:
        raise ValueError(f"unknown artifact {name!r}; expected one of {ARTIFACT_NAMES}")
    spec = ARTIFACTS[name]
    violations: list[str] = []
    for col, families in spec.items():
        if col not in df.columns:
            violations.append(f"{name}: missing required column '{col}'")
            continue
        fam = dtype_family(df[col])
        if fam not in families:
            violations.append(
                f"{name}: column '{col}' has dtype family '{fam}' (dtype {df[col].dtype}); "
                f"expected one of {sorted(families)}"
            )
    return violations


def assert_artifact(name: str, df: pd.DataFrame) -> None:
    """Raise ``SchemaError`` if the frame violates the contract (else return None)."""
    violations = validate_artifact(name, df)
    if violations:
        raise SchemaError(f"{name} failed schema validation:\n  - " + "\n  - ".join(violations))
