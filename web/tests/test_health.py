"""Observability tests: liveness/readiness probes, the JSON log formatter + request-id
middleware, and the gated Sentry initialiser. Fully offline — the readiness artifact probe
points at a tmp dir, and the Sentry test exercises only the no-DSN (no-import) path.
"""

from __future__ import annotations

import json
import logging

import pytest
from config.log import JSONFormatter, request_id_var
from config.observability import init_sentry
from infrastructure.storage import artifacts

pytestmark = pytest.mark.django_db  # readyz probes the default DB connection

_CORE = ("dashboard_data", "dashboard_metrics")


@pytest.fixture
def cached_artifacts(settings, tmp_path):
    """Serve the core artifacts from a local dir with the required files present (present+fresh)."""
    for name in _CORE:
        (tmp_path / artifacts.PARQUET_FILES[name]).write_bytes(b"PARQUET")
    settings.ARTIFACTS_LOCAL_DIR = str(tmp_path)
    return tmp_path


# ── liveness ───────────────────────────────────────────────────────────────────────────
def test_livez_ok(client):
    resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}
    # Middleware stamps a correlation id on every response.
    assert resp["X-Request-ID"]


# ── readiness ──────────────────────────────────────────────────────────────────────────
def test_readyz_ready_when_db_and_artifacts_healthy(client, cached_artifacts):
    resp = client.get("/readyz")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ready"
    names = {c["name"]: c["ok"] for c in body["checks"]}
    assert names == {"database": True, "artifacts": True}
    assert "cache_metrics" in body


def test_readyz_not_ready_when_artifacts_missing(client, settings, tmp_path):
    settings.ARTIFACTS_LOCAL_DIR = ""  # use the (empty) cache dir → nothing cached
    settings.ARTIFACTS_CACHE_DIR = tmp_path
    resp = client.get("/readyz")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "not_ready"
    artifacts_check = next(c for c in body["checks"] if c["name"] == "artifacts")
    assert artifacts_check["ok"] is False
    assert "not cached" in artifacts_check["detail"]


def test_readyz_is_uncached(client, cached_artifacts):
    resp = client.get("/readyz")
    assert "no-cache" in resp.get("Cache-Control", "")


def test_readyz_hides_raw_db_error(client, cached_artifacts, monkeypatch):
    # A DB outage must yield 503 with a generic detail — never the raw exception (which can leak
    # host/driver/connection internals) — and the response must still get through (the access-log
    # middleware, which lazily loads the session user from the now-dead DB, must not 500 it).
    from django.db import connections
    from django.db.utils import OperationalError

    secret = "password=SUPERSECRET host=10.9.8.7"

    def boom(*args, **kwargs):
        raise OperationalError(f"could not connect to server: {secret}")

    monkeypatch.setattr(connections["default"], "cursor", boom)
    resp = client.get("/readyz")
    assert resp.status_code == 503
    db = next(c for c in resp.json()["checks"] if c["name"] == "database")
    assert db["ok"] is False
    assert db["detail"] == "unavailable"
    assert "SUPERSECRET" not in resp.content.decode()


# ── request id / access log ──────────────────────────────────────────────────────────────
def test_request_id_honours_upstream_header(client, cached_artifacts):
    resp = client.get("/readyz", HTTP_X_REQUEST_ID="trace-abc-123")
    assert resp["X-Request-ID"] == "trace-abc-123"


def test_access_log_emitted_for_content_requests(client, caplog):
    with caplog.at_level(logging.INFO, logger="web.request"):
        client.get("/healthz")  # quiet path — must NOT log
        client.get("/nope-404")  # content path — must log
    messages = [r.getMessage() for r in caplog.records if r.name == "web.request"]
    assert any("/nope-404" in m for m in messages)
    assert not any("/healthz" in m for m in messages)


# ── JSON formatter ───────────────────────────────────────────────────────────────────────
def test_json_formatter_core_schema_and_extras():
    record = logging.LogRecord(
        name="web.request", level=logging.INFO, pathname=__file__, lineno=1,
        msg="GET %s", args=("/x",), exc_info=None,
    )
    record.status = 200
    record.duration_ms = 12.3
    token = request_id_var.set("rid-xyz")
    try:
        out = json.loads(JSONFormatter().format(record))
    finally:
        request_id_var.reset(token)
    assert out["level"] == "INFO"
    assert out["logger"] == "web.request"
    assert out["message"] == "GET /x"
    assert out["request_id"] == "rid-xyz"
    assert out["status"] == 200
    assert out["duration_ms"] == 12.3
    assert "timestamp" in out


def test_json_formatter_serialises_exception():
    try:
        raise ValueError("boom")
    except ValueError:
        record = logging.LogRecord(
            name="x", level=logging.ERROR, pathname=__file__, lineno=1,
            msg="failed", args=(), exc_info=logging.sys.exc_info(),
        )
    out = json.loads(JSONFormatter().format(record))
    assert "ValueError: boom" in out["exception"]


# ── sentry gating ────────────────────────────────────────────────────────────────────────
def test_init_sentry_noop_without_dsn():
    # No DSN → returns False and never imports sentry_sdk (prod-only dependency).
    assert init_sentry(dsn="", environment="test") is False


def test_init_sentry_initialises_with_dsn(monkeypatch):
    # With a DSN, init_sentry lazily imports sentry_sdk and calls init with release/env tags.
    # Inject fake modules so the prod-only dependency needn't be installed in the test env.
    import sys
    import types

    calls: dict = {}
    fake_sdk = types.ModuleType("sentry_sdk")
    fake_sdk.init = lambda **kw: calls.update(kw)  # type: ignore[attr-defined]
    fake_integrations = types.ModuleType("sentry_sdk.integrations")
    fake_django = types.ModuleType("sentry_sdk.integrations.django")
    fake_django.DjangoIntegration = type("DjangoIntegration", (), {})  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "sentry_sdk", fake_sdk)
    monkeypatch.setitem(sys.modules, "sentry_sdk.integrations", fake_integrations)
    monkeypatch.setitem(sys.modules, "sentry_sdk.integrations.django", fake_django)

    ok = init_sentry(
        dsn="https://key@o0.ingest.sentry.io/1", environment="production", release="abc123"
    )
    assert ok is True
    assert calls["environment"] == "production"
    assert calls["release"] == "abc123"
    assert calls["send_default_pii"] is False
