"""History tests: service upsert/trim/clear, login-required + POST-only clear, and the
recording side effect of visiting a company page."""

from __future__ import annotations

import pytest
from apps.history import services
from apps.history.models import HistoryItem
from django.contrib.auth import get_user_model

pytestmark = pytest.mark.django_db

User = get_user_model()
PW = "hist-pw-123!"


def _user(name: str = "viewer") -> object:
    return User.objects.create_user(username=name, email=f"{name}@example.com", password=PW)


# ── service (ORM CRUD) ───────────────────────────────────────────────────────────────
def test_record_upserts_and_uppercases():
    user = _user()
    services.record(user, "aapl")
    services.record(user, "AAPL")  # same ticker, different case → one row, re-stamped
    assert HistoryItem.objects.filter(user=user).count() == 1
    assert services.recent_tickers(user) == ["AAPL"]


def test_record_is_most_recent_first():
    user = _user()
    for t in ("AAPL", "MSFT", "NVDA"):
        services.record(user, t)
    services.record(user, "AAPL")  # bump AAPL back to the front
    assert services.recent_tickers(user) == ["AAPL", "NVDA", "MSFT"]


def test_record_trims_to_cap():
    user = _user()
    for i in range(services.CAP + 10):
        services.record(user, f"T{i:03d}")
    assert HistoryItem.objects.filter(user=user).count() == services.CAP
    # the oldest (T000..) were trimmed; the newest survive
    assert "T000" not in services.recent_tickers(user, limit=services.CAP)
    assert f"T{services.CAP + 9:03d}" in services.recent_tickers(user)


def test_clear_and_isolation_between_users():
    a, b = _user("a"), _user("b")
    services.record(a, "MSFT")
    services.record(b, "MSFT")
    services.clear(a)
    assert services.recent_tickers(a) == []
    assert services.recent_tickers(b) == ["MSFT"]  # other user untouched


# ── auth gating ──────────────────────────────────────────────────────────────────────
def test_history_page_requires_login(client):
    resp = client.get("/history/")
    assert resp.status_code == 302 and "/accounts/login/" in resp.headers["Location"]


def test_clear_requires_login_then_post(client):
    assert client.post("/history/clear/").status_code == 302  # anonymous → login redirect
    client.force_login(_user())
    assert client.get("/history/clear/").status_code == 405  # POST-only


# ── endpoints (authenticated) ────────────────────────────────────────────────────────
def test_clear_roundtrip(client):
    user = _user()
    services.record(user, "NVDA")
    client.force_login(user)
    resp = client.post("/history/clear/")
    assert resp.status_code == 302 and resp.headers["Location"] == "/history/"
    assert services.recent_tickers(user) == []


def test_visiting_company_page_records_history(client, artifacts_from_fixtures):
    user = _user()
    client.force_login(user)
    assert client.get("/companies/AAPL/").status_code == 200
    assert services.recent_tickers(user) == ["AAPL"]


def test_anonymous_company_visit_records_nothing(client, artifacts_from_fixtures):
    client.get("/companies/AAPL/")
    assert not HistoryItem.objects.exists()


def test_history_page_lists_tickers(client, artifacts_from_fixtures):
    user = _user()
    services.record(user, "AAPL")
    client.force_login(user)
    html = client.get("/history/").content.decode()
    assert "AAPL" in html and "Apple Inc." in html  # company name resolved from meta
