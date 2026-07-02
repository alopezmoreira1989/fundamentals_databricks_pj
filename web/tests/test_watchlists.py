"""Watchlist tests: service CRUD, login-required + POST-only endpoints, and the UI toggle."""

from __future__ import annotations

import pytest
from apps.watchlists import services
from apps.watchlists.models import WatchlistItem
from django.contrib.auth import get_user_model

pytestmark = pytest.mark.django_db

User = get_user_model()
PW = "watch-pw-123!"


def _user(name: str = "watcher") -> object:
    return User.objects.create_user(username=name, email=f"{name}@example.com", password=PW)


# ── service (ORM CRUD) ───────────────────────────────────────────────────────────────
def test_add_is_idempotent_and_uppercases():
    user = _user()
    services.add(user, "aapl")
    services.add(user, "AAPL")  # same ticker, different case
    assert services.list_tickers(user) == ["AAPL"]
    assert WatchlistItem.objects.filter(user=user).count() == 1
    assert services.contains(user, "aapl") and services.contains(user, "AAPL")


def test_remove_and_isolation_between_users():
    a, b = _user("a"), _user("b")
    services.add(a, "MSFT")
    services.add(b, "MSFT")
    services.remove(a, "msft")
    assert not services.contains(a, "MSFT")
    assert services.contains(b, "MSFT")  # other user's list untouched


# ── auth gating ──────────────────────────────────────────────────────────────────────
def test_watchlist_page_requires_login(client):
    resp = client.get("/watchlist/")
    assert resp.status_code == 302 and "/accounts/login/" in resp.headers["Location"]


def test_add_requires_login_then_post(client):
    # anonymous → login redirect (auth checked before method)
    assert client.post("/watchlist/add/", {"ticker": "AAPL"}).status_code == 302
    assert not WatchlistItem.objects.exists()
    # authenticated GET → 405 (POST-only)
    client.force_login(_user())
    assert client.get("/watchlist/add/").status_code == 405


# ── endpoints (authenticated) ────────────────────────────────────────────────────────
def test_add_and_remove_roundtrip_with_safe_next(client):
    user = _user()
    client.force_login(user)

    added = client.post("/watchlist/add/", {"ticker": "nvda", "next": "/companies/NVDA/"})
    assert added.status_code == 302 and added.headers["Location"] == "/companies/NVDA/"
    assert services.contains(user, "NVDA")

    # an off-site next is ignored → falls back to the watchlist page
    removed = client.post(
        "/watchlist/remove/", {"ticker": "NVDA", "next": "https://evil.example/"}
    )
    assert removed.status_code == 302 and removed.headers["Location"] == "/watchlist/"
    assert not services.contains(user, "NVDA")


def test_watchlist_page_lists_tickers(client, artifacts_from_fixtures):
    user = _user()
    services.add(user, "AAPL")
    client.force_login(user)
    html = client.get("/watchlist/").content.decode()
    assert "AAPL" in html and "Apple Inc." in html  # company name resolved from meta


def test_company_page_toggle_reflects_state(client, artifacts_from_fixtures):
    user = _user()
    client.force_login(user)
    # not on the list → "add" control
    assert "☆ Watchlist" in client.get("/companies/AAPL/").content.decode()
    services.add(user, "AAPL")
    # on the list → "remove" control
    assert "★ On watchlist" in client.get("/companies/AAPL/").content.decode()
