"""Watchlist tests: named-list CRUD, item membership across lists, login-required + POST-only
endpoints, and the company-page list picker."""

from __future__ import annotations

import pytest
from apps.watchlists import services
from apps.watchlists.models import DEFAULT_WATCHLIST_NAME, Watchlist, WatchlistItem
from django.contrib.auth import get_user_model

pytestmark = pytest.mark.django_db

User = get_user_model()
PW = "watch-pw-123!"


def _user(name: str = "watcher") -> object:
    return User.objects.create_user(username=name, email=f"{name}@example.com", password=PW)


# ── list CRUD (ORM) ──────────────────────────────────────────────────────────────────
def test_default_list_is_created_on_demand():
    user = _user()
    wl = services.get_default(user)
    assert wl.name == DEFAULT_WATCHLIST_NAME
    assert services.get_default(user).id == wl.id  # idempotent


def test_create_rename_delete_and_dup_guard():
    user = _user()
    energy = services.create(user, "Energy")
    assert energy is not None
    assert services.create(user, "Energy") is None  # duplicate name rejected
    assert services.create(user, "  ") is None  # blank rejected

    assert services.rename(user, energy.id, "Oil & Gas") is True
    assert Watchlist.objects.get(id=energy.id).name == "Oil & Gas"

    compounders = services.create(user, "Compounders")
    assert compounders is not None
    assert services.rename(user, energy.id, "Compounders") is False  # name clash

    services.delete(user, energy.id)
    assert not Watchlist.objects.filter(id=energy.id).exists()


# ── item membership ──────────────────────────────────────────────────────────────────
def test_add_defaults_to_default_list_and_uppercases():
    user = _user()
    services.add(user, "aapl")
    services.add(user, "AAPL")  # same ticker, different case
    default = services.get_default(user)
    assert services.list_tickers(default) == ["AAPL"]
    assert WatchlistItem.objects.filter(watchlist=default).count() == 1
    assert services.contains(user, "aapl")


def test_ticker_can_live_in_several_lists():
    user = _user()
    energy = services.create(user, "Energy")
    compounders = services.create(user, "Compounders")
    services.add(user, "XOM", energy.id)
    services.add(user, "XOM", compounders.id)
    assert services.lists_for_ticker(user, "XOM") == {energy.id, compounders.id}
    # removing from one leaves the other
    services.remove(user, "XOM", energy.id)
    assert services.lists_for_ticker(user, "XOM") == {compounders.id}
    assert services.contains(user, "XOM")


def test_add_to_named_list_creates_or_reuses():
    user = _user()
    first = services.add_to_named_list(user, "MSFT", "Tech")
    second = services.add_to_named_list(user, "AAPL", "Tech")
    assert first is not None and second is not None
    assert first.watchlist_id == second.watchlist_id  # same list reused
    assert Watchlist.objects.filter(user=user, name="Tech").count() == 1
    assert services.add_to_named_list(user, "AAPL", "  ") is None


def test_isolation_between_users():
    a, b = _user("a"), _user("b")
    services.add(a, "MSFT")
    services.add(b, "MSFT")
    services.remove(a, "msft")
    assert not services.contains(a, "MSFT")
    assert services.contains(b, "MSFT")  # other user's list untouched


def test_cannot_touch_another_users_list():
    a, b = _user("a"), _user("b")
    a_list = services.create(a, "Private")
    assert services.get_watchlist(b, a_list.id) is None
    assert services.rename(b, a_list.id, "Hacked") is False
    services.delete(b, a_list.id)  # no-op
    assert Watchlist.objects.filter(id=a_list.id).exists()
    assert services.add(b, "XOM", a_list.id) is None  # can't add into it


# ── auth gating ──────────────────────────────────────────────────────────────────────
def test_overview_requires_login(client):
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

    # an off-site next is ignored → falls back to the overview page
    removed = client.post(
        "/watchlist/remove/", {"ticker": "NVDA", "next": "https://evil.example/"}
    )
    assert removed.status_code == 302 and removed.headers["Location"] == "/watchlist/"
    assert not services.contains(user, "NVDA")


def test_create_and_detail_page(client):
    user = _user()
    client.force_login(user)
    client.post("/watchlist/create/", {"name": "Energy"})
    energy = Watchlist.objects.get(user=user, name="Energy")
    services.add(user, "XOM", energy.id)
    html = client.get(f"/watchlist/{energy.id}/").content.decode()
    assert "Energy" in html and "XOM" in html


def test_add_into_specific_list_via_endpoint(client):
    user = _user()
    client.force_login(user)
    energy = services.create(user, "Energy")
    client.post("/watchlist/add/", {"ticker": "CVX", "watchlist": str(energy.id)})
    assert services.lists_for_ticker(user, "CVX") == {energy.id}


def test_add_new_named_list_via_picker_field(client):
    user = _user()
    client.force_login(user)
    client.post("/watchlist/add/", {"ticker": "GOOG", "watchlist_name": "Tech"})
    assert Watchlist.objects.filter(user=user, name="Tech").exists()
    assert services.contains(user, "GOOG")


def test_overview_lists_watchlists(client):
    user = _user()
    services.create(user, "Energy")
    client.force_login(user)
    html = client.get("/watchlist/").content.decode()
    assert "Energy" in html


def test_company_page_picker_reflects_membership(client, artifacts_from_fixtures):
    user = _user()
    energy = services.create(user, "Energy")
    client.force_login(user)
    # not on any list → outline "add" state for the list
    html = client.get("/companies/AAPL/").content.decode()
    assert "☆ Energy" in html
    services.add(user, "AAPL", energy.id)
    # on the list → "on list" state
    html = client.get("/companies/AAPL/").content.decode()
    assert "★ Energy" in html
