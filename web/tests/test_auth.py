"""Auth flow tests: signup, login, logout, and the auth-aware navbar (SQLite, no artifacts)."""

from __future__ import annotations

import pytest
from django.contrib.auth import get_user_model
from django.test import override_settings

pytestmark = pytest.mark.django_db

User = get_user_model()

# A small, fast limit for the lockout tests below — independent of the real AXES_FAILURE_LIMIT
# default (5, or the AXES_FAILURE_LIMIT env var in prod) so the test doesn't need 5 requests
# and stays correct even if that default changes.
_AXES_TEST_SETTINGS = {"AXES_FAILURE_LIMIT": 3, "AXES_LOCKOUT_PARAMETERS": [["username", "ip_address"]]}


def test_signup_get_renders_form_for_anonymous(client):
    resp = client.get("/accounts/signup/")
    assert resp.status_code == 200
    assert "form" in resp.context


def test_signup_get_redirects_already_authenticated_user(client):
    client.force_login(User.objects.create_user(username="already", email="a@example.com", password="pw-123456!"))
    resp = client.get("/accounts/signup/")
    assert resp.status_code == 302 and resp.headers["Location"] == "/"


def test_signup_creates_user_logs_in_and_redirects(client):
    resp = client.post(
        "/accounts/signup/",
        {
            "username": "newbie",
            "email": "newbie@example.com",
            "password1": "sup3r-s3cret-pw",
            "password2": "sup3r-s3cret-pw",
        },
    )
    assert resp.status_code == 302 and resp.headers["Location"] == "/"
    user = User.objects.get(username="newbie")
    assert user.email == "newbie@example.com"
    # session is authenticated after signup
    assert client.get("/").wsgi_request.user.is_authenticated


def test_signup_rejects_mismatched_passwords(client):
    resp = client.post(
        "/accounts/signup/",
        {
            "username": "x",
            "email": "x@example.com",
            "password1": "sup3r-s3cret-pw",
            "password2": "different-pw",
        },
    )
    assert resp.status_code == 200  # re-renders with errors
    assert not User.objects.filter(username="x").exists()


def test_signup_requires_email(client):
    resp = client.post(
        "/accounts/signup/",
        {"username": "noemail", "password1": "sup3r-s3cret-pw", "password2": "sup3r-s3cret-pw"},
    )
    assert resp.status_code == 200
    assert not User.objects.filter(username="noemail").exists()


def test_login_valid_and_invalid(client):
    User.objects.create_user(username="bob", email="bob@example.com", password="pw-bob-123!")
    ok = client.post("/accounts/login/", {"username": "bob", "password": "pw-bob-123!"})
    assert ok.status_code == 302 and ok.headers["Location"] == "/"

    bad = client.post("/accounts/login/", {"username": "bob", "password": "wrong"})
    assert bad.status_code == 200  # re-renders with an error, no redirect


@override_settings(**_AXES_TEST_SETTINGS)
def test_login_locks_out_after_repeated_failures(client):
    """#181 item 2: repeated failed logins for one (username, ip) pair get locked out —
    even a subsequent CORRECT password is rejected until the cooloff period elapses."""
    User.objects.create_user(username="erin", email="erin@example.com", password="pw-erin-123!")

    for _ in range(_AXES_TEST_SETTINGS["AXES_FAILURE_LIMIT"] - 1):
        resp = client.post("/accounts/login/", {"username": "erin", "password": "wrong"})
        assert resp.status_code == 200  # normal invalid-credentials re-render, not locked yet

    # This failure hits the limit — axes locks out on the triggering attempt itself.
    locked = client.post("/accounts/login/", {"username": "erin", "password": "wrong"})
    assert locked.status_code == 429

    # Locked out now: even the CORRECT password is rejected while cooled off.
    still_locked = client.post("/accounts/login/", {"username": "erin", "password": "pw-erin-123!"})
    assert still_locked.status_code == 429
    assert not still_locked.wsgi_request.user.is_authenticated


@override_settings(**_AXES_TEST_SETTINGS)
def test_login_lockout_is_scoped_to_username_and_ip(client):
    """A different username from the same client isn't caught by another user's lockout —
    AXES_LOCKOUT_PARAMETERS=[["username", "ip_address"]] scopes to the specific pair."""
    User.objects.create_user(username="frank", email="frank@example.com", password="pw-frank-1!")
    User.objects.create_user(username="grace", email="grace@example.com", password="pw-grace-1!")

    for _ in range(_AXES_TEST_SETTINGS["AXES_FAILURE_LIMIT"]):
        client.post("/accounts/login/", {"username": "frank", "password": "wrong"})

    # frank is locked out...
    frank_attempt = client.post("/accounts/login/", {"username": "frank", "password": "pw-frank-1!"})
    assert frank_attempt.status_code == 429

    # ...but grace, from the same test client (same IP), is unaffected.
    grace_attempt = client.post("/accounts/login/", {"username": "grace", "password": "pw-grace-1!"})
    assert grace_attempt.status_code == 302 and grace_attempt.headers["Location"] == "/"


def test_logout_requires_post_and_clears_session(client):
    user = User.objects.create_user(username="carol", email="carol@example.com", password="pw-carol-1!")
    # force_login sets the session directly rather than going through authenticate() — these
    # two tests aren't exercising login itself, and AxesBackend.authenticate() requires a real
    # request object that client.login()'s shortcut doesn't provide (see test_login_* above
    # for the tests that actually exercise the login view + axes).
    client.force_login(user)
    assert client.get("/").wsgi_request.user.is_authenticated

    assert client.get("/accounts/logout/").status_code == 405  # GET not allowed
    resp = client.post("/accounts/logout/")
    assert resp.status_code == 302
    assert not client.get("/").wsgi_request.user.is_authenticated


def test_navbar_reflects_auth_state(client):
    anon = client.get("/").content.decode()
    assert "Log in" in anon and "Sign up" in anon

    user = User.objects.create_user(username="dave", email="dave@example.com", password="pw-dave-12!")
    client.force_login(user)  # see test_logout_requires_post_and_clears_session for why
    body = client.get("/").content.decode()
    assert "Log out" in body and "dave" in body
