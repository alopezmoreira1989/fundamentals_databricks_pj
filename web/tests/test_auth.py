"""Auth flow tests: signup, login, logout, and the auth-aware navbar (SQLite, no artifacts)."""

from __future__ import annotations

import pytest
from django.contrib.auth import get_user_model

pytestmark = pytest.mark.django_db

User = get_user_model()


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


def test_logout_requires_post_and_clears_session(client):
    User.objects.create_user(username="carol", email="carol@example.com", password="pw-carol-1!")
    client.login(username="carol", password="pw-carol-1!")
    assert client.get("/").wsgi_request.user.is_authenticated

    assert client.get("/accounts/logout/").status_code == 405  # GET not allowed
    resp = client.post("/accounts/logout/")
    assert resp.status_code == 302
    assert not client.get("/").wsgi_request.user.is_authenticated


def test_navbar_reflects_auth_state(client):
    anon = client.get("/").content.decode()
    assert "Log in" in anon and "Sign up" in anon

    User.objects.create_user(username="dave", email="dave@example.com", password="pw-dave-12!")
    client.login(username="dave", password="pw-dave-12!")
    body = client.get("/").content.decode()
    assert "Log out" in body and "dave" in body
