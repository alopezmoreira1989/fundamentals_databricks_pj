"""Tests for the custom user model, its manager, configuration, and authentication."""

from __future__ import annotations

import uuid

import pytest
from apps.users.models import User, UserManager
from apps.users.services import authenticate_user, create_user, get_by_email
from django.conf import settings
from django.contrib.auth import get_user_model
from django.db import IntegrityError

pytestmark = pytest.mark.django_db


# ── configuration ────────────────────────────────────────────────────────────────────
def test_auth_user_model_is_the_custom_model():
    assert settings.AUTH_USER_MODEL == "users.User"
    assert get_user_model() is User


def test_username_login_is_kept_and_email_is_required_for_superuser():
    assert User.USERNAME_FIELD == "username"
    assert "email" in User.REQUIRED_FIELDS


def test_primary_key_is_uuid():
    user = create_user(username="pk", email="pk@example.com", password="pw")
    assert isinstance(user.pk, uuid.UUID)
    # pk field is a UUIDField, not an auto/int field
    assert User._meta.pk.get_internal_type() == "UUIDField"


# ── creation ─────────────────────────────────────────────────────────────────────────
def test_create_user_sets_fields_and_hashes_password():
    user = create_user(username="alice", email="alice@example.com", password="s3cret")
    assert user.username == "alice"
    assert user.email == "alice@example.com"
    assert user.is_active is True
    assert user.is_staff is False
    assert user.is_superuser is False
    # password is stored hashed, not in clear text
    assert user.password != "s3cret"
    assert user.check_password("s3cret")


def test_email_must_be_unique():
    create_user(username="bob", email="dup@example.com", password="pw")
    with pytest.raises(IntegrityError):
        create_user(username="bob2", email="dup@example.com", password="pw")


# ── manager ──────────────────────────────────────────────────────────────────────────
def test_manager_is_custom_user_manager():
    assert isinstance(User.objects, UserManager)


def test_manager_requires_email():
    with pytest.raises(ValueError, match="email address is required"):
        User.objects.create_user(username="noemail", email="", password="pw")


def test_create_superuser_flags_and_requires_email():
    admin = User.objects.create_superuser(
        username="root", email="root@example.com", password="pw"
    )
    assert admin.is_staff is True
    assert admin.is_superuser is True
    with pytest.raises(ValueError, match="email address is required"):
        User.objects.create_superuser(username="root2", email="", password="pw")


# ── authentication ───────────────────────────────────────────────────────────────────
def test_authentication_succeeds_with_correct_credentials():
    create_user(username="carol", email="carol@example.com", password="hunter2")
    assert authenticate_user(username="carol", password="hunter2") is not None


def test_authentication_fails_with_wrong_password():
    create_user(username="dave", email="dave@example.com", password="right")
    assert authenticate_user(username="dave", password="wrong") is None


def test_authentication_fails_for_unknown_user():
    assert authenticate_user(username="ghost", password="whatever") is None


def test_inactive_user_cannot_authenticate():
    create_user(
        username="ex", email="ex@example.com", password="pw", is_active=False
    )
    assert authenticate_user(username="ex", password="pw") is None


# ── service helper ───────────────────────────────────────────────────────────────────
def test_get_by_email_returns_user_or_none():
    created = create_user(username="erin", email="erin@example.com", password="pw")
    assert get_by_email("erin@example.com") == created
    assert get_by_email("missing@example.com") is None
