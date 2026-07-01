"""Users application service — use-case orchestration for the users domain.

Per the data-access rules (``docs/architecture.md``), this is the tier views call. It uses
the Django ORM **directly**: user CRUD is trivial ORM work, so a pass-through ``UserRepository``
would add indirection, not value. If a genuine repository case appears later (a composed
query, DTO mapping across models, caching, a second data source), introduce one then — the
views calling these functions won't need to change.
"""

from __future__ import annotations

from django.contrib.auth import authenticate as _authenticate
from django.contrib.auth import get_user_model

User = get_user_model()


def create_user(*, username: str, email: str, password: str, **extra_fields):
    """Create and persist a regular user (password hashed by the manager)."""
    return User.objects.create_user(
        username=username, email=email, password=password, **extra_fields
    )


def authenticate_user(*, username: str, password: str):
    """Return the user for valid credentials, else ``None`` (Django's ModelBackend)."""
    return _authenticate(username=username, password=password)


def get_by_email(email: str):
    """Return the user with this (unique) email, or ``None`` if there is none."""
    return User.objects.filter(email=email).first()
