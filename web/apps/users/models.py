"""Custom user model.

Built on ``AbstractUser`` (not ``AbstractBaseUser``): username-based login is kept, and we
gain Django's permissions/staff/superuser plumbing for free. Two deliberate deviations from
the stock model:

* **UUID primary key** — every application entity uses a UUID pk from the start (never
  Auto/BigAutoField); see ``docs/architecture.md`` → *UUID primary keys*.
* **Unique, required email** — email is a stable identifier alongside the username.

The model is intentionally minimal (YAGNI): no profile/preference fields until a feature
actually needs them.
"""

from __future__ import annotations

import uuid

from django.contrib.auth.models import AbstractUser
from django.contrib.auth.models import UserManager as DjangoUserManager
from django.db import models


class UserManager(DjangoUserManager):
    """Manager that enforces the unique-email invariant at creation time.

    ``AbstractUser`` leaves email optional; because we made it unique+required, both the
    normal and superuser creation paths must actually receive one (the DB unique constraint
    would otherwise reject a second blank email, and we prefer a clear error up front).
    Username handling and password hashing are inherited unchanged.
    """

    def create_user(self, username, email=None, password=None, **extra_fields):
        if not email:
            raise ValueError("An email address is required to create a user.")
        return super().create_user(username, email=email, password=password, **extra_fields)

    def create_superuser(self, username, email=None, password=None, **extra_fields):
        if not email:
            raise ValueError("An email address is required to create a superuser.")
        return super().create_superuser(username, email=email, password=password, **extra_fields)


class User(AbstractUser):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    # Override AbstractUser.email (which is optional and non-unique) to be unique + required.
    email = models.EmailField("email address", unique=True)

    objects = UserManager()

    # USERNAME_FIELD stays "username" and REQUIRED_FIELDS stays ["email"] (both inherited),
    # so username login is supported and `createsuperuser` still prompts for the email.

    def __str__(self) -> str:
        return self.username
