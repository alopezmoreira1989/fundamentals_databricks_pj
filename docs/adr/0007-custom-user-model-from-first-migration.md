# ADR-0007: Custom user model, set before the first migration

- **Status:** Accepted
- **Date:** 2026-07-03  <!-- recorded retroactively; decided before any migration ran -->
- **Deciders:** repo owner

## Context

The app has authenticated users (ADR-0002) and will grow user-related fields over time. Django
ships a default `User` model, but **swapping the user model after migrations have run is a
well-known, painful migration** — Django's own docs advise setting a custom user model at the
very start of a project even if it is initially identical to the default. We also want two
things the default doesn't give: a **UUID primary key** (ADR-0005) and a **unique, required
email** (the default email is optional and non-unique).

## Decision

We will define a custom user model from the outset — `apps/users/models.py`:
`User(AbstractUser)` with a UUID pk and a unique/required email, plus a small `UserManager` that
enforces the email invariant on `create_user`/`create_superuser`. `AUTH_USER_MODEL = "users.User"`
is set **before the first migration**, so Django's default `User` is never migrated. The model is
intentionally minimal (YAGNI) — username login and Django's permission/staff/superuser plumbing
are kept; no profile fields until one is actually needed. Per ADR-0006, users CRUD uses the ORM
directly from the service layer (no `UserRepository`).

## Consequences

- Future user fields are ordinary additive migrations on our own model — we never face the
  swap-the-user-model migration.
- UUID pk and a unique/required email are guaranteed from row one; the email invariant is
  enforced in one place (`UserManager`).
- `AUTH_USER_MODEL` is effectively immutable now — it was pinned before the first migration and
  must not change.
- `AbstractUser` keeps username as the login field. If email-as-username is ever wanted, that is a
  new decision (superseding ADR) with its own migration, not a tweak here.

## Alternatives considered

- **Django's default `User`.** Rejected: no UUID pk, optional/non-unique email, and switching
  away later is the painful migration this ADR exists to avoid.
- **`AbstractBaseUser` (fully custom).** Rejected as over-engineering: we'd reimplement the
  permissions/staff/superuser machinery `AbstractUser` provides for free, with no need yet.
- **A separate `Profile` model 1:1 with the default `User`.** Rejected: still leaves the default
  user's integer pk and weak email, and adds a join for every user attribute.
