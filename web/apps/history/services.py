"""Browsing-history application service — trivial user-owned ORM CRUD, used directly (no repo).

``record`` upserts the (user, ticker) row (bumping its ``viewed_at``) then trims the user's
history to the ``CAP`` most-recent entries. Tickers are upper-cased so history is keyed
case-insensitively. Analytical reads still go through repositories; this is Postgres-owned
per-user state, so the ORM is used straight from the service (see ``docs/architecture.md``).
"""

from __future__ import annotations

from apps.users.models import User

from .models import HistoryItem

CAP = 50  # keep at most this many recently-viewed companies per user


def record(user: User, ticker: str) -> None:
    """Mark ``ticker`` as just viewed by ``user``, then trim to the ``CAP`` most recent."""
    ticker = ticker.upper()
    # update_or_create bumps viewed_at (auto_now) on an existing row, or inserts a new one.
    HistoryItem.objects.update_or_create(user=user, ticker=ticker)
    keep = list(
        HistoryItem.objects.filter(user=user)
        .order_by("-viewed_at")
        .values_list("id", flat=True)[:CAP]
    )
    HistoryItem.objects.filter(user=user).exclude(id__in=keep).delete()


def recent_tickers(user: User, limit: int = CAP) -> list[str]:
    """The user's recently-viewed tickers, most-recent first (bounded by ``limit``)."""
    return list(
        HistoryItem.objects.filter(user=user)
        .order_by("-viewed_at")
        .values_list("ticker", flat=True)[:limit]
    )


def clear(user: User) -> None:
    """Drop the user's entire browsing history."""
    HistoryItem.objects.filter(user=user).delete()
