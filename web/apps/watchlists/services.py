"""Watchlists application service — trivial user-owned ORM CRUD, used directly (no repository).

Per the data-access rules (``docs/architecture.md``): analytical storage goes through a
repository, but simple Postgres-owned CRUD like this uses the ORM straight from the service.
Tickers are normalised to upper case so membership is case-insensitive.
"""

from __future__ import annotations

from apps.users.models import User

from .models import WatchlistItem


def add(user: User, ticker: str) -> WatchlistItem:
    """Add ``ticker`` to the user's watchlist (idempotent)."""
    item, _ = WatchlistItem.objects.get_or_create(user=user, ticker=ticker.upper())
    return item


def remove(user: User, ticker: str) -> None:
    """Remove ``ticker`` from the user's watchlist (no-op if absent)."""
    WatchlistItem.objects.filter(user=user, ticker=ticker.upper()).delete()


def list_tickers(user: User) -> list[str]:
    """The user's watchlisted tickers, alphabetical."""
    return list(WatchlistItem.objects.filter(user=user).values_list("ticker", flat=True))


def contains(user: User, ticker: str) -> bool:
    """Whether ``ticker`` is on the user's watchlist."""
    return WatchlistItem.objects.filter(user=user, ticker=ticker.upper()).exists()
