"""Watchlists application service — trivial user-owned ORM CRUD, used directly (no repository).

Per the data-access rules (``docs/architecture.md``): analytical storage goes through a
repository, but simple Postgres-owned CRUD like this uses the ORM straight from the service.
Tickers are normalised to upper case so membership is case-insensitive. ``watchlist=None`` on the
item operations targets the user's default list, keeping the original single-list call sites valid.
"""

from __future__ import annotations

from uuid import UUID

from django.db.models import Count

from apps.users.models import User

from .models import DEFAULT_WATCHLIST_NAME, Watchlist, WatchlistItem


# ── lists ────────────────────────────────────────────────────────────────────────────
def get_default(user: User) -> Watchlist:
    """The user's default watchlist, creating it on first use."""
    watchlist, _ = Watchlist.objects.get_or_create(user=user, name=DEFAULT_WATCHLIST_NAME)
    return watchlist


def list_watchlists(user: User) -> list[Watchlist]:
    """All of the user's lists, alphabetical."""
    return list(Watchlist.objects.filter(user=user))


def list_watchlists_with_counts(user: User) -> list[Watchlist]:
    """The user's lists, each annotated with an ``item_count`` (for the overview page)."""
    return list(Watchlist.objects.filter(user=user).annotate(item_count=Count("items")))


def get_watchlist(user: User, watchlist_id: UUID | str) -> Watchlist | None:
    """A single list owned by ``user`` (``None`` if it doesn't exist / isn't theirs)."""
    return Watchlist.objects.filter(user=user, id=watchlist_id).first()


def create(user: User, name: str) -> Watchlist | None:
    """Create a named list; ``None`` if the name is blank or already taken by this user."""
    name = name.strip()
    if not name or Watchlist.objects.filter(user=user, name=name).exists():
        return None
    return Watchlist.objects.create(user=user, name=name)


def rename(user: User, watchlist_id: UUID | str, name: str) -> bool:
    """Rename one of the user's lists; ``False`` if missing, blank, or a duplicate name."""
    name = name.strip()
    watchlist = get_watchlist(user, watchlist_id)
    if watchlist is None or not name:
        return False
    if Watchlist.objects.filter(user=user, name=name).exclude(id=watchlist.id).exists():
        return False
    watchlist.name = name
    watchlist.save(update_fields=["name"])
    return True


def delete(user: User, watchlist_id: UUID | str) -> None:
    """Delete one of the user's lists (and its items); no-op if absent."""
    Watchlist.objects.filter(user=user, id=watchlist_id).delete()


# ── items ────────────────────────────────────────────────────────────────────────────
def _resolve(user: User, watchlist_id: UUID | str | None) -> Watchlist | None:
    """The target list: the named one if owned by ``user``, else the default when ``None``."""
    if watchlist_id is None:
        return get_default(user)
    return get_watchlist(user, watchlist_id)


def add(user: User, ticker: str, watchlist_id: UUID | str | None = None) -> WatchlistItem | None:
    """Add ``ticker`` to the given list (default list if unspecified); idempotent."""
    watchlist = _resolve(user, watchlist_id)
    if watchlist is None:
        return None
    item, _ = WatchlistItem.objects.get_or_create(watchlist=watchlist, ticker=ticker.upper())
    return item


def add_to_named_list(user: User, ticker: str, name: str) -> WatchlistItem | None:
    """Add ``ticker`` to the list called ``name``, creating that list if needed; ``None`` if blank."""
    name = name.strip()
    if not name:
        return None
    watchlist, _ = Watchlist.objects.get_or_create(user=user, name=name)
    item, _ = WatchlistItem.objects.get_or_create(watchlist=watchlist, ticker=ticker.upper())
    return item


def remove(user: User, ticker: str, watchlist_id: UUID | str | None = None) -> None:
    """Remove ``ticker`` from the given list (default list if unspecified); no-op if absent."""
    watchlist = _resolve(user, watchlist_id)
    if watchlist is None:
        return
    WatchlistItem.objects.filter(watchlist=watchlist, ticker=ticker.upper()).delete()


def list_tickers(watchlist: Watchlist) -> list[str]:
    """The tickers in a single list, alphabetical."""
    return list(watchlist.items.values_list("ticker", flat=True))


def contains(user: User, ticker: str) -> bool:
    """Whether ``ticker`` is on *any* of the user's lists."""
    return WatchlistItem.objects.filter(watchlist__user=user, ticker=ticker.upper()).exists()


def lists_for_ticker(user: User, ticker: str) -> set[UUID]:
    """Ids of the user's lists that contain ``ticker`` (drives the picker's checkmarks)."""
    return set(
        WatchlistItem.objects.filter(watchlist__user=user, ticker=ticker.upper()).values_list(
            "watchlist_id", flat=True
        )
    )
