"""Watchlist models — named lists a user tracks, each holding a set of tickers.

A user owns zero or more :class:`Watchlist` rows (e.g. 'Energy', 'Compounders'); each holds
:class:`WatchlistItem` rows. The same ticker may live in several of a user's lists. Every user
gets an implicit default list (see ``services.get_default``) so the single-list UX still works.
Both are application entities, so they carry explicit UUID primary keys (never Auto/BigAutoField).
"""

from __future__ import annotations

import uuid

from django.conf import settings
from django.db import models

DEFAULT_WATCHLIST_NAME = "Watchlist"


class Watchlist(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="watchlists",
    )
    name = models.CharField(max_length=64)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["name"]
        constraints = [
            models.UniqueConstraint(fields=["user", "name"], name="uniq_watchlist_user_name"),
        ]

    def __str__(self) -> str:
        return f"{self.user_id}:{self.name}"


class WatchlistItem(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    watchlist = models.ForeignKey(
        Watchlist,
        on_delete=models.CASCADE,
        related_name="items",
    )
    ticker = models.CharField(max_length=16)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["ticker"]
        constraints = [
            models.UniqueConstraint(fields=["watchlist", "ticker"], name="uniq_watchlist_item_ticker"),
        ]

    def __str__(self) -> str:
        return f"{self.watchlist_id}:{self.ticker}"
