"""Favorite model — one row per (user, ticker) the user has starred as a favorite.

Favorites are distinct from the watchlist (see ``apps.watchlists``): the watchlist is the set
the user is *tracking*, favorites are the ones they've *marked as liked*. Same one-row-per-pair
shape. Application entity, so it carries an explicit UUID primary key (never Auto/BigAutoField).
"""

from __future__ import annotations

import uuid

from django.conf import settings
from django.db import models


class FavoriteItem(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="favorite_items",
    )
    ticker = models.CharField(max_length=16)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["ticker"]
        constraints = [
            models.UniqueConstraint(fields=["user", "ticker"], name="uniq_favorite_user_ticker"),
        ]

    def __str__(self) -> str:
        return f"{self.user_id}:{self.ticker}"
