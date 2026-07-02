"""Watchlist model — one row per (user, ticker) the user is tracking.

A user's watchlist is simply their set of ``WatchlistItem`` rows (no named lists yet — YAGNI).
Application entity, so it carries an explicit UUID primary key (never Auto/BigAutoField).
"""

from __future__ import annotations

import uuid

from django.conf import settings
from django.db import models


class WatchlistItem(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="watchlist_items",
    )
    ticker = models.CharField(max_length=16)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["ticker"]
        constraints = [
            models.UniqueConstraint(fields=["user", "ticker"], name="uniq_watchlist_user_ticker"),
        ]

    def __str__(self) -> str:
        return f"{self.user_id}:{self.ticker}"
