"""Browsing-history model — one row per (user, ticker) the user has recently viewed.

Re-viewing a company updates its ``viewed_at`` in place (unique per user+ticker), so the row
count per user is bounded by the number of *distinct* companies viewed; the service trims that
to the most-recent ``CAP`` (see ``services.record``). Application entity → explicit UUID pk.

``viewed_at`` is set by the service (not ``auto_now``) so it can be kept strictly increasing per
user — wall-clock resolution is coarse on some platforms (~15ms on Windows), and equal timestamps
would make ``-viewed_at`` ordering non-deterministic. The service guarantees each record advances
the user's max ``viewed_at``, so ordering is exact.
"""

from __future__ import annotations

import uuid

from django.conf import settings
from django.db import models


class HistoryItem(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="history_items",
    )
    ticker = models.CharField(max_length=16)
    viewed_at = models.DateTimeField()  # set by the service, kept strictly increasing per user

    class Meta:
        ordering = ["-viewed_at"]  # most-recently viewed first
        constraints = [
            models.UniqueConstraint(fields=["user", "ticker"], name="uniq_history_user_ticker"),
        ]

    def __str__(self) -> str:
        return f"{self.user_id}:{self.ticker}"
