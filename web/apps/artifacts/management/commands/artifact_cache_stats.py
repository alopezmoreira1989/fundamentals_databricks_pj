"""Print this process's artifact-cache hit/miss metrics (hits / stale_hits / misses / refreshes
/ errors). Counters are per-process and in-memory — a cross-process metrics backend is the
observability task (#157)."""

from __future__ import annotations

from typing import Any

from django.core.management.base import BaseCommand
from infrastructure import storage


class Command(BaseCommand):
    help = "Print the artifact-cache hit/miss metrics for this process."

    def handle(self, *args: Any, **options: Any) -> None:
        for name, value in storage.METRICS.snapshot().items():
            self.stdout.write(f"{name}: {value}")
