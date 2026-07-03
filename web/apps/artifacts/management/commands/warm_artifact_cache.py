"""Pre-warm / refresh the published-artifact cache.

Run as a deploy step (before starting gunicorn) and on the Release publish cadence (cron /
systemd timer) so request-path reads are cache hits, never cold downloads:

    python manage.py warm_artifact_cache            # force a fresh copy of every artifact
    python manage.py warm_artifact_cache --respect-ttl   # skip artifacts still within the TTL
"""

from __future__ import annotations

from typing import Any

from django.core.management.base import BaseCommand, CommandParser
from infrastructure import storage


class Command(BaseCommand):
    help = "Fetch a fresh local copy of every published artifact (deploy / scheduled cache warm)."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--respect-ttl",
            action="store_true",
            help="skip artifacts still within ARTIFACTS_TTL instead of forcing a re-download",
        )
        parser.add_argument(
            "--no-validate",
            action="store_true",
            help="skip schema validation of the downloaded artifacts",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        report = storage.warm(
            force=not options["respect_ttl"],
            validate=not options["no_validate"],
        )
        for name, status in report.items():
            self.stdout.write(f"{name}: {status}")
        self.stdout.write(self.style.SUCCESS("artifact cache warmed"))
