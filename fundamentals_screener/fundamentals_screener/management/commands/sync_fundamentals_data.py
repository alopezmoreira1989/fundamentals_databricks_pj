from django.core.management.base import BaseCommand

from fundamentals_screener import data_source


class Command(BaseCommand):
    help = (
        'Download the latest dashboard_* parquet artifacts + meta JSON from '
        "fundamentals_databricks_pj's GitHub Release \"latest\" into FUNDAMENTALS_DATA_PATH, "
        'then validate them against the fundamentals_pipeline schema contract.'
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--force', action='store_true',
            help='Re-download every artifact even if already cached.',
        )

    def handle(self, *args, **options):
        updated = data_source.sync(force=options['force'])
        if updated:
            self.stdout.write(self.style.SUCCESS(f"Updated: {', '.join(updated)}"))
        else:
            self.stdout.write('Already up to date.')

        violations = data_source.validate()
        if violations:
            self.stdout.write(self.style.ERROR('Schema violations:'))
            for violation in violations:
                self.stdout.write(f'  - {violation}')
        else:
            self.stdout.write(self.style.SUCCESS('All artifacts valid.'))
