from django.apps import AppConfig


class FundamentalsScreenerConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "fundamentals_screener"
    label = "fundamentals_screener"
    verbose_name = "Fundamentals Screener"

    def ready(self) -> None:
        from . import checks  # noqa: F401 — registers the system checks as a side effect
