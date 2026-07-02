from django.apps import AppConfig


class HistoryConfig(AppConfig):
    # Imported as `apps.history`; label is the last component, `history`.
    name = "apps.history"
    label = "history"
    default_auto_field = "django.db.models.BigAutoField"
