from django.apps import AppConfig


class ArtifactsConfig(AppConfig):
    # Operational app for the published-artifact cache: management commands to warm the cache
    # (on deploy / on schedule) and to read hit/miss metrics. No models, no request-time views.
    name = "apps.artifacts"
    label = "artifacts"
    default_auto_field = "django.db.models.BigAutoField"
