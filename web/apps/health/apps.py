from django.apps import AppConfig


class HealthConfig(AppConfig):
    # Operational app exposing liveness/readiness probes for the platform's health checks and
    # uptime monitor. No models; the readiness verdict is computed by ``services.health``.
    name = "apps.health"
    label = "health"
    default_auto_field = "django.db.models.BigAutoField"
