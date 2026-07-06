"""DRF serializers over the repository read-model DTOs.

These are output-only serializers: they mirror the frozen dataclasses in ``repositories.dtos``
field-for-field (DRF reads them by attribute access) and are never used to validate input — the
API is read-only. No business logic lives here; a renamed DTO field changes only the matching
serializer field, nothing above it.
"""

from __future__ import annotations

from rest_framework import serializers


class CompanySummarySerializer(serializers.Serializer):
    ticker = serializers.CharField()
    name = serializers.CharField()
    sector = serializers.CharField(allow_null=True)
    industry = serializers.CharField(allow_null=True)
    exchange = serializers.CharField(allow_null=True)
    country = serializers.CharField(allow_null=True)
    description = serializers.CharField(allow_null=True)
    website = serializers.CharField(allow_null=True)
    employees = serializers.IntegerField(allow_null=True)
    founded = serializers.CharField(allow_null=True)
    has_logo = serializers.BooleanField(allow_null=True)


class MetricPointSerializer(serializers.Serializer):
    ticker = serializers.CharField()
    metric = serializers.CharField()
    unit = serializers.CharField(allow_null=True)
    fiscal_year = serializers.IntegerField()
    value = serializers.FloatField(allow_null=True)
    category = serializers.CharField(allow_null=True)
    subcategory = serializers.CharField(allow_null=True)
    sort_order = serializers.FloatField(allow_null=True)


class CompanyDetailSerializer(serializers.Serializer):
    summary = CompanySummarySerializer()
    metrics = MetricPointSerializer(many=True)


class ScreenRowSerializer(serializers.Serializer):
    ticker = serializers.CharField()
    fiscal_year = serializers.IntegerField()
    value = serializers.FloatField(allow_null=True)


class CompanyListRowSerializer(serializers.Serializer):
    ticker = serializers.CharField()
    name = serializers.CharField()
    sector = serializers.CharField(allow_null=True)
    industry = serializers.CharField(allow_null=True)
    country = serializers.CharField(allow_null=True)
    metric_value = serializers.FloatField(allow_null=True)
    fiscal_year = serializers.IntegerField(allow_null=True)
    has_logo = serializers.BooleanField(allow_null=True)


class FootballBarSerializer(serializers.Serializer):
    method = serializers.CharField()
    bear = serializers.FloatField()
    mid = serializers.FloatField()
    bull = serializers.FloatField()
    fiscal_year = serializers.IntegerField()


class FootballFieldSerializer(serializers.Serializer):
    bars = FootballBarSerializer(many=True)
    price = serializers.FloatField(allow_null=True)


# ── response envelopes (describe the viewset payloads for the OpenAPI schema) ───────────
class CompanyListResponseSerializer(serializers.Serializer):
    count = serializers.IntegerField()
    page = serializers.IntegerField()
    page_size = serializers.IntegerField()
    results = CompanyListRowSerializer(many=True)


class ScreenResponseSerializer(serializers.Serializer):
    metric = serializers.CharField()
    count = serializers.IntegerField()
    results = ScreenRowSerializer(many=True)


class ValuationResponseSerializer(serializers.Serializer):
    ticker = serializers.CharField()
    margin_of_safety = MetricPointSerializer(many=True)
    football_field = FootballFieldSerializer()


class ErrorBodySerializer(serializers.Serializer):
    status = serializers.IntegerField()
    message = serializers.CharField()


class ErrorEnvelopeSerializer(serializers.Serializer):
    """The single error shape: ``{"error": {"status", "message"}}`` (see apps.api.exceptions)."""

    error = ErrorBodySerializer()
