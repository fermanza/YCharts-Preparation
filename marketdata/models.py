"""Provider provenance and conservative per-security source versioning."""

import uuid

from django.db import models


class Security(models.Model):
    symbol = models.CharField(max_length=20, unique=True)
    currency = models.CharField(max_length=3, default="USD")
    data_version = models.PositiveBigIntegerField(default=0)

    def __str__(self):
        return self.symbol


class ImportBatch(models.Model):
    class State(models.TextChoices):
        PENDING = "pending"
        ACCEPTED = "accepted"
        REJECTED = "rejected"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    sha256 = models.CharField(max_length=64, unique=True)
    filename = models.CharField(max_length=255)
    provider = models.CharField(max_length=50, default="synthetic-demo")
    date = models.DateField(null=True)
    state = models.CharField(max_length=10, choices=State.choices, default=State.PENDING)
    errors = models.JSONField(default=list)
    symbols = models.JSONField(default=list)
    inserted = models.PositiveIntegerField(default=0)
    updated = models.PositiveIntegerField(default=0)
    ignored = models.PositiveIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True)


class PriceBar(models.Model):
    security = models.ForeignKey(Security, on_delete=models.PROTECT, related_name="prices")
    date = models.DateField()
    close = models.DecimalField(max_digits=16, decimal_places=6)
    adjusted_close = models.DecimalField(max_digits=16, decimal_places=6)
    volume = models.PositiveBigIntegerField()
    provider_revision = models.PositiveBigIntegerField()
    source_batch = models.ForeignKey(ImportBatch, on_delete=models.PROTECT, related_name="prices")

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["security", "date"], name="unique_price_date"),
            models.CheckConstraint(condition=models.Q(close__gt=0), name="positive_close"),
            models.CheckConstraint(
                condition=models.Q(adjusted_close__gt=0), name="positive_adjusted_close"
            ),
            models.CheckConstraint(
                condition=models.Q(provider_revision__gte=1), name="positive_revision"
            ),
        ]
        ordering = ["date"]


class MetricSnapshot(models.Model):
    security = models.ForeignKey(Security, on_delete=models.CASCADE, related_name="metrics")
    date = models.DateField()
    source_version = models.PositiveBigIntegerField()
    calculation_version = models.CharField(max_length=20, default="v1")
    return_1d = models.FloatField(null=True)
    sma_20 = models.FloatField(null=True)
    volatility_20 = models.FloatField(null=True)
    drawdown = models.FloatField(null=True)
    calculated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["security", "date"], name="unique_metric_date")
        ]
        ordering = ["date"]
