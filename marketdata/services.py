"""Business services shared by Airflow, management commands and repair workers.

PostgreSQL row locks serialize input changes and publication per security.
SQLite supports a single-writer local walkthrough, not these lock guarantees.
"""

import csv
import hashlib
import io
import logging
import math
import re
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pandas as pd
from django.db import transaction
from django.utils import timezone

from .metrics import calculate_metrics
from .models import ImportBatch, MetricSnapshot, PriceBar, Security

logger = logging.getLogger(__name__)
FIELDS = {"symbol", "date", "close", "adjusted_close", "volume", "provider_revision"}
MAX_FILE_BYTES = 64 * 1024 * 1024
MAX_ROWS = 250_000
MAX_INTEGER = 2**63 - 1


class BatchRejected(ValueError):
    """Deterministic input failure: retrying the same bytes cannot fix it."""

    def __init__(self, errors, batch_id=None):
        self.errors = errors if isinstance(errors, list) else [str(errors)]
        self.batch_id = str(batch_id) if batch_id else None
        super().__init__("; ".join(self.errors))


class PipelineIncomplete(RuntimeError):
    """Required output is absent or derived from an obsolete input version."""


def parse_date(value):
    if type(value) is date:
        return value
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError("date must use YYYY-MM-DD")
    return date.fromisoformat(value)


def _price(value):
    number = Decimal(value)
    if not number.is_finite() or not 0 < number < Decimal("10000000000"):
        raise ValueError("price must be finite and positive with at most ten integer digits")
    if number != number.quantize(Decimal("0.000001")):
        raise ValueError("price supports at most six fractional digits")
    return number


def _integer(value, minimum):
    if not re.fullmatch(r"[0-9]+", value):
        raise ValueError("volume and revision must be unsigned integers")
    result = int(value)
    if not minimum <= result <= MAX_INTEGER:
        raise ValueError("integer is outside supported bounds")
    return result


def _validate(raw, expected_date):
    try:
        reader = csv.DictReader(io.StringIO(raw.decode("utf-8-sig")), strict=True)
        if set(reader.fieldnames or []) != FIELDS or len(reader.fieldnames or []) != len(FIELDS):
            raise BatchRejected(f"CSV header must contain exactly: {', '.join(sorted(FIELDS))}")
        rows, seen, errors = [], set(), []
        for line, row in enumerate(reader, start=2):
            if len(rows) >= MAX_ROWS:
                raise BatchRejected(f"partition exceeds {MAX_ROWS} rows; shard the input")
            try:
                if None in row or any(value is None for value in row.values()):
                    raise ValueError("row has missing or extra fields")
                symbol = row["symbol"]
                if not re.fullmatch(r"[A-Z][A-Z0-9.-]{0,19}", symbol):
                    raise ValueError("symbol must use uppercase supported characters, length 1–20")
                trade_date = parse_date(row["date"])
                if expected_date and trade_date != expected_date:
                    raise ValueError(f"row date does not match partition {expected_date}")
                key = (symbol, trade_date)
                if key in seen:
                    raise ValueError("duplicate symbol/date inside partition")
                seen.add(key)
                rows.append(
                    {
                        "symbol": symbol,
                        "date": trade_date,
                        "close": _price(row["close"]),
                        "adjusted_close": _price(row["adjusted_close"]),
                        "volume": _integer(row["volume"], 0),
                        "provider_revision": _integer(row["provider_revision"], 1),
                    }
                )
            except (ValueError, InvalidOperation, TypeError) as error:
                errors.append(f"line {line}: {error}")
                if len(errors) >= 20:
                    break
        if errors:
            raise BatchRejected(errors)
        if not rows:
            raise BatchRejected("partition has no price rows")
        if len({row["date"] for row in rows}) != 1:
            raise BatchRejected("each partition must contain exactly one observation date")
        return rows
    except (UnicodeDecodeError, csv.Error) as error:
        raise BatchRejected(f"invalid UTF-8 CSV: {error}") from error


def _record_rejection(checksum, filename, error):
    with transaction.atomic():
        batch, _ = ImportBatch.objects.get_or_create(
            sha256=checksum, defaults={"filename": filename[:255]}
        )
        batch = ImportBatch.objects.select_for_update().get(pk=batch.pk)
        # A caller's wrong expected date cannot overwrite an accepted audit.
        if batch.state != ImportBatch.State.ACCEPTED:
            batch.state = ImportBatch.State.REJECTED
            batch.errors = error.errors
            batch.completed_at = timezone.now()
            batch.save(update_fields=["state", "errors", "completed_at"])
        error.batch_id = str(batch.pk)


def _summary(batch, replayed=False):
    return {
        "batch_id": str(batch.pk),
        "symbols": batch.symbols,
        "inserted": batch.inserted,
        "updated": batch.updated,
        "ignored": batch.ignored,
        "replayed": replayed,
    }


def ingest_file(path, expected_date=None):
    """Validate daily input then commit all accepted revisions atomically.

    Replaying an accepted digest returns its original audit counters and
    replayed=True. Older revisions are ignored; equal revisions with conflicting
    values reject the whole batch; higher revisions replace prices and invalidate
    derived history through a per-security source version.
    """
    path = Path(path)
    expected_date = parse_date(expected_date) if expected_date else None
    with path.open("rb") as source:
        raw = source.read(MAX_FILE_BYTES + 1)
    checksum = hashlib.sha256(raw).hexdigest()
    try:
        if len(raw) > MAX_FILE_BYTES:
            raise BatchRejected("partition exceeds 64 MiB; shard the input")
        rows = _validate(raw, expected_date)
        with transaction.atomic():
            batch, _ = ImportBatch.objects.get_or_create(
                sha256=checksum, defaults={"filename": path.name[:255]}
            )
            batch = ImportBatch.objects.select_for_update().get(pk=batch.pk)
            if batch.state == ImportBatch.State.ACCEPTED:
                return _summary(batch, replayed=True)
            symbols = sorted({row["symbol"] for row in rows})
            for symbol in symbols:
                Security.objects.get_or_create(symbol=symbol)
            securities = {
                item.symbol: item
                for item in Security.objects.select_for_update()
                .filter(symbol__in=symbols)
                .order_by("symbol")
            }
            trade_date = rows[0]["date"]
            existing = {
                item.security_id: item
                for item in PriceBar.objects.filter(
                    security_id__in=[security.pk for security in securities.values()],
                    date=trade_date,
                )
            }
            inserts, updates, changed = [], [], set()
            ignored = 0
            for row in rows:
                security = securities[row["symbol"]]
                current = existing.get(security.pk)
                values = {
                    key: row[key]
                    for key in ("close", "adjusted_close", "volume", "provider_revision")
                }
                if current and current.provider_revision > row["provider_revision"]:
                    ignored += 1
                    continue
                if current and current.provider_revision == row["provider_revision"]:
                    if any(getattr(current, key) != value for key, value in values.items()):
                        raise BatchRejected(
                            f"{security.symbol} {trade_date}: conflicting values at revision "
                            f"{current.provider_revision}"
                        )
                    ignored += 1
                    continue
                if current:
                    for key, value in values.items():
                        setattr(current, key, value)
                    current.source_batch = batch
                    updates.append(current)
                else:
                    inserts.append(
                        PriceBar(security=security, date=trade_date, source_batch=batch, **values)
                    )
                changed.add(security.symbol)
            PriceBar.objects.bulk_create(inserts, batch_size=1000)
            PriceBar.objects.bulk_update(
                updates,
                ["close", "adjusted_close", "volume", "provider_revision", "source_batch"],
                batch_size=1000,
            )
            for symbol in changed:
                securities[symbol].data_version += 1
            Security.objects.bulk_update(
                [securities[symbol] for symbol in changed], ["data_version"], batch_size=1000
            )
            batch.state = ImportBatch.State.ACCEPTED
            batch.errors = []
            batch.date = trade_date
            batch.symbols = symbols
            batch.inserted, batch.updated, batch.ignored = len(inserts), len(updates), ignored
            batch.completed_at = timezone.now()
            batch.save()
            summary = _summary(batch)
    except BatchRejected as error:
        _record_rejection(checksum, path.name, error)
        logger.warning(
            "provider_batch_rejected batch_id=%s errors=%s", error.batch_id, error.errors
        )
        raise
    logger.info("provider_batch_accepted %s", summary)
    return summary


def calculate_batch(symbols, as_of):
    """Upsert full history through as_of while holding one security's row lock.

    One history bounds working memory; rebuilding the prefix correctly propagates
    historical corrections to later returns, rolling windows and running peaks.
    Later snapshots are retained but remain stale after a correction.
    """
    as_of = parse_date(as_of)
    if not symbols or len(symbols) > 100:
        raise ValueError("calculation batch must contain 1–100 symbols")
    count = 0
    for symbol in sorted(set(symbols)):
        with transaction.atomic():
            security = Security.objects.select_for_update().get(symbol=symbol)
            history = list(
                PriceBar.objects.filter(security=security, date__lte=as_of).values(
                    "date", "adjusted_close"
                )
            )
            if not history:
                raise PipelineIncomplete(f"{symbol} has no prices through {as_of}")
            frame = pd.DataFrame(history).assign(symbol=symbol)
            frame["adjusted_close"] = frame["adjusted_close"].astype(float)
            snapshots = []
            for row in calculate_metrics(frame).itertuples(index=False):
                values = {
                    key: None if pd.isna(getattr(row, key)) else float(getattr(row, key))
                    for key in ("return_1d", "sma_20", "volatility_20", "drawdown")
                }
                if any(value is not None and not math.isfinite(value) for value in values.values()):
                    raise PipelineIncomplete(f"{symbol}: non-finite calculation")
                snapshots.append(
                    MetricSnapshot(
                        security=security,
                        date=row.date.date(),
                        source_version=security.data_version,
                        calculation_version="v1",
                        **values,
                    )
                )
            MetricSnapshot.objects.bulk_create(
                snapshots,
                update_conflicts=True,
                unique_fields=["security", "date"],
                update_fields=[
                    "source_version",
                    "calculation_version",
                    "return_1d",
                    "sma_20",
                    "volatility_20",
                    "drawdown",
                    "calculated_at",
                ],
                batch_size=1000,
            )
            count += len(snapshots)
    return {"symbols": len(set(symbols)), "snapshots": count, "as_of": as_of.isoformat()}


def verify_pipeline(batch_id, as_of):
    """Verify exact partition date plus complete current-version metric history."""
    as_of = parse_date(as_of)
    batch = ImportBatch.objects.get(pk=batch_id)
    if batch.state != ImportBatch.State.ACCEPTED or batch.date != as_of:
        raise PipelineIncomplete("expected an accepted partition matching as_of")
    missing = []
    with transaction.atomic():
        securities = list(
            Security.objects.select_for_update().filter(symbol__in=batch.symbols).order_by("symbol")
        )
        if len(securities) != len(batch.symbols):
            raise PipelineIncomplete("accepted partition references missing securities")
        for security in securities:
            price_dates = set(
                PriceBar.objects.filter(security=security, date__lte=as_of).values_list(
                    "date", flat=True
                )
            )
            metric_dates = set(
                MetricSnapshot.objects.filter(
                    security=security,
                    date__lte=as_of,
                    source_version=security.data_version,
                    calculation_version="v1",
                ).values_list("date", flat=True)
            )
            if as_of not in price_dates or price_dates != metric_dates:
                missing.append(security.symbol)
    if missing:
        raise PipelineIncomplete(f"missing or stale metrics: {', '.join(missing[:20])}")
    return {
        "batch_id": str(batch.pk),
        "as_of": as_of.isoformat(),
        "verified_securities": len(batch.symbols),
        "status": "complete",
    }
