import csv
from datetime import date, timedelta
from decimal import Decimal

import pytest
from django.core.management import call_command

from marketdata.models import ImportBatch, MetricSnapshot, PriceBar, Security
from marketdata.services import (
    BatchRejected,
    PipelineIncomplete,
    calculate_batch,
    ingest_file,
    verify_pipeline,
)
from marketdata.tasks import recalculate_metrics

pytestmark = pytest.mark.django_db
FIELDS = ["symbol", "date", "close", "adjusted_close", "volume", "provider_revision"]
DAY = date(2026, 10, 9)


def csv_file(tmp_path, rows, name="prices.csv"):
    path = tmp_path / name
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    return path


def row(symbol="ALFA", day=DAY, price="100.000000", revision=1, **overrides):
    return {
        "symbol": symbol,
        "date": str(day),
        "close": price,
        "adjusted_close": price,
        "volume": 100,
        "provider_revision": revision,
        **overrides,
    }


def test_replay_does_not_duplicate_or_invalidate_metrics(tmp_path):
    path = csv_file(tmp_path, [row()])
    first = ingest_file(path, DAY)
    calculate_batch(["ALFA"], DAY)
    replay = ingest_file(path, DAY)
    assert first["batch_id"] == replay["batch_id"]
    assert replay["replayed"] is True
    assert PriceBar.objects.count() == ImportBatch.objects.count() == 1
    assert Security.objects.get().data_version == MetricSnapshot.objects.get().source_version == 1


def test_revision_ordering_and_stale_version(tmp_path):
    ingest_file(csv_file(tmp_path, [row()]))
    calculate_batch(["ALFA"], DAY)
    result = ingest_file(csv_file(tmp_path, [row(price="105", revision=2)], "new.csv"))
    assert result["updated"] == 1
    assert PriceBar.objects.get().adjusted_close == Decimal("105")
    assert Security.objects.get().data_version == 2
    assert MetricSnapshot.objects.get().source_version == 1
    assert ingest_file(csv_file(tmp_path, [row(price="90")], "old.csv"))["ignored"] == 1
    assert PriceBar.objects.get().adjusted_close == Decimal("105")


@pytest.mark.parametrize(
    "invalid",
    [
        {"adjusted_close": "NaN"},
        {"close": "0"},
        {"adjusted_close": "-1"},
        {"close": "Infinity"},
        {"volume": "-1"},
        {"volume": "1.5"},
        {"provider_revision": "0"},
        {"close": "1.0000001"},
        {"symbol": "alfa"},
        {"date": "not-a-date"},
        {"volume": str(2**63)},
        {"date": "20261009"},
    ],
)
def test_invalid_row_rejects_entire_partition(tmp_path, invalid):
    path = csv_file(tmp_path, [row(), row(**{"symbol": "BETA", **invalid})])
    with pytest.raises(BatchRejected) as caught:
        ingest_file(path)
    assert PriceBar.objects.count() == Security.objects.count() == 0
    batch = ImportBatch.objects.get()
    assert batch.state == "rejected" and batch.errors
    assert caught.value.batch_id == str(batch.pk)


def test_same_revision_conflict_rolls_back_other_security(tmp_path):
    ingest_file(csv_file(tmp_path, [row("BETA")]))
    with pytest.raises(BatchRejected, match="conflicting values"):
        ingest_file(csv_file(tmp_path, [row(), row("BETA", price="999")], "conflict.csv"))
    assert list(Security.objects.values_list("symbol", flat=True)) == ["BETA"]
    assert PriceBar.objects.get().close == Decimal("100")


def test_duplicate_and_wrong_date_are_rejected(tmp_path):
    with pytest.raises(BatchRejected, match="duplicate"):
        ingest_file(csv_file(tmp_path, [row(), row()]))
    with pytest.raises(BatchRejected, match="partition"):
        ingest_file(csv_file(tmp_path, [row()], "wrong.csv"), DAY - timedelta(days=1))


def test_wrong_date_cannot_corrupt_accepted_audit(tmp_path):
    path = csv_file(tmp_path, [row()])
    result = ingest_file(path, DAY)
    with pytest.raises(BatchRejected):
        ingest_file(path, DAY - timedelta(days=1))
    assert ImportBatch.objects.get(pk=result["batch_id"]).state == "accepted"


def test_historical_correction_propagates_and_partial_repair_stays_stale(tmp_path):
    first_day = DAY - timedelta(days=1)
    ingest_file(csv_file(tmp_path, [row(day=first_day)], "first.csv"))
    last = ingest_file(csv_file(tmp_path, [row(price="110")], "last.csv"))
    calculate_batch(["ALFA"], DAY)
    assert MetricSnapshot.objects.get(date=DAY).return_1d == pytest.approx(0.1)
    ingest_file(csv_file(tmp_path, [row(day=first_day, price="105", revision=2)], "repair.csv"))
    calculate_batch(["ALFA"], first_day)
    assert MetricSnapshot.objects.get(date=first_day).return_1d is None
    with pytest.raises(PipelineIncomplete, match="stale"):
        verify_pipeline(last["batch_id"], DAY)
    calculate_batch(["ALFA"], DAY)
    assert MetricSnapshot.objects.get(date=DAY).return_1d == pytest.approx(110 / 105 - 1)
    assert verify_pipeline(last["batch_id"], DAY)["status"] == "complete"
    calculate_batch(["ALFA"], DAY)
    assert MetricSnapshot.objects.count() == 2


def test_missing_historical_snapshot_fails_quality_gate(tmp_path):
    ingest_file(csv_file(tmp_path, [row(day=DAY - timedelta(days=1))], "first.csv"))
    last = ingest_file(csv_file(tmp_path, [row()], "last.csv"))
    calculate_batch(["ALFA"], DAY)
    MetricSnapshot.objects.filter(date=DAY - timedelta(days=1)).delete()
    with pytest.raises(PipelineIncomplete):
        verify_pipeline(last["batch_id"], DAY)


def test_celery_task_is_idempotent_on_redelivery(tmp_path):
    ingest_file(csv_file(tmp_path, [row()]))
    assert recalculate_metrics.apply(args=(["ALFA"], str(DAY)), throw=True).result["snapshots"] == 1
    recalculate_metrics.apply(args=(["ALFA"], str(DAY)), throw=True)
    assert MetricSnapshot.objects.count() == 1


def test_demo_replay_preserves_cardinality(capsys):
    call_command("demo_pipeline")
    original = (
        PriceBar.objects.count(),
        MetricSnapshot.objects.count(),
        ImportBatch.objects.count(),
    )
    assert original == (87, 87, 29)
    call_command("demo_pipeline")
    assert original == (
        PriceBar.objects.count(),
        MetricSnapshot.objects.count(),
        ImportBatch.objects.count(),
    )
    assert "complete" in capsys.readouterr().out
