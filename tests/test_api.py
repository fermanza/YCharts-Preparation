from datetime import date

import pytest

from marketdata.models import MetricSnapshot
from marketdata.services import calculate_batch, ingest_file
from tests.test_pipeline import DAY, csv_file, row

pytestmark = pytest.mark.django_db


def test_health_security_and_audit_read_endpoints(client, tmp_path):
    ingest_file(csv_file(tmp_path, [row()]))
    assert client.get("/health/").json() == {"status": "ok"}
    assert client.get("/api/securities/").json()["securities"][0]["symbol"] == "ALFA"
    assert client.get("/api/batches/").json()["batches"][0]["state"] == "accepted"
    assert client.post("/api/securities/").status_code == 405


def test_warmup_nulls_and_stale_metric_rejection(client, tmp_path):
    ingest_file(csv_file(tmp_path, [row()]))
    calculate_batch(["ALFA"], DAY)
    response = client.get("/api/securities/ALFA/metrics/")
    assert response.status_code == 200
    assert response.json()["metrics"][0]["return_1d"] is None
    assert response.json()["metrics"][0]["drawdown"] == 0
    ingest_file(csv_file(tmp_path, [row(price="105", revision=2)], "new.csv"))
    response = client.get("/api/securities/ALFA/metrics/")
    assert response.status_code == 409
    assert response.json()["error"] == "data_recalculation_required"
    calculate_batch(["ALFA"], DAY)
    assert client.get("/api/securities/ALFA/metrics/").status_code == 200


@pytest.mark.parametrize("query", ["limit=0", "limit=abc", "limit=101", "as_of=wrong"])
def test_invalid_input_is_400(client, query):
    assert client.get(f"/api/securities/ALFA/metrics/?{query}").status_code == 400


def test_unknown_and_missing_metrics(client, tmp_path):
    assert client.get("/api/securities/UNKNOWN/metrics/").status_code == 404
    ingest_file(csv_file(tmp_path, [row()]))
    assert client.get("/api/securities/ALFA/metrics/").status_code == 404


def test_cutoff_excludes_newer_observations(client, tmp_path):
    ingest_file(csv_file(tmp_path, [row(day=date(2026, 10, 8))], "first.csv"))
    ingest_file(csv_file(tmp_path, [row()], "last.csv"))
    calculate_batch(["ALFA"], DAY)
    response = client.get("/api/securities/ALFA/metrics/?as_of=2026-10-08")
    assert [item["date"] for item in response.json()["metrics"]] == ["2026-10-08"]


def test_missing_latest_snapshot_cannot_look_current(client, tmp_path):
    ingest_file(csv_file(tmp_path, [row(day=date(2026, 10, 8))], "first.csv"))
    ingest_file(csv_file(tmp_path, [row()], "last.csv"))
    calculate_batch(["ALFA"], date(2026, 10, 8))
    assert client.get("/api/securities/ALFA/metrics/?limit=1").status_code == 409
    assert client.get("/api/securities/ALFA/metrics/?as_of=2026-10-08").status_code == 200


def test_old_calculation_version_is_refused(client, tmp_path):
    ingest_file(csv_file(tmp_path, [row()]))
    calculate_batch(["ALFA"], DAY)
    MetricSnapshot.objects.update(calculation_version="old")
    assert client.get("/api/securities/ALFA/metrics/").status_code == 409
