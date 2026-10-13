"""Real PostgreSQL contention checks; automatically skipped on SQLite.

Events hold actual application transactions open. pg_stat_activity then proves
the contender is waiting on a database lock before its holder is released.
"""

import hashlib
import time
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from threading import Event

import pytest
from django.db import close_old_connections, connection, connections, transaction

from marketdata import services
from marketdata.models import ImportBatch, MetricSnapshot, PriceBar, Security
from marketdata.services import PipelineIncomplete, calculate_batch, ingest_file, verify_pipeline
from tests.test_pipeline import DAY, csv_file, row

pytestmark = [
    pytest.mark.django_db(transaction=True),
    pytest.mark.skipif(connection.vendor != "postgresql", reason="Requires PostgreSQL row locks"),
]


def start_work(pool, operation):
    """Publish the backend PID before running work on a separate DB connection."""
    process_ids = Queue()

    def run():
        close_old_connections()
        try:
            with connections["default"].cursor() as cursor:
                cursor.execute("SELECT pg_backend_pid()")
                process_ids.put(cursor.fetchone()[0])
            return operation()
        finally:
            connections.close_all()

    future = pool.submit(run)
    return future, process_ids.get(timeout=10)


def assert_waiting_on_lock(process_id, blocker=None):
    deadline = time.monotonic() + 10
    last = None
    while time.monotonic() < deadline:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT wait_event_type, pg_blocking_pids(pid) "
                "FROM pg_stat_activity WHERE pid = %s",
                [process_id],
            )
            last = cursor.fetchone()
        if last and last[0] == "Lock" and last[1]:
            if blocker is not None:
                assert blocker in last[1], last
            return
        time.sleep(0.02)
    pytest.fail(f"Backend {process_id} did not reach a real lock wait; last state: {last}")


def test_simultaneous_identical_digest_commits_once_and_replays(tmp_path, monkeypatch):
    path = csv_file(tmp_path, [row()])
    checksum = hashlib.sha256(path.read_bytes()).hexdigest()
    entered, release = Event(), Event()
    summarize = services._summary

    def hold_first_uncommitted_batch(batch, replayed=False):
        result = summarize(batch, replayed)
        if batch.sha256 == checksum and not replayed:
            entered.set()
            assert release.wait(10), "Timed out releasing the first import transaction"
        return result

    monkeypatch.setattr(services, "_summary", hold_first_uncommitted_batch)
    with ThreadPoolExecutor(max_workers=2) as pool:
        try:
            first, first_pid = start_work(pool, lambda: ingest_file(path, DAY))
            assert entered.wait(10)
            second, second_pid = start_work(pool, lambda: ingest_file(path, DAY))
            assert_waiting_on_lock(second_pid, first_pid)
            assert not second.done()
        finally:
            release.set()
        first_result, second_result = first.result(timeout=10), second.result(timeout=10)

    assert first_result["batch_id"] == second_result["batch_id"]
    assert first_result["replayed"] is False
    assert second_result["replayed"] is True
    assert ImportBatch.objects.count() == PriceBar.objects.count() == 1
    assert Security.objects.get().data_version == 1


def test_racing_provider_revisions_preserve_highest_revision(tmp_path):
    ingest_file(csv_file(tmp_path, [row()], "initial.csv"))
    revision_two = csv_file(tmp_path, [row(price="120", revision=2)], "r2.csv")
    revision_three = csv_file(tmp_path, [row(price="130", revision=3)], "r3.csv")

    with ThreadPoolExecutor(max_workers=2) as pool:
        with transaction.atomic():
            Security.objects.select_for_update().get(symbol="ALFA")
            second, second_pid = start_work(pool, lambda: ingest_file(revision_two, DAY))
            third, third_pid = start_work(pool, lambda: ingest_file(revision_three, DAY))
            assert_waiting_on_lock(second_pid)
            assert_waiting_on_lock(third_pid)
            assert not second.done() and not third.done()
        results = [second.result(timeout=10), third.result(timeout=10)]

    price = PriceBar.objects.get()
    assert price.provider_revision == 3
    assert str(price.adjusted_close) == "130.000000"
    assert Security.objects.get().data_version in (2, 3)
    assert sum(result["updated"] for result in results) in (1, 2)
    assert ImportBatch.objects.filter(state=ImportBatch.State.ACCEPTED).count() == 3


def test_correction_waits_for_calculation_then_marks_its_output_stale(tmp_path, monkeypatch):
    ingest_file(csv_file(tmp_path, [row()], "initial.csv"))
    correction = csv_file(tmp_path, [row(price="105", revision=2)], "correction.csv")
    entered, release = Event(), Event()
    calculate = services.calculate_metrics

    def hold_calculation(frame):
        entered.set()
        assert release.wait(10), "Timed out releasing the calculation transaction"
        return calculate(frame)

    monkeypatch.setattr(services, "calculate_metrics", hold_calculation)
    with ThreadPoolExecutor(max_workers=2) as pool:
        try:
            calculation, calculation_pid = start_work(pool, lambda: calculate_batch(["ALFA"], DAY))
            assert entered.wait(10)
            update, update_pid = start_work(pool, lambda: ingest_file(correction, DAY))
            assert_waiting_on_lock(update_pid, calculation_pid)
            assert not update.done()
        finally:
            release.set()
        assert calculation.result(timeout=10)["snapshots"] == 1
        assert update.result(timeout=10)["updated"] == 1

    assert Security.objects.get().data_version == 2
    assert MetricSnapshot.objects.get().source_version == 1


def test_quality_gate_waits_for_ingest_commit_and_rejects_old_metrics(tmp_path, monkeypatch):
    original = ingest_file(csv_file(tmp_path, [row()], "initial.csv"))
    calculate_batch(["ALFA"], DAY)
    correction = csv_file(tmp_path, [row(price="105", revision=2)], "correction.csv")
    checksum = hashlib.sha256(correction.read_bytes()).hexdigest()
    entered, release = Event(), Event()
    summarize = services._summary

    def hold_correction_before_commit(batch, replayed=False):
        result = summarize(batch, replayed)
        if batch.sha256 == checksum and not replayed:
            entered.set()
            assert release.wait(10), "Timed out releasing the correction transaction"
        return result

    monkeypatch.setattr(services, "_summary", hold_correction_before_commit)
    with ThreadPoolExecutor(max_workers=2) as pool:
        try:
            update, update_pid = start_work(pool, lambda: ingest_file(correction, DAY))
            assert entered.wait(10)
            gate, gate_pid = start_work(pool, lambda: verify_pipeline(original["batch_id"], DAY))
            assert_waiting_on_lock(gate_pid, update_pid)
            assert not gate.done()
        finally:
            release.set()
        assert update.result(timeout=10)["updated"] == 1
        with pytest.raises(PipelineIncomplete, match="stale"):
            gate.result(timeout=10)

    assert Security.objects.get().data_version == 2
    assert MetricSnapshot.objects.get().source_version == 1
