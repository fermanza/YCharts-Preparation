"""Run with the isolated Airflow environment; normal app tests skip this module."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("airflow")

import pendulum
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.timetables.base import TimeRestriction
from airflow.timetables.interval import CronDataIntervalTimetable

from dags.financial_daily import (
    financial_daily,
    resolve_date,
    write_json_atomic,
    write_manifests,
)


def test_dag_graph_and_quality_gate():
    assert set(financial_daily.task_ids) == {
        "resolve_as_of",
        "ingest_provider",
        "write_chunk_manifest",
        "calculate_metrics",
        "verify_quality",
    }
    assert financial_daily.max_active_runs == 1
    assert financial_daily.max_active_tasks == 4
    assert financial_daily.catchup is False
    assert financial_daily.is_paused_upon_creation is True
    assert financial_daily.get_task("calculate_metrics").upstream_task_ids == {
        "write_chunk_manifest"
    }
    assert financial_daily.get_task("verify_quality").upstream_task_ids == {
        "ingest_provider",
        "calculate_metrics",
    }
    assert financial_daily.get_task("verify_quality").trigger_rule.value == "all_success"
    assert financial_daily.get_task("ingest_provider").retries == 2
    assert financial_daily.get_task("calculate_metrics").retry_exponential_backoff is True


def test_explicit_daily_data_interval_has_prior_day_start():
    timetable = financial_daily.timetable
    assert isinstance(timetable, CronDataIntervalTimetable)
    info = timetable.next_dagrun_info(
        last_automated_data_interval=None,
        restriction=TimeRestriction(
            earliest=pendulum.datetime(2026, 10, 9, 23, tz="UTC"),
            latest=None,
            catchup=True,
        ),
    )
    assert info.data_interval.start == pendulum.datetime(2026, 10, 9, 23, tz="UTC")
    assert info.data_interval.end == pendulum.datetime(2026, 10, 10, 23, tz="UTC")
    assert resolve_date("scheduled", "2026-10-08", info.data_interval.start) == "2026-10-09"


@pytest.mark.parametrize(
    "override", [None, "", "10/09/2026", "2026-02-30", "20261009", "2026-10-09T00:00:00"]
)
def test_manual_requires_strict_valid_iso_date(override):
    with pytest.raises(AirflowFailException):
        resolve_date("manual", override, pendulum.datetime(2026, 10, 8, tz="UTC"))


def test_manual_override_and_weekend_skip():
    assert resolve_date("manual", "2026-10-09", None) == "2026-10-09"
    with pytest.raises(AirflowSkipException):
        resolve_date("manual", "2026-10-10", None)
    with pytest.raises(AirflowSkipException):
        resolve_date("scheduled", None, pendulum.datetime(2026, 10, 11, 23, tz="UTC"))
    with pytest.raises(AirflowFailException):
        resolve_date("scheduled", None, None)


def test_chunks_are_bounded_atomic_references_and_replay_stable(tmp_path):
    symbols = [f"SEC{index:04}" for index in range(205)]
    references = write_manifests("../../batch", symbols, "2026-10-09", tmp_path)
    assert len(references) == 3
    chunks = [json.loads(Path(reference).read_text()) for reference in references]
    assert [len(chunk["symbols"]) for chunk in chunks] == [100, 100, 5]
    assert [symbol for chunk in chunks for symbol in chunk["symbols"]] == symbols
    assert all(chunk["as_of"] == "2026-10-09" for chunk in chunks)
    assert all(Path(reference).is_relative_to(tmp_path) for reference in references)
    assert write_manifests("../../batch", symbols, "2026-10-09", tmp_path) == references
    assert not list(tmp_path.rglob("*.tmp"))
    with pytest.raises(AirflowFailException):
        write_manifests("empty", [], "2026-10-09", tmp_path)


def test_failed_atomic_replace_preserves_existing_manifest(tmp_path, monkeypatch):
    path = tmp_path / "manifest.json"
    write_json_atomic(path, {"revision": 1})

    def fail_replace(*args):
        raise OSError("simulated storage outage")

    monkeypatch.setattr("dags.financial_daily.os.replace", fail_replace)
    with pytest.raises(OSError, match="storage outage"):
        write_json_atomic(path, {"revision": 2})
    assert json.loads(path.read_text()) == {"revision": 1}
    assert not list(tmp_path.glob("*.tmp"))


def test_parse_does_not_initialize_application_or_celery():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import dags.financial_daily; "
            "assert 'config.settings' not in sys.modules; "
            "assert 'config.celery' not in sys.modules; "
            "assert 'marketdata.services' not in sys.modules",
        ],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr


def test_provider_validation_fails_without_retry(monkeypatch):
    from marketdata.services import BatchRejected

    def reject(*args, **kwargs):
        raise BatchRejected("negative close")

    monkeypatch.setattr("dags.financial_daily.setup_django", lambda: None)
    monkeypatch.setattr("marketdata.services.ingest_file", reject)
    with pytest.raises(AirflowFailException, match="Provider validation failed"):
        financial_daily.get_task("ingest_provider").python_callable("2026-10-09")


def test_quality_gate_incomplete_output_fails_without_retry(tmp_path, monkeypatch):
    from marketdata.services import PipelineIncomplete

    def incomplete(*args, **kwargs):
        raise PipelineIncomplete("stale metrics")

    path = tmp_path / "import.json"
    write_json_atomic(path, {"batch_id": "demo", "as_of": "2026-10-09"})
    monkeypatch.setattr("dags.financial_daily.setup_django", lambda: None)
    monkeypatch.setattr("marketdata.services.verify_pipeline", incomplete)
    with pytest.raises(AirflowFailException, match="Pipeline quality validation failed"):
        financial_daily.get_task("verify_quality").python_callable(str(path))
