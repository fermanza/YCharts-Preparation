"""Daily provider imports, bounded calculation shards, and a final quality gate.

Importing this module does not initialize Django, query a database, or load Celery.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

import pendulum
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.sdk import DAG, Param, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CHUNK_SIZE = 100


def setup_django() -> None:
    """Initialize the application's ORM only in executing tasks."""
    import sys

    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django

    django.setup()


def resolve_date(run_type: str, override: str | None, interval_start: datetime | None) -> str:
    """Scheduled intervals use their start date; manual runs require a date."""
    if run_type == "manual":
        if not override:
            raise AirflowFailException("Manual runs require params.as_of, for example 2026-10-09.")
        try:
            as_of = date.fromisoformat(override)
        except (ValueError, TypeError) as exc:
            raise AirflowFailException("params.as_of must be an ISO date: YYYY-MM-DD.") from exc
        if as_of.isoformat() != override:
            raise AirflowFailException("params.as_of must be an ISO date: YYYY-MM-DD.")
    else:
        if interval_start is None:
            raise AirflowFailException("A scheduled run requires data_interval_start.")
        as_of = interval_start.date()
    if as_of.weekday() >= 5:
        raise AirflowSkipException(
            f"{as_of} is a weekend; this demo provider publishes weekdays only."
        )
    return as_of.isoformat()


def write_json_atomic(target: Path, payload: dict) -> None:
    """Keep readers from seeing a partially written manifest after a retry."""
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", dir=target.parent, suffix=".tmp", delete=False
        ) as handle:
            temporary = Path(handle.name)
            json.dump(payload, handle, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def write_manifests(batch_id: str, symbols: list[str], as_of: str, work_dir: Path) -> list[str]:
    """Atomically publish 100-symbol shards; return paths rather than datasets."""
    if not symbols:
        raise AirflowFailException("Provider batch contains no securities to calculate.")
    folder = work_dir / "manifests" / hashlib.sha256(batch_id.encode()).hexdigest()
    paths = []
    for index, start in enumerate(range(0, len(symbols), CHUNK_SIZE)):
        target = folder / f"chunk-{index:05d}.json"
        payload = {
            "batch_id": batch_id,
            "as_of": as_of,
            "symbols": symbols[start : start + CHUNK_SIZE],
        }
        write_json_atomic(target, payload)
        paths.append(str(target))
    return paths


with DAG(
    dag_id="financial_daily",
    description="Import synthetic prices, calculate pandas metrics, and enforce freshness.",
    schedule=CronDataIntervalTimetable("0 23 * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    is_paused_upon_creation=True,
    default_args={
        "owner": "financial-data",
        "retries": 2,
        "retry_delay": timedelta(seconds=15),
        "retry_exponential_backoff": True,
        "execution_timeout": timedelta(minutes=10),
    },
    params={
        "as_of": Param(
            None,
            type=["null", "string"],
            format="date",
            description="Required for manual runs: YYYY-MM-DD; try 2026-10-09.",
        )
    },
    tags=["interview", "financial-data", "synthetic"],
    doc_md="""
    This demo imports a local provider file and maps calculations across manifest shards.
    Scheduled runs use the start of the completed daily interval. Manual runs require
    `as_of`, such as `2026-10-09`. Unpause before running; pause after the demo.
    The final task succeeds only when every calculation succeeds and the database
    contains complete, current metrics for the imported securities.
    """,
) as financial_daily:

    @task
    def resolve_as_of() -> str:
        context = get_current_context()
        run_type = context["dag_run"].run_type
        return resolve_date(
            getattr(run_type, "value", run_type),
            context["params"].get("as_of"),
            context.get("data_interval_start"),
        )

    @task(multiple_outputs=False)
    def ingest_provider(as_of: str) -> str:
        setup_django()
        from marketdata.services import BatchRejected, ingest_file

        provider_dir = Path(os.environ.get("DATA_DIR", str(PROJECT_ROOT / "data" / "provider")))
        try:
            result = ingest_file(
                provider_dir / f"{as_of}.csv", expected_date=date.fromisoformat(as_of)
            )
        except BatchRejected as exc:
            raise AirflowFailException(f"Provider validation failed: {exc}") from exc
        # Put import metadata in durable storage; return only its path in XCom.
        work_dir = Path(os.environ.get("DATA_WORK_DIR", str(PROJECT_ROOT / "data" / "work")))
        batch_id = str(result["batch_id"])
        index_path = work_dir / "imports" / f"{hashlib.sha256(batch_id.encode()).hexdigest()}.json"
        write_json_atomic(
            index_path, {"batch_id": batch_id, "as_of": as_of, "symbols": result["symbols"]}
        )
        return str(index_path)

    @task
    def write_chunk_manifest(index_path: str) -> list[str]:
        with Path(index_path).open() as handle:
            manifest = json.load(handle)
        work_dir = Path(os.environ.get("DATA_WORK_DIR", str(PROJECT_ROOT / "data" / "work")))
        return write_manifests(
            manifest["batch_id"], manifest["symbols"], manifest["as_of"], work_dir
        )

    @task(multiple_outputs=False)
    def calculate_metrics(manifest_path: str) -> dict:
        setup_django()
        from marketdata.services import PipelineIncomplete, calculate_batch

        with Path(manifest_path).open() as handle:
            shard = json.load(handle)
        try:
            return calculate_batch(shard["symbols"], date.fromisoformat(shard["as_of"]))
        except (ValueError, PipelineIncomplete) as exc:
            raise AirflowFailException(f"Calculation validation failed: {exc}") from exc

    @task(trigger_rule="all_success", multiple_outputs=False)
    def verify_quality(index_path: str) -> dict:
        setup_django()
        from marketdata.services import PipelineIncomplete, verify_pipeline

        with Path(index_path).open() as handle:
            manifest = json.load(handle)
        try:
            return verify_pipeline(manifest["batch_id"], date.fromisoformat(manifest["as_of"]))
        except PipelineIncomplete as exc:
            raise AirflowFailException(f"Pipeline quality validation failed: {exc}") from exc

    as_of = resolve_as_of()
    provider = ingest_provider(as_of)
    shards = write_chunk_manifest(provider)
    calculations = calculate_metrics.expand(manifest_path=shards)
    quality = verify_quality(provider)
    calculations >> quality
