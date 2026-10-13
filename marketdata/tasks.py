"""Explicit repair worker entry point; scheduled Airflow services don't import this."""

from django.db import OperationalError

from config.celery import app

from .services import calculate_batch


@app.task(
    autoretry_for=(OperationalError,),
    retry_backoff=True,
    retry_jitter=True,
    retry_kwargs={"max_retries": 3},
    acks_late=True,
)
def recalculate_metrics(symbols, as_of):
    return calculate_batch(symbols, as_of)
