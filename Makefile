.PHONY: setup demo test lint serve airflow
setup:
	uv sync --locked
	uv run python manage.py migrate
demo:
	uv run python manage.py demo_pipeline
test:
	uv run pytest
lint:
	uv run ruff check .
serve:
	uv run python manage.py runserver 127.0.0.1:8000
airflow:
	bash scripts/airflow-local.sh

