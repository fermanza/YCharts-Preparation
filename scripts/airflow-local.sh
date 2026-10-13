#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_dir"
export AIRFLOW_HOME="${AIRFLOW_HOME:-$project_dir/.airflow}"

if ! command -v uv >/dev/null 2>&1; then
  echo 'Install uv first: https://docs.astral.sh/uv/getting-started/installation/' >&2
  exit 1
fi

if [[ ! -x "$project_dir/.airflow-venv/bin/python" ]]; then
  uv venv --python 3.12 "$project_dir/.airflow-venv"
fi

if ! "$project_dir/.airflow-venv/bin/python" -c 'from importlib.metadata import version; assert version("apache-airflow") == "3.1.7"' >/dev/null 2>&1; then
  uv pip install --python "$project_dir/.airflow-venv/bin/python" \
    'apache-airflow==3.1.7' \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.1.7/constraints-3.12.txt
fi
uv pip install --python "$project_dir/.airflow-venv/bin/python" -e "$project_dir" \
  'apache-airflow==3.1.7' \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.1.7/constraints-3.12.txt

# Standalone launches child components using the `airflow` executable on PATH.
export PATH="$project_dir/.airflow-venv/bin:$PATH"
export AIRFLOW__CORE__DAGS_FOLDER="$project_dir/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__PARALLELISM=4
export AIRFLOW__CORE__DAG_RUN_CONF_OVERRIDES_PARAMS=True
export AIRFLOW__CORE__MAX_MAP_LENGTH=4096
export AIRFLOW__API__HOST=127.0.0.1
export AIRFLOW__API__PORT="${AIRFLOW_PORT:-8080}"
export AIRFLOW__CORE__EXECUTION_API_SERVER_URL="http://127.0.0.1:${AIRFLOW__API__PORT}/execution/"
export PYTHONPATH="$project_dir${PYTHONPATH:+:$PYTHONPATH}"
export DJANGO_SETTINGS_MODULE=config.settings
export DATA_DIR="${DATA_DIR:-$project_dir/data/provider}"
export DATA_WORK_DIR="${DATA_WORK_DIR:-$project_dir/data/work}"

if [[ $# -gt 0 ]]; then
  exec airflow "$@"
fi

python manage.py migrate
printf 'Airflow UI: http://127.0.0.1:%s\n' "$AIRFLOW__API__PORT"
echo 'Local admin login credentials appear below. Ctrl+C stops standalone components.'
exec airflow standalone
