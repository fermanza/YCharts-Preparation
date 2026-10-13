# Verification record

Recreated and verified on **October 12, 2026 (America/Mexico_City)** using Python 3.12.12 on macOS. All financial inputs are fictional fixtures.

| Check | Result |
| --- | --- |
| Core PostgreSQL suite | 96 passed, including four real contention tests; Airflow module skipped in the core environment |
| Airflow 3.1.7 SDK suite | 14 passed in its officially constrained environment |
| Real Airflow scheduler run | `interview_auth_smoke_20261012` succeeded; all five stages, including mapped calculation and final verification, passed |
| Airflow authentication | Generated admin login accepted; anonymous token request rejected |
| Real Celery and Redis | Queued task restored all 29 deleted ALFA snapshots |
| Replayed core demo | 87 prices, 87 snapshots and 29 accepted batches preserved |
| Django checks / migration drift | Passed, with no pending model changes |
| Ruff, shell syntax and Compose configuration | Passed |
| CI workflow YAML | Parsed successfully; GitHub execution not performed |

The PostgreSQL and Airflow suites contain **110 distinct passing tests**. SQLite-only testing skips PostgreSQL contention tests and the separate Airflow module.

## Integration details

PostgreSQL verification used a disposable loopback cluster and observed real `pg_stat_activity` lock waits and `pg_blocking_pids`. Tests covered simultaneous exact replay, competing provider revisions, correction waiting for a calculation, and verification waiting for an import commit. The cluster was stopped and removed.

The authenticated Airflow standalone run used the real API server, scheduler, DAG processor, triggerer and LocalExecutor. The manual October 9 partition resolved its date, imported prices, wrote atomic chunk manifests, ran mapped calculations and returned `status=complete` for three securities. The final constrained-environment run took about 18 seconds after startup. This is a small demonstration, not a throughput benchmark. A scheduled weekend run also skipped as intended.

The local launcher now places the Airflow executable on `PATH`, accepts normal CLI subcommands, applies official constraints when installing the application, and retains admin authentication. `config/__init__.py` no longer initializes Celery. That explicit initialization boundary resolved the prior macOS fork failure without disabling Objective-C safety guards.

Celery verification used a real worker with `-A config.celery:app --pool=solo --concurrency=1`, isolated Redis and temporary SQLite. A queued recalculation rebuilt 29 ALFA snapshots at the current source version. Temporary worker, Redis, databases and logs were cleaned up.

## Runtime state and limits

All verification servers are stopped. Ports 8000, 8080 and the temporary Airflow port are closed. The local demo database remains populated so the synchronous demo can be replayed immediately.

Docker configuration was validated; containers were not built or run because Docker Desktop/Engine was stopped. The included GitHub Actions workflow has not run on GitHub. No benchmark at 200,000 securities or 4,000 metrics was performed. Production vendor feeds, exchange calendars, expected-universe manifests and historical knowledge-time queries remain design extensions documented in the architecture.
