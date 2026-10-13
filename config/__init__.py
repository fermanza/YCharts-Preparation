"""Django configuration; Celery is initialized explicitly via config.celery:app.

Keep this package free of eager worker imports: Airflow's macOS forked task
processes must not initialize Celery's native process-title library here.
"""
