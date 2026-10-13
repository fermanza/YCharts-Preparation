import json

from django.core.management.base import BaseCommand, CommandError

from marketdata.services import calculate_batch, parse_date


class Command(BaseCommand):
    help = "Repair metrics directly or submit a bounded job to the explicit Celery app."

    def add_arguments(self, parser):
        parser.add_argument("--symbols", nargs="+", required=True)
        parser.add_argument("--as-of", required=True)
        parser.add_argument("--async", action="store_true", dest="async_task")

    def handle(self, *args, **options):
        try:
            cutoff = parse_date(options["as_of"]).isoformat()
            symbols = options["symbols"]
            if not 1 <= len(symbols) <= 100:
                raise ValueError("provide 1–100 symbols")
        except ValueError as error:
            raise CommandError(str(error)) from error
        if options["async_task"]:
            # Airflow and ordinary Django setup never import the Celery worker.
            from marketdata.tasks import recalculate_metrics

            job = recalculate_metrics.delay(symbols, cutoff)
            self.stdout.write(f"queued_task_id={job.id}")
        else:
            self.stdout.write(json.dumps(calculate_batch(symbols, cutoff)))
