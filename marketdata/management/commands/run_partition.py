import json
import os
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from marketdata.services import (
    BatchRejected,
    calculate_batch,
    ingest_file,
    parse_date,
    verify_pipeline,
)


class Command(BaseCommand):
    help = "Execute the same services as Airflow for a single date, without a scheduler."

    def add_arguments(self, parser):
        parser.add_argument("--date", required=True)
        parser.add_argument(
            "--data-dir", default=os.getenv("DATA_DIR", str(settings.BASE_DIR / "data/provider"))
        )

    def handle(self, *args, **options):
        try:
            cutoff = parse_date(options["date"])
            result = ingest_file(Path(options["data_dir"]) / f"{cutoff}.csv", expected_date=cutoff)
            for offset in range(0, len(result["symbols"]), 100):
                calculate_batch(result["symbols"][offset : offset + 100], cutoff)
            self.stdout.write(json.dumps(verify_pipeline(result["batch_id"], cutoff)))
        except (BatchRejected, ValueError, OSError) as error:
            raise CommandError(str(error)) from error
