import json
import os
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from marketdata.models import ImportBatch, MetricSnapshot, PriceBar
from marketdata.services import (
    BatchRejected,
    calculate_batch,
    ingest_file,
    parse_date,
    verify_pipeline,
)


class Command(BaseCommand):
    help = "Load deterministic synthetic history, calculate metrics and verify publication."

    def add_arguments(self, parser):
        parser.add_argument("--as-of", default="2026-10-09")
        parser.add_argument(
            "--data-dir", default=os.getenv("DATA_DIR", str(settings.BASE_DIR / "data/provider"))
        )

    def handle(self, *args, **options):
        try:
            cutoff = parse_date(options["as_of"])
            files = sorted(
                path
                for path in Path(options["data_dir"]).glob("*.csv")
                if parse_date(path.stem) <= cutoff
            )
            if not files or files[-1].stem != cutoff.isoformat():
                raise ValueError("as-of must match a supplied provider partition")
            symbols, last = set(), None
            for path in files:
                last = ingest_file(path, expected_date=parse_date(path.stem))
                symbols.update(last["symbols"])
            symbols = sorted(symbols)
            for offset in range(0, len(symbols), 100):
                calculate_batch(symbols[offset : offset + 100], cutoff)
            summary = verify_pipeline(last["batch_id"], cutoff)
        except (BatchRejected, ValueError, OSError) as error:
            raise CommandError(str(error)) from error
        self.stdout.write(self.style.SUCCESS(json.dumps(summary)))
        self.stdout.write(
            f"prices={PriceBar.objects.count()} metrics={MetricSnapshot.objects.count()} "
            f"accepted_batches={ImportBatch.objects.filter(state='accepted').count()}"
        )
        self.stdout.write("Synthetic data only. Read API: /api/securities/ALFA/metrics/")
