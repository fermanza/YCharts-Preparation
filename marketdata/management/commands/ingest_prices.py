import json

from django.core.management.base import BaseCommand, CommandError

from marketdata.services import BatchRejected, ingest_file


class Command(BaseCommand):
    help = "Validate and atomically import one daily provider CSV."

    def add_arguments(self, parser):
        parser.add_argument("path")
        parser.add_argument("--date", default=None)

    def handle(self, *args, **options):
        try:
            result = ingest_file(options["path"], expected_date=options["date"])
        except (BatchRejected, ValueError, OSError) as error:
            raise CommandError(str(error)) from error
        self.stdout.write(json.dumps(result))
