from django.db import connection, transaction
from django.http import JsonResponse
from django.views.decorators.http import require_GET

from .models import ImportBatch, MetricSnapshot, PriceBar, Security


@require_GET
def health(request):
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1")
    return JsonResponse({"status": "ok"})


def _limit(request, default=30):
    value = int(request.GET.get("limit", default))
    if not 1 <= value <= 100:
        raise ValueError("limit must be between 1 and 100")
    return value


@require_GET
def securities(request):
    try:
        limit = _limit(request)
    except ValueError as error:
        return JsonResponse({"error": str(error)}, status=400)
    rows = list(
        Security.objects.order_by("symbol").values("symbol", "currency", "data_version")[:limit]
    )
    return JsonResponse({"securities": rows})


@require_GET
def metrics(request, symbol):
    try:
        limit = _limit(request)
        # Avoid importing calculation libraries into a read-only endpoint.
        from datetime import date

        cutoff = date.fromisoformat(request.GET["as_of"]) if "as_of" in request.GET else None
    except ValueError as error:
        return JsonResponse({"error": str(error)}, status=400)
    try:
        security = Security.objects.get(symbol=symbol.upper())
    except Security.DoesNotExist:
        return JsonResponse({"error": "unknown_security"}, status=404)
    with transaction.atomic():
        security = Security.objects.select_for_update().get(pk=security.pk)
        query = MetricSnapshot.objects.filter(security=security)
        price_query = PriceBar.objects.filter(security=security)
        if cutoff:
            query = query.filter(date__lte=cutoff)
            price_query = price_query.filter(date__lte=cutoff)
        rows = list(
            query.order_by("-date").values(
                "date",
                "source_version",
                "calculation_version",
                "return_1d",
                "sma_20",
                "volatility_20",
                "drawdown",
            )[:limit]
        )
        if not rows:
            return JsonResponse(
                {"error": "metrics_not_available", "symbol": security.symbol}, status=404
            )
        expected_dates = list(price_query.order_by("-date").values_list("date", flat=True)[:limit])
        if [row["date"] for row in rows] != expected_dates or any(
            row["source_version"] != security.data_version or row["calculation_version"] != "v1"
            for row in rows
        ):
            return JsonResponse(
                {
                    "error": "data_recalculation_required",
                    "symbol": security.symbol,
                    "data_version": security.data_version,
                },
                status=409,
            )
    return JsonResponse(
        {
            "symbol": security.symbol,
            "currency": security.currency,
            "data_version": security.data_version,
            "metrics": rows,
        }
    )


@require_GET
def batches(request):
    try:
        limit = _limit(request)
    except ValueError as error:
        return JsonResponse({"error": str(error)}, status=400)
    rows = list(
        ImportBatch.objects.order_by("-created_at").values(
            "id", "date", "state", "filename", "inserted", "updated", "ignored", "errors"
        )[:limit]
    )
    return JsonResponse({"batches": rows})
