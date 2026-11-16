"""HTTP views for the SELDON service.

The web process holds no model: every model package runs in a worker image
of its own. So readiness here means only that the database can be reached,
and each live deployment's health is read from the heartbeat its workers
write there, which shows a stalled or stopped forecaster without the web
process loading anything.
"""

import logging

from django.conf import settings
from django.db import DatabaseError
from django.http import HttpRequest, JsonResponse
from django.utils import timezone

from seldon.models import ModelDeployment

logger = logging.getLogger("seldon.views")


def healthz(request: HttpRequest) -> JsonResponse:
    """Report the running version, the database, and each live deployment.

    The status code carries readiness so a Kubernetes readiness probe can use
    it directly: 200 while the database answers, 503 when it does not. A
    deployment whose worker has stopped reporting in does not make the web
    process unready; its heartbeat age shows it.

    Args:
        request: The incoming request.

    Returns:
        A JSON response naming the version, whether the database answered,
        and for each deployment that is not retired its package name,
        version, contract version, last heartbeat, and the heartbeat's age
        in seconds (``None`` when no worker has reported in).
    """
    try:
        rows = list(
            ModelDeployment.objects.filter(retired_at__isnull=True)
            .order_by("package_name", "package_version")
            .values(
                "package_name",
                "package_version",
                "contract_version",
                "last_heartbeat_at",
            )
        )
    except DatabaseError:
        logger.exception("/healthz could not read the database")
        return JsonResponse(
            {
                "status": "not_ready",
                "app_version": settings.APP_VERSION,
                "database": False,
                "deployments": [],
            },
            status=503,
        )
    now = timezone.now()
    deployments = []
    for row in rows:
        heartbeat = row["last_heartbeat_at"]
        deployments.append(
            {
                **row,
                "heartbeat_age_seconds": (
                    None if heartbeat is None else (now - heartbeat).total_seconds()
                ),
            }
        )
    return JsonResponse(
        {
            "status": "ready",
            "app_version": settings.APP_VERSION,
            "database": True,
            "deployments": deployments,
        }
    )
