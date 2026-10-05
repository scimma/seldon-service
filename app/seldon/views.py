"""HTTP views for the SELDON service."""

from dataclasses import asdict
from importlib import metadata

from django.conf import settings
from django.http import HttpRequest, JsonResponse

from seldon import services


def healthz(request: HttpRequest) -> JsonResponse:
    """Report the running version and whether the model is ready to serve.

    The status code carries readiness so a Kubernetes readiness probe can use
    it directly: a container that never loads its checkpoint stays unready and
    its rollout stalls visibly instead of accepting traffic.

    Args:
        request: The incoming request.

    Returns:
        A JSON response, 200 once the model has loaded and 503 before.
    """
    identity = services.loaded_model_identity()
    ready = identity is not None
    payload = {
        "status": "ready" if ready else "not_ready",
        "ready": ready,
        "app_version": settings.APP_VERSION,
        "torch_version": metadata.version("torch"),
        "model": asdict(identity) if ready else None,
    }
    return JsonResponse(payload, status=200 if ready else 503)
