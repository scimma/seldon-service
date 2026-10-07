"""Django application config for the SELDON adapter."""

import logging

from django.apps import AppConfig
from django.conf import settings

logger = logging.getLogger(__name__)


class SeldonConfig(AppConfig):
    """Application config for the ``seldon`` app."""

    name = "seldon"
    verbose_name = "SELDON forecasting"

    def ready(self) -> None:
        """Pin torch's thread pool, log startup, and optionally load the model.

        Left at its default, torch sizes its pool to the host's cores, not the
        container's CPU limit, and a CPU quota then makes forecasts 15-40x
        slower. ``OMP_NUM_THREADS`` in the container environment covers the
        time before Django starts; this applies the validated setting.

        With ``SELDON_WARMUP`` on (the server entrypoint sets it) the model
        loads here, so a server process is ready before its first request.
        It stays off elsewhere because this runs in every process that sets
        up Django, including the test runner and every management command.

        Raises:
            pydantic.ValidationError: If a ``SELDON_*`` variable holds a value
                its field rejects.
            seldon.infrastructure.ml.loader.IncompatibleCheckpointError: If
                warmup is on and the configured checkpoint cannot be served.
        """
        import torch

        from seldon.config.settings import get_settings

        model_settings = get_settings()
        threads = model_settings.torch_threads
        torch.set_num_threads(threads)
        logger.info(
            "SELDON starting with APP_VERSION=%s, torch %s, %d thread(s)",
            settings.APP_VERSION,
            torch.__version__,
            threads,
        )
        if model_settings.warmup:
            from seldon import services

            services.loaded_model()
