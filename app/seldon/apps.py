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
        """Pin torch's thread pool to ``SELDON_TORCH_THREADS`` and log startup.

        Left at its default, torch sizes its pool to the host's cores, not the
        container's CPU limit, and a CPU quota then makes forecasts 15-40x
        slower. ``OMP_NUM_THREADS`` in the container environment covers the
        time before Django starts; this applies the validated setting.

        Raises:
            pydantic.ValidationError: If a ``SELDON_*`` variable holds a value
                its field rejects.
        """
        import torch

        from seldon.config.settings import get_settings

        threads = get_settings().torch_threads
        torch.set_num_threads(threads)
        logger.info(
            "SELDON starting with APP_VERSION=%s, torch %s, %d thread(s)",
            settings.APP_VERSION,
            torch.__version__,
            threads,
        )
