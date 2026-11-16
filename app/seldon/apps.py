"""Django application config for the SELDON service.

Startup loads no model and imports no model library: every process that
sets up Django runs this, and model packages run only in worker images,
which pin their own thread pools.
"""

import logging

from django.apps import AppConfig
from django.conf import settings

logger = logging.getLogger(__name__)


class SeldonConfig(AppConfig):
    """Application config for the ``seldon`` app."""

    name = "seldon"
    verbose_name = "SELDON forecasting"

    def ready(self) -> None:
        """Log the release this process runs."""
        logger.info("SELDON starting with APP_VERSION=%s", settings.APP_VERSION)
