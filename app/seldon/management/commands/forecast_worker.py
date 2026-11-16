"""Run the forecast worker for the one model package installed in this image.

Each worker image holds the service and exactly one model package, which may
pin its own torch, so every deployed release runs in processes of its own
and never inside intake. The worker loads the package named by
``SELDON_WORKER_PACKAGE``, registers its release under the wheel digest
recorded in the image (``SELDON_WORKER_DIGEST`` or ``--digest``), and then
turns owed detections into stored outcomes until stopped. A release the
service refuses (a digest that differs from the one registered for the same
name and version, an unsupported contract version, or a retired release)
stops the worker before it claims anything.

Options:

- ``--until-idle``: exit once nothing is due or leased for the deployment,
  for replays and tests.
- ``--digest``: the wheel's sha256, overriding ``SELDON_WORKER_DIGEST``.
"""

import logging
from typing import Any

from django.core.management.base import BaseCommand, CommandError, CommandParser
from pydantic import ValidationError
from seldon_model_contract import PackageNotFoundError, load_package

from seldon.config.worker import get_worker_settings
from seldon.deployments import DeploymentRefused
from seldon.forecasting.worker import start

logger = logging.getLogger("seldon.forecasting")


class Command(BaseCommand):
    """Run the forecast worker loop."""

    help = "Turn owed detections into stored outcomes for this image's package."

    def add_arguments(self, parser: CommandParser) -> None:
        """Add the command's options.

        Args:
            parser: The command's argument parser.
        """
        parser.add_argument(
            "--until-idle",
            action="store_true",
            help="Exit once nothing is due or leased for the deployment.",
        )
        parser.add_argument(
            "--digest",
            help="The package wheel's sha256; overrides SELDON_WORKER_DIGEST.",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        """Load the package, register it, and run the loop.

        Args:
            *args: Unused.
            **options: The parsed options.

        Raises:
            CommandError: When the settings are invalid, no digest is given,
                the package cannot be loaded, or its release is refused.
        """
        try:
            settings = get_worker_settings()
        except ValidationError as error:
            raise CommandError(f"Invalid SELDON_WORKER_ settings: {error}") from error
        digest = options["digest"] or settings.digest
        if not digest:
            raise CommandError(
                "No artifact digest: set SELDON_WORKER_DIGEST or pass --digest."
            )
        try:
            package = load_package(settings.package)
        except (PackageNotFoundError, TypeError) as error:
            raise CommandError(str(error)) from error
        try:
            worker = start(package, settings, digest)
        except DeploymentRefused as error:
            raise CommandError(f"Refusing to start: {error}") from error
        totals = worker.run(until_idle=options["until_idle"])
        logger.info(
            "Idle: %d products, %d refusals, %d failures stored for %s",
            totals.products,
            totals.refusals,
            totals.failures,
            worker.deployment,
        )
