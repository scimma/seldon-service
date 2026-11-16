"""Show and change which heads of each deployed model release run.

A package declares the heads it offers; nothing reaches live alerts until an
operator enables a head here. Each change is written as a new head revision
and never edits an old one, so the record of what ran when stays intact and
workers only ever read it. A revision covers detections that become owed at
or after it takes effect: enabling a head does not reach back over
detections owed while it was off, unless the operator backdates it with
``--from``. A head's first revision can be backdated freely; a later one
cannot take effect before the head's latest revision, so a gap left by a
disable stays uncovered by this deployment.

Subcommands:

- ``list``: every deployment, its heads, and the revision in effect now.
- ``enable <package> <head> [--version V] [--from ISO-8601] [--inputs JSON]``
- ``disable <package> <head> [--version V]``
- ``retire <package> [--version V]``: stop owing the release any work.

``<package>`` is a package name. Without ``--version`` the command acts on
the one deployment of that name that is not retired.
"""

import json
import logging
from argparse import ArgumentParser
from datetime import datetime
from typing import Any

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from seldon import deployments
from seldon.models import ModelDeployment

logger = logging.getLogger("seldon.deployments")


def find_deployment(package_name: str, version: str | None) -> ModelDeployment:
    """Return the deployment a command acts on.

    Args:
        package_name: The package's name.
        version: The package version, or ``None`` for the single deployment
            of that name that is not retired.

    Returns:
        The deployment.

    Raises:
        CommandError: When no deployment matches, or several do.
    """
    matches = ModelDeployment.objects.filter(package_name=package_name)
    if version is not None:
        deployment = matches.filter(package_version=version).first()
        if deployment is None:
            raise CommandError(f"No deployment {package_name} {version}.")
        return deployment
    live = list(matches.filter(retired_at__isnull=True).order_by("package_version"))
    if not live:
        raise CommandError(f"No deployment of {package_name} that is not retired.")
    if len(live) > 1:
        versions = ", ".join(d.package_version for d in live)
        raise CommandError(
            f"{package_name} has several deployments that are not retired "
            f"({versions}); choose one with --version."
        )
    return live[0]


def parse_from(text: str | None) -> datetime:
    """Parse ``--from``, or return now when it is absent.

    Args:
        text: An ISO-8601 time with a timezone, or ``None``.

    Returns:
        The time the revision takes effect.

    Raises:
        CommandError: When the time cannot be parsed or has no timezone.
    """
    if text is None:
        return timezone.now()
    try:
        moment = parse_datetime(text)
    except ValueError:
        moment = None
    if moment is None:
        raise CommandError(f"--from must be an ISO-8601 time, got {text!r}.")
    if timezone.is_naive(moment):
        raise CommandError(
            f"--from must include a timezone (for example 'Z'), got {text!r}."
        )
    return moment


def parse_inputs(text: str | None) -> Any:
    """Parse ``--inputs`` as JSON.

    Args:
        text: The JSON text, or ``None``.

    Returns:
        The parsed value, or ``None`` when absent.

    Raises:
        CommandError: When the text is not JSON.
    """
    if text is None:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError as error:
        raise CommandError(f"--inputs is not valid JSON: {error}") from error


class Command(BaseCommand):
    """List deployments, enable or disable their heads, or retire them."""

    help = "List model deployments and change which of their heads run."

    def add_arguments(self, parser: ArgumentParser) -> None:
        """Declare the subcommands and their options.

        Args:
            parser: The command's argument parser.
        """
        actions = parser.add_subparsers(dest="action", required=True)
        actions.add_parser("list", help="Show deployments, heads, and revisions.")

        enable = actions.add_parser("enable", help="Enable a head.")
        enable.add_argument("package", help="Package name.")
        enable.add_argument("head", help="Head name, as the package declares it.")
        enable.add_argument("--version", help="Package version.")
        enable.add_argument(
            "--from",
            dest="effective_from",
            help="ISO-8601 time with a timezone from which detections become "
            "owed for the head (default now). An earlier time covers detections "
            "already owed.",
        )
        enable.add_argument(
            "--inputs",
            help="Input templates as a JSON object with every input kind the "
            "head declares, for example "
            '\'{"times": {"start_days": 0, "days_past_last": 60, "step_days": 1}, '
            '"bands": [["lsst", "r"]]}\'. Default: the package\'s own.',
        )

        disable = actions.add_parser("disable", help="Disable a head from now.")
        disable.add_argument("package", help="Package name.")
        disable.add_argument("head", help="Head name, as the package declares it.")
        disable.add_argument("--version", help="Package version.")

        retire = actions.add_parser("retire", help="Retire a deployment.")
        retire.add_argument("package", help="Package name.")
        retire.add_argument("--version", help="Package version.")

    def handle(self, *args: Any, **options: Any) -> None:
        """Run the chosen subcommand.

        Args:
            *args: Unused.
            **options: The parsed options.

        Raises:
            CommandError: When the target or the change is refused.
        """
        action = options["action"]
        if action == "list":
            self._list()
            return
        deployment = find_deployment(options["package"], options["version"])
        if action == "retire":
            self._retire(deployment)
            return
        enabled = action == "enable"
        if enabled:
            effective_at = parse_from(options["effective_from"])
            inputs = parse_inputs(options["inputs"])
        else:
            effective_at = timezone.now()
            inputs = None
        try:
            revision = deployments.set_head(
                deployment, options["head"], enabled, effective_at, inputs
            )
        except ValueError as error:
            raise CommandError(str(error)) from error
        self.stdout.write(
            f"{'Enabled' if enabled else 'Disabled'} {revision.head} for "
            f"{deployment} from {revision.effective_at.isoformat()}."
        )

    def _retire(self, deployment: ModelDeployment) -> None:
        """Mark a deployment retired.

        Args:
            deployment: The deployment.
        """
        if deployment.retired_at is not None:
            self.stdout.write(
                f"{deployment} was already retired at "
                f"{deployment.retired_at.isoformat()}."
            )
            return
        deployment.retired_at = timezone.now()
        deployment.save(update_fields=["retired_at"])
        logger.info("Retired deployment %s", deployment)
        self.stdout.write(f"Retired {deployment}.")

    def _list(self) -> None:
        """Print each deployment and the revision in effect for each head."""
        now = timezone.now()
        rows = ModelDeployment.objects.order_by("package_name", "registered_at")
        for deployment in rows:
            status = (
                f"retired {deployment.retired_at.isoformat()}"
                if deployment.retired_at
                else "live"
            )
            self.stdout.write(
                f"{deployment}  contract {deployment.contract_version}  "
                f"digest {deployment.artifact_digest}  {status}"
            )
            current = deployments.current_revisions(deployment, at=now)
            declared = [
                head.get("name")
                for head in (deployment.declaration or {}).get("heads", [])
                if isinstance(head, dict) and head.get("name")
            ]
            for head in declared + sorted(set(current) - set(declared)):
                revision = current.get(head)
                if revision is None:
                    self.stdout.write(f"  {head}  not enabled")
                    continue
                state = "enabled" if revision.enabled else "disabled"
                line = f"  {head}  {state} since {revision.effective_at.isoformat()}"
                if revision.enabled:
                    line += f"  inputs {json.dumps(revision.inputs)}"
                self.stdout.write(line)
