"""Tests for the ``model_heads`` operator command.

These need the compose ``db`` service; run them with
``run/seldonctl slim_dev test``.
"""

import json
from datetime import UTC, datetime, timedelta
from io import StringIO

from django.core.management import CommandError, call_command
from django.test import TestCase
from django.utils import timezone
from seldon_model_contract import (
    CONTRACT_VERSION,
    Declaration,
    HeadDeclaration,
    InputKind,
    OutputKind,
    TimeGrid,
)

from seldon import deployments
from seldon.models import HeadRevision, ModelDeployment

PACKAGE = "seldon-next"
DIGEST = "sha256:" + "a" * 64


def declaration(version: str = "2.1.0") -> Declaration:
    """Build a declaration with a flux head and a newly declared redshift head.

    Args:
        version: The package version.

    Returns:
        The declaration.
    """
    return Declaration(
        package_name=PACKAGE,
        package_version=version,
        contract_version=CONTRACT_VERSION,
        accepted=frozenset({("lsst", "r"), ("lsst", "g")}),
        heads=(
            HeadDeclaration(
                "flux_forecast",
                OutputKind.FLUX_SERIES,
                {
                    InputKind.TIMES: TimeGrid(0.0, 60.0, 1.0),
                    InputKind.BANDS: (("lsst", "r"), ("lsst", "g")),
                },
            ),
            HeadDeclaration("redshift", OutputKind.SCALAR),
        ),
    )


def run(*args: str) -> str:
    """Run the command and return what it printed.

    Args:
        *args: The command's arguments.

    Returns:
        Its standard output.
    """
    out = StringIO()
    call_command("model_heads", *args, stdout=out)
    return out.getvalue()


class EnableTests(TestCase):
    """Enabling a head."""

    def setUp(self) -> None:
        self.deployment = deployments.register(declaration(), DIGEST)

    def test_enabling_a_newly_declared_head_creates_a_revision(self) -> None:
        before = timezone.now()
        run("enable", PACKAGE, "redshift")
        revision = HeadRevision.objects.get(deployment=self.deployment)
        self.assertEqual(revision.head, "redshift")
        self.assertTrue(revision.enabled)
        self.assertEqual(revision.inputs, {})
        self.assertGreaterEqual(revision.effective_at, before)
        owed_before = before - timedelta(minutes=1)
        self.assertIsNone(
            deployments.revision_in_effect(self.deployment, "redshift", owed_before)
        )
        owed_after = timezone.now() + timedelta(minutes=1)
        self.assertEqual(
            deployments.revision_in_effect(self.deployment, "redshift", owed_after),
            revision,
        )

    def test_undeclared_head_is_refused(self) -> None:
        with self.assertRaisesMessage(CommandError, "photo_z"):
            run("enable", PACKAGE, "photo_z")
        self.assertFalse(HeadRevision.objects.exists())

    def test_from_backdates_the_revision(self) -> None:
        run("enable", PACKAGE, "redshift", "--from", "2026-10-01T00:00:00Z")
        revision = HeadRevision.objects.get()
        self.assertEqual(revision.effective_at, datetime(2026, 10, 1, tzinfo=UTC))

    def test_from_without_a_timezone_is_refused(self) -> None:
        with self.assertRaisesMessage(CommandError, "timezone"):
            run("enable", PACKAGE, "redshift", "--from", "2026-10-01T00:00:00")
        self.assertFalse(HeadRevision.objects.exists())

    def test_inputs_are_validated_and_stored(self) -> None:
        inputs = {
            "times": {"start_days": 0, "days_past_last": 30, "step_days": 0.5},
            "bands": [["lsst", "r"]],
        }
        run("enable", PACKAGE, "flux_forecast", "--inputs", json.dumps(inputs))
        self.assertEqual(HeadRevision.objects.get().inputs, inputs)

    def test_bad_inputs_are_refused(self) -> None:
        cases = {
            "not json": "{",
            "not an object": "[]",
            "kind the head does not take": json.dumps(
                {"wavelengths": [1, 2], "bands": [["lsst", "r"]]}
            ),
            "missing a declared kind": json.dumps({"bands": [["lsst", "r"]]}),
            "malformed grid": json.dumps(
                {
                    "times": {"start_days": 0, "days_past_last": 30, "step_days": 0},
                    "bands": [["lsst", "r"]],
                }
            ),
            "malformed bands": json.dumps(
                {
                    "times": {"start_days": 0, "days_past_last": 30, "step_days": 1},
                    "bands": ["r"],
                }
            ),
        }
        for label, text in cases.items():
            with self.subTest(label), self.assertRaises(CommandError):
                run("enable", PACKAGE, "flux_forecast", "--inputs", text)
        with self.assertRaises(CommandError):
            run("enable", PACKAGE, "redshift", "--inputs", json.dumps({"times": {}}))
        self.assertFalse(HeadRevision.objects.exists())

    def test_disable_then_enable_leaves_a_gap(self) -> None:
        run("enable", PACKAGE, "redshift", "--from", "2026-10-01T00:00:00Z")
        run("disable", PACKAGE, "redshift")
        disabled = HeadRevision.objects.get(enabled=False)
        run("enable", PACKAGE, "redshift")
        self.assertEqual(HeadRevision.objects.count(), 3)
        in_gap = deployments.revision_in_effect(
            self.deployment, "redshift", disabled.effective_at
        )
        self.assertEqual(in_gap, disabled)

    def test_from_before_the_latest_revision_is_refused(self) -> None:
        """Re-enabling after a disable with a time before the disable could
        never apply, so the command refuses it and writes nothing."""
        run("enable", PACKAGE, "redshift", "--from", "2026-10-01T00:00:00Z")
        run("disable", PACKAGE, "redshift")
        with self.assertRaisesMessage(CommandError, "would never take effect"):
            run("enable", PACKAGE, "redshift", "--from", "2026-10-02T00:00:00Z")
        self.assertEqual(HeadRevision.objects.count(), 2)

    def test_disable_of_undeclared_head_is_refused(self) -> None:
        with self.assertRaisesMessage(CommandError, "photo_z"):
            run("disable", PACKAGE, "photo_z")


class TargetTests(TestCase):
    """Choosing the deployment a command acts on."""

    def test_unknown_package_is_an_error(self) -> None:
        with self.assertRaisesMessage(CommandError, "no-such-package"):
            run("enable", "no-such-package", "redshift")

    def test_several_live_versions_need_a_version(self) -> None:
        deployments.register(declaration("2.1.0"), DIGEST)
        newer = deployments.register(declaration("2.2.0"), "sha256:" + "b" * 64)
        with self.assertRaisesMessage(CommandError, "--version"):
            run("enable", PACKAGE, "redshift")
        run("enable", PACKAGE, "redshift", "--version", "2.2.0")
        self.assertEqual(HeadRevision.objects.get().deployment, newer)

    def test_retired_versions_are_not_the_default(self) -> None:
        old = deployments.register(declaration("2.1.0"), DIGEST)
        newer = deployments.register(declaration("2.2.0"), "sha256:" + "b" * 64)
        run("retire", PACKAGE, "--version", "2.1.0")
        run("enable", PACKAGE, "redshift")
        self.assertEqual(HeadRevision.objects.get().deployment, newer)
        old.refresh_from_db()
        self.assertIsNotNone(old.retired_at)


class RetireTests(TestCase):
    """Retiring a deployment."""

    def test_retire_sets_retired_at(self) -> None:
        deployments.register(declaration(), DIGEST)
        before = timezone.now()
        run("retire", PACKAGE)
        deployment = ModelDeployment.objects.get()
        self.assertGreaterEqual(deployment.retired_at, before)

    def test_retire_with_nothing_live_is_an_error(self) -> None:
        deployments.register(declaration(), DIGEST)
        run("retire", PACKAGE)
        with self.assertRaises(CommandError):
            run("retire", PACKAGE)


class ListTests(TestCase):
    """Listing deployments and their heads."""

    def test_lists_deployments_heads_and_current_revisions(self) -> None:
        deployments.register(declaration(), DIGEST)
        run("enable", PACKAGE, "redshift")
        out = run("list")
        self.assertIn(f"{PACKAGE} 2.1.0", out)
        self.assertIn(DIGEST, out)
        lines = {line.split()[0]: line for line in out.splitlines() if line[:2] == "  "}
        self.assertIn("enabled", lines["redshift"])
        self.assertIn("not enabled", lines["flux_forecast"])

    def test_lists_pre_contract_deployments(self) -> None:
        deployment = ModelDeployment.objects.create(
            package_name="seldon-pre-contract",
            package_version="old.ckpt",
            contract_version="0.0",
            artifact_digest="pre-contract",
            declaration={},
            retired_at=timezone.now(),
        )
        HeadRevision.objects.create(
            deployment=deployment,
            head="flux_forecast",
            enabled=True,
            effective_at=datetime(1970, 1, 1, tzinfo=UTC),
        )
        out = run("list")
        self.assertIn("seldon-pre-contract old.ckpt", out)
        self.assertIn("retired", out)
        self.assertIn("flux_forecast", out)
