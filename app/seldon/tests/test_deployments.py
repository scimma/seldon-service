"""Tests for the deployment registry and head revision lookups.

These need the compose ``db`` service; run them with
``run/seldonctl slim_dev test``.
"""

from datetime import UTC, datetime, timedelta

from django.test import TestCase
from seldon_model_contract import (
    CONTRACT_VERSION,
    Declaration,
    HeadDeclaration,
    InputKind,
    OutputKind,
    TimeGrid,
)

from seldon import deployments
from seldon.deployments import DeploymentRefused
from seldon.models import HeadRevision, ModelDeployment

DIGEST = "sha256:" + "a" * 64
OTHER_DIGEST = "sha256:" + "b" * 64
T0 = datetime(2026, 11, 1, tzinfo=UTC)


def declaration(
    version: str = "2.1.0", contract_version: str = CONTRACT_VERSION
) -> Declaration:
    """Build a declaration with a flux head and a newly declared redshift head.

    Args:
        version: The package version.
        contract_version: The contract version it implements.

    Returns:
        The declaration.
    """
    return Declaration(
        package_name="seldon-next",
        package_version=version,
        contract_version=contract_version,
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


def revision(
    deployment: ModelDeployment, head: str, enabled: bool, at: datetime
) -> HeadRevision:
    """Write a head revision directly.

    Args:
        deployment: The deployment.
        head: The head's name.
        enabled: Whether the head is enabled under it.
        at: When it takes effect.

    Returns:
        The revision.
    """
    return HeadRevision.objects.create(
        deployment=deployment, head=head, enabled=enabled, effective_at=at
    )


class RegisterTests(TestCase):
    """Registering a deployment."""

    def test_same_release_and_digest_reuses_one_deployment(self) -> None:
        first = deployments.register(declaration(), DIGEST)
        second = deployments.register(declaration(), DIGEST)
        self.assertEqual(first.pk, second.pk)
        self.assertEqual(ModelDeployment.objects.count(), 1)
        self.assertEqual(first.artifact_digest, DIGEST)
        self.assertEqual(first.contract_version, CONTRACT_VERSION)
        self.assertEqual(first.declaration, declaration().to_dict())

    def test_new_version_is_a_new_deployment(self) -> None:
        deployments.register(declaration("2.1.0"), DIGEST)
        deployments.register(declaration("2.2.0"), OTHER_DIGEST)
        self.assertEqual(ModelDeployment.objects.count(), 2)

    def test_different_digest_is_refused_naming_both(self) -> None:
        deployments.register(declaration(), DIGEST)
        with self.assertRaises(DeploymentRefused) as caught:
            deployments.register(declaration(), OTHER_DIGEST)
        self.assertIn(DIGEST, str(caught.exception))
        self.assertIn(OTHER_DIGEST, str(caught.exception))
        self.assertEqual(ModelDeployment.objects.get().artifact_digest, DIGEST)

    def test_unsupported_contract_major_is_refused(self) -> None:
        with self.assertRaises(DeploymentRefused) as caught:
            deployments.register(declaration(contract_version="2.0"), DIGEST)
        self.assertIn("2.0", str(caught.exception))
        self.assertFalse(ModelDeployment.objects.exists())

    def test_retired_deployment_is_not_registered_again(self) -> None:
        deployment = deployments.register(declaration(), DIGEST)
        deployment.retired_at = T0
        deployment.save()
        with self.assertRaises(DeploymentRefused) as caught:
            deployments.register(declaration(), DIGEST)
        self.assertIn("retired", str(caught.exception))


class RevisionInEffectTests(TestCase):
    """Which revision covers a detection owed at a given time."""

    def setUp(self) -> None:
        self.deployment = deployments.register(declaration(), DIGEST)

    def test_newly_declared_head_covers_only_detections_owed_after_enabling(
        self,
    ) -> None:
        self.assertIsNone(
            deployments.revision_in_effect(self.deployment, "redshift", T0)
        )
        enabled = revision(self.deployment, "redshift", True, T0)
        before = deployments.revision_in_effect(
            self.deployment, "redshift", T0 - timedelta(seconds=1)
        )
        self.assertIsNone(before)
        self.assertEqual(
            deployments.revision_in_effect(self.deployment, "redshift", T0), enabled
        )
        self.assertEqual(
            deployments.revision_in_effect(
                self.deployment, "redshift", T0 + timedelta(days=1)
            ),
            enabled,
        )

    def test_disable_then_enable_leaves_the_gap_uncovered(self) -> None:
        revision(self.deployment, "redshift", True, T0)
        disabled = revision(self.deployment, "redshift", False, T0 + timedelta(days=1))
        again = revision(self.deployment, "redshift", True, T0 + timedelta(days=2))
        gap = deployments.revision_in_effect(
            self.deployment, "redshift", T0 + timedelta(days=1, hours=12)
        )
        self.assertEqual(gap, disabled)
        self.assertFalse(gap.enabled)
        self.assertEqual(
            deployments.revision_in_effect(
                self.deployment, "redshift", T0 + timedelta(days=3)
            ),
            again,
        )

    def test_heads_and_deployments_do_not_mix(self) -> None:
        other = deployments.register(declaration("2.2.0"), OTHER_DIGEST)
        revision(other, "redshift", True, T0)
        revision(self.deployment, "flux_forecast", True, T0)
        self.assertIsNone(
            deployments.revision_in_effect(self.deployment, "redshift", T0)
        )

    def test_current_revisions_are_latest_in_effect_per_head(self) -> None:
        revision(self.deployment, "redshift", True, T0)
        disabled = revision(self.deployment, "redshift", False, T0 + timedelta(days=1))
        flux = revision(self.deployment, "flux_forecast", True, T0)
        current = deployments.current_revisions(
            self.deployment, at=T0 + timedelta(days=2)
        )
        self.assertEqual(current, {"redshift": disabled, "flux_forecast": flux})
