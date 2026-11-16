"""Tests for the ``/healthz`` route.

The route is what a Kubernetes readiness probe calls, so its status code
carries readiness. The web process holds no model; it is ready when it can
reach the database, and it reports each live deployment's worker heartbeat
from there so a stalled forecaster shows up without the web process loading
anything.
"""

from datetime import timedelta
from unittest import mock

from django.db import OperationalError
from django.test import TestCase, override_settings
from django.utils import timezone

from seldon.models import ModelDeployment


def deployment(
    version: str, heartbeat_seconds_ago: float | None, retired: bool = False
) -> ModelDeployment:
    """Register a deployment with a heartbeat some seconds old.

    Args:
        version: Its package version.
        heartbeat_seconds_ago: Age of its last heartbeat, or ``None`` for a
            deployment whose worker never reported in.
        retired: Whether it is retired.

    Returns:
        The deployment.
    """
    now = timezone.now()
    return ModelDeployment.objects.create(
        package_name="seldon-reference",
        package_version=version,
        contract_version="1.0",
        artifact_digest="sha256:" + version,
        declaration={},
        last_heartbeat_at=(
            None
            if heartbeat_seconds_ago is None
            else now - timedelta(seconds=heartbeat_seconds_ago)
        ),
        retired_at=now if retired else None,
    )


class HealthzTests(TestCase):
    """``/healthz`` reports the version, the database, and each deployment."""

    def test_reports_application_version(self) -> None:
        """The payload names the ``APP_VERSION`` the process started with."""
        with override_settings(APP_VERSION="v9.8.7-test"):
            response = self.client.get("/healthz")

        self.assertEqual(response["Content-Type"], "application/json")
        self.assertEqual(response.json()["app_version"], "v9.8.7-test")

    def test_reports_each_live_deployment_and_its_heartbeat_age(self) -> None:
        """Two live deployments are listed with their release, contract, and
        heartbeat age (about 30 s, and none for one never heard from); the
        retired one is left out."""
        deployment("1.0.0", 30.0)
        deployment("1.1.0", None)
        deployment("0.9.0", 5.0, retired=True)

        response = self.client.get("/healthz")

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["status"], "ready")
        self.assertTrue(body["database"])
        listed = {d["package_version"]: d for d in body["deployments"]}
        self.assertEqual(sorted(listed), ["1.0.0", "1.1.0"])
        first = listed["1.0.0"]
        self.assertEqual(first["package_name"], "seldon-reference")
        self.assertEqual(first["contract_version"], "1.0")
        self.assertGreaterEqual(first["heartbeat_age_seconds"], 30.0)
        self.assertLess(first["heartbeat_age_seconds"], 90.0)
        self.assertIsNone(listed["1.1.0"]["heartbeat_age_seconds"])
        self.assertIsNone(listed["1.1.0"]["last_heartbeat_at"])

    def test_ready_in_an_image_with_no_model_package(self) -> None:
        """With no model package installed and none registered, the probe
        passes and loads nothing."""
        with (
            mock.patch("seldon_model_contract.discovery.entry_points", return_value=[]),
            mock.patch(
                "seldon_model_contract.load_package",
                side_effect=AssertionError("/healthz loaded a model"),
            ),
        ):
            response = self.client.get("/healthz")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["deployments"], [])

    def test_unreachable_database_is_not_ready(self) -> None:
        """When the database cannot be read the probe fails with 503."""
        with mock.patch.object(
            ModelDeployment.objects,
            "filter",
            side_effect=OperationalError("connection refused"),
        ):
            response = self.client.get("/healthz")

        self.assertEqual(response.status_code, 503)
        body = response.json()
        self.assertEqual(body["status"], "not_ready")
        self.assertFalse(body["database"])
        self.assertEqual(body["deployments"], [])
