"""Tests for the ``/healthz`` route.

The route is what a Kubernetes readiness probe calls, so its status code
carries readiness: a pod whose checkpoint has not loaded must not be marked
ready. No loader exists yet (U3 adds it), so only the not-ready half is
exercised here; the ready-after-load half lands with the real load.
"""

from django.test import SimpleTestCase, override_settings


class HealthzTests(SimpleTestCase):
    """``/healthz`` reports the running version and the model's readiness."""

    def test_reports_application_version(self) -> None:
        """The payload names the ``APP_VERSION`` the process started with."""
        with override_settings(APP_VERSION="v9.8.7-test"):
            response = self.client.get("/healthz")

        self.assertEqual(response["Content-Type"], "application/json")
        self.assertEqual(response.json()["app_version"], "v9.8.7-test")

    def test_reports_torch_version(self) -> None:
        """The payload names the installed torch build."""
        response = self.client.get("/healthz")

        self.assertEqual(response.json()["torch_version"], "2.9.0+cpu")

    def test_not_ready_before_checkpoint_loads(self) -> None:
        """Before any checkpoint has loaded the probe fails with 503."""
        response = self.client.get("/healthz")

        self.assertEqual(response.status_code, 503)
        body = response.json()
        self.assertEqual(body["status"], "not_ready")
        self.assertFalse(body["ready"])
        self.assertIsNone(body["model"])
