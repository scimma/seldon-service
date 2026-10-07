"""Tests for the ``/healthz`` route.

The route is what a Kubernetes readiness probe calls, so its status code
carries readiness: a pod whose checkpoint has not loaded must not be marked
ready.
"""

from functools import lru_cache
from unittest import mock

from django.test import SimpleTestCase, override_settings

from seldon import services
from seldon.infrastructure.ml.loader import LoadedModel


@lru_cache(maxsize=1)
def _no_load_yet() -> LoadedModel:
    """Stand in for a process that has not loaded; healthz must not call it."""
    raise AssertionError("/healthz triggered a model load")


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
        with mock.patch.object(services, "loaded_model", _no_load_yet):
            response = self.client.get("/healthz")

        self.assertEqual(response.status_code, 503)
        body = response.json()
        self.assertEqual(body["status"], "not_ready")
        self.assertFalse(body["ready"])
        self.assertIsNone(body["model"])

    def test_ready_once_checkpoint_has_loaded(self) -> None:
        """After the load the probe passes and names library and checkpoint."""
        services.loaded_model()

        response = self.client.get("/healthz")

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["status"], "ready")
        self.assertTrue(body["ready"])
        self.assertEqual(
            body["model"],
            {
                "library_version": "1.2.0",
                "checkpoint": "seldon-2.0-roman-elasticc/epoch=1086-val_loss=1.18.ckpt",
            },
        )
