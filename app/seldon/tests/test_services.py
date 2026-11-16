"""Tests for ``seldon.services.request_products`` and the torch-free service.

The service process holds no model: products come from the one model package
installed in a worker image. A stub package stands in for it here, found
through patched entry points, so these run in any image.
"""

import os
import subprocess
import sys
from unittest import mock

from django.test import SimpleTestCase
from seldon_model_contract import InputKind, ObjectResult, Packet, Point, Product

from seldon import services
from seldon.tests.test_forecast_worker import (
    CLASSES,
    FLUX,
    STUB_BANDS,
    StubPackage,
    fake_entry_points,
)


def packet(object_id: str = "1") -> Packet:
    """A two-point Rubin packet.

    Args:
        object_id: The object's id.

    Returns:
        The packet.
    """
    return Packet(
        object_id=object_id,
        points=(
            Point("lsst", "r", 61000.0, 1000.0, 10.0, 31.4),
            Point("lsst", "g", 61002.0, 1500.0, 10.0, 31.4),
        ),
    )


class RequestProductsTests(SimpleTestCase):
    """Products from the package installed in this image."""

    def setUp(self) -> None:
        """Forget any package loaded by an earlier test."""
        services.installed_package.cache_clear()
        self.addCleanup(services.installed_package.cache_clear)

    def test_runs_the_installed_package_with_default_inputs(self) -> None:
        """The single installed package gets each head's declared default
        inputs resolved for each packet (daily from the first point to three
        days past the last, both stub bands) and its results come back as
        returned."""
        package = StubPackage()
        with (
            fake_entry_points(package),
            mock.patch.dict(os.environ, {"SELDON_WORKER_PACKAGE": ""}),
        ):
            results = services.request_products(
                [packet("1"), packet("2")], [FLUX, CLASSES]
            )

        self.assertEqual([r.object_id for r in results], ["1", "2"])
        self.assertIsInstance(results[0], ObjectResult)
        self.assertIsInstance(results[0].outcomes[FLUX], Product)
        (requests,) = package.requests
        flux = next(r for r in requests if r.head == FLUX)
        inputs = flux.for_object("1")
        self.assertEqual(
            inputs[InputKind.TIMES], tuple(61000.0 + day for day in range(6))
        )
        self.assertEqual(inputs[InputKind.BANDS], STUB_BANDS)

    def test_named_package_is_chosen_among_several(self) -> None:
        """With two packages installed, ``SELDON_WORKER_PACKAGE`` picks one."""
        chosen = StubPackage(name="chosen")
        other = StubPackage(name="other")
        entries = [
            mock.Mock(load=lambda: other),
            mock.Mock(load=lambda: chosen),
        ]
        entries[0].name, entries[1].name = "other", "chosen"
        with (
            mock.patch(
                "seldon_model_contract.discovery.entry_points", return_value=entries
            ),
            mock.patch.dict(os.environ, {"SELDON_WORKER_PACKAGE": "chosen"}),
        ):
            services.request_products([packet()], [CLASSES])

        self.assertEqual(len(chosen.calls), 1)
        self.assertEqual(other.calls, [])

    def test_several_packages_without_a_name_is_an_error(self) -> None:
        """Two installed packages and no name: the error lists both."""
        entries = [mock.Mock(), mock.Mock()]
        entries[0].name, entries[1].name = "first", "second"
        with (
            mock.patch(
                "seldon_model_contract.discovery.entry_points", return_value=entries
            ),
            mock.patch.dict(os.environ, {"SELDON_WORKER_PACKAGE": ""}),
            self.assertRaises(services.NoModelPackageError) as caught,
        ):
            services.request_products([packet()], [CLASSES])

        self.assertIn("first", str(caught.exception))
        self.assertIn("second", str(caught.exception))

    def test_no_package_installed_is_an_error_naming_the_problem(self) -> None:
        """In an image with no model package the call says none is installed."""
        with (
            mock.patch("seldon_model_contract.discovery.entry_points", return_value=[]),
            mock.patch.dict(os.environ, {"SELDON_WORKER_PACKAGE": ""}),
            self.assertRaises(services.NoModelPackageError) as caught,
        ):
            services.request_products([packet()], [CLASSES])

        self.assertIn("No model package is installed", str(caught.exception))

    def test_missing_named_package_is_an_error_naming_it(self) -> None:
        """A package named by ``SELDON_WORKER_PACKAGE`` but not installed is
        named in the error."""
        with (
            mock.patch("seldon_model_contract.discovery.entry_points", return_value=[]),
            mock.patch.dict(os.environ, {"SELDON_WORKER_PACKAGE": "seldon-reference"}),
            self.assertRaises(services.NoModelPackageError) as caught,
        ):
            services.request_products([packet()], [CLASSES])

        self.assertIn("seldon-reference", str(caught.exception))

    def test_undeclared_head_is_an_error_naming_the_heads(self) -> None:
        """Asking for a head the package does not declare names the ones it
        does, and the package is not called."""
        package = StubPackage()
        with (
            fake_entry_points(package),
            mock.patch.dict(os.environ, {"SELDON_WORKER_PACKAGE": ""}),
            self.assertRaises(ValueError) as caught,
        ):
            services.request_products([packet()], ["redshift"])

        self.assertIn("redshift", str(caught.exception))
        self.assertIn(FLUX, str(caught.exception))
        self.assertEqual(package.calls, [])


class TorchFreeImportTests(SimpleTestCase):
    """The service's own modules never import torch or the model library."""

    def test_service_modules_import_no_model_stack(self) -> None:
        """A fresh interpreter set up for Django imports the facade, the
        views, the app config, intake, the readers and the worker without
        importing torch or ``seldon_core``."""
        code = (
            "import sys\n"
            "import django\n"
            "django.setup()\n"
            "import seldon.services, seldon.views, seldon.apps\n"
            "import seldon.intake.engine\n"
            "import seldon.management.commands.consume_alerts\n"
            "import seldon.management.commands.replay_alerts\n"
            "import seldon.management.commands.alert_report\n"
            "import seldon.management.commands.compare_replays\n"
            "import seldon.forecasting.worker\n"
            "print(sorted(m for m in sys.modules\n"
            "    if m.split('.')[0] in ('torch', 'seldon_core')))\n"
        )

        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            check=False,
            env={**os.environ, "DJANGO_SETTINGS_MODULE": "seldon_project.settings"},
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "[]")
