"""Tests that each built image carries what it should, and nothing more.

These run inside a container and inspect the installed environment rather
than application logic. The Dockerfile builds two images and names the one
running in ``SELDON_IMAGE_TARGET``:

- ``service`` (``app`` and ``consumer``): no torch and no model package.
- ``worker`` (a forecaster): the pinned CPU torch, the environment the model
  library needs before import, and exactly one model package with its wheel
  digest recorded.

Each class skips outside the image it describes.
"""

import os
import re
import subprocess
import sys
import unittest
from importlib import metadata
from importlib.util import find_spec
from pathlib import Path

from django.conf import settings
from django.test import SimpleTestCase
from seldon_model_contract import registered_names

PINNED_TORCH = "2.9.0+cpu"
IMAGE_TARGET = os.environ.get("SELDON_IMAGE_TARGET")
REFERENCE_DISTRIBUTION = "seldon-reference-model"
REFERENCE = "seldon-reference"


def reference_installed() -> bool:
    """Say whether the reference model package is installed here.

    Returns:
        True when its distribution is installed.
    """
    try:
        metadata.version(REFERENCE_DISTRIBUTION)
    except metadata.PackageNotFoundError:
        return False
    return True


class RequirementsTests(SimpleTestCase):
    """The service's requirements cannot pull in a model runtime."""

    def test_requirements_has_no_torch_entry(self) -> None:
        """``requirements.txt`` never names torch, so pip cannot swap the build."""
        lines = (Path(settings.BASE_DIR) / "requirements.txt").read_text().splitlines()
        names = [
            re.split(r"[\s<>=!~;\[@]", line.strip(), maxsplit=1)[0].lower()
            for line in lines
            if line.strip() and not line.strip().startswith("#")
        ]

        self.assertNotIn("torch", names)


@unittest.skipUnless(IMAGE_TARGET == "service", "runs in the service image")
class ServiceImageTests(SimpleTestCase):
    """The service image holds no model and no model runtime."""

    def test_torch_is_absent(self) -> None:
        """Importing torch fails: nothing the service installs pulls it in."""
        self.assertIsNone(find_spec("torch"))

    def test_no_model_package_is_installed(self) -> None:
        """Only the contract package is here; no model registers."""
        self.assertEqual(registered_names(), [])


@unittest.skipUnless(IMAGE_TARGET == "worker", "runs in the worker image")
class WorkerImageTests(SimpleTestCase):
    """A worker image holds the pinned runtime and exactly one model package."""

    def test_torch_is_the_pinned_cpu_build(self) -> None:
        """Version matches the pin and no CUDA runtime is present or usable."""
        import torch

        self.assertEqual(torch.__version__, PINNED_TORCH)
        self.assertIsNone(torch.version.cuda)
        self.assertFalse(torch.cuda.is_available())

    def test_compile_disabled_in_a_fresh_interpreter(self) -> None:
        """A bare interpreter that imports nothing of ours sees compile disabled.

        The library compiles at construction and imports ``matplotlib.pyplot``
        at module scope, so these must come from the container environment,
        not from anything the application sets after import.
        """
        probe = (
            "import os, json; env = dict(os.environ);"
            "import torch._dynamo;"
            "print(json.dumps([env.get('TORCH_COMPILE_DISABLE'),"
            " env.get('MPLBACKEND'), torch._dynamo.config.disable]))"
        )
        result = subprocess.run(
            [sys.executable, "-c", probe],
            capture_output=True,
            text=True,
            check=True,
            env=os.environ.copy(),
            cwd="/",
        )

        self.assertEqual(result.stdout.strip(), '["1", "Agg", true]')

    def test_exactly_one_model_package_is_discoverable(self) -> None:
        """The image is one deployment: one package answers the entry point."""
        self.assertEqual(len(registered_names()), 1)

    def test_worker_finds_the_recorded_digest_unconfigured(self) -> None:
        """With no digest setting, the worker takes the wheel's sha256 the
        image recorded at build time."""
        env = {
            key: value
            for key, value in os.environ.items()
            if key != "SELDON_WORKER_DIGEST"
        }
        env["SELDON_WORKER_PACKAGE"] = registered_names()[0]
        probe = (
            "from seldon.config.worker import WorkerSettings as S; print(S().digest)"
        )
        result = subprocess.run(
            [sys.executable, "-c", probe],
            capture_output=True,
            text=True,
            check=True,
            env=env,
            cwd=settings.BASE_DIR,
        )

        self.assertRegex(result.stdout.strip(), r"^sha256:[0-9a-f]{64}$")


@unittest.skipUnless(reference_installed(), f"{REFERENCE_DISTRIBUTION} not installed")
class ReferencePackageImageTests(SimpleTestCase):
    """The reference package, where an image installs it."""

    def test_reference_package_is_discoverable(self) -> None:
        """Its entry point is registered under the name workers load."""
        self.assertIn(REFERENCE, registered_names())

    def test_threads_are_pinned_by_the_package(self) -> None:
        """Loading the model sets torch's pool to ``SELDON_TORCH_THREADS``,
        not to the host's core count."""
        probe = (
            "from seldon_reference_model.runtime import loaded_model;"
            "loaded_model();"
            "import torch; print(torch.get_num_threads())"
        )
        env = os.environ.copy()
        env.update({"SELDON_TORCH_THREADS": "2", "OMP_NUM_THREADS": "3"})
        result = subprocess.run(
            [sys.executable, "-c", probe],
            capture_output=True,
            text=True,
            check=True,
            env=env,
            cwd="/",
        )

        self.assertEqual(result.stdout.strip().splitlines()[-1], "2")
