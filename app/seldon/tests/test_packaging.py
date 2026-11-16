"""Tests that the built image carries what CPU-only serving needs.

These run inside the container and inspect the installed environment rather
than application logic: the torch build, the process environment, and the
requirements file that produced the image. The service itself no longer
loads a model, so the checkpoint and the thread pool are the model
package's to check.
"""

import os
import re
import subprocess
import sys
from pathlib import Path

import torch
from django.conf import settings
from django.test import SimpleTestCase

PINNED_TORCH = "2.9.0+cpu"


class TorchBuildTests(SimpleTestCase):
    """The installed torch is the pinned CPU-index build."""

    def test_torch_is_the_pinned_cpu_build(self) -> None:
        """Version matches the pin and no CUDA runtime is present or usable."""
        self.assertEqual(torch.__version__, PINNED_TORCH)
        self.assertIsNone(torch.version.cuda)
        self.assertFalse(torch.cuda.is_available())

    def test_requirements_has_no_torch_entry(self) -> None:
        """``requirements.txt`` never names torch, so pip cannot swap the build."""
        lines = (Path(settings.BASE_DIR) / "requirements.txt").read_text().splitlines()
        names = [
            re.split(r"[\s<>=!~;\[@]", line.strip(), maxsplit=1)[0].lower()
            for line in lines
            if line.strip() and not line.strip().startswith("#")
        ]

        self.assertNotIn("torch", names)


class ContainerEnvironmentTests(SimpleTestCase):
    """Flags the library needs are set before any Python code runs."""

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
