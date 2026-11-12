"""Tests that the built image carries what CPU-only serving needs.

These run inside the container and inspect the installed environment rather
than application logic: the torch build, the process environment, the baked
checkpoint, and the requirements file that produced the image.
"""

import os
import re
import subprocess
import sys
from pathlib import Path

import torch
from django.apps import apps
from django.conf import settings
from django.test import SimpleTestCase

from seldon.config.settings import get_settings

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


class CheckpointTests(SimpleTestCase):
    """The checkpoint is baked into the image at the configured default path."""

    def test_checkpoint_and_hparams_exist_at_default_path(self) -> None:
        """Both files exist, non-empty, where the settings point by default."""
        defaults = get_settings()

        self.assertEqual(defaults.checkpoint_path.name, "epoch=1086-val_loss=1.18.ckpt")
        self.assertGreater(defaults.checkpoint_path.stat().st_size, 0)
        self.assertEqual(defaults.hparams_path.name, "hparams.yaml")
        self.assertGreater(defaults.hparams_path.stat().st_size, 0)


class TorchThreadTests(SimpleTestCase):
    """App startup pins torch's thread pool to ``SELDON_TORCH_THREADS``."""

    def test_startup_applies_configured_thread_count(self) -> None:
        """``ready()`` sets torch's thread count from the setting."""
        original = torch.get_num_threads()
        previous = os.environ.get("SELDON_TORCH_THREADS")
        os.environ["SELDON_TORCH_THREADS"] = "3"
        try:
            apps.get_app_config("seldon").ready()
            self.assertEqual(torch.get_num_threads(), 3)
        finally:
            if previous is None:
                del os.environ["SELDON_TORCH_THREADS"]
            else:
                os.environ["SELDON_TORCH_THREADS"] = previous
            torch.set_num_threads(original)

    def test_running_process_uses_configured_thread_count(self) -> None:
        """The test process itself started with the configured count."""
        self.assertEqual(torch.get_num_threads(), get_settings().torch_threads)
