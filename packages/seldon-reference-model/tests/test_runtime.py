"""The runtime's settings and the thread pinning on the first model load."""

import os
import unittest
from pathlib import Path
from unittest import mock

from seldon_reference_model import runtime

VARIABLES = ("SELDON_CHECKPOINT_PATH", "SELDON_HPARAMS_PATH", "SELDON_TORCH_THREADS")


def environment(**values: str) -> dict[str, str]:
    """Return the process environment with the three settings replaced."""
    env = {k: v for k, v in os.environ.items() if k not in VARIABLES}
    env.update(values)
    return env


class SettingsTests(unittest.TestCase):
    def test_unset_and_empty_variables_use_the_packaged_checkpoint(self) -> None:
        for values in ({}, {name: "" for name in VARIABLES}):
            with self.subTest(values=values):
                with mock.patch.dict(os.environ, environment(**values), clear=True):
                    settings = runtime.get_settings()
                self.assertEqual(settings.torch_threads, 1)
                self.assertTrue(settings.checkpoint_path.is_file())
                self.assertTrue(settings.hparams_path.is_file())
                self.assertTrue(
                    settings.checkpoint_path.is_relative_to(runtime.CHECKPOINT_DIR)
                )

    def test_variables_override_the_defaults(self) -> None:
        values = {
            "SELDON_CHECKPOINT_PATH": "/models/x.ckpt",
            "SELDON_HPARAMS_PATH": "/models/hparams.yaml",
            "SELDON_TORCH_THREADS": "4",
        }
        with mock.patch.dict(os.environ, environment(**values), clear=True):
            settings = runtime.get_settings()
        self.assertEqual(settings.checkpoint_path, Path("/models/x.ckpt"))
        self.assertEqual(settings.hparams_path, Path("/models/hparams.yaml"))
        self.assertEqual(settings.torch_threads, 4)

    def test_thread_count_must_be_a_positive_integer(self) -> None:
        for value in ("0", "-2", "two"):
            with self.subTest(value=value):
                env = environment(SELDON_TORCH_THREADS=value)
                with mock.patch.dict(os.environ, env, clear=True):
                    with self.assertRaises(ValueError):
                        runtime.get_settings()


class ThreadPinningTests(unittest.TestCase):
    def test_first_load_pins_threads_before_loading(self) -> None:
        import torch

        calls = []
        runtime.loaded_model.cache_clear()
        self.addCleanup(runtime.loaded_model.cache_clear)
        env = environment(SELDON_TORCH_THREADS="3")
        with (
            mock.patch.dict(os.environ, env, clear=True),
            mock.patch.object(
                torch, "set_num_threads", side_effect=lambda n: calls.append(n)
            ),
            mock.patch.object(
                runtime,
                "load_model",
                side_effect=lambda *_: calls.append("load") or "model",
            ),
        ):
            self.assertEqual(runtime.loaded_model(), "model")
            self.assertEqual(runtime.loaded_model(), "model")
        self.assertEqual(calls, [3, "load"])


if __name__ == "__main__":
    unittest.main()
