"""Tests for the checkpoint loader and its cached service seam.

These load the checkpoint baked into the image. The suite runs in a container
with no training data and, for the no-data proof, no network; nothing here
mounts or fetches either.
"""

import shutil
import tempfile
from pathlib import Path
from typing import Any, Iterator

import torch
import yaml
from django.test import SimpleTestCase

from seldon import services
from seldon.config.settings import get_settings
from seldon.infrastructure.ml.loader import (
    IncompatibleCheckpointError,
    UnsafeFilterDirectoryError,
    installed_filter_dir,
    load_model,
    prepare_config,
)

# 43 bands times the four places hparams.yaml records them (U1 finding).
EXPECTED_FILTER_PATHS = 172


def _strings(node: Any) -> Iterator[str]:
    """Yield every string value anywhere in a nested config."""
    if isinstance(node, dict):
        for value in node.values():
            yield from _strings(value)
    elif isinstance(node, list):
        for item in node:
            yield from _strings(item)
    elif isinstance(node, str):
        yield node


def _installed_package_data_dir() -> Path:
    """The installed library's own data directory, found without the loader."""
    import TAE

    return Path(TAE.__file__).resolve().parent / "data"


class TempDirTestCase(SimpleTestCase):
    """Gives each test a scratch directory removed afterwards."""

    def setUp(self) -> None:
        """Create the scratch directory."""
        self.tmp = Path(tempfile.mkdtemp(prefix="seldon-"))
        self.addCleanup(shutil.rmtree, self.tmp)


class LoadTests(SimpleTestCase):
    """The baked-in checkpoint loads to a ready experiment on CPU."""

    def test_loads_with_no_training_data_on_disk(self) -> None:
        """No light-curve data exists, yet the experiment comes up ready."""
        hparams = yaml.safe_load(get_settings().hparams_path.read_text())
        for recorded in hparams["dataset"]["config"]["lightcurve_path"]:
            self.assertFalse(Path(recorded).exists(), recorded)

        experiment = services.loaded_model().experiment

        self.assertEqual(type(experiment).__name__, "SELDONEnergyClassifierExperiment3")
        self.assertEqual(len(experiment.class_map), 9)
        self.assertEqual(experiment.data.flux_stats["t_max"], 20.0)

    def test_stale_experiment_dataloader_name_does_not_block_load(self) -> None:
        """The guard reads ``dataset.name``, not the stale ``exp_params`` copy.

        The shipped checkpoint records the incompatible loader's name under
        ``exp_params.config.params.dataset`` and must still load.
        """
        hparams = yaml.safe_load(get_settings().hparams_path.read_text())
        self.assertEqual(
            hparams["exp_params"]["config"]["params"]["dataset"], "LightCurveDataLoader"
        )
        self.assertEqual(hparams["dataset"]["name"], "FlexibleLightCurveDataLoader")

        experiment = services.loaded_model().experiment

        self.assertEqual(type(experiment.data).__name__, "FlexibleLightCurveDataLoader")

    def test_loading_twice_returns_the_cached_object(self) -> None:
        """A second call in the same process returns the identical object."""
        first = services.loaded_model()
        second = services.loaded_model()

        self.assertIs(first, second)
        self.assertIs(first.experiment, second.experiment)

    def test_model_is_on_cpu_in_evaluation_mode(self) -> None:
        """Every tensor sits on the CPU and no module is in training mode."""
        experiment = services.loaded_model().experiment

        devices = {t.device.type for t in experiment.parameters()}
        devices |= {t.device.type for t in experiment.buffers()}
        self.assertEqual(devices, {"cpu"})
        self.assertFalse(experiment.training)
        self.assertFalse(any(module.training for module in experiment.modules()))


class CompatibilityGuardTests(TempDirTestCase):
    """Checkpoints the adapter cannot serve fail with a named reason."""

    def test_rejects_dataloader_that_computes_stats_from_data(self) -> None:
        """Covers AE2: the sibling loader is named, not a missing-file error."""
        defaults = get_settings()
        hparams = yaml.safe_load(defaults.hparams_path.read_text())
        hparams["dataset"]["name"] = "LightCurveDataLoader"
        modified = self.tmp / "hparams.yaml"
        modified.write_text(yaml.safe_dump(hparams))

        with self.assertRaises(IncompatibleCheckpointError) as caught:
            load_model(defaults.checkpoint_path, modified)

        self.assertIn("'LightCurveDataLoader'", str(caught.exception))

    def test_unexpected_missing_key_fails_the_load(self) -> None:
        """A state dict lacking a key the model expects is an error, not a warning."""
        defaults = get_settings()
        checkpoint = torch.load(
            defaults.checkpoint_path, map_location="cpu", weights_only=True
        )
        dropped = sorted(checkpoint["state_dict"])[0]
        del checkpoint["state_dict"][dropped]
        truncated = self.tmp / "truncated.ckpt"
        torch.save(checkpoint, truncated)

        with self.assertRaises(IncompatibleCheckpointError) as caught:
            load_model(truncated, defaults.hparams_path)

        self.assertIn(dropped, str(caught.exception))


class FilterPathTests(TempDirTestCase):
    """Training-time filter paths are repointed at the installed package."""

    def test_every_filter_path_resolves_inside_installed_package(self) -> None:
        """All four recorded places are rewritten to existing package files."""
        config = prepare_config(get_settings().hparams_path, installed_filter_dir())
        package_data = _installed_package_data_dir()

        filter_paths = [s for s in _strings(config) if s.endswith(".dat")]
        self.assertEqual(len(filter_paths), EXPECTED_FILTER_PATHS)
        for path in filter_paths:
            resolved = Path(path).resolve()
            self.assertTrue(resolved.is_file(), path)
            self.assertEqual(resolved.parent, package_data, path)

    def _copy_filters_to(self, directory: Path) -> Path:
        """Place a real copy of the installed filter files at ``directory``."""
        shutil.copytree(_installed_package_data_dir(), directory)
        return directory

    def test_refuses_filter_directory_named_jwst_or_total(self) -> None:
        """A directory whose own name holds a unit-switching substring is refused."""
        hparams_path = get_settings().hparams_path
        neutral = self._copy_filters_to(self.tmp / "filters")
        prepare_config(hparams_path, neutral)  # the copy itself is usable

        for name in ("jwst_filters", "total_filters"):
            with self.subTest(name=name):
                directory = self._copy_filters_to(self.tmp / name)
                with self.assertRaises(UnsafeFilterDirectoryError):
                    prepare_config(hparams_path, directory)

    def test_refuses_filter_directory_under_jwst_ancestor(self) -> None:
        """An ancestor directory holding ``jwst`` is refused, not only the leaf."""
        directory = self._copy_filters_to(self.tmp / "jwst_home" / "filters")

        with self.assertRaises(UnsafeFilterDirectoryError):
            prepare_config(get_settings().hparams_path, directory)
