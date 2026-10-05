"""Load a SELDON checkpoint into a ready experiment on CPU, with no training data.

A checkpoint directory holds ``hparams.yaml`` and a Lightning checkpoint file.
The YAML records the training cluster's filter paths and data paths, and the
library's loaders read both, so loading it as-is fails. This module edits the
config in memory (never on disk, which is unsafe across workers), forces the
data module's no-data path, builds model, data module, and experiment through
the library's factories, and loads the weights explicitly onto the CPU.

Nothing here imports Django and nothing here caches; ``seldon.services`` holds
the per-process cache.
"""

import inspect
import logging
import time
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path
from typing import Any

import torch
import torch._dynamo
from omegaconf import OmegaConf

from seldon.domain.errors import IncompatibleCheckpointError

logger = logging.getLogger(__name__)

# The library's filter reader decides wavelength units by testing for these
# substrings anywhere in the path string (``TAE.util.read_transmission_filter``:
# ``'jwst' in filename`` means microns, else ``'total' in filename`` means
# nanometres). The leaf file names are meant to carry them; a directory that
# does silently rescales every filter read through it.
UNIT_SWITCHING_SUBSTRINGS = ("jwst", "total")

# Every key the checkpoint's state dict may lack, or carry beyond the model's,
# at this library tag. Both are empty: at seldon_core v1.2.0 this load path
# matches every key (docs/solutions/ml-runtime/seldon-cpu-viability.md).
ALLOWED_MISSING_KEYS: frozenset[str] = frozenset()
ALLOWED_UNEXPECTED_KEYS: frozenset[str] = frozenset()

# What a dataloader's constructor must accept for the no-data path to work:
# statistics, weights, and class map injected from hparams.yaml instead of
# computed from light curves, and the flag that skips reading them.
INJECTED_DATALOADER_PARAMETERS = frozenset(
    {"setup_from_data", "flux_stats", "band_weights", "class_map"}
)


class UnsafeFilterDirectoryError(IncompatibleCheckpointError, ValueError):
    """The filter directory's path would corrupt filter wavelength units.

    It refuses the load as any incompatibility does, so it shares that code.
    """


@dataclass(frozen=True)
class LoadedModel:
    """A ready experiment and where it came from.

    Attributes:
        experiment: The library's Lightning experiment, on CPU, in eval mode.
        checkpoint_path: The checkpoint file its weights came from.
        hparams_path: The ``hparams.yaml`` it was built from.
        library_version: The installed ``seldon_core`` distribution version.
    """

    experiment: Any
    checkpoint_path: Path
    hparams_path: Path
    library_version: str


def installed_filter_dir() -> Path:
    """Return the installed library's own transmission-filter directory.

    Returns:
        The ``data`` directory inside the installed ``TAE`` package.
    """
    import TAE

    return Path(TAE.__file__).parent / "data"


def _filter_dicts(config: dict[str, Any]) -> list[dict[str, str]]:
    """Return the four band-name-to-file maps that ``hparams.yaml`` records.

    The three nested ``filedict`` blocks are what the factories read; the
    top-level ``transmission_filters`` map is not read at load time and is
    rewritten only so the in-memory config holds no training-cluster path.

    Args:
        config: The parsed ``hparams.yaml``.

    Returns:
        The maps, each still part of ``config``, so editing one edits it.
    """
    model = config["model_params"]["config"]
    embedder = model["encoder"]["config"]["embedder"]["config"]
    return [
        config["transmission_filters"],
        config["dataset"]["config"]["band_index_map"]["config"]["filedict"],
        embedder["band_embedding"]["config"]["bandpass"]["config"]["filedict"],
        model["decoder"]["config"]["bandpass_embedding"]["config"]["bandpass"][
            "config"
        ]["filedict"],
    ]


def prepare_config(hparams_path: Path, filter_dir: Path) -> dict[str, Any]:
    """Read ``hparams.yaml`` and repoint its filter paths at ``filter_dir``.

    Each recorded path keeps its file name and moves to ``filter_dir``. The
    file name selects the units in the library's reader; the directory must
    not, so a directory path containing ``jwst`` or ``total`` is refused.

    Args:
        hparams_path: The checkpoint's ``hparams.yaml``.
        filter_dir: The directory holding the filter files, normally
            ``installed_filter_dir()``.

    Returns:
        The parsed config with all four filter maps rewritten.

    Raises:
        UnsafeFilterDirectoryError: If ``filter_dir``'s path contains a
            unit-switching substring in any component.
    """
    directory = Path(filter_dir).absolute()
    found = [s for s in UNIT_SWITCHING_SUBSTRINGS if s in str(directory)]
    if found:
        raise UnsafeFilterDirectoryError(
            f"Filter directory {directory} contains {found!r}; the library's "
            "filter reader switches wavelength units on these substrings "
            "anywhere in a path, so every filter read from here would be "
            "mis-scaled. Install the library under a path without them."
        )

    config = OmegaConf.to_container(OmegaConf.load(hparams_path))
    for filedict in _filter_dicts(config):
        for band, recorded in filedict.items():
            filedict[band] = str(directory / Path(recorded).name)
    return config


def _check_dataloader(name: str) -> None:
    """Refuse a dataloader that cannot take statistics from ``hparams.yaml``.

    Args:
        name: The dataloader named by ``dataset.name``, the field the factory
            builds from. (``exp_params.config.params.dataset`` also names a
            dataloader but is stale and unused.)

    Raises:
        IncompatibleCheckpointError: If the registered dataloader's
            constructor lacks a parameter the no-data path injects.
        KeyError: If no dataloader of that name is registered.
    """
    from seldon_core.datasets.dataset_factory import MODULES

    accepted = set(inspect.signature(MODULES[name].__init__).parameters)
    lacking = sorted(INJECTED_DATALOADER_PARAMETERS - accepted)
    if lacking:
        raise IncompatibleCheckpointError(
            f"Checkpoint dataloader {name!r} cannot accept normalization "
            f"statistics from hparams.yaml (its constructor lacks {lacking}); "
            "it computes them from training data, which is not present. Only "
            "checkpoints trained with a dataloader that accepts them, such as "
            "'FlexibleLightCurveDataLoader', can be served."
        )


def load_model(checkpoint_path: Path, hparams_path: Path) -> LoadedModel:
    """Build the experiment a checkpoint describes and load its weights on CPU.

    Each call builds a new experiment, and the library's data module starts a
    ``multiprocessing.Manager`` process each time; call it once per process
    through ``seldon.services.loaded_model``.

    Args:
        checkpoint_path: The Lightning checkpoint file.
        hparams_path: The ``hparams.yaml`` it was trained with.

    Returns:
        The ready experiment and its provenance.

    Raises:
        IncompatibleCheckpointError: If the dataloader cannot run without
            training data, the state dict's keys differ from the model's
            beyond the allowlists, or the decoder is class-conditional.
        UnsafeFilterDirectoryError: If the installed filter directory's path
            would corrupt filter wavelength units.
        RuntimeError: If torch compilation is enabled in this process.
    """
    from seldon_core.datasets.dataset_factory import get_module as get_dataset
    from seldon_core.experiments.factory import get_module as get_experiment
    from seldon_core.models.factory import get_module as get_model

    started = time.perf_counter()
    config = prepare_config(hparams_path, installed_filter_dir())
    _check_dataloader(config["dataset"]["name"])

    dataset = config["dataset"]["config"]
    dataset["setup_from_data"] = False
    dataset["num_workers"] = 0
    dataset["pin_memory"] = False
    dataset["augmentations"] = ["FullSample"]

    # The experiment reads the class map off the data module while it is
    # constructed, so the data module must exist and be set up first.
    model = get_model(config["model_params"]["name"], config["model_params"]["config"])
    data = get_dataset(config["dataset"]["name"], dataset)
    data.band_array = []  # library defect: the no-data setup reads it unset
    data.setup()
    experiment = get_experiment(
        config["exp_params"]["name"],
        {"model": model, "data": data, **config["exp_params"]["config"]},
    )

    checkpoint = torch.load(checkpoint_path, map_location="cpu", weights_only=True)
    keys = experiment.load_state_dict(checkpoint["state_dict"], strict=False)
    missing = sorted(set(keys.missing_keys) - ALLOWED_MISSING_KEYS)
    unexpected = sorted(set(keys.unexpected_keys) - ALLOWED_UNEXPECTED_KEYS)
    if missing or unexpected:
        raise IncompatibleCheckpointError(
            f"Checkpoint {checkpoint_path} does not match the model built from "
            f"{hparams_path}: missing keys {missing}, unexpected keys "
            f"{unexpected}."
        )

    # A class-conditional decoder would condition every forecast on whatever
    # class index the caller supplies, including a placeholder label.
    if experiment.model.class_conditional_decoder:
        raise IncompatibleCheckpointError(
            f"Checkpoint {checkpoint_path} uses a class-conditional decoder; "
            "this adapter serves only unconditioned forecasts."
        )
    if not torch._dynamo.config.disable:
        raise RuntimeError(
            "torch compilation is enabled; set TORCH_COMPILE_DISABLE=1 before "
            "torch is first imported. Only the uncompiled mode is qualified."
        )

    experiment.eval()
    loaded = LoadedModel(
        experiment=experiment,
        checkpoint_path=Path(checkpoint_path),
        hparams_path=Path(hparams_path),
        library_version=metadata.version("seldon_core"),
    )
    logger.info(
        "Loaded SELDON checkpoint %s (seldon_core %s) in %.2f s",
        checkpoint_path,
        loaded.library_version,
        time.perf_counter() - started,
    )
    return loaded
