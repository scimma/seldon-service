"""This process's model: where its checkpoint is, and the one cached load.

Settings come from ``SELDON_``-prefixed environment variables, so a worker
image can point at another checkpoint without code changes. The defaults are
the checkpoint shipped inside this package.

The model loads once per process (about a second and roughly 1.7 GB), and
torch's thread count is pinned before that first load: left at torch's
default, the pool is sized to the host's cores and a CPU quota then makes
forecasts 15-40x slower.
"""

import os
from collections.abc import Sequence
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from seldon_reference_model.forecast_service import forecast_objects
from seldon_reference_model.ml.capabilities import read_capabilities, read_provenance
from seldon_reference_model.ml.loader import LoadedModel, load_model
from seldon_reference_model.models.capabilities import ModelCapabilities
from seldon_reference_model.models.forecast import ObjectForecast
from seldon_reference_model.models.provenance import Provenance
from seldon_reference_model.models.request import ObjectRequest

CHECKPOINT_DIR = (
    Path(__file__).resolve().parent / "checkpoints" / "seldon-2.0-roman-elasticc"
)


@dataclass(frozen=True)
class Settings:
    """Where the checkpoint is and how many threads torch may use.

    Attributes:
        checkpoint_path: The Lightning checkpoint file
            (``SELDON_CHECKPOINT_PATH``).
        hparams_path: The ``hparams.yaml`` it was trained with
            (``SELDON_HPARAMS_PATH``).
        torch_threads: torch's intra-op thread count; set it to the
            container's CPU limit (``SELDON_TORCH_THREADS``).
    """

    checkpoint_path: Path
    hparams_path: Path
    torch_threads: int


def get_settings() -> Settings:
    """Read the settings from the environment, with packaged defaults.

    Returns:
        The settings. An empty variable counts as unset.

    Raises:
        ValueError: When ``SELDON_TORCH_THREADS`` is not a positive integer.
    """
    checkpoint = os.environ.get("SELDON_CHECKPOINT_PATH") or str(
        CHECKPOINT_DIR / "checkpoints" / "epoch=1086-val_loss=1.18.ckpt"
    )
    hparams = os.environ.get("SELDON_HPARAMS_PATH") or str(
        CHECKPOINT_DIR / "hparams.yaml"
    )
    threads = int(os.environ.get("SELDON_TORCH_THREADS") or 1)
    if threads < 1:
        raise ValueError("SELDON_TORCH_THREADS must be at least 1")
    return Settings(Path(checkpoint), Path(hparams), threads)


@lru_cache(maxsize=1)
def loaded_model() -> LoadedModel:
    """Return this process's model, loading it on first call.

    Returns:
        The loaded model for the configured checkpoint.

    Raises:
        seldon_reference_model.ml.loader.IncompatibleCheckpointError: If the
            configured checkpoint cannot be served by this adapter.
    """
    import torch

    settings = get_settings()
    torch.set_num_threads(settings.torch_threads)
    return load_model(settings.checkpoint_path, settings.hparams_path)


@lru_cache(maxsize=1)
def model_description() -> tuple[ModelCapabilities, Provenance]:
    """Return what the cached model can serve and where it came from."""
    loaded = loaded_model()
    return read_capabilities(loaded), read_provenance(loaded)


def forecast(requests: Sequence[ObjectRequest]) -> list[ObjectForecast]:
    """Forecast one or more objects with the process's cached model.

    Args:
        requests: The objects to forecast, in the order results return.

    Returns:
        One forecast per request, in request order.

    Raises:
        seldon_reference_model.errors.SeldonRejection: If a request cannot be
            served; its ``code`` says why.
    """
    capabilities, provenance = model_description()
    return forecast_objects(requests, loaded_model(), capabilities, provenance)
