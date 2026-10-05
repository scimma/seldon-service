"""Service seams between the web layer and the model adapter.

Nothing here imports Django, so the same seams serve a view, a management
command, or a process with no request cycle at all.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from functools import lru_cache

from seldon.config.settings import get_settings
from seldon.domain.models.capabilities import ModelCapabilities
from seldon.domain.models.forecast import ObjectForecast
from seldon.domain.models.provenance import Provenance
from seldon.domain.models.request import ObjectRequest
from seldon.domain.services.forecast_service import forecast_objects
from seldon.infrastructure.ml.capabilities import read_capabilities, read_provenance
from seldon.infrastructure.ml.loader import LoadedModel, load_model


@dataclass(frozen=True)
class ModelIdentity:
    """What a loaded model reports about itself.

    Attributes:
        library_version: The installed ``seldon_core`` version it was built with.
        checkpoint: The checkpoint it was loaded from, as
            ``<checkpoint directory>/<checkpoint file>``.
    """

    library_version: str
    checkpoint: str


@lru_cache(maxsize=1)
def loaded_model() -> LoadedModel:
    """Return this process's model, loading it on first call.

    The load takes about a second and roughly 1.7 GB of memory, so it happens
    once per process and every later call returns the same object.

    Returns:
        The loaded model for the configured checkpoint.

    Raises:
        seldon.infrastructure.ml.loader.IncompatibleCheckpointError: If the
            configured checkpoint cannot be served by this adapter.
        seldon.infrastructure.ml.loader.UnsafeFilterDirectoryError: If the
            installed filter directory would corrupt wavelength units.
    """
    settings = get_settings()
    return load_model(settings.checkpoint_path, settings.hparams_path)


def loaded_model_identity() -> ModelIdentity | None:
    """Report whether this process has loaded the model, and which one.

    This never triggers a load itself: a readiness probe calling it must not
    start a second-long, memory-heavy load on the request path.

    Returns:
        The identity of the loaded model, or ``None`` when no model has loaded.
    """
    if loaded_model.cache_info().currsize == 0:
        return None
    model = loaded_model()
    return ModelIdentity(
        library_version=model.library_version,
        checkpoint=model.checkpoint_id,
    )


@lru_cache(maxsize=1)
def _model_description() -> tuple[ModelCapabilities, Provenance]:
    """Return what the cached model can serve and where it came from.

    Both depend only on the loaded model, so they are read once per process
    rather than on every forecast.

    Returns:
        The loaded model's capabilities and provenance.

    Raises:
        seldon.infrastructure.ml.capabilities.BandVocabularyMismatchError: If
            the model's band vocabularies disagree.
    """
    loaded = loaded_model()
    return read_capabilities(loaded), read_provenance(loaded)


def forecast(requests: Sequence[ObjectRequest]) -> list[ObjectForecast]:
    """Forecast one or more objects with the process's cached model.

    This is the one call a caller needs. Build an ``ObjectRequest`` per
    object, giving its photometry, the zero point of its flux, and the grid
    to forecast on, and pass them together; they run as one batch::

        from seldon import services
        from seldon.domain.models.request import ObjectRequest

        (result,) = services.forecast([
            ObjectRequest(
                times=[0.0, 1.0, 2.0],  # days
                flux=[120.0, 340.0, 310.0],
                flux_err=[10.0, 12.0, 12.0],
                detected=[True, True, True],
                bands=["r", "g", "r"],  # LSST, Roman WFI, or NIRCam names
                eval_times=[3.0, 4.0],
                eval_bands=["r", "g"],
                zero_point=27.5,  # AB magnitude zero point of the flux
            )
        ])
        result.flux, result.flux_err  # on the grid, in the caller's order
        result.regime.in_training_regime  # read before trusting the values

    Each ``ObjectForecast`` carries every output family from the one pass
    (flux and uncertainty, basis parameters, class probabilities and the
    predicted class, the latent), the zero point its flux is expressed in,
    a training-regime indicator, and provenance. Read what you need. Values
    are numpy arrays and plain Python; no torch tensor is returned. The
    first call in a process loads the model, which takes about a second.

    Args:
        requests: The objects to forecast, in the order results return.

    Returns:
        One forecast per request, in request order.

    Raises:
        seldon.domain.errors.SeldonRejection: If the request cannot be
            served; its ``code`` says why (no zero point, a band the
            checkpoint does not know, no usable observation, or an
            incompatible checkpoint). A sparse light curve or an untrained
            band is served, with the regime indicator set, not refused.
    """
    capabilities, provenance = _model_description()
    return forecast_objects(requests, loaded_model(), capabilities, provenance)
