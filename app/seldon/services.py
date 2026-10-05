"""Service seams between the web layer and the model adapter.

Nothing here imports Django, so the same seams serve a view, a management
command, or a process with no request cycle at all.
"""

from dataclasses import dataclass
from functools import lru_cache

from seldon.config.settings import get_settings
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
        checkpoint=f"{model.hparams_path.parent.name}/{model.checkpoint_path.name}",
    )
