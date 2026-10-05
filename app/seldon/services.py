"""Service seams between the web layer and the model adapter."""

from dataclasses import dataclass


@dataclass(frozen=True)
class ModelIdentity:
    """What a loaded model reports about itself.

    Attributes:
        library_version: The installed ``seldon_core`` version it was built with.
        checkpoint: The checkpoint file it was loaded from.
    """

    library_version: str
    checkpoint: str


def loaded_model_identity() -> ModelIdentity | None:
    """Report whether this process has loaded the model, and which one.

    The per-process cached loader is not built yet; it will report through
    this function once it exists. Until then no model is ever loaded here.

    Returns:
        The identity of the loaded model, or ``None`` when no model has loaded.
    """
    return None
