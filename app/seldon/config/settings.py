"""Model-related settings, read from ``SELDON_``-prefixed environment variables.

Django's own settings stay in ``seldon_project.settings``; this class holds
everything the model adapter consumes.
"""

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# The checkpoint is baked into the image next to the application code
# (``/app/checkpoints/...`` in the container), so the default needs no fetch.
_CHECKPOINT_DIR = (
    Path(__file__).resolve().parents[2] / "checkpoints" / "seldon-2.0-roman-elasticc"
)


class Settings(BaseSettings):
    """Model settings for the SELDON service.

    Attributes:
        checkpoint_path: The Lightning checkpoint file to load.
        hparams_path: The ``hparams.yaml`` the checkpoint was trained with.
        torch_threads: torch's intra-op thread count. Set it to the
            container's CPU limit: left at torch's default, the pool is sized
            to the host's cores and a CPU quota then makes forecasts 15-40x
            slower (docs/solutions/ml-runtime/seldon-cpu-viability.md).
        warmup: Load the model at app startup instead of on first use. Off by
            default because startup runs in every process that sets up Django,
            including tests and management commands; the server entrypoint
            turns it on.
    """

    checkpoint_path: Path = Field(
        _CHECKPOINT_DIR / "checkpoints" / "epoch=1086-val_loss=1.18.ckpt"
    )
    hparams_path: Path = Field(_CHECKPOINT_DIR / "hparams.yaml")
    torch_threads: int = Field(1, ge=1)
    warmup: bool = Field(False)

    model_config = SettingsConfigDict(
        env_prefix="SELDON_",
        case_sensitive=False,
        # An empty SELDON_* value means "unset", not "blank this default".
        env_ignore_empty=True,
    )


def get_settings() -> Settings:
    """Build the model settings from the current environment.

    Returns:
        A freshly validated ``Settings`` instance.

    Raises:
        pydantic.ValidationError: If a ``SELDON_*`` variable holds a value its
            field rejects.
    """
    return Settings()
