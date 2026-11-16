"""Why the adapter refused to forecast, as a small closed set of codes.

Every refusal carries a ``code`` from ``RejectionCode`` on the error itself,
so the surfaces above the adapter (a view, a metric, a future MCP server) can
tally refusals by category without matching message text. The codes are a
public contract: add one only when a new kind of refusal cannot be told apart
from these.

Nothing here imports Django or torch.
"""

from enum import Enum
from typing import ClassVar


class RejectionCode(str, Enum):
    """The category of a refusal, stable across messages and releases."""

    MISSING_ZERO_POINT = "missing_zero_point"
    UNKNOWN_BAND = "unknown_band"
    NO_USABLE_OBSERVATIONS = "no_usable_observations"
    INCOMPATIBLE_CHECKPOINT = "incompatible_checkpoint"


class SeldonRejection(Exception):
    """Base for every refusal the adapter distinguishes.

    Attributes:
        code: The refusal's category.
    """

    code: ClassVar[RejectionCode]


class IncompatibleCheckpointError(SeldonRejection, RuntimeError):
    """The checkpoint cannot be served by this adapter, for a named reason."""

    code = RejectionCode.INCOMPATIBLE_CHECKPOINT


class MissingZeroPointError(SeldonRejection, ValueError):
    """The request declares no zero point for its flux."""

    code = RejectionCode.MISSING_ZERO_POINT


class UnknownBandError(SeldonRejection, ValueError):
    """A band name does not resolve in the checkpoint's band vocabulary.

    Attributes:
        bands: The unresolved names, as the caller gave them.
    """

    code = RejectionCode.UNKNOWN_BAND

    def __init__(self, message: str, bands: tuple[str, ...]) -> None:
        """Record the message and the unresolved names.

        Args:
            message: The human-readable reason, naming the bands.
            bands: The unresolved names, as the caller gave them.
        """
        super().__init__(message)
        self.bands = bands


class NoUsableObservationsError(SeldonRejection, ValueError):
    """The object has no observation the model could condition on."""

    code = RejectionCode.NO_USABLE_OBSERVATIONS
