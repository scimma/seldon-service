"""The shared codes for refusals and caveats.

Every package refuses and qualifies its products in these terms, so the
service and its consumers can count and filter them across model versions
without reading message text. A package decides which code applies; the
contract only fixes what each code means.

Adding a code is a minor contract version. A reader that meets a code it does
not know keeps it as given rather than dropping it.
"""

from enum import Enum


class RefusalCode(str, Enum):
    """Why a package returned no product for an object and head."""

    MISSING_ZERO_POINT = "missing_zero_point"
    """A point the package needs has no flux zero point."""

    UNKNOWN_BAND = "unknown_band"
    """No point is in a survey and band the package accepts."""

    NO_USABLE_OBSERVATIONS = "no_usable_observations"
    """Points are present but none can condition the model."""

    INCOMPATIBLE_CHECKPOINT = "incompatible_checkpoint"
    """The package cannot serve this request with the model it carries."""


class CaveatCode(str, Enum):
    """Why a product should be read with care. The product is still returned."""

    SPARSE_LIGHT_CURVE = "sparse_light_curve"
    """Fewer usable observations than the model was trained on. Detail:
    ``n_observations`` and ``min_points``."""

    UNTRAINED_BAND = "untrained_band"
    """An observed or requested band carried no training data. Detail:
    ``bands``, a list of ``[survey, band]`` pairs."""

    EVAL_SPAN_BEYOND_TRAINING = "eval_span_beyond_training"
    """The requested times reach past the spans the model was trained on.
    Detail: ``eval_span_days``."""

    EXCLUDED_POINTS = "excluded_points"
    """Points from survey and band pairs the package does not accept were
    left out. Detail: ``pairs``, a list of ``[survey, band]`` pairs, and
    ``n_points``."""


_REFUSAL_VALUES = frozenset(c.value for c in RefusalCode)
_CAVEAT_VALUES = frozenset(c.value for c in CaveatCode)


def is_known_refusal(code: str) -> bool:
    """Whether ``code`` is in the refusal vocabulary.

    Args:
        code: A refusal code as a string.

    Returns:
        ``True`` when the code is a ``RefusalCode`` value.
    """
    return code in _REFUSAL_VALUES


def is_known_caveat(code: str) -> bool:
    """Whether ``code`` is in the caveat vocabulary.

    Args:
        code: A caveat code as a string.

    Returns:
        ``True`` when the code is a ``CaveatCode`` value.
    """
    return code in _CAVEAT_VALUES
