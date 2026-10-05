"""What a caller asks the adapter to forecast.

A forecast request is a sequence of ``ObjectRequest`` values; one object is
the one-element case. Arrays are copied into read-only numpy arrays at
construction so a frozen request cannot change under the adapter.

The flux zero point has no default. A caller states it, per object or per
observation, or declares ``ZeroPointDeclaration.AT_TRAINING_ZERO_POINT`` to
assert the flux is already at ``TRAINING_ZERO_POINT_MAG``. Leaving it out is
representable (``None``) only so validation can refuse it by name.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum

import numpy as np

from seldon.domain.models.provenance import TRAINING_ZERO_POINT_MAG

__all__ = [
    "TRAINING_ZERO_POINT_MAG",
    "ObjectRequest",
    "ZeroPoint",
    "ZeroPointDeclaration",
]


class ZeroPointDeclaration(Enum):
    """An explicit statement about the flux zero point in place of a value."""

    AT_TRAINING_ZERO_POINT = "at_training_zero_point"
    """The flux is already at ``TRAINING_ZERO_POINT_MAG``; the caller vouches."""


# One AB magnitude zero point for the object, one per observation, or a
# declaration. ``None`` means the caller stated nothing, which is refused.
ZeroPoint = float | np.ndarray | ZeroPointDeclaration | None


def _frozen(values: Sequence | np.ndarray, dtype: type, name: str) -> np.ndarray:
    """Copy values into a read-only one-dimensional array.

    Args:
        values: The caller's values.
        dtype: The array's element type.
        name: The field name, for the error message.

    Returns:
        A new read-only array.

    Raises:
        ValueError: If the values are not one-dimensional.
    """
    array = np.array(values, dtype=dtype)
    if array.ndim != 1:
        raise ValueError(f"{name} must be one-dimensional, got shape {array.shape}.")
    array.setflags(write=False)
    return array


@dataclass(frozen=True)
class ObjectRequest:
    """One object's photometry and the grid to forecast it on.

    Attributes:
        times: Observation times, in days.
        flux: Observed flux at the caller's zero point.
        flux_err: One-sigma flux uncertainty, same units as ``flux``.
        detected: Per-observation detection flag.
        bands: Per-observation band name, as the caller gives it.
        eval_times: Times to forecast at, in days.
        eval_bands: Band to forecast in at each of ``eval_times``.
        zero_point: The flux's AB magnitude zero point: a float for the
            object, an array with one value per observation, or a
            ``ZeroPointDeclaration``. ``None`` (the default) is refused by
            validation.

    Raises:
        ValueError: At construction, if a field is not one-dimensional, the
            five per-observation fields differ in length, the two evaluation
            fields differ in length, a per-observation zero point does not
            have one value per observation, or a detection flag is not 0, 1,
            True, or False.
    """

    times: np.ndarray
    flux: np.ndarray
    flux_err: np.ndarray
    detected: np.ndarray
    bands: tuple[str, ...]
    eval_times: np.ndarray
    eval_bands: tuple[str, ...]
    zero_point: ZeroPoint = None

    def __post_init__(self) -> None:
        """Freeze the arrays and check that per-point fields line up."""
        # The flag indexes a two-row embedding, so only 0 and 1 are flags;
        # check before the bool coercion would turn a 2 into True.
        if not np.isin(np.asarray(self.detected), (0, 1)).all():
            raise ValueError("detected must hold only 0, 1, True, or False.")
        set_ = object.__setattr__
        set_(self, "times", _frozen(self.times, float, "times"))
        set_(self, "flux", _frozen(self.flux, float, "flux"))
        set_(self, "flux_err", _frozen(self.flux_err, float, "flux_err"))
        set_(self, "detected", _frozen(self.detected, bool, "detected"))
        set_(self, "bands", tuple(self.bands))
        set_(self, "eval_times", _frozen(self.eval_times, float, "eval_times"))
        set_(self, "eval_bands", tuple(self.eval_bands))

        observed = {
            "times": len(self.times),
            "flux": len(self.flux),
            "flux_err": len(self.flux_err),
            "detected": len(self.detected),
            "bands": len(self.bands),
        }
        if len(set(observed.values())) != 1:
            raise ValueError(f"Per-observation fields differ in length: {observed}.")
        if len(self.eval_times) != len(self.eval_bands):
            raise ValueError(
                f"eval_times has {len(self.eval_times)} values but eval_bands "
                f"has {len(self.eval_bands)}."
            )

        zero_point = self.zero_point
        if zero_point is None or isinstance(zero_point, ZeroPointDeclaration):
            return
        if np.ndim(zero_point) == 0:
            set_(self, "zero_point", float(zero_point))
            return
        per_point = _frozen(zero_point, float, "zero_point")
        if len(per_point) != len(self.times):
            raise ValueError(
                f"zero_point has {len(per_point)} values for "
                f"{len(self.times)} observations."
            )
        set_(self, "zero_point", per_point)
