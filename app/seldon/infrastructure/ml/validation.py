"""Decide whether the checkpoint can serve a request, before any encoding.

Validation refuses three things, each with its own ``RejectionCode``: a
request with no zero point, a band name the checkpoint's vocabulary does not
hold (in the photometry or on the evaluation grid), and an object with no
usable observation. A sparse light curve is not refused. Nothing is dropped
or rescaled here; that is encoding's job, and it takes the band indices
resolved here so each name is looked up exactly once.

Nothing here imports Django or torch.
"""

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

import numpy as np

from seldon.domain.errors import (
    MissingZeroPointError,
    NoUsableObservationsError,
    UnknownBandError,
)
from seldon.domain.models.capabilities import ModelCapabilities
from seldon.domain.models.request import (
    TRAINING_ZERO_POINT_MAG,
    ObjectRequest,
    ZeroPointDeclaration,
)


@dataclass(frozen=True)
class ValidatedObject:
    """A request the checkpoint can serve, with its bands resolved.

    Attributes:
        request: The request as the caller built it.
        band_indices: The model's band index for each observation.
        eval_band_indices: The model's band index for each evaluation point.
    """

    request: ObjectRequest
    band_indices: np.ndarray
    eval_band_indices: np.ndarray


def normalize_band(name: str) -> str:
    """Normalize a band name the way the library's data module does.

    ``FlexibleLightCurveDataLoader`` looks bands up as
    ``x.lower() if len(x) == 1 else x``: single-character names are lowered,
    longer ones are matched as given.

    Args:
        name: The caller's band name.

    Returns:
        The name to look up in the checkpoint's band index map.
    """
    return name.lower() if len(name) == 1 else name


def _unknown(names: Iterable[str], band_index: Mapping[str, int]) -> list[str]:
    """Return the names that do not resolve, first occurrence order, no repeats.

    Args:
        names: The caller's band names.
        band_index: The checkpoint's band name to index map.

    Returns:
        The unresolved names, as the caller gave them.
    """
    return list(dict.fromkeys(n for n in names if normalize_band(n) not in band_index))


def _resolve(names: Sequence[str], band_index: Mapping[str, int]) -> np.ndarray:
    """Map band names to the model's indices.

    Args:
        names: Band names already known to resolve.
        band_index: The checkpoint's band name to index map.

    Returns:
        A read-only integer array of indices.
    """
    indices = np.array([band_index[normalize_band(n)] for n in names], dtype=np.int64)
    indices.setflags(write=False)
    return indices


def validate_object(
    request: ObjectRequest, capabilities: ModelCapabilities
) -> ValidatedObject:
    """Refuse a request the checkpoint cannot serve, or resolve its bands.

    An observation is usable when its time, flux, and flux error are all
    finite. One usable observation is enough; sparsity is not a refusal.

    Args:
        request: One object's photometry and evaluation grid.
        capabilities: The loaded checkpoint's capabilities.

    Returns:
        The request with its band indices resolved.

    Raises:
        MissingZeroPointError: If the request states no zero point.
        UnknownBandError: If a band in the observations or the evaluation
            grid is not in the checkpoint's vocabulary.
        NoUsableObservationsError: If no observation is usable.
    """
    if request.zero_point is None:
        raise MissingZeroPointError(
            "The request states no flux zero point. Give the AB magnitude zero "
            "point of the flux (one value, or one per observation), or declare "
            f"{ZeroPointDeclaration.AT_TRAINING_ZERO_POINT.name} if the flux is "
            f"already at the model's {TRAINING_ZERO_POINT_MAG} mag zero point."
        )

    band_index = capabilities.band_index
    observed = _unknown(request.bands, band_index)
    evaluated = _unknown(request.eval_bands, band_index)
    if observed or evaluated:
        sides = []
        if observed:
            sides.append(f"observations {observed}")
        if evaluated:
            sides.append(f"evaluation grid {evaluated}")
        raise UnknownBandError(
            f"Bands not in the checkpoint's vocabulary, in the "
            f"{' and the '.join(sides)}. This checkpoint knows only LSST, "
            "Roman WFI, and JWST NIRCam filter names.",
            bands=tuple(dict.fromkeys(observed + evaluated)),
        )

    usable = (
        np.isfinite(request.times)
        & np.isfinite(request.flux)
        & np.isfinite(request.flux_err)
    )
    if not usable.any():
        raise NoUsableObservationsError(
            f"None of the object's {len(request.times)} observations has a "
            "finite time, flux, and flux error."
        )

    return ValidatedObject(
        request=request,
        band_indices=_resolve(request.bands, band_index),
        eval_band_indices=_resolve(request.eval_bands, band_index),
    )


def validate_objects(
    requests: Sequence[ObjectRequest], capabilities: ModelCapabilities
) -> tuple[ValidatedObject, ...]:
    """Validate each object of a request, refusing at the first that fails.

    Args:
        requests: The objects to forecast.
        capabilities: The loaded checkpoint's capabilities.

    Returns:
        One validated object per request, in order.

    Raises:
        MissingZeroPointError: See ``validate_object``.
        UnknownBandError: See ``validate_object``.
        NoUsableObservationsError: See ``validate_object``.
    """
    return tuple(validate_object(request, capabilities) for request in requests)
