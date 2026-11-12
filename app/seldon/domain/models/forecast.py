"""What a caller receives for each object it asked to forecast.

Every output family the forward pass computes is here, so a caller reads the
fields it wants and ignores the rest. Values are plain Python and read-only
numpy arrays: no torch tensor crosses this boundary.

Nothing here imports Django or torch.
"""

from collections.abc import Mapping
from dataclasses import dataclass

import numpy as np

from seldon.domain.models.provenance import Provenance


@dataclass(frozen=True)
class RegimeIndicator:
    """Whether a request falls inside the regime the model was trained on.

    Two axes can put a forecast outside that regime, and either one clears
    ``in_training_regime``: too few usable observations, or a band that
    carried no training observations. The evaluation grid's span is reported
    as a measure against the training time scale and never flags a forecast
    on its own: the time scale is a normalization divisor, not a bound.

    Attributes:
        in_training_regime: ``False`` when the forecast is sparse or touches
            an untrained band. Read this before trusting a forecast.
        sparse: The object has fewer usable observations than the training
            selection filter admitted.
        n_observations: Usable observations the model conditioned on.
        min_points: The training selection filter: light curves with fewer
            observations were never trained on.
        untrained_bands: Bands, in the checkpoint's spelling and sorted, that
            appear among the usable observations or on the evaluation grid
            and contributed no training observations. Empty when none do.
        eval_span_days: Latest minus earliest evaluation time, in days.
        eval_span_in_time_scales: ``eval_span_days`` divided by the training
            time scale; values well above 1 are far from a typical training
            span.
        time_scale_days: The training time scale, in days.
    """

    in_training_regime: bool
    sparse: bool
    n_observations: int
    min_points: int
    untrained_bands: tuple[str, ...]
    eval_span_days: float
    eval_span_in_time_scales: float
    time_scale_days: float


@dataclass(frozen=True)
class ObjectForecast:
    """One object's forecast, with every output family from a single pass.

    The evaluation-grid arrays are in the caller's order and line up with
    each other: ``flux[k]`` is the forecast at ``eval_times[k]`` in
    ``eval_bands[k]``.

    Attributes:
        eval_times: The caller's evaluation times, in days.
        eval_bands: The caller's evaluation bands, as given.
        flux: Forecast flux at ``zero_point_mag``.
        flux_err: One-sigma forecast uncertainty, same units as ``flux``.
        zero_point_mag: The AB magnitude zero point ``flux`` is expressed
            in: the caller's, when it gave one value for the object, else the
            training zero point. Fluxes at different zero points are not
            comparable without it.
        parameters: The decoder's interpretable basis parameters, by name.
        class_probabilities: Probability of each class the checkpoint names,
            summing to one. Head outputs with no name are dropped.
        predicted_class: The most probable named class.
        latent_mean: The deterministic latent mean the forecast and the class
            prediction are decoded from.
        latent_sigma: The latent's per-dimension standard deviation.
        regime: Whether the request is inside the training regime.
        provenance: The library, checkpoint, and zero point behind the values.
    """

    eval_times: np.ndarray
    eval_bands: tuple[str, ...]
    flux: np.ndarray
    flux_err: np.ndarray
    zero_point_mag: float
    parameters: Mapping[str, float]
    class_probabilities: Mapping[str, float]
    predicted_class: str
    latent_mean: np.ndarray
    latent_sigma: np.ndarray
    regime: RegimeIndicator
    provenance: Provenance
