"""Turn requests into forecasts: validate, encode, run, assemble.

The adapter does the model work; this module orders the steps and adds what
only the request and the checkpoint's capabilities can tell: whether each
object sits inside the regime the model was trained on.
"""

from collections.abc import Sequence

import numpy as np

from seldon.domain.models.capabilities import ModelCapabilities
from seldon.domain.models.forecast import ObjectForecast, RegimeIndicator
from seldon.domain.models.provenance import Provenance
from seldon.domain.models.request import ObjectRequest
from seldon.infrastructure.ml.encoding import encode_objects
from seldon.infrastructure.ml.forecast import run_forecast
from seldon.infrastructure.ml.loader import LoadedModel
from seldon.infrastructure.ml.validation import (
    normalize_band,
    usable_observations,
    validate_objects,
)


def regime_indicator(
    request: ObjectRequest, capabilities: ModelCapabilities
) -> RegimeIndicator:
    """Place a validated request against the training regime on three axes.

    Sparsity: fewer usable observations than ``min_points``, the training
    selection filter, which kept light curves with at least that many.
    Bands: any usable observation's band or evaluation band whose training
    weight is infinite. Span: the evaluation grid's extent over the training
    time scale, reported and never flagged.

    Args:
        request: A request that passed validation.
        capabilities: The loaded checkpoint's capabilities.

    Returns:
        The request's regime indicator.
    """
    usable = usable_observations(request)
    n_observations = int(usable.sum())
    sparse = n_observations < capabilities.min_points

    observed = np.asarray(request.bands, dtype=object)[usable]
    bands = {normalize_band(b) for b in (*observed, *request.eval_bands)}
    untrained = tuple(sorted(bands & capabilities.untrained_bands))

    eval_times = request.eval_times
    span = float(eval_times.max() - eval_times.min()) if len(eval_times) else 0.0
    return RegimeIndicator(
        in_training_regime=not sparse and not untrained,
        sparse=sparse,
        n_observations=n_observations,
        min_points=capabilities.min_points,
        untrained_bands=untrained,
        eval_span_days=span,
        eval_span_in_time_scales=span / capabilities.time_scale,
        time_scale_days=capabilities.time_scale,
    )


def forecast_objects(
    requests: Sequence[ObjectRequest],
    loaded: LoadedModel,
    capabilities: ModelCapabilities,
    provenance: Provenance,
) -> list[ObjectForecast]:
    """Forecast each object in one batched forward pass.

    Args:
        requests: The objects to forecast; one object is the one-element case.
        loaded: The loaded model.
        capabilities: Its capabilities, from ``read_capabilities``.
        provenance: Its provenance, from ``read_provenance``.

    Returns:
        One forecast per request, in request order.

    Raises:
        seldon.domain.errors.MissingZeroPointError: If a request states no
            zero point.
        seldon.domain.errors.UnknownBandError: If a band is not in the
            checkpoint's vocabulary.
        seldon.domain.errors.NoUsableObservationsError: If an object has no
            usable observation.
    """
    validated = validate_objects(requests, capabilities)
    encoded = encode_objects(
        validated, capabilities, loaded.experiment.data.dataset.transform
    )
    outputs = run_forecast(loaded.experiment, encoded, capabilities.class_map)
    return [
        ObjectForecast(
            eval_times=request.eval_times,
            eval_bands=request.eval_bands,
            flux=output.flux,
            flux_err=output.flux_err,
            zero_point_mag=output.zero_point_mag,
            parameters=output.parameters,
            class_probabilities=output.class_probabilities,
            predicted_class=output.predicted_class,
            latent_mean=output.latent_mean,
            latent_sigma=output.latent_sigma,
            regime=regime_indicator(request, capabilities),
            provenance=provenance,
        )
        for request, output in zip(requests, outputs, strict=True)
    ]
