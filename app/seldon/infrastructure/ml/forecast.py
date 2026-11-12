"""Run the forward pass on an encoded batch and read each object's output back.

One forward pass yields every output family. Reconstructed flux and its error
come back in physical units at the training zero point, are trimmed to each
object's own evaluation-grid length (the evaluation side of a batch carries
no mask, so a shorter object's tail is another object's padding), restored
to the caller's order, and rescaled to the object's output zero point.

The class prediction is taken from the deterministic latent mean. The
forward pass's ``class_idx`` is not a prediction: it is the placeholder label
encoding supplied, echoed back. The class head may be wider than the
checkpoint's class map (this checkpoint's tenth output, ``KN``, did not
survive the training selection cut); outputs with no name are dropped and
the rest renormalized.

Everything returned is numpy or plain Python. Nothing here imports Django.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

import numpy as np
import torch

from seldon.domain.models.provenance import TRAINING_ZERO_POINT_MAG
from seldon.infrastructure.ml.encoding import EncodedBatch


@dataclass(frozen=True)
class ModelOutput:
    """One object's model outputs, off torch and in the caller's terms.

    Attributes:
        flux: Forecast flux on the caller's evaluation grid, in the caller's
            order, at ``zero_point_mag``.
        flux_err: One-sigma forecast uncertainty, same units as ``flux``.
        zero_point_mag: The AB magnitude zero point of ``flux``.
        parameters: The decoder's basis parameters, by name.
        class_probabilities: Probability of each named class, summing to one.
        predicted_class: The most probable named class.
        latent_mean: The latent mean.
        latent_sigma: The latent standard deviation.
    """

    flux: np.ndarray
    flux_err: np.ndarray
    zero_point_mag: float
    parameters: Mapping[str, float]
    class_probabilities: Mapping[str, float]
    predicted_class: str
    latent_mean: np.ndarray
    latent_sigma: np.ndarray


def _read_only(values: np.ndarray) -> np.ndarray:
    """Return a float64 copy that cannot be written.

    Args:
        values: Numeric values.

    Returns:
        A new read-only float64 array.
    """
    array = np.array(values, dtype=np.float64)
    array.setflags(write=False)
    return array


def named_class_probabilities(
    head_probabilities: np.ndarray, class_map: Mapping[str, int]
) -> list[dict[str, float]]:
    """Keep the head outputs the class map names, renormalized to sum to one.

    The rule compares the head against the map, not a fixed index: a head
    exactly as wide as its map keeps every output unchanged.

    Args:
        head_probabilities: ``[batch, head width]`` probabilities, each row
            summing to one over the full head.
        class_map: Class name to head index, as the checkpoint records it.

    Returns:
        One mapping per row from class name to probability, in head order.
    """
    named = sorted(class_map.items(), key=lambda item: item[1])
    indices = [index for _, index in named]
    kept = np.asarray(head_probabilities, dtype=np.float64)[:, indices]
    kept = kept / kept.sum(axis=1, keepdims=True)
    return [
        {name: float(p) for (name, _), p in zip(named, row, strict=True)}
        for row in kept
    ]


def run_forecast(
    experiment: Any, encoded: EncodedBatch, class_map: Mapping[str, int]
) -> list[ModelOutput]:
    """Run one forward pass and read every object's outputs back.

    Args:
        experiment: The loaded experiment, in eval mode.
        encoded: The batch from ``encode_objects``.
        class_map: Class name to head index, from the checkpoint.

    Returns:
        One output per encoded object, in request order.
    """
    with torch.inference_mode():
        out = experiment(encoded.batch)
        flux, flux_err = experiment.post_process_prediction(
            out["reconstructed"], out["error"]
        )
        head = experiment.model.predict_class(out["z_mean"], probs=True)

    flux = flux[..., 0].double().numpy()
    flux_err = flux_err[..., 0].double().numpy()
    parameters = {
        name: value.double().numpy() for name, value in out["parameters"].items()
    }
    z_mean = out["z_mean"].double().numpy()
    z_sigma = out["z_sigma"].double().numpy()
    probabilities = named_class_probabilities(head.double().numpy(), class_map)

    outputs = []
    for i, obj in enumerate(encoded.objects):
        # Trim to this object's grid before restoring its order.
        restore = obj.eval_restore
        factor = 10 ** (0.4 * (obj.output_zero_point - TRAINING_ZERO_POINT_MAG))
        object_probabilities = probabilities[i]
        outputs.append(
            ModelOutput(
                flux=_read_only(flux[i, : obj.n_eval][restore] * factor),
                flux_err=_read_only(flux_err[i, : obj.n_eval][restore] * factor),
                zero_point_mag=float(obj.output_zero_point),
                parameters=MappingProxyType(
                    {name: float(value[i]) for name, value in parameters.items()}
                ),
                class_probabilities=MappingProxyType(object_probabilities),
                predicted_class=max(
                    object_probabilities, key=object_probabilities.__getitem__
                ),
                latent_mean=_read_only(z_mean[i]),
                latent_sigma=_read_only(z_sigma[i]),
            )
        )
    return outputs
