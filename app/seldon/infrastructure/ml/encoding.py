"""Turn validated photometry into the tensors the model's forward pass reads.

Per object, encoding drops unusable observations, rescales flux from the
caller's zero point to ``TRAINING_ZERO_POINT_MAG``, applies the checkpoint's
own flux normalization, anchors times at the earliest usable observation and
divides by the checkpoint's time scale, and sorts both the observations and
the evaluation grid by time. The evaluation grid is sorted with its own
permutation, and the inverse is recorded so the forecast can be returned in
the caller's order. (The author's notebook sorted the grid with the
observations' permutation, which mispairs times with bands, or fails when the
grid is shorter than the photometry.)

A sequence of objects is zero-padded into one batch. The observation side
carries a mask; the evaluation side carries none, and a padded band index of
0 is a real band, so each object's lengths are recorded for trimming output.

Nothing here imports Django.
"""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np
import torch

from seldon.domain.models.capabilities import ModelCapabilities
from seldon.domain.models.request import TRAINING_ZERO_POINT_MAG, ObjectRequest
from seldon.infrastructure.ml.validation import ValidatedObject, usable_observations

# The value every padded tensor position holds. Golden outputs depend on it.
PAD_VALUE = 0.0

# The forward pass looks a class name up in the class map, but the loader
# refuses a class-conditional decoder, so the name never shapes a forecast.
PLACEHOLDER_CLASS_NAME = "SNIa"

# ``(flux, flux_err) -> (normalized flux, normalized flux_err)``, normally the
# loaded data module's ``experiment.data.dataset.transform``.
FluxTransform = Callable[[np.ndarray, np.ndarray], tuple[np.ndarray, np.ndarray]]


@dataclass(frozen=True)
class EncodedObject:
    """One object's model inputs and what is needed to read its output back.

    Attributes:
        inputs: The unbatched forward-pass fields: ``x_input`` (float32,
            ``[n_observations, 4]``: time, flux, flux error, detection),
            ``time_full`` (float32, ``[n_eval, 1]``), ``band_idx`` (int64,
            ``[n_observations, 1]``), ``band_idx_full`` (int64,
            ``[n_eval, 1]``), ``pad_mask_part`` (bool, ``[n_observations]``),
            and ``class_name`` (str).
        n_observations: Usable observations encoded, after dropping the rest.
        n_eval: Evaluation points, as the caller asked for them.
        eval_restore: Indexing the time-sorted evaluation output with this
            array returns it in the caller's order.
        output_zero_point: The AB magnitude zero point the forecast flux is
            to be returned in. Multiply flux at the training zero point by
            ``10 ** (0.4 * (output_zero_point - TRAINING_ZERO_POINT_MAG))``.
    """

    inputs: dict[str, Any]
    n_observations: int
    n_eval: int
    eval_restore: np.ndarray
    output_zero_point: float


@dataclass(frozen=True)
class EncodedBatch:
    """A batch the forward pass consumes, with each object's record.

    Attributes:
        batch: The forward-pass fields of ``EncodedObject.inputs``, each
            tensor zero-padded along its sequence axis and stacked on a new
            leading batch axis; ``class_name`` is a list of strings.
        objects: Each object's encoding, in request order.
        pad_value: The value padded positions hold.
    """

    batch: dict[str, Any]
    objects: tuple[EncodedObject, ...]
    pad_value: float


def _float32(values: np.ndarray) -> torch.Tensor:
    """Convert an array to a float32 tensor.

    Args:
        values: Numeric or boolean values.

    Returns:
        A new float32 tensor.
    """
    return torch.from_numpy(np.asarray(values, dtype=np.float32))


def _zero_point_factor(zero_point: float | np.ndarray) -> float | np.ndarray:
    """Return the factor that moves flux from a zero point to the training one.

    Args:
        zero_point: AB magnitude zero point, scalar or one per observation.

    Returns:
        ``10 ** (-0.4 * (zero_point - TRAINING_ZERO_POINT_MAG))``.
    """
    return 10 ** (-0.4 * (zero_point - TRAINING_ZERO_POINT_MAG))


def _output_zero_point(request: ObjectRequest) -> float:
    """Choose the one zero point an object's forecast flux is returned in.

    A scalar zero point is the caller's system, so the forecast returns in
    it. A declared training zero point and a per-observation zero point both
    return at the training zero point: the latter states no single system,
    and picking one observation's would be arbitrary.

    Args:
        request: A validated request.

    Returns:
        The AB magnitude zero point for the forecast flux.
    """
    if isinstance(request.zero_point, float):
        return request.zero_point
    return TRAINING_ZERO_POINT_MAG


def encode_object(
    validated: ValidatedObject,
    capabilities: ModelCapabilities,
    flux_transform: FluxTransform,
) -> EncodedObject:
    """Encode one validated object into unbatched forward-pass inputs.

    Args:
        validated: The object, from ``validate_object``.
        capabilities: The loaded checkpoint's capabilities.
        flux_transform: The checkpoint's flux normalization.

    Returns:
        The object's inputs and its record for reading output back.
    """
    request = validated.request
    usable = usable_observations(request)
    times = request.times[usable]
    order = np.argsort(times, kind="stable")

    factor = 1.0
    if isinstance(request.zero_point, (float, np.ndarray)):
        factor = _zero_point_factor(request.zero_point)
    factor = np.broadcast_to(factor, request.flux.shape)[usable][order]
    flux, flux_err = flux_transform(
        request.flux[usable][order] * factor, request.flux_err[usable][order] * factor
    )

    anchor = times[order][0]
    scale = capabilities.time_scale
    eval_order = np.argsort(request.eval_times, kind="stable")
    eval_restore = np.empty_like(eval_order)
    eval_restore[eval_order] = np.arange(len(eval_order))
    eval_restore.setflags(write=False)

    x_input = torch.stack(
        [
            _float32((times[order] - anchor) / scale),
            _float32(flux),
            _float32(flux_err),
            _float32(request.detected[usable][order]),
        ],
        dim=1,
    )
    inputs = {
        "x_input": x_input,
        "time_full": _float32((request.eval_times[eval_order] - anchor) / scale)[
            :, None
        ],
        "band_idx": torch.from_numpy(validated.band_indices[usable][order])[:, None],
        "band_idx_full": torch.from_numpy(validated.eval_band_indices[eval_order])[
            :, None
        ],
        "pad_mask_part": torch.ones(len(times), dtype=torch.bool),
        "class_name": PLACEHOLDER_CLASS_NAME,
    }
    return EncodedObject(
        inputs=inputs,
        n_observations=len(times),
        n_eval=len(eval_order),
        eval_restore=eval_restore,
        output_zero_point=_output_zero_point(request),
    )


def _pad_stack(tensors: list[torch.Tensor]) -> torch.Tensor:
    """Pad tensors along their first axis to the longest and stack them.

    Args:
        tensors: Tensors that differ at most in their first axis.

    Returns:
        A tensor with a new leading batch axis, padded with ``PAD_VALUE``.
    """
    longest = max(t.shape[0] for t in tensors)
    padded = [
        torch.cat([t, t.new_full((longest - t.shape[0], *t.shape[1:]), PAD_VALUE)])
        for t in tensors
    ]
    return torch.stack(padded)


def encode_objects(
    validated: Sequence[ValidatedObject],
    capabilities: ModelCapabilities,
    flux_transform: FluxTransform,
) -> EncodedBatch:
    """Encode a sequence of validated objects into one padded batch.

    A single object is the one-element case. ``pad_mask_part`` is ``True``
    exactly at real observations, since padding a boolean with ``PAD_VALUE``
    gives ``False``.

    Args:
        validated: The objects, from ``validate_objects``.
        capabilities: The loaded checkpoint's capabilities.
        flux_transform: The checkpoint's flux normalization, normally
            ``experiment.data.dataset.transform``.

    Returns:
        The batch and each object's record.
    """
    objects = tuple(encode_object(v, capabilities, flux_transform) for v in validated)
    batch: dict[str, Any] = {}
    for key, value in objects[0].inputs.items():
        values = [o.inputs[key] for o in objects]
        batch[key] = _pad_stack(values) if isinstance(value, torch.Tensor) else values
    return EncodedBatch(batch=batch, objects=objects, pad_value=PAD_VALUE)
