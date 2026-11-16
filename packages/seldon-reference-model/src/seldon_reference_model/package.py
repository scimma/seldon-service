"""The pinned SELDON checkpoint served through the model contract.

This is the reference implementation of ``seldon_model_contract``: it maps
survey-aware packets onto the checkpoint's bare band names, refuses or
qualifies each object on its own, runs every servable object in one batched
forward pass, and turns that pass into the three products the checkpoint
offers. The latent never leaves the package.

The checkpoint names bands without saying which survey they come from, so
the survey is inferred from the name: single letters are LSST, the eight
Roman WFI filters are Roman, and every other name is a JWST NIRCam filter.
"""

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from importlib import metadata

import numpy as np

from seldon_model_contract import (
    CONTRACT_VERSION,
    Caveat,
    CaveatCode,
    Declaration,
    HeadDeclaration,
    HeadRequest,
    InputKind,
    ObjectResult,
    Outcome,
    OutputKind,
    Packet,
    Point,
    Product,
    Refusal,
    RefusalCode,
    TimeGrid,
)
from seldon_reference_model import runtime
from seldon_reference_model.errors import SeldonRejection
from seldon_reference_model.ml.validation import validate_object
from seldon_reference_model.models.capabilities import ModelCapabilities
from seldon_reference_model.models.forecast import ObjectForecast
from seldon_reference_model.models.provenance import TRAINING_ZERO_POINT_MAG
from seldon_reference_model.models.request import ObjectRequest

DISTRIBUTION = "seldon-reference-model"
FLUX_FORECAST = "flux_forecast"
CLASSIFICATION = "classification"
BASIS_PARAMETERS = "basis_parameters"
LSST = "lsst"
ROMAN = "roman"
JWST = "jwst"
ROMAN_WFI = frozenset({"F062", "F087", "F106", "F129", "F146", "F158", "F184", "F213"})
LSST_BANDS = ("u", "g", "r", "i", "z", "y")
DEFAULT_GRID = TimeGrid(start_days=0.0, days_past_last=60.0, step_days=1.0)

Pair = tuple[str, str]


def survey_of(band: str) -> str:
    """Return the survey a checkpoint band name belongs to.

    Args:
        band: A band name from the checkpoint's vocabulary.

    Returns:
        ``"lsst"``, ``"roman"``, or ``"jwst"``.
    """
    if len(band) == 1:
        return LSST
    return ROMAN if band in ROMAN_WFI else JWST


def accepted_pairs(capabilities: ModelCapabilities) -> dict[Pair, str]:
    """Map each accepted ``(survey, band)`` pair to the checkpoint's name.

    Args:
        capabilities: The checkpoint's capabilities.

    Returns:
        Every band the checkpoint knows, keyed by its survey-qualified pair.
    """
    return {(survey_of(name), name): name for name in capabilities.band_index}


@dataclass
class _Object:
    """One packet's progress through ``predict``."""

    packet: Packet
    excluded: dict[Pair, int] = field(default_factory=dict)
    refusal: Refusal | None = None
    request: ObjectRequest | None = None
    flux_refusal: Refusal | None = None
    grid_requested: bool = False
    forecast: ObjectForecast | None = None


class ReferencePackage:
    """The ``seldon-reference`` model package."""

    def describe(self) -> Declaration:
        """Return the checkpoint's accepted pairs and its three heads."""
        capabilities, _ = runtime.model_description()
        return Declaration(
            package_name=DISTRIBUTION,
            package_version=metadata.version(DISTRIBUTION),
            contract_version=CONTRACT_VERSION,
            accepted=frozenset(accepted_pairs(capabilities)),
            heads=(
                HeadDeclaration(
                    FLUX_FORECAST,
                    OutputKind.FLUX_SERIES,
                    {
                        InputKind.TIMES: DEFAULT_GRID,
                        InputKind.BANDS: tuple((LSST, b) for b in LSST_BANDS),
                    },
                    "Forecast flux and uncertainty at requested times and bands.",
                ),
                HeadDeclaration(
                    CLASSIFICATION,
                    OutputKind.CLASS_PROBABILITIES,
                    description="Probability of each transient class.",
                ),
                HeadDeclaration(
                    BASIS_PARAMETERS,
                    OutputKind.NAMED_VALUES,
                    description="The decoder's interpretable basis parameters.",
                ),
            ),
        )

    def predict(
        self, packets: Sequence[Packet], requests: Sequence[HeadRequest]
    ) -> list[ObjectResult]:
        """Forecast every servable packet in one batch; refuse the rest.

        Args:
            packets: The objects, with unique ids.
            requests: The heads to run, with concrete inputs per object.

        Returns:
            One result per packet, in order, with one outcome per request.
        """
        capabilities, _ = runtime.model_description()
        pairs = accepted_pairs(capabilities)
        flux_request = next((r for r in requests if r.head == FLUX_FORECAST), None)
        objects = [self._prepare(p, pairs, flux_request) for p in packets]
        for obj in objects:
            if obj.request is not None:
                try:
                    validate_object(obj.request, capabilities)
                except SeldonRejection as error:
                    obj.refusal = _refusal(error)
                    obj.request = None
        servable = [obj for obj in objects if obj.request is not None]
        forecasts = runtime.forecast([obj.request for obj in servable])
        for obj, forecast in zip(servable, forecasts, strict=True):
            obj.forecast = forecast
        return [
            ObjectResult(
                obj.packet.object_id,
                {r.head: self._outcome(obj, r.head) for r in requests},
            )
            for obj in objects
        ]

    def _prepare(
        self,
        packet: Packet,
        pairs: Mapping[Pair, str],
        flux_request: HeadRequest | None,
    ) -> _Object:
        """Exclude unaccepted points and build the checkpoint's request."""
        obj = _Object(packet)
        kept = []
        for point in packet.points:
            pair = (point.survey, point.band)
            if pair in pairs:
                kept.append(point)
            else:
                obj.excluded[pair] = obj.excluded.get(pair, 0) + 1
        if not kept:
            obj.refusal = Refusal(
                RefusalCode.UNKNOWN_BAND.value,
                "no point is in a survey and band this package accepts",
                {"pairs": _sorted_pairs(obj.excluded)},
            )
            return obj
        if any(p.zero_point is None for p in kept):
            obj.refusal = Refusal(
                RefusalCode.MISSING_ZERO_POINT.value,
                "a point has no flux zero point",
                {},
            )
            return obj
        zero_points = np.array([p.zero_point for p in kept], dtype=float)
        if not _convertible(zero_points):
            obj.refusal = Refusal(
                RefusalCode.MISSING_ZERO_POINT.value,
                "a point's flux zero point is too far from the training zero "
                "point to convert",
                {},
            )
            return obj
        inputs = flux_request.for_object(packet.object_id) if flux_request else {}
        times = list(inputs.get(InputKind.TIMES, ()))
        bands = [tuple(b) for b in inputs.get(InputKind.BANDS, ())]
        unknown = [b for b in bands if b not in pairs]
        if unknown:
            obj.flux_refusal = Refusal(
                RefusalCode.UNKNOWN_BAND.value,
                "a requested band is not one this package accepts",
                {"pairs": [list(b) for b in unknown]},
            )
        obj.grid_requested = bool(times and bands and not unknown)
        if obj.grid_requested:
            eval_times, eval_bands = _requested_grid(times, bands, pairs)
        else:
            eval_times, eval_bands = _last_observation_grid(kept, pairs)
        obj.request = ObjectRequest(
            times=[p.time for p in kept],
            flux=[p.flux for p in kept],
            flux_err=[p.flux_err for p in kept],
            detected=[p.detected for p in kept],
            bands=[pairs[(p.survey, p.band)] for p in kept],
            eval_times=eval_times,
            eval_bands=eval_bands,
            zero_point=(
                float(zero_points[0])
                if np.all(zero_points == zero_points[0])
                else zero_points
            ),
        )
        return obj

    def _outcome(self, obj: _Object, head: str) -> Outcome:
        """Return one head's outcome for one prepared object."""
        if obj.refusal is not None:
            return obj.refusal
        forecast = obj.forecast
        assert forecast is not None
        caveats = _caveats(obj, forecast, head)
        if head == CLASSIFICATION:
            return Product(
                OutputKind.CLASS_PROBABILITIES,
                {"probabilities": dict(forecast.class_probabilities)},
                caveats,
            )
        if head == BASIS_PARAMETERS:
            return Product(
                OutputKind.NAMED_VALUES,
                {"values": {k: float(v) for k, v in forecast.parameters.items()}},
                caveats,
            )
        if head != FLUX_FORECAST:
            return Refusal(
                RefusalCode.INCOMPATIBLE_CHECKPOINT.value,
                f"this package has no head named {head!r}",
                {},
            )
        if obj.flux_refusal is not None:
            return obj.flux_refusal
        if not obj.grid_requested:
            return Product(
                OutputKind.FLUX_SERIES,
                {
                    "times": [],
                    "bands": [],
                    "flux": [],
                    "flux_err": [],
                    "zero_point": float(forecast.zero_point_mag),
                    "eval_span_days": 0.0,
                    "eval_span_in_time_scales": 0.0,
                },
                caveats,
            )
        regime = forecast.regime
        return Product(
            OutputKind.FLUX_SERIES,
            {
                "times": [float(t) for t in forecast.eval_times],
                "bands": _survey_pairs(forecast.eval_bands),
                "flux": [float(f) for f in forecast.flux],
                "flux_err": [float(f) for f in forecast.flux_err],
                "zero_point": float(forecast.zero_point_mag),
                "eval_span_days": float(regime.eval_span_days),
                "eval_span_in_time_scales": float(regime.eval_span_in_time_scales),
            },
            caveats,
        )


def _requested_grid(
    times: Sequence[float], bands: Sequence[Pair], pairs: Mapping[Pair, str]
) -> tuple[list[float], list[str]]:
    """Return every requested time in every requested band.

    Args:
        times: The requested evaluation times.
        bands: The requested pairs, all of them accepted.
        pairs: The checkpoint's name for each accepted pair.

    Returns:
        The grid's times and the checkpoint's band names, point by point.
    """
    grid = [(t, pairs[b]) for t in times for b in bands]
    return [t for t, _ in grid], [b for _, b in grid]


def _last_observation_grid(
    kept: Sequence[Point], pairs: Mapping[Pair, str]
) -> tuple[list[float], list[str]]:
    """Return a one-point grid at the last kept observation.

    The forward pass needs an evaluation grid, so this stands in when the
    request gives the object no grid or names a band this package refuses.

    Args:
        kept: The object's accepted points.
        pairs: The checkpoint's name for each accepted pair.

    Returns:
        The single time and the checkpoint's band name for it.
    """
    last = max(kept, key=lambda p: p.time)
    return [last.time], [pairs[(last.survey, last.band)]]


def _survey_pairs(bands: Iterable[str]) -> list[list[str]]:
    """Return ``[survey, band]`` for each checkpoint band name."""
    return [[survey_of(b), b] for b in bands]


def _sorted_pairs(pairs: Iterable[Pair]) -> list[list[str]]:
    """Return the pairs as sorted ``[survey, band]`` lists."""
    return sorted([list(p) for p in pairs])


def _convertible(zero_points: np.ndarray) -> bool:
    """Say whether every zero point converts to and from the training one.

    The checkpoint rescales flux by ``10 ** (0.4 * offset)`` in both
    directions, so an offset whose scale factor overflows a float would fail
    the whole batch's forward pass rather than this one object.

    Args:
        zero_points: One AB magnitude zero point per kept point.

    Returns:
        True when every scale factor is finite.
    """
    offsets = np.abs(zero_points - TRAINING_ZERO_POINT_MAG)
    with np.errstate(over="ignore"):
        return bool(np.all(np.isfinite(np.power(10.0, 0.4 * offsets))))


def _refusal(error: SeldonRejection) -> Refusal:
    """Turn the checkpoint's rejection into a contract refusal.

    ``_prepare`` has already excluded every point and requested band the
    checkpoint does not know, so no rejection reaching here names bands.
    """
    return Refusal(error.code.value, str(error), {})


def _caveats(obj: _Object, forecast: ObjectForecast, head: str) -> tuple[Caveat, ...]:
    """Return the caveats that apply to one head's product for one object.

    Only the flux head evaluates at the requested bands, so only its
    ``untrained_band`` caveat counts them; the other heads count observed
    bands alone.

    Args:
        obj: The prepared object.
        forecast: Its forecast.
        head: The head whose product the caveats go on.

    Returns:
        The caveats, in a fixed order.
    """
    regime = forecast.regime
    untrained = regime.untrained_bands
    if head != FLUX_FORECAST or not obj.grid_requested:
        assert obj.request is not None
        observed = set(obj.request.bands)
        untrained = tuple(b for b in untrained if b in observed)
    caveats = []
    if regime.sparse:
        caveats.append(
            Caveat(
                CaveatCode.SPARSE_LIGHT_CURVE.value,
                {
                    "n_observations": regime.n_observations,
                    "min_points": regime.min_points,
                },
            )
        )
    if untrained:
        caveats.append(
            Caveat(
                CaveatCode.UNTRAINED_BAND.value,
                {"bands": _survey_pairs(untrained)},
            )
        )
    if obj.excluded:
        caveats.append(
            Caveat(
                CaveatCode.EXCLUDED_POINTS.value,
                {
                    "pairs": _sorted_pairs(obj.excluded),
                    "n_points": sum(obj.excluded.values()),
                },
            )
        )
    return tuple(caveats)
