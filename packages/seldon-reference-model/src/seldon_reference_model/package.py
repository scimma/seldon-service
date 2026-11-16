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

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from importlib import metadata
from typing import Any

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
    Product,
    Refusal,
    RefusalCode,
    TimeGrid,
)
from seldon_reference_model import runtime
from seldon_reference_model.errors import SeldonRejection, UnknownBandError
from seldon_reference_model.ml.validation import validate_object
from seldon_reference_model.models.capabilities import ModelCapabilities
from seldon_reference_model.models.forecast import ObjectForecast
from seldon_reference_model.models.request import ObjectRequest

DISTRIBUTION = "seldon-reference-model"
FLUX_FORECAST = "flux_forecast"
CLASSIFICATION = "classification"
BASIS_PARAMETERS = "basis_parameters"
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
        return "lsst"
    return "roman" if band in ROMAN_WFI else "jwst"


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
                        InputKind.BANDS: tuple(("lsst", b) for b in LSST_BANDS),
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
                    obj.refusal = _refusal(error, obj.request)
                    obj.request = None
        servable = [obj for obj in objects if obj.request is not None]
        forecasts = runtime.forecast([obj.request for obj in servable])
        for obj, forecast in zip(servable, forecasts, strict=True):
            obj.forecast = forecast
        return [
            ObjectResult(
                obj.packet.object_id,
                {r.head: self._outcome(obj, r.head, capabilities) for r in requests},
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
                {"pairs": sorted([list(p) for p in obj.excluded])},
            )
            return obj
        if any(p.zero_point is None for p in kept):
            obj.refusal = Refusal(
                RefusalCode.MISSING_ZERO_POINT.value,
                "a point has no flux zero point",
                {},
            )
            return obj
        eval_times, eval_bands = self._grid(packet, kept, pairs, flux_request, obj)
        zero_points = np.array([p.zero_point for p in kept], dtype=float)
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

    def _grid(
        self,
        packet: Packet,
        kept: Sequence[Any],
        pairs: Mapping[Pair, str],
        flux_request: HeadRequest | None,
        obj: _Object,
    ) -> tuple[list[float], list[str]]:
        """Return the evaluation grid, from the flux request when it has one."""
        inputs = flux_request.for_object(packet.object_id) if flux_request else {}
        times = list(inputs.get(InputKind.TIMES, ()))
        bands = list(inputs.get(InputKind.BANDS, ()))
        unknown = [list(b) for b in bands if tuple(b) not in pairs]
        if unknown:
            obj.flux_refusal = Refusal(
                RefusalCode.UNKNOWN_BAND.value,
                "a requested band is not one this package accepts",
                {"pairs": unknown},
            )
        if not times or not bands or unknown:
            # The forward pass needs a grid; the flux head is not served
            # from this one, so one point at the last observation suffices.
            last = max(kept, key=lambda p: p.time)
            return [last.time], [pairs[(last.survey, last.band)]]
        grid = [(t, pairs[tuple(b)]) for t in times for b in bands]
        return [t for t, _ in grid], [b for _, b in grid]

    def _outcome(
        self, obj: _Object, head: str, capabilities: ModelCapabilities
    ) -> Outcome:
        """Return one head's outcome for one prepared object."""
        if obj.refusal is not None:
            return obj.refusal
        forecast = obj.forecast
        assert forecast is not None
        caveats = _caveats(obj, forecast)
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
        regime = forecast.regime
        return Product(
            OutputKind.FLUX_SERIES,
            {
                "times": [float(t) for t in forecast.eval_times],
                "bands": [[survey_of(b), b] for b in forecast.eval_bands],
                "flux": [float(f) for f in forecast.flux],
                "flux_err": [float(f) for f in forecast.flux_err],
                "zero_point": float(forecast.zero_point_mag),
                "eval_span_days": float(regime.eval_span_days),
                "eval_span_in_time_scales": float(regime.eval_span_in_time_scales),
            },
            caveats,
        )


def _refusal(error: SeldonRejection, request: ObjectRequest) -> Refusal:
    """Turn the checkpoint's rejection into a contract refusal."""
    detail: dict[str, Any] = {}
    if isinstance(error, UnknownBandError):
        detail["pairs"] = [[survey_of(b), b] for b in error.bands]
    return Refusal(error.code.value, str(error), detail)


def _caveats(obj: _Object, forecast: ObjectForecast) -> tuple[Caveat, ...]:
    """Return the caveats that apply to every product of one object."""
    regime = forecast.regime
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
    if regime.untrained_bands:
        caveats.append(
            Caveat(
                CaveatCode.UNTRAINED_BAND.value,
                {"bands": [[survey_of(b), b] for b in regime.untrained_bands]},
            )
        )
    if obj.excluded:
        caveats.append(
            Caveat(
                CaveatCode.EXCLUDED_POINTS.value,
                {
                    "pairs": sorted([list(p) for p in obj.excluded]),
                    "n_points": sum(obj.excluded.values()),
                },
            )
        )
    return tuple(caveats)
