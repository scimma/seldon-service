"""Tests for resolving a head's input templates per object.

The default flux-forecast template must reproduce the grid alert forecasts
use today, so it is compared against ``forecast_grid`` for the same light
curve rather than against restated numbers.
"""

from django.test import SimpleTestCase
from seldon_model_contract import (
    HeadDeclaration,
    HeadRequest,
    InputKind,
    Packet,
    Point,
    TimeGrid,
)

from seldon.domain.contract.inputs import resolve_inputs
from seldon.domain.intake.grid import GRID_BANDS, forecast_grid

LSST_BANDS = tuple(("lsst", band) for band in "ugrizy")

FLUX_FORECAST = HeadDeclaration(
    name="flux_forecast",
    output_kind="flux_series",
    inputs={
        InputKind.TIMES: TimeGrid(start_days=0, days_past_last=60, step_days=1),
        InputKind.BANDS: LSST_BANDS,
    },
)


def _packet(object_id: str, times: list[float]) -> Packet:
    """A packet of LSST r points at the given times.

    Args:
        object_id: The packet's object id.
        times: Observation times, MJD TAI, in any order.

    Returns:
        The packet.
    """
    return Packet(
        object_id=object_id,
        points=tuple(
            Point(
                survey="lsst",
                band="r",
                time=t,
                flux=1000.0,
                flux_err=25.0,
                zero_point=31.4,
            )
            for t in times
        ),
    )


class ResolveInputsTests(SimpleTestCase):
    """Templates become concrete per-object inputs."""

    def test_default_template_reproduces_todays_grid(self) -> None:
        """Times x bands, band by band, equals ``forecast_grid``'s output."""
        times = [61004.0, 61000.5, 61010.25]
        request = resolve_inputs(FLUX_FORECAST, {}, [_packet("1", times)])
        inputs = request.for_object("1")
        eval_times, eval_bands = forecast_grid(min(times), max(times))
        self.assertEqual(
            [t for _ in inputs[InputKind.BANDS] for t in inputs[InputKind.TIMES]],
            eval_times,
        )
        self.assertEqual(
            [b for _, b in inputs[InputKind.BANDS] for _ in inputs[InputKind.TIMES]],
            eval_bands,
        )
        self.assertEqual(
            tuple(b for _, b in inputs[InputKind.BANDS]), tuple(GRID_BANDS)
        )

    def test_objects_with_different_last_observations_get_different_grids(
        self,
    ) -> None:
        """One template resolves against each packet's own observations."""
        request = resolve_inputs(
            FLUX_FORECAST,
            {},
            [_packet("1", [61000.0, 61005.0]), _packet("2", [61000.0, 61030.0])],
        )
        short = request.for_object("1")[InputKind.TIMES]
        long = request.for_object("2")[InputKind.TIMES]
        self.assertEqual(short[-1], 61065.0)
        self.assertEqual(long[-1], 61090.0)
        self.assertEqual(len(long) - len(short), 25)
        self.assertEqual(request.head, "flux_forecast")

    def test_revision_inputs_override_defaults_per_kind(self) -> None:
        """A kind the revision sets replaces the default; others stay."""
        revision = {"times": {"start_days": -2, "days_past_last": 5, "step_days": 0.5}}
        request = resolve_inputs(
            FLUX_FORECAST, revision, [_packet("1", [61000.0, 61001.0])]
        )
        inputs = request.for_object("1")
        self.assertEqual(inputs[InputKind.TIMES][0], 60998.0)
        self.assertEqual(inputs[InputKind.TIMES][-1], 61006.0)
        self.assertEqual(len(inputs[InputKind.TIMES]), 17)
        self.assertEqual(inputs[InputKind.BANDS], LSST_BANDS)

        revision = {"bands": [["lsst", "g"], ["ztf", "r"]]}
        request = resolve_inputs(
            FLUX_FORECAST, revision, [_packet("1", [61000.0, 61001.0])]
        )
        inputs = request.for_object("1")
        self.assertEqual(inputs[InputKind.BANDS], (("lsst", "g"), ("ztf", "r")))
        self.assertEqual(len(inputs[InputKind.TIMES]), 62)

    def test_revision_input_the_head_does_not_take_is_rejected(self) -> None:
        """An input the head never declared cannot be set on it."""
        classification = HeadDeclaration(
            name="classification", output_kind="class_probabilities"
        )
        with self.assertRaises(ValueError):
            resolve_inputs(
                classification, {"bands": [["lsst", "g"]]}, [_packet("1", [61000.0])]
            )

    def test_head_without_inputs_gets_empty_inputs(self) -> None:
        """A head that declares no inputs needs only the packet."""
        classification = HeadDeclaration(
            name="classification", output_kind="class_probabilities"
        )
        request = resolve_inputs(classification, {}, [_packet("1", [61000.0])])
        self.assertEqual(dict(request.for_object("1")), {})

    def test_request_round_trips_through_its_json_form(self) -> None:
        """The resolved request validates as a contract head request."""
        request = resolve_inputs(FLUX_FORECAST, {}, [_packet("1", [61000.0, 61003.5])])
        self.assertEqual(HeadRequest.from_dict(request.to_dict()), request)
