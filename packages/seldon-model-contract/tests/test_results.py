"""Results, declarations, and head requests survive JSON round trips."""

import json
import unittest

from seldon_model_contract import (
    Caveat,
    Declaration,
    HeadDeclaration,
    HeadRequest,
    InputKind,
    ObjectResult,
    OutputKind,
    Product,
    Refusal,
    TimeGrid,
)


def round_trip(data: dict) -> dict:
    return json.loads(json.dumps(data))


class ObjectResultTests(unittest.TestCase):
    def test_product_refusal_and_caveats_round_trip(self) -> None:
        result = ObjectResult(
            object_id="obj-1",
            outcomes={
                "flux_forecast": Product(
                    kind=OutputKind.FLUX_SERIES,
                    payload={
                        "times": [61000.0, 61001.0],
                        "bands": [["lsst", "g"], ["lsst", "r"]],
                        "flux": [1.0, 2.0],
                        "flux_err": [0.1, 0.2],
                        "zero_point": 27.5,
                    },
                    caveats=(
                        Caveat(
                            "sparse_light_curve",
                            {"n_observations": 3, "min_points": 5},
                        ),
                    ),
                ),
                "redshift": Refusal("unknown_band", "no accepted band", {"pairs": []}),
            },
        )
        again = ObjectResult.from_dict(round_trip(result.to_dict()))
        self.assertEqual(again.to_dict(), result.to_dict())
        self.assertEqual(
            again.outcomes["flux_forecast"].caveats[0].code, "sparse_light_curve"
        )
        self.assertIsInstance(again.outcomes["redshift"], Refusal)

    def test_unknown_codes_load_and_report_unknown(self) -> None:
        caveat = Caveat.from_dict({"code": "brand_new_caveat"})
        refusal = Refusal("brand_new_refusal", "")
        self.assertFalse(caveat.is_known)
        self.assertFalse(refusal.is_known)
        self.assertTrue(Caveat("excluded_points").is_known)
        self.assertTrue(Refusal("unknown_band", "").is_known)

    def test_misaligned_flux_series_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            Product(
                kind=OutputKind.FLUX_SERIES,
                payload={
                    "times": [1.0, 2.0],
                    "bands": [["lsst", "g"]],
                    "flux": [1.0, 2.0],
                    "flux_err": [0.1, 0.2],
                    "zero_point": 27.5,
                },
            )

    def test_class_probabilities_need_numbers(self) -> None:
        with self.assertRaises(ValueError):
            Product(OutputKind.CLASS_PROBABILITIES, {"probabilities": {"SNIa": "high"}})


class DeclarationTests(unittest.TestCase):
    def declaration(self) -> Declaration:
        return Declaration(
            package_name="stub",
            package_version="0.1.0",
            contract_version="1.0",
            accepted=frozenset({("lsst", "g"), ("lsst", "r")}),
            heads=(
                HeadDeclaration(
                    "flux_forecast",
                    OutputKind.FLUX_SERIES,
                    {
                        InputKind.TIMES: TimeGrid(0.0, 60.0, 1.0),
                        InputKind.BANDS: (("lsst", "g"), ("lsst", "r")),
                    },
                ),
                HeadDeclaration("classification", OutputKind.CLASS_PROBABILITIES),
            ),
        )

    def test_declaration_round_trips(self) -> None:
        declaration = self.declaration()
        again = Declaration.from_dict(round_trip(declaration.to_dict()))
        self.assertEqual(again.to_dict(), declaration.to_dict())
        self.assertEqual(
            again.head("flux_forecast").inputs[InputKind.TIMES].step_days, 1.0
        )

    def test_duplicate_head_names_are_rejected(self) -> None:
        data = self.declaration().to_dict()
        data["heads"].append(data["heads"][0])
        with self.assertRaises(ValueError):
            Declaration.from_dict(data)

    def test_unknown_output_kind_is_rejected(self) -> None:
        data = self.declaration().to_dict()
        data["heads"][1]["output_kind"] = "hologram"
        with self.assertRaises(ValueError):
            Declaration.from_dict(data)


class HeadRequestTests(unittest.TestCase):
    def test_request_round_trips_with_per_object_inputs(self) -> None:
        request = HeadRequest(
            "flux_forecast",
            {
                "a": {InputKind.TIMES: (1.0, 2.0), InputKind.BANDS: (("lsst", "g"),)},
                "b": {InputKind.TIMES: (5.0,), InputKind.BANDS: (("lsst", "r"),)},
            },
        )
        again = HeadRequest.from_dict(round_trip(request.to_dict()))
        self.assertEqual(again.to_dict(), request.to_dict())
        self.assertEqual(again.for_object("b")[InputKind.TIMES], (5.0,))
        self.assertEqual(dict(again.for_object("missing")), {})


class TimeGridTests(unittest.TestCase):
    def test_grid_runs_from_first_observation_to_days_past_last(self) -> None:
        grid = TimeGrid(start_days=0.0, days_past_last=3.0, step_days=1.0)
        self.assertEqual(
            grid.resolve(100.0, 101.5), (100.0, 101.0, 102.0, 103.0, 104.0)
        )

    def test_two_objects_get_different_grids(self) -> None:
        grid = TimeGrid(0.0, 2.0, 1.0)
        self.assertNotEqual(grid.resolve(10.0, 11.0), grid.resolve(10.0, 15.0))

    def test_non_positive_step_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            TimeGrid(0.0, 1.0, 0.0)


if __name__ == "__main__":
    unittest.main()
