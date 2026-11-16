"""The reference package through the model contract's describe and predict.

The golden fixtures are sent as contract packets, all three in one batch, and
must reproduce the recorded forecast and class probabilities at the existing
tolerance. The other tests pin how the package excludes, refuses, and
qualifies objects one at a time.
"""

import unittest

import numpy as np
from fixtures.reference_light_curves import reference_light_curves
from fixtures.reference_outputs import REFERENCE_OUTPUTS
from seldon_model_contract import (
    HeadRequest,
    InputKind,
    Packet,
    Point,
    Product,
    Refusal,
    load_package,
)

from seldon_reference_model.models.provenance import TRAINING_ZERO_POINT_MAG
from seldon_reference_model.models.request import ObjectRequest, ZeroPointDeclaration
from seldon_reference_model.package import ReferencePackage, survey_of

RELATIVE_TOLERANCE = 1e-4
PROBABILITY_TOLERANCE = 1e-4
HEADS = ("flux_forecast", "classification", "basis_parameters")


def packet_from(name: str, request: ObjectRequest) -> Packet:
    """Turn a golden-fixture request into the equivalent contract packet."""
    zero_point = request.zero_point
    if zero_point is ZeroPointDeclaration.AT_TRAINING_ZERO_POINT:
        zero_point = TRAINING_ZERO_POINT_MAG
    return Packet(
        name,
        tuple(
            Point(
                survey=survey_of(band),
                band=band,
                time=float(t),
                flux=float(f),
                flux_err=float(e),
                zero_point=float(zero_point),
                detected=bool(d),
            )
            for t, f, e, d, band in zip(
                request.times,
                request.flux,
                request.flux_err,
                request.detected,
                request.bands,
            )
        ),
    )


def lsst_packet(object_id: str, n: int, extra: tuple[Point, ...] = ()) -> Packet:
    """A smooth LSST light curve of ``n`` points at the Rubin zero point."""
    points = tuple(
        Point(
            "lsst",
            "gri"[k % 3],
            61000.0 + 1.5 * k,
            30000.0 * np.exp(-((k - 10) ** 2) / 60.0) + 200.0,
            150.0,
            31.4,
        )
        for k in range(n)
    )
    return Packet(object_id, points + extra)


def requests(packets: list[Packet], times: tuple[float, ...] = (61010.0,)) -> list:
    bands = (("lsst", "g"), ("lsst", "r"))
    return [
        HeadRequest(
            "flux_forecast",
            {
                p.object_id: {InputKind.TIMES: times, InputKind.BANDS: bands}
                for p in packets
            },
        ),
        HeadRequest("classification"),
        HeadRequest("basis_parameters"),
    ]


class GoldenOutputTests(unittest.TestCase):
    """The recorded forecasts, reproduced through the contract."""

    @classmethod
    def setUpClass(cls) -> None:
        fixtures = reference_light_curves()
        packets = [packet_from(name, r) for name, r in fixtures.items()]
        head_requests = [
            HeadRequest(
                "flux_forecast",
                {
                    name: {
                        InputKind.TIMES: tuple(float(t) for t in r.eval_times),
                        InputKind.BANDS: tuple((survey_of(b), b) for b in r.eval_bands),
                    }
                    for name, r in fixtures.items()
                },
            ),
            HeadRequest("classification"),
        ]
        cls.fixtures = fixtures
        results = ReferencePackage().predict(packets, head_requests)
        cls.results = {r.object_id: r for r in results}

    def test_flux_matches_the_recorded_values_at_each_fixture_point(self) -> None:
        for name, request in self.fixtures.items():
            product = self.results[name].outcomes["flux_forecast"]
            self.assertIsInstance(product, Product, name)
            payload = product.payload
            index = {
                (t, b[1]): i
                for i, (t, b) in enumerate(zip(payload["times"], payload["bands"]))
            }
            expected = REFERENCE_OUTPUTS[name]
            for k, (t, band) in enumerate(zip(request.eval_times, request.eval_bands)):
                i = index[(float(t), band)]
                with self.subTest(fixture=name, time=float(t), band=band):
                    np.testing.assert_allclose(
                        payload["flux"][i], expected["flux"][k], rtol=RELATIVE_TOLERANCE
                    )
                    np.testing.assert_allclose(
                        payload["flux_err"][i],
                        expected["flux_err"][k],
                        rtol=RELATIVE_TOLERANCE,
                    )

    def test_class_probabilities_match_the_recorded_values(self) -> None:
        for name in self.fixtures:
            product = self.results[name].outcomes["classification"]
            expected = REFERENCE_OUTPUTS[name]["class_probabilities"]
            with self.subTest(fixture=name):
                self.assertEqual(set(product.payload["probabilities"]), set(expected))
                for label, value in expected.items():
                    self.assertAlmostEqual(
                        product.payload["probabilities"][label],
                        value,
                        delta=PROBABILITY_TOLERANCE,
                    )


class ObjectHandlingTests(unittest.TestCase):
    """Exclusion, refusal, and caveats, one object at a time."""

    def predict(self, packets: list[Packet]) -> dict:
        results = ReferencePackage().predict(packets, requests(packets))
        return {r.object_id: r for r in results}

    def test_ztf_points_are_excluded_and_do_not_change_the_forecast(self) -> None:
        ztf = tuple(
            Point("ztf", "g", 61002.0 + 2 * k, 90000.0, 300.0, 25.0) for k in range(4)
        )
        results = self.predict(
            [lsst_packet("clean", 40), lsst_packet("mixed", 40, ztf)]
        )
        clean = results["clean"].outcomes["flux_forecast"]
        mixed = results["mixed"].outcomes["flux_forecast"]
        excluded = [c for c in mixed.caveats if c.code == "excluded_points"]
        self.assertEqual(len(excluded), 1)
        self.assertEqual(
            excluded[0].to_dict()["detail"], {"pairs": [["ztf", "g"]], "n_points": 4}
        )
        np.testing.assert_allclose(
            mixed.payload["flux"], clean.payload["flux"], rtol=RELATIVE_TOLERANCE
        )

    def test_object_with_only_ztf_points_is_refused_naming_them(self) -> None:
        ztf = Packet(
            "ztf",
            tuple(Point("ztf", "r", 61000.0 + k, 100.0, 5.0, 25.0) for k in range(5)),
        )
        outcomes = self.predict([ztf])["ztf"].outcomes
        for head in HEADS:
            with self.subTest(head=head):
                self.assertIsInstance(outcomes[head], Refusal)
                self.assertEqual(outcomes[head].code, "unknown_band")
                self.assertEqual(
                    outcomes[head].to_dict()["detail"], {"pairs": [["ztf", "r"]]}
                )

    def test_one_bad_object_is_refused_and_the_others_are_served(self) -> None:
        bad = lsst_packet("no-zero-point", 40)
        bad = Packet(
            bad.object_id,
            (Point("lsst", "g", 61000.0, 10.0, 1.0, None),) + bad.points[1:],
        )
        results = self.predict([lsst_packet("a", 40), bad, lsst_packet("b", 40)])
        self.assertEqual(
            results["no-zero-point"].outcomes["classification"].code,
            "missing_zero_point",
        )
        for name in ("a", "b"):
            self.assertIsInstance(results[name].outcomes["flux_forecast"], Product)

    def test_sparse_light_curve_is_served_with_a_caveat(self) -> None:
        product = self.predict([lsst_packet("sparse", 6)])["sparse"].outcomes[
            "flux_forecast"
        ]
        sparse = [c for c in product.caveats if c.code == "sparse_light_curve"]
        self.assertEqual(len(sparse), 1)
        self.assertEqual(sparse[0].detail["n_observations"], 6)
        self.assertEqual(sparse[0].detail["min_points"], 32)

    def test_no_product_carries_the_latent(self) -> None:
        outcomes = self.predict([lsst_packet("x", 40)])["x"].outcomes
        for head in HEADS:
            text = str(outcomes[head].to_dict())
            self.assertNotIn("latent", text)

    def test_requested_band_outside_the_vocabulary_refuses_only_the_flux_head(
        self,
    ) -> None:
        packet = lsst_packet("x", 40)
        head_requests = requests([packet])
        head_requests[0] = HeadRequest(
            "flux_forecast",
            {"x": {InputKind.TIMES: (61010.0,), InputKind.BANDS: (("ztf", "g"),)}},
        )
        outcomes = {
            r.object_id: r for r in ReferencePackage().predict([packet], head_requests)
        }["x"].outcomes
        self.assertEqual(outcomes["flux_forecast"].code, "unknown_band")
        self.assertIsInstance(outcomes["classification"], Product)


class EntryPointTests(unittest.TestCase):
    def test_package_is_installed_under_its_entry_point(self) -> None:
        package = load_package("seldon-reference")
        declaration = package.describe()
        self.assertEqual(declaration.package_name, "seldon-reference-model")
        self.assertIn(("lsst", "g"), declaration.accepted)
        self.assertIn(("roman", "F062"), declaration.accepted)
        self.assertNotIn(("ztf", "g"), declaration.accepted)
        self.assertEqual([h.name for h in declaration.heads], list(HEADS))


if __name__ == "__main__":
    unittest.main()
