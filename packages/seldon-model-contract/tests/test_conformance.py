"""The conformance kit passes a conforming stub and fails each broken one."""

import io
import unittest
from collections.abc import Sequence
from contextlib import redirect_stderr, redirect_stdout
from unittest import mock

from seldon_model_contract import (
    Caveat,
    Declaration,
    HeadDeclaration,
    HeadRequest,
    InputKind,
    ObjectResult,
    OutputKind,
    Packet,
    Product,
    Refusal,
    TimeGrid,
)
from seldon_model_contract.conformance import check_package
from seldon_model_contract.conformance.__main__ import main

ACCEPTED = frozenset(
    {("lsst", b) for b in "ugrizy"} | {("roman", b) for b in ("F062", "F106", "F158")}
)


class ConformingStub:
    """Accepts LSST and Roman points; forecasts flux as a smooth function of them."""

    def describe(self) -> Declaration:
        return Declaration(
            package_name="conforming-stub",
            package_version="0.1.0",
            contract_version="1.0",
            accepted=ACCEPTED,
            heads=(
                HeadDeclaration(
                    "flux_forecast",
                    OutputKind.FLUX_SERIES,
                    {
                        InputKind.TIMES: TimeGrid(0.0, 10.0, 5.0),
                        InputKind.BANDS: (("lsst", "g"), ("lsst", "r")),
                    },
                ),
                HeadDeclaration("classification", OutputKind.CLASS_PROBABILITIES),
            ),
        )

    def accepted_points(self, packet: Packet) -> list:
        return [p for p in packet.points if (p.survey, p.band) in ACCEPTED]

    def level(self, packet: Packet) -> float:
        points = self.accepted_points(packet)
        return sum(p.flux * 10 ** (-0.4 * (p.zero_point - 27.5)) for p in points) / len(
            points
        )

    def caveats(self, packet: Packet) -> tuple[Caveat, ...]:
        foreign = sorted(
            {(p.survey, p.band) for p in packet.points} - ACCEPTED, key=str
        )
        if not foreign:
            return ()
        n = sum((p.survey, p.band) not in ACCEPTED for p in packet.points)
        return (
            Caveat(
                "excluded_points", {"pairs": [list(p) for p in foreign], "n_points": n}
            ),
        )

    def outcome(self, packet: Packet, request: HeadRequest, batch_size: int):
        if not self.accepted_points(packet):
            return Refusal("unknown_band", "no accepted points", {"pairs": []})
        level = self.level(packet)
        if request.head == "classification":
            return Product(
                OutputKind.CLASS_PROBABILITIES,
                {"probabilities": {"SNIa": 0.7, "SNII": 0.3}},
                self.caveats(packet),
            )
        inputs = request.for_object(packet.object_id)
        times = inputs[InputKind.TIMES]
        bands = inputs[InputKind.BANDS]
        grid = [(t, b) for t in times for b in bands]
        return Product(
            OutputKind.FLUX_SERIES,
            {
                "times": [t for t, _ in grid],
                "bands": [list(b) for _, b in grid],
                "flux": [level for _ in grid],
                "flux_err": [0.1 * level for _ in grid],
                "zero_point": 27.5,
            },
            self.caveats(packet),
        )

    def predict(
        self, packets: Sequence[Packet], requests: Sequence[HeadRequest]
    ) -> list[ObjectResult]:
        return [
            ObjectResult(
                p.object_id,
                {r.head: self.outcome(p, r, len(packets)) for r in requests},
            )
            for p in packets
        ]


class ZtfAsLsstStub(ConformingStub):
    """Reads every point as if its band were LSST, ignoring the survey."""

    def accepted_points(self, packet: Packet) -> list:
        return [p for p in packet.points if ("lsst", p.band) in ACCEPTED]

    def caveats(self, packet: Packet) -> tuple[Caveat, ...]:
        return ()


class UnknownCaveatStub(ConformingStub):
    def caveats(self, packet: Packet) -> tuple[Caveat, ...]:
        return (Caveat("made_up_caveat"),) + super().caveats(packet)


class BatchDependentStub(ConformingStub):
    def outcome(self, packet: Packet, request: HeadRequest, batch_size: int):
        outcome = super().outcome(packet, request, batch_size)
        if (
            isinstance(outcome, Product)
            and outcome.kind is OutputKind.CLASS_PROBABILITIES
        ):
            share = 1.0 / (1 + batch_size)
            return Product(
                outcome.kind,
                {"probabilities": {"SNIa": 1 - share, "SNII": share}},
                outcome.caveats,
            )
        return outcome


class MissingHeadStub(ConformingStub):
    def predict(self, packets, requests):  # noqa: ANN001, ANN201
        results = super().predict(packets, requests)
        first = results[0]
        results[0] = ObjectResult(
            first.object_id,
            {h: o for h, o in first.outcomes.items() if h != "classification"},
        )
        return results


def failed_checks(package: object) -> set[str]:
    return {
        r.name
        for r in check_package(package, "stub", max_batch=12).results
        if not r.passed
    }


class ConformanceKitTests(unittest.TestCase):
    def test_conforming_stub_passes_every_check(self) -> None:
        report = check_package(ConformingStub(), "stub", max_batch=12)
        self.assertTrue(report.passed, report.render())
        self.assertEqual(
            [r.name for r in report.results],
            [
                "declaration",
                "completeness",
                "vocabulary",
                "exclusion",
                "batch_independence",
                "json_round_trip",
                "determinism",
            ],
        )

    def test_reading_ztf_g_as_lsst_g_fails_exclusion(self) -> None:
        self.assertIn("exclusion", failed_checks(ZtfAsLsstStub()))

    def test_unknown_caveat_code_fails_vocabulary(self) -> None:
        self.assertEqual(failed_checks(UnknownCaveatStub()), {"vocabulary"})

    def test_batch_dependent_output_fails_batch_independence(self) -> None:
        self.assertEqual(failed_checks(BatchDependentStub()), {"batch_independence"})

    def test_omitted_head_fails_completeness(self) -> None:
        self.assertIn("completeness", failed_checks(MissingHeadStub()))

    def test_unsupported_contract_major_stops_at_the_declaration(self) -> None:
        stub = ConformingStub()
        declaration = stub.describe()
        stub.describe = lambda: Declaration(  # type: ignore[method-assign]
            declaration.package_name,
            declaration.package_version,
            "2.0",
            declaration.accepted,
            declaration.heads,
        )
        report = check_package(stub, "stub")
        self.assertEqual(
            [(r.name, r.passed) for r in report.results], [("declaration", False)]
        )


class CommandLineTests(unittest.TestCase):
    def test_exit_codes_follow_the_report(self) -> None:
        with mock.patch(
            "seldon_model_contract.conformance.__main__.load_package",
            side_effect=[ConformingStub(), UnknownCaveatStub()],
        ):
            with redirect_stdout(io.StringIO()) as out:
                self.assertEqual(main(["good", "--max-batch", "8"]), 0)
                self.assertEqual(main(["bad", "--max-batch", "8"]), 1)
        self.assertIn("FAIL  vocabulary", out.getvalue())

    def test_unloadable_package_exits_two(self) -> None:
        with redirect_stderr(io.StringIO()) as err:
            self.assertEqual(main(["no-such-package"]), 2)
        self.assertIn("no-such-package", err.getvalue())


if __name__ == "__main__":
    unittest.main()
