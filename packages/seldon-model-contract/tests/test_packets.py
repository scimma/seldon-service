"""Packets survive JSON round trips and reject malformed points by name."""

import json
import unittest

from seldon_model_contract import Packet


def point(survey: str, band: str, time: float, **overrides: object) -> dict:
    data = {
        "survey": survey,
        "band": band,
        "time": time,
        "flux": 1200.0,
        "flux_err": 35.0,
        "zero_point": 31.4,
    }
    data.update(overrides)
    return data


def mixed_packet() -> dict:
    return {
        "object_id": "obj-1",
        "points": [
            point("lsst", "g", 61000.1),
            point("lsst", "r", 61000.2),
            point("ztf", "g", 61001.0, zero_point=23.9),
        ],
        "metadata": {"ra": 150.1, "dec": -30.2, "broker": "fink"},
    }


class PacketRoundTripTests(unittest.TestCase):
    def test_rubin_and_ztf_points_round_trip_unchanged(self) -> None:
        packet = Packet.from_dict(mixed_packet())
        again = Packet.from_dict(json.loads(json.dumps(packet.to_dict())))
        self.assertEqual(again, packet)
        self.assertEqual(
            [(p.survey, p.band) for p in again.points],
            [("lsst", "g"), ("lsst", "r"), ("ztf", "g")],
        )
        self.assertEqual(again.points[2].zero_point, 23.9)

    def test_unknown_top_level_field_is_ignored(self) -> None:
        data = mixed_packet()
        data["spectra_v2"] = {"anything": [1, 2, 3]}
        packet = Packet.from_dict(data)
        self.assertNotIn("spectra_v2", packet.to_dict())

    def test_extensions_are_kept(self) -> None:
        data = mixed_packet()
        data["extensions"] = {"host_galaxy": {"photo_z": 0.12}}
        packet = Packet.from_dict(data)
        self.assertEqual(packet.to_dict()["extensions"], data["extensions"])


class PointValidationTests(unittest.TestCase):
    def assert_rejected(self, field: str, **overrides: object) -> None:
        data = mixed_packet()
        data["points"][0].update(overrides)
        with self.assertRaises(ValueError) as caught:
            Packet.from_dict(data)
        self.assertIn(field, str(caught.exception))

    def test_missing_or_malformed_fields_are_rejected_by_name(self) -> None:
        cases = [
            ("survey", {"survey": None}),
            ("band", {"band": None}),
            ("time", {"time": None}),
            ("flux", {"flux": None}),
            ("flux", {"flux": float("nan")}),
            ("detected", {"detected": "yes"}),
        ]
        for field, overrides in cases:
            with self.subTest(field=field, overrides=overrides):
                self.assert_rejected(field, **overrides)

    def test_missing_zero_point_loads_as_null(self) -> None:
        data = mixed_packet()
        data["points"][0] = point("lsst", "g", 61000.1, zero_point=None)
        packet = Packet.from_dict(data)
        self.assertIsNone(packet.points[0].zero_point)

    def test_detected_defaults_to_true_and_round_trips_false(self) -> None:
        data = mixed_packet()
        data["points"][1]["detected"] = False
        packet = Packet.from_dict(data)
        self.assertTrue(packet.points[0].detected)
        self.assertFalse(Packet.from_dict(packet.to_dict()).points[1].detected)

    def test_non_scalar_metadata_is_rejected(self) -> None:
        data = mixed_packet()
        data["metadata"]["nested"] = {"a": 1}
        with self.assertRaises(ValueError) as caught:
            Packet.from_dict(data)
        self.assertIn("metadata.nested", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
