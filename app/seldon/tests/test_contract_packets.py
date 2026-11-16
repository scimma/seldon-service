"""Tests for turning an intake light curve into a contract packet.

The packet must carry the same measurements a forecast request uses today,
so the expected points come from ``light_curve_points`` rather than being
restated here; what the packet adds is each point's survey, zero point, and
detection flag.
"""

from datetime import datetime, timezone

from django.test import SimpleTestCase
from seldon_model_contract import Packet

from seldon.domain.contract.packets import LSST_SURVEY, build_packet
from seldon.domain.intake.delivery import BrokerRole, Delivery, Point
from seldon.domain.intake.light_curve import LSST_ZERO_POINT, light_curve_points

RECEIVED = datetime(2026, 11, 13, 0, 5, tzinfo=timezone.utc)
DETECTION_ID = 3_068_394_473_225_158_659


def _delivery() -> Delivery:
    """A delivery with mixed-case bands, a duplicate, and a later point.

    Returns:
        The delivery of diaSource ``DETECTION_ID``.
    """
    detection = Point(
        observed_mjd=61010.25,
        band="R",
        flux=1500.0,
        flux_err=30.0,
        dia_source_id=DETECTION_ID,
    )
    previous = (
        Point(observed_mjd=61000.5, band="g", flux=900.0, flux_err=20.0),
        Point(
            observed_mjd=61000.5,
            band="G",
            flux=905.0,
            flux_err=21.0,
            dia_source_id=11,
        ),
        Point(observed_mjd=61004.0, band="i", flux=1200.0, flux_err=25.0),
        Point(observed_mjd=61020.0, band="r", flux=1800.0, flux_err=40.0),
    )
    return Delivery(
        dia_object_id=170019696011640884,
        detection=detection,
        previous=previous,
        broker="fink",
        role=BrokerRole.QUALIFYING,
        reliability=0.9,
        sn_like_score=0.8,
        published_at=None,
        received_at=RECEIVED,
        schema_version="lsst.v10_0",
    )


class BuildPacketTests(SimpleTestCase):
    """A delivery's light curve becomes a survey-aware packet."""

    def test_object_id_is_the_detections_dia_source_id(self) -> None:
        """The packet is keyed by the detection it is built for."""
        self.assertEqual(build_packet(_delivery()).object_id, str(DETECTION_ID))

    def test_points_are_the_light_curve_points(self) -> None:
        """The packet holds today's light curve: same times, bands, fluxes."""
        delivery = _delivery()
        expected = light_curve_points(delivery)
        packet = build_packet(delivery)
        self.assertEqual(
            [(p.time, p.band, p.flux, p.flux_err) for p in packet.points],
            [(p.observed_mjd, p.band, p.flux, p.flux_err) for p in expected],
        )

    def test_every_point_is_an_lsst_detection_at_the_lsst_zero_point(self) -> None:
        """Survey, zero point, and detection flag are set on every point."""
        packet = build_packet(_delivery())
        self.assertEqual(LSST_SURVEY, "lsst")
        self.assertTrue(packet.points)
        for point in packet.points:
            self.assertEqual(point.survey, "lsst")
            self.assertEqual(point.zero_point, LSST_ZERO_POINT)
            self.assertEqual(point.zero_point, 31.4)
            self.assertIs(point.detected, True)

    def test_packet_round_trips_through_its_json_form(self) -> None:
        """The packet validates as a contract packet."""
        packet = build_packet(_delivery())
        self.assertEqual(Packet.from_dict(packet.to_dict()), packet)
