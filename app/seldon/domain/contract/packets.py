"""Turn the light curve a detection is owed a product for into a packet.

A package must see exactly the measurements today's forecast request is made
from, so the points come from ``light_curve_points`` and are not selected
again here. What the contract adds is each point's identity: every Rubin
measurement is an LSST detection in nJy, so it carries survey ``lsst`` and
the LSST zero point, and a package can tell it from another survey's point
in the same band. The span guard on the light curve stays with intake, which
records it as a skip before any packet is built.
"""

from seldon_model_contract import Packet, Point

from seldon.domain.intake.delivery import Delivery
from seldon.domain.intake.light_curve import LSST_ZERO_POINT, light_curve_points

#: The contract's survey name for every Rubin measurement.
LSST_SURVEY = "lsst"


def build_packet(delivery: Delivery) -> Packet:
    """Build the contract packet for a delivery's detection.

    Args:
        delivery: The detection's earliest-received delivery, as intake
            rebuilt it from storage.

    Returns:
        A packet keyed by the detection's ``diaSourceId``, holding its light
        curve as LSST detections at the LSST zero point, sorted by time.
    """
    return Packet(
        object_id=str(delivery.dia_source_id),
        points=tuple(
            Point(
                survey=LSST_SURVEY,
                band=point.band,
                time=point.observed_mjd,
                flux=point.flux,
                flux_err=point.flux_err,
                zero_point=LSST_ZERO_POINT,
                detected=True,
            )
            for point in light_curve_points(delivery)
        ),
    )
