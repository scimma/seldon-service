"""One broker's delivery of one Rubin detection, in a broker-neutral form.

Every broker adapter turns its own message format into a ``Delivery``, so the
intake engine never sees a broker's format.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class BrokerRole(Enum):
    """How a broker's deliveries are used."""

    QUALIFYING = "qualifying"
    """Its scores can qualify a detection."""

    FULL_STREAM = "full_stream"
    """It supplies detections, including later ones, but never qualifies one."""


@dataclass(frozen=True)
class Point:
    """One Rubin measurement as a broker sent it.

    Attributes:
        observed_mjd: Observation time, as an MJD in TAI.
        band: Band name as the broker sent it.
        flux: Difference-image PSF flux, in nJy.
        flux_err: One-sigma uncertainty of ``flux``, in nJy.
        dia_source_id: Rubin's ``diaSourceId``, when the broker sent it.
    """

    observed_mjd: float
    band: str
    flux: float
    flux_err: float
    dia_source_id: int | None = None


@dataclass(frozen=True)
class Delivery:
    """One broker's delivery of one detection, with what its alert carried.

    Attributes:
        dia_object_id: Rubin's ``diaObjectId``.
        detection: The detection the alert is about; its ``dia_source_id`` is
            always set.
        previous: Earlier measurements the alert carried, as sent.
        broker: The configured broker name.
        role: The broker's configured role.
        reliability: The broker's reliability score for the detection.
        sn_like_score: The broker's SN-like score for the detection, when it
            gave one.
        published_at: When the broker published the alert, when known.
        received_at: When the service received it.
        schema_version: The broker's alert schema version.
        classifier_version: The broker's classifier version, when it gave one.
    """

    dia_object_id: int
    detection: Point
    previous: tuple[Point, ...]
    broker: str
    role: BrokerRole
    reliability: float | None
    sn_like_score: float | None
    published_at: datetime | None
    received_at: datetime
    schema_version: str = ""
    classifier_version: str = ""

    @property
    def dia_source_id(self) -> int:
        """The detection's ``diaSourceId``.

        Returns:
            The identifier of the detection this delivery is about.
        """
        return self.detection.dia_source_id
