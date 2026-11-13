"""Babamul alerts, from every ``babamul.lsst.*`` topic: the full stream.

Each Kafka message is one Avro object container file. The triggering
detection is ``candidate`` (a Rubin diaSource). Earlier detections in
``prv_candidates`` carry no identifiers and give their time as a UTC Julian
date ``jd``, converted here to Rubin's MJD TAI. ``fp_hists`` (forced
photometry) is left out. Babamul gives no SN-like score.
"""

import io
from collections.abc import Mapping
from typing import Any

import fastavro

from seldon.domain.intake.delivery import Delivery, Point
from seldon.domain.intake.time_scales import MJD_EPOCH_JD, TAI_MINUS_UTC_SECONDS
from seldon.infrastructure.brokers.base import MissingFieldError, ParseContext
from seldon.infrastructure.brokers.parsers import (
    band_name,
    dia_source_point,
    field,
    identifier,
    points,
    rubin_id,
    score,
)


def decode(key: bytes | None, value: bytes) -> list[dict[str, Any]]:
    """Decode one Babamul message, an Avro object container file.

    Args:
        key: Unused.
        value: The container file.

    Returns:
        Every record the container holds (one, in practice).
    """
    return list(fastavro.reader(io.BytesIO(value)))


def jd_utc_to_mjd_tai(jd: float) -> float:
    """Turn a UTC Julian date into an MJD in TAI.

    Args:
        jd: A UTC Julian date.

    Returns:
        The same moment as an MJD in TAI.
    """
    return jd - MJD_EPOCH_JD + TAI_MINUS_UTC_SECONDS / 86_400


def _previous(entry: Mapping[str, Any]) -> Point:
    """A ``prv_candidates`` entry as a point without an identifier.

    Args:
        entry: One earlier detection.

    Returns:
        Its point, timed in MJD TAI.

    Raises:
        MissingFieldError: If a needed field is absent.
    """
    return Point(
        observed_mjd=jd_utc_to_mjd_tai(float(field(entry, "jd"))),
        band=band_name(entry),
        flux=float(field(entry, "psfFlux")),
        flux_err=float(field(entry, "psfFluxErr")),
    )


def parse(alert: Mapping[str, Any], context: ParseContext) -> Delivery:
    """Turn a Babamul alert into a delivery of its candidate.

    Args:
        alert: The decoded alert.
        context: Where and when it arrived.

    Returns:
        The delivery, with no SN-like score.

    Raises:
        MissingFieldError: If a field the delivery needs is absent.
    """
    candidate = field(alert, "candidate")
    try:
        detection = dia_source_point(candidate)
        dia_object_id = rubin_id(candidate, "diaObjectId")
    except MissingFieldError as missing:
        raise MissingFieldError(f"candidate.{missing.field}") from None
    return Delivery(
        dia_object_id=dia_object_id,
        detection=detection,
        previous=points(alert.get("prv_candidates") or (), _previous, "prv_candidates"),
        broker=context.broker,
        role=context.role,
        reliability=score(alert, "candidate.reliability"),
        sn_like_score=None,
        published_at=context.published_at,
        received_at=context.received_at,
    )


def identify(alert: Mapping[str, Any]) -> str:
    """The alert's identifier for an intake skip.

    Args:
        alert: The decoded alert.

    Returns:
        Its ``diaSourceId``, else its ``objectId``, else an empty string.
    """
    return identifier(alert, "candidate.diaSourceId", "candid", "objectId")
