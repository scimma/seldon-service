"""Messages from the service's own Lasair filter, in full-alert mode.

Each Kafka message is one JSON object: the filter's selected columns at top
level (``diaObjectId``, Sherlock's ``classification``, ``latestR``, ...) and
the full alert under ``alert``. No detection is named as the trigger; it is
the latest entry of ``alert.diaSourcesList``, and the other entries are its
history. ``alert.diaForcedSourcesList`` (forced photometry) is left out.

The SN-like signal is Sherlock's categorical host-context class: ``SN`` or
``NT`` scores 1.0 and any other class 0.0.
"""

import json
from collections.abc import Mapping
from typing import Any

from seldon.domain.intake.delivery import Delivery, Point
from seldon.infrastructure.brokers.base import MissingFieldError, ParseContext
from seldon.infrastructure.brokers.parsers import field, identifier, points, score

#: Sherlock classes that count as SN-like.
SN_LIKE_CLASSES = frozenset({"SN", "NT"})


def decode(key: bytes | None, value: bytes) -> list[dict[str, Any]]:
    """Decode one Lasair message.

    Args:
        key: Unused; Lasair sends no key.
        value: The JSON message.

    Returns:
        The one decoded message.

    Raises:
        ValueError: If the value is not a JSON object.
    """
    message = json.loads(value)
    if not isinstance(message, dict):
        raise ValueError("message is not a JSON object")
    return [message]


def _point(source: Mapping[str, Any]) -> Point:
    """A diaSource as a point.

    Args:
        source: One entry of ``diaSourcesList``.

    Returns:
        Its point, with its ``diaSourceId``.

    Raises:
        MissingFieldError: If a needed field is absent.
    """
    return Point(
        observed_mjd=float(field(source, "midpointMjdTai")),
        band=str(field(source, "band")),
        flux=float(field(source, "psfFlux")),
        flux_err=float(field(source, "psfFluxErr")),
        dia_source_id=int(field(source, "diaSourceId")),
    )


def _time(source: Mapping[str, Any]) -> float:
    """A diaSource's time, for finding the latest; earliest when unset.

    Args:
        source: One entry of ``diaSourcesList``.

    Returns:
        Its ``midpointMjdTai``, or minus infinity when absent.
    """
    try:
        return float(field(source, "midpointMjdTai"))
    except MissingFieldError:
        return float("-inf")


def parse(message: Mapping[str, Any], context: ParseContext) -> Delivery:
    """Turn a Lasair filter message into a delivery of its latest detection.

    Args:
        message: The decoded message.
        context: Where and when it arrived.

    Returns:
        The delivery.

    Raises:
        MissingFieldError: If a field the delivery needs is absent, including
            an empty ``alert.diaSourcesList``.
    """
    classification = str(field(message, "classification"))
    sources = field(message, "alert.diaSourcesList")
    if not sources:
        raise MissingFieldError("alert.diaSourcesList")
    latest_index = max(range(len(sources)), key=lambda i: _time(sources[i]))
    latest = sources[latest_index]
    try:
        detection = _point(latest)
    except MissingFieldError as missing:
        raise MissingFieldError(f"alert.diaSourcesList.{missing.field}") from None
    reliability = score(latest, "reliability")
    history = [s for i, s in enumerate(sources) if i != latest_index]
    return Delivery(
        dia_object_id=int(field(message, "diaObjectId")),
        detection=detection,
        previous=points(history, _point, "alert.diaSourcesList"),
        broker=context.broker,
        role=context.role,
        reliability=reliability,
        sn_like_score=1.0 if classification in SN_LIKE_CLASSES else 0.0,
        published_at=context.published_at,
        received_at=context.received_at,
    )


def identify(message: Mapping[str, Any]) -> str:
    """The message's identifier for an intake skip.

    Args:
        message: The decoded message.

    Returns:
        Its ``diaObjectId``, or an empty string.
    """
    return identifier(message, "diaObjectId", "alert.diaObjectId")
