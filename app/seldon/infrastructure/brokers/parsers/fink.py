"""Fink alerts from ``fink_most_likely_sn_lsst``.

Each Kafka message is one schemaless Avro record whose writer schema rides
in the message key as JSON, as ``fink-client`` reads it. The triggering
detection is ``diaSource``; ``prvDiaSources`` are earlier detections, each
with its ``diaSourceId``; ``prvDiaForcedSources`` (forced photometry) is left
out. The SN-like score is SuperNNova's ``clf.snnSnVsOthers_score``.
"""

import io
import json
from collections.abc import Mapping
from typing import Any

import fastavro

from seldon.domain.intake.delivery import Delivery
from seldon.infrastructure.brokers.base import MissingFieldError, ParseContext
from seldon.infrastructure.brokers.parsers import (
    dia_source_point,
    field,
    identifier,
    points,
    rubin_id,
    score,
)


def decode(key: bytes | None, value: bytes) -> list[dict[str, Any]]:
    """Decode one Fink message with the schema carried in its key.

    Args:
        key: The writer schema, as JSON.
        value: The schemaless Avro record.

    Returns:
        The one decoded alert.

    Raises:
        ValueError: If the key holds no schema or the record does not decode.
    """
    if not key:
        raise ValueError("message has no schema key")
    try:
        schema = fastavro.parse_schema(json.loads(key))
        return [fastavro.schemaless_reader(io.BytesIO(value), schema)]
    except (EOFError, fastavro.schema.SchemaParseException) as error:
        raise ValueError("message does not decode") from error


def parse(alert: Mapping[str, Any], context: ParseContext) -> Delivery:
    """Turn a Fink alert into a delivery.

    Args:
        alert: The decoded alert.
        context: Where and when it arrived.

    Returns:
        The delivery of its ``diaSource``.

    Raises:
        MissingFieldError: If a field the delivery needs is absent.
    """
    source = field(alert, "diaSource")
    try:
        detection = dia_source_point(source)
    except MissingFieldError as missing:
        raise MissingFieldError(f"diaSource.{missing.field}") from None
    reliability = score(alert, "diaSource.reliability")
    sn_like = score(alert, "clf.snnSnVsOthers_score")
    return Delivery(
        dia_object_id=rubin_id(alert, "diaSource.diaObjectId"),
        detection=detection,
        previous=points(
            alert.get("prvDiaSources") or (), dia_source_point, "prvDiaSources"
        ),
        broker=context.broker,
        role=context.role,
        reliability=reliability,
        sn_like_score=sn_like,
        published_at=context.published_at,
        received_at=context.received_at,
        schema_version=str(alert.get("lsst_schema_version") or "")[:64],
        classifier_version=str(alert.get("fink_science_version") or "")[:128],
    )


def identify(alert: Mapping[str, Any]) -> str:
    """The alert's identifier for an intake skip.

    Args:
        alert: The decoded alert.

    Returns:
        Its ``diaSourceId``, else its ``diaObjectId``, else an empty string.
    """
    return identifier(
        alert, "diaSource.diaSourceId", "diaSourceId", "diaSource.diaObjectId"
    )
