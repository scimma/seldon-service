"""One parser per broker, from a decoded alert to a broker-neutral delivery.

Each parser module offers:

- ``decode(key, value)``: the decoded alerts one Kafka message carries;
- ``parse(alert, context)``: the delivery, raising ``MissingFieldError``
  when the alert lacks a field the delivery needs;
- ``identify(alert)``: the alert's identifier for an intake skip.

``parse_alert`` turns a parser failure into an intake skip, so a malformed
alert never stops the consumer.
"""

import logging
import math
from collections.abc import Callable, Iterable, Mapping
from types import ModuleType
from typing import Any

from seldon.domain.intake.delivery import Delivery, Point
from seldon.infrastructure.brokers.base import (
    MAX_REASON_LENGTH,
    IntakeSkipReport,
    MissingFieldError,
    ParseContext,
)

logger = logging.getLogger("seldon.brokers.parsers")


def field(alert: Mapping[str, Any], path: str) -> Any:
    """Read a field that must be present and set.

    Args:
        alert: The decoded alert, or a part of it.
        path: The field's dotted path, such as ``"diaSource.psfFlux"``.

    Returns:
        The field's value.

    Raises:
        MissingFieldError: If the field is absent, null, or a NaN number.
    """
    value: Any = alert
    for part in path.split("."):
        if not isinstance(value, Mapping) or part not in value:
            raise MissingFieldError(path)
        value = value[part]
    if value is None or (isinstance(value, float) and math.isnan(value)):
        raise MissingFieldError(path)
    return value


def dia_source_point(source: Mapping[str, Any]) -> Point:
    """A Rubin diaSource as a point, with its ``diaSourceId``.

    Args:
        source: One diaSource, with Rubin's field names.

    Returns:
        Its point.

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


def score(alert: Mapping[str, Any], path: str) -> float | None:
    """Read a score that must be present but may be unset.

    A null, NaN, or out-of-range value (Fink sends ``-1`` for a score it did
    not compute) is no score at all, never a probability.

    Args:
        alert: The decoded alert, or a part of it.
        path: The score's dotted path.

    Returns:
        The score in [0, 1], or ``None`` when it was not given.

    Raises:
        MissingFieldError: If the field is absent.
    """
    head, _, last = path.rpartition(".")
    parent = field(alert, head) if head else alert
    if not isinstance(parent, Mapping) or last not in parent:
        raise MissingFieldError(path)
    value = parent[last]
    if value is None:
        return None
    value = float(value)
    if math.isnan(value) or not 0.0 <= value <= 1.0:
        return None
    return value


def points(
    entries: Iterable[Mapping[str, Any]],
    build: Callable[[Mapping[str, Any]], Point],
    source: str,
) -> tuple[Point, ...]:
    """Build history points, dropping any that lack a usable measurement.

    One unusable earlier point should not cost the whole alert, so it is
    left out and logged instead of skipping the alert.

    Args:
        entries: The history entries as the broker sent them.
        build: Turns one entry into a point, raising ``MissingFieldError``.
        source: Name of the history list, for the log.

    Returns:
        The usable points, in the order sent.
    """
    built = []
    for index, entry in enumerate(entries):
        try:
            built.append(build(entry))
        except MissingFieldError as missing:
            logger.debug("dropping %s[%d]: missing %s", source, index, missing.field)
    return tuple(built)


def identifier(alert: Mapping[str, Any], *paths: str) -> str:
    """The first identifier the alert carries, for an intake skip.

    Args:
        alert: The decoded alert.
        *paths: Dotted paths to try, in order.

    Returns:
        The first one present, as text, or an empty string.
    """
    for path in paths:
        try:
            return str(field(alert, path))
        except MissingFieldError:
            continue
    return ""


def _formats() -> dict[str, ModuleType]:
    """The parser module for each broker name.

    Returns:
        Broker name to parser module.
    """
    from seldon.infrastructure.brokers.parsers import babamul, fink, lasair

    return {"fink": fink, "lasair": lasair, "babamul": babamul}


def parser_for(broker: str) -> ModuleType:
    """The parser module for a broker.

    Args:
        broker: The configured broker name.

    Returns:
        Its parser module.

    Raises:
        KeyError: If no parser exists for that broker.
    """
    return _formats()[broker]


def parse_alert(
    broker: str, alert: Mapping[str, Any], context: ParseContext
) -> Delivery | IntakeSkipReport:
    """Parse one decoded alert, reporting a malformed one as an intake skip.

    Args:
        broker: Whose format the alert is in (selects the parser).
        alert: The decoded alert.
        context: Where and when it arrived.

    Returns:
        The delivery, or an intake skip naming what was wrong.
    """
    parser = parser_for(broker)
    try:
        return parser.parse(alert, context)
    except MissingFieldError as missing:
        reason = str(missing)
    except (TypeError, ValueError, AttributeError) as malformed:
        reason = f"malformed alert: {type(malformed).__name__}"
    try:
        raw_identifier = parser.identify(alert)
    except Exception:  # an unreadable identifier must not lose the skip
        raw_identifier = ""
    return IntakeSkipReport(
        broker=context.broker,
        reason=reason[:MAX_REASON_LENGTH],
        occurred_at=context.received_at,
        raw_identifier=raw_identifier,
    )
