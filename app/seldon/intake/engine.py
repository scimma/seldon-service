"""Turn one broker's delivery into stored detections and owed detections.

Intake decides which detections are owed a product and never calls a model:
model packages run in their own worker processes, each in an image that may
pin its own torch, and pick owed detections up from the database. So the
intake path imports no model code and holds no row lock across inference.

The consumer hands the engine a batch of one broker's polled alerts, in poll
order, with every broker's thresholds in effect. Each alert is a
broker-neutral ``Delivery`` or an intake skip the adapter reported. For each
delivery the engine records it against its detection and decides which of
the object's detections are now owed. A detection becomes owed with the
wall-clock time it did so and a snapshot of the thresholds and qualifying
event in effect, so a later setting change or an earlier detection that
qualifies afterwards does not rewrite why it was owed, and a redelivery
changes neither. The owed time is the wall clock's even during a replay,
because which head settings cover a detection is decided by comparing it
with the wall-clock times those settings took effect.

The whole batch is one transaction, with a savepoint per delivery, so a
value the database refuses becomes an intake skip without undoing the rest
of the batch, while any other database error rolls the whole batch back. The result says whether the batch may be acknowledged; it is returned
only after the transaction has committed, so a crash before acknowledgement
causes a redelivery, which finds the work already stored.

A detection waits (pending) until its object qualifies on a detection
observed at or before it. Every pending detection is held for seven days
from its first delivery: if its object qualifies in that time it is owed,
otherwise ``sweep_pending`` records it as missed (object qualified on a
later detection) or discards it (object never qualified). The sweep never
touches an owed detection.

A detection's products are made from the light curve carried by the alert
that delivered it, rebuilt from the points stored for that delivery. When
several brokers delivered the detection, the earliest-received delivery is
used. Its packet is built once, when the detection would become owed: a
detection whose packet cannot be built is recorded as an intake skip and
left pending, so it never reaches a worker. With several consumer
processes, each delivery's work runs under a lock on its object's row, so a
detection is owed by the first transaction to hold that lock, from the
deliveries committed before it; a batch that conflicts with another
process's concurrent work is retried at once.

Every alert whose work is newly stored (a new delivery or a new intake
skip) adds one count to the intake-lag record. The batch's counts are
applied once, at the end of the transaction that stores its work, with the
lag from publication to a commit time read from the caller's clock just
before the transaction ends.
"""

import logging
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

from django.db import DatabaseError, DataError, transaction
from django.db.models import Min, Q, QuerySet
from django.utils import timezone
from seldon_model_contract import Packet

from seldon.domain.contract.packets import build_packet
from seldon.domain.intake.delivery import BrokerRole, Delivery, Point
from seldon.domain.intake.grid import LightCurveSpanError, check_span
from seldon.domain.intake.light_curve import (
    light_curve_points,
    normalize_band,
    observed_key,
)
from seldon.domain.intake.qualification import Thresholds, qualifies
from seldon.infrastructure.brokers.base import IntakeSkipReport
from seldon.models import Delivery as StoredDelivery
from seldon.models import (
    ConsumerLastCommit,
    Detection,
    IntakeLagCount,
    IntakeSkip,
    LightCurvePoint,
    RubinObject,
)

logger = logging.getLogger("seldon.intake")

#: How long a pending delivery is held for its object to qualify.
PENDING_HOLD = timedelta(days=7)

#: Intake-skip reason for a light curve longer than any Rubin alert carries.
SKIP_LIGHT_CURVE_SPAN = "light_curve_span"

#: Intake-skip reason for any other failure to read an alert's light curve.
SKIP_INVALID_REQUEST = "invalid_request"

#: Intake-skip reason for a detection whose contract packet cannot be built.
SKIP_PACKET_BUILD = "packet_build"

#: Intake-skip reason for an alert holding a value the database cannot store.
SKIP_UNSTORABLE_VALUE = "unstorable_value"

#: Most attempts at a batch that fails only because of another consumer
#: process's concurrent work. A conflict that keeps recurring then takes the
#: ordinary database-error path, so a fault that merely looks like one
#: cannot spin the consumer.
CONFLICT_ATTEMPTS = 5

#: Postgres error codes that concurrent work by another process causes:
#: deadlock detected, serialization failure, and unique violation (a row
#: another process inserted and committed first).
CONFLICT_SQLSTATES = frozenset({"40P01", "40001", "23505"})


def _sqlstate(error: BaseException) -> str | None:
    """The Postgres error code behind a database error, if any.

    Args:
        error: A Django database error.

    Returns:
        The five-character SQLSTATE, or ``None`` when there is none.
    """
    cause = error.__cause__
    return getattr(cause, "sqlstate", None) or getattr(cause, "pgcode", None)


def is_concurrency_conflict(error: BaseException) -> bool:
    """Whether a database error comes from another process's concurrent work.

    Such an error goes away when the transaction is simply run again.

    Args:
        error: A Django database error.

    Returns:
        ``True`` for a deadlock, a serialization failure, or a unique
        violation.
    """
    return _sqlstate(error) in CONFLICT_SQLSTATES


@dataclass(frozen=True)
class IntakeResult:
    """What processing one delivery did, and whether to acknowledge it.

    Attributes:
        acknowledge: ``True`` once everything the delivery caused is
            committed; ``False`` when a database error rolled it back and
            the broker message must be left for redelivery.
        owed: ``diaSourceId`` of each detection the delivery made owed, in
            observation order.
        skip_reason: The intake-skip reason when the alert was skipped.
    """

    acknowledge: bool
    owed: tuple[int, ...] = ()
    skip_reason: str | None = None


@dataclass(frozen=True)
class BatchResult:
    """What processing a batch of polled alerts did, and whether to acknowledge it.

    Attributes:
        acknowledge: ``True`` once everything the batch caused is committed;
            ``False`` when a database error rolled the whole batch back and
            none of it may be acknowledged.
        results: Each alert's result, in batch order; empty when rolled back.
    """

    acknowledge: bool
    results: tuple[IntakeResult, ...] = ()


@dataclass(frozen=True)
class SweepResult:
    """What one sweep of expired pending detections did.

    Attributes:
        missed: Detections recorded as missed.
        discarded: Detections discarded with their deliveries.
    """

    missed: int
    discarded: int


class IntakeLagTally:
    """Intake-lag counts for one transaction's alerts, applied at its end.

    Add each alert whose work the transaction newly stores, then flush once,
    just before the transaction commits, with the commit time. Flushing
    inside the transaction means a rollback drops the counts with the work.
    """

    def __init__(self) -> None:
        """Start with no alerts."""
        self._alerts: list[tuple[str, datetime | None, datetime]] = []

    def add(
        self, broker: str, published_at: datetime | None, received_at: datetime
    ) -> None:
        """Note one alert whose work this transaction stores.

        Args:
            broker: The configured broker name.
            published_at: When the broker published it, if known.
            received_at: When the service received it.
        """
        self._alerts.append((broker, published_at, received_at))

    def flush(self, committed_at: datetime) -> None:
        """Add the noted alerts' counts and forget them.

        Args:
            committed_at: When the transaction's work commits.
        """
        counts = Counter(
            IntakeLagCount.bucket(broker, published_at, received_at, committed_at)
            for broker, published_at, received_at in self._alerts
        )
        IntakeLagCount.add_counts(counts)
        self._alerts.clear()


def record_intake_skip(
    broker: str, reason: str, occurred_at: datetime, raw_identifier: str = ""
) -> bool:
    """Record an alert that intake could not turn into owed work.

    Adapters' callers use this for alerts that cannot be parsed or lack a
    required field; the engine uses it when a light curve or packet cannot
    be built.
    A skip with an identifier is stored once: a redelivered alert whose
    skip is already stored (same broker, reason and identifier) adds
    nothing.

    Args:
        broker: The configured broker name.
        reason: Why the alert was skipped, as a short code.
        occurred_at: When it was skipped.
        raw_identifier: The alert's identifier as received, when readable.

    Returns:
        Whether a new skip was stored.
    """
    raw_identifier = raw_identifier[:255]
    if (
        raw_identifier
        and IntakeSkip.objects.filter(
            broker=broker, reason=reason, raw_identifier=raw_identifier
        ).exists()
    ):
        return False
    logger.warning(
        "Skipped %s alert %s: %s", broker, raw_identifier or "(unknown)", reason
    )
    IntakeSkip.objects.create(
        broker=broker,
        reason=reason,
        occurred_at=occurred_at,
        raw_identifier=raw_identifier,
    )
    return True


def process_batch(
    outcomes: Sequence[Delivery | IntakeSkipReport],
    thresholds: Mapping[str, Thresholds],
    clock: Callable[[], datetime] = timezone.now,
    instance: str | None = None,
) -> BatchResult:
    """Store a batch of polled alerts in one transaction.

    Alerts are handled in order. Each delivery is recorded and the
    detections it makes owed are marked so; each reported intake skip is
    stored. A delivery whose light curve cannot be read, or holding a value
    the database refuses, becomes an intake skip; its savepoint keeps the
    rest of the batch. The batch's intake-lag counts are applied once, at the end of
    the transaction, with the commit time read from ``clock``; so is the
    consumer instance's last-commit time for the broker, when ``instance``
    is given.

    When another consumer process's concurrent work makes the transaction
    fail (a deadlock, a serialization failure, or a row another process has
    just inserted), the whole batch is rolled back and retried at once, up
    to ``CONFLICT_ATTEMPTS`` attempts in all.

    Args:
        outcomes: One broker's polled alerts, in poll order.
        thresholds: Every qualifying broker's cut and threshold in effect,
            by broker name; snapshotted with each detection made owed.
        clock: Returns the current aware time; read just before the
            batch's work commits, as the commit time of its lag. Replay
            passes its own clock; the time a detection becomes owed is
            always read from the wall clock.
        instance: The consumer instance storing the batch; its last-commit
            record for the broker is updated in the batch's transaction.

    Returns:
        The result; acknowledge the batch only when its ``acknowledge`` is
        ``True``.

    Raises:
        KeyError: If a qualifying broker has no entry in ``thresholds``.
    """
    brokers = ", ".join(sorted({outcome.broker for outcome in outcomes}))
    for attempt in range(1, CONFLICT_ATTEMPTS + 1):
        tally = IntakeLagTally()
        results: list[IntakeResult] = []
        try:
            with transaction.atomic():
                for outcome in outcomes:
                    results.append(_process_one(outcome, thresholds, tally))
                committed_at = clock()
                tally.flush(committed_at)
                if instance is not None and outcomes:
                    ConsumerLastCommit.record(
                        outcomes[0].broker, instance, committed_at
                    )
        except DatabaseError as error:
            if attempt < CONFLICT_ATTEMPTS and is_concurrency_conflict(error):
                logger.warning(
                    "Concurrent write conflict storing a batch of %d alerts "
                    "from %s (%s); retrying it at once",
                    len(outcomes),
                    brokers,
                    _sqlstate(error),
                )
                continue
            logger.exception(
                "Database error storing a batch of %d alerts from %s; "
                "leaving it unacknowledged",
                len(outcomes),
                brokers,
            )
            return BatchResult(acknowledge=False)
        return BatchResult(acknowledge=True, results=tuple(results))
    raise AssertionError("unreachable")  # every attempt returns or continues


def process_delivery(
    delivery: Delivery,
    thresholds: Mapping[str, Thresholds],
    clock: Callable[[], datetime] = timezone.now,
) -> IntakeResult:
    """Record one delivery and mark every detection it makes owed.

    The delivery is a batch of its own: one transaction, counted in the
    intake-lag record at its end.

    Args:
        delivery: One broker's delivery of one detection.
        thresholds: Every qualifying broker's cut and threshold in effect,
            by broker name; snapshotted with each detection made owed.
        clock: Returns the current aware time; read just before the
            delivery's work commits, as the commit time of its lag.

    Returns:
        The result; acknowledge the broker message only when its
        ``acknowledge`` is ``True``.

    Raises:
        KeyError: If a qualifying broker has no entry in ``thresholds``.
    """
    batch = process_batch([delivery], thresholds, clock=clock)
    if not batch.acknowledge:
        return IntakeResult(acknowledge=False)
    return batch.results[0]


def _process_one(
    outcome: Delivery | IntakeSkipReport,
    thresholds: Mapping[str, Thresholds],
    tally: IntakeLagTally,
) -> IntakeResult:
    """Store one polled alert's work inside its batch's transaction.

    Args:
        outcome: A delivery, or an intake skip the adapter reported.
        thresholds: Every qualifying broker's thresholds, by name.
        tally: The batch's intake-lag counts; the alert is added when its
            work is newly stored.

    Returns:
        The alert's result.

    Raises:
        DatabaseError: On any database failure other than a refused value
            in a delivery; the caller rolls the batch back.
    """
    if isinstance(outcome, IntakeSkipReport):
        if record_intake_skip(
            outcome.broker, outcome.reason, outcome.occurred_at, outcome.raw_identifier
        ):
            tally.add(outcome.broker, outcome.published_at, outcome.occurred_at)
        return IntakeResult(acknowledge=True, skip_reason=outcome.reason)

    delivery = outcome
    try:
        points = light_curve_points(delivery)
        check_span(points[0].observed_mjd, points[-1].observed_mjd)
    except (ValueError, ArithmeticError) as exc:
        return _skip_delivery(delivery, _skip_reason(exc), tally)

    try:
        with transaction.atomic():
            result, newly_stored = _process(delivery, thresholds)
    except DataError:
        # The database refused a value in this alert: retrying cannot help.
        return _skip_delivery(delivery, SKIP_UNSTORABLE_VALUE, tally)
    if newly_stored:
        tally.add(delivery.broker, delivery.published_at, delivery.received_at)
    return result


def _skip_delivery(
    delivery: Delivery, reason: str, tally: IntakeLagTally
) -> IntakeResult:
    """Store a delivery as an intake skip and count it.

    Args:
        delivery: The delivery that cannot be stored or owed.
        reason: The intake-skip reason.
        tally: The batch's intake-lag counts; the alert is added when its
            skip is newly stored.

    Returns:
        The acknowledgeable result naming the reason.
    """
    if record_intake_skip(
        delivery.broker, reason, delivery.received_at, str(delivery.dia_source_id)
    ):
        tally.add(delivery.broker, delivery.published_at, delivery.received_at)
    return IntakeResult(acknowledge=True, skip_reason=reason)


def sweep_pending(now: datetime) -> SweepResult:
    """Close every pending detection first delivered seven or more days ago.

    Only pending detections are considered; an owed one is never touched. A
    detection observed at or after its object's qualifying detection would
    have been owed and is recorded as missed. Any other, including one of a
    qualified object observed before the qualifying detection (never owed),
    is discarded with its deliveries, the points no other
    delivery carried, and the object itself when it has no detection left.

    Args:
        now: The time the sweep is run for.

    Returns:
        How many detections were missed and discarded.
    """
    cutoff = now - PENDING_HOLD
    object_ids = set(
        _expired_pending(Detection.objects.all(), cutoff).values_list(
            "rubin_object_id", flat=True
        )
    )
    missed = discarded = 0
    for object_id in object_ids:
        with transaction.atomic():
            rubin_object = RubinObject.objects.select_for_update().get(pk=object_id)
            expired = list(_expired_pending(rubin_object.detections.all(), cutoff))
            qualifying = rubin_object.qualifying_detection
            missed_ids = [
                detection.pk
                for detection in expired
                if qualifying is not None
                and detection.observed_mjd >= qualifying.observed_mjd
            ]
            if missed_ids:
                Detection.objects.filter(pk__in=missed_ids).update(
                    state=Detection.State.MISSED
                )
                missed += len(missed_ids)
            discard_ids = [d.pk for d in expired if d.pk not in set(missed_ids)]
            if not discard_ids:
                continue
            StoredDelivery.objects.filter(detection_id__in=discard_ids).delete()
            Detection.objects.filter(pk__in=discard_ids).delete()
            discarded += len(discard_ids)
            LightCurvePoint.objects.filter(
                dia_object_id=rubin_object.dia_object_id, deliveries__isnull=True
            ).delete()
            if not rubin_object.detections.exists():
                rubin_object.delete()
    logger.info("Pending sweep: %d missed, %d discarded", missed, discarded)
    return SweepResult(missed=missed, discarded=discarded)


def _expired_pending(
    detections: QuerySet[Detection], cutoff: datetime
) -> QuerySet[Detection]:
    """Narrow detections to pending ones first delivered at or before cutoff.

    Args:
        detections: A ``Detection`` queryset.
        cutoff: The latest first-delivery time that has expired.

    Returns:
        The narrowed queryset.
    """
    return (
        detections.filter(state=Detection.State.PENDING)
        .annotate(first_received=Min("deliveries__received_at"))
        .filter(first_received__lte=cutoff)
    )


def _skip_reason(exc: ValueError | ArithmeticError) -> str:
    """Name why an alert's light curve could not be used.

    Args:
        exc: The error reading the light curve raised.

    Returns:
        The intake-skip reason.
    """
    if isinstance(exc, LightCurveSpanError):
        return SKIP_LIGHT_CURVE_SPAN
    return SKIP_INVALID_REQUEST


def _process(
    delivery: Delivery,
    thresholds: Mapping[str, Thresholds],
) -> tuple[IntakeResult, bool]:
    """Do the work of one delivery inside its savepoint.

    The object's row is locked first, so deliveries of one object from
    several consumers are applied one at a time.

    Args:
        delivery: One broker's delivery of one detection.
        thresholds: Every qualifying broker's thresholds, by name.

    Returns:
        The result, acknowledgeable once the batch's transaction commits, and
        whether this broker's delivery was newly stored (``False`` for a
        redelivery).
    """
    rubin_object, _ = RubinObject.objects.select_for_update().get_or_create(
        dia_object_id=delivery.dia_object_id
    )
    detection, _ = Detection.objects.get_or_create(
        dia_source_id=delivery.dia_source_id,
        defaults={
            "rubin_object": rubin_object,
            "state": Detection.State.PENDING,
            "observed_mjd": delivery.detection.observed_mjd,
        },
    )
    newly_stored = _record_delivery(delivery, detection)

    if delivery.role is BrokerRole.QUALIFYING:
        limits = thresholds[delivery.broker]
        if qualifies(delivery, limits):
            _qualify(rubin_object, detection, delivery, limits)

    owed: list[int] = []
    if rubin_object.qualifying_detection_id is not None:
        owed = _owe_pending(rubin_object, thresholds, delivery.received_at)
    return IntakeResult(acknowledge=True, owed=tuple(owed)), newly_stored


def _record_delivery(delivery: Delivery, detection: Detection) -> bool:
    """Store a broker's first delivery of a detection and the points it carried.

    A redelivery from the same broker changes nothing: the first delivery,
    with its times and points, is kept.

    Args:
        delivery: The broker's delivery.
        detection: The stored detection it delivers.

    Returns:
        Whether the delivery was newly stored.
    """
    stored, created = StoredDelivery.objects.get_or_create(
        detection=detection,
        broker=delivery.broker,
        defaults={
            "role": delivery.role.value,
            "reliability_score": delivery.reliability,
            "sn_like_score": delivery.sn_like_score,
            "published_at": delivery.published_at,
            "received_at": delivery.received_at,
            "schema_version": delivery.schema_version,
            "classifier_version": delivery.classifier_version,
        },
    )
    if created:
        stored.points.add(
            *_stored_points(
                delivery.dia_object_id, (delivery.detection, *delivery.previous)
            )
        )
    return created


def _stored_points(dia_object_id: int, points: Sequence[Point]) -> set[int]:
    """Find or store an alert's measurements, identified as intake identifies them.

    A measurement is matched by its ``diaSourceId`` when it carries one,
    otherwise by object, band, and rounded time. A measurement already
    stored keeps its values; a ``diaSourceId`` it lacked is filled in when
    this copy carries one. The object's stored measurements are read in one
    query and new ones written in one.

    Args:
        dia_object_id: The measured object's ``diaObjectId``.
        points: The measurements as a broker sent them.

    Returns:
        The primary keys of the stored measurements.
    """
    source_ids = [p.dia_source_id for p in points if p.dia_source_id is not None]
    existing = LightCurvePoint.objects.filter(
        Q(dia_object_id=dia_object_id) | Q(dia_source_id__in=source_ids)
    )
    by_source = {p.dia_source_id: p for p in existing if p.dia_source_id is not None}
    by_key = {
        (p.band, p.observed_key): p
        for p in existing
        if p.dia_object_id == dia_object_id
    }
    new: list[LightCurvePoint] = []
    filled: list[LightCurvePoint] = []
    matched: list[LightCurvePoint] = []
    for point in points:
        band = normalize_band(point.band)
        key = (band, observed_key(point.observed_mjd))
        stored = None
        if point.dia_source_id is not None:
            stored = by_source.get(point.dia_source_id)
        if stored is None:
            stored = by_key.get(key)
            if (
                stored is not None
                and stored.dia_source_id is None
                and point.dia_source_id is not None
            ):
                stored.dia_source_id = point.dia_source_id
                by_source[point.dia_source_id] = stored
                if stored.pk is not None:
                    filled.append(stored)
        if stored is None:
            stored = LightCurvePoint(
                dia_object_id=dia_object_id,
                dia_source_id=point.dia_source_id,
                observed_mjd=point.observed_mjd,
                observed_key=key[1],
                band=band,
                flux=point.flux,
                flux_err=point.flux_err,
            )
            new.append(stored)
            by_key[key] = stored
            if point.dia_source_id is not None:
                by_source[point.dia_source_id] = stored
        matched.append(stored)
    LightCurvePoint.objects.bulk_create(new)
    if filled:
        LightCurvePoint.objects.bulk_update(filled, ["dia_source_id"])
    return {stored.pk for stored in matched}


def _qualify(
    rubin_object: RubinObject,
    detection: Detection,
    delivery: Delivery,
    limits: Thresholds,
) -> None:
    """Make a qualifying detection the object's qualifying event if earliest.

    The qualifying event is the earliest-observed detection that qualified,
    since every detection observed at or after it is owed.

    Args:
        rubin_object: The locked object.
        detection: The detection the delivery qualified.
        delivery: The qualifying delivery.
        limits: The delivering broker's thresholds in effect.
    """
    current = rubin_object.qualifying_detection
    if current is not None and current.observed_mjd <= detection.observed_mjd:
        return
    rubin_object.qualifying_detection = detection
    rubin_object.qualifying_broker = delivery.broker
    rubin_object.qualifying_reliability_score = delivery.reliability
    rubin_object.qualifying_sn_like_score = delivery.sn_like_score
    rubin_object.qualifying_reliability_cut = limits.reliability_cut
    rubin_object.qualifying_sn_like_threshold = limits.sn_like_threshold
    rubin_object.qualified_at = delivery.received_at
    rubin_object.save()
    logger.info(
        "diaObject %s qualified on diaSource %s from %s",
        rubin_object.dia_object_id,
        detection.dia_source_id,
        delivery.broker,
    )


def _owe_pending(
    rubin_object: RubinObject, thresholds: Mapping[str, Thresholds], now: datetime
) -> list[int]:
    """Move the qualified object's pending detections to owed or missed.

    Every pending detection observed at or after the qualifying detection
    is owed, unless it was first delivered seven or more days ago: then it
    is missed. A detection whose packet cannot be built is recorded as an
    intake skip instead and stays pending, so no worker is ever handed it;
    a later delivery of its object finds the skip already stored.

    Args:
        rubin_object: The locked, qualified object.
        thresholds: Every qualifying broker's thresholds, by name.
        now: The time of the delivery being processed, on the caller's
            clock; it decides the hold and dates any skip.

    Returns:
        The ``diaSourceId`` of each detection made owed, in observation
        order.
    """
    pending = (
        rubin_object.detections.filter(
            state=Detection.State.PENDING,
            observed_mjd__gte=rubin_object.qualifying_detection.observed_mjd,
        )
        .annotate(first_received=Min("deliveries__received_at"))
        .order_by("observed_mjd")
    )
    owed: list[int] = []
    for detection in pending:
        if detection.first_received <= now - PENDING_HOLD:
            detection.state = Detection.State.MISSED
            detection.save(update_fields=["state"])
            continue
        source = _earliest_delivery(detection)
        try:
            owed_packet(detection, source)
        except (ValueError, ArithmeticError) as exc:
            reason = (
                SKIP_LIGHT_CURVE_SPAN
                if isinstance(exc, LightCurveSpanError)
                else SKIP_PACKET_BUILD
            )
            logger.warning(
                "No packet for diaSource %s: %s", detection.dia_source_id, exc
            )
            record_intake_skip(source.broker, reason, now, str(detection.dia_source_id))
            continue
        detection.state = Detection.State.OWED
        detection.owed_at = timezone.now()
        detection.qualification = _qualification(rubin_object, thresholds)
        detection.save(update_fields=["state", "owed_at", "qualification"])
        owed.append(detection.dia_source_id)
        logger.info("diaSource %s is owed", detection.dia_source_id)
    return owed


def _qualification(
    rubin_object: RubinObject, thresholds: Mapping[str, Thresholds]
) -> dict:
    """The snapshot of why a detection is owed, as it is at this moment.

    Args:
        rubin_object: The qualified object.
        thresholds: Every qualifying broker's thresholds, by name.

    Returns:
        Every broker's reliability cut and SN-like threshold in effect, and
        the object's qualifying event.
    """
    return {
        "thresholds": {
            name: {
                "reliability_cut": limits.reliability_cut,
                "sn_like_threshold": limits.sn_like_threshold,
            }
            for name, limits in thresholds.items()
        },
        "qualifying_event": rubin_object.qualifying_event(),
    }


def _earliest_delivery(detection: Detection) -> StoredDelivery:
    """The delivery a detection's products are made from.

    Args:
        detection: A stored detection.

    Returns:
        Its earliest-received delivery, the first stored on a tie.
    """
    return detection.deliveries.order_by("received_at", "pk").first()


def owed_packet(detection: Detection, source: StoredDelivery | None = None) -> Packet:
    """Build the contract packet a detection is owed products for.

    The packet holds the light curve its earliest-received delivery carried,
    rebuilt from the points stored for that delivery. Intake builds it once,
    when the detection would become owed, to refuse a detection no packet
    can be built for.

    Args:
        detection: A stored detection.
        source: Its earliest-received delivery, when already read.

    Returns:
        The packet.

    Raises:
        LightCurveSpanError: If the light curve spans more than any Rubin
            alert carries.
        ValueError: If the stored delivery has no point for the detection,
            or the packet cannot otherwise be built.
    """
    delivery = _rebuilt_delivery(source or _earliest_delivery(detection), detection)
    points = light_curve_points(delivery)
    check_span(points[0].observed_mjd, points[-1].observed_mjd)
    return build_packet(delivery)


def _rebuilt_delivery(source: StoredDelivery, detection: Detection) -> Delivery:
    """Rebuild a stored delivery as the domain delivery its alert made.

    Args:
        source: The stored delivery.
        detection: The detection it delivered.

    Returns:
        The delivery, with the detection and the other points it carried.

    Raises:
        ValueError: If none of its points is the detection itself.
    """
    rows = list(source.points.all())
    points = {
        row.pk: Point(
            observed_mjd=row.observed_mjd,
            band=row.band,
            flux=row.flux,
            flux_err=row.flux_err,
            dia_source_id=row.dia_source_id,
        )
        for row in rows
    }
    detection_pk = next(
        (row.pk for row in rows if row.dia_source_id == detection.dia_source_id),
        None,
    )
    if detection_pk is None:
        raise ValueError(
            f"Delivery of diaSource {detection.dia_source_id} has no point for it."
        )
    return Delivery(
        dia_object_id=detection.rubin_object.dia_object_id,
        detection=points.pop(detection_pk),
        previous=tuple(points.values()),
        broker=source.broker,
        role=BrokerRole(source.role),
        reliability=source.reliability_score,
        sn_like_score=source.sn_like_score,
        published_at=source.published_at,
        received_at=source.received_at,
        schema_version=source.schema_version,
        classifier_version=source.classifier_version,
    )
