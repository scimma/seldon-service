"""Turn one broker's delivery into stored detections, forecasts, and refusals.

The consumer hands the engine one broker-neutral ``Delivery`` at a time with
every broker's thresholds in effect. The engine records the delivery against
its detection, decides which of the object's detections are now owed a
forecast, forecasts each one in its own call, and stores every forecast or
refusal in the same transaction as the detection's state change. The result
says whether the broker message may be acknowledged; it is returned only
after that transaction has committed, so a crash before acknowledgement
causes a redelivery, which finds the forecast already stored.

A detection waits (pending) until its object qualifies on a detection
observed at or before it. Every pending detection is held for seven days
from its first delivery: if its object qualifies in that time it is
forecast, otherwise ``sweep_pending`` records it as missed (object qualified
on a later detection) or discards it (object never qualified).

Each forecast is made from the light curve carried by the alert that
delivered the detection, rebuilt from the points stored for that delivery.
When several brokers delivered the detection, the earliest-received
delivery is used.
"""

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

from django.conf import settings
from django.db import DatabaseError, DataError, transaction
from django.db.models import Min, Q, QuerySet

from seldon import services
from seldon.domain.errors import IncompatibleCheckpointError, SeldonRejection
from seldon.domain.intake.delivery import BrokerRole, Delivery, Point
from seldon.domain.intake.grid import LightCurveSpanError, forecast_grid
from seldon.domain.intake.light_curve import (
    forecast_request,
    light_curve_points,
    normalize_band,
    observed_key,
)
from seldon.domain.intake.qualification import Thresholds, qualifies
from seldon.domain.models.forecast import ObjectForecast
from seldon.domain.models.request import ObjectRequest
from seldon.models import Delivery as StoredDelivery
from seldon.models import (
    Detection,
    Forecast,
    IntakeSkip,
    LightCurvePoint,
    Refusal,
    RubinObject,
)

logger = logging.getLogger("seldon.intake")

#: How long an unforecast delivery is held for its object to qualify.
PENDING_HOLD = timedelta(days=7)

#: Intake-skip reason for a light curve longer than any Rubin alert carries.
SKIP_LIGHT_CURVE_SPAN = "light_curve_span"

#: Intake-skip reason for any other failure to build the forecast request.
SKIP_INVALID_REQUEST = "invalid_request"

#: Intake-skip reason for an alert holding a value the database cannot store.
SKIP_UNSTORABLE_VALUE = "unstorable_value"


@dataclass(frozen=True)
class IntakeResult:
    """What processing one delivery did, and whether to acknowledge it.

    Attributes:
        acknowledge: ``True`` once everything the delivery caused is
            committed; ``False`` when a database error rolled it back and
            the broker message must be left for redelivery.
        forecast: ``diaSourceId`` of each detection forecast.
        refused: ``diaSourceId`` of each detection the model refused.
        skip_reason: The intake-skip reason when the alert was skipped.
    """

    acknowledge: bool
    forecast: tuple[int, ...] = ()
    refused: tuple[int, ...] = ()
    skip_reason: str | None = None


@dataclass(frozen=True)
class SweepResult:
    """What one sweep of expired pending detections did.

    Attributes:
        missed: Detections recorded as missed.
        discarded: Detections discarded with their deliveries.
    """

    missed: int
    discarded: int


def record_intake_skip(
    broker: str, reason: str, occurred_at: datetime, raw_identifier: str = ""
) -> IntakeSkip:
    """Record an alert that intake could not turn into a forecast request.

    Adapters' callers use this for alerts that cannot be parsed or lack a
    required field; the engine uses it when the request cannot be built.

    Args:
        broker: The configured broker name.
        reason: Why the alert was skipped, as a short code.
        occurred_at: When it was skipped.
        raw_identifier: The alert's identifier as received, when readable.

    Returns:
        The stored skip.
    """
    logger.warning(
        "Skipped %s alert %s: %s", broker, raw_identifier or "(unknown)", reason
    )
    return IntakeSkip.objects.create(
        broker=broker,
        reason=reason,
        occurred_at=occurred_at,
        raw_identifier=raw_identifier[:255],
    )


def process_delivery(
    delivery: Delivery, thresholds: Mapping[str, Thresholds]
) -> IntakeResult:
    """Record one delivery and forecast every detection it makes owed.

    Args:
        delivery: One broker's delivery of one detection.
        thresholds: Every qualifying broker's cut and threshold in effect,
            by broker name; stored with each forecast.

    Returns:
        The result; acknowledge the broker message only when its
        ``acknowledge`` is ``True``.

    Raises:
        IncompatibleCheckpointError: If the configured checkpoint cannot be
            served; the process must stop.
        KeyError: If a qualifying broker has no entry in ``thresholds``.
    """
    try:
        try:
            points = light_curve_points(delivery)
            forecast_grid(points[0].observed_mjd, points[-1].observed_mjd)
        except (ValueError, ArithmeticError) as exc:
            reason = _skip_reason(exc)
            with transaction.atomic():
                record_intake_skip(
                    delivery.broker,
                    reason,
                    delivery.received_at,
                    str(delivery.dia_source_id),
                )
            return IntakeResult(acknowledge=True, skip_reason=reason)

        checkpoint = services.loaded_model().checkpoint_id
        try:
            with transaction.atomic():
                return _process(delivery, thresholds, checkpoint)
        except DataError:
            # The database refused a value in this alert: retrying cannot help.
            with transaction.atomic():
                record_intake_skip(
                    delivery.broker,
                    SKIP_UNSTORABLE_VALUE,
                    delivery.received_at,
                    str(delivery.dia_source_id),
                )
            return IntakeResult(acknowledge=True, skip_reason=SKIP_UNSTORABLE_VALUE)
    except DatabaseError:
        logger.exception(
            "Database error processing %s delivery of diaSource %s; "
            "leaving it unacknowledged",
            delivery.broker,
            delivery.dia_source_id,
        )
        return IntakeResult(acknowledge=False)


def sweep_pending(now: datetime) -> SweepResult:
    """Close every pending detection first delivered seven or more days ago.

    A detection whose object has qualified (on a later detection) is
    recorded as missed. One whose object never qualified is discarded with
    its deliveries, the points no other delivery carried, and the object
    itself when it has no detection left.

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
            expired_ids = [detection.pk for detection in expired]
            if rubin_object.qualifying_detection_id is not None:
                Detection.objects.filter(pk__in=expired_ids).update(
                    state=Detection.State.MISSED
                )
                missed += len(expired_ids)
                continue
            StoredDelivery.objects.filter(detection_id__in=expired_ids).delete()
            Detection.objects.filter(pk__in=expired_ids).delete()
            discarded += len(expired_ids)
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


def _skip_reason(exc: ValueError) -> str:
    """Name why a forecast request could not be built.

    Args:
        exc: The error request construction raised.

    Returns:
        The intake-skip reason.
    """
    if isinstance(exc, LightCurveSpanError):
        return SKIP_LIGHT_CURVE_SPAN
    return SKIP_INVALID_REQUEST


def _process(
    delivery: Delivery, thresholds: Mapping[str, Thresholds], checkpoint: str
) -> IntakeResult:
    """Do the work of ``process_delivery`` inside its transaction.

    The object's row is locked first, so deliveries of one object from
    several consumers are applied one at a time.

    Args:
        delivery: One broker's delivery of one detection.
        thresholds: Every qualifying broker's thresholds, by name.
        checkpoint: The current checkpoint's identifier.

    Returns:
        The result, acknowledgeable once the transaction commits.
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
    _record_delivery(delivery, detection)

    if delivery.role is BrokerRole.QUALIFYING:
        limits = thresholds[delivery.broker]
        if qualifies(delivery, limits):
            _qualify(rubin_object, detection, delivery, limits)

    if detection.state in (Detection.State.FORECAST, Detection.State.REFUSED):
        # A replay after a checkpoint upgrade is forecast again.
        if not (
            Forecast.objects.filter(detection=detection, checkpoint=checkpoint).exists()
            or Refusal.objects.filter(
                detection=detection, checkpoint=checkpoint
            ).exists()
        ):
            detection.state = Detection.State.OWED
            detection.save(update_fields=["state"])

    if rubin_object.qualifying_detection_id is not None:
        _owe_pending(rubin_object, delivery.received_at)

    forecast: list[int] = []
    refused: list[int] = []
    owed = rubin_object.detections.filter(state=Detection.State.OWED).order_by(
        "observed_mjd"
    )
    for owed_detection in owed:
        state = _forecast_detection(
            owed_detection, checkpoint, thresholds, delivery.received_at
        )
        if state == Detection.State.FORECAST:
            forecast.append(owed_detection.dia_source_id)
        elif state == Detection.State.REFUSED:
            refused.append(owed_detection.dia_source_id)
    return IntakeResult(
        acknowledge=True, forecast=tuple(forecast), refused=tuple(refused)
    )


def _record_delivery(delivery: Delivery, detection: Detection) -> None:
    """Store a broker's first delivery of a detection and the points it carried.

    A redelivery from the same broker changes nothing: the first delivery,
    with its times and points, is kept.

    Args:
        delivery: The broker's delivery.
        detection: The stored detection it delivers.
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
    since every detection observed at or after it is owed a forecast.

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


def _owe_pending(rubin_object: RubinObject, now: datetime) -> None:
    """Move the qualified object's pending detections to owed or missed.

    Every pending detection observed at or after the qualifying detection
    is owed, unless it was first delivered seven or more days ago: then it
    is missed.

    Args:
        rubin_object: The locked, qualified object.
        now: The time of the delivery being processed.
    """
    pending = rubin_object.detections.filter(
        state=Detection.State.PENDING,
        observed_mjd__gte=rubin_object.qualifying_detection.observed_mjd,
    ).annotate(first_received=Min("deliveries__received_at"))
    for detection in pending:
        if detection.first_received <= now - PENDING_HOLD:
            detection.state = Detection.State.MISSED
        else:
            detection.state = Detection.State.OWED
        detection.save(update_fields=["state"])


def _forecast_detection(
    detection: Detection,
    checkpoint: str,
    thresholds: Mapping[str, Thresholds],
    now: datetime,
) -> str:
    """Forecast one owed detection and store the forecast or the refusal.

    Args:
        detection: The owed detection.
        checkpoint: The current checkpoint's identifier.
        thresholds: Every qualifying broker's thresholds, by name.
        now: The time of the delivery being processed.

    Returns:
        The detection's state afterwards: forecast, refused, or still owed
        when its request could not be built.

    Raises:
        IncompatibleCheckpointError: If the checkpoint cannot be served.
    """
    source = detection.deliveries.order_by("received_at", "pk").first()
    try:
        request = forecast_request(_rebuilt_delivery(source, detection))
    except ValueError as exc:
        record_intake_skip(
            source.broker, _skip_reason(exc), now, str(detection.dia_source_id)
        )
        return detection.state

    try:
        (result,) = services.forecast([request])
    except IncompatibleCheckpointError:
        raise
    except SeldonRejection as exc:
        Refusal.objects.create(
            detection=detection,
            checkpoint=checkpoint,
            code=exc.code.value,
            message=str(exc),
            bands=list(getattr(exc, "bands", ())),
            app_version=settings.APP_VERSION,
        )
        detection.state = Detection.State.REFUSED
        detection.save(update_fields=["state"])
        logger.info(
            "Model refused diaSource %s: %s", detection.dia_source_id, exc.code.value
        )
        return detection.state

    _store_forecast(detection, request, result, thresholds)
    detection.state = Detection.State.FORECAST
    detection.save(update_fields=["state"])
    logger.info("Forecast diaSource %s", detection.dia_source_id)
    return detection.state


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


def _store_forecast(
    detection: Detection,
    request: ObjectRequest,
    result: ObjectForecast,
    thresholds: Mapping[str, Thresholds],
) -> Forecast:
    """Store a forecast with exactly what was passed and what came back.

    Args:
        detection: The detection forecast.
        request: The request passed to the forecast call.
        result: The forecast it returned.
        thresholds: Every qualifying broker's thresholds, by name.

    Returns:
        The stored forecast.
    """
    regime = result.regime
    provenance = result.provenance
    return Forecast.objects.create(
        detection=detection,
        checkpoint=provenance.checkpoint,
        light_curve={
            "times": request.times.tolist(),
            "flux": request.flux.tolist(),
            "flux_err": request.flux_err.tolist(),
            "detected": request.detected.tolist(),
            "bands": list(request.bands),
        },
        zero_point=request.zero_point,
        grid={
            "eval_times": request.eval_times.tolist(),
            "eval_bands": list(request.eval_bands),
        },
        outputs={
            "flux": result.flux.tolist(),
            "flux_err": result.flux_err.tolist(),
            "zero_point_mag": float(result.zero_point_mag),
            "parameters": {k: float(v) for k, v in result.parameters.items()},
            "class_probabilities": {
                k: float(v) for k, v in result.class_probabilities.items()
            },
            "predicted_class": result.predicted_class,
            "latent_mean": result.latent_mean.tolist(),
            "latent_sigma": result.latent_sigma.tolist(),
        },
        in_training_regime=regime.in_training_regime,
        sparse=regime.sparse,
        n_observations=regime.n_observations,
        min_points=regime.min_points,
        untrained_bands=list(regime.untrained_bands),
        eval_span_days=regime.eval_span_days,
        eval_span_in_time_scales=regime.eval_span_in_time_scales,
        time_scale_days=regime.time_scale_days,
        library_version=provenance.library_version,
        provenance_zero_point_mag=provenance.zero_point_mag,
        torch_version=provenance.torch_version,
        app_version=settings.APP_VERSION,
        thresholds={
            name: {
                "reliability_cut": limits.reliability_cut,
                "sn_like_threshold": limits.sn_like_threshold,
            }
            for name, limits in thresholds.items()
        },
        qualifying_event=detection.rubin_object.qualifying_event(),
    )
