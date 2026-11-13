"""Tests for the intake engine: deliveries in, forecasts and refusals stored.

These run against the compose ``db`` service and the real checkpoint; no
inference is mocked. Run them with ``run/seldonctl slim_dev test``.

Every expected value is written out from the scenario: which detections are
forecast, which brokers delivered them, and which points each alert carried.
"""

from datetime import UTC, datetime, timedelta
from unittest import mock

from django.db import DatabaseError, IntegrityError, transaction
from django.test import TestCase, override_settings

from seldon.domain.intake.delivery import BrokerRole, Delivery, Point
from seldon.domain.intake.qualification import Thresholds
from seldon.intake.engine import process_delivery, sweep_pending
from seldon.models import (
    Delivery as StoredDelivery,
)
from seldon.models import (
    Detection,
    Forecast,
    IntakeSkip,
    LightCurvePoint,
    Refusal,
    RubinObject,
)

CHECKPOINT = "seldon-2.0-roman-elasticc/epoch=1086-val_loss=1.18.ckpt"
OLD_CHECKPOINT = "seldon-1.9-roman-elasticc/epoch=900-val_loss=1.30.ckpt"
OBJECT_ID = 313953456790
OTHER_OBJECT_ID = 313953456791
FIRST_NIGHT = 61000.25
RECEIVED = datetime(2026, 11, 1, 6, 0, tzinfo=UTC)

# Fink and Lasair qualify; Babamul is the full-stream source and has none.
THRESHOLDS = {
    "fink": Thresholds(reliability_cut=0.6, sn_like_threshold=0.5),
    "lasair": Thresholds(reliability_cut=0.7, sn_like_threshold=0.4),
}
STORED_THRESHOLDS = {
    "fink": {"reliability_cut": 0.6, "sn_like_threshold": 0.5},
    "lasair": {"reliability_cut": 0.7, "sn_like_threshold": 0.4},
}
ROLES = {
    "fink": BrokerRole.QUALIFYING,
    "lasair": BrokerRole.QUALIFYING,
    "alerce": BrokerRole.QUALIFYING,
    "babamul": BrokerRole.FULL_STREAM,
}
ABOVE = (0.9, 0.9)
BELOW = (0.1, 0.1)


def source_id(night: int, object_id: int = OBJECT_ID) -> int:
    """The ``diaSourceId`` of the object's detection on a given night.

    Args:
        night: Nights after the first, starting at 0.
        object_id: The object's ``diaObjectId``.

    Returns:
        A distinct identifier per object and night.
    """
    return object_id * 100 + night


def point(night: int, band: str = "r", object_id: int = OBJECT_ID) -> Point:
    """The object's measurement on a given night, as a broker sends it.

    Args:
        night: Nights after the first, starting at 0.
        band: The band name as sent.
        object_id: The object's ``diaObjectId``.

    Returns:
        A brightening detection with its ``diaSourceId``.
    """
    return Point(
        observed_mjd=FIRST_NIGHT + night,
        band=band,
        flux=1000.0 + 400.0 * night,
        flux_err=50.0,
        dia_source_id=source_id(night, object_id),
    )


def delivery(
    night: int,
    broker: str,
    scores: tuple[float, float] | None = None,
    previous: tuple[Point, ...] | None = None,
    received_at: datetime = RECEIVED,
    object_id: int = OBJECT_ID,
) -> Delivery:
    """One broker's delivery of the detection on a given night.

    Args:
        night: Nights after the first, starting at 0.
        broker: The broker name; its role comes from ``ROLES``.
        scores: Reliability and SN-like score, or ``None`` for neither.
        previous: The earlier points the alert carried; by default every
            earlier night, in band r.
        received_at: When the service received it.
        object_id: The object's ``diaObjectId``.

    Returns:
        The broker-neutral delivery.
    """
    if previous is None:
        previous = tuple(point(n, object_id=object_id) for n in range(night))
    reliability, sn_like = scores if scores is not None else (None, None)
    return Delivery(
        dia_object_id=object_id,
        detection=point(night, object_id=object_id),
        previous=previous,
        broker=broker,
        role=ROLES[broker],
        reliability=reliability,
        sn_like_score=sn_like,
        published_at=received_at - timedelta(seconds=30),
        received_at=received_at,
        schema_version="test-1",
    )


def forecast_of(night: int, object_id: int = OBJECT_ID) -> Forecast:
    """The stored forecast of the object's detection on a given night.

    Args:
        night: Nights after the first, starting at 0.
        object_id: The object's ``diaObjectId``.

    Returns:
        The one forecast stored for that detection.
    """
    return Forecast.objects.get(detection__dia_source_id=source_id(night, object_id))


def state_of(night: int, object_id: int = OBJECT_ID) -> str:
    """The stored state of the object's detection on a given night.

    Args:
        night: Nights after the first, starting at 0.
        object_id: The object's ``diaObjectId``.

    Returns:
        The detection's state.
    """
    return Detection.objects.get(dia_source_id=source_id(night, object_id)).state


@override_settings(APP_VERSION="v9.8.7-test")
class QualificationTests(TestCase):
    """A detection is forecast once some broker's scores qualify it."""

    def test_second_broker_above_threshold_qualifies_and_is_named(self) -> None:
        """Fink scores night 0 below its threshold, so nothing is forecast and
        the detection waits. Lasair then scores the same detection above its
        own threshold: one forecast is stored, the object's qualifying event
        names Lasair with its scores and its cut and threshold, and the
        forecast records both deliveries and every broker's thresholds."""
        first = process_delivery(delivery(0, "fink", BELOW), THRESHOLDS)

        self.assertTrue(first.acknowledge)
        self.assertEqual(state_of(0), Detection.State.PENDING)
        self.assertFalse(Forecast.objects.exists())

        second = process_delivery(
            delivery(0, "lasair", (0.8, 0.45), received_at=RECEIVED + timedelta(1)),
            THRESHOLDS,
        )

        self.assertTrue(second.acknowledge)
        self.assertEqual(Forecast.objects.count(), 1)
        stored = forecast_of(0)
        self.assertEqual(stored.checkpoint, CHECKPOINT)
        self.assertEqual(stored.thresholds, STORED_THRESHOLDS)
        self.assertEqual(stored.app_version, "v9.8.7-test")
        self.assertEqual(stored.zero_point, 31.4)
        self.assertEqual(stored.light_curve["times"], [61000.25])
        self.assertEqual(stored.light_curve["flux"], [1000.0])
        self.assertEqual(stored.light_curve["bands"], ["r"])
        self.assertEqual(stored.grid["eval_times"][:2], [61000.25, 61001.25])
        self.assertEqual(len(stored.grid["eval_times"]), 6 * 61)
        self.assertTrue(stored.sparse)
        self.assertFalse(stored.in_training_regime)
        self.assertEqual(stored.n_observations, 1)
        self.assertEqual(state_of(0), Detection.State.FORECAST)

        rubin_object = RubinObject.objects.get(dia_object_id=OBJECT_ID)
        self.assertEqual(rubin_object.qualifying_detection.dia_source_id, source_id(0))
        self.assertEqual(rubin_object.qualifying_broker, "lasair")
        self.assertEqual(rubin_object.qualifying_reliability_score, 0.8)
        self.assertEqual(rubin_object.qualifying_sn_like_score, 0.45)
        self.assertEqual(rubin_object.qualifying_reliability_cut, 0.7)
        self.assertEqual(rubin_object.qualifying_sn_like_threshold, 0.4)
        self.assertIsNotNone(rubin_object.qualified_at)

        deliveries = {
            d.broker: d for d in stored.detection.deliveries.all()  # type: ignore
        }
        self.assertEqual(sorted(deliveries), ["fink", "lasair"])
        self.assertEqual(deliveries["fink"].sn_like_score, 0.1)
        self.assertEqual(deliveries["lasair"].received_at, RECEIVED + timedelta(1))

    def test_third_broker_of_forecast_detection_only_adds_delivery(self) -> None:
        """Night 0 is forecast after Fink qualifies it. ALeRCE then delivers
        the same detection, also above threshold: ALeRCE is added to its
        deliveries and no second forecast is made."""
        process_delivery(delivery(0, "fink", ABOVE), THRESHOLDS)
        thresholds = {**THRESHOLDS, "alerce": Thresholds(0.5, 0.5)}

        result = process_delivery(delivery(0, "alerce", ABOVE), thresholds)

        self.assertTrue(result.acknowledge)
        self.assertEqual(Forecast.objects.count(), 1)
        self.assertEqual(
            sorted(
                StoredDelivery.objects.filter(
                    detection__dia_source_id=source_id(0)
                ).values_list("broker", flat=True)
            ),
            ["alerce", "fink"],
        )
        self.assertEqual(state_of(0), Detection.State.FORECAST)

    def test_later_detection_below_threshold_from_full_stream_is_forecast(
        self,
    ) -> None:
        """The object qualifies on its second detection (night 1, Fink above
        threshold; night 0 was below). Nights 2 and 3 arrive from Fink below
        threshold and are forecast because the object has qualified. Night 4
        arrives only from Babamul, the full-stream source, with scores 0.2
        and 0.05: it is forecast over all five nights, and those scores are
        stored on its delivery."""
        process_delivery(delivery(0, "fink", BELOW), THRESHOLDS)
        process_delivery(delivery(1, "fink", ABOVE), THRESHOLDS)
        process_delivery(delivery(2, "fink", BELOW), THRESHOLDS)
        process_delivery(delivery(3, "fink", BELOW), THRESHOLDS)

        result = process_delivery(delivery(4, "babamul", (0.2, 0.05)), THRESHOLDS)

        self.assertTrue(result.acknowledge)
        self.assertEqual(state_of(0), Detection.State.PENDING)
        self.assertEqual(
            sorted(Forecast.objects.values_list("detection__dia_source_id", flat=True)),
            [source_id(1), source_id(2), source_id(3), source_id(4)],
        )
        fifth = forecast_of(4)
        self.assertEqual(
            fifth.light_curve["times"],
            [61000.25, 61001.25, 61002.25, 61003.25, 61004.25],
        )
        (babamul,) = fifth.detection.deliveries.all()  # type: ignore
        self.assertEqual(babamul.broker, "babamul")
        self.assertEqual(babamul.role, StoredDelivery.Role.FULL_STREAM)
        self.assertEqual(babamul.reliability_score, 0.2)
        self.assertEqual(babamul.sn_like_score, 0.05)
        self.assertEqual(
            RubinObject.objects.get(
                dia_object_id=OBJECT_ID
            ).qualifying_detection.dia_source_id,
            source_id(1),
        )


@override_settings(APP_VERSION="v9.8.7-test")
class PendingDetectionTests(TestCase):
    """Detections delivered before their object qualifies wait, then are owed."""

    def test_late_score_on_earlier_detection_forecasts_both(self) -> None:
        """Fink scores night 0 below threshold and Babamul delivers night 1;
        both wait. Lasair later scores night 0 above threshold: night 0 and
        night 1 are both forecast."""
        process_delivery(delivery(0, "fink", BELOW), THRESHOLDS)
        process_delivery(delivery(1, "babamul"), THRESHOLDS)
        self.assertEqual(state_of(1), Detection.State.PENDING)

        result = process_delivery(delivery(0, "lasair", ABOVE), THRESHOLDS)

        self.assertTrue(result.acknowledge)
        self.assertEqual(state_of(0), Detection.State.FORECAST)
        self.assertEqual(state_of(1), Detection.State.FORECAST)
        self.assertEqual(Forecast.objects.count(), 2)

    def test_full_stream_detection_uses_only_its_own_alert_points(self) -> None:
        """Babamul delivers night 2, carrying night 1 but not night 0, before
        any qualifying broker has seen the object. Fink then delivers the
        earlier night 1 above threshold, carrying night 0. Both are forecast,
        and night 2's forecast is made from nights 1 and 2 only, exactly the
        points Babamul's alert carried, though night 0 is now stored."""
        process_delivery(delivery(2, "babamul", previous=(point(1),)), THRESHOLDS)
        self.assertEqual(state_of(2), Detection.State.PENDING)

        process_delivery(delivery(1, "fink", ABOVE, previous=(point(0),)), THRESHOLDS)

        self.assertEqual(forecast_of(1).light_curve["times"], [61000.25, 61001.25])
        later = forecast_of(2)
        self.assertEqual(later.light_curve["times"], [61001.25, 61002.25])
        self.assertEqual(later.light_curve["flux"], [1400.0, 1800.0])
        self.assertEqual(later.grid["eval_times"][0], 61001.25)
        self.assertEqual(LightCurvePoint.objects.count(), 3)

    def test_earlier_detection_delivered_after_qualification_waits(self) -> None:
        """The object qualifies on night 2. Babamul then delivers night 1,
        observed before the qualifying detection: it is not owed and waits."""
        process_delivery(delivery(2, "fink", ABOVE), THRESHOLDS)

        process_delivery(delivery(1, "babamul"), THRESHOLDS)

        self.assertEqual(state_of(1), Detection.State.PENDING)
        self.assertEqual(Forecast.objects.count(), 1)

    def test_qualifying_detection_delivered_over_seven_days_after_pending(
        self,
    ) -> None:
        """Babamul delivers night 1 on 1 November. Fink qualifies the object on
        night 0 eight days later, before any sweep: night 1 was delivered
        more than seven days before its object qualified, so it is recorded
        as missed instead of forecast."""
        process_delivery(delivery(1, "babamul"), THRESHOLDS)

        process_delivery(
            delivery(0, "fink", ABOVE, received_at=RECEIVED + timedelta(days=8)),
            THRESHOLDS,
        )

        self.assertEqual(state_of(0), Detection.State.FORECAST)
        self.assertEqual(state_of(1), Detection.State.MISSED)
        self.assertEqual(Forecast.objects.count(), 1)


@override_settings(APP_VERSION="v9.8.7-test")
class RefusalAndSkipTests(TestCase):
    """Model refusals close a detection; malformed alerts are intake skips."""

    def test_unknown_band_refuses_one_detection_and_forecasts_the_other(
        self,
    ) -> None:
        """Babamul delivers night 0 (band r) and night 1, whose alert also
        carries a night-0.5 point in band "w". Fink then qualifies night 0:
        both are owed, night 0 is forecast, and night 1 is refused as
        unknown_band naming "w"."""
        odd = Point(observed_mjd=FIRST_NIGHT + 0.5, band="w", flux=900.0, flux_err=40.0)
        process_delivery(delivery(0, "babamul"), THRESHOLDS)
        process_delivery(delivery(1, "babamul", previous=(point(0), odd)), THRESHOLDS)

        result = process_delivery(delivery(0, "fink", ABOVE), THRESHOLDS)

        self.assertTrue(result.acknowledge)
        self.assertEqual(Forecast.objects.count(), 1)
        self.assertEqual(state_of(0), Detection.State.FORECAST)
        self.assertEqual(state_of(1), Detection.State.REFUSED)
        refusal = Refusal.objects.get(detection__dia_source_id=source_id(1))
        self.assertEqual(refusal.code, "unknown_band")
        self.assertEqual(refusal.bands, ["w"])
        self.assertIn("w", refusal.message)
        self.assertEqual(refusal.checkpoint, CHECKPOINT)
        self.assertEqual(refusal.app_version, "v9.8.7-test")

    def test_refused_detection_redelivered_adds_delivery_only(self) -> None:
        """Night 0 is refused for its band "w". Lasair delivers it again
        above threshold: Lasair is added and no second refusal is stored."""
        odd = Point(observed_mjd=FIRST_NIGHT - 1, band="w", flux=900.0, flux_err=40.0)
        process_delivery(delivery(0, "fink", ABOVE, previous=(odd,)), THRESHOLDS)

        process_delivery(delivery(0, "lasair", ABOVE, previous=(odd,)), THRESHOLDS)

        self.assertEqual(Refusal.objects.count(), 1)
        self.assertEqual(StoredDelivery.objects.count(), 2)
        self.assertEqual(state_of(0), Detection.State.REFUSED)

    def test_request_construction_failure_is_an_intake_skip(self) -> None:
        """Fink delivers night 500 above threshold, carrying night 0: the light
        curve spans 500 days, more than any Rubin alert carries, so the
        request cannot be built. The alert is an intake skip, acknowledged,
        with no refusal, forecast, or detection stored."""
        result = process_delivery(
            delivery(500, "fink", ABOVE, previous=(point(0),)), THRESHOLDS
        )

        self.assertTrue(result.acknowledge)
        (skip,) = IntakeSkip.objects.all()
        self.assertEqual(skip.broker, "fink")
        self.assertEqual(skip.reason, "light_curve_span")
        self.assertEqual(skip.raw_identifier, str(source_id(500)))
        self.assertEqual(skip.occurred_at, RECEIVED)
        self.assertFalse(Refusal.objects.exists())
        self.assertFalse(Forecast.objects.exists())
        self.assertFalse(Detection.objects.exists())


@override_settings(APP_VERSION="v9.8.7-test")
class ExactlyOnceTests(TestCase):
    """A detection is forecast once per checkpoint, and only after commit."""

    def test_redelivery_leaves_one_forecast_and_first_delivery_time(self) -> None:
        """Fink delivers night 0 above threshold, then delivers the same alert
        again an hour later: one forecast, one delivery, the first time."""
        process_delivery(delivery(0, "fink", ABOVE), THRESHOLDS)

        result = process_delivery(
            delivery(0, "fink", ABOVE, received_at=RECEIVED + timedelta(hours=1)),
            THRESHOLDS,
        )

        self.assertTrue(result.acknowledge)
        self.assertEqual(Forecast.objects.count(), 1)
        (stored,) = StoredDelivery.objects.all()
        self.assertEqual(stored.received_at, RECEIVED)

    def test_database_error_during_store_is_not_acknowledgeable(self) -> None:
        """Saving the forecast fails with a database error: nothing from the
        delivery is stored and the result says not to acknowledge it."""
        with mock.patch.object(
            Forecast, "save", side_effect=DatabaseError("disk full")
        ):
            result = process_delivery(delivery(0, "fink", ABOVE), THRESHOLDS)

        self.assertFalse(result.acknowledge)
        self.assertFalse(Forecast.objects.exists())
        self.assertFalse(Detection.objects.exists())
        self.assertFalse(StoredDelivery.objects.exists())

    def test_replay_after_checkpoint_upgrade_forecasts_again(self) -> None:
        """Night 0 was forecast under an older checkpoint. Fink's alert is
        replayed under the current one: a second forecast is stored under
        the current checkpoint and the old one is kept."""
        process_delivery(delivery(0, "fink", ABOVE), THRESHOLDS)
        Forecast.objects.update(checkpoint=OLD_CHECKPOINT)

        process_delivery(delivery(0, "fink", ABOVE), THRESHOLDS)

        self.assertCountEqual(
            Forecast.objects.values_list("checkpoint", flat=True),
            [CHECKPOINT, OLD_CHECKPOINT],
        )


@override_settings(APP_VERSION="v9.8.7-test")
class SweepTests(TestCase):
    """Pending detections are held seven days, then missed or discarded."""

    def test_seven_day_old_pending_missed_if_qualified_else_discarded(
        self,
    ) -> None:
        """Night 0 of a qualified object (qualified on night 1) and night 0 of
        an object that never qualified were both delivered seven days ago,
        and night 1 of the unqualified object six days ago. Night 0 of the
        qualified object was observed before its qualifying detection, so it
        was never owed a forecast: the sweep discards it rather than counting
        it missed, keeping the qualified object and its forecast. It discards
        the unqualified object's night 0 with its delivery and its points,
        and leaves the six-day-old detection waiting."""
        now = RECEIVED + timedelta(days=7)
        process_delivery(delivery(1, "fink", ABOVE), THRESHOLDS)
        process_delivery(delivery(0, "babamul", previous=()), THRESHOLDS)
        process_delivery(
            delivery(0, "babamul", previous=(), object_id=OTHER_OBJECT_ID),
            THRESHOLDS,
        )
        process_delivery(
            delivery(
                1,
                "babamul",
                previous=(),
                received_at=RECEIVED + timedelta(days=1),
                object_id=OTHER_OBJECT_ID,
            ),
            THRESHOLDS,
        )

        sweep_pending(now)

        self.assertFalse(Detection.objects.filter(dia_source_id=source_id(0)).exists())
        self.assertEqual(state_of(1), Detection.State.FORECAST)
        self.assertFalse(
            Detection.objects.filter(state=Detection.State.MISSED).exists()
        )
        self.assertFalse(
            Detection.objects.filter(
                dia_source_id=source_id(0, OTHER_OBJECT_ID)
            ).exists()
        )
        self.assertFalse(
            LightCurvePoint.objects.filter(
                dia_source_id=source_id(0, OTHER_OBJECT_ID)
            ).exists()
        )
        self.assertEqual(state_of(1, OTHER_OBJECT_ID), Detection.State.PENDING)
        self.assertEqual(Forecast.objects.count(), 1)


class StoredQualifyingEventTests(TestCase):
    """Each forecast keeps the qualifying event in effect when it was made."""

    def test_forecast_keeps_its_event_when_an_earlier_detection_qualifies(
        self,
    ) -> None:
        """Night 2 qualifies on Fink and is forecast. Lasair then qualifies the
        earlier night 0, which becomes the object's qualifying event; night 2's
        stored forecast still names Fink's night-2 qualification."""
        process_delivery(delivery(2, "fink", ABOVE), THRESHOLDS)
        process_delivery(delivery(0, "lasair", ABOVE), THRESHOLDS)

        self.assertEqual(
            RubinObject.objects.get(dia_object_id=OBJECT_ID).qualifying_broker,
            "lasair",
        )
        self.assertEqual(
            forecast_of(2).qualifying_event,
            {
                "dia_source_id": source_id(2),
                "broker": "fink",
                "reliability_score": 0.9,
                "sn_like_score": 0.9,
                "reliability_cut": 0.6,
                "sn_like_threshold": 0.5,
            },
        )
        self.assertEqual(forecast_of(0).qualifying_event["broker"], "lasair")


class RefusalIdentityTests(TestCase):
    """A detection is refused at most once per checkpoint."""

    def test_second_refusal_under_same_checkpoint_is_rejected(self) -> None:
        """The database refuses a duplicate refusal for detection and checkpoint."""
        rubin_object = RubinObject.objects.create(dia_object_id=OBJECT_ID)
        detection = Detection.objects.create(
            dia_source_id=source_id(0),
            rubin_object=rubin_object,
            state=Detection.State.REFUSED,
            observed_mjd=FIRST_NIGHT,
        )
        fields = {
            "detection": detection,
            "checkpoint": CHECKPOINT,
            "code": "unknown_band",
            "message": "Unknown bands: 'w'.",
            "bands": ["w"],
            "app_version": "v9.8.7-test",
        }
        Refusal.objects.create(**fields)

        with self.assertRaises(IntegrityError), transaction.atomic():
            Refusal.objects.create(**fields)


class UnstorableValueTests(TestCase):
    """A value the database cannot store is skipped, not retried forever."""

    def test_value_too_long_for_its_column_is_an_acknowledged_skip(self) -> None:
        """A delivery whose band name exceeds the stored eight characters makes
        the database refuse the row. That is the alert's fault, not an outage:
        it is recorded as an intake skip and acknowledged."""
        bad = Point(
            observed_mjd=FIRST_NIGHT,
            band="verylongband",
            flux=1000.0,
            flux_err=50.0,
            dia_source_id=source_id(0),
        )
        result = process_delivery(
            Delivery(
                dia_object_id=OBJECT_ID,
                detection=bad,
                previous=(),
                broker="babamul",
                role=BrokerRole.FULL_STREAM,
                reliability=None,
                sn_like_score=None,
                published_at=RECEIVED,
                received_at=RECEIVED,
            ),
            THRESHOLDS,
        )

        self.assertTrue(result.acknowledge)
        (skip,) = IntakeSkip.objects.all()
        self.assertEqual(skip.reason, "unstorable_value")
        self.assertFalse(Detection.objects.exists())
