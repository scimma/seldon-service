"""Tests for the intake engine: deliveries in, owed detections out.

Intake decides which detections are owed a product and records when and why;
it never calls a model. These run against the compose ``db`` service. Run
them with ``run/seldonctl slim_dev test``.

Every expected value is written out from the scenario: which detections are
owed, which brokers delivered them, and which points each alert carried.
"""

import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest import mock

from django.db import DatabaseError, IntegrityError, transaction
from django.test import SimpleTestCase, TestCase
from django.utils import timezone

from seldon import services
from seldon.domain.intake.delivery import BrokerRole, Delivery, Point
from seldon.domain.intake.qualification import Thresholds
from seldon.intake.engine import owed_packet, process_delivery, sweep_pending
from seldon.models import (
    Delivery as StoredDelivery,
)
from seldon.models import (
    Detection,
    Forecast,
    IntakeSkip,
    LightCurvePoint,
    OutcomeClaim,
    Refusal,
    RubinObject,
)

CHECKPOINT = "seldon-2.0-roman-elasticc/epoch=1086-val_loss=1.18.ckpt"
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


def detection_of(night: int, object_id: int = OBJECT_ID) -> Detection:
    """The stored detection of the object on a given night.

    Args:
        night: Nights after the first, starting at 0.
        object_id: The object's ``diaObjectId``.

    Returns:
        The detection.
    """
    return Detection.objects.get(dia_source_id=source_id(night, object_id))


def state_of(night: int, object_id: int = OBJECT_ID) -> str:
    """The stored state of the object's detection on a given night.

    Args:
        night: Nights after the first, starting at 0.
        object_id: The object's ``diaObjectId``.

    Returns:
        The detection's state.
    """
    return detection_of(night, object_id).state


def packet_times(night: int, object_id: int = OBJECT_ID) -> list[float]:
    """The observation times in the packet the detection is owed a product for.

    Args:
        night: Nights after the first, starting at 0.
        object_id: The object's ``diaObjectId``.

    Returns:
        Each packet point's time, in packet order.
    """
    return [p.time for p in owed_packet(detection_of(night, object_id)).points]


def qualifying_event(night: int, broker: str, scores: tuple[float, float]) -> dict:
    """The qualifying event a qualification snapshot records.

    Args:
        night: Night of the qualifying detection.
        broker: The broker that qualified it.
        scores: That broker's reliability and SN-like scores.

    Returns:
        The event as stored.
    """
    limits = STORED_THRESHOLDS[broker]
    return {
        "dia_source_id": source_id(night),
        "broker": broker,
        "reliability_score": scores[0],
        "sn_like_score": scores[1],
        "reliability_cut": limits["reliability_cut"],
        "sn_like_threshold": limits["sn_like_threshold"],
    }


class NoOutcomesMixin:
    """Asserts that intake stored no product, refusal, or claim."""

    def assertNoOutcomes(self) -> None:  # noqa: N802
        """No outcome of any kind, old or new, was stored."""
        self.assertFalse(OutcomeClaim.objects.exists())  # type: ignore[attr-defined]
        self.assertFalse(Forecast.objects.exists())  # type: ignore[attr-defined]
        self.assertFalse(Refusal.objects.exists())  # type: ignore[attr-defined]


class QualificationTests(NoOutcomesMixin, TestCase):
    """A detection is owed once some broker's scores qualify it."""

    def test_second_broker_above_threshold_qualifies_and_is_named(self) -> None:
        """Fink scores night 0 below its threshold, so the detection waits.
        Lasair then scores the same detection above its own threshold: the
        detection is owed, with the wall-clock time it became owed and a
        snapshot of every broker's thresholds and the qualifying event naming
        Lasair; the object's qualifying event names Lasair too, and no
        product is stored."""
        first = process_delivery(delivery(0, "fink", BELOW), THRESHOLDS)

        self.assertTrue(first.acknowledge)
        self.assertEqual(state_of(0), Detection.State.PENDING)
        self.assertIsNone(detection_of(0).owed_at)
        self.assertEqual(detection_of(0).qualification, {})

        before = timezone.now()
        second = process_delivery(
            delivery(0, "lasair", (0.8, 0.45), received_at=RECEIVED + timedelta(1)),
            THRESHOLDS,
        )
        after = timezone.now()

        self.assertTrue(second.acknowledge)
        self.assertEqual(second.owed, (source_id(0),))
        stored = detection_of(0)
        self.assertEqual(stored.state, Detection.State.OWED)
        self.assertTrue(before <= stored.owed_at <= after)
        self.assertEqual(
            stored.qualification,
            {
                "thresholds": STORED_THRESHOLDS,
                "qualifying_event": qualifying_event(0, "lasair", (0.8, 0.45)),
            },
        )
        self.assertNoOutcomes()

        rubin_object = RubinObject.objects.get(dia_object_id=OBJECT_ID)
        self.assertEqual(rubin_object.qualifying_detection.dia_source_id, source_id(0))
        self.assertEqual(rubin_object.qualifying_broker, "lasair")
        self.assertEqual(rubin_object.qualifying_reliability_score, 0.8)
        self.assertEqual(rubin_object.qualifying_sn_like_score, 0.45)
        self.assertEqual(rubin_object.qualifying_reliability_cut, 0.7)
        self.assertEqual(rubin_object.qualifying_sn_like_threshold, 0.4)
        self.assertIsNotNone(rubin_object.qualified_at)

        deliveries = {d.broker: d for d in stored.deliveries.all()}  # type: ignore
        self.assertEqual(sorted(deliveries), ["fink", "lasair"])
        self.assertEqual(deliveries["fink"].sn_like_score, 0.1)
        self.assertEqual(deliveries["lasair"].received_at, RECEIVED + timedelta(1))

    def test_second_broker_redelivering_owed_detection_changes_nothing_owed(
        self,
    ) -> None:
        """Night 0 is owed after Fink qualifies it. ALeRCE then delivers the
        same detection, also above threshold, under thresholds that now
        include its own: ALeRCE is added to its deliveries, and the time it
        became owed and its qualification snapshot are unchanged."""
        process_delivery(delivery(0, "fink", ABOVE), THRESHOLDS)
        owed = detection_of(0)
        thresholds = {**THRESHOLDS, "alerce": Thresholds(0.5, 0.5)}

        result = process_delivery(
            delivery(0, "alerce", ABOVE, received_at=RECEIVED + timedelta(hours=2)),
            thresholds,
        )

        self.assertTrue(result.acknowledge)
        self.assertEqual(result.owed, ())
        self.assertEqual(
            sorted(
                StoredDelivery.objects.filter(
                    detection__dia_source_id=source_id(0)
                ).values_list("broker", flat=True)
            ),
            ["alerce", "fink"],
        )
        again = detection_of(0)
        self.assertEqual(again.state, Detection.State.OWED)
        self.assertEqual(again.owed_at, owed.owed_at)
        self.assertEqual(again.qualification, owed.qualification)
        self.assertEqual(again.qualification["thresholds"], STORED_THRESHOLDS)
        self.assertNoOutcomes()

    def test_later_detection_below_threshold_from_full_stream_is_owed(
        self,
    ) -> None:
        """The object qualifies on its second detection (night 1, Fink above
        threshold; night 0 was below). Nights 2 and 3 arrive from Fink below
        threshold and are owed because the object has qualified. Night 4
        arrives only from Babamul, the full-stream source, with scores 0.2
        and 0.05: it is owed over all five nights, and those scores are
        stored on its delivery."""
        process_delivery(delivery(0, "fink", BELOW), THRESHOLDS)
        process_delivery(delivery(1, "fink", ABOVE), THRESHOLDS)
        process_delivery(delivery(2, "fink", BELOW), THRESHOLDS)
        process_delivery(delivery(3, "fink", BELOW), THRESHOLDS)

        result = process_delivery(delivery(4, "babamul", (0.2, 0.05)), THRESHOLDS)

        self.assertTrue(result.acknowledge)
        self.assertEqual(result.owed, (source_id(4),))
        self.assertEqual(state_of(0), Detection.State.PENDING)
        self.assertEqual(
            sorted(
                Detection.objects.filter(state=Detection.State.OWED).values_list(
                    "dia_source_id", flat=True
                )
            ),
            [source_id(1), source_id(2), source_id(3), source_id(4)],
        )
        self.assertEqual(
            packet_times(4), [61000.25, 61001.25, 61002.25, 61003.25, 61004.25]
        )
        (babamul,) = detection_of(4).deliveries.all()  # type: ignore
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
        self.assertNoOutcomes()


class PendingDetectionTests(TestCase):
    """Detections delivered before their object qualifies wait, then are owed."""

    def test_late_score_on_earlier_detection_owes_both(self) -> None:
        """Fink scores night 0 below threshold and Babamul delivers night 1;
        both wait. Lasair later scores night 0 above threshold: night 0 and
        night 1 are both owed, under Lasair's qualification."""
        process_delivery(delivery(0, "fink", BELOW), THRESHOLDS)
        process_delivery(delivery(1, "babamul"), THRESHOLDS)
        self.assertEqual(state_of(1), Detection.State.PENDING)

        result = process_delivery(delivery(0, "lasair", ABOVE), THRESHOLDS)

        self.assertTrue(result.acknowledge)
        self.assertEqual(result.owed, (source_id(0), source_id(1)))
        for night in (0, 1):
            owed = detection_of(night)
            self.assertEqual(owed.state, Detection.State.OWED)
            self.assertIsNotNone(owed.owed_at)
            self.assertEqual(
                owed.qualification["qualifying_event"],
                qualifying_event(0, "lasair", ABOVE),
            )

    def test_full_stream_detection_uses_only_its_own_alert_points(self) -> None:
        """Babamul delivers night 2, carrying night 1 but not night 0, before
        any qualifying broker has seen the object. Fink then delivers the
        earlier night 1 above threshold, carrying night 0. Both are owed,
        and night 2's packet holds nights 1 and 2 only, exactly the points
        Babamul's alert carried, though night 0 is now stored."""
        process_delivery(delivery(2, "babamul", previous=(point(1),)), THRESHOLDS)
        self.assertEqual(state_of(2), Detection.State.PENDING)

        process_delivery(delivery(1, "fink", ABOVE, previous=(point(0),)), THRESHOLDS)

        self.assertEqual(packet_times(1), [61000.25, 61001.25])
        later = owed_packet(detection_of(2))
        self.assertEqual(later.object_id, str(source_id(2)))
        self.assertEqual([p.time for p in later.points], [61001.25, 61002.25])
        self.assertEqual([p.flux for p in later.points], [1400.0, 1800.0])
        self.assertEqual(state_of(2), Detection.State.OWED)
        self.assertEqual(LightCurvePoint.objects.count(), 3)

    def test_earlier_detection_delivered_after_qualification_waits(self) -> None:
        """The object qualifies on night 2. Babamul then delivers night 1,
        observed before the qualifying detection: it is not owed and waits."""
        process_delivery(delivery(2, "fink", ABOVE), THRESHOLDS)

        process_delivery(delivery(1, "babamul"), THRESHOLDS)

        self.assertEqual(state_of(1), Detection.State.PENDING)
        self.assertIsNone(detection_of(1).owed_at)
        self.assertEqual(state_of(2), Detection.State.OWED)

    def test_qualifying_detection_delivered_over_seven_days_after_pending(
        self,
    ) -> None:
        """Babamul delivers night 1 on 1 November. Fink qualifies the object on
        night 0 eight days later, before any sweep: night 1 was delivered
        more than seven days before its object qualified, so it is recorded
        as missed instead of owed."""
        process_delivery(delivery(1, "babamul"), THRESHOLDS)

        process_delivery(
            delivery(0, "fink", ABOVE, received_at=RECEIVED + timedelta(days=8)),
            THRESHOLDS,
        )

        self.assertEqual(state_of(0), Detection.State.OWED)
        self.assertEqual(state_of(1), Detection.State.MISSED)
        self.assertIsNone(detection_of(1).owed_at)


class SkipTests(NoOutcomesMixin, TestCase):
    """Alerts or packets intake cannot use are intake skips, not owed."""

    def test_unknown_band_is_owed_for_the_model_to_judge(self) -> None:
        """Babamul delivers night 0 (band r) and night 1, whose alert also
        carries a night-0.5 point in band "w". Fink then qualifies night 0:
        both are owed, and night 1's packet keeps the "w" point, since
        whether a band can be used is the model package's decision."""
        odd = Point(observed_mjd=FIRST_NIGHT + 0.5, band="w", flux=900.0, flux_err=40.0)
        process_delivery(delivery(0, "babamul"), THRESHOLDS)
        process_delivery(delivery(1, "babamul", previous=(point(0), odd)), THRESHOLDS)

        result = process_delivery(delivery(0, "fink", ABOVE), THRESHOLDS)

        self.assertTrue(result.acknowledge)
        self.assertEqual(state_of(0), Detection.State.OWED)
        self.assertEqual(state_of(1), Detection.State.OWED)
        self.assertEqual(
            [(p.survey, p.band) for p in owed_packet(detection_of(1)).points],
            [("lsst", "r"), ("lsst", "w"), ("lsst", "r")],
        )
        self.assertNoOutcomes()

    def test_request_construction_failure_is_an_intake_skip(self) -> None:
        """Fink delivers night 500 above threshold, carrying night 0: the light
        curve spans 500 days, more than any Rubin alert carries, so no
        packet can be built from it. The alert is an intake skip,
        acknowledged, with no detection or outcome stored."""
        result = process_delivery(
            delivery(500, "fink", ABOVE, previous=(point(0),)), THRESHOLDS
        )

        self.assertTrue(result.acknowledge)
        (skip,) = IntakeSkip.objects.all()
        self.assertEqual(skip.broker, "fink")
        self.assertEqual(skip.reason, "light_curve_span")
        self.assertEqual(skip.raw_identifier, str(source_id(500)))
        self.assertEqual(skip.occurred_at, RECEIVED)
        self.assertFalse(Detection.objects.exists())
        self.assertNoOutcomes()

    def test_detection_whose_packet_cannot_be_built_is_skipped_not_owed(
        self,
    ) -> None:
        """Babamul delivers night 0 and Fink qualifies it an hour later, but
        no packet can be built from Babamul's earliest-received delivery: an
        intake skip is recorded once, under Babamul and the detection's
        diaSourceId, and the detection stays pending with no owed time or
        qualification. Lasair's later delivery records no second skip."""
        process_delivery(delivery(0, "babamul"), THRESHOLDS)

        with mock.patch(
            "seldon.intake.engine.build_packet",
            side_effect=ValueError("point 0 has a non-finite flux"),
        ):
            with self.assertLogs("seldon.intake", level="WARNING"):
                result = process_delivery(
                    delivery(
                        0, "fink", ABOVE, received_at=RECEIVED + timedelta(hours=1)
                    ),
                    THRESHOLDS,
                )
            process_delivery(
                delivery(0, "lasair", ABOVE, received_at=RECEIVED + timedelta(hours=2)),
                THRESHOLDS,
            )

        self.assertTrue(result.acknowledge)
        self.assertEqual(result.owed, ())
        (skip,) = IntakeSkip.objects.all()
        self.assertEqual(skip.broker, "babamul")
        self.assertEqual(skip.reason, "packet_build")
        self.assertEqual(skip.raw_identifier, str(source_id(0)))
        self.assertEqual(skip.occurred_at, RECEIVED + timedelta(hours=1))
        stored = detection_of(0)
        self.assertEqual(stored.state, Detection.State.PENDING)
        self.assertIsNone(stored.owed_at)
        self.assertEqual(stored.qualification, {})
        self.assertEqual(StoredDelivery.objects.count(), 3)
        self.assertNoOutcomes()


class ExactlyOnceTests(TestCase):
    """A detection becomes owed once, and only when its work commits."""

    def test_redelivery_keeps_owed_time_and_first_delivery_time(self) -> None:
        """Fink delivers night 0 above threshold, then delivers the same alert
        again an hour later: one delivery, kept with the first time, and the
        detection's owed time and qualification are unchanged."""
        process_delivery(delivery(0, "fink", ABOVE), THRESHOLDS)
        owed = detection_of(0)

        result = process_delivery(
            delivery(0, "fink", ABOVE, received_at=RECEIVED + timedelta(hours=1)),
            THRESHOLDS,
        )

        self.assertTrue(result.acknowledge)
        self.assertEqual(result.owed, ())
        (stored,) = StoredDelivery.objects.all()
        self.assertEqual(stored.received_at, RECEIVED)
        again = detection_of(0)
        self.assertEqual(again.owed_at, owed.owed_at)
        self.assertEqual(again.qualification, owed.qualification)

    def test_database_error_while_owing_is_not_acknowledgeable(self) -> None:
        """Saving the detection as owed fails with a database error: nothing
        from the delivery is stored and the result says not to acknowledge
        it."""
        real_save = Detection.save

        def failing_save(detection: Detection, *args: Any, **kwargs: Any) -> None:
            if "owed_at" in (kwargs.get("update_fields") or ()):
                raise DatabaseError("disk full")
            real_save(detection, *args, **kwargs)

        with (
            mock.patch.object(Detection, "save", failing_save),
            self.assertLogs("seldon.intake", level="ERROR"),
        ):
            result = process_delivery(delivery(0, "fink", ABOVE), THRESHOLDS)

        self.assertFalse(result.acknowledge)
        self.assertFalse(Detection.objects.exists())
        self.assertFalse(StoredDelivery.objects.exists())


class WithoutModelTests(TestCase):
    """Intake works with no model loaded and never asks for one."""

    def test_qualifying_delivery_is_owed_without_any_model_call(self) -> None:
        """Every way of reaching the model fails if called; Fink's qualifying
        delivery still leaves night 0 owed."""
        no_model = AssertionError("intake must not reach a model")
        with (
            mock.patch.object(services, "loaded_model", side_effect=no_model),
            mock.patch.object(services, "forecast", side_effect=no_model),
        ):
            result = process_delivery(delivery(0, "fink", ABOVE), THRESHOLDS)

        self.assertTrue(result.acknowledge)
        self.assertEqual(state_of(0), Detection.State.OWED)


class IntakeImportTests(SimpleTestCase):
    """The intake path imports none of the model stack."""

    def test_intake_modules_import_no_model_code(self) -> None:
        """A fresh interpreter set up for Django imports the engine and both
        intake commands without importing ``seldon.services``, the
        checkpoint-loading modules, or ``seldon_core``."""
        code = (
            "import sys\n"
            "import django\n"
            "django.setup()\n"
            "import seldon.intake.engine\n"
            "import seldon.management.commands.consume_alerts\n"
            "import seldon.management.commands.replay_alerts\n"
            "print(sorted(m for m in sys.modules if m == 'seldon.services'\n"
            "    or m.startswith(('seldon.infrastructure.ml', 'seldon_core'))))\n"
        )

        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            check=False,
            env={
                **os.environ,
                "DJANGO_SETTINGS_MODULE": "seldon_project.settings",
                "SELDON_WARMUP": "false",
            },
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "[]")


class SweepTests(TestCase):
    """Pending detections are held seven days, then missed or discarded."""

    def test_seven_day_old_pending_missed_if_qualified_else_discarded(
        self,
    ) -> None:
        """Night 0 of a qualified object (qualified on night 1) and night 0 of
        an object that never qualified were both delivered seven days ago,
        and night 1 of the unqualified object six days ago. Night 0 of the
        qualified object was observed before its qualifying detection, so it
        was never owed: the sweep discards it rather than counting it
        missed, keeping the qualified object and its owed detection. It
        discards the unqualified object's night 0 with its delivery and its
        points, and leaves the six-day-old detection waiting."""
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
        self.assertEqual(state_of(1), Detection.State.OWED)
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

    def test_sweep_never_touches_owed_detections(self) -> None:
        """Fink qualifies night 0, then Babamul delivers night 1 and is owed
        too; both were first delivered thirty days before the sweep. The
        sweep leaves both owed, with their deliveries, owed times and
        qualification snapshots."""
        process_delivery(delivery(0, "fink", ABOVE), THRESHOLDS)
        process_delivery(delivery(1, "babamul"), THRESHOLDS)
        before = {
            d.dia_source_id: (d.state, d.owed_at, d.qualification)
            for d in Detection.objects.all()
        }

        result = sweep_pending(RECEIVED + timedelta(days=30))

        self.assertEqual((result.missed, result.discarded), (0, 0))
        after = {
            d.dia_source_id: (d.state, d.owed_at, d.qualification)
            for d in Detection.objects.all()
        }
        self.assertEqual(after, before)
        self.assertEqual(
            {state for state, _, _ in after.values()}, {Detection.State.OWED}
        )
        self.assertEqual(StoredDelivery.objects.count(), 2)


class QualificationSnapshotTests(TestCase):
    """Each owed detection keeps the qualification in effect when it was owed."""

    def test_owed_detection_keeps_its_event_when_an_earlier_one_qualifies(
        self,
    ) -> None:
        """Night 2 qualifies on Fink and is owed. Lasair then qualifies the
        earlier night 0, which becomes the object's qualifying event; night
        2's snapshot still names Fink's night-2 qualification, and night 0's
        names Lasair's."""
        process_delivery(delivery(2, "fink", ABOVE), THRESHOLDS)
        process_delivery(delivery(0, "lasair", ABOVE), THRESHOLDS)

        self.assertEqual(
            RubinObject.objects.get(dia_object_id=OBJECT_ID).qualifying_broker,
            "lasair",
        )
        self.assertEqual(
            detection_of(2).qualification["qualifying_event"],
            qualifying_event(2, "fink", ABOVE),
        )
        self.assertEqual(
            detection_of(0).qualification["qualifying_event"],
            qualifying_event(0, "lasair", ABOVE),
        )


class RefusalIdentityTests(TestCase):
    """A detection is refused at most once per checkpoint in the old table."""

    def test_second_refusal_under_same_checkpoint_is_rejected(self) -> None:
        """The database refuses a duplicate refusal for detection and checkpoint."""
        rubin_object = RubinObject.objects.create(dia_object_id=OBJECT_ID)
        detection = Detection.objects.create(
            dia_source_id=source_id(0),
            rubin_object=rubin_object,
            state=Detection.State.OWED,
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
