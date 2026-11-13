"""Tests for the ``alert_report`` inspection command.

Every expected number below is worked out by hand from the records each test
builds. These need the compose ``db`` service; run them with
``run/seldonctl slim_dev test``.
"""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from io import StringIO
from types import SimpleNamespace
from typing import Any
from unittest import mock

from django.apps import apps
from django.core.management import CommandError, call_command
from django.test import TestCase

from seldon.models import (
    Delivery,
    Detection,
    Forecast,
    IntakeSkip,
    Refusal,
    RubinObject,
    StreamGap,
)

CHECKPOINT = "seldon-2.0-roman-elasticc/epoch=1086-val_loss=1.18.ckpt"
NEXT_CHECKPOINT = "seldon-2.1-roman-elasticc/epoch=1200-val_loss=1.10.ckpt"
MJD_EPOCH = datetime(1858, 11, 17, tzinfo=UTC)
TAI_MINUS_UTC = timedelta(seconds=37)
COMMAND = "seldon.management.commands.alert_report"


def mjd_tai(utc: datetime) -> float:
    """Express a UTC time as the MJD in TAI that Rubin would report.

    Args:
        utc: The time, in UTC.

    Returns:
        The MJD of that instant on the TAI scale.
    """
    return (utc + TAI_MINUS_UTC - MJD_EPOCH) / timedelta(days=1)


def at(text: str) -> datetime:
    """Read a UTC time written as ``YYYY-MM-DD HH:MM:SS``.

    Args:
        text: The time.

    Returns:
        The aware datetime.
    """
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


def make_detection(
    rubin_object: RubinObject, dia_source_id: int, observed: datetime, state: str
) -> Detection:
    """Store a detection observed at a UTC time.

    Args:
        rubin_object: The object it belongs to.
        dia_source_id: Its ``diaSourceId``.
        observed: When it was observed, in UTC.
        state: Its state.

    Returns:
        The stored detection.
    """
    return Detection.objects.create(
        dia_source_id=dia_source_id,
        rubin_object=rubin_object,
        state=state,
        observed_mjd=mjd_tai(observed),
    )


def make_delivery(
    detection: Detection,
    broker: str,
    published: datetime | None,
    received: datetime,
    role: str = Delivery.Role.QUALIFYING,
) -> Delivery:
    """Store one broker's delivery of a detection.

    Args:
        detection: The detection delivered.
        broker: The broker name.
        published: When the broker published it, or ``None``.
        received: When the service received it.
        role: The broker's role.

    Returns:
        The stored delivery.
    """
    return Delivery.objects.create(
        detection=detection,
        broker=broker,
        role=role,
        published_at=published,
        received_at=received,
    )


def make_forecast(
    detection: Detection,
    stored: datetime,
    checkpoint: str = CHECKPOINT,
    qualifying_event: dict[str, Any] | None = None,
    **regime: Any,
) -> Forecast:
    """Store a forecast of a detection and set when it was stored.

    Args:
        detection: The detection forecast.
        stored: The stored time to record.
        checkpoint: The checkpoint that produced it.
        qualifying_event: The qualifying event snapshot.
        **regime: Overrides for the regime and output fields.

    Returns:
        The stored forecast, re-read so ``stored_at`` is the one set.
    """
    fields: dict[str, Any] = {
        "in_training_regime": True,
        "sparse": False,
        "n_observations": 7,
        "outputs": {"predicted_class": "SNIa"},
    }
    fields.update(regime)
    forecast = Forecast.objects.create(
        detection=detection,
        checkpoint=checkpoint,
        light_curve={},
        zero_point=31.4,
        grid={},
        min_points=5,
        untrained_bands=[],
        eval_span_days=60.0,
        eval_span_in_time_scales=0.6,
        time_scale_days=100.0,
        library_version="1.2.0",
        provenance_zero_point_mag=27.5,
        torch_version="2.9.0+cpu",
        app_version="v0.0.0-test",
        thresholds={},
        qualifying_event=qualifying_event or {},
        **fields,
    )
    Forecast.objects.filter(pk=forecast.pk).update(stored_at=stored)
    return Forecast.objects.get(pk=forecast.pk)


def make_refusal(
    detection: Detection, stored: datetime, checkpoint: str = CHECKPOINT
) -> Refusal:
    """Store a refusal of a detection and set when it was stored.

    Args:
        detection: The detection refused.
        stored: The stored time to record.
        checkpoint: The checkpoint that refused it.

    Returns:
        The stored refusal.
    """
    refusal = Refusal.objects.create(
        detection=detection,
        checkpoint=checkpoint,
        code="no_usable_observations",
        message="no usable observations",
        app_version="v0.0.0-test",
    )
    Refusal.objects.filter(pk=refusal.pk).update(stored_at=stored)
    return Refusal.objects.get(pk=refusal.pk)


def report(*args: str) -> str:
    """Run the report command and capture what it printed.

    Args:
        *args: The subcommand and its arguments.

    Returns:
        The command's standard output.
    """
    out = StringIO()
    call_command("alert_report", *args, stdout=out)
    return out.getvalue()


def report_json(*args: str) -> Any:
    """Run the report command with ``--json`` and parse its output.

    Args:
        *args: The subcommand and its arguments.

    Returns:
        The parsed JSON document.
    """
    return json.loads(report(*args, "--json"))


def database_fingerprint() -> dict[str, tuple[int, str]]:
    """Count and hash every row of every table the service stores.

    Returns:
        For each table, its row count and a digest of every row's values.
    """
    fingerprint = {}
    models = list(apps.get_app_config("seldon").get_models())
    models.append(Delivery.points.through)
    for model in models:
        rows = list(model.objects.order_by("pk").values())
        digest = hashlib.sha256(repr(rows).encode()).hexdigest()
        fingerprint[model._meta.db_table] = (len(rows), digest)
    return fingerprint


class ObjectReportTests(TestCase):
    """One object's forecasts and refusals, in observation order."""

    @classmethod
    def setUpTestData(cls) -> None:
        """An object with three forecasts and one refusal, stored out of order.

        Its four detections are observed on 1, 2, 3, and 4 November. The
        third is forecast first, the second is refused, the first is forecast
        under the current checkpoint, and the fourth under the next one. A
        second object has a forecast of its own that must not appear.
        """
        cls.obj = RubinObject.objects.create(
            dia_object_id=1001,
            qualifying_broker="lasair",
            qualifying_reliability_score=0.9,
            qualifying_sn_like_score=1.0,
            qualifying_reliability_cut=0.6,
            qualifying_sn_like_threshold=0.5,
        )
        first = make_detection(cls.obj, 11, at("2026-11-01 04:00:00"), "forecast")
        second = make_detection(cls.obj, 12, at("2026-11-02 04:00:00"), "refused")
        third = make_detection(cls.obj, 13, at("2026-11-03 04:00:00"), "forecast")
        fourth = make_detection(cls.obj, 14, at("2026-11-04 04:00:00"), "forecast")
        cls.obj.qualifying_detection = first
        cls.obj.save()
        event = {
            "dia_source_id": 11,
            "broker": "lasair",
            "reliability_score": 0.9,
            "sn_like_score": 1.0,
            "reliability_cut": 0.6,
            "sn_like_threshold": 0.5,
        }
        cls.event = event
        received = at("2026-11-05 00:00:00")
        make_delivery(first, "lasair", received, received)
        make_delivery(first, "babamul", received, received, "full_stream")
        make_delivery(second, "fink", received, received)
        make_delivery(third, "babamul", received, received, "full_stream")
        make_delivery(fourth, "fink", received, received)
        make_forecast(third, at("2026-11-05 00:01:00"), qualifying_event=event)
        make_refusal(second, at("2026-11-05 00:02:00"))
        make_forecast(
            first,
            at("2026-11-05 00:03:00"),
            qualifying_event=event,
            in_training_regime=False,
            sparse=True,
            n_observations=2,
            outputs={"predicted_class": "SNII"},
        )
        make_forecast(
            fourth,
            at("2026-11-05 00:04:00"),
            checkpoint=NEXT_CHECKPOINT,
            qualifying_event=event,
        )
        other = RubinObject.objects.create(dia_object_id=2002)
        stray = make_detection(other, 21, at("2026-11-01 05:00:00"), "forecast")
        make_forecast(stray, at("2026-11-05 00:05:00"))

    def test_lists_forecasts_and_refusals_in_observation_order(self) -> None:
        """Three forecasts and one refusal come back in observation order.

        Each record names its detection, checkpoint, regime mark, predicted
        class, delivering brokers, and the qualifying event it was stored
        with; the refusal carries its code instead of a class.
        """
        result = report_json("object", "1001")

        self.assertEqual(result["dia_object_id"], 1001)
        records = result["records"]
        self.assertEqual(
            [(r["kind"], r["dia_source_id"]) for r in records],
            [("forecast", 11), ("refusal", 12), ("forecast", 13), ("forecast", 14)],
        )
        self.assertEqual(
            [r["checkpoint"] for r in records],
            [CHECKPOINT, CHECKPOINT, CHECKPOINT, NEXT_CHECKPOINT],
        )
        first = records[0]
        self.assertFalse(first["in_training_regime"])
        self.assertTrue(first["sparse"])
        self.assertEqual(first["n_observations"], 2)
        self.assertEqual(first["predicted_class"], "SNII")
        self.assertEqual(first["brokers"], ["babamul", "lasair"])
        self.assertEqual(first["qualifying_event"], self.event)
        self.assertEqual(first["observed_utc"], "2026-11-01T04:00:00+00:00")
        self.assertEqual(first["night"], "2026-10-31")
        refusal = records[1]
        self.assertEqual(refusal["code"], "no_usable_observations")
        self.assertIsNone(refusal["predicted_class"])
        self.assertTrue(records[2]["in_training_regime"])
        self.assertEqual(records[2]["n_observations"], 7)

    def test_plain_text_prints_records_in_observation_order(self) -> None:
        """The plain-text listing shows the four records in the same order."""
        text = report("object", "1001")

        positions = [text.index(f"diaSource {n}") for n in (11, 12, 13, 14)]
        self.assertEqual(positions, sorted(positions))
        self.assertIn(NEXT_CHECKPOINT, text)
        self.assertIn("out of regime", text)
        self.assertNotIn("diaSource 21", text)

    def test_unknown_object_is_an_error(self) -> None:
        """Asking for an object the service never received fails clearly."""
        with self.assertRaises(CommandError):
            report("object", "9999")


class CoverageReportTests(TestCase):
    """Coverage of owed detections per broker and per night."""

    @classmethod
    def setUpTestData(cls) -> None:
        """Seven detections around the boundary between two nights.

        The night of 11 November runs from 11 November 12:00 UTC to
        12 November 12:00 UTC. In it: a detection at 11:59 UTC on
        12 November forecast (Fink and Babamul delivered it), one at 23:00
        refused (Lasair), one at 03:00 missed (Fink), one at 04:00 still owed
        (Fink), and one forecast (Lasair) at 11:59:43 UTC, whose TAI time is
        already past noon. One at 05:00 is pending and counts nowhere. In the
        night of 12 November: one at 12:01 UTC forecast (Fink).
        """
        obj = RubinObject.objects.create(dia_object_id=3003)
        received = at("2026-11-12 13:00:00")
        rows = [
            (31, "2026-11-12 11:59:00", "forecast", ["fink", "babamul"]),
            (32, "2026-11-12 12:01:00", "forecast", ["fink"]),
            (33, "2026-11-11 23:00:00", "refused", ["lasair"]),
            (34, "2026-11-12 03:00:00", "missed", ["fink"]),
            (35, "2026-11-12 04:00:00", "owed", ["fink"]),
            (36, "2026-11-12 05:00:00", "pending", ["fink"]),
            (37, "2026-11-12 11:59:43", "forecast", ["lasair"]),
        ]
        for dia_source_id, observed, state, brokers in rows:
            detection = make_detection(obj, dia_source_id, at(observed), state)
            for broker in brokers:
                make_delivery(detection, broker, received, received)

    def test_counts_per_broker_per_night(self) -> None:
        """Each night's counts per broker, and over all brokers, by hand.

        Night of 11 November: Fink delivered three owed detections (one
        forecast, one missed, one owed), share one third; Lasair two (one
        refused, one forecast), share one; Babamul one forecast, share one.
        Over all brokers five detections, three forecast or refused. Night of
        12 November: Fink's one forecast.
        """
        result = report_json("coverage")

        nights = {n["night"]: n for n in result["nights"]}
        self.assertEqual(sorted(nights), ["2026-11-11", "2026-11-12"])
        earlier = nights["2026-11-11"]
        self.assertEqual(
            earlier["brokers"]["fink"],
            {
                "detections": 3,
                "forecast": 1,
                "refused": 0,
                "missed": 1,
                "owed": 1,
                "share": 1 / 3,
            },
        )
        self.assertEqual(
            earlier["brokers"]["lasair"],
            {
                "detections": 2,
                "forecast": 1,
                "refused": 1,
                "missed": 0,
                "owed": 0,
                "share": 1.0,
            },
        )
        self.assertEqual(earlier["brokers"]["babamul"]["detections"], 1)
        self.assertEqual(earlier["brokers"]["babamul"]["share"], 1.0)
        self.assertEqual(
            earlier["all"],
            {
                "detections": 5,
                "forecast": 2,
                "refused": 1,
                "missed": 1,
                "owed": 1,
                "share": 0.6,
            },
        )
        later = nights["2026-11-12"]
        self.assertEqual(list(later["brokers"]), ["fink"])
        self.assertEqual(later["brokers"]["fink"]["forecast"], 1)
        self.assertEqual(later["brokers"]["fink"]["share"], 1.0)

    def test_night_range_selects_nights(self) -> None:
        """Asking for the night of 12 November alone leaves out the earlier one."""
        result = report_json(
            "coverage", "--from-night", "2026-11-12", "--to-night", "2026-11-12"
        )

        self.assertEqual([n["night"] for n in result["nights"]], ["2026-11-12"])
        self.assertEqual(result["nights"][0]["all"]["detections"], 1)

    def test_plain_text_names_each_night_and_broker(self) -> None:
        """The plain-text table has a row per night and broker."""
        text = report("coverage")

        self.assertIn("2026-11-11", text)
        self.assertIn("fink", text)
        self.assertIn("33.3%", text)
        self.assertIn("60.0%", text)


class LatencyReportTests(TestCase):
    """Publish-to-stored latency, leaving out replays."""

    @classmethod
    def setUpTestData(cls) -> None:
        """Six forecasts of one object, with the silence period at 60 minutes.

        Times are minutes after 12 November 01:00 UTC.

        - Qualifying detection 41, qualified by Lasair: Fink published it at
          0 and it arrived at 1; Lasair published it at 2 and it arrived at 3;
          forecast stored at 6. Measured from Lasair: 4 minutes.
        - Later detection 42: Babamul published at 10, arrived at 10.5;
          Fink published at 11, arrived at 12; stored at 18. Measured from
          the earliest arrival, Babamul's: 8 minutes.
        - Detection 43: published at 20, arrived at 140 (a replay); stored
          at 141. Left out.
        - Detection 44: no publish time; stored at 30. Counted apart.
        - Detection 45: published at 40, arrived at 45; stored at 60:
          20 minutes.
        - Detection 46: refused at 50 under the current checkpoint and
          forecast at 500 under the next one. A re-forecast; left out.
        """
        start = at("2026-11-12 01:00:00")

        def t(minutes: float) -> datetime:
            return start + timedelta(minutes=minutes)

        obj = RubinObject.objects.create(dia_object_id=4004)
        event = {"dia_source_id": 41, "broker": "lasair"}
        observed = at("2026-11-12 00:00:00")
        qualifying = make_detection(obj, 41, observed, "forecast")
        make_delivery(qualifying, "fink", t(0), t(1))
        make_delivery(qualifying, "lasair", t(2), t(3))
        make_forecast(qualifying, t(6), qualifying_event=event)

        later = make_detection(obj, 42, observed + timedelta(hours=1), "forecast")
        make_delivery(later, "babamul", t(10), t(10.5), "full_stream")
        make_delivery(later, "fink", t(11), t(12))
        make_forecast(later, t(18), qualifying_event=event)

        replayed = make_detection(obj, 43, observed + timedelta(hours=2), "forecast")
        make_delivery(replayed, "fink", t(20), t(140))
        make_forecast(replayed, t(141), qualifying_event=event)

        unpublished = make_detection(obj, 44, observed + timedelta(hours=3), "forecast")
        make_delivery(unpublished, "fink", None, t(25))
        make_forecast(unpublished, t(30), qualifying_event=event)

        slow = make_detection(obj, 45, observed + timedelta(hours=4), "forecast")
        make_delivery(slow, "fink", t(40), t(45))
        make_forecast(slow, t(60), qualifying_event=event)

        upgraded = make_detection(obj, 46, observed + timedelta(hours=5), "forecast")
        make_delivery(upgraded, "fink", t(48), t(49))
        make_refusal(upgraded, t(50))
        make_forecast(
            upgraded, t(500), checkpoint=NEXT_CHECKPOINT, qualifying_event=event
        )

    def test_percentile_excludes_replayed_deliveries(self) -> None:
        """Measured latencies are 4, 8, and 20 minutes; the replay is left out.

        By nearest rank the 90th percentile of three values is the third,
        20 minutes; two of three are within 10 minutes. Had the replay's
        121 minutes been counted, the 90th percentile would be 121.
        """
        with mock.patch.dict("os.environ", {"SELDON_INTAKE_SILENCE_MINUTES": "60"}):
            result = report_json("latency")

        self.assertEqual(result["forecasts"], 6)
        self.assertEqual(result["measured"], 3)
        self.assertEqual(
            result["excluded"],
            {"replayed": 1, "no_published_at": 1, "reforecast": 1, "no_delivery": 0},
        )
        self.assertEqual(result["percentile"], 90.0)
        self.assertEqual(result["percentile_minutes"], 20.0)
        self.assertEqual(result["within_threshold"], 2)
        self.assertAlmostEqual(result["share_within_threshold"], 2 / 3)
        self.assertEqual(result["by_broker"]["lasair"]["measured"], 1)
        self.assertEqual(result["by_broker"]["lasair"]["percentile_minutes"], 4.0)
        self.assertEqual(result["by_broker"]["babamul"]["percentile_minutes"], 8.0)
        self.assertEqual(result["by_broker"]["fink"]["percentile_minutes"], 20.0)

    def test_percentile_and_threshold_are_chosen(self) -> None:
        """The median of 4, 8, and 20 is 8; one of three is within 5 minutes."""
        with mock.patch.dict("os.environ", {"SELDON_INTAKE_SILENCE_MINUTES": "60"}):
            result = report_json(
                "latency", "--percentile", "50", "--threshold-minutes", "5"
            )

        self.assertEqual(result["percentile_minutes"], 8.0)
        self.assertEqual(result["within_threshold"], 1)

    def test_longer_silence_period_keeps_the_delivery(self) -> None:
        """With a 180-minute silence period the 120-minute arrival is no replay.

        The latencies are then 4, 8, 20, and 121 minutes, and the 90th
        percentile by nearest rank is the fourth, 121.
        """
        with mock.patch.dict("os.environ", {"SELDON_INTAKE_SILENCE_MINUTES": "180"}):
            result = report_json("latency")

        self.assertEqual(result["measured"], 4)
        self.assertEqual(result["percentile_minutes"], 121.0)

    def test_stored_window_selects_forecasts(self) -> None:
        """Forecasts stored before 01:20 UTC are the first two: 4 and 8 minutes."""
        with mock.patch.dict("os.environ", {"SELDON_INTAKE_SILENCE_MINUTES": "60"}):
            result = report_json("latency", "--until", "2026-11-12T01:20:00+00:00")

        self.assertEqual(result["forecasts"], 2)
        self.assertEqual(result["percentile_minutes"], 8.0)

    def test_plain_text_states_the_percentile(self) -> None:
        """The plain-text report names the percentile and the share."""
        with mock.patch.dict("os.environ", {"SELDON_INTAKE_SILENCE_MINUTES": "60"}):
            text = report("latency")

        self.assertIn("90th percentile: 20.0 min", text)
        self.assertIn("66.7%", text)


class GapsReportTests(TestCase):
    """Stream gaps per broker, and time since each broker's last delivery."""

    NOW = at("2026-11-12 18:00:00")

    @classmethod
    def setUpTestData(cls) -> None:
        """Gaps for Fink and Lasair, and deliveries at known times.

        Fink has a confirmed gap from 02:00 to 02:30 and a suspected one from
        05:00 to 06:00; Lasair a suspected gap from 03:00 to 04:00. Fink last
        delivered at 17:55 (five minutes before now) and Lasair at 15:00
        (three hours before). Babamul is enabled but never delivered.
        """
        StreamGap.objects.create(
            broker="fink",
            started_at=at("2026-11-12 05:00:00"),
            ended_at=at("2026-11-12 06:00:00"),
            kind="suspected",
        )
        StreamGap.objects.create(
            broker="fink",
            started_at=at("2026-11-12 02:00:00"),
            ended_at=at("2026-11-12 02:30:00"),
            kind="confirmed",
        )
        StreamGap.objects.create(
            broker="lasair",
            started_at=at("2026-11-12 03:00:00"),
            ended_at=at("2026-11-12 04:00:00"),
            kind="suspected",
        )
        obj = RubinObject.objects.create(dia_object_id=5005)
        first = make_detection(obj, 51, at("2026-11-12 01:00:00"), "pending")
        second = make_detection(obj, 52, at("2026-11-12 02:00:00"), "pending")
        make_delivery(first, "fink", None, at("2026-11-12 10:00:00"))
        make_delivery(second, "fink", None, at("2026-11-12 17:55:00"))
        make_delivery(first, "lasair", None, at("2026-11-12 15:00:00"))

    def setUp(self) -> None:
        """Fix the clock and the enabled brokers for each test."""
        brokers = (
            SimpleNamespace(name="fink", enabled=True),
            SimpleNamespace(name="lasair", enabled=True),
            SimpleNamespace(name="babamul", enabled=True),
        )
        for target, value in (
            (f"{COMMAND}.timezone.now", self.NOW),
            (f"{COMMAND}.load_broker_settings", brokers),
        ):
            patcher = mock.patch(target, return_value=value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_lists_gaps_and_time_since_last_delivery(self) -> None:
        """Each broker's gaps in start order, with kind, bounds, and silence."""
        result = report_json("gaps")

        brokers = {b["broker"]: b for b in result["brokers"]}
        self.assertEqual(sorted(brokers), ["babamul", "fink", "lasair"])
        fink = brokers["fink"]
        self.assertEqual(fink["last_delivery_received_at"], "2026-11-12T17:55:00+00:00")
        self.assertEqual(fink["seconds_since_last_delivery"], 300.0)
        self.assertEqual(
            fink["gaps"],
            [
                {
                    "kind": "confirmed",
                    "started_at": "2026-11-12T02:00:00+00:00",
                    "ended_at": "2026-11-12T02:30:00+00:00",
                    "duration_seconds": 1800.0,
                },
                {
                    "kind": "suspected",
                    "started_at": "2026-11-12T05:00:00+00:00",
                    "ended_at": "2026-11-12T06:00:00+00:00",
                    "duration_seconds": 3600.0,
                },
            ],
        )
        self.assertEqual(brokers["lasair"]["seconds_since_last_delivery"], 10800.0)
        self.assertEqual(len(brokers["lasair"]["gaps"]), 1)
        self.assertIsNone(brokers["babamul"]["last_delivery_received_at"])
        self.assertIsNone(brokers["babamul"]["seconds_since_last_delivery"])
        self.assertEqual(brokers["babamul"]["gaps"], [])

    def test_broker_option_narrows_the_listing(self) -> None:
        """Naming Lasair lists Lasair alone."""
        result = report_json("gaps", "--broker", "lasair")

        self.assertEqual([b["broker"] for b in result["brokers"]], ["lasair"])

    def test_plain_text_shows_silence_and_never(self) -> None:
        """Plain text gives each broker's silence, and 'never' for Babamul."""
        text = report("gaps")

        self.assertIn("never", text)
        self.assertIn("5m", text)
        self.assertIn("3h00m", text)
        self.assertIn("suspected", text)
        self.assertIn("confirmed", text)


class SkipsReportTests(TestCase):
    """Intake skips per broker, night, and reason."""

    @classmethod
    def setUpTestData(cls) -> None:
        """Four skips around the noon boundary between two nights.

        Fink skipped two alerts for ``missing_field`` at 11:59 and 03:00 UTC
        on 12 November (night of 11 November) and one for ``light_curve_span``
        at 12:01 (night of 12 November). Lasair skipped one ``parse_error``
        at 13:00 on 12 November.
        """
        for broker, reason, occurred, ident in (
            ("fink", "missing_field", "2026-11-12 11:59:00", "a1"),
            ("fink", "missing_field", "2026-11-12 03:00:00", "a2"),
            ("fink", "light_curve_span", "2026-11-12 12:01:00", "a3"),
            ("lasair", "parse_error", "2026-11-12 13:00:00", ""),
        ):
            IntakeSkip.objects.create(
                broker=broker,
                reason=reason,
                occurred_at=at(occurred),
                raw_identifier=ident,
            )

    def test_counts_per_night_broker_and_reason(self) -> None:
        """Three groups: two Fink skips in the earlier night, two in the later."""
        result = report_json("skips")

        self.assertEqual(
            result["counts"],
            [
                {
                    "night": "2026-11-11",
                    "broker": "fink",
                    "reason": "missing_field",
                    "count": 2,
                },
                {
                    "night": "2026-11-12",
                    "broker": "fink",
                    "reason": "light_curve_span",
                    "count": 1,
                },
                {
                    "night": "2026-11-12",
                    "broker": "lasair",
                    "reason": "parse_error",
                    "count": 1,
                },
            ],
        )
        self.assertNotIn("skips", result)

    def test_list_option_lists_each_skip(self) -> None:
        """With ``--list`` and Fink named, Fink's three skips are listed in time order."""
        result = report_json("skips", "--list", "--broker", "fink")

        self.assertEqual(
            [s["raw_identifier"] for s in result["skips"]], ["a2", "a1", "a3"]
        )
        self.assertEqual(len(result["counts"]), 2)


class ReadOnlyTests(TestCase):
    """No report changes anything stored."""

    @classmethod
    def setUpTestData(cls) -> None:
        """One object with a forecast, a refusal, a gap, and a skip."""
        obj = RubinObject.objects.create(dia_object_id=6006)
        first = make_detection(obj, 61, at("2026-11-12 01:00:00"), "forecast")
        second = make_detection(obj, 62, at("2026-11-12 02:00:00"), "refused")
        obj.qualifying_detection = first
        obj.qualifying_broker = "fink"
        obj.save()
        make_delivery(
            first, "fink", at("2026-11-12 03:00:00"), at("2026-11-12 03:01:00")
        )
        make_delivery(
            second, "fink", at("2026-11-12 03:00:00"), at("2026-11-12 03:01:00")
        )
        make_forecast(
            first,
            at("2026-11-12 03:05:00"),
            qualifying_event={"dia_source_id": 61, "broker": "fink"},
        )
        make_refusal(second, at("2026-11-12 03:06:00"))
        StreamGap.objects.create(
            broker="fink",
            started_at=at("2026-11-12 04:00:00"),
            ended_at=at("2026-11-12 05:00:00"),
            kind="suspected",
        )
        IntakeSkip.objects.create(
            broker="fink", reason="missing_field", occurred_at=at("2026-11-12 06:00:00")
        )

    def test_every_subcommand_leaves_the_database_unchanged(self) -> None:
        """Row counts and contents of every table match before and after."""
        invocations = [
            ("object", "6006"),
            ("coverage",),
            ("latency",),
            ("gaps",),
            ("skips", "--list"),
        ]
        for args in invocations:
            for output in ((), ("--json",)):
                with self.subTest(args=args + output):
                    before = database_fingerprint()
                    report(*args, *output)
                    self.assertEqual(database_fingerprint(), before)
