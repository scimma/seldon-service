"""Tests for the alert consumer: the long-running intake process.

The consumer reads recorded broker alerts from ``seldon.tests.fixtures.
broker_alerts`` through an in-memory fake adapter that behaves like a Kafka
partition: a log of messages and a committed position that only moves when
an alert is acknowledged. The engine, the database (the compose ``db``
service), and the real checkpoint are used as they are; no inference is
mocked. Database outages are injected with a query wrapper.

Expected forecasts are worked out here from the recorded alerts and the
thresholds these tests configure, never from the consumer or the engine.
"""

import copy
import os
import signal
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest import mock

import yaml
from django.core.management import CommandError, call_command
from django.db import OperationalError, connection
from django.test import TestCase

from seldon import services
from seldon.config.intake import IntakeSettings
from seldon.config.settings import get_settings
from seldon.domain.intake.delivery import BrokerRole, Delivery
from seldon.domain.intake.qualification import Thresholds
from seldon.infrastructure.brokers.base import (
    BrokerAdapter,
    GapKind,
    IntakeSkipReport,
    ParseContext,
    Polled,
    StreamGapReport,
)
from seldon.infrastructure.brokers.parsers import parse_alert
from seldon.management.commands.consume_alerts import IntakeConsumer
from seldon.models import Detection, Forecast, IntakeSkip, Refusal, StreamGap
from seldon.tests.fixtures.broker_alerts import babamul, fink, lasair

#: When the fake brokers' alerts are received.
RECEIVED = datetime(2026, 11, 13, 1, 0, tzinfo=UTC)

#: The cuts these tests configure: Fink 0.6 / 0.5, Lasair 0.6 / 1.0.
FINK_CUT, FINK_SN_LIKE = 0.6, 0.5
LASAIR_CUT, LASAIR_SN_LIKE = 0.6, 1.0
THRESHOLDS = {
    "fink": Thresholds(reliability_cut=FINK_CUT, sn_like_threshold=FINK_SN_LIKE),
    "lasair": Thresholds(reliability_cut=LASAIR_CUT, sn_like_threshold=LASAIR_SN_LIKE),
}

#: The environment the command reads: every broker enabled, with stand-in
#: connection values (the fake adapters never use them).
BROKER_ENV = {
    "SELDON_FINK_ENABLED": "true",
    "SELDON_FINK_BOOTSTRAP_SERVERS": "fink.invalid:9092",
    "SELDON_FINK_GROUP_ID": "test-group",
    "SELDON_FINK_RELIABILITY_CUT": str(FINK_CUT),
    "SELDON_FINK_SN_LIKE_THRESHOLD": str(FINK_SN_LIKE),
    "SELDON_LASAIR_ENABLED": "true",
    "SELDON_LASAIR_RELIABILITY_CUT": str(LASAIR_CUT),
    "SELDON_LASAIR_SN_LIKE_THRESHOLD": str(LASAIR_SN_LIKE),
    "SELDON_BABAMUL_ENABLED": "true",
    "SELDON_BABAMUL_USERNAME": "tester",
    "SELDON_BABAMUL_PASSWORD": "not-a-password",
    "SELDON_BABAMUL_GROUP_ID": "tester-group",
}

ROLES = {
    "fink": BrokerRole.QUALIFYING,
    "lasair": BrokerRole.QUALIFYING,
    "babamul": BrokerRole.FULL_STREAM,
}


def fink_source(message: dict[str, Any]) -> tuple[int, float, float | None]:
    """A recorded Fink alert's diaSourceId, reliability, and SN-like score.

    Args:
        message: One recorded Fink message.

    Returns:
        The detection's identifier and its two scores, as recorded.
    """
    alert = message["alert"]
    source = alert["diaSource"]
    return (
        source["diaSourceId"],
        source["reliability"],
        alert["clf"]["snnSnVsOthers_score"],
    )


def lasair_source(message: dict[str, Any]) -> tuple[int, float, str]:
    """A recorded Lasair alert's latest diaSourceId, reliability, and class.

    Args:
        message: One recorded Lasair message.

    Returns:
        The latest detection's identifier, its reliability, and the
        Sherlock class, as recorded.
    """
    sources = message["alert"]["alert"]["diaSourcesList"]
    latest = max(sources, key=lambda s: s["midpointMjdTai"])
    return (
        latest["diaSourceId"],
        latest["reliability"],
        message["alert"]["classification"],
    )


def expected_forecasts() -> set[int]:
    """The detections the recorded alerts qualify, under these tests' cuts.

    A Fink alert qualifies when its reliability and SuperNNova score meet
    Fink's cut and threshold. A Lasair alert qualifies when its reliability
    meets Lasair's cut and Sherlock calls it SN or NT. Babamul never
    qualifies. Each recorded alert is a different object, so a qualifying
    alert is forecast and nothing else is.

    Returns:
        The diaSourceId of each detection owed a forecast.
    """
    owed = set()
    for message in fink.ALERTS:
        source_id, reliability, sn_like = fink_source(message)
        if reliability >= FINK_CUT and sn_like is not None and sn_like >= 0.5:
            owed.add(source_id)
    for message in lasair.ALERTS:
        source_id, reliability, classification = lasair_source(message)
        if reliability >= LASAIR_CUT and classification in ("SN", "NT"):
            owed.add(source_id)
    return owed


class SimulatedCrash(Exception):
    """The process dying between storing a forecast and acknowledging it."""


class FakeStream:
    """One broker's partition: a message log and the committed position.

    The position survives the adapter, as a Kafka group's committed offset
    does, so a new adapter on the same stream resumes where acknowledgement
    left off.
    """

    def __init__(self, broker: str, messages: list[dict[str, Any]]) -> None:
        """Hold the log.

        Args:
            broker: The configured broker name.
            messages: Recorded messages, in stream order.
        """
        self.broker = broker
        self.messages = messages
        self.committed = 0


class FakeAdapter(BrokerAdapter):
    """A broker adapter over a ``FakeStream`` that records what it is told.

    Like a Kafka consumer, polling advances a session position past what it
    returns whether or not it is acknowledged; only a new adapter starts
    again from the committed position.
    """

    def __init__(
        self,
        stream: FakeStream,
        on_exhausted: Callable[[], None] | None = None,
        on_ack: Callable[[Polled], None] | None = None,
        on_poll: Callable[[], None] | None = None,
        gap_reports: list[StreamGapReport] | None = None,
        received_at: Callable[[], datetime] = lambda: RECEIVED,
        batch: int = 1,
    ) -> None:
        """Attach to the stream at its committed position.

        Args:
            stream: The partition to read.
            on_exhausted: Called on each poll that finds nothing left.
            on_ack: Called with each alert before its commit is recorded;
                whatever it raises stops the commit.
            on_poll: Called at the start of each poll.
            gap_reports: Gaps to report on the first ``gaps`` call.
            received_at: When alerts are received.
            batch: Most messages one poll returns.
        """
        self.name = stream.broker
        self.role = ROLES[stream.broker]
        self.stream = stream
        self.position = stream.committed
        self.on_exhausted = on_exhausted
        self.on_ack = on_ack
        self.on_poll = on_poll
        self.received_at = received_at
        self.batch = batch
        self.polls = 0
        self.acked: list[int] = []
        self.stored_at_ack: list[bool] = []
        self.closed = False
        self._gaps = list(gap_reports or [])

    def poll(self, timeout: float, max_messages: int = 100) -> list[Polled]:
        """Parse the next messages after the session position.

        Args:
            timeout: Ignored; the fake never waits.
            max_messages: Most messages to take.

        Returns:
            The parsed alerts, each with its offset as the receipt.
        """
        self.polls += 1
        if self.on_poll is not None:
            self.on_poll()
        end = min(len(self.stream.messages), self.position + self.batch)
        end = min(end, self.position + max_messages)
        polled = []
        for offset in range(self.position, end):
            message = self.stream.messages[offset]
            context = ParseContext(
                broker=self.name,
                role=self.role,
                topic=message["topic"],
                published_at=datetime.fromtimestamp(
                    message["kafka_timestamp_ms"] / 1000, tz=UTC
                ),
                received_at=self.received_at(),
            )
            outcome = parse_alert(self.name, message["alert"], context)
            polled.append(Polled(outcome=outcome, receipt=offset))
        self.position = end
        if not polled and self.on_exhausted is not None:
            self.on_exhausted()
        return polled

    def ack(self, polled: Polled) -> None:
        """Commit past the alert, noting whether its result was stored.

        Args:
            polled: An alert from ``poll``.

        Raises:
            AssertionError: If acknowledged out of stream order.
        """
        if polled.receipt != self.stream.committed:
            raise AssertionError(
                f"acknowledged offset {polled.receipt}, "
                f"expected {self.stream.committed}"
            )
        if self.on_ack is not None:
            self.on_ack(polled)
        self.stored_at_ack.append(stored(polled.outcome))
        self.acked.append(polled.receipt)
        self.stream.committed = polled.receipt + 1

    def gaps(self) -> list[StreamGapReport]:
        """Report the configured gaps once.

        Returns:
            The gaps not yet reported.
        """
        found, self._gaps = self._gaps, []
        return found

    def close(self) -> None:
        """Note the close."""
        self.closed = True


def stored(outcome: Delivery | IntakeSkipReport) -> bool:
    """Whether what an alert caused is in the database.

    Args:
        outcome: A polled delivery or skip.

    Returns:
        For a delivery, whether its detection and this broker's delivery of
        it are stored; for a skip, whether the skip is.
    """
    if isinstance(outcome, IntakeSkipReport):
        return IntakeSkip.objects.filter(
            broker=outcome.broker, raw_identifier=outcome.raw_identifier
        ).exists()
    return Detection.objects.filter(
        dia_source_id=outcome.dia_source_id, deliveries__broker=outcome.broker
    ).exists()


def send_sigterm() -> None:
    """Deliver SIGTERM to this process, as ``docker stop`` does.

    Raises:
        AssertionError: If nothing handles SIGTERM, which would otherwise
            kill the test run.
    """
    if signal.getsignal(signal.SIGTERM) in (signal.SIG_DFL, None):
        raise AssertionError("no SIGTERM handler installed")
    signal.raise_signal(signal.SIGTERM)


class FakeClock:
    """A clock the test moves by hand."""

    def __init__(self, now: datetime) -> None:
        """Start the clock.

        Args:
            now: The starting time.
        """
        self.now = now

    def __call__(self) -> datetime:
        """The current time.

        Returns:
            The clock's time.
        """
        return self.now


def intake(silence_minutes: float = 60.0) -> IntakeSettings:
    """Intake timing for a night from 23:00 to 11:00 UTC.

    Args:
        silence_minutes: The silence period.

    Returns:
        The settings.
    """
    return IntakeSettings(
        silence_minutes=silence_minutes,
        night_start_utc_hour=23,
        night_end_utc_hour=11,
    )


def streams() -> dict[str, FakeStream]:
    """A fresh stream per broker, holding its recorded alerts.

    Returns:
        Broker name to stream.
    """
    return {
        "fink": FakeStream("fink", fink.ALERTS),
        "lasair": FakeStream("lasair", lasair.ALERTS),
        "babamul": FakeStream("babamul", babamul.ALERTS),
    }


def stored_results() -> dict[int, int]:
    """How many forecasts and refusals each detection has.

    Returns:
        diaSourceId to the count of its forecasts plus refusals.
    """
    counts: dict[int, int] = {}
    for row in Forecast.objects.values_list("detection__dia_source_id", flat=True):
        counts[row] = counts.get(row, 0) + 1
    for row in Refusal.objects.values_list("detection__dia_source_id", flat=True):
        counts[row] = counts.get(row, 0) + 1
    return counts


@mock.patch.dict(os.environ, BROKER_ENV)
class ConsumeAlertsCommandTests(TestCase):
    """The ``consume_alerts`` command against recorded alerts."""

    def run_command(self, adapters: list[FakeAdapter]) -> None:
        """Run the command with these adapters in place of Kafka.

        Args:
            adapters: The adapters the command reads.
        """
        with mock.patch(
            "seldon.management.commands.consume_alerts.build_adapters",
            return_value=adapters,
        ):
            call_command("consume_alerts", poll_timeout=0.01)

    def test_recorded_alerts_are_forecast_and_acknowledged_after_commit(
        self,
    ) -> None:
        """Replaying every broker's recorded alerts stores a forecast for
        each detection the alerts qualify (the Lasair alerts above Lasair's
        cut, the Fink alert above Fink's) and nothing for the rest, and every
        alert is acknowledged, in order, only once its delivery is stored.
        SIGTERM, sent once every stream is read, stops the command cleanly.
        """
        by_broker = streams()
        read: set[str] = set()

        def exhausted(name: str) -> Callable[[], None]:
            def note() -> None:
                read.add(name)
                if read == set(by_broker):
                    send_sigterm()

            return note

        adapters = [
            FakeAdapter(stream, on_exhausted=exhausted(name))
            for name, stream in by_broker.items()
        ]
        previous = signal.getsignal(signal.SIGTERM)

        self.run_command(adapters)

        owed = expected_forecasts()
        self.assertTrue(owed)
        self.assertEqual(
            set(Forecast.objects.values_list("detection__dia_source_id", flat=True)),
            owed,
        )
        self.assertEqual(Forecast.objects.count(), len(owed))
        for adapter in adapters:
            with self.subTest(broker=adapter.name):
                count = len(adapter.stream.messages)
                self.assertEqual(adapter.acked, list(range(count)))
                self.assertEqual(adapter.stored_at_ack, [True] * count)
                self.assertTrue(adapter.closed)
        self.assertEqual(signal.getsignal(signal.SIGTERM), previous)

    def test_crash_after_commit_before_ack_then_restart_forecasts_once(
        self,
    ) -> None:
        """The process dies after the first Lasair forecast is stored but
        before its alert is acknowledged. On restart the alert is delivered
        again, from the committed position, and the detection still has one
        forecast; then it is acknowledged."""
        stream = FakeStream("lasair", lasair.ALERTS[:1])
        (source_id, _, _) = lasair_source(lasair.ALERTS[0])

        def crash(polled: Polled) -> None:
            raise SimulatedCrash()

        with self.assertRaises(SimulatedCrash):
            self.run_command([FakeAdapter(stream, on_ack=crash)])
        self.assertEqual(stored_results(), {source_id: 1})
        self.assertEqual(stream.committed, 0)

        restarted = FakeAdapter(stream, on_exhausted=send_sigterm)
        self.run_command([restarted])

        self.assertEqual(restarted.acked, [0])
        self.assertEqual(stored_results(), {source_id: 1})
        self.assertEqual(
            Forecast.objects.filter(detection__dia_source_id=source_id).count(), 1
        )

    def test_sigterm_mid_batch_stops_after_the_current_delivery(self) -> None:
        """SIGTERM arrives while the first of two polled Lasair alerts is
        being acknowledged. That alert is finished; the second is neither
        processed nor acknowledged, and the adapter is closed."""
        stream = FakeStream("lasair", lasair.ALERTS[:2])
        adapter = FakeAdapter(stream, on_ack=lambda polled: send_sigterm(), batch=2)
        (second_id, _, _) = lasair_source(lasair.ALERTS[1])

        self.run_command([adapter])

        self.assertEqual(adapter.acked, [0])
        self.assertFalse(Detection.objects.filter(dia_source_id=second_id).exists())
        self.assertTrue(adapter.closed)

    def test_incompatible_checkpoint_exits_before_reading_any_delivery(
        self,
    ) -> None:
        """With hparams naming a dataloader that cannot run without training
        data, the model fails to load and the command exits with an error;
        no adapter is built and no alert is polled or stored."""
        hparams = yaml.safe_load(get_settings().hparams_path.read_text())
        hparams["dataset"]["name"] = "LightCurveDataLoader"
        adapter = FakeAdapter(FakeStream("lasair", lasair.ALERTS))
        build = mock.Mock(return_value=[adapter])
        services.loaded_model.cache_clear()
        self.addCleanup(services.loaded_model.cache_clear)

        with tempfile.TemporaryDirectory() as tmp:
            modified = Path(tmp) / "hparams.yaml"
            modified.write_text(yaml.safe_dump(hparams))
            with (
                mock.patch.dict(os.environ, {"SELDON_HPARAMS_PATH": str(modified)}),
                mock.patch(
                    "seldon.management.commands.consume_alerts.build_adapters",
                    build,
                ),
                self.assertRaises(CommandError) as failure,
            ):
                call_command("consume_alerts", poll_timeout=0.01)

        self.assertIn("LightCurveDataLoader", str(failure.exception))
        build.assert_not_called()
        self.assertEqual(adapter.polls, 0)
        self.assertFalse(Detection.objects.exists())


class IntakeConsumerTests(TestCase):
    """The consumer loop with an injected clock, sleep, and database outage."""

    def test_database_outage_leaves_deliveries_unacknowledged_until_recovery(
        self,
    ) -> None:
        """The database goes down as the first Lasair alert is polled and
        stays down for three attempts at it. The consumer backs off longer
        each time, acknowledges nothing, and polls nothing new; once the
        database is back, both alerts are stored, forecast once each, and
        acknowledged in order."""
        outage = {"on": False}
        sleeps: list[float] = []
        stream = FakeStream("lasair", lasair.ALERTS[:2])
        consumer: IntakeConsumer

        def acked_during_outage(polled: Polled) -> None:
            if outage["on"]:
                raise AssertionError("acknowledged during the outage")

        def first_poll_starts_outage() -> None:
            if adapter.polls == 1:
                outage["on"] = True

        adapter = FakeAdapter(
            stream,
            on_ack=acked_during_outage,
            on_poll=first_poll_starts_outage,
            on_exhausted=lambda: consumer.stop(),
        )

        def sleep(seconds: float) -> None:
            sleeps.append(seconds)
            self.assertEqual(adapter.acked, [])
            self.assertEqual(adapter.polls, 1)
            if len(sleeps) == 3:
                outage["on"] = False

        def fail_queries(execute, sql, params, many, context):  # noqa: ANN001
            # Savepoint statements pass so the test's own transaction
            # survives; everything the engine asks of the database fails.
            if outage["on"] and "SAVEPOINT" not in sql.upper():
                raise OperationalError("database is down")
            return execute(sql, params, many, context)

        consumer = IntakeConsumer(
            [adapter], THRESHOLDS, intake(), poll_timeout=0.0, sleep=sleep
        )
        with (
            connection.execute_wrapper(fail_queries),
            self.assertLogs("seldon", level="WARNING"),
        ):
            consumer.run()

        self.assertEqual(len(sleeps), 3)
        self.assertEqual(sleeps, sorted(sleeps))
        self.assertGreater(sleeps[-1], sleeps[0])
        self.assertEqual(adapter.acked, [0, 1])
        owed = {lasair_source(m)[0] for m in lasair.ALERTS[:2]}
        self.assertEqual(stored_results(), {source_id: 1 for source_id in owed})

    def test_intake_skip_is_recorded_then_acknowledged(self) -> None:
        """A Lasair alert without its Sherlock class cannot be parsed: the
        skip is stored with the broker's name and then acknowledged."""
        broken = copy.deepcopy(lasair.ALERTS[0])
        del broken["alert"]["classification"]
        stream = FakeStream("lasair", [broken])
        consumer: IntakeConsumer
        adapter = FakeAdapter(stream, on_exhausted=lambda: consumer.stop())
        consumer = IntakeConsumer([adapter], THRESHOLDS, intake(), poll_timeout=0.0)

        with self.assertLogs("seldon", level="WARNING"):
            consumer.run()

        self.assertEqual(adapter.acked, [0])
        self.assertEqual(adapter.stored_at_ack, [True])
        skip = IntakeSkip.objects.get()
        self.assertEqual(skip.broker, "lasair")
        self.assertIn("classification", skip.reason)
        self.assertFalse(Detection.objects.exists())

    def test_reported_gaps_are_stored(self) -> None:
        """A confirmed gap the adapter reports is stored as a stream gap
        with its broker, bounds, and kind."""
        started = datetime(2026, 11, 12, 23, 0, tzinfo=UTC)
        ended = datetime(2026, 11, 13, 2, 30, tzinfo=UTC)
        consumer: IntakeConsumer
        adapter = FakeAdapter(
            FakeStream("babamul", []),
            on_exhausted=lambda: consumer.stop(),
            gap_reports=[StreamGapReport("babamul", started, ended, GapKind.CONFIRMED)],
        )
        consumer = IntakeConsumer(
            [adapter],
            THRESHOLDS,
            intake(),
            poll_timeout=0.0,
            clock=FakeClock(datetime(2026, 11, 13, 12, 0, tzinfo=UTC)),
        )

        consumer.run()

        gap = StreamGap.objects.get()
        self.assertEqual(
            (gap.broker, gap.started_at, gap.ended_at, gap.kind),
            ("babamul", started, ended, StreamGap.Kind.CONFIRMED),
        )

    def silent_run(self, start: datetime, polls: int, step: timedelta) -> None:
        """Run a consumer whose only broker delivers nothing.

        Args:
            start: When the consumer starts.
            polls: How many polls to make before stopping.
            step: How far the clock moves on each poll.
        """
        clock = FakeClock(start)
        consumer: IntakeConsumer

        def tick() -> None:
            clock.now += step
            if adapter.polls >= polls:
                consumer.stop()

        adapter = FakeAdapter(FakeStream("fink", []), on_exhausted=tick)
        consumer = IntakeConsumer(
            [adapter], THRESHOLDS, intake(), poll_timeout=0.0, clock=clock
        )
        consumer.run()

    def test_night_silence_records_one_suspected_gap(self) -> None:
        """Fink delivers nothing from 23:30 UTC. Silence passes the 60-minute
        period at 00:40, the first poll after 00:30; one suspected gap is
        recorded from the consumer's start to then, and no second one for
        the rest of the same silent stretch."""
        start = datetime(2026, 11, 12, 23, 30, tzinfo=UTC)

        with self.assertLogs("seldon", level="WARNING"):
            self.silent_run(start, polls=24, step=timedelta(minutes=10))

        gap = StreamGap.objects.get()
        self.assertEqual(
            (gap.broker, gap.started_at, gap.ended_at, gap.kind),
            (
                "fink",
                start,
                datetime(2026, 11, 13, 0, 40, tzinfo=UTC),
                StreamGap.Kind.SUSPECTED,
            ),
        )

    def test_daytime_silence_records_no_gap(self) -> None:
        """Fink delivers nothing from 12:00 to 22:00 UTC, outside the Rubin
        night: no gap is recorded."""
        self.silent_run(
            datetime(2026, 11, 13, 12, 0, tzinfo=UTC),
            polls=60,
            step=timedelta(minutes=10),
        )

        self.assertFalse(StreamGap.objects.exists())

    def test_silence_counts_from_the_start_of_the_night(self) -> None:
        """Fink delivers nothing from 21:00 UTC. The day's silence does not
        count: the gap starts at 23:00, when the night does, and is recorded
        at 00:10, the first poll more than 60 minutes after it."""
        with self.assertLogs("seldon", level="WARNING"):
            self.silent_run(
                datetime(2026, 11, 12, 21, 0, tzinfo=UTC),
                polls=24,
                step=timedelta(minutes=10),
            )

        gap = StreamGap.objects.get()
        self.assertEqual(
            (gap.started_at, gap.ended_at),
            (
                datetime(2026, 11, 12, 23, 0, tzinfo=UTC),
                datetime(2026, 11, 13, 0, 10, tzinfo=UTC),
            ),
        )

    def test_pending_detection_past_seven_days_is_swept(self) -> None:
        """Babamul's recorded alerts leave their detections pending. A
        consumer started more than seven days after they were received
        sweeps them at start: their objects never qualified, so they are
        discarded."""
        consumer: IntakeConsumer
        adapter = FakeAdapter(
            FakeStream("babamul", babamul.ALERTS),
            on_exhausted=lambda: consumer.stop(),
        )
        consumer = IntakeConsumer(
            [adapter],
            THRESHOLDS,
            intake(),
            poll_timeout=0.0,
            clock=FakeClock(RECEIVED),
        )
        consumer.run()
        self.assertEqual(
            Detection.objects.filter(state=Detection.State.PENDING).count(),
            len(babamul.ALERTS),
        )

        later: IntakeConsumer
        idle = FakeAdapter(FakeStream("babamul", []), on_exhausted=lambda: later.stop())
        later = IntakeConsumer(
            [idle],
            THRESHOLDS,
            intake(),
            poll_timeout=0.0,
            clock=FakeClock(RECEIVED + timedelta(days=7, hours=1)),
        )
        later.run()

        self.assertFalse(Detection.objects.exists())
