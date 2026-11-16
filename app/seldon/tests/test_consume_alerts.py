"""Tests for the alert consumer: the long-running intake process.

The consumer reads recorded broker alerts from ``seldon.tests.fixtures.
broker_alerts`` through an in-memory fake adapter that behaves like a Kafka
partition: a log of messages and a committed position that only moves when
an alert is acknowledged. The engine and the database (the compose ``db``
service) are used as they are. Database outages are injected with a query
wrapper.

Expected owed detections are worked out here from the recorded alerts and
the thresholds these tests configure, never from the consumer or the engine.
"""

import copy
import os
import signal
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest import mock

from confluent_kafka import KafkaError, KafkaException
from django.core.management import call_command
from django.db import OperationalError, connection
from django.test import TestCase

from seldon import services
from seldon.config.intake import IntakeSettings
from seldon.domain.intake.delivery import BrokerRole, Delivery, Point
from seldon.domain.intake.qualification import Thresholds
from seldon.infrastructure.brokers.base import (
    BrokerAdapter,
    GapKind,
    IntakeSkipReport,
    ParseContext,
    Polled,
    StreamGapReport,
)
from seldon.infrastructure.brokers.kafka import KafkaBrokerAdapter
from seldon.infrastructure.brokers.parsers import parse_alert
from seldon.management.commands.consume_alerts import IntakeConsumer
from seldon.models import (
    Detection,
    IntakeLagCount,
    IntakeSkip,
    OutcomeClaim,
    StreamGap,
)
from seldon.tests.fixtures.broker_alerts import babamul, fink, lasair
from seldon.tests.test_broker_adapters import (
    FakeConsumer,
    _lasair_messages,
    _lasair_settings,
    _ms,
)

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


def expected_owed() -> set[int]:
    """The detections the recorded alerts qualify, under these tests' cuts.

    A Fink alert qualifies when its reliability and SuperNNova score meet
    Fink's cut and threshold. A Lasair alert qualifies when its reliability
    meets Lasair's cut and Sherlock calls it SN or NT. Babamul never
    qualifies. Each recorded alert is a different object, so a qualifying
    alert's detection is owed and nothing else is.

    Returns:
        The diaSourceId of each detection owed.
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
    """The process dying between storing an alert's work and acknowledging it."""


class FakeStream:
    """One broker's partition: a message log and the committed position.

    The position survives the adapter, as a Kafka group's committed offset
    does, so a new adapter on the same stream resumes where acknowledgement
    left off.
    """

    def __init__(
        self, broker: str, messages: list[dict[str, Any] | Delivery | IntakeSkipReport]
    ) -> None:
        """Hold the log.

        Args:
            broker: The configured broker name.
            messages: Recorded messages, or ready-made deliveries and
                skips, in stream order.
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
            on_ack: Called with each alert of an acknowledged batch before
                the batch's commit is recorded; whatever it raises stops the
                commit.
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
        self.timeouts: list[float] = []
        self.acked: list[int] = []
        self.commits: list[int] = []
        self.stored_at_ack: list[bool] = []
        self.closed = False
        self._gaps = list(gap_reports or [])

    def poll(self, timeout: float, max_messages: int = 100) -> list[Polled]:
        """Parse the next messages after the session position.

        Args:
            timeout: Recorded; the fake never waits.
            max_messages: Most messages to take.

        Returns:
            The parsed alerts, each with its offset as the receipt.
        """
        self.polls += 1
        self.timeouts.append(timeout)
        if self.on_poll is not None:
            self.on_poll()
        end = min(len(self.stream.messages), self.position + self.batch)
        end = min(end, self.position + max_messages)
        polled = []
        for offset in range(self.position, end):
            message = self.stream.messages[offset]
            if isinstance(message, (Delivery, IntakeSkipReport)):
                polled.append(Polled(outcome=message, receipt=offset))
                continue
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

    def ack(self, batch: Sequence[Polled]) -> None:
        """Commit past a batch, noting whether each alert's result was stored.

        The fake stream is one partition, so a batch is one commit, past its
        last alert.

        Args:
            batch: Consecutive alerts from ``poll``.

        Raises:
            AssertionError: If acknowledged out of stream order.
        """
        expected = list(
            range(self.stream.committed, self.stream.committed + len(batch))
        )
        receipts = [polled.receipt for polled in batch]
        if receipts != expected:
            raise AssertionError(
                f"acknowledged offsets {receipts}, expected {expected}"
            )
        for polled in batch:
            if self.on_ack is not None:
                self.on_ack(polled)
            self.stored_at_ack.append(stored(polled.outcome))
        self.acked.extend(receipts)
        self.stream.committed = receipts[-1] + 1
        self.commits.append(self.stream.committed)

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


def intake(silence_minutes: float = 60.0, batch_size: int = 100) -> IntakeSettings:
    """Intake settings for a night from 23:00 to 11:00 UTC.

    Args:
        silence_minutes: The silence period.
        batch_size: Most alerts in one batch.

    Returns:
        The settings.
    """
    return IntakeSettings(
        silence_minutes=silence_minutes,
        night_start_utc_hour=23,
        night_end_utc_hour=11,
        batch_size=batch_size,
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


def owed_times() -> dict[int, datetime]:
    """When each owed detection became owed, checking intake stored no outcome.

    Returns:
        diaSourceId to the owed time of each detection in the owed state.

    Raises:
        AssertionError: If any outcome claim is stored; intake owes, and
            outcomes are left to the forecast workers.
    """
    if OutcomeClaim.objects.exists():
        raise AssertionError("intake stored an outcome claim")
    return dict(
        Detection.objects.filter(state=Detection.State.OWED).values_list(
            "dia_source_id", "owed_at"
        )
    )


#: diaObjectId of the first made-up object; each made-up delivery has its own.
MADE_UP_OBJECT = 515151515100


def made_up_delivery(index: int, broker: str = "lasair", band: str = "r") -> Delivery:
    """A one-point delivery of a made-up object of its own.

    A Lasair delivery scores 0.9 reliability and 2.0 SN-like, above Lasair's
    cut and threshold here, so its detection is owed; a Babamul
    delivery is full-stream and leaves its detection pending. Each is
    published 30 s before it is received.

    Args:
        index: Which made-up object; its detection's ``diaSourceId`` is
            ``(MADE_UP_OBJECT + index) * 100``.
        broker: ``lasair`` or ``babamul``.
        band: The band name as sent.

    Returns:
        The delivery.
    """
    object_id = MADE_UP_OBJECT + index
    qualifying = ROLES[broker] is BrokerRole.QUALIFYING
    return Delivery(
        dia_object_id=object_id,
        detection=Point(
            observed_mjd=61000.25,
            band=band,
            flux=1000.0,
            flux_err=50.0,
            dia_source_id=object_id * 100,
        ),
        previous=(),
        broker=broker,
        role=ROLES[broker],
        reliability=0.9 if qualifying else None,
        sn_like_score=2.0 if qualifying else None,
        published_at=RECEIVED - timedelta(seconds=30),
        received_at=RECEIVED,
        schema_version="test-1",
    )


def lag_count_total() -> int:
    """How many alerts the intake-lag record has counted.

    Returns:
        The sum of every stored count.
    """
    return sum(IntakeLagCount.objects.values_list("count", flat=True))


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

    def test_recorded_alerts_are_owed_and_acknowledged_after_commit(
        self,
    ) -> None:
        """Replaying every broker's recorded alerts owes each detection the
        alerts qualify (the Lasair alerts above Lasair's
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

        owed = expected_owed()
        self.assertTrue(owed)
        self.assertEqual(set(owed_times()), owed)
        for adapter in adapters:
            with self.subTest(broker=adapter.name):
                count = len(adapter.stream.messages)
                self.assertEqual(adapter.acked, list(range(count)))
                self.assertEqual(adapter.stored_at_ack, [True] * count)
                self.assertTrue(adapter.closed)
        self.assertEqual(signal.getsignal(signal.SIGTERM), previous)

    def test_crash_after_commit_before_ack_then_restart_owes_once(
        self,
    ) -> None:
        """The process dies after the first Lasair alert's detection is
        stored as owed but before its alert is acknowledged. On restart the
        alert is delivered again, from the committed position, and the
        detection keeps the time it first became owed; then it is
        acknowledged."""
        stream = FakeStream("lasair", lasair.ALERTS[:1])
        source_id, _, _ = lasair_source(lasair.ALERTS[0])

        def crash(polled: Polled) -> None:
            raise SimulatedCrash()

        with self.assertRaises(SimulatedCrash):
            self.run_command([FakeAdapter(stream, on_ack=crash)])
        first = owed_times()
        self.assertEqual(set(first), {source_id})
        self.assertEqual(stream.committed, 0)

        restarted = FakeAdapter(stream, on_exhausted=send_sigterm)
        self.run_command([restarted])

        self.assertEqual(restarted.acked, [0])
        self.assertEqual(owed_times(), first)

    def test_sigterm_mid_batch_stops_after_that_batch_is_acknowledged(
        self,
    ) -> None:
        """Three Babamul alerts wait on the stream, two to a poll. SIGTERM
        arrives with the first query of the first batch. That batch of two
        is stored and acknowledged in one commit; the broker is not polled
        again, the third alert is not stored, and the adapter is closed."""
        outcomes = [made_up_delivery(i, "babamul") for i in range(3)]
        armed = {"on": False}

        def arm() -> None:
            armed["on"] = True

        def sigterm_on_first_query(execute, sql, params, many, context):  # noqa: ANN001
            if armed["on"]:
                armed["on"] = False
                send_sigterm()
            return execute(sql, params, many, context)

        adapter = FakeAdapter(FakeStream("babamul", outcomes), on_poll=arm, batch=2)

        with connection.execute_wrapper(sigterm_on_first_query):
            self.run_command([adapter])

        self.assertEqual(adapter.acked, [0, 1])
        self.assertEqual(adapter.commits, [2])
        self.assertEqual(adapter.stored_at_ack, [True, True])
        self.assertEqual(adapter.polls, 1)
        self.assertFalse(stored(outcomes[2]))
        self.assertTrue(adapter.closed)

    def test_command_consumes_without_loading_a_model(self) -> None:
        """Loading a model or forecasting fails if asked for. The command
        still reads the recorded Lasair alerts and owes the detections they
        qualify, so intake never needs a model."""
        stream = FakeStream("lasair", lasair.ALERTS)
        no_model = AssertionError("intake must not reach a model")

        with (
            mock.patch.object(services, "installed_package", side_effect=no_model),
            mock.patch.object(services, "request_products", side_effect=no_model),
        ):
            self.run_command([FakeAdapter(stream, on_exhausted=send_sigterm)])

        self.assertEqual(stream.committed, len(lasair.ALERTS))
        owed = {
            lasair_source(m)[0]
            for m in lasair.ALERTS
            if lasair_source(m)[1] >= LASAIR_CUT and lasair_source(m)[2] in ("SN", "NT")
        }
        self.assertTrue(owed)
        self.assertEqual(set(owed_times()), owed)


class BatchIntakeTests(TestCase):
    """Polled alerts are stored and acknowledged in batches."""

    def test_two_alert_batch_is_one_commit_after_both_are_stored(self) -> None:
        """Two Babamul alerts polled together are one batch: the broker is
        committed once, past the second alert, and only once both alerts'
        deliveries are stored."""
        outcomes = [made_up_delivery(i, "babamul") for i in range(2)]
        both_stored_at_ack: list[bool] = []
        consumer: IntakeConsumer
        adapter = FakeAdapter(
            FakeStream("babamul", outcomes),
            batch=2,
            on_ack=lambda polled: both_stored_at_ack.append(
                all(stored(outcome) for outcome in outcomes)
            ),
            on_exhausted=lambda: consumer.stop(),
        )
        consumer = IntakeConsumer([adapter], THRESHOLDS, intake(), poll_timeout=0.0)

        consumer.run()

        self.assertEqual(adapter.commits, [2])
        self.assertEqual(adapter.acked, [0, 1])
        self.assertEqual(both_stored_at_ack, [True, True])

    def test_five_alert_batch_commits_once_after_all_five_are_stored(self) -> None:
        """Five Lasair alerts polled together, each owed, are one batch: one
        commit past the fifth, made when all five deliveries are stored and
        their detections owed; each alert is counted once in the lag record."""
        outcomes = [made_up_delivery(i) for i in range(5)]
        all_stored_at_ack: list[bool] = []

        def check(polled: Polled) -> None:
            all_stored_at_ack.append(
                all(stored(outcome) for outcome in outcomes) and len(owed_times()) == 5
            )

        consumer: IntakeConsumer
        adapter = FakeAdapter(
            FakeStream("lasair", outcomes),
            batch=5,
            on_ack=check,
            on_exhausted=lambda: consumer.stop(),
        )
        consumer = IntakeConsumer([adapter], THRESHOLDS, intake(), poll_timeout=0.0)

        consumer.run()

        self.assertEqual(adapter.commits, [5])
        self.assertEqual(all_stored_at_ack, [True] * 5)
        self.assertEqual(set(owed_times()), {o.dia_source_id for o in outcomes})
        self.assertEqual(lag_count_total(), 5)

    def test_batch_size_setting_splits_the_polled_alerts(self) -> None:
        """With a batch size of two, five Babamul alerts polled together are
        three batches, committed past the second, fourth and fifth."""
        outcomes = [made_up_delivery(i, "babamul") for i in range(5)]
        consumer: IntakeConsumer
        adapter = FakeAdapter(
            FakeStream("babamul", outcomes),
            batch=5,
            on_exhausted=lambda: consumer.stop(),
        )
        consumer = IntakeConsumer(
            [adapter], THRESHOLDS, intake(batch_size=2), poll_timeout=0.0
        )

        consumer.run()

        self.assertEqual(adapter.commits, [2, 4, 5])
        self.assertEqual(adapter.acked, list(range(5)))

    def test_outage_at_the_fourth_alert_rolls_back_the_whole_batch(self) -> None:
        """The database goes down as the fourth of five polled Lasair alerts
        is stored, after the first three were stored and owed in the batch's
        transaction, and stays down for the first retry. Nothing of the
        batch is left stored and nothing is acknowledged while it is down;
        once it is back, all five are stored, owed, and acknowledged in one
        commit, with five lag counts."""
        outcomes = [made_up_delivery(i) for i in range(5)]
        fourth = outcomes[3].dia_object_id
        outage = {"armed": True, "on": False}
        sleeps: list[float] = []
        during_outage: list[tuple[int, int, list[int]]] = []

        def fail_queries(execute, sql, params, many, context):  # noqa: ANN001
            if outage["armed"] and not many and fourth in (params or ()):
                outage["armed"] = False
                outage["on"] = True
            # Savepoint statements pass so the transactions roll back.
            if outage["on"] and "SAVEPOINT" not in sql.upper():
                raise OperationalError("database is down")
            return execute(sql, params, many, context)

        def sleep(seconds: float) -> None:
            sleeps.append(seconds)
            outage["on"] = False
            during_outage.append(
                (
                    Detection.objects.count(),
                    len(owed_times()),
                    list(adapter.acked),
                )
            )
            outage["on"] = len(sleeps) < 2

        consumer: IntakeConsumer
        adapter = FakeAdapter(
            FakeStream("lasair", outcomes),
            batch=5,
            on_exhausted=lambda: consumer.stop(),
        )
        consumer = IntakeConsumer(
            [adapter], THRESHOLDS, intake(), poll_timeout=0.0, sleep=sleep
        )
        with (
            connection.execute_wrapper(fail_queries),
            self.assertLogs("seldon", level="WARNING"),
        ):
            consumer.run()

        self.assertFalse(outage["armed"])
        self.assertEqual(len(sleeps), 2)
        self.assertEqual(during_outage, [(0, 0, []), (0, 0, [])])
        self.assertEqual(adapter.commits, [5])
        self.assertEqual(adapter.acked, list(range(5)))
        self.assertEqual(set(owed_times()), {o.dia_source_id for o in outcomes})
        self.assertEqual(lag_count_total(), 5)

    def test_crash_after_the_batch_commits_redelivers_without_duplicates(
        self,
    ) -> None:
        """The process dies after a batch of five Lasair alerts is committed
        but before the broker is acknowledged. On restart all five are
        delivered again from the committed position: no detection's owed
        time changes and no alert gets a second lag count."""
        outcomes = [made_up_delivery(i) for i in range(5)]
        stream = FakeStream("lasair", outcomes)

        def crash(polled: Polled) -> None:
            raise SimulatedCrash()

        crashing = FakeAdapter(stream, batch=5, on_ack=crash)
        with self.assertRaises(SimulatedCrash):
            IntakeConsumer([crashing], THRESHOLDS, intake(), poll_timeout=0.0).run()
        self.assertEqual(stream.committed, 0)
        first = owed_times()
        self.assertEqual(set(first), {o.dia_source_id for o in outcomes})
        self.assertEqual(lag_count_total(), 5)

        restarted: IntakeConsumer
        adapter = FakeAdapter(stream, batch=5, on_exhausted=lambda: restarted.stop())
        restarted = IntakeConsumer([adapter], THRESHOLDS, intake(), poll_timeout=0.0)
        restarted.run()

        self.assertEqual(adapter.acked, list(range(5)))
        self.assertEqual(adapter.commits, [5])
        self.assertEqual(owed_times(), first)
        self.assertEqual(lag_count_total(), 5)

    def test_redelivered_skips_are_stored_and_counted_once(self) -> None:
        """A batch holding an adapter-reported skip and a delivery the
        database refuses is stored, then the process dies before the broker
        is acknowledged. On redelivery neither skip is stored again and no
        alert is counted twice in the lag record."""
        outcomes: list[Delivery | IntakeSkipReport] = [
            made_up_delivery(0, "babamul"),
            IntakeSkipReport(
                broker="babamul",
                reason="missing field",
                occurred_at=RECEIVED,
                raw_identifier="babamul.topic[0]@1",
                published_at=RECEIVED - timedelta(seconds=30),
            ),
            made_up_delivery(2, "babamul", band="verylongband"),
        ]
        stream = FakeStream("babamul", outcomes)

        def crash(polled: Polled) -> None:
            raise SimulatedCrash()

        crashing = FakeAdapter(stream, batch=3, on_ack=crash)
        with self.assertLogs("seldon", level="WARNING"):
            with self.assertRaises(SimulatedCrash):
                IntakeConsumer([crashing], THRESHOLDS, intake(), poll_timeout=0.0).run()
        self.assertEqual(IntakeSkip.objects.count(), 2)
        self.assertEqual(lag_count_total(), 3)

        restarted: IntakeConsumer
        adapter = FakeAdapter(stream, batch=3, on_exhausted=lambda: restarted.stop())
        restarted = IntakeConsumer([adapter], THRESHOLDS, intake(), poll_timeout=0.0)
        restarted.run()

        self.assertEqual(adapter.commits, [3])
        self.assertEqual(IntakeSkip.objects.count(), 2)
        self.assertEqual(lag_count_total(), 3)

    def test_unstorable_value_in_the_third_alert_is_a_skip_inside_the_batch(
        self,
    ) -> None:
        """The third of five Babamul alerts in a batch names a band longer
        than the database stores. It becomes an acknowledged intake skip;
        alerts one, two, four and five are stored, and all five are
        acknowledged in one commit."""
        outcomes = [
            made_up_delivery(i, "babamul", band="verylongband" if i == 2 else "r")
            for i in range(5)
        ]
        consumer: IntakeConsumer
        adapter = FakeAdapter(
            FakeStream("babamul", outcomes),
            batch=5,
            on_exhausted=lambda: consumer.stop(),
        )
        consumer = IntakeConsumer([adapter], THRESHOLDS, intake(), poll_timeout=0.0)

        with self.assertLogs("seldon", level="WARNING"):
            consumer.run()

        self.assertEqual(adapter.commits, [5])
        self.assertEqual(adapter.acked, list(range(5)))
        self.assertEqual([stored(o) for o in outcomes], [True, True, False, True, True])
        skip = IntakeSkip.objects.get()
        self.assertEqual(
            (skip.broker, skip.reason, skip.raw_identifier),
            ("babamul", "unstorable_value", str(outcomes[2].dia_source_id)),
        )
        self.assertEqual(lag_count_total(), 5)


class PollSchedulingTests(TestCase):
    """Brokers are polled without waiting; the loop waits only when idle."""

    def test_quiet_brokers_never_hold_up_a_busy_one(self) -> None:
        """Fink and Lasair are empty and Babamul holds 300 alerts. Every
        poll of every broker is made with a zero timeout, Babamul's alerts
        are taken in three batches of 100, and the loop waits for the poll
        timeout only once, after the cycle that found nothing anywhere."""
        outcomes = [made_up_delivery(i, "babamul") for i in range(300)]
        busy = FakeAdapter(FakeStream("babamul", outcomes), batch=100)
        quiet = [FakeAdapter(FakeStream(name, [])) for name in ("fink", "lasair")]
        consumer: IntakeConsumer
        sleeps: list[tuple[float, int]] = []

        def stop_on_second_empty_poll() -> None:
            if busy.polls == 5:
                consumer.stop()

        busy.on_exhausted = stop_on_second_empty_poll
        consumer = IntakeConsumer(
            [*quiet, busy],
            THRESHOLDS,
            intake(),
            poll_timeout=0.5,
            sleep=lambda seconds: sleeps.append((seconds, len(busy.acked))),
        )

        consumer.run()

        for adapter in (*quiet, busy):
            with self.subTest(broker=adapter.name):
                self.assertEqual(adapter.polls, 5)
                self.assertEqual(adapter.timeouts, [0.0] * 5)
        self.assertEqual(busy.commits, [100, 200, 300])
        self.assertEqual(sleeps, [(0.5, 300)])

    def test_idle_cycle_waits_once_for_the_poll_timeout(self) -> None:
        """Every broker is empty. After each cycle the loop waits once for
        the 0.5 s poll timeout; the cycle in which it is stopped is not
        followed by a wait."""
        adapters = [
            FakeAdapter(FakeStream(name, [])) for name in ("fink", "lasair", "babamul")
        ]
        consumer: IntakeConsumer
        sleeps: list[float] = []
        adapters[-1].on_exhausted = lambda: (
            consumer.stop() if adapters[-1].polls == 3 else None
        )
        consumer = IntakeConsumer(
            adapters, THRESHOLDS, intake(), poll_timeout=0.5, sleep=sleeps.append
        )

        consumer.run()

        self.assertEqual(sleeps, [0.5, 0.5])
        for adapter in adapters:
            self.assertEqual(adapter.timeouts, [0.0] * 3)


class IntakeConsumerTests(TestCase):
    """The consumer loop with an injected clock, sleep, and database outage."""

    def test_database_outage_leaves_deliveries_unacknowledged_until_recovery(
        self,
    ) -> None:
        """The database goes down as the first Lasair alert is polled and
        stays down for three attempts at it. The consumer backs off longer
        each time, acknowledges nothing, and polls nothing new; once the
        database is back, both alerts are stored, their detections owed, and
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
        self.assertEqual(set(owed_times()), owed)

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


class OnceRefusingConsumer(FakeConsumer):
    """A fake Kafka consumer whose first commit a rebalance refuses."""

    def __init__(self, retained: list[Any]) -> None:
        """Hold the partition's messages.

        Args:
            retained: The messages on partition 0, in offset order.
        """
        super().__init__(retained)
        self.refusals = 0
        self.empty_polls = 0

    def consume(self, num_messages: int = 1, timeout: float = -1) -> list[Any]:
        """Hand out messages, counting polls that find none.

        Returns:
            The next messages.
        """
        out = super().consume(num_messages, timeout)
        if not out:
            self.empty_polls += 1
        return out

    def commit(
        self,
        message: Any = None,
        offsets: list[Any] | None = None,
        asynchronous: bool = True,
    ) -> None:
        """Refuse the first commit as a rebalance would, record the rest.

        Raises:
            KafkaException: On the first commit.
        """
        if self.refusals == 0:
            self.refusals += 1
            raise KafkaException(KafkaError(KafkaError.UNKNOWN_MEMBER_ID))
        super().commit(message, offsets, asynchronous)


class RebalanceTests(TestCase):
    """A rebalance refusing a broker commit does not stop the consumer."""

    def test_refused_commit_leaves_the_batch_unacknowledged_and_runs_on(
        self,
    ) -> None:
        """Two Lasair messages are taken one per batch through the real
        Kafka adapter. A rebalance refuses the first batch's commit
        (UNKNOWN_MEMBER_ID). The consumer keeps running: both alerts are
        stored, the first is never committed, and the second commits the
        partition past itself."""
        kafka = OnceRefusingConsumer(_lasair_messages(0, _ms(RECEIVED))[:2])
        adapter = KafkaBrokerAdapter(
            _lasair_settings(), consumer=kafka, clock=lambda: RECEIVED
        )
        consumer = IntakeConsumer(
            [adapter],
            THRESHOLDS,
            intake(),
            poll_timeout=0.0,
            max_messages=1,
            finished=lambda: kafka.empty_polls > 0,
        )

        with self.assertLogs("seldon.brokers.kafka", level="WARNING") as logs:
            consumer.run()

        self.assertEqual(kafka.refusals, 1)
        self.assertEqual([(c.partition, c.offset) for c in kafka.commits], [(0, 2)])
        self.assertTrue(any("rebalance" in line for line in logs.output))
        for message in lasair.ALERTS[:2]:
            self.assertTrue(
                Detection.objects.filter(
                    dia_source_id=lasair_source(message)[0]
                ).exists()
            )
