"""Tests for replaying recordings through the real Kafka adapter and consumer.

Recordings are written here with ``seldon.replay.recording``'s writer from
the recorded alerts in ``seldon.tests.fixtures.broker_alerts``, re-encoded to
the raw bytes each broker sends: Lasair as JSON, Fink as schemaless Avro with
its schema in the key, Babamul as an Avro container. The schemas are inferred
from the decoded alerts, and every encoding is checked to decode back to the
alert it came from.

Wall time is a fake the tests move: waiting advances it, so a paced replay
runs instantly. Expected release and receipt times are worked out here from
the recorded publication times, the replay's start and its speed-up, never
from the code under test. The replay target is the test database itself;
the database guard is tested on its own.
"""

import copy
import io
import json
import os
import tempfile
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest import mock

import fastavro
from confluent_kafka import TIMESTAMP_CREATE_TIME, TopicPartition
from django.core.management import CommandError, call_command
from django.db import connection, transaction
from django.test import SimpleTestCase, TestCase

from seldon.domain.intake.delivery import BrokerRole
from seldon.domain.intake.qualification import Thresholds
from seldon.infrastructure.brokers.base import ParseContext
from seldon.infrastructure.brokers.parsers import parse_alert
from seldon.intake.engine import process_delivery, sweep_pending
from seldon.management.commands.replay_alerts import run_replay
from seldon.models import Delivery as StoredDelivery
from seldon.models import Detection, Forecast, Refusal
from seldon.replay.recording import RecordedMessage, RecordingWriter
from seldon.replay.source import Replay, ReplayMode, load_recording
from seldon.tests.fixtures.broker_alerts import babamul, fink, lasair

#: The recorded alerts' publication night, as a replay start.
T0 = datetime(2026, 11, 13, 1, 0, tzinfo=UTC)

#: The cuts these tests configure, as the environment gives them.
THRESHOLDS = {
    "fink": Thresholds(reliability_cut=0.6, sn_like_threshold=0.5),
    "lasair": Thresholds(reliability_cut=0.6, sn_like_threshold=1.0),
}

#: Every broker disabled, as in a test environment, with these tests' cuts.
REPLAY_ENV = {
    "SELDON_FINK_ENABLED": "false",
    "SELDON_FINK_RELIABILITY_CUT": "0.6",
    "SELDON_FINK_SN_LIKE_THRESHOLD": "0.5",
    "SELDON_LASAIR_ENABLED": "false",
    "SELDON_LASAIR_RELIABILITY_CUT": "0.6",
    "SELDON_LASAIR_SN_LIKE_THRESHOLD": "1.0",
    "SELDON_BABAMUL_ENABLED": "false",
    "SELDON_INTAKE_SILENCE_MINUTES": "60",
    "SELDON_INTAKE_NIGHT_START_UTC_HOUR": "23",
    "SELDON_INTAKE_NIGHT_END_UTC_HOUR": "11",
}

ROLES = {
    "fink": BrokerRole.QUALIFYING,
    "lasair": BrokerRole.QUALIFYING,
    "babamul": BrokerRole.FULL_STREAM,
}

_SCALAR = ["null", "boolean", "long", "double", "string"]


def _ms(moment: datetime) -> int:
    """Milliseconds since the epoch.

    Args:
        moment: An aware datetime.

    Returns:
        Its Kafka timestamp.
    """
    return int(moment.timestamp() * 1000)


def _utc(ms: int) -> datetime:
    """An aware datetime from a Kafka timestamp.

    Args:
        ms: Milliseconds since the epoch.

    Returns:
        The moment.
    """
    return datetime.fromtimestamp(ms / 1000, tz=UTC)


def _avro_type(examples: list[Any], names: list[str]) -> Any:
    """An Avro type that holds every example value, null included.

    Args:
        examples: Values the type must hold.
        names: Record names used so far; a new record's name is appended.

    Returns:
        The type: a nullable record, array, or scalar union.

    Raises:
        ValueError: If the examples mix records, arrays and scalars.
    """
    present = [value for value in examples if value is not None]
    if present and all(isinstance(value, dict) for value in present):
        name = f"r{len(names)}"
        names.append(name)
        keys = list(dict.fromkeys(key for value in present for key in value))
        fields = [
            {"name": key, "type": _avro_type([v.get(key) for v in present], names)}
            for key in keys
        ]
        return ["null", {"type": "record", "name": name, "fields": fields}]
    if present and all(isinstance(value, list) for value in present):
        items = [item for value in present for item in value]
        return ["null", {"type": "array", "items": _avro_type(items, names)}]
    if any(isinstance(value, (dict, list)) for value in present):
        raise ValueError("examples mix records, arrays and scalars")
    return _SCALAR


def avro_schema(alert: dict[str, Any]) -> dict[str, Any]:
    """A record schema inferred from one decoded alert.

    Args:
        alert: The decoded alert.

    Returns:
        A schema whose records match the alert's dicts field for field.
    """
    return _avro_type([alert], [])[1]


def fink_bytes(alert: dict[str, Any]) -> tuple[bytes, bytes]:
    """A Fink message: the schema as JSON in the key, schemaless Avro value.

    Args:
        alert: The decoded alert.

    Returns:
        The key and value.
    """
    schema = avro_schema(alert)
    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, fastavro.parse_schema(schema), alert)
    return json.dumps(schema).encode(), buffer.getvalue()


def babamul_bytes(alert: dict[str, Any]) -> bytes:
    """A Babamul message: an Avro container holding the alert.

    Args:
        alert: The decoded alert.

    Returns:
        The value.
    """
    buffer = io.BytesIO()
    fastavro.writer(buffer, fastavro.parse_schema(avro_schema(alert)), [alert])
    return buffer.getvalue()


def lasair_bytes(alert: dict[str, Any]) -> bytes:
    """A Lasair message: the alert as JSON.

    Args:
        alert: The decoded alert.

    Returns:
        The value.
    """
    return json.dumps(alert).encode()


def babamul_alert(index: int) -> dict[str, Any]:
    """A recorded Babamul alert made a distinct detection of its own object.

    Args:
        index: Added to the recorded diaSourceId and diaObjectId.

    Returns:
        The decoded alert.
    """
    alert = copy.deepcopy(babamul.ALERTS[0]["alert"])
    candidate = alert["candidate"]
    candidate["diaSourceId"] += index
    candidate["diaObjectId"] += index
    return alert


def message(
    topic: str,
    partition: int,
    offset: int,
    published: datetime,
    value: bytes = b"{}",
    key: bytes | None = None,
) -> RecordedMessage:
    """A recorded message with a create-time timestamp.

    Args:
        topic: Its topic.
        partition: Its partition.
        offset: Its offset.
        published: Its publication time.
        value: Its value.
        key: Its key.

    Returns:
        The message.
    """
    return RecordedMessage(
        topic=topic,
        partition=partition,
        offset=offset,
        timestamp_type=TIMESTAMP_CREATE_TIME,
        timestamp_ms=_ms(published),
        key=key,
        value=value,
    )


def write_recording(
    root: Path,
    by_broker: dict[str, list[RecordedMessage]],
    start: datetime = T0,
    empty_topics: dict[str, str] | None = None,
) -> None:
    """Write a recording with ``recording.py``'s writer.

    Args:
        root: The recording directory.
        by_broker: Each broker's messages, in offset order per partition.
        start: The capture window's start, which is the replay's start.
        empty_topics: Brokers recorded with nothing, each with one empty
            partition of the topic given.
    """
    writer = RecordingWriter(root, start, start + timedelta(days=30))
    for broker, messages in by_broker.items():
        files = {}
        for recorded in messages:
            place = (recorded.topic, recorded.partition)
            if place not in files:
                files[place] = writer.partition(broker, *place)
            files[place].write(recorded)
        writer.broker_done(broker, list(files.values()), 1.0, None)
    for broker, topic in (empty_topics or {}).items():
        writer.broker_done(broker, [writer.partition(broker, topic, 0)], 1.0, None)
    writer.finish()


class FakeWall:
    """Wall time that moves only when the replay waits."""

    def __init__(self) -> None:
        """Start at zero with no waits."""
        self.now = 0.0
        self.waits: list[float] = []

    def monotonic(self) -> float:
        """The current wall time.

        Returns:
            Seconds since the fake started.
        """
        return self.now

    def wait(self, seconds: float) -> None:
        """Wait, moving the time on.

        Args:
            seconds: How long.
        """
        self.waits.append(seconds)
        self.now += seconds


def replay_of(
    root: Path,
    mode: ReplayMode,
    wall: FakeWall,
    speed: float = 1.0,
    brokers: Iterable[str] | None = None,
) -> Replay:
    """A replay of a recording on fake wall time.

    Args:
        root: The recording directory.
        mode: The replay mode.
        wall: The fake wall time.
        speed: The speed-up, for a paced replay.
        brokers: The brokers to replay; all recorded when omitted.

    Returns:
        The replay.
    """
    return Replay(
        load_recording(root, brokers),
        mode,
        speed=speed,
        run_size=100,
        monotonic=wall.monotonic,
        wait=wall.wait,
    )


def subscribe_all(replay: Replay) -> dict[str, Any]:
    """Each broker's file-backed consumer, subscribed and assigned.

    Args:
        replay: The replay.

    Returns:
        Broker name to consumer.
    """
    consumers = {}
    for broker in replay.brokers:
        consumer = replay.consumer(broker)
        consumer.subscribe(
            ["any"], on_assign=lambda c, partitions: c.assign(partitions)
        )
        consumers[broker] = consumer
    return consumers


def commit_all(consumer: Any, messages: list[Any]) -> None:
    """Commit past each message, as the adapter does.

    Args:
        consumer: The file-backed consumer.
        messages: Messages it returned.
    """
    for handed in messages:
        consumer.commit(
            offsets=[
                TopicPartition(handed.topic(), handed.partition(), handed.offset() + 1)
            ],
            asynchronous=False,
        )


def place(handed: Any, broker: str) -> tuple[str, int, int]:
    """A returned message's broker, partition and offset.

    Args:
        handed: A message a consumer returned.
        broker: Its broker.

    Returns:
        The triple.
    """
    return broker, handed.partition(), handed.offset()


def drain_ordered(replay: Replay) -> list[list[tuple[str, int, int]]]:
    """Drive an ordered replay to the end, committing every message.

    Args:
        replay: An ordered replay.

    Returns:
        The runs released, each as its messages' broker, partition, offset.
    """
    consumers = subscribe_all(replay)
    runs = []
    while not replay.finished():
        replay.before_cycle()
        run = []
        for broker, consumer in consumers.items():
            handed = consumer.consume(100, 0.0)
            commit_all(consumer, handed)
            run += [place(m, broker) for m in handed]
        runs.append(run)
    return runs


class OrderedReleaseTests(SimpleTestCase):
    """Ordered mode releases runs in publication order, one run at a time."""

    def test_runs_follow_publication_order_and_wait_for_acknowledgement(
        self,
    ) -> None:
        """Fink at T0, Babamul at T0+1 s and T0+2 s, Lasair at T0+3 s release
        as three runs: Fink, both Babamul, Lasair. Babamul's run is held while
        Fink's message is unacknowledged and released once it is."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_recording(
                root,
                {
                    "fink": [message("f", 0, 0, T0)],
                    "babamul": [
                        message("b", 0, 0, T0 + timedelta(seconds=1)),
                        message("b", 0, 1, T0 + timedelta(seconds=2)),
                    ],
                    "lasair": [message("l", 0, 0, T0 + timedelta(seconds=3))],
                },
            )
            replay = replay_of(root, ReplayMode.ORDERED, FakeWall())
            consumers = subscribe_all(replay)

            replay.before_cycle()
            first = consumers["fink"].consume(100, 0.0)
            self.assertEqual([place(m, "fink") for m in first], [("fink", 0, 0)])
            self.assertEqual(consumers["babamul"].consume(100, 0.0), [])
            replay.before_cycle()
            self.assertEqual(consumers["babamul"].consume(100, 0.0), [])

            commit_all(consumers["fink"], first)
            replay.before_cycle()
            second = consumers["babamul"].consume(100, 0.0)
            self.assertEqual(
                [place(m, "babamul") for m in second],
                [("babamul", 0, 0), ("babamul", 0, 1)],
            )
            commit_all(consumers["babamul"], second)
            replay.before_cycle()
            third = consumers["lasair"].consume(100, 0.0)
            self.assertEqual([place(m, "lasair") for m in third], [("lasair", 0, 0)])
            self.assertFalse(replay.finished())
            commit_all(consumers["lasair"], third)
            self.assertTrue(replay.finished())

    def test_equal_publication_times_order_by_broker_partition_offset(
        self,
    ) -> None:
        """Four messages published at the same moment release Babamul's
        before Lasair's, and Babamul's partition 0 (offsets 2, 7) before its
        partition 1 (offset 3), as one Babamul run and one Lasair run."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_recording(
                root,
                {
                    "lasair": [message("t", 0, 5, T0)],
                    "babamul": [
                        message("t", 1, 3, T0),
                        message("t", 0, 2, T0),
                        message("t", 0, 7, T0),
                    ],
                },
            )
            runs = drain_ordered(replay_of(root, ReplayMode.ORDERED, FakeWall()))

        self.assertEqual(
            runs,
            [
                [("babamul", 0, 2), ("babamul", 0, 7), ("babamul", 1, 3)],
                [("lasair", 0, 5)],
            ],
        )

    def test_run_is_capped_at_one_poll(self) -> None:
        """101 consecutive Babamul messages release as a run of 100 and a run
        of one."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_recording(
                root,
                {
                    "babamul": [
                        message("b", 0, i, T0 + timedelta(seconds=i))
                        for i in range(101)
                    ]
                },
            )
            runs = drain_ordered(replay_of(root, ReplayMode.ORDERED, FakeWall()))

        self.assertEqual([len(run) for run in runs], [100, 1])


class StreamingTests(SimpleTestCase):
    """A replay reads each partition file lazily, never the whole recording."""

    #: Brokers, each with partitions of one topic, each this many messages.
    LAYOUT = {"babamul": 3, "fink": 2, "lasair": 2}
    PER_PARTITION = 150

    def recorded(self) -> dict[str, list[RecordedMessage]]:
        """Messages whose publication times interleave across partitions.

        Each partition's times step by a different stride, and every fifth
        message is stamped a little earlier than the one before it, so it is
        held back to that earlier time.

        Returns:
            Each broker's messages, in offset order per partition.
        """
        by_broker: dict[str, list[RecordedMessage]] = {}
        for b, (broker, partitions) in enumerate(self.LAYOUT.items()):
            by_broker[broker] = []
            for partition in range(partitions):
                stride = 1.0 + 0.37 * (b * 3 + partition)
                for i in range(self.PER_PARTITION):
                    seconds = i * stride - (0.5 if i % 5 == 4 else 0.0)
                    by_broker[broker].append(
                        message(
                            broker[0],
                            partition,
                            1000 + i,
                            T0 + timedelta(seconds=seconds),
                        )
                    )
        return by_broker

    @staticmethod
    def fully_sorted(
        by_broker: dict[str, list[RecordedMessage]],
    ) -> list[tuple[str, int, int]]:
        """Every message in global publication order, worked out here.

        A message's order time is its publication time, held back to the
        latest time before it in its partition; ties go by broker, topic,
        partition and offset.

        Args:
            by_broker: Each broker's messages, in offset order per partition.

        Returns:
            Broker, partition and offset of each, in order.
        """
        keyed = []
        for broker, messages in by_broker.items():
            latest: dict[tuple[str, int], int] = {}
            for m in messages:
                held = max(m.timestamp_ms, latest.get((m.topic, m.partition), 0))
                latest[(m.topic, m.partition)] = held
                keyed.append((held, broker, m.topic, m.partition, m.offset))
        return [(k[1], k[3], k[4]) for k in sorted(keyed)]

    def spy_on_reads(self) -> tuple[Any, dict[tuple[str, int], int]]:
        """Count, per partition, the messages its file reader has yielded.

        Returns:
            A patcher for the replay's partition reader, and the counts.
        """
        from seldon.replay import source

        real = source.read_partition
        read: dict[tuple[str, int], int] = {}

        def counting(root: Path, entry: dict[str, Any]) -> Any:
            for recorded in real(root, entry):
                key = (entry["topic"], entry["partition"])
                read[key] = read.get(key, 0) + 1
                yield recorded

        return mock.patch.object(source, "read_partition", counting), read

    def assert_bounded(
        self,
        read: dict[tuple[str, int], int],
        handed: dict[tuple[str, int], int],
        bound: int,
        peaks: dict[tuple[str, int], int],
    ) -> None:
        """Note each partition's read-but-not-handed count against a bound.

        Args:
            read: Messages read per partition.
            handed: Messages handed out per partition.
            bound: Most a partition may hold.
            peaks: Each partition's largest count so far, updated.
        """
        for key, count in read.items():
            held = count - handed.get(key, 0)
            peaks[key] = max(peaks.get(key, 0), held)
            self.assertLessEqual(held, bound, key)

    def test_max_replay_holds_a_bounded_number_per_partition(self) -> None:
        """A maximum-rate replay of 1,050 messages over seven partitions,
        polled ten at a time, releases them in fully sorted order per broker,
        and no partition ever has more than the ten of one poll plus one
        read ahead of what the consumers have handed out."""
        by_broker = self.recorded()
        expected = self.fully_sorted(by_broker)
        patcher, read = self.spy_on_reads()
        handed: dict[tuple[str, int], int] = {}
        peaks: dict[tuple[str, int], int] = {}
        released: dict[str, list[tuple[str, int, int]]] = {}
        with tempfile.TemporaryDirectory() as tmp, patcher:
            root = Path(tmp)
            write_recording(root, by_broker)
            replay = replay_of(root, ReplayMode.MAX, FakeWall())
            consumers = subscribe_all(replay)
            while not replay.finished():
                for broker, consumer in consumers.items():
                    out = consumer.consume(10, 0.0)
                    self.assert_bounded(read, handed, 11, peaks)
                    for m in out:
                        key = (m.topic(), m.partition())
                        handed[key] = handed.get(key, 0) + 1
                    commit_all(consumer, out)
                    released.setdefault(broker, []).extend(
                        place(m, broker) for m in out
                    )

        for broker in self.LAYOUT:
            self.assertEqual(released[broker], [p for p in expected if p[0] == broker])
        self.assertEqual(len(peaks), sum(self.LAYOUT.values()))
        self.assertEqual(sum(handed.values()), len(expected))

    def test_ordered_replay_releases_in_fully_sorted_order_with_bounded_reads(
        self,
    ) -> None:
        """An ordered replay of the same recording with runs of at most ten
        releases every message in fully sorted order, and no partition ever
        has more than one run plus one read ahead of what is handed out."""
        by_broker = self.recorded()
        expected = self.fully_sorted(by_broker)
        patcher, read = self.spy_on_reads()
        handed: dict[tuple[str, int], int] = {}
        peaks: dict[tuple[str, int], int] = {}
        released: list[tuple[str, int, int]] = []
        with tempfile.TemporaryDirectory() as tmp, patcher:
            root = Path(tmp)
            write_recording(root, by_broker)
            replay = Replay(
                load_recording(root),
                ReplayMode.ORDERED,
                run_size=10,
                monotonic=FakeWall().monotonic,
                wait=FakeWall().wait,
            )
            consumers = subscribe_all(replay)
            while not replay.finished():
                replay.before_cycle()
                self.assert_bounded(read, handed, 11, peaks)
                for broker, consumer in consumers.items():
                    out = consumer.consume(10, 0.0)
                    for m in out:
                        key = (m.topic(), m.partition())
                        handed[key] = handed.get(key, 0) + 1
                    commit_all(consumer, out)
                    released += [place(m, broker) for m in out]

        self.assertEqual(released, expected)
        self.assertEqual(len(peaks), sum(self.LAYOUT.values()))


def stored_snapshot() -> dict[int, dict[str, Any]]:
    """Every stored detection's outcome and deliveries.

    Row ids, ``stored_at`` and ``app_version`` are left out.

    Returns:
        diaSourceId to its state, deliveries, forecasts and refusals.
    """
    snapshot = {}
    for detection in Detection.objects.order_by("dia_source_id"):
        snapshot[detection.dia_source_id] = {
            "state": detection.state,
            "deliveries": sorted(
                StoredDelivery.objects.filter(detection=detection).values_list(
                    "broker",
                    "reliability_score",
                    "sn_like_score",
                    "published_at",
                    "received_at",
                )
            ),
            "forecasts": list(
                Forecast.objects.filter(detection=detection).values(
                    "checkpoint",
                    "light_curve",
                    "grid",
                    "outputs",
                    "thresholds",
                    "qualifying_event",
                )
            ),
            "refusals": list(
                Refusal.objects.filter(detection=detection).values_list("code", "bands")
            ),
        }
    return snapshot


class _RollBack(Exception):
    """Undo a transaction after reading what it stored."""


@mock.patch.dict(os.environ, REPLAY_ENV)
class ReplayIntakeTests(TestCase):
    """Replays through the real adapter and consumer into the test database."""

    def test_paced_replay_releases_on_the_sped_up_clock(self) -> None:
        """Three Babamul messages one second apart, replayed at 2x, release
        at T0, T0+0.5 s and T0+1 s; each delivery is received, and stamped
        published, at its release time. Between them the consumer waits the
        0.5 s until the next is due."""
        published = [T0 + timedelta(seconds=i) for i in range(3)]
        wall = FakeWall()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_recording(
                root,
                {
                    "babamul": [
                        message("b", 0, i, moment, babamul_bytes(babamul_alert(i)))
                        for i, moment in enumerate(published)
                    ]
                },
            )
            summary = run_replay(
                replay_of(root, ReplayMode.PACED, wall, speed=2.0),
                poll_timeout=1.0,
            )

        released = [T0 + (moment - T0) / 2 for moment in published]
        base = babamul.ALERTS[0]["alert"]["candidate"]["diaSourceId"]
        received = [
            StoredDelivery.objects.get(detection__dia_source_id=base + i)
            for i in range(3)
        ]
        self.assertEqual([d.received_at for d in received], released)
        self.assertEqual([d.published_at for d in received], released)
        self.assertEqual(wall.waits, [0.5, 0.5])
        self.assertEqual(summary.alerts, {"babamul": 3})

    def test_commits_stay_in_memory_and_no_kafka_client_is_made(self) -> None:
        """Replaying two Lasair messages, taken as one batch, commits offset
        2 of partition 0 in the file-backed consumer, carrying the second
        message's replay publication time, and never builds a network Kafka
        consumer."""
        published = [T0, T0 + timedelta(seconds=1)]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_recording(
                root,
                {
                    "lasair": [
                        message(
                            lasair.ALERTS[i]["topic"],
                            0,
                            i,
                            moment,
                            lasair_bytes(lasair.ALERTS[i]["alert"]),
                        )
                        for i, moment in enumerate(published)
                    ]
                },
            )
            replay = replay_of(root, ReplayMode.MAX, FakeWall())
            with mock.patch(
                "seldon.infrastructure.brokers.kafka.Consumer",
                side_effect=AssertionError("network consumer built"),
            ) as network:
                run_replay(replay, poll_timeout=0.01)

        network.assert_not_called()
        commits = replay.consumer("lasair").commits
        self.assertEqual(
            [(tp.topic, tp.partition, tp.offset, tp.metadata) for tp in commits],
            [(lasair.ALERTS[1]["topic"], 0, 2, str(_ms(T0)))],
        )

    def test_quiet_brokers_are_polled_without_waiting(self) -> None:
        """A maximum-rate replay of two Babamul messages beside empty Fink
        and Lasair recordings takes both Babamul messages in one poll, and no
        empty broker's poll waits."""
        wall = FakeWall()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_recording(
                root,
                {
                    "babamul": [
                        message("b", 0, i, T0 + timedelta(seconds=i), babamul_bytes(a))
                        for i, a in enumerate([babamul_alert(0), babamul_alert(1)])
                    ]
                },
                empty_topics={"fink": "f", "lasair": "l"},
            )
            summary = run_replay(
                replay_of(root, ReplayMode.MAX, wall), poll_timeout=0.25
            )

        self.assertEqual(wall.waits, [])
        self.assertEqual(summary.alerts, {"babamul": 2, "fink": 0, "lasair": 0})

    def test_ordered_run_shares_the_held_clock_and_defers_the_sweep(self) -> None:
        """101 Babamul messages a second apart from T0 then one at T0+200 s:
        the first run of 100 is received at T0, its first message's time; the
        sweep due at T0+30 s, inside that run, fires at T0+200 s, after all
        100 are acknowledged and before the last message is released."""
        published = [T0 + timedelta(seconds=i) for i in range(100)]
        published.append(T0 + timedelta(seconds=200))
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_recording(
                root,
                {
                    "babamul": [
                        message("b", 0, i, moment, babamul_bytes(babamul_alert(i)))
                        for i, moment in enumerate(published)
                    ]
                },
            )
            replay = replay_of(root, ReplayMode.ORDERED, FakeWall())
            sweeps: list[tuple[datetime, int]] = []

            def spy(now: datetime) -> Any:
                commits = replay.consumer("babamul").commits
                sweeps.append((now, commits[-1].offset if commits else 0))
                return sweep_pending(now)

            with mock.patch(
                "seldon.management.commands.consume_alerts.sweep_pending", spy
            ):
                run_replay(
                    replay, poll_timeout=0.01, sweep_interval=timedelta(seconds=30)
                )

        base = babamul.ALERTS[0]["alert"]["candidate"]["diaSourceId"]
        received = dict(
            StoredDelivery.objects.values_list(
                "detection__dia_source_id", "received_at"
            )
        )
        self.assertEqual([received[base + i] for i in range(100)], [T0] * 100)
        self.assertEqual(received[base + 100], T0 + timedelta(seconds=200))
        self.assertEqual(sweeps, [(T0, 0), (T0 + timedelta(seconds=200), 100)])

    def test_sweep_runs_on_replay_time(self) -> None:
        """Babamul alerts published at T0 and T0+8 days, replayed in real
        time with a 12-hour poll timeout: the seven-day sweep on the replay
        clock discards the first detection before the second arrives, which
        stays pending."""
        base = babamul.ALERTS[0]["alert"]["candidate"]["diaSourceId"]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_recording(
                root,
                {
                    "babamul": [
                        message("b", 0, 0, T0, babamul_bytes(babamul_alert(0))),
                        message(
                            "b",
                            0,
                            1,
                            T0 + timedelta(days=8),
                            babamul_bytes(babamul_alert(1)),
                        ),
                    ]
                },
            )
            with self.assertLogs("seldon", level="INFO"):
                run_replay(
                    replay_of(root, ReplayMode.PACED, FakeWall()),
                    poll_timeout=12 * 3600.0,
                )

        self.assertFalse(Detection.objects.filter(dia_source_id=base).exists())
        self.assertEqual(
            Detection.objects.get(dia_source_id=base + 1).state,
            Detection.State.PENDING,
        )

    def test_recorded_fixtures_replay_to_the_same_outcomes_as_direct_intake(
        self,
    ) -> None:
        """The recorded Babamul, Fink and Lasair alerts, re-encoded to the
        bytes each broker sends and replayed in ordered mode, store the same
        detections, deliveries, forecasts and refusals as handing the same
        alerts to ``process_delivery`` in publication order, each received at
        the publication time of the first message of its broker's run."""
        encoded: dict[str, list[RecordedMessage]] = {}
        direct: list[tuple[str, dict[str, Any], datetime, datetime]] = []
        for broker, module in (
            ("babamul", babamul),
            ("fink", fink),
            ("lasair", lasair),
        ):
            first_ms = module.ALERTS[0]["kafka_timestamp_ms"]
            encoded[broker] = []
            for offset, recorded in enumerate(module.ALERTS):
                alert = recorded["alert"]
                if broker == "fink":
                    key, value = fink_bytes(alert)
                    decoded = fastavro.schemaless_reader(
                        io.BytesIO(value), fastavro.parse_schema(json.loads(key))
                    )
                elif broker == "babamul":
                    key, value = None, babamul_bytes(alert)
                    decoded = next(fastavro.reader(io.BytesIO(value)))
                else:
                    key, value = None, lasair_bytes(alert)
                    decoded = json.loads(value)
                # As JSON, so the NaN some alerts carry compares equal.
                self.assertEqual(json.dumps(decoded), json.dumps(alert))
                ms = recorded["kafka_timestamp_ms"]
                encoded[broker].append(
                    message(recorded["topic"], 0, offset, _utc(ms), value, key)
                )
                # Each broker's two alerts are consecutive in publication
                # order, so they form one run received at the first's time.
                direct.append((broker, recorded, _utc(ms), _utc(first_ms)))

        with self.assertRaises(_RollBack), transaction.atomic():
            for broker, recorded, published, received in direct:
                context = ParseContext(
                    broker=broker,
                    role=ROLES[broker],
                    topic=recorded["topic"],
                    published_at=published,
                    received_at=received,
                )
                delivery = parse_alert(broker, recorded["alert"], context)
                result = process_delivery(delivery, THRESHOLDS, clock=lambda: received)
                self.assertTrue(result.acknowledge)
            expected = stored_snapshot()
            raise _RollBack()

        start = min(_utc(m["kafka_timestamp_ms"]) for m in babamul.ALERTS)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_recording(root, encoded, start=start)
            run_replay(
                replay_of(root, ReplayMode.ORDERED, FakeWall()), poll_timeout=0.01
            )

        self.assertEqual(len(expected), 6)
        self.assertTrue(any(entry["forecasts"] for entry in expected.values()))
        self.assertEqual(stored_snapshot(), expected)


class DatabaseGuardTests(TestCase):
    """The command refuses the live database and existing databases."""

    def test_configured_database_is_refused_before_reading_the_recording(
        self,
    ) -> None:
        """Naming the configured database stops the command before the
        recording is read or any database is created."""
        live = connection.settings_dict["NAME"]
        with (
            mock.patch(
                "seldon.management.commands.replay_alerts.read_manifest"
            ) as read,
            mock.patch(
                "seldon.management.commands.replay_alerts.create_database"
            ) as create,
            self.assertRaises(CommandError) as refused,
        ):
            call_command("replay_alerts", recording="/nonexistent", database=live)

        self.assertIn("configured", str(refused.exception))
        read.assert_not_called()
        create.assert_not_called()

    def test_existing_database_is_refused_without_replace(self) -> None:
        """A replay database that already exists (here ``postgres``, which
        always does) is refused, and nothing is replayed."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_recording(root, {"lasair": [message("l", 0, 0, T0)]})
            with (
                mock.patch(
                    "seldon.management.commands.replay_alerts.run_replay"
                ) as replay,
                self.assertRaises(CommandError) as refused,
            ):
                call_command("replay_alerts", recording=str(root), database="postgres")

        self.assertIn("exists", str(refused.exception))
        replay.assert_not_called()
