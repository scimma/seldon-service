"""Tests for the ``capture_alerts`` command and the recording reader.

The command runs against an in-memory fake of the Kafka consumer, so no
network is used. Each fake topic partition holds messages before, inside
and after the capture window; expected bytes come from the fake's own
messages.
"""

import json
import logging
import os
import re
import tempfile
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest import mock

from confluent_kafka import TIMESTAMP_CREATE_TIME, TopicPartition
from django.core.management import CommandError, call_command
from django.test import SimpleTestCase

from seldon.replay.recording import RecordedMessage, read_manifest, read_messages

# Distinctive stand-ins for credentials; none of them may surface in text.
SECRET_USER = "sentinel-user-7f3a"
SECRET_PASSWORD = "sentinel-password-91c2"
SECRET_GROUP = f"{SECRET_USER}-sentinel-group"
SECRET_SERVERS = "sentinel-servers.invalid:9093"
LASAIR_GROUP = "sentinel-lasair-group-55d1"
LASAIR_SERVERS = "sentinel-lasair-servers.invalid:9092"
SECRETS = (
    SECRET_USER,
    SECRET_PASSWORD,
    SECRET_GROUP,
    SECRET_SERVERS,
    LASAIR_GROUP,
    LASAIR_SERVERS,
)

BROKER_ENV = {
    "SELDON_LASAIR_ENABLED": "true",
    "SELDON_LASAIR_GROUP_ID": LASAIR_GROUP,
    "SELDON_LASAIR_BOOTSTRAP_SERVERS": LASAIR_SERVERS,
    "SELDON_BABAMUL_ENABLED": "true",
    "SELDON_BABAMUL_USERNAME": SECRET_USER,
    "SELDON_BABAMUL_PASSWORD": SECRET_PASSWORD,
    "SELDON_BABAMUL_GROUP_ID": SECRET_GROUP,
    "SELDON_BABAMUL_BOOTSTRAP_SERVERS": SECRET_SERVERS,
}

LASAIR_TOPIC = "lasair_366c0efe0df"
BABAMUL_TOPIC = "babamul.lsst.no-color"
OTHER_TOPIC = "unrelated.topic"

WINDOW_FROM = "2026-11-14T01:00:00Z"
WINDOW_TO = "2026-11-14T02:00:00Z"
START_MS = int(datetime(2026, 11, 14, 1, tzinfo=timezone.utc).timestamp() * 1000)
END_MS = int(datetime(2026, 11, 14, 2, tzinfo=timezone.utc).timestamp() * 1000)
BEFORE_MS = START_MS - 60_000
AFTER_MS = END_MS + 60_000


class FakeMessage:
    """A Kafka message as ``confluent_kafka.Message`` presents it."""

    def __init__(
        self,
        topic: str,
        partition: int,
        offset: int,
        timestamp_ms: int,
        key: bytes | None,
        value: bytes,
    ) -> None:
        """Hold one message.

        Args:
            topic: Its topic.
            partition: Its partition.
            offset: Its offset.
            timestamp_ms: Its create time, in ms since the epoch.
            key: Its key.
            value: Its payload.
        """
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._timestamp_ms = timestamp_ms
        self._key = key
        self._value = value

    def error(self) -> None:
        """No message here is an error."""
        return None

    def topic(self) -> str:
        """The topic."""
        return self._topic

    def partition(self) -> int:
        """The partition."""
        return self._partition

    def offset(self) -> int:
        """The offset."""
        return self._offset

    def key(self) -> bytes | None:
        """The key."""
        return self._key

    def value(self) -> bytes:
        """The payload."""
        return self._value

    def timestamp(self) -> tuple[int, int]:
        """The create time."""
        return TIMESTAMP_CREATE_TIME, self._timestamp_ms


class FakeTopic:
    """Topic metadata as ``list_topics`` presents it."""

    def __init__(self, partitions: list[int]) -> None:
        """Hold the partition ids.

        Args:
            partitions: The topic's partition ids.
        """
        self.partitions = {p: object() for p in partitions}


class FakeMetadata:
    """Cluster metadata as ``list_topics`` presents it."""

    def __init__(self, topics: dict[str, FakeTopic]) -> None:
        """Hold the topics.

        Args:
            topics: Topic name to its metadata.
        """
        self.topics = topics


class FakeConsumer:
    """The part of ``confluent_kafka.Consumer`` the capture uses.

    Holds each topic partition's retained messages in offset order and
    serves them from the assigned offsets, a few per ``consume``.
    """

    def __init__(
        self,
        config: dict[str, Any],
        retained: dict[tuple[str, int], list[FakeMessage]],
    ) -> None:
        """Set up the partitions.

        Args:
            config: The client configuration the capture passed.
            retained: Each topic partition's messages, in offset order.
        """
        self.config = config
        self.retained = retained
        self.commits: list[Any] = []
        self.subscribed: list[Any] = []
        self.committed_calls: list[Any] = []
        self._positions: dict[tuple[str, int], int] = {}
        self._paused: set[tuple[str, int]] = set()
        self.closed = False

    def list_topics(self, topic: str | None = None, timeout: float = -1) -> Any:
        """Every topic the fake holds, plus one with no messages.

        Returns:
            The cluster metadata.
        """
        partitions: dict[str, list[int]] = {}
        for name, partition in self.retained:
            partitions.setdefault(name, []).append(partition)
        return FakeMetadata({name: FakeTopic(ps) for name, ps in partitions.items()})

    def get_watermark_offsets(
        self, partition: TopicPartition, timeout: float = -1, cached: bool = False
    ) -> tuple[int, int]:
        """The low and high watermarks of a partition.

        Returns:
            The first offset and one past the last.
        """
        messages = self.retained[(partition.topic, partition.partition)]
        if not messages:
            return 0, 0
        return messages[0].offset(), messages[-1].offset() + 1

    def offsets_for_times(
        self, partitions: list[TopicPartition], timeout: float = -1
    ) -> list[TopicPartition]:
        """The first offset at or after each requested time.

        Returns:
            The partitions with offsets; -1 where no message is that late.
        """
        found = []
        for tp in partitions:
            offset = -1
            for message in self.retained[(tp.topic, tp.partition)]:
                if message.timestamp()[1] >= tp.offset:
                    offset = message.offset()
                    break
            found.append(TopicPartition(tp.topic, tp.partition, offset))
        return found

    def assign(self, partitions: list[TopicPartition]) -> None:
        """Start reading each partition at its offset."""
        for tp in partitions:
            self._positions[(tp.topic, tp.partition)] = tp.offset

    def pause(self, partitions: list[TopicPartition]) -> None:
        """Stop serving the partitions."""
        for tp in partitions:
            self._paused.add((tp.topic, tp.partition))

    def position(self, partitions: list[TopicPartition]) -> list[TopicPartition]:
        """The next offset to read on each partition."""
        return [
            TopicPartition(
                tp.topic, tp.partition, self._positions[(tp.topic, tp.partition)]
            )
            for tp in partitions
        ]

    def consume(self, num_messages: int = 1, timeout: float = -1) -> list[FakeMessage]:
        """Up to two messages from each unpaused assigned partition.

        Returns:
            The messages, partition by partition.
        """
        batch = []
        for key, position in self._positions.items():
            if key in self._paused:
                continue
            ahead = [m for m in self.retained[key] if m.offset() >= position][:2]
            batch += ahead
            if ahead:
                self._positions[key] = ahead[-1].offset() + 1
        return batch[:num_messages]

    def subscribe(self, *args: Any, **kwargs: Any) -> None:
        """Record a subscription, which the capture must never make."""
        self.subscribed.append((args, kwargs))

    def commit(self, *args: Any, **kwargs: Any) -> None:
        """Record a commit, which the capture must never make."""
        self.commits.append((args, kwargs))

    def committed(self, *args: Any, **kwargs: Any) -> list[TopicPartition]:
        """Record a committed-offset lookup, which the capture never needs."""
        self.committed_calls.append((args, kwargs))
        return []

    def close(self) -> None:
        """Close the connection."""
        self.closed = True


def _partition_messages(topic: str, partition: int, first: int) -> list[FakeMessage]:
    """Two messages before the window, three inside, two after.

    Args:
        topic: The topic.
        partition: The partition.
        first: The first retained offset.

    Returns:
        The partition's messages, in offset order.
    """
    times = [BEFORE_MS, BEFORE_MS + 1, START_MS, START_MS + 500, END_MS - 1]
    times += [END_MS, AFTER_MS]
    messages = []
    for index, timestamp in enumerate(times):
        offset = first + index
        key = None if index == 3 else f"key-{partition}-{offset}".encode()
        # Bytes that are not text, including a zero byte, to check exactness.
        value = bytes([partition, index, 0, 255]) + os.urandom(64)
        messages.append(FakeMessage(topic, partition, offset, timestamp, key, value))
    return messages


def _retained() -> dict[str, dict[tuple[str, int], list[FakeMessage]]]:
    """Each broker's retained messages.

    Returns:
        Broker name to its topic partitions' messages: Lasair's topic has two
        partitions, one empty one; Babamul's matching topic has one, and a
        topic its pattern does not match has one.
    """
    return {
        "lasair": {
            (LASAIR_TOPIC, 0): _partition_messages(LASAIR_TOPIC, 0, 10),
            (LASAIR_TOPIC, 1): _partition_messages(LASAIR_TOPIC, 1, 40),
            (LASAIR_TOPIC, 2): [],
        },
        "babamul": {
            (BABAMUL_TOPIC, 0): _partition_messages(BABAMUL_TOPIC, 0, 0),
            (OTHER_TOPIC, 0): _partition_messages(OTHER_TOPIC, 0, 0),
        },
    }


@mock.patch.dict(os.environ, BROKER_ENV, clear=True)
class CaptureAlertsCommandTests(SimpleTestCase):
    """The ``capture_alerts`` command against fake consumers."""

    def setUp(self) -> None:
        """Make a fresh output directory and fake broker contents."""
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.output = Path(self._tmp.name) / "recording"
        self.retained = _retained()
        self.consumers: list[FakeConsumer] = []

    def factory(self) -> Callable[[dict[str, Any]], FakeConsumer]:
        """A consumer factory that serves each broker's retained messages.

        Returns:
            A stand-in for ``confluent_kafka.Consumer``.
        """

        def build(config: dict[str, Any]) -> FakeConsumer:
            broker = (
                "babamul" if config["bootstrap.servers"] == SECRET_SERVERS else "lasair"
            )
            consumer = FakeConsumer(config, self.retained[broker])
            self.consumers.append(consumer)
            return consumer

        return build

    def run_command(self, *extra: str) -> None:
        """Capture the window into the output directory with fakes.

        Args:
            *extra: Further command-line arguments.
        """
        with mock.patch(
            "seldon.management.commands.capture_alerts.Consumer", self.factory()
        ):
            call_command(
                "capture_alerts",
                "--from",
                WINDOW_FROM,
                "--to",
                WINDOW_TO,
                "--output",
                str(self.output),
                *extra,
            )

    def expected(self, broker: str, topic: str) -> list[RecordedMessage]:
        """The in-window messages of a broker's topic, as recorded.

        Args:
            broker: The broker.
            topic: The topic.

        Returns:
            Messages with timestamps in the window, partition by partition.
        """
        found = []
        for (name, _partition), messages in sorted(self.retained[broker].items()):
            if name != topic:
                continue
            for m in messages:
                if START_MS <= m.timestamp()[1] < END_MS:
                    found.append(
                        RecordedMessage(
                            topic=m.topic(),
                            partition=m.partition(),
                            offset=m.offset(),
                            timestamp_type=TIMESTAMP_CREATE_TIME,
                            timestamp_ms=m.timestamp()[1],
                            key=m.key(),
                            value=m.value(),
                        )
                    )
        return found

    def test_only_in_window_messages_are_recorded_exactly(self) -> None:
        """Of messages before, inside and after the window on two partitions,
        the reader returns exactly the in-window ones, with their offsets,
        timestamps, keys and value bytes unchanged."""
        self.run_command()

        lasair = list(read_messages(self.output, "lasair"))
        self.assertEqual(lasair, self.expected("lasair", LASAIR_TOPIC))
        self.assertEqual(len(lasair), 6)
        self.assertEqual({m.partition for m in lasair}, {0, 1})
        manifest = read_manifest(self.output)
        self.assertEqual(manifest["brokers"]["lasair"]["messages"], 6)
        self.assertEqual(
            manifest["brokers"]["lasair"]["bytes"],
            sum(len(m.value) + len(m.key or b"") for m in lasair),
        )

    def test_babamul_pattern_selects_its_topics(self) -> None:
        """Babamul's topic pattern is resolved against the broker's topics:
        its matching topic is recorded and an unrelated one is not."""
        self.run_command("--brokers", "babamul")

        babamul = list(read_messages(self.output, "babamul"))
        self.assertEqual(babamul, self.expected("babamul", BABAMUL_TOPIC))
        self.assertEqual(list(read_manifest(self.output)["brokers"]), ["babamul"])

    def test_capture_never_commits_or_uses_the_live_group(self) -> None:
        """No consumer commits, subscribes, or looks up committed offsets,
        automatic commits are off, and neither broker's live group id is
        the one the capture uses."""
        self.run_command()

        self.assertEqual(len(self.consumers), 2)
        for consumer in self.consumers:
            self.assertEqual(consumer.commits, [])
            self.assertEqual(consumer.subscribed, [])
            self.assertEqual(consumer.committed_calls, [])
            self.assertIs(consumer.config["enable.auto.commit"], False)
            self.assertNotIn(consumer.config["group.id"], (LASAIR_GROUP, SECRET_GROUP))
            self.assertTrue(consumer.closed)

    def test_babamul_group_is_prefixed_with_the_username(self) -> None:
        """Babamul's capture group starts with ``<username>-``, as Babamul
        requires."""
        self.run_command("--brokers", "babamul")

        (consumer,) = self.consumers
        self.assertTrue(consumer.config["group.id"].startswith(f"{SECRET_USER}-"))

    def test_credentials_stay_out_of_logs_manifest_and_paths(self) -> None:
        """No credential appears in a log line, the manifest, or the name of
        any file the capture writes."""
        with self.assertLogs("seldon", level=logging.DEBUG) as logs:
            logging.getLogger("seldon").debug("start")
            self.run_command()

        texts = ["\n".join(logs.output)]
        texts.append((self.output / "manifest.json").read_text())
        texts += [str(path) for path in self.output.rglob("*")]
        for text in texts:
            for secret in SECRETS:
                self.assertNotIn(secret, text)

    def test_rate_is_logged_beside_the_required_rate(self) -> None:
        """Each broker's log line gives its message count, bytes, elapsed
        time, measured MB/s and the MB/s required at the target."""
        with self.assertLogs("seldon.capture", level=logging.INFO) as logs:
            self.run_command()

        output = "\n".join(logs.output)
        self.assertRegex(output, r"lasair: 6 messages, \d+ bytes in [\d.]+ s")
        self.assertRegex(output, r"lasair: .*MB/s.*required 0\.084 MB/s")
        self.assertRegex(output, r"babamul: .*MB/s.*required 2\.7 MB/s")
        manifest = read_manifest(self.output)
        self.assertEqual(manifest["brokers"]["babamul"]["required_mb_per_s"], 2.7)
        self.assertIn("mb_per_s", manifest["brokers"]["lasair"])
        self.assertIn("elapsed_seconds", manifest["brokers"]["lasair"])

    def test_second_capture_of_the_window_needs_replace(self) -> None:
        """Capturing the same window again without ``--replace`` stops with
        a message naming the option, and the first recording is unchanged;
        with ``--replace`` the capture runs."""
        self.run_command()
        first_manifest = (self.output / "manifest.json").read_bytes()
        first = list(read_messages(self.output, "lasair"))
        self.retained["lasair"][(LASAIR_TOPIC, 0)] = []

        with self.assertRaisesRegex(CommandError, re.escape("--replace")):
            self.run_command()
        self.assertEqual((self.output / "manifest.json").read_bytes(), first_manifest)
        self.assertEqual(list(read_messages(self.output, "lasair")), first)

        self.run_command("--replace")
        self.assertEqual(len(list(read_messages(self.output, "lasair"))), 3)

    def test_empty_partition_is_an_empty_entry(self) -> None:
        """A partition with no messages is recorded as an entry holding no
        messages, not an error."""
        self.run_command("--brokers", "lasair")

        entries = read_manifest(self.output)["brokers"]["lasair"]["partitions"]
        empty = [e for e in entries if e["partition"] == 2]
        self.assertEqual(len(empty), 1)
        self.assertEqual(empty[0]["topic"], LASAIR_TOPIC)
        self.assertEqual(empty[0]["messages"], 0)
        self.assertTrue((self.output / empty[0]["file"]).exists())
