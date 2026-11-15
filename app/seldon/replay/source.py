"""Replay a recording through the Kafka adapter as if it came off the network.

A ``Replay`` holds a recording's messages and releases them on a replay
clock. It hands each broker a ``FileConsumer``, a stand-in for
``confluent_kafka.Consumer`` that the Kafka adapter takes through its
``consumer=`` seam, so decoding, parsing, receipt and acknowledgement run
unchanged. Bind the adapter's clock and the intake consumer's clock to
``Replay.clock``.

The replay clock. The replay's start is the recording window's start. Each
message's replay publication time is its recorded publication time, shifted
to the replay's start and divided by the speed-up; the consumer presents it
as the message's timestamp, so lag is measured against it. A message whose
timestamp is earlier than one before it in its partition is held back to
that earlier message's time, because Kafka never reorders a partition. A
message with no timestamp counts as published at the replay's start.

The modes:

- paced: the clock runs at wall rate from the replay's start, and a message
  is released when the clock reaches its replay publication time;
- max: every message is released at once, at the replay's start, and the
  clock runs at wall rate from there;
- ordered: messages are released in global publication order (ties broken
  by broker, topic, partition, offset) as runs of consecutive messages from
  one broker, each run at most ``run_size`` messages so it is one poll. The
  next run is released only once every message of the one before is
  committed. ``before_cycle`` releases it: it moves the clock to the run's
  first recorded publication time (not sped up) and holds it there for the
  whole run. Call it at the start of each intake cycle, before the sweep, so
  the sweep is evaluated at run boundaries only.

In paced and max mode, a ``consume`` that finds nothing due waits in wall
time, up to its timeout or until the broker's next message is due, as
librdkafka blocks on a quiet broker. In ordered mode nothing can become due
by waiting, so ``consume`` returns at once.

Commits are recorded in memory only; nothing here opens a network
connection. The whole recording is held in memory.
"""

import time
from collections import deque
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any

from confluent_kafka import OFFSET_INVALID, TIMESTAMP_NOT_AVAILABLE, TopicPartition

from seldon.replay.recording import RecordedMessage, read_manifest, read_messages


#: The Kafka timestamp epoch.
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class ReplayMode(StrEnum):
    """How a replay releases messages."""

    PACED = "paced"
    """On the replay clock, at a chosen speed-up."""

    MAX = "max"
    """All at once, to find the ceiling."""

    ORDERED = "ordered"
    """One broker run at a time, each after the last is committed."""


@dataclass(frozen=True)
class Recording:
    """A recording's messages, loaded.

    Attributes:
        start: The capture window's start, aware UTC.
        partitions: Each broker's recorded topic partitions, empty ones
            included, in manifest order.
        messages: Each broker's messages, each partition in offset order.
    """

    start: datetime
    partitions: dict[str, list[tuple[str, int]]]
    messages: dict[str, list[RecordedMessage]]


def load_recording(root: Path, brokers: Iterable[str] | None = None) -> Recording:
    """Read a recording into memory.

    Args:
        root: The recording directory.
        brokers: The brokers to load; every recorded one when omitted.

    Returns:
        The recording.

    Raises:
        FileNotFoundError: If the directory holds no recording.
        KeyError: If a named broker is not in the recording.
    """
    manifest = read_manifest(root)
    if manifest is None:
        raise FileNotFoundError(f"no recording in {root}")
    recorded = manifest["brokers"]
    names = list(recorded) if brokers is None else list(brokers)
    for name in names:
        if name not in recorded:
            raise KeyError(name)
    return Recording(
        start=datetime.fromisoformat(manifest["window"]["from"]),
        partitions={
            name: [(e["topic"], e["partition"]) for e in recorded[name]["partitions"]]
            for name in names
        },
        messages={name: list(read_messages(root, name)) for name in names},
    )


class ReplayClock:
    """The time every part of a replayed intake reads."""

    def __init__(self, start: datetime, monotonic: Callable[[], float]) -> None:
        """Start running from ``start`` now.

        Args:
            start: The replay's start.
            monotonic: Wall seconds, as ``time.monotonic``.
        """
        self._start = start
        self._monotonic = monotonic
        self._origin = monotonic()
        self._held: datetime | None = None

    def __call__(self) -> datetime:
        """The current replay time.

        Returns:
            The held time, or the start plus the wall time since then.
        """
        if self._held is not None:
            return self._held
        return self._start + timedelta(seconds=self._monotonic() - self._origin)

    def hold(self, moment: datetime) -> None:
        """Stop the clock at a moment until held elsewhere.

        Args:
            moment: The time to read from now on.
        """
        self._held = moment


@dataclass(frozen=True)
class ReplayMessage:
    """A recorded message, presented as ``confluent_kafka.Message`` is.

    Attributes:
        broker: Its broker.
        recorded: The message as captured.
        release_at: Its replay publication time.
        order_at: When it is ordered: its recorded publication time, held
            back to the one before it in its partition.
    """

    broker: str
    recorded: RecordedMessage
    release_at: datetime
    order_at: datetime

    def error(self) -> None:
        """No replayed message is an error."""
        return None

    def topic(self) -> str:
        """The topic."""
        return self.recorded.topic

    def partition(self) -> int:
        """The partition."""
        return self.recorded.partition

    def offset(self) -> int:
        """The offset."""
        return self.recorded.offset

    def key(self) -> bytes | None:
        """The key, as recorded."""
        return self.recorded.key

    def value(self) -> bytes | None:
        """The value, as recorded."""
        return self.recorded.value

    def timestamp(self) -> tuple[int, int]:
        """The timestamp type and the replay publication time in ms."""
        if self.recorded.timestamp_type == TIMESTAMP_NOT_AVAILABLE:
            return TIMESTAMP_NOT_AVAILABLE, self.recorded.timestamp_ms
        since_epoch = self.release_at - _EPOCH
        return self.recorded.timestamp_type, since_epoch // timedelta(milliseconds=1)

    def sort_key(self) -> tuple[datetime, str, str, int, int]:
        """Its place in global publication order.

        Returns:
            Order time, broker, topic, partition, offset.
        """
        r = self.recorded
        return self.order_at, self.broker, r.topic, r.partition, r.offset


def _utc(timestamp_ms: int) -> datetime:
    """An aware UTC datetime from a Kafka timestamp.

    Args:
        timestamp_ms: Milliseconds since the epoch.

    Returns:
        The moment.
    """
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)


class Replay:
    """A recording being released to file-backed consumers on a replay clock."""

    def __init__(
        self,
        recording: Recording,
        mode: ReplayMode,
        *,
        run_size: int,
        speed: float = 1.0,
        monotonic: Callable[[], float] = time.monotonic,
        wait: Callable[[float], None] = time.sleep,
    ) -> None:
        """Schedule every message; nothing is released until polled.

        Args:
            recording: The loaded recording.
            mode: How messages are released.
            run_size: Most messages in an ordered run: the consumer's most
                messages a poll.
            speed: The speed-up of a paced replay.
            monotonic: Wall seconds, as ``time.monotonic``.
            wait: Waits wall seconds, as ``time.sleep``.

        Raises:
            ValueError: If ``speed`` is not positive.
        """
        if speed <= 0:
            raise ValueError("speed must be positive")
        self.mode = mode
        self.monotonic = monotonic
        self.brokers = list(recording.messages)
        self._wait = wait
        self._start = recording.start
        self._partitions = recording.partitions
        self._committed: dict[tuple[str, str, int], int] = {}
        self._lows: dict[tuple[str, str, int], int] = {}
        self._ends: dict[tuple[str, str, int], int] = {}
        self._queues: dict[str, deque[ReplayMessage]] = {}
        everything: list[ReplayMessage] = []
        for broker, messages in recording.messages.items():
            scheduled = self._schedule(broker, messages, speed)
            scheduled.sort(key=ReplayMessage.sort_key)
            self._queues[broker] = deque(scheduled)
            everything += scheduled
        self._runs: deque[list[ReplayMessage]] = deque()
        self._current: list[ReplayMessage] = []
        self.clock = ReplayClock(recording.start, monotonic)
        if mode is ReplayMode.ORDERED:
            everything.sort(key=ReplayMessage.sort_key)
            for item in everything:
                last = self._runs[-1] if self._runs else None
                if last and last[0].broker == item.broker and len(last) < run_size:
                    last.append(item)
                else:
                    self._runs.append([item])
            self._queues = {broker: deque() for broker in self.brokers}
            self.clock.hold(self._runs[0][0].release_at if self._runs else self._start)
        self._consumers = {
            broker: FileConsumer(self, broker) for broker in self.brokers
        }

    def _schedule(
        self, broker: str, messages: list[RecordedMessage], speed: float
    ) -> list[ReplayMessage]:
        """Give each of a broker's messages its order and release times.

        Args:
            broker: The broker.
            messages: Its messages, each partition in offset order.
            speed: The paced speed-up.

        Returns:
            The scheduled messages.
        """
        latest: dict[tuple[str, int], datetime] = {}
        scheduled = []
        for recorded in messages:
            partition = (recorded.topic, recorded.partition)
            self._lows.setdefault((broker, *partition), recorded.offset)
            self._ends[(broker, *partition)] = recorded.offset
            if recorded.timestamp_type == TIMESTAMP_NOT_AVAILABLE:
                published = self._start
            else:
                published = _utc(recorded.timestamp_ms)
            order_at = max(published, latest.get(partition, published))
            latest[partition] = order_at
            if self.mode is ReplayMode.MAX:
                release_at = self._start
            elif self.mode is ReplayMode.ORDERED:
                release_at = order_at
            else:
                release_at = self._start + (order_at - self._start) / speed
            scheduled.append(ReplayMessage(broker, recorded, release_at, order_at))
        return scheduled

    def consumer(self, broker: str) -> "FileConsumer":
        """A broker's file-backed consumer.

        Args:
            broker: The broker.

        Returns:
            Its consumer, the same one on every call.
        """
        return self._consumers[broker]

    def partitions(self, broker: str) -> list[tuple[str, int]]:
        """A broker's recorded topic partitions.

        Args:
            broker: The broker.

        Returns:
            Topic and partition of each.
        """
        return list(self._partitions[broker])

    def watermarks(self, broker: str, topic: str, partition: int) -> tuple[int, int]:
        """A recorded partition's first offset and the offset after its last.

        Args:
            broker: The broker.
            topic: The topic.
            partition: The partition.

        Returns:
            Low and high watermarks; both 0 for an empty partition.
        """
        key = (broker, topic, partition)
        if key not in self._lows:
            return 0, 0
        return self._lows[key], self._ends[key] + 1

    def take(self, broker: str, num_messages: int, timeout: float) -> list[Any]:
        """Release a broker's due messages, waiting when none is due.

        Args:
            broker: The broker polled.
            num_messages: Most messages to return.
            timeout: Longest wall wait, in seconds, in paced and max mode.

        Returns:
            The released messages, in order; empty when none is due.
        """
        queue = self._queues[broker]
        if self.mode is not ReplayMode.ORDERED:
            now = self.clock()
            if not queue or queue[0].release_at > now:
                delay = timeout
                if queue:
                    delay = min(delay, (queue[0].release_at - now).total_seconds())
                if delay > 0:
                    self._wait(delay)
            now = self.clock()
        out = []
        while queue and len(out) < num_messages:
            if self.mode is not ReplayMode.ORDERED and queue[0].release_at > now:
                break
            out.append(queue.popleft())
        return out

    def before_cycle(self) -> None:
        """In ordered mode, release the next run once the last is committed.

        The clock moves to the next run's first publication time and holds
        there. Nothing happens in paced or max mode, or while the last run
        has an uncommitted message.
        """
        if self.mode is not ReplayMode.ORDERED or not self._runs:
            return
        if not all(self._is_committed(m) for m in self._current):
            return
        self._current = self._runs.popleft()
        self.clock.hold(self._current[0].release_at)
        self._queues[self._current[0].broker].extend(self._current)

    def commit(self, broker: str, offsets: Iterable[TopicPartition]) -> None:
        """Record commits, in memory only.

        Args:
            broker: The committing broker.
            offsets: Each partition's next offset to read.
        """
        for tp in offsets:
            key = (broker, tp.topic, tp.partition)
            self._committed[key] = max(self._committed.get(key, -1), tp.offset)

    def _is_committed(self, item: ReplayMessage) -> bool:
        """Whether a commit covers a message.

        Args:
            item: The message.

        Returns:
            ``True`` once its partition is committed past it.
        """
        key = (item.broker, item.recorded.topic, item.recorded.partition)
        return self._committed.get(key, -1) > item.recorded.offset

    def finished(self) -> bool:
        """Whether every message is released and committed.

        Returns:
            ``True`` when the replay is drained and acknowledged.
        """
        if self._runs or any(self._queues.values()):
            return False
        return all(
            self._committed.get(key, -1) > last for key, last in self._ends.items()
        )


class FileConsumer:
    """The part of ``confluent_kafka.Consumer`` the Kafka adapter calls.

    Attributes:
        commits: Every offset committed, in order.
        handed: Every message returned by ``consume``, in order.
        assigned: The partitions assigned.
        closed: Whether ``close`` was called.
    """

    def __init__(self, replay: Replay, broker: str) -> None:
        """Serve one broker's recorded partitions.

        Args:
            replay: The replay that releases its messages.
            broker: The broker.
        """
        self._replay = replay
        self._broker = broker
        self._on_assign: Callable[..., None] | None = None
        self._on_revoke: Callable[..., None] | None = None
        self._assigned_once = False
        self.commits: list[TopicPartition] = []
        self.handed: list[ReplayMessage] = []
        self.assigned: list[TopicPartition] = []
        self.closed = False

    def _all_partitions(self) -> list[TopicPartition]:
        """Every recorded partition, with no offset.

        Returns:
            The partitions.
        """
        return [
            TopicPartition(topic, partition, OFFSET_INVALID)
            for topic, partition in self._replay.partitions(self._broker)
        ]

    def subscribe(
        self,
        topics: list[str],
        on_assign: Callable[..., None] | None = None,
        on_revoke: Callable[..., None] | None = None,
    ) -> None:
        """Note the callbacks; every recorded partition is assigned on the
        first ``consume``, whatever the topics.

        Args:
            topics: Ignored; the recording decides the partitions.
            on_assign: Called once with every recorded partition.
            on_revoke: Called once, on close, with the same partitions.
        """
        self._on_assign = on_assign
        self._on_revoke = on_revoke

    def committed(
        self, partitions: list[TopicPartition], timeout: float | None = None
    ) -> list[TopicPartition]:
        """No partition has a committed position before the replay.

        Args:
            partitions: The partitions asked about.
            timeout: Ignored.

        Returns:
            Each partition with an invalid offset.
        """
        return [
            TopicPartition(tp.topic, tp.partition, OFFSET_INVALID) for tp in partitions
        ]

    def get_watermark_offsets(
        self,
        partition: TopicPartition,
        timeout: float | None = None,
        cached: bool = False,
    ) -> tuple[int, int]:
        """A recorded partition's first offset and the offset after its last.

        Args:
            partition: The partition.
            timeout: Ignored.
            cached: Ignored.

        Returns:
            Low and high watermarks.
        """
        return self._replay.watermarks(
            self._broker, partition.topic, partition.partition
        )

    def assign(self, partitions: list[TopicPartition]) -> None:
        """Take the assignment.

        Args:
            partitions: The partitions.
        """
        self.assigned = list(partitions)

    def consume(self, num_messages: int = 1, timeout: float = -1) -> list[Any]:
        """Assign once, then return released messages.

        Args:
            num_messages: Most messages to return.
            timeout: Longest wall wait when none is due, in seconds.

        Returns:
            The messages, in order.
        """
        if not self._assigned_once:
            self._assigned_once = True
            if self._on_assign is not None:
                self._on_assign(self, self._all_partitions())
            else:
                self.assign(self._all_partitions())
        out = self._replay.take(self._broker, num_messages, timeout)
        self.handed += out
        return out

    def commit(
        self,
        message: Any = None,
        offsets: list[TopicPartition] | None = None,
        asynchronous: bool = True,
    ) -> None:
        """Record a commit in memory.

        Args:
            message: Unused; the adapter commits offsets.
            offsets: Each partition's next offset to read.
            asynchronous: Ignored; the record is immediate.
        """
        self.commits.extend(offsets or [])
        self._replay.commit(self._broker, offsets or [])

    def close(self) -> None:
        """Revoke the assignment and stop."""
        if self._on_revoke is not None and self.assigned:
            self._on_revoke(self, list(self.assigned))
        self.closed = True
