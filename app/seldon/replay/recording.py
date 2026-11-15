"""Write and read local recordings of broker traffic.

A recording is a directory holding one file per broker, topic and partition
and a ``manifest.json``. Each partition file is a gzip stream of records in
offset order. A record is a fixed big-endian header (offset, timestamp
type, timestamp in ms, key length, value length; a length of -1 means the
key or value was absent) followed by the raw key and value bytes, so every
message reads back exactly as the broker served it. The partition's topic
and number are kept in the manifest, not parsed from the file name.

The manifest records the capture window and, per broker, the message count,
key and value bytes, elapsed seconds, measured MB/s, the MB/s required at
the target, and every partition entry. It is written last, so a directory
without one holds no complete recording.

Nothing here sees broker credentials: file names come from broker names,
topic names and partition numbers only.
"""

import gzip
import json
import os
import struct
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import IO, Any

#: The manifest's file name inside a recording.
MANIFEST_NAME = "manifest.json"

# offset, timestamp type, timestamp ms, key length, value length.
_HEADER = struct.Struct(">qbqii")
_ABSENT = -1


@dataclass(frozen=True)
class RecordedMessage:
    """One broker message as captured.

    Attributes:
        topic: Its topic.
        partition: Its partition.
        offset: Its offset.
        timestamp_type: The Kafka timestamp type the broker reported.
        timestamp_ms: Its timestamp in ms since the epoch, as reported.
        key: Its key bytes, or ``None`` when it had none.
        value: Its value bytes, or ``None`` when it had none.
    """

    topic: str
    partition: int
    offset: int
    timestamp_type: int
    timestamp_ms: int
    key: bytes | None
    value: bytes | None


class PartitionFile:
    """One topic partition's records, being written."""

    def __init__(self, root: Path, broker: str, topic: str, partition: int) -> None:
        """Create the partition's file, empty.

        Args:
            root: The recording directory.
            broker: The broker name.
            topic: The topic.
            partition: The partition.
        """
        self.topic = topic
        self.partition = partition
        self.relative = f"{broker}/{topic}-{partition}.rec.gz"
        self.messages = 0
        self.bytes = 0
        path = root / self.relative
        path.parent.mkdir(parents=True, exist_ok=True)
        self._file: IO[bytes] = gzip.open(path, "wb")

    def write(self, message: RecordedMessage) -> None:
        """Append one message.

        Args:
            message: The message; its topic and partition are this file's.
        """
        key, value = message.key, message.value
        self._file.write(
            _HEADER.pack(
                message.offset,
                message.timestamp_type,
                message.timestamp_ms,
                _ABSENT if key is None else len(key),
                _ABSENT if value is None else len(value),
            )
        )
        if key is not None:
            self._file.write(key)
        if value is not None:
            self._file.write(value)
        self.messages += 1
        self.bytes += len(key or b"") + len(value or b"")

    def close(self) -> None:
        """Finish the gzip stream."""
        self._file.close()

    def entry(self) -> dict[str, Any]:
        """The partition's manifest entry.

        Returns:
            Topic, partition, file path relative to the recording, message
            count and bytes.
        """
        return {
            "topic": self.topic,
            "partition": self.partition,
            "file": self.relative,
            "messages": self.messages,
            "bytes": self.bytes,
        }


class RecordingWriter:
    """A recording of one capture window, being written."""

    def __init__(self, root: Path, start: datetime, end: datetime) -> None:
        """Start a recording, removing any earlier manifest in ``root``.

        The caller decides whether an earlier recording may be replaced;
        removing its manifest first means a capture that fails part-way
        leaves no manifest describing half-rewritten files.

        Args:
            root: The recording directory, created if absent.
            start: The window's start, aware UTC.
            end: The window's end, aware UTC.
        """
        self.root = root
        self._window = {"from": start.isoformat(), "to": end.isoformat()}
        self._brokers: dict[str, dict[str, Any]] = {}
        root.mkdir(parents=True, exist_ok=True)
        (root / MANIFEST_NAME).unlink(missing_ok=True)

    def partition(self, broker: str, topic: str, partition: int) -> PartitionFile:
        """Open a partition's file.

        Args:
            broker: The broker name.
            topic: The topic.
            partition: The partition.

        Returns:
            The partition file, to write and then pass to ``broker_done``.
        """
        return PartitionFile(self.root, broker, topic, partition)

    def broker_done(
        self,
        broker: str,
        files: list[PartitionFile],
        elapsed_seconds: float,
        required_mb_per_s: float | None,
    ) -> dict[str, Any]:
        """Close a broker's partition files and record its summary.

        Args:
            broker: The broker name.
            files: Every partition file written for it, empty ones included.
            elapsed_seconds: Time spent fetching its messages.
            required_mb_per_s: The rate it needs at the target, if known.

        Returns:
            The broker's manifest summary.
        """
        for file in files:
            file.close()
        total = sum(file.bytes for file in files)
        summary = {
            "messages": sum(file.messages for file in files),
            "bytes": total,
            "elapsed_seconds": elapsed_seconds,
            "mb_per_s": total / 1e6 / elapsed_seconds if elapsed_seconds else 0.0,
            "required_mb_per_s": required_mb_per_s,
            "partitions": [file.entry() for file in files],
        }
        self._brokers[broker] = summary
        return summary

    def finish(self) -> None:
        """Write the manifest, completing the recording."""
        manifest = {"window": self._window, "brokers": self._brokers}
        partial = self.root / f"{MANIFEST_NAME}.partial"
        partial.write_text(json.dumps(manifest, indent=2))
        os.replace(partial, self.root / MANIFEST_NAME)


def read_manifest(root: Path) -> dict[str, Any] | None:
    """A recording's manifest.

    Args:
        root: The recording directory.

    Returns:
        The manifest, or ``None`` when the directory holds no recording.
    """
    path = Path(root) / MANIFEST_NAME
    if not path.exists():
        return None
    return json.loads(path.read_text())


def _read_exact(file: IO[bytes], length: int) -> bytes | None:
    """Read a key or value of a recorded length.

    Args:
        file: The open partition file.
        length: The recorded length; -1 when absent.

    Returns:
        The bytes, or ``None`` when absent.

    Raises:
        EOFError: If the file ends before ``length`` bytes.
    """
    if length == _ABSENT:
        return None
    data = file.read(length)
    if len(data) != length:
        raise EOFError("recording ends inside a record")
    return data


def read_partition(root: Path, entry: dict[str, Any]) -> Iterator[RecordedMessage]:
    """One partition's messages, read from its file one at a time.

    Only the message being yielded is held in memory, so a recording of any
    size can be read through.

    Args:
        root: The recording directory.
        entry: The partition's manifest entry.

    Yields:
        The partition's messages in offset order.

    Raises:
        EOFError: If the partition file is truncated.
    """
    with gzip.open(Path(root) / entry["file"], "rb") as file:
        while header := file.read(_HEADER.size):
            if len(header) != _HEADER.size:
                raise EOFError("recording ends inside a record header")
            offset, ts_type, ts_ms, key_len, value_len = _HEADER.unpack(header)
            yield RecordedMessage(
                topic=entry["topic"],
                partition=entry["partition"],
                offset=offset,
                timestamp_type=ts_type,
                timestamp_ms=ts_ms,
                key=_read_exact(file, key_len),
                value=_read_exact(file, value_len),
            )


def read_messages(root: Path, broker: str) -> Iterator[RecordedMessage]:
    """Every message recorded for a broker.

    Args:
        root: The recording directory.
        broker: The broker name.

    Yields:
        Each partition's messages in offset order, partition after partition
        in manifest order.

    Raises:
        FileNotFoundError: If the directory holds no recording.
        KeyError: If the recording holds nothing for ``broker``.
        EOFError: If a partition file is truncated.
    """
    root = Path(root)
    manifest = read_manifest(root)
    if manifest is None:
        raise FileNotFoundError(f"no {MANIFEST_NAME} in {root}")
    for entry in manifest["brokers"][broker]["partitions"]:
        yield from read_partition(root, entry)
