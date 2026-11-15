"""Copy every enabled broker's retained messages in a time window to local files.

The capture never touches the live consumer groups. Each broker is read
under a capture group id of its own (Babamul's still starts with
``<username>-``, as Babamul requires), with automatic commits off. Every
partition of the broker's topics is assigned explicitly, never subscribed,
starting at the first offset at or after the window start, and read until a
message's timestamp reaches the window end or the partition's high
watermark, taken before reading starts, is reached. Nothing is committed.

Every message read in that range is written, with its topic, partition,
offset, timestamp, key and value unchanged, by
``seldon.replay.recording``. A partition with nothing in the window gets an
empty entry. The manifest and the log give, per broker, the message count,
bytes, elapsed time and sustained fetch rate in MB/s beside the rate the
broker needs at the intake target.

A recording is refused when the output directory already holds one, unless
``--replace`` is given. Nothing here logs or writes the client
configuration, which holds credentials, and Kafka errors are logged by name
only, since their text can name broker endpoints.
"""

import logging
import re
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, KafkaException, TopicPartition
from django.core.management.base import BaseCommand, CommandError, CommandParser

from seldon.config.brokers import BrokerSettings, load_broker_settings
from seldon.infrastructure.brokers.kafka import kafka_config
from seldon.replay.recording import (
    PartitionFile,
    RecordedMessage,
    RecordingWriter,
    read_manifest,
)

logger = logging.getLogger("seldon.capture")

#: Where recordings go by default, one directory per window.
DEFAULT_OUTPUT_ROOT = Path("/app/var/recordings")

#: Suffix of the capture group id; Babamul's is prefixed ``<username>-``.
CAPTURE_GROUP = "seldon-capture"

#: Babamul: 150 alerts a second at about 18 KB each.
BABAMUL_REQUIRED_MB_PER_S = 2.7
#: Fink: twice its 2026-11-14 burst of 1,824 alerts an hour, at 187 KB each
#: (the average measured over its 2026-11-13 and 2026-11-14 traffic).
FINK_REQUIRED_MB_PER_S = 0.19
#: Lasair: twice its 2026-11-14 burst of about 4,700 alerts an hour at 32 KB each.
LASAIR_REQUIRED_MB_PER_S = 0.084

REQUIRED_MB_PER_S = {
    "babamul": BABAMUL_REQUIRED_MB_PER_S,
    "fink": FINK_REQUIRED_MB_PER_S,
    "lasair": LASAIR_REQUIRED_MB_PER_S,
}

#: Longest wait for broker metadata, offsets and watermarks, in seconds.
METADATA_TIMEOUT_SECONDS = 30.0

#: Longest wait in one consume call, in seconds.
CONSUME_TIMEOUT_SECONDS = 1.0

#: Most messages taken in one consume call.
CONSUME_BATCH = 500


def capture_config(settings: BrokerSettings) -> dict[str, Any]:
    """The Kafka configuration for capturing a broker.

    It is the broker's own configuration with a capture group id in place
    of the live one and automatic commits off. It holds credentials: pass it
    to the client and never log it.

    Args:
        settings: The broker's settings.

    Returns:
        A ``confluent_kafka.Consumer`` configuration.
    """
    config = kafka_config(settings)
    group = CAPTURE_GROUP
    if settings.name == "babamul" and settings.username is not None:
        group = f"{settings.username.get_secret_value()}-{CAPTURE_GROUP}"
    config["group.id"] = group
    config["enable.auto.commit"] = False
    return config


def resolve_topics(names: list[str], entries: list[str]) -> list[str]:
    """The topic names a broker's configured topics stand for.

    Args:
        names: The topics the broker holds.
        entries: Configured topics; one starting with ``^`` is a pattern.

    Returns:
        Each named topic, and each held topic a pattern matches, sorted.
    """
    found = set()
    for entry in entries:
        if entry.startswith("^"):
            found |= {name for name in names if re.search(entry, name)}
        else:
            found.add(entry)
    return sorted(found)


def capture_broker(
    settings: BrokerSettings,
    start: datetime,
    end: datetime,
    writer: RecordingWriter,
    consumer_factory: Callable[[dict[str, Any]], Any] = Consumer,
) -> dict[str, Any]:
    """Record one broker's messages in the window.

    Args:
        settings: The broker's settings.
        start: The window's start, aware UTC.
        end: The window's end, aware UTC.
        writer: The recording to write into.
        consumer_factory: Builds a consumer from a configuration.

    Returns:
        The broker's manifest summary.

    Raises:
        CommandError: On a fatal Kafka error.
    """
    name = settings.name
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    consumer = consumer_factory(capture_config(settings))
    try:
        metadata = consumer.list_topics(timeout=METADATA_TIMEOUT_SECONDS).topics
        topics = resolve_topics(list(metadata), settings.topic_list)
        wanted = [
            TopicPartition(topic, partition, start_ms)
            for topic in topics
            for partition in sorted(metadata[topic].partitions)
        ]
        starts = consumer.offsets_for_times(wanted, timeout=METADATA_TIMEOUT_SECONDS)
        files: dict[tuple[str, int], PartitionFile] = {}
        high_watermarks: dict[tuple[str, int], int] = {}
        assignment = []
        for tp in starts:
            key = (tp.topic, tp.partition)
            files[key] = writer.partition(name, tp.topic, tp.partition)
            _low, high = consumer.get_watermark_offsets(
                TopicPartition(tp.topic, tp.partition),
                timeout=METADATA_TIMEOUT_SECONDS,
                cached=False,
            )
            if 0 <= tp.offset < high:
                high_watermarks[key] = high
                assignment.append(TopicPartition(tp.topic, tp.partition, tp.offset))
        logger.info(
            "%s: reading %d of %d partitions over %d topics",
            name,
            len(assignment),
            len(files),
            len(topics),
        )

        began = time.monotonic()
        if assignment:
            consumer.assign(assignment)

        def finish(key: tuple[str, int]) -> None:
            """Stop reading a partition."""
            del high_watermarks[key]
            consumer.pause([TopicPartition(*key)])

        while high_watermarks:
            messages = consumer.consume(
                num_messages=CONSUME_BATCH, timeout=CONSUME_TIMEOUT_SECONDS
            )
            if not messages:
                # The last offsets below a watermark may never be delivered
                # (transaction markers, compaction), so check positions.
                positions = consumer.position(
                    [TopicPartition(*key) for key in high_watermarks]
                )
                for tp in positions:
                    key = (tp.topic, tp.partition)
                    if tp.offset >= high_watermarks[key]:
                        finish(key)
                continue
            for message in messages:
                error = message.error()
                if error is not None:
                    if error.fatal():
                        raise CommandError(f"{name}: fatal Kafka error {error.name()}")
                    logger.warning("%s: Kafka error %s", name, error.name())
                    continue
                key = (message.topic(), message.partition())
                if key not in high_watermarks:
                    continue
                timestamp_type, timestamp_ms = message.timestamp()
                if timestamp_ms >= end_ms:
                    finish(key)
                    continue
                files[key].write(
                    RecordedMessage(
                        topic=message.topic(),
                        partition=message.partition(),
                        offset=message.offset(),
                        timestamp_type=timestamp_type,
                        timestamp_ms=timestamp_ms,
                        key=message.key(),
                        value=message.value(),
                    )
                )
                if message.offset() + 1 >= high_watermarks[key]:
                    finish(key)
        elapsed = time.monotonic() - began
    except KafkaException as error:
        raise CommandError(
            f"{name}: Kafka error {error.args[0].name() if error.args else ''}"
        ) from None
    finally:
        consumer.close()

    required = REQUIRED_MB_PER_S.get(name)
    summary = writer.broker_done(name, list(files.values()), elapsed, required)
    level = logging.INFO
    if required is not None and summary["mb_per_s"] < required:
        level = logging.WARNING
    logger.log(
        level,
        "%s: %d messages, %d bytes in %.1f s: %.3f MB/s (required %s MB/s)",
        name,
        summary["messages"],
        summary["bytes"],
        elapsed,
        summary["mb_per_s"],
        "unknown" if required is None else f"{required:g}",
    )
    return summary


def parse_utc(text: str) -> datetime:
    """An ISO 8601 time as aware UTC; a time without an offset is UTC.

    Args:
        text: The time.

    Returns:
        The moment, in UTC.
    """
    moment = datetime.fromisoformat(text)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC)


def window_label(start: datetime, end: datetime) -> str:
    """A directory name for a window.

    Args:
        start: The window's start, aware UTC.
        end: The window's end, aware UTC.

    Returns:
        ``<start>-<end>`` in compact ISO 8601 basic form.
    """
    return f"{start:%Y%m%dT%H%M%SZ}-{end:%Y%m%dT%H%M%SZ}"


class Command(BaseCommand):
    """Capture retained broker traffic in a window to a local recording."""

    help = __doc__.splitlines()[0]

    def add_arguments(self, parser: CommandParser) -> None:
        """Add the window, broker, output and replace options.

        Args:
            parser: The command's argument parser.
        """
        parser.add_argument(
            "--from",
            dest="start",
            type=parse_utc,
            required=True,
            help="Window start, ISO 8601 (UTC when no offset is given).",
        )
        parser.add_argument(
            "--to",
            dest="end",
            type=parse_utc,
            required=True,
            help="Window end, ISO 8601 (UTC when no offset is given).",
        )
        parser.add_argument(
            "--brokers",
            nargs="+",
            choices=sorted(REQUIRED_MB_PER_S),
            help="Brokers to capture; every enabled broker when omitted.",
        )
        parser.add_argument(
            "--output",
            type=Path,
            help=f"Recording directory; {DEFAULT_OUTPUT_ROOT}/<window> by default.",
        )
        parser.add_argument(
            "--replace",
            action="store_true",
            help="Overwrite a recording already in the output directory.",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        """Capture each chosen broker in turn, then write the manifest.

        Args:
            *args: Unused.
            **options: The parsed options.

        Raises:
            CommandError: If a chosen broker is not enabled, the output
                already holds a recording and ``--replace`` is not given, or
                Kafka fails fatally.
        """
        start, end = options["start"], options["end"]
        enabled = {s.name: s for s in load_broker_settings() if s.enabled}
        names = options["brokers"] or list(enabled)
        missing = [name for name in names if name not in enabled]
        if missing:
            raise CommandError(f"Not enabled: {', '.join(missing)}")
        output = options["output"] or DEFAULT_OUTPUT_ROOT / window_label(start, end)

        existing = read_manifest(output)
        if existing is not None and not options["replace"]:
            window = existing["window"]
            raise CommandError(
                f"{output} already holds a recording of {window['from']} to "
                f"{window['to']}; pass --replace to overwrite it"
            )

        writer = RecordingWriter(output, start, end)
        logger.info("Capturing %s to %s into %s", start, end, output)
        for name in names:
            capture_broker(enabled[name], start, end, writer, Consumer)
        writer.finish()
        logger.info("Recording written to %s", output)
