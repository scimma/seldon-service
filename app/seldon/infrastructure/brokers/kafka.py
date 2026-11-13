"""A broker adapter over Kafka, for Fink, Lasair, and Babamul.

Offsets are committed only when the caller acknowledges an alert
(``enable.auto.commit`` is off), so an alert is never lost between receipt
and storage. Each commit carries the acknowledged message's publish time as
its metadata. On assignment, a partition whose committed offset is older
than its first retained offset has lost messages: reading resumes at the
first retained offset, and a confirmed gap is reported from the last
acknowledged message's publish time to the first retained message's. A
partition with no committed offset (the service never read it) starts at
the newest message, and its earlier history is not a gap.

Nothing here logs the client configuration, which holds credentials.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import (
    TIMESTAMP_NOT_AVAILABLE,
    Consumer,
    KafkaError,
    KafkaException,
    TopicPartition,
)

from seldon.config.brokers import BrokerSettings
from seldon.infrastructure.brokers.base import (
    MAX_REASON_LENGTH,
    BrokerAdapter,
    GapKind,
    IntakeSkipReport,
    ParseContext,
    Polled,
    StreamGapReport,
)
from seldon.infrastructure.brokers.parsers import parse_alert, parser_for

logger = logging.getLogger("seldon.brokers.kafka")

#: Seconds to wait for the group coordinator on assignment.
COORDINATOR_TIMEOUT_SECONDS = 10.0


@dataclass(frozen=True)
class KafkaReceipt:
    """Where a polled alert sits in its stream, for acknowledging it.

    Attributes:
        topic: The message's topic.
        partition: Its partition.
        offset: Its offset.
        timestamp_ms: Its broker timestamp in ms since the epoch, if any.
    """

    topic: str
    partition: int
    offset: int
    timestamp_ms: int | None

    def place(self) -> str:
        """The message's place in the stream, as text.

        Returns:
            ``topic[partition]@offset``.
        """
        return f"{self.topic}[{self.partition}]@{self.offset}"


def kafka_config(settings: BrokerSettings) -> dict[str, Any]:
    """The Kafka consumer configuration for a broker.

    The result holds credentials in plain text: pass it to the client and
    never log it.

    Args:
        settings: The broker's settings.

    Returns:
        A ``confluent_kafka.Consumer`` configuration.

    Raises:
        ValueError: If the bootstrap servers or group id are unset.
    """
    if settings.bootstrap_servers is None or settings.group_id is None:
        raise ValueError(
            f"{settings.name}: bootstrap_servers and group_id are required"
        )
    config: dict[str, Any] = {
        "bootstrap.servers": settings.bootstrap_servers.get_secret_value(),
        "group.id": settings.group_id.get_secret_value(),
        "enable.auto.commit": False,
        "auto.offset.reset": "latest",
        "security.protocol": settings.security_protocol,
    }
    if settings.security_protocol.upper().startswith("SASL"):
        config["sasl.mechanism"] = settings.sasl_mechanism
        if settings.username is not None:
            config["sasl.username"] = settings.username.get_secret_value()
        if settings.password is not None:
            config["sasl.password"] = settings.password.get_secret_value()
    if settings.max_partition_fetch_bytes is not None:
        config["max.partition.fetch.bytes"] = settings.max_partition_fetch_bytes
    return config


def _utc(timestamp_ms: int) -> datetime:
    """An aware UTC datetime from a Kafka timestamp.

    Args:
        timestamp_ms: Milliseconds since the epoch.

    Returns:
        The moment, in UTC.
    """
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)


def _committed_time(metadata: str | None) -> datetime | None:
    """The publish time a commit recorded in its metadata.

    Args:
        metadata: The commit metadata.

    Returns:
        The last acknowledged message's publish time, or ``None`` when the
        commit did not record one.
    """
    try:
        return _utc(int(metadata))
    except (TypeError, ValueError):
        return None


class KafkaBrokerAdapter(BrokerAdapter):
    """One broker's Kafka topics, as deliveries, skips, and gaps."""

    def __init__(
        self,
        settings: BrokerSettings,
        consumer: Any | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        """Subscribe to the broker's topics.

        Args:
            settings: The broker's settings.
            consumer: A ``confluent_kafka.Consumer`` or a stand-in; one is
                built from ``settings`` when omitted.
            clock: Returns the current aware UTC time; the system clock when
                omitted.
        """
        self.name = settings.name
        self.role = settings.role
        self._parser = parser_for(settings.name)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._consumer = consumer or Consumer(kafka_config(settings))
        # Partitions that lost messages, with the last acknowledged publish
        # time, until their first retained message arrives.
        self._pending: dict[tuple[str, int], datetime | None] = {}
        self._gaps: list[StreamGapReport] = []
        topics = settings.topic_list
        self._consumer.subscribe(
            topics, on_assign=self._on_assign, on_revoke=self._on_revoke
        )
        logger.info("%s: subscribed to %s", self.name, ", ".join(topics))

    def _on_assign(self, consumer: Any, partitions: list[TopicPartition]) -> None:
        """Resume each partition, skipping past and recording lost messages.

        Args:
            consumer: The consumer being assigned.
            partitions: The partitions assigned to it.
        """
        committed = {
            (tp.topic, tp.partition): tp
            for tp in consumer.committed(
                partitions, timeout=COORDINATOR_TIMEOUT_SECONDS
            )
        }
        for tp in partitions:
            position = committed.get((tp.topic, tp.partition))
            if position is None or position.offset < 0:
                continue
            low, _high = consumer.get_watermark_offsets(
                tp, timeout=COORDINATOR_TIMEOUT_SECONDS, cached=False
            )
            if position.offset < low:
                logger.warning(
                    "%s: %s[%d] lost messages %d to %d; resuming at %d",
                    self.name,
                    tp.topic,
                    tp.partition,
                    position.offset,
                    low - 1,
                    low,
                )
                tp.offset = low
                self._pending[(tp.topic, tp.partition)] = _committed_time(
                    position.metadata
                )
        consumer.assign(partitions)
        logger.info("%s: assigned %d partitions", self.name, len(partitions))

    def _on_revoke(self, consumer: Any, partitions: list[TopicPartition]) -> None:
        """Forget unfinished gaps on partitions this consumer no longer reads.

        Args:
            consumer: The consumer losing the partitions.
            partitions: The partitions revoked.
        """
        for tp in partitions:
            self._pending.pop((tp.topic, tp.partition), None)

    def _close_gap(self, topic: str, partition: int, ended_at: datetime) -> None:
        """Report a partition's gap once its first retained message arrives.

        Args:
            topic: The message's topic.
            partition: Its partition.
            ended_at: Its publish time.
        """
        key = (topic, partition)
        if key not in self._pending:
            return
        started_at = self._pending.pop(key)
        if started_at is None:
            logger.warning(
                "%s: %s[%d] gap start unknown (no time on the commit)",
                self.name,
                topic,
                partition,
            )
            started_at = ended_at
        self._gaps.append(
            StreamGapReport(
                broker=self.name,
                started_at=started_at,
                ended_at=ended_at,
                kind=GapKind.CONFIRMED,
            )
        )

    def _skip(self, reason: str, at: datetime, receipt: KafkaReceipt) -> Polled:
        """An intake skip for a message that could not be read.

        Args:
            reason: Why.
            at: When.
            receipt: The message's place.

        Returns:
            The skip, ready to acknowledge.
        """
        logger.warning("%s: skipping %s: %s", self.name, receipt.place(), reason)
        return Polled(
            outcome=IntakeSkipReport(
                broker=self.name,
                reason=reason[:MAX_REASON_LENGTH],
                occurred_at=at,
                raw_identifier=receipt.place(),
            ),
            receipt=receipt,
        )

    def poll(self, timeout: float, max_messages: int = 100) -> list[Polled]:
        """Wait for messages and parse each into a delivery or a skip.

        Args:
            timeout: Longest wait, in seconds.
            max_messages: Most messages to take in one call.

        Returns:
            The polled alerts in stream order; empty when none arrived.

        Raises:
            confluent_kafka.KafkaException: On a fatal client error.
        """
        messages = self._consumer.consume(num_messages=max_messages, timeout=timeout)
        received_at = self._clock()
        polled: list[Polled] = []
        for message in messages:
            error = message.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    continue
                if error.fatal():
                    raise KafkaException(error)
                logger.warning("%s: Kafka error: %s", self.name, error.str())
                continue
            timestamp_type, timestamp_ms = message.timestamp()
            if timestamp_type == TIMESTAMP_NOT_AVAILABLE:
                timestamp_ms = None
            published_at = None if timestamp_ms is None else _utc(timestamp_ms)
            receipt = KafkaReceipt(
                message.topic(), message.partition(), message.offset(), timestamp_ms
            )
            self._close_gap(
                receipt.topic, receipt.partition, published_at or received_at
            )
            try:
                alerts = self._parser.decode(message.key(), message.value())
            except Exception as error:  # any undecodable payload is a skip
                polled.append(
                    self._skip(
                        f"undecodable message: {type(error).__name__}",
                        received_at,
                        receipt,
                    )
                )
                continue
            if not alerts:
                polled.append(self._skip("empty message", received_at, receipt))
                continue
            context = ParseContext(
                broker=self.name,
                role=self.role,
                topic=receipt.topic,
                published_at=published_at,
                received_at=received_at,
            )
            for alert in alerts:
                outcome = parse_alert(self.name, alert, context)
                if isinstance(outcome, IntakeSkipReport):
                    logger.info(
                        "%s: skipping %s: %s",
                        self.name,
                        outcome.raw_identifier or receipt.place(),
                        outcome.reason,
                    )
                    if not outcome.raw_identifier:
                        outcome = replace(outcome, raw_identifier=receipt.place())
                polled.append(Polled(outcome=outcome, receipt=receipt))
        return polled

    def ack(self, polled: Polled) -> None:
        """Commit the offset after the alert's message, synchronously.

        The commit's metadata records the message's publish time, so a later
        gap can say where reading stopped.

        Args:
            polled: An alert this adapter returned from ``poll``.
        """
        receipt: KafkaReceipt = polled.receipt
        metadata = "" if receipt.timestamp_ms is None else str(receipt.timestamp_ms)
        self._consumer.commit(
            offsets=[
                TopicPartition(
                    receipt.topic,
                    receipt.partition,
                    receipt.offset + 1,
                    metadata=metadata,
                )
            ],
            asynchronous=False,
        )

    def gaps(self) -> list[StreamGapReport]:
        """Gaps found since the last call.

        Returns:
            Each confirmed gap once, in the order found.
        """
        found, self._gaps = self._gaps, []
        return found

    def close(self) -> None:
        """Leave the consumer group and close the connection."""
        self._consumer.close()
        logger.info("%s: closed", self.name)
