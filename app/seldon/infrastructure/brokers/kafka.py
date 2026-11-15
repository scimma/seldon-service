"""A broker adapter over Kafka, for Fink, Lasair, and Babamul.

Offsets are committed only when the caller acknowledges a batch of alerts
(``enable.auto.commit`` is off), so an alert is never lost between receipt
and storage. A batch is committed with one synchronous commit per partition,
past the last whole message acknowledged on it, and each commit carries that
message's publish time as its metadata. On assignment, a partition whose committed offset is older
than its first retained offset has lost messages: reading resumes at the
first retained offset, and a confirmed gap is reported from the last
acknowledged message's publish time to the first retained message's. A
partition with no committed offset (the service never read it) starts at
the newest message, and its earlier history is not a gap.

When several consumer processes share a group, a rebalance can revoke a
partition between storing a batch and committing it. The broker then
refuses the commit; that refusal is logged as a rebalance and the rest of
the batch's partitions are still committed. The refused messages are
redelivered to the partition's new reader, which finds their work already
stored.

Nothing here logs the client configuration, which holds credentials.
"""

import logging
from collections.abc import Callable, Sequence
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
from seldon.intake.timing import Stage, StageTimer

logger = logging.getLogger("seldon.brokers.kafka")

#: Seconds to wait for the group coordinator on assignment.
COORDINATOR_TIMEOUT_SECONDS = 10.0

#: Commit errors that mean a rebalance took the partition from this
#: consumer: the commit is refused and the messages go to the new reader.
REBALANCE_ERRORS = frozenset(
    {
        KafkaError.ILLEGAL_GENERATION,
        KafkaError.UNKNOWN_MEMBER_ID,
        KafkaError.REBALANCE_IN_PROGRESS,
        KafkaError._ASSIGNMENT_LOST,
    }
)


def _rebalance_error(error: KafkaException) -> KafkaError | None:
    """The rebalance error a refused commit carries, if it is one.

    Args:
        error: What the commit raised.

    Returns:
        The Kafka error when it means the partition was revoked; ``None``
        for any other failure.
    """
    if not error.args or not isinstance(error.args[0], KafkaError):
        return None
    kafka_error = error.args[0]
    return kafka_error if kafka_error.code() in REBALANCE_ERRORS else None


@dataclass(frozen=True)
class KafkaReceipt:
    """Where a polled alert sits in its stream, for acknowledging it.

    Attributes:
        topic: The message's topic.
        partition: Its partition.
        offset: Its offset.
        timestamp_ms: Its broker timestamp in ms since the epoch, if any.
        last_in_message: Whether this is the message's last alert; only
            acknowledging the last commits the message's offset.
    """

    topic: str
    partition: int
    offset: int
    timestamp_ms: int | None
    last_in_message: bool = True

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
        timer: StageTimer | None = None,
    ) -> None:
        """Subscribe to the broker's topics.

        Args:
            settings: The broker's settings.
            consumer: A ``confluent_kafka.Consumer`` or a stand-in; one is
                built from ``settings`` when omitted.
            clock: Returns the current aware UTC time; the system clock when
                omitted.
            timer: Times the consume, decode, parse and commit calls; a
                timer of the adapter's own when omitted.
        """
        self.name = settings.name
        self.role = settings.role
        self._parser = parser_for(settings.name)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._timer = timer or StageTimer()
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

    def _skip(
        self,
        reason: str,
        at: datetime,
        receipt: KafkaReceipt,
        published_at: datetime | None,
    ) -> Polled:
        """An intake skip for a message that could not be read.

        Args:
            reason: Why.
            at: When.
            receipt: The message's place.
            published_at: The message's publish time, if it has one.

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
                published_at=published_at,
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
        with self._timer.stage(Stage.POLL_WAIT):
            messages = self._consumer.consume(
                num_messages=max_messages, timeout=timeout
            )
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
                with self._timer.stage(Stage.DECODE):
                    alerts = self._parser.decode(message.key(), message.value())
            except Exception as error:  # any undecodable payload is a skip
                polled.append(
                    self._skip(
                        f"undecodable message: {type(error).__name__}",
                        received_at,
                        receipt,
                        published_at,
                    )
                )
                continue
            if not alerts:
                polled.append(
                    self._skip("empty message", received_at, receipt, published_at)
                )
                continue
            context = ParseContext(
                broker=self.name,
                role=self.role,
                topic=receipt.topic,
                published_at=published_at,
                received_at=received_at,
            )
            for index, alert in enumerate(alerts):
                alert_receipt = replace(
                    receipt, last_in_message=index == len(alerts) - 1
                )
                with self._timer.stage(Stage.PARSE):
                    outcome = parse_alert(self.name, alert, context)
                if isinstance(outcome, IntakeSkipReport):
                    logger.info(
                        "%s: skipping %s: %s",
                        self.name,
                        outcome.raw_identifier or receipt.place(),
                        outcome.reason,
                    )
                    outcome = replace(
                        outcome,
                        raw_identifier=outcome.raw_identifier or receipt.place(),
                        published_at=published_at,
                    )
                polled.append(Polled(outcome=outcome, receipt=alert_receipt))
        return polled

    def ack(self, batch: Sequence[Polled]) -> None:
        """Commit each partition past the batch's last whole message on it.

        One synchronous commit per partition, at the offset after the last
        message whose final alert is in the batch. A message can carry
        several alerts; its offset is committed only once its last alert is
        acknowledged, so when a batch ends partway through a message, that
        partition is committed only through the message before it, and a
        crash before the rest is stored redelivers all of the message's
        alerts. Each commit's metadata records its message's publish time,
        so a later gap can say where reading stopped.

        A commit refused because a rebalance revoked the partition is logged
        and skipped; the other partitions are still committed, and the
        refused messages are left to be redelivered.

        Args:
            batch: Consecutive alerts this adapter returned from ``poll``, in
                poll order.

        Raises:
            confluent_kafka.KafkaException: If a commit fails for any reason
                other than a rebalance.
        """
        last: dict[tuple[str, int], KafkaReceipt] = {}
        for polled in batch:
            receipt: KafkaReceipt = polled.receipt
            if receipt.last_in_message:
                last[(receipt.topic, receipt.partition)] = receipt
        for receipt in last.values():
            metadata = "" if receipt.timestamp_ms is None else str(receipt.timestamp_ms)
            try:
                with self._timer.stage(Stage.COMMIT):
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
            except KafkaException as error:
                rebalance = _rebalance_error(error)
                if rebalance is None:
                    raise
                logger.warning(
                    "%s: rebalance refused the commit of %s (%s); leaving it "
                    "to be redelivered",
                    self.name,
                    receipt.place(),
                    rebalance.name(),
                )
                continue
            self._timer.count_commit()

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
