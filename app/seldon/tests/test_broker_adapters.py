"""Tests for broker settings and the Kafka broker adapter.

The adapter runs against an in-memory fake of the Kafka consumer, so no
network is used. Messages are recorded Lasair alerts (plain JSON) from
``seldon.tests.fixtures.broker_alerts``; expected values come from those
recordings and from the fake's own offsets and timestamps.
"""

import json
import logging
import os
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any
from unittest import mock

from confluent_kafka import OFFSET_INVALID, TIMESTAMP_CREATE_TIME, TopicPartition
from django.test import SimpleTestCase
from pydantic import ValidationError

from seldon.config.brokers import (
    BabamulSettings,
    FinkSettings,
    LasairSettings,
    load_broker_settings,
)
from seldon.domain.intake.delivery import BrokerRole, Delivery
from seldon.domain.intake.qualification import Thresholds, qualifies
from seldon.infrastructure.brokers.base import (
    GapKind,
    IntakeSkipReport,
    StreamGapReport,
)
from seldon.infrastructure.brokers.kafka import KafkaBrokerAdapter, kafka_config
from seldon.tests.fixtures.broker_alerts import lasair

RECEIVED = datetime(2026, 11, 13, 1, 0, tzinfo=timezone.utc)
TOPIC = "lasair_366c0efe0df"

# Distinctive stand-ins for credentials; none of them may surface in text.
SECRET_USER = "sentinel-user-7f3a"
SECRET_PASSWORD = "sentinel-password-91c2"
SECRET_GROUP = f"{SECRET_USER}-sentinel-group"
SECRET_SERVERS = "sentinel-servers.invalid:9093"


def _ms(moment: datetime) -> int:
    """Milliseconds since the epoch.

    Args:
        moment: An aware datetime.

    Returns:
        Its Kafka timestamp.
    """
    return int(moment.timestamp() * 1000)


class FakeMessage:
    """A Kafka message as ``confluent_kafka.Message`` presents it."""

    def __init__(
        self, partition: int, offset: int, value: bytes, timestamp_ms: int
    ) -> None:
        """Hold one message.

        Args:
            partition: Its partition.
            offset: Its offset.
            value: Its payload.
            timestamp_ms: Its create time, in ms since the epoch.
        """
        self._partition = partition
        self._offset = offset
        self._value = value
        self._timestamp_ms = timestamp_ms

    def error(self) -> None:
        """No message here is an error."""
        return None

    def topic(self) -> str:
        """The topic."""
        return TOPIC

    def partition(self) -> int:
        """The partition."""
        return self._partition

    def offset(self) -> int:
        """The offset."""
        return self._offset

    def key(self) -> None:
        """Lasair messages carry no key."""
        return None

    def value(self) -> bytes:
        """The payload."""
        return self._value

    def timestamp(self) -> tuple[int, int]:
        """The create time."""
        return TIMESTAMP_CREATE_TIME, self._timestamp_ms


class FakeConsumer:
    """The part of ``confluent_kafka.Consumer`` the adapter uses.

    One partition of ``TOPIC``. On the first ``consume`` it runs the
    assignment callback, then returns messages starting from the assigned
    offset (or the committed one, or the first retained one).
    """

    def __init__(
        self,
        retained: list[FakeMessage],
        committed: TopicPartition | None = None,
    ) -> None:
        """Set up the partition.

        Args:
            retained: The messages still on the broker, in offset order.
            committed: The group's committed position, if any.
        """
        self.retained = retained
        self._committed = committed
        self.commits: list[TopicPartition] = []
        self.assigned: list[TopicPartition] = []
        self.subscribed: list[str] = []
        self._on_assign: Callable[..., None] | None = None
        self._position: int | None = None
        self.closed = False

    def subscribe(
        self,
        topics: list[str],
        on_assign: Callable[..., None] | None = None,
        on_revoke: Callable[..., None] | None = None,
    ) -> None:
        """Record the subscription and its callbacks."""
        self.subscribed = list(topics)
        self._on_assign = on_assign

    def committed(
        self, partitions: list[TopicPartition], timeout: float | None = None
    ) -> list[TopicPartition]:
        """The committed position of each partition."""
        if self._committed is None:
            return [
                TopicPartition(p.topic, p.partition, OFFSET_INVALID) for p in partitions
            ]
        return [self._committed]

    def get_watermark_offsets(
        self,
        partition: TopicPartition,
        timeout: float | None = None,
        cached: bool = False,
    ) -> tuple[int, int]:
        """The first retained offset and the next offset to be written."""
        low = self.retained[0].offset() if self.retained else 0
        high = self.retained[-1].offset() + 1 if self.retained else 0
        return low, high

    def assign(self, partitions: list[TopicPartition]) -> None:
        """Take the assignment, honouring an explicit offset."""
        self.assigned = list(partitions)
        offset = partitions[0].offset
        if offset >= 0:
            self._position = offset
        elif self._committed is not None:
            self._position = self._committed.offset
        else:
            self._position = self.retained[0].offset() if self.retained else 0

    def consume(self, num_messages: int = 1, timeout: float = -1) -> list[FakeMessage]:
        """Run the assignment once, then hand out messages in order."""
        if self._on_assign is not None and self._position is None:
            self._on_assign(self, [TopicPartition(TOPIC, 0, OFFSET_INVALID)])
        if self._position is None:
            return []
        out = [m for m in self.retained if m.offset() >= self._position][:num_messages]
        if out:
            self._position = out[-1].offset() + 1
        return out

    def commit(
        self,
        message: Any = None,
        offsets: list[TopicPartition] | None = None,
        asynchronous: bool = True,
    ) -> None:
        """Record a commit."""
        self.commits.extend(offsets or [])

    def close(self) -> None:
        """Record the close."""
        self.closed = True


def _lasair_messages(first_offset: int, first_ms: int) -> list[FakeMessage]:
    """The recorded Lasair alerts as consecutive messages on partition 0.

    Args:
        first_offset: Offset of the first message.
        first_ms: Timestamp of the first; each next one is a second later.

    Returns:
        The messages.
    """
    return [
        FakeMessage(
            0,
            first_offset + i,
            json.dumps(recorded["alert"]).encode(),
            first_ms + 1000 * i,
        )
        for i, recorded in enumerate(lasair.ALERTS)
    ]


def _lasair_settings(**overrides: Any) -> LasairSettings:
    """Enabled Lasair settings from a controlled environment.

    Args:
        **overrides: Field values to set.

    Returns:
        The settings.
    """
    with mock.patch.dict(os.environ, {}, clear=True):
        return LasairSettings(enabled=True, **overrides)


def _adapter(consumer: FakeConsumer, settings: Any = None) -> KafkaBrokerAdapter:
    """A Lasair adapter over a fake consumer with a fixed clock.

    Args:
        consumer: The fake consumer.
        settings: Broker settings; enabled Lasair defaults when omitted.

    Returns:
        The adapter.
    """
    return KafkaBrokerAdapter(
        settings or _lasair_settings(),
        consumer=consumer,
        clock=lambda: RECEIVED,
    )


class BrokerSettingsTests(SimpleTestCase):
    """Per-broker roles, thresholds, and credentials from the environment."""

    def test_defaults_match_the_chosen_brokers(self) -> None:
        """With nothing set, Fink and Lasair are qualifying with reliability
        cuts of 0.6 and SN-like thresholds of 0.5 and 1.0; Babamul is the full
        stream over every babamul.lsst topic; none is enabled."""
        with mock.patch.dict(os.environ, {}, clear=True):
            fink_settings, lasair_settings, babamul_settings = load_broker_settings()
        self.assertIs(fink_settings.role, BrokerRole.QUALIFYING)
        self.assertEqual(fink_settings.thresholds, Thresholds(0.6, 0.5))
        self.assertEqual(fink_settings.topic_list, ["fink_most_likely_sn_lsst"])
        self.assertIs(lasair_settings.role, BrokerRole.QUALIFYING)
        self.assertEqual(lasair_settings.thresholds, Thresholds(0.6, 1.0))
        self.assertEqual(lasair_settings.topic_list, [TOPIC])
        self.assertIs(babamul_settings.role, BrokerRole.FULL_STREAM)
        self.assertIsNone(babamul_settings.thresholds)
        self.assertEqual(babamul_settings.topic_list, [r"^babamul\.lsst\..*"])
        for settings in (fink_settings, lasair_settings, babamul_settings):
            self.assertFalse(settings.enabled)

    def test_environment_sets_role_and_thresholds(self) -> None:
        """SELDON_FINK_* variables set Fink's cut and threshold; an empty one
        means unset, so the default stays."""
        env = {
            "SELDON_FINK_RELIABILITY_CUT": "0.75",
            "SELDON_FINK_SN_LIKE_THRESHOLD": "",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            settings = FinkSettings()
        self.assertEqual(settings.thresholds, Thresholds(0.75, 0.5))

    def test_credentials_stay_out_of_the_text_form(self) -> None:
        """Babamul's username, password, group id and servers, set from the
        environment, appear in neither repr, str, nor a model dump, yet reach
        the Kafka client configuration."""
        env = {
            "SELDON_BABAMUL_ENABLED": "true",
            "SELDON_BABAMUL_USERNAME": SECRET_USER,
            "SELDON_BABAMUL_PASSWORD": SECRET_PASSWORD,
            "SELDON_BABAMUL_GROUP_ID": SECRET_GROUP,
            "SELDON_BABAMUL_BOOTSTRAP_SERVERS": SECRET_SERVERS,
        }
        with mock.patch.dict(os.environ, env, clear=True):
            settings = BabamulSettings()
        texts = [
            repr(settings),
            str(settings),
            str(settings.model_dump()),
            settings.model_dump_json(),
        ]
        for text in texts:
            for secret in (SECRET_USER, SECRET_PASSWORD, SECRET_SERVERS):
                self.assertNotIn(secret, text)
        config = kafka_config(settings)
        self.assertEqual(config["sasl.username"], SECRET_USER)
        self.assertEqual(config["sasl.password"], SECRET_PASSWORD)
        self.assertEqual(config["group.id"], SECRET_GROUP)
        self.assertEqual(config["bootstrap.servers"], SECRET_SERVERS)
        self.assertEqual(config["security.protocol"], "SASL_PLAINTEXT")
        self.assertEqual(config["sasl.mechanism"], "SCRAM-SHA-512")
        self.assertIs(config["enable.auto.commit"], False)

    def test_babamul_group_must_start_with_the_username(self) -> None:
        """Babamul refuses a consumer group not prefixed ``<username>-``, and
        the refusal does not echo the password."""
        env = {
            "SELDON_BABAMUL_ENABLED": "true",
            "SELDON_BABAMUL_USERNAME": SECRET_USER,
            "SELDON_BABAMUL_PASSWORD": SECRET_PASSWORD,
            "SELDON_BABAMUL_GROUP_ID": "someone-else-group",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            with self.assertRaises(ValidationError) as caught:
                BabamulSettings()
        self.assertNotIn(SECRET_PASSWORD, str(caught.exception))
        self.assertNotIn(SECRET_USER, str(caught.exception))

    def test_credentials_stay_out_of_the_logs(self) -> None:
        """Loading settings and running an adapter through poll, gap, and
        acknowledge logs nothing that contains a credential."""
        env = {
            "SELDON_LASAIR_ENABLED": "true",
            "SELDON_LASAIR_GROUP_ID": SECRET_GROUP,
            "SELDON_LASAIR_BOOTSTRAP_SERVERS": SECRET_SERVERS,
            "SELDON_LASAIR_USERNAME": SECRET_USER,
            "SELDON_LASAIR_PASSWORD": SECRET_PASSWORD,
        }
        start = datetime(2026, 11, 12, 23, 0, tzinfo=timezone.utc)
        consumer = FakeConsumer(
            _lasair_messages(10, _ms(RECEIVED)),
            committed=TopicPartition(TOPIC, 0, 5, metadata=str(_ms(start))),
        )
        with self.assertLogs("seldon", level=logging.DEBUG) as logs:
            logging.getLogger("seldon").debug("start")
            with mock.patch.dict(os.environ, env, clear=True):
                settings = LasairSettings()
            adapter = _adapter(consumer, settings)
            for polled in adapter.poll(timeout=0.1):
                adapter.ack(polled)
            adapter.gaps()
            adapter.close()
        output = "\n".join(logs.output)
        for secret in (SECRET_USER, SECRET_PASSWORD, SECRET_GROUP, SECRET_SERVERS):
            self.assertNotIn(secret, output)


class KafkaAdapterTests(SimpleTestCase):
    """Polling, acknowledging, and gap reporting over a fake consumer."""

    def test_poll_delivers_parsed_alerts_with_publish_and_receipt_times(self) -> None:
        """Each recorded Lasair message polls to a delivery for its object,
        published at the message's timestamp and received at the clock's
        time."""
        first_ms = _ms(datetime(2026, 11, 13, 0, 30, tzinfo=timezone.utc))
        consumer = FakeConsumer(_lasair_messages(0, first_ms))
        adapter = _adapter(consumer)
        polled = adapter.poll(timeout=0.1, max_messages=10)
        self.assertEqual(consumer.subscribed, [TOPIC])
        self.assertEqual(len(polled), len(lasair.ALERTS))
        for i, (item, recorded) in enumerate(zip(polled, lasair.ALERTS)):
            delivery = item.outcome
            self.assertIsInstance(delivery, Delivery)
            self.assertEqual(delivery.dia_object_id, recorded["alert"]["diaObjectId"])
            self.assertEqual(delivery.broker, "lasair")
            self.assertEqual(
                delivery.published_at,
                datetime.fromtimestamp((first_ms + 1000 * i) / 1000, tz=timezone.utc),
            )
            self.assertEqual(delivery.received_at, RECEIVED)

    def test_offsets_commit_only_when_acknowledged(self) -> None:
        """Polling commits nothing; acknowledging a delivery commits the offset
        after its message, on its partition."""
        consumer = FakeConsumer(_lasair_messages(40, _ms(RECEIVED)))
        adapter = _adapter(consumer)
        polled = adapter.poll(timeout=0.1, max_messages=10)
        self.assertEqual(consumer.commits, [])
        adapter.ack(polled[0])
        self.assertEqual(len(consumer.commits), 1)
        commit = consumer.commits[0]
        self.assertEqual(
            (commit.topic, commit.partition, commit.offset), (TOPIC, 0, 41)
        )

    def test_lost_messages_report_a_confirmed_gap(self) -> None:
        """When the group's committed offset (5, last acknowledged at 23:00)
        is older than the first retained offset (10, published at 01:00), the
        adapter resumes at 10 and reports one confirmed gap from 23:00 to
        01:00; asking again reports nothing new."""
        last_acked = datetime(2026, 11, 12, 23, 0, tzinfo=timezone.utc)
        first_retained = datetime(2026, 11, 13, 1, 0, tzinfo=timezone.utc)
        consumer = FakeConsumer(
            _lasair_messages(10, _ms(first_retained)),
            committed=TopicPartition(TOPIC, 0, 5, metadata=str(_ms(last_acked))),
        )
        adapter = _adapter(consumer)
        polled = adapter.poll(timeout=0.1)
        self.assertEqual(
            polled[0].outcome.dia_object_id, lasair.ALERTS[0]["alert"]["diaObjectId"]
        )
        self.assertEqual(consumer.assigned[0].offset, 10)
        self.assertEqual(
            adapter.gaps(),
            [
                StreamGapReport(
                    broker="lasair",
                    started_at=last_acked,
                    ended_at=first_retained,
                    kind=GapKind.CONFIRMED,
                )
            ],
        )
        self.assertEqual(adapter.gaps(), [])

    def test_acknowledged_time_survives_for_the_next_gap(self) -> None:
        """Acknowledging stores the message's publish time with the commit, so
        a later gap can start where reading stopped."""
        published = datetime(2026, 11, 13, 0, 45, tzinfo=timezone.utc)
        consumer = FakeConsumer(_lasair_messages(3, _ms(published)))
        adapter = _adapter(consumer)
        adapter.ack(adapter.poll(timeout=0.1)[0])
        self.assertEqual(consumer.commits[0].metadata, str(_ms(published)))

    def test_first_start_is_not_a_gap(self) -> None:
        """With no committed offset (the service never read this stream),
        retained history is not a gap."""
        consumer = FakeConsumer(_lasair_messages(10, _ms(RECEIVED)))
        adapter = _adapter(consumer)
        adapter.poll(timeout=0.1)
        self.assertEqual(adapter.gaps(), [])

    def test_committed_offset_still_retained_is_not_a_gap(self) -> None:
        """A committed offset at or after the first retained one loses
        nothing."""
        consumer = FakeConsumer(
            _lasair_messages(10, _ms(RECEIVED)),
            committed=TopicPartition(TOPIC, 0, 10, metadata="0"),
        )
        adapter = _adapter(consumer)
        adapter.poll(timeout=0.1)
        self.assertEqual(adapter.gaps(), [])

    def test_undecodable_message_is_an_intake_skip(self) -> None:
        """A message that is not JSON polls to an intake skip that names the
        message's place in the stream, and can still be acknowledged."""
        consumer = FakeConsumer([FakeMessage(0, 7, b"\x00not json", _ms(RECEIVED))])
        adapter = _adapter(consumer)
        polled = adapter.poll(timeout=0.1)
        self.assertEqual(len(polled), 1)
        skip = polled[0].outcome
        self.assertIsInstance(skip, IntakeSkipReport)
        self.assertEqual(skip.broker, "lasair")
        self.assertIn("7", skip.raw_identifier)
        adapter.ack(polled[0])
        self.assertEqual(consumer.commits[0].offset, 8)

    def test_full_stream_broker_never_qualifies(self) -> None:
        """A broker configured as full-stream delivers recorded alerts with the
        full-stream role, and none of them qualifies even under thresholds of
        zero that its scores would otherwise meet."""
        consumer = FakeConsumer(_lasair_messages(0, _ms(RECEIVED)))
        adapter = _adapter(consumer, _lasair_settings(role=BrokerRole.FULL_STREAM))
        polled = adapter.poll(timeout=0.1, max_messages=10)
        self.assertEqual(len(polled), len(lasair.ALERTS))
        for item in polled:
            self.assertIs(item.outcome.role, BrokerRole.FULL_STREAM)
            self.assertFalse(qualifies(item.outcome, Thresholds(0.0, 0.0)))


class MultiRecordMessageTests(SimpleTestCase):
    """A message carrying several alerts is committed only after its last."""

    def test_offset_commits_after_the_last_alert_of_a_message(self) -> None:
        """One message decodes to two alerts. Acknowledging the first commits
        nothing, so a crash before the second is stored redelivers both;
        acknowledging the second commits the offset after the message."""
        consumer = FakeConsumer(_lasair_messages(40, _ms(RECEIVED))[:1])
        two_alerts = [lasair.ALERTS[0]["alert"], lasair.ALERTS[1]["alert"]]
        with mock.patch(
            "seldon.infrastructure.brokers.parsers.lasair.decode",
            return_value=two_alerts,
        ):
            adapter = _adapter(consumer)
            polled = adapter.poll(timeout=0.1, max_messages=10)

        self.assertEqual(len(polled), 2)
        adapter.ack(polled[0])
        self.assertEqual(consumer.commits, [])
        adapter.ack(polled[1])
        self.assertEqual([c.offset for c in consumer.commits], [41])
