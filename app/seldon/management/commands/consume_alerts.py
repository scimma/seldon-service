"""Consume broker alerts continuously and store a forecast for each one owed.

This is the service's intake process. It loads the model once, then polls
every enabled broker in turn, hands each delivery to the intake engine, and
acknowledges the broker message only after the engine reports its result
committed. A crash between the two causes a redelivery after restart, which
the engine turns into no second forecast.

On a database error the delivery is left unacknowledged and retried, with
exponential backoff, before anything later from that broker is read: a
Kafka consumer does not redeliver within a session, so skipping ahead would
lose it. Intake skips the adapters report are stored and then acknowledged;
gaps they report are stored as stream gaps. A broker that delivers nothing
for longer than the silence period during the Rubin night is recorded as a
suspected gap. Pending detections are swept at start and every hour.

SIGTERM or SIGINT stops the process after the delivery in hand.
"""

import logging
import signal
import threading
from collections import deque
from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime, timedelta
from types import FrameType
from typing import Any

from django.core.management.base import BaseCommand, CommandError, CommandParser
from django.db import DatabaseError, connection

from seldon import services
from seldon.config.brokers import BrokerSettings, load_broker_settings
from seldon.config.intake import IntakeSettings, get_intake_settings
from seldon.domain.intake.qualification import Thresholds
from seldon.infrastructure.brokers import (
    BrokerAdapter,
    GapKind,
    IntakeSkipReport,
    Polled,
    StreamGapReport,
    build_adapters,
)
from seldon.intake.engine import process_delivery, record_intake_skip, sweep_pending
from seldon.models import StreamGap

logger = logging.getLogger("seldon.consumer")

#: Longest wait for alerts in one poll of one broker, in seconds.
DEFAULT_POLL_TIMEOUT = 1.0

#: Most alerts taken from one broker in one poll.
DEFAULT_MAX_MESSAGES = 100

#: How often pending detections are swept, in minutes.
DEFAULT_SWEEP_MINUTES = 60.0

#: First and longest wait after a database error, in seconds.
BACKOFF_START = 1.0
BACKOFF_MAX = 60.0


def utc_now() -> datetime:
    """The current time.

    Returns:
        An aware UTC datetime.
    """
    return datetime.now(UTC)


def qualifying_thresholds(
    settings: Iterable[BrokerSettings],
) -> dict[str, Thresholds]:
    """The thresholds of every enabled qualifying broker.

    Args:
        settings: Every broker's settings.

    Returns:
        Broker name to its cut and threshold.
    """
    found = {}
    for broker in settings:
        limits = broker.thresholds
        if broker.enabled and limits is not None:
            found[broker.name] = limits
    return found


def night_window(
    now: datetime, start_hour: int, end_hour: int
) -> tuple[datetime, datetime] | None:
    """The Rubin night containing a moment, if it falls in one.

    Hours are UTC. When ``start_hour`` is later than ``end_hour`` the night
    crosses midnight; when they are equal every hour is night.

    Args:
        now: An aware moment.
        start_hour: The UTC hour the night starts.
        end_hour: The UTC hour it ends.

    Returns:
        The night's start and end, or ``None`` when ``now`` is daytime.
    """
    now = now.astimezone(UTC)
    day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    one_day = timedelta(days=1)
    if start_hour < end_hour:
        if start_hour <= now.hour < end_hour:
            return day.replace(hour=start_hour), day.replace(hour=end_hour)
        return None
    if now.hour >= start_hour:
        return day.replace(hour=start_hour), (day + one_day).replace(hour=end_hour)
    if now.hour < end_hour:
        return (day - one_day).replace(hour=start_hour), day.replace(hour=end_hour)
    return None


class _Broker:
    """One adapter and what the consumer tracks for it.

    Attributes:
        adapter: The broker's adapter.
        queue: Polled alerts not yet acknowledged, in poll order.
        last_activity: When it last returned anything, or when the consumer
            started.
        silence_recorded: Whether the current silent stretch has its gap.
    """

    def __init__(self, adapter: BrokerAdapter, started: datetime) -> None:
        """Track a broker from the consumer's start.

        Args:
            adapter: The broker's adapter.
            started: When the consumer started.
        """
        self.adapter = adapter
        self.queue: deque[Polled] = deque()
        self.last_activity = started
        self.silence_recorded = False


class IntakeConsumer:
    """The intake loop over a set of broker adapters."""

    def __init__(
        self,
        adapters: Iterable[BrokerAdapter],
        thresholds: Mapping[str, Thresholds],
        intake: IntakeSettings,
        *,
        poll_timeout: float = DEFAULT_POLL_TIMEOUT,
        max_messages: int = DEFAULT_MAX_MESSAGES,
        sweep_interval: timedelta = timedelta(minutes=DEFAULT_SWEEP_MINUTES),
        clock: Callable[[], datetime] = utc_now,
        sleep: Callable[[float], None] | None = None,
    ) -> None:
        """Set up the loop; nothing is read until ``run``.

        Args:
            adapters: One adapter per broker to read.
            thresholds: Every enabled qualifying broker's thresholds.
            intake: The silence period and the Rubin night's hours.
            poll_timeout: Longest wait in one poll of one broker, in seconds.
            max_messages: Most alerts taken in one poll.
            sweep_interval: How often to sweep pending detections.
            clock: Returns the current aware UTC time.
            sleep: Waits a number of seconds; by default a wait that
                ``stop`` cuts short.
        """
        self._adapters = list(adapters)
        self._thresholds = dict(thresholds)
        self._silence = timedelta(minutes=intake.silence_minutes)
        self._night_hours = (intake.night_start_utc_hour, intake.night_end_utc_hour)
        self._poll_timeout = poll_timeout
        self._max_messages = max_messages
        self._sweep_interval = sweep_interval
        self._clock = clock
        self._stopping = threading.Event()
        self._sleep = sleep or (lambda seconds: self._stopping.wait(seconds))
        self._backoff = BACKOFF_START
        self._unsaved_gaps: list[StreamGapReport] = []
        self._next_sweep: datetime | None = None

    def stop(self) -> None:
        """Stop after the delivery in hand; safe to call from a signal handler."""
        self._stopping.set()

    @property
    def stopping(self) -> bool:
        """Whether ``stop`` has been called.

        Returns:
            ``True`` once a stop was asked for.
        """
        return self._stopping.is_set()

    def run(self) -> None:
        """Consume until stopped, then close every adapter.

        Raises:
            seldon.domain.errors.IncompatibleCheckpointError: If the
                checkpoint cannot serve a forecast.
            KeyError: If a qualifying broker has no thresholds.
        """
        brokers = [_Broker(adapter, self._clock()) for adapter in self._adapters]
        if not brokers:
            logger.warning("No broker is enabled; only sweeping pending detections")
        try:
            while not self.stopping:
                if not self._cycle(brokers):
                    self._back_off()
                    continue
                self._backoff = BACKOFF_START
                if not brokers:
                    self._sleep(self._poll_timeout)
        finally:
            for broker in brokers:
                try:
                    broker.adapter.close()
                except Exception:  # closing one must not keep others open
                    logger.exception("Closing %s failed", broker.adapter.name)
        logger.info("Consumer stopped")

    def _cycle(self, brokers: list[_Broker]) -> bool:
        """Sweep if due, store gaps, and read each broker once.

        Args:
            brokers: Every broker read.

        Returns:
            ``False`` when a database error stopped the cycle.
        """
        if not self._sweep_if_due() or not self._store_gaps():
            return False
        for broker in brokers:
            if self.stopping:
                return True
            if not broker.queue:
                polled = broker.adapter.poll(self._poll_timeout, self._max_messages)
                self._unsaved_gaps.extend(broker.adapter.gaps())
                if polled:
                    broker.queue.extend(polled)
                    broker.last_activity = self._clock()
                    broker.silence_recorded = False
            if not self._drain(broker):
                return False
            if not self._check_silence(broker):
                return False
        return self._store_gaps()

    def _drain(self, broker: _Broker) -> bool:
        """Handle a broker's polled alerts in order, acknowledging each.

        Args:
            broker: The broker whose queue to work through.

        Returns:
            ``False`` when a database error left the head unacknowledged.
        """
        while broker.queue and not self.stopping:
            polled = broker.queue[0]
            if not self._handle(polled.outcome):
                return False
            broker.adapter.ack(polled)
            broker.queue.popleft()
        return True

    def _handle(self, outcome: Any) -> bool:
        """Store what one polled alert causes.

        Args:
            outcome: A ``Delivery`` or an ``IntakeSkipReport``.

        Returns:
            Whether it is committed and may be acknowledged.
        """
        if isinstance(outcome, IntakeSkipReport):
            try:
                record_intake_skip(
                    outcome.broker,
                    outcome.reason,
                    outcome.occurred_at,
                    outcome.raw_identifier,
                )
            except DatabaseError:
                logger.exception("Database error storing a %s skip", outcome.broker)
                self._reset_connection()
                return False
            return True
        result = process_delivery(outcome, self._thresholds)
        if not result.acknowledge:
            self._reset_connection()
        return result.acknowledge

    def _sweep_if_due(self) -> bool:
        """Sweep pending detections at start and every sweep interval.

        Returns:
            ``False`` when a database error stopped the sweep; it is retried
            on the next cycle.
        """
        now = self._clock()
        if self._next_sweep is not None and now < self._next_sweep:
            return True
        try:
            sweep_pending(now)
        except DatabaseError:
            logger.exception("Database error sweeping pending detections")
            self._reset_connection()
            return False
        self._next_sweep = now + self._sweep_interval
        return True

    def _store_gaps(self) -> bool:
        """Store the gaps reported but not yet stored.

        Returns:
            ``False`` when a database error kept some unstored; they are
            retried on the next cycle.
        """
        while self._unsaved_gaps:
            report = self._unsaved_gaps[0]
            if not self._store_gap(report):
                return False
            self._unsaved_gaps.pop(0)
        return True

    def _store_gap(self, report: StreamGapReport) -> bool:
        """Store one stream gap.

        Args:
            report: The gap.

        Returns:
            Whether it was stored.
        """
        try:
            StreamGap.objects.create(
                broker=report.broker,
                started_at=report.started_at,
                ended_at=report.ended_at,
                kind=StreamGap.Kind(report.kind.value),
            )
        except DatabaseError:
            logger.exception("Database error storing a %s stream gap", report.broker)
            self._reset_connection()
            return False
        logger.warning(
            "%s: %s gap from %s to %s",
            report.broker,
            report.kind.value,
            report.started_at.isoformat(),
            report.ended_at.isoformat(),
        )
        return True

    def _check_silence(self, broker: _Broker) -> bool:
        """Record a suspected gap for a broker silent too long in the night.

        Silence counts from the broker's last activity (or the consumer's
        start), but not from before the current night started. Each silent
        stretch is recorded once, ending when the silence was detected.

        Args:
            broker: The broker to check.

        Returns:
            ``False`` when a database error kept the gap unstored; it is
            tried again on the next cycle.
        """
        if broker.silence_recorded:
            return True
        now = self._clock()
        night = night_window(now, *self._night_hours)
        if night is None:
            return True
        silent_since = max(broker.last_activity, night[0])
        if now - silent_since <= self._silence:
            return True
        name = broker.adapter.name
        stored = self._store_gap(
            StreamGapReport(
                broker=name,
                started_at=silent_since,
                ended_at=now,
                kind=GapKind.SUSPECTED,
            )
        )
        broker.silence_recorded = stored
        return stored

    def _back_off(self) -> None:
        """Wait after a database error, longer each time up to a limit."""
        logger.warning("Database unavailable; retrying in %.0f s", self._backoff)
        self._sleep(self._backoff)
        self._backoff = min(self._backoff * 2, BACKOFF_MAX)

    @staticmethod
    def _reset_connection() -> None:
        """Drop a broken database connection so the next query reconnects.

        Inside a transaction (as under a test case) the connection is left
        alone: closing it there would end the transaction.
        """
        if not connection.in_atomic_block:
            connection.close_if_unusable_or_obsolete()


class Command(BaseCommand):
    """Consume broker alerts until stopped."""

    help = (
        "Read every enabled broker's alerts, forecast each detection owed, "
        "and store the results; stop cleanly on SIGTERM."
    )

    def add_arguments(self, parser: CommandParser) -> None:
        """Add the polling and sweep options.

        Args:
            parser: The command's argument parser.
        """
        parser.add_argument(
            "--poll-timeout",
            type=float,
            default=DEFAULT_POLL_TIMEOUT,
            help="Longest wait in one poll of one broker, in seconds.",
        )
        parser.add_argument(
            "--max-messages",
            type=int,
            default=DEFAULT_MAX_MESSAGES,
            help="Most alerts taken from one broker in one poll.",
        )
        parser.add_argument(
            "--sweep-minutes",
            type=float,
            default=DEFAULT_SWEEP_MINUTES,
            help="Minutes between sweeps of pending detections.",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        """Load the model, connect to the brokers, and consume until stopped.

        Args:
            *args: Unused.
            **options: The parsed options.

        Raises:
            CommandError: If the model cannot be loaded; nothing is read.
        """
        try:
            model = services.loaded_model()
        except Exception as error:
            raise CommandError(f"Model failed to load; not consuming: {error}")
        logger.info("Model loaded from %s", model.checkpoint_id)

        broker_settings = load_broker_settings()
        intake = get_intake_settings()
        thresholds = qualifying_thresholds(broker_settings)
        consumer = IntakeConsumer(
            build_adapters(broker_settings),
            thresholds,
            intake,
            poll_timeout=options["poll_timeout"],
            max_messages=options["max_messages"],
            sweep_interval=timedelta(minutes=options["sweep_minutes"]),
        )

        def on_signal(signum: int, frame: FrameType | None) -> None:
            """Ask the consumer to stop after the delivery in hand."""
            logger.info("Received %s; stopping", signal.Signals(signum).name)
            consumer.stop()

        previous = {
            signum: signal.signal(signum, on_signal)
            for signum in (signal.SIGTERM, signal.SIGINT)
        }
        try:
            consumer.run()
        finally:
            for signum, handler in previous.items():
                signal.signal(signum, handler)
