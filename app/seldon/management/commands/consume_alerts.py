"""Consume broker alerts continuously and store a forecast for each one owed.

This is the service's intake process. It loads the model once, then polls
every enabled broker in turn without waiting, and hands each broker's polled
alerts to the intake engine in batches of up to ``SELDON_INTAKE_BATCH_SIZE``
consecutive alerts, in poll order. Each batch is stored in one transaction,
and the broker is acknowledged only after the engine reports the batch
committed, with one commit per partition. A crash between the two causes a
redelivery after restart, which the engine turns into no second forecast.
Only after a cycle in which no broker returned an alert does the loop wait,
once, for the poll timeout before the next cycle.

On a database error the batch is rolled back, left unacknowledged and
retried, with exponential backoff, before anything later from that broker
is read: a Kafka consumer does not redeliver within a session, so skipping
ahead would lose it. Intake skips the adapters report are stored and then
acknowledged; gaps they report are stored as stream gaps. Each newly stored
alert, skips included, is counted in the intake-lag record with the
consumer's clock, read as its batch commits, as its commit time. A broker
that delivers nothing for longer than the silence period during the Rubin
night is recorded as a suspected gap. Pending detections are swept at start
and every hour. The wall time spent in each intake stage is logged every
five minutes and once at shutdown.

Several consumer processes may run at once, in the same consumer groups,
each reading every broker; Kafka spreads the partitions among them. A
conflict with another process's concurrent writes retries the batch at once
(see ``process_batch``) without the outage backoff. The sweep and the
silence check are housekeeping that one process at a time does: each runs in
a transaction that first tries a Postgres advisory lock, and a process that
does not get it skips that housekeeping for the cycle. The lock is taken
per transaction, never held across cycles, so a dropped connection cannot
silently keep or lose it. Each batch records its instance's last commit for
its broker (``ConsumerLastCommit``), and the silence check reads each
broker's latest commit across every instance, so a process that holds none
of a broker's partitions records no false gap. A broker commit refused
because a rebalance revoked the partition leaves the batch to be redelivered
to the partition's new reader; its work is already stored, and the
redelivery stores nothing twice.

SIGTERM or SIGINT stops the process after the batch in hand is committed
and acknowledged.
"""

import itertools
import logging
import os
import signal
import socket
import threading
from collections import deque
from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime, timedelta
from types import FrameType
from typing import Any

from django.core.management.base import BaseCommand, CommandError, CommandParser
from django.db import DatabaseError, connection, transaction
from pydantic import ValidationError

from seldon import services
from seldon.config.brokers import BrokerSettings, load_broker_settings
from seldon.config.intake import (
    IntakeSettings,
    get_consumer_settings,
    get_intake_settings,
)
from seldon.domain.intake.qualification import Thresholds
from seldon.infrastructure.brokers import (
    BrokerAdapter,
    GapKind,
    Polled,
    StreamGapReport,
    build_adapters,
)
from seldon.intake.engine import is_concurrency_conflict, process_batch, sweep_pending
from seldon.intake.timing import DEFAULT_TIMING_SECONDS, Stage, StageTimer
from seldon.models import ConsumerLastCommit, StreamGap

logger = logging.getLogger("seldon.consumer")

#: How long the loop waits after a cycle in which no broker returned an
#: alert, in seconds.
DEFAULT_POLL_TIMEOUT = 1.0

#: Most alerts taken from one broker in one poll.
DEFAULT_MAX_MESSAGES = 100

#: How often pending detections are swept, in minutes.
DEFAULT_SWEEP_MINUTES = 60.0

#: First and longest wait after a database error, in seconds.
BACKOFF_START = 1.0
BACKOFF_MAX = 60.0

#: The Postgres advisory lock key (a fixed 64-bit integer) the consumer
#: process doing housekeeping holds for the length of one sweep or one
#: silence check. Advisory locks are per database, so a replay database's
#: consumers never contend with the live ones.
HOUSEKEEPING_LOCK_KEY = 0x5E1D0_4005E


def consumer_instance_id() -> str:
    """Name this consumer process among the others sharing the database.

    The host name and process id: in a compose replica the host name is
    the container's, and a container restarted in place keeps both, so
    it goes on updating the same last-commit rows.

    Returns:
        ``<hostname>-<pid>``.
    """
    return f"{socket.gethostname()}-{os.getpid()}"


def housekeeping_lock() -> bool:
    """Try to take the housekeeping lock for the current transaction.

    Call it inside ``transaction.atomic``; the lock is released when that
    transaction ends.

    Returns:
        Whether this process holds the lock; ``False`` when another one
        does.
    """
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_try_advisory_xact_lock(%s)", [HOUSEKEEPING_LOCK_KEY])
        return bool(cursor.fetchone()[0])


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
        started: When the consumer started; silence never counts from
            before it.
    """

    def __init__(self, adapter: BrokerAdapter, started: datetime) -> None:
        """Track a broker from the consumer's start.

        Args:
            adapter: The broker's adapter.
            started: When the consumer started.
        """
        self.adapter = adapter
        self.queue: deque[Polled] = deque()
        self.started = started


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
        before_cycle: Callable[[], None] | None = None,
        finished: Callable[[], bool] | None = None,
        timer: StageTimer | None = None,
        timing_interval: float = DEFAULT_TIMING_SECONDS,
        idle_wait: Callable[[float], None] | None = None,
        instance: str | None = None,
    ) -> None:
        """Set up the loop; nothing is read until ``run``.

        Args:
            adapters: One adapter per broker to read.
            thresholds: Every enabled qualifying broker's thresholds.
            intake: The silence period, the Rubin night's hours, and the
                batch size.
            poll_timeout: How long to wait after a cycle in which no broker
                returned an alert, in seconds; every poll itself is made
                without waiting.
            max_messages: Most alerts taken in one poll.
            sweep_interval: How often to sweep pending detections.
            clock: Returns the current aware UTC time; also the commit time
                each alert's intake lag is measured to.
            sleep: Waits a number of seconds; by default a wait that
                ``stop`` cuts short.
            before_cycle: Called at the start of every cycle, before the
                sweep; a replay releases its next ordered run here.
            finished: Says whether nothing is left to read; the loop stops
                after a cycle that leaves it ``True``. Live intake never
                finishes.
            timer: Accumulates each intake stage's wall time; pass the one
                the adapters were built with so their stages are counted.
                A timer of the loop's own when omitted.
            timing_interval: Seconds between logged timing summaries, on
                the timer's clock; one is also logged at shutdown.
            idle_wait: Waits up to a number of seconds after a cycle in
                which no broker returned an alert; ``sleep`` when omitted. A
                replay passes a wait that ends when its next message is due.
            instance: This consumer's name in the shared last-commit
                record; ``consumer_instance_id()`` when omitted.
        """
        self._adapters = list(adapters)
        self._thresholds = dict(thresholds)
        self._silence = timedelta(minutes=intake.silence_minutes)
        self._night_hours = (intake.night_start_utc_hour, intake.night_end_utc_hour)
        self._poll_timeout = poll_timeout
        self._max_messages = max_messages
        self._batch_size = intake.batch_size
        self._sweep_interval = sweep_interval
        self._clock = clock
        self._stopping = threading.Event()
        self._sleep = sleep or (lambda seconds: self._stopping.wait(seconds))
        self._idle_wait = idle_wait or self._sleep
        self._backoff = BACKOFF_START
        self._unsaved_gaps: list[StreamGapReport] = []
        self._next_sweep: datetime | None = None
        self._before_cycle = before_cycle
        self._finished = finished
        self._timer = timer or StageTimer()
        self._timing_interval = timing_interval
        self._instance = instance or consumer_instance_id()

    def stop(self) -> None:
        """Stop after the batch in hand; safe to call from a signal handler."""
        self._stopping.set()

    @property
    def stopping(self) -> bool:
        """Whether ``stop`` has been called.

        Returns:
            ``True`` once a stop was asked for.
        """
        return self._stopping.is_set()

    def run(self) -> None:
        """Consume until stopped or finished, then close every adapter.

        Raises:
            seldon.domain.errors.IncompatibleCheckpointError: If the
                checkpoint cannot serve a forecast.
            KeyError: If a qualifying broker has no thresholds.
        """
        brokers = [_Broker(adapter, self._clock()) for adapter in self._adapters]
        self._timer.restart()
        if not brokers:
            logger.warning("No broker is enabled; only sweeping pending detections")
        try:
            while not self.stopping:
                if self._before_cycle is not None:
                    self._before_cycle()
                taken = self._cycle(brokers)
                if taken is None:
                    self._back_off()
                    continue
                self._backoff = BACKOFF_START
                self._timer.log_if_due(self._timing_interval)
                if self._finished is not None and self._finished():
                    break
                if not taken and not self.stopping:
                    with self._timer.stage(Stage.POLL_WAIT):
                        self._idle_wait(self._poll_timeout)
        finally:
            for broker in brokers:
                try:
                    broker.adapter.close()
                except Exception:  # closing one must not keep others open
                    logger.exception("Closing %s failed", broker.adapter.name)
            self._timer.log_summary()
        logger.info("Consumer stopped")

    def _cycle(self, brokers: list[_Broker]) -> int | None:
        """Sweep if due, store gaps, read each broker once, check silence.

        Each broker is polled without waiting, unless alerts it returned
        earlier are still queued (a database error left them unacknowledged):
        those are handled first, and it is not polled again until they are.

        Args:
            brokers: Every broker read.

        Returns:
            How many alerts the cycle had to handle, polled or still queued;
            ``None`` when a database error stopped the cycle.
        """
        if not self._sweep_if_due() or not self._store_gaps():
            return None
        taken = 0
        for broker in brokers:
            if self.stopping:
                return taken
            if not broker.queue:
                polled = broker.adapter.poll(0.0, self._max_messages)
                self._unsaved_gaps.extend(broker.adapter.gaps())
                broker.queue.extend(polled)
            taken += len(broker.queue)
            if not self._drain(broker):
                return None
        if not self._check_silence(brokers):
            return None
        return taken if self._store_gaps() else None

    def _drain(self, broker: _Broker) -> bool:
        """Handle a broker's queued alerts in batches, in order.

        Each batch is up to the batch size of the queue's first alerts. It
        is acknowledged once its work is committed, and then leaves the
        queue. A stop asked for while a batch is in hand takes effect after
        that batch is acknowledged.

        Args:
            broker: The broker whose queue to work through.

        Returns:
            ``False`` when a database error rolled back the batch in hand; it
            stays queued, unacknowledged.
        """
        while broker.queue and not self.stopping:
            batch = list(itertools.islice(broker.queue, self._batch_size))
            with self._timer.stage(Stage.DATABASE):
                handled = self._handle(batch)
            if not handled:
                return False
            broker.adapter.ack(batch)
            for _ in batch:
                broker.queue.popleft()
                self._timer.count_alert()
        return True

    def _handle(self, batch: list[Polled]) -> bool:
        """Store what a batch of polled alerts causes, in one transaction.

        Args:
            batch: Consecutive polled alerts from one broker.

        Returns:
            Whether the batch is committed and may be acknowledged.
        """
        result = process_batch(
            [polled.outcome for polled in batch],
            self._thresholds,
            clock=self._clock,
            timer=self._timer,
            instance=self._instance,
        )
        if not result.acknowledge:
            self._reset_connection()
        return result.acknowledge

    def _sweep_if_due(self) -> bool:
        """Sweep pending detections at start and every sweep interval.

        The sweep runs in one transaction holding the housekeeping lock. When
        another process holds it, or the sweep conflicts with another
        process's writes, it is skipped and tried again on the next cycle.

        Returns:
            ``False`` when a database error stopped the sweep; it is retried
            on the next cycle.
        """
        now = self._clock()
        if self._next_sweep is not None and now < self._next_sweep:
            return True
        try:
            with transaction.atomic():
                if not housekeeping_lock():
                    logger.debug("Another consumer holds housekeeping; no sweep")
                    return True
                sweep_pending(now)
        except DatabaseError as error:
            if is_concurrency_conflict(error):
                logger.warning("Sweep conflicted with another consumer; retrying")
                return True
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
            self._create_gap(report)
        except DatabaseError:
            logger.exception("Database error storing a %s stream gap", report.broker)
            self._reset_connection()
            return False
        return True

    @staticmethod
    def _create_gap(report: StreamGapReport) -> None:
        """Insert one stream gap and log it.

        Args:
            report: The gap.
        """
        StreamGap.objects.create(
            broker=report.broker,
            started_at=report.started_at,
            ended_at=report.ended_at,
            kind=StreamGap.Kind(report.kind.value),
        )
        logger.warning(
            "%s: %s gap from %s to %s",
            report.broker,
            report.kind.value,
            report.started_at.isoformat(),
            report.ended_at.isoformat(),
        )

    def _check_silence(self, brokers: list[_Broker]) -> bool:
        """Record a suspected gap for each broker silent too long in the night.

        A broker's silence counts from its latest commit by any consumer
        instance, but not from before this consumer started or the current
        night started. Each silent stretch is recorded once, ending when the
        silence was detected: a stretch that already has a suspected gap
        ending inside it is not recorded again. The check runs in one
        transaction holding the housekeeping lock, and is skipped for the
        cycle when another process holds it.

        Args:
            brokers: Every broker read.

        Returns:
            ``False`` when a database error kept a gap unstored; the check
            runs again on the next cycle.
        """
        now = self._clock()
        night = night_window(now, *self._night_hours)
        if night is None or not brokers:
            return True
        try:
            with transaction.atomic():
                if not housekeeping_lock():
                    return True
                latest = ConsumerLastCommit.latest(
                    [broker.adapter.name for broker in brokers]
                )
                for broker in brokers:
                    name = broker.adapter.name
                    silent_since = max(
                        latest.get(name, broker.started), broker.started, night[0]
                    )
                    if now - silent_since <= self._silence:
                        continue
                    if StreamGap.objects.filter(
                        broker=name,
                        kind=StreamGap.Kind.SUSPECTED,
                        ended_at__gt=silent_since,
                    ).exists():
                        continue
                    self._create_gap(
                        StreamGapReport(
                            broker=name,
                            started_at=silent_since,
                            ended_at=now,
                            kind=GapKind.SUSPECTED,
                        )
                    )
        except DatabaseError as error:
            if is_concurrency_conflict(error):
                logger.warning("Silence check conflicted with another consumer")
                return True
            logger.exception("Database error checking broker silence")
            self._reset_connection()
            return False
        return True

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
            help=(
                "Seconds to wait after a cycle in which no broker returned "
                "an alert; brokers are always polled without waiting."
            ),
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
        parser.add_argument(
            "--timing-seconds",
            type=float,
            default=DEFAULT_TIMING_SECONDS,
            help="Seconds between logged summaries of time spent per intake stage.",
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

        try:
            processes = get_consumer_settings().processes
        except ValidationError as error:
            raise CommandError(f"Invalid SELDON_CONSUMER_PROCESSES: {error}")
        instance = consumer_instance_id()
        logger.info(
            "Consumer instance %s, configured for %d process(es)", instance, processes
        )

        broker_settings = load_broker_settings()
        intake = get_intake_settings()
        thresholds = qualifying_thresholds(broker_settings)
        timer = StageTimer()
        consumer = IntakeConsumer(
            build_adapters(broker_settings, timer=timer),
            thresholds,
            intake,
            poll_timeout=options["poll_timeout"],
            max_messages=options["max_messages"],
            sweep_interval=timedelta(minutes=options["sweep_minutes"]),
            timer=timer,
            timing_interval=options["timing_seconds"],
            instance=instance,
        )

        def on_signal(signum: int, frame: FrameType | None) -> None:
            """Ask the consumer to stop after the batch in hand."""
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
