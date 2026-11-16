"""Tests for several consumer processes writing to one database at once.

Each simulated process is a thread with its own database connection, so
row locks, unique indexes and advisory locks contend as they do between
real processes. These are ``TransactionTestCase`` tests: a ``TestCase``
wraps everything in one transaction on one connection and cannot show
lock contention. The engine and the database (the compose ``db`` service)
are used as they are.

Where the order of two transactions matters, the test forces it: the first
thread holds its lock until the database reports the second thread waiting
on a lock, and only then goes on.
"""

import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest import mock

from django.core.management import call_command
from django.db import connection
from django.test import TransactionTestCase

from seldon.domain.intake.delivery import Point
from seldon.intake import engine
from seldon.intake.engine import owed_packet, process_batch
from seldon.management.commands.consume_alerts import IntakeConsumer
from seldon.management.commands.migrate_locked import MIGRATE_LOCK_KEY
from seldon.models import ConsumerLastCommit
from seldon.models import Delivery as StoredDelivery
from seldon.models import (
    Detection,
    LightCurvePoint,
    RubinObject,
    StreamGap,
)
from seldon.tests.test_consume_alerts import (
    FakeAdapter,
    FakeClock,
    FakeStream,
    intake,
)
from seldon.tests.test_intake_engine import (
    ABOVE,
    BELOW,
    FIRST_NIGHT,
    OBJECT_ID,
    OTHER_OBJECT_ID,
    THRESHOLDS,
    delivery,
    point,
    source_id,
)

#: Longest a test waits for a thread or a lock, in seconds.
WAIT_SECONDS = 30.0


def run_threads(*calls: Callable[[], Any]) -> list[Any]:
    """Run each call in a thread of its own, all released together.

    Each thread closes its database connection when it ends, as a process
    would on exit, so the test database can be torn down.

    Args:
        *calls: The work of each simulated process.

    Returns:
        Each call's return value, in order.

    Raises:
        AssertionError: If a thread is still running after the wait.
        BaseException: The first exception a call raised.
    """
    barrier = threading.Barrier(len(calls))
    results: list[Any] = [None] * len(calls)
    errors: list[BaseException | None] = [None] * len(calls)

    def work(index: int, call: Callable[[], Any]) -> None:
        try:
            barrier.wait(timeout=WAIT_SECONDS)
            results[index] = call()
        except BaseException as error:  # reported in the main thread
            errors[index] = error
        finally:
            connection.close()

    threads = [
        threading.Thread(target=work, args=(i, call), name=f"process-{i}")
        for i, call in enumerate(calls)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(WAIT_SECONDS * 2)
        if thread.is_alive():
            raise AssertionError(f"{thread.name} did not finish")
    for error in errors:
        if error is not None:
            raise error
    return results


def wait_for_lock_waiter() -> None:
    """Wait until another session of this database is waiting on a lock.

    Raises:
        AssertionError: If none waits within the wait.
    """
    deadline = time.monotonic() + WAIT_SECONDS
    with connection.cursor() as cursor:
        while time.monotonic() < deadline:
            cursor.execute("SELECT pg_stat_clear_snapshot()")
            cursor.execute(
                "SELECT count(*) FROM pg_stat_activity "
                "WHERE datname = current_database() AND wait_event_type = 'Lock' "
                "AND pid <> pg_backend_pid()"
            )
            if cursor.fetchone()[0]:
                return
            time.sleep(0.02)
    raise AssertionError("no other session waited on a lock")


def hold_after(name: str, holds: Callable[..., bool]) -> tuple[Any, threading.Event]:
    """Patch an engine step so a chosen call pauses until another waits.

    The chosen call runs the step, then keeps its transaction open until
    another session is waiting on a lock, and sets the returned event as
    it starts waiting, so the other thread can be started then.

    Args:
        name: The engine function to wrap.
        holds: Given the step's arguments, whether this call is the one to
            hold; only its first match holds.

    Returns:
        The patcher (use as a context manager) and the event.
    """
    original = getattr(engine, name)
    holding = threading.Event()

    def held(*args: Any, **kwargs: Any) -> Any:
        result = original(*args, **kwargs)
        if not holding.is_set() and holds(*args, **kwargs):
            holding.set()
            wait_for_lock_waiter()
        return result

    return mock.patch.object(engine, name, side_effect=held), holding


def after(event: threading.Event, call: Callable[[], Any]) -> Callable[[], Any]:
    """A call that starts only once an event is set.

    Args:
        event: The event to wait for.
        call: The work.

    Returns:
        The delayed work.
    """

    def delayed() -> Any:
        if not event.wait(WAIT_SECONDS):
            raise AssertionError("the first thread never took its lock")
        return call()

    return delayed


class SameDetectionFromTwoProcessesTests(TransactionTestCase):
    """Babamul's and Lasair's deliveries of one detection, handled at once."""

    def deliver_both(self, first: str) -> list[float]:
        """Deliver night 2 from Babamul and Lasair in two threads at once.

        Both deliveries are received at the same moment. Babamul's carries
        nights 0 and 1; Lasair's carries only night 1 and qualifies the
        object. The thread delivering ``first`` takes the object's lock and
        holds it until the other is waiting for it.

        Args:
            first: The broker whose delivery commits first.

        Returns:
            The observation times in night 2's packet.
        """
        babamul = delivery(2, "babamul", previous=(point(0), point(1)))
        lasair = delivery(2, "lasair", ABOVE, previous=(point(1),))
        patcher, holding = hold_after(
            "_record_delivery", lambda d, detection: d.broker == first
        )
        calls = {
            name: (lambda d=d: process_batch([d], THRESHOLDS))
            for name, d in (("babamul", babamul), ("lasair", lasair))
        }
        second = "lasair" if first == "babamul" else "babamul"
        with patcher:
            results = run_threads(calls[first], after(holding, calls[second]))

        self.assertTrue(all(result.acknowledge for result in results))
        committed = StoredDelivery.objects.order_by("pk")
        self.assertEqual([d.broker for d in committed], [first, second])
        detection = Detection.objects.get(dia_source_id=source_id(2))
        self.assertEqual(detection.state, Detection.State.OWED)
        return [p.time for p in owed_packet(detection).points]

    def test_babamul_committed_first_is_the_packet_source(self) -> None:
        """Babamul's delivery commits first and leaves night 2 pending;
        Lasair's then qualifies it. It is owed once, and its packet holds
        Babamul's three points."""
        times = self.deliver_both("babamul")

        self.assertEqual(times, [FIRST_NIGHT, FIRST_NIGHT + 1, FIRST_NIGHT + 2])

    def test_lasair_committed_first_is_the_packet_source(self) -> None:
        """Lasair's delivery commits first and is owed at once, its packet
        holding its two points; Babamul's then only adds a delivery."""
        times = self.deliver_both("lasair")

        self.assertEqual(times, [FIRST_NIGHT + 1, FIRST_NIGHT + 2])


class ConcurrentInsertTests(TransactionTestCase):
    """Rows another process has just created are found, not duplicated."""

    def test_same_new_object_and_detection_from_two_processes(self) -> None:
        """Fink (below threshold) and Babamul deliver night 0 of an object
        neither has seen, at once. Both batches commit, with one object, one
        detection and two deliveries stored."""
        fink = delivery(0, "fink", BELOW)
        babamul = delivery(0, "babamul")
        patcher, holding = hold_after(
            "_record_delivery", lambda d, detection: d.broker == "fink"
        )
        with patcher:
            results = run_threads(
                lambda: process_batch([fink], THRESHOLDS),
                after(holding, lambda: process_batch([babamul], THRESHOLDS)),
            )

        self.assertTrue(all(result.acknowledge for result in results))
        self.assertEqual(RubinObject.objects.count(), 1)
        self.assertEqual(Detection.objects.count(), 1)
        self.assertEqual(StoredDelivery.objects.count(), 2)

    def test_same_light_curve_point_from_two_processes(self) -> None:
        """Two objects' alerts both carry the same earlier measurement,
        identified by one diaSourceId, and are stored at once. The second
        insert of that point is refused by its unique index once the first
        commits; that batch is retried and both finish, with the point
        stored once."""
        shared = Point(
            observed_mjd=FIRST_NIGHT,
            band="r",
            flux=900.0,
            flux_err=50.0,
            dia_source_id=999_000_000_001,
        )
        one = delivery(1, "babamul", previous=(shared,))
        other = delivery(1, "babamul", previous=(shared,), object_id=OTHER_OBJECT_ID)
        patcher, holding = hold_after(
            "_stored_points", lambda object_id, points: object_id == OBJECT_ID
        )
        with patcher, self.assertLogs("seldon.intake", level="WARNING") as logs:
            results = run_threads(
                lambda: process_batch([one], THRESHOLDS),
                after(holding, lambda: process_batch([other], THRESHOLDS)),
            )

        self.assertTrue(all(result.acknowledge for result in results))
        self.assertEqual(
            LightCurvePoint.objects.filter(dia_source_id=shared.dia_source_id).count(),
            1,
        )
        self.assertEqual(Detection.objects.count(), 2)
        self.assertTrue(any("retrying" in line for line in logs.output))


class OppositeLockOrderTests(TransactionTestCase):
    """Two batches locking the same objects in opposite orders."""

    def test_deadlocked_batch_is_retried_and_both_finish(self) -> None:
        """Lasair's batch delivers object A then object B; Fink's delivers B
        then A, both above threshold. Each takes its first object's lock
        before either asks for its second, so the database breaks a
        deadlock by failing one batch. That batch is retried at once, and
        both commit with both detections owed."""
        for object_id in (OBJECT_ID, OTHER_OBJECT_ID):
            RubinObject.objects.create(dia_object_id=object_id)
        lasair = [
            delivery(0, "lasair", ABOVE),
            delivery(0, "lasair", ABOVE, object_id=OTHER_OBJECT_ID),
        ]
        fink = [
            delivery(0, "fink", ABOVE, object_id=OTHER_OBJECT_ID),
            delivery(0, "fink", ABOVE),
        ]
        barrier = threading.Barrier(2)
        calls = threading.local()
        original = engine._process

        def meet_after_first(*args: Any, **kwargs: Any) -> Any:
            result = original(*args, **kwargs)
            count = getattr(calls, "count", 0)
            calls.count = count + 1
            if count == 0:
                barrier.wait(timeout=WAIT_SECONDS)
            return result

        with (
            mock.patch.object(engine, "_process", side_effect=meet_after_first),
            self.assertLogs("seldon.intake", level="WARNING") as logs,
        ):
            results = run_threads(
                lambda: process_batch(lasair, THRESHOLDS),
                lambda: process_batch(fink, THRESHOLDS),
            )

        self.assertTrue(all(result.acknowledge for result in results))
        self.assertEqual(
            sorted(
                Detection.objects.filter(state=Detection.State.OWED).values_list(
                    "dia_source_id", flat=True
                )
            ),
            sorted([source_id(0), source_id(0, OTHER_OBJECT_ID)]),
        )
        self.assertEqual(StoredDelivery.objects.count(), 4)
        self.assertTrue(any("retrying" in line for line in logs.output))


#: Midnight UTC, inside the Rubin night (23:00 to 11:00 UTC).
MIDNIGHT = datetime(2026, 11, 13, 0, 0, tzinfo=UTC)


def silent_babamul_consumer(
    instance: str, on_poll: Callable[[FakeClock], None] | None = None
) -> IntakeConsumer:
    """A consumer whose only broker, Babamul, delivers nothing.

    It starts at midnight; its one poll moves its clock two hours on, past
    the 60-minute silence period, and it stops after that poll's cycle.

    Args:
        instance: The consumer's instance name.
        on_poll: Also called on the poll, with the moved clock.

    Returns:
        The consumer, not yet run.
    """
    clock = FakeClock(MIDNIGHT)
    consumer: IntakeConsumer

    def tick() -> None:
        clock.now += timedelta(hours=2)
        if on_poll is not None:
            on_poll(clock)

    adapter = FakeAdapter(
        FakeStream("babamul", []), on_poll=tick, on_exhausted=lambda: consumer.stop()
    )
    consumer = IntakeConsumer(
        [adapter],
        THRESHOLDS,
        intake(),
        poll_timeout=0.0,
        clock=clock,
        instance=instance,
    )
    return consumer


class HousekeepingTests(TransactionTestCase):
    """The sweep and the silence check, with several consumers."""

    def test_only_the_lock_holder_sweeps_and_checks_silence(self) -> None:
        """Consumers B and A both find Babamul silent for two hours of the
        night. B takes the housekeeping lock for its sweep and holds it
        while A runs a whole cycle: A neither sweeps nor records a gap. B
        then finishes, checks silence and records the one gap."""
        sweeps: list[str] = []
        b_holding = threading.Event()
        a_done = threading.Event()
        errors: list[BaseException] = []

        def sweep(now: datetime) -> None:
            sweeps.append(threading.current_thread().name)
            if threading.current_thread().name == "consumer-b":
                b_holding.set()
                if not a_done.wait(WAIT_SECONDS):
                    raise AssertionError("consumer A never finished")

        def run_b() -> None:
            try:
                silent_babamul_consumer("host-b-2").run()
            except BaseException as error:  # reported in the main thread
                errors.append(error)
            finally:
                connection.close()

        with mock.patch(
            "seldon.management.commands.consume_alerts.sweep_pending",
            side_effect=sweep,
        ):
            b = threading.Thread(target=run_b, name="consumer-b")
            b.start()
            self.assertTrue(b_holding.wait(WAIT_SECONDS))
            silent_babamul_consumer("host-a-1").run()
            gaps_while_b_held = StreamGap.objects.count()
            with self.assertLogs("seldon.consumer", level="WARNING"):
                a_done.set()
                b.join(WAIT_SECONDS)
        self.assertFalse(b.is_alive())
        self.assertEqual(errors, [])

        self.assertEqual(gaps_while_b_held, 0)
        self.assertEqual(sweeps, ["consumer-b"])
        gap = StreamGap.objects.get()
        self.assertEqual(
            (gap.broker, gap.kind, gap.started_at, gap.ended_at),
            (
                "babamul",
                StreamGap.Kind.SUSPECTED,
                MIDNIGHT,
                MIDNIGHT + timedelta(hours=2),
            ),
        )

    def test_holder_without_babamul_partitions_reads_another_commit(self) -> None:
        """This consumer holds none of Babamul's partitions and hears nothing
        from it for two hours of the night, while another instance commits
        a Babamul batch five minutes before the check. No Babamul gap is
        recorded."""

        def other_commits(clock: FakeClock) -> None:
            ConsumerLastCommit.record(
                "babamul", "host-b-2", clock.now - timedelta(minutes=5)
            )

        silent_babamul_consumer("host-a-1", on_poll=other_commits).run()

        self.assertFalse(StreamGap.objects.exists())

    def test_batch_records_its_instance_commit_time(self) -> None:
        """A batch stored for instance host-a-1 sets that instance's Babamul
        last-commit time to the clock's time as the batch commits."""
        committed = MIDNIGHT + timedelta(minutes=7)

        result = process_batch(
            [delivery(0, "babamul")],
            THRESHOLDS,
            clock=lambda: committed,
            instance="host-a-1",
        )

        self.assertTrue(result.acknowledge)
        row = ConsumerLastCommit.objects.get()
        self.assertEqual(
            (row.broker, row.instance, row.committed_at),
            ("babamul", "host-a-1", committed),
        )


class MigrationLockTests(TransactionTestCase):
    """Replicas starting together migrate one at a time."""

    def test_migrate_waits_while_another_process_holds_the_lock(self) -> None:
        """Another process holds the migration lock. ``migrate_locked`` in a
        second process waits for it, and finishes once it is released."""
        finished = threading.Event()
        errors: list[BaseException] = []

        def migrate() -> None:
            try:
                call_command("migrate_locked", verbosity=0)
                finished.set()
            except BaseException as error:  # reported in the main thread
                errors.append(error)
            finally:
                connection.close()

        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_lock(%s)", [MIGRATE_LOCK_KEY])
        thread = threading.Thread(target=migrate, name="replica-2")
        thread.start()
        try:
            wait_for_lock_waiter()
            self.assertFalse(finished.is_set())
        finally:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_unlock(%s)", [MIGRATE_LOCK_KEY])
        thread.join(WAIT_SECONDS)

        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertTrue(finished.is_set())
