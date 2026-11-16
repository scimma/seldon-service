"""Tests for forecast workers as separate processes sharing one database.

Each replica is a forked process with its own database connection, so it
can be killed outright, as a container is, and its leases are left to lapse
as they would be in production. These are ``TransactionTestCase`` tests:
the work a test sets up must be committed for the replicas to see it. The
database is the compose ``db`` service.
"""

import multiprocessing
import os
import signal
import time
from collections.abc import Callable, Sequence
from typing import Any

from django.db import connections
from django.test import TransactionTestCase
from seldon_model_contract import Packet

from seldon import deployments
from seldon.forecasting.worker import start
from seldon.models import (
    Detection,
    HeadRefusal,
    OutcomeClaim,
    Product,
    ProductFailure,
)
from seldon.tests.test_forecast_worker import (
    CLASSES,
    DIGEST,
    FLUX,
    StubPackage,
    enable,
    owe,
    worker_settings,
)

#: Longest a test waits for a replica, in seconds.
WAIT_SECONDS = 60.0

#: A lease short enough for a test to wait out.
SHORT_LEASE = 1.0

CONTEXT = multiprocessing.get_context("fork")


def replica(
    package: StubPackage,
    report: Any = None,
    barrier: Any = None,
    **overrides: Any,
) -> None:
    """Run one worker replica until nothing is due; the body of a process.

    Args:
        package: The package it serves.
        report: A queue to put the number of products it stored on.
        barrier: A barrier to wait at before running.
        **overrides: Worker settings to change.
    """
    worker = start(package, worker_settings(poll_seconds=0.05, **overrides))
    if barrier is not None:
        barrier.wait(WAIT_SECONDS)
    totals = worker.run(until_idle=True)
    if report is not None:
        report.put(totals.products)


def spawn(target: Callable[..., None], *args: Any, **kwargs: Any) -> Any:
    """Start ``target`` in a forked process with a database connection of its own.

    The parent's connections are closed first, so the child never shares
    the parent's socket; each side opens its own on first use.

    Args:
        target: The process's work.
        *args: Its positional arguments.
        **kwargs: Its keyword arguments.

    Returns:
        The started process.
    """

    def body() -> None:
        try:
            target(*args, **kwargs)
        finally:
            connections.close_all()

    connections.close_all()
    process = CONTEXT.Process(target=body)
    process.start()
    return process


def finish(process: Any) -> int:
    """Wait for a process to end.

    Args:
        process: The process.

    Returns:
        Its exit code.

    Raises:
        AssertionError: If it is still running after the wait.
    """
    process.join(WAIT_SECONDS)
    if process.is_alive():
        process.kill()
        raise AssertionError("replica did not finish")
    return process.exitcode


def prepare(count: int, heads: Sequence[str] = (CLASSES,)) -> StubPackage:
    """Register the stub package, enable heads, and owe detections.

    Args:
        count: How many detections to owe.
        heads: The heads to enable.

    Returns:
        A stub like the one registered, for the replicas to copy.
    """
    package = StubPackage()
    deployment = deployments.register(package.describe(), DIGEST)
    for head in heads:
        enable(deployment, head)
    owe(count)
    return package


def hold(started: Any, seconds: float) -> Callable[[Sequence[Packet]], None]:
    """A ``predict`` hook that reports it was called, then blocks.

    Args:
        started: An event set when ``predict`` is first called.
        seconds: How long it blocks.

    Returns:
        The hook.
    """

    def blocked(packets: Sequence[Packet]) -> None:
        started.set()
        time.sleep(seconds)

    return blocked


class ReplicaTests(TransactionTestCase):
    """Several replicas of one deployment, run as processes."""

    def assert_one_product_each(self, claims: int, attempts: int) -> None:
        """Every claim holds one product and nothing else.

        Args:
            claims: How many claims there must be.
            attempts: The attempts every claim must show.
        """
        rows = list(OutcomeClaim.objects.all())
        self.assertEqual(len(rows), claims)
        self.assertEqual(
            {(c.state, c.attempts, c.leased_by) for c in rows},
            {("product", attempts, "")},
        )
        self.assertEqual(Product.objects.count(), claims)
        self.assertFalse(HeadRefusal.objects.exists())
        self.assertFalse(ProductFailure.objects.exists())

    def test_two_replicas_drain_the_queue_without_duplicates(self) -> None:
        """Two replicas start together on twelve owed detections and two
        heads, two detections a batch. Every claim gets exactly one product
        on its first attempt, and each replica stores some of them."""
        package = prepare(12, heads=(FLUX, CLASSES))
        package.on_predict = lambda packets: time.sleep(0.05)
        report = CONTEXT.Queue()
        barrier = CONTEXT.Barrier(2)

        processes = [
            spawn(replica, package, report, barrier, batch_size=2) for _ in range(2)
        ]
        codes = [finish(process) for process in processes]

        self.assertEqual(codes, [0, 0])
        stored = sorted(report.get(timeout=WAIT_SECONDS) for _ in processes)
        self.assertEqual(sum(stored), 24)
        self.assertGreater(stored[0], 0)
        self.assertEqual(
            OutcomeClaim.objects.filter(head=FLUX).count(),
            OutcomeClaim.objects.filter(head=CLASSES).count(),
        )
        self.assert_one_product_each(24, attempts=1)

    def test_killed_replica_leaves_nothing_and_another_finishes_its_batch(
        self,
    ) -> None:
        """A replica is killed while ``predict`` runs on three detections.
        Nothing is stored and the claims stay leased to it. Once the leases
        lapse, a second replica stores a product for each, on attempt two."""
        package = prepare(3)
        started = CONTEXT.Event()
        package.on_predict = hold(started, WAIT_SECONDS)

        doomed = spawn(replica, package, lease_seconds=SHORT_LEASE)
        self.assertTrue(started.wait(WAIT_SECONDS))
        doomed.kill()
        self.assertEqual(finish(doomed), -signal.SIGKILL)

        claims = list(OutcomeClaim.objects.all())
        self.assertEqual(
            {(c.state, c.attempts, bool(c.leased_by)) for c in claims},
            {("due", 1, True)},
        )
        self.assertEqual(len(claims), 3)
        self.assertFalse(Product.objects.exists())

        package.on_predict = None
        self.assertEqual(finish(spawn(replica, package)), 0)
        self.assert_one_product_each(3, attempts=2)

    def test_object_that_kills_its_replica_fails_after_three_attempts(self) -> None:
        """One of four detections makes the package kill its process.
        Replicas are started one after another until one finishes: the bad
        detection ends with a failure after three attempts, and the others
        with products."""
        package = prepare(4)
        bad = str(sorted(Detection.objects.values_list("dia_source_id", flat=True))[1])

        def die_on_bad(packets: Sequence[Packet]) -> None:
            if any(packet.object_id == bad for packet in packets):
                os.kill(os.getpid(), signal.SIGKILL)

        package.on_predict = die_on_bad
        codes = []
        for _ in range(6):
            codes.append(finish(spawn(replica, package, lease_seconds=SHORT_LEASE)))
            if codes[-1] == 0:
                break

        self.assertEqual(codes, [-signal.SIGKILL] * 3 + [0])
        failed = OutcomeClaim.objects.get(detection__dia_source_id=int(bad))
        self.assertEqual((failed.state, failed.attempts), ("failure", 3))
        self.assertIn("lapsed", failed.failure.reason)
        others = OutcomeClaim.objects.exclude(pk=failed.pk)
        self.assertEqual({(c.state, c.attempts) for c in others}, {("product", 2)})
        self.assertEqual(Product.objects.count(), 3)

    def test_replica_hung_past_its_lease_is_overtaken(self) -> None:
        """A replica's ``predict`` hangs longer than its lease. A second
        replica takes the lapsed claims and stores them; when the first
        wakes, it stores nothing over them."""
        package = prepare(2)
        started = CONTEXT.Event()
        hung_package = StubPackage(on_predict=hold(started, 4 * SHORT_LEASE))

        hung = spawn(replica, hung_package, lease_seconds=SHORT_LEASE)
        self.assertTrue(started.wait(WAIT_SECONDS))
        self.assertEqual(finish(spawn(replica, package)), 0)
        self.assert_one_product_each(2, attempts=2)
        self.assertEqual(finish(hung), 0)

        self.assert_one_product_each(2, attempts=2)
