"""Tests for the intake-lag record: publication-to-commit lag per broker and night.

Each newly stored alert adds one count under its broker, its publication
night (12:00 to 12:00 UTC, named by the start date), and its lag in whole
seconds from publication to the commit of its work. These run against the
compose ``db`` service and the real checkpoint.

Every expected bucket is worked out here from the publication and commit
times each test sets, never from the code under test.
"""

from collections.abc import Callable, Sequence
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta

from django.db import transaction
from django.test import TestCase

from seldon.domain.intake.delivery import BrokerRole, Delivery, Point
from seldon.domain.intake.qualification import Thresholds
from seldon.infrastructure.brokers.base import (
    BrokerAdapter,
    IntakeSkipReport,
    Polled,
    StreamGapReport,
)
from seldon.intake.engine import IntakeLagTally, process_delivery, sweep_pending
from seldon.management.commands.consume_alerts import IntakeConsumer
from seldon.models import Detection, IntakeLagCount, IntakeSkip
from seldon.tests.test_consume_alerts import FakeClock, intake

THRESHOLDS = {
    "fink": Thresholds(reliability_cut=0.6, sn_like_threshold=0.5),
    "lasair": Thresholds(reliability_cut=0.7, sn_like_threshold=0.4),
}
FIRST_MJD = 61000.25
OBJECT_ID = 424242424200

#: Published one minute before midnight UTC: the night of 12 November.
LATE = datetime(2026, 11, 12, 23, 59, tzinfo=UTC)
LATE_NIGHT = date(2026, 11, 12)


def clock_at(moment: datetime) -> Callable[[], datetime]:
    """A clock stopped at one moment.

    Args:
        moment: The time it reads.

    Returns:
        The clock.
    """
    return lambda: moment


def babamul(
    object_id: int,
    published: datetime | None,
    received: datetime,
    nights: int = 1,
) -> Delivery:
    """A Babamul (full-stream) delivery, which leaves its detection pending.

    Args:
        object_id: The object's ``diaObjectId``; its detection's
            ``diaSourceId`` is ``object_id * 100 + nights - 1``.
        published: When Babamul published it, or ``None``.
        received: When the service received it.
        nights: How many nightly points it carries, the last being the
            detection.

    Returns:
        The delivery.
    """
    points = [
        Point(
            observed_mjd=FIRST_MJD + n,
            band="r",
            flux=1000.0 + 400.0 * n,
            flux_err=50.0,
            dia_source_id=object_id * 100 + n,
        )
        for n in range(nights)
    ]
    return Delivery(
        dia_object_id=object_id,
        detection=points[-1],
        previous=tuple(points[:-1]),
        broker="babamul",
        role=BrokerRole.FULL_STREAM,
        reliability=None,
        sn_like_score=None,
        published_at=published,
        received_at=received,
        schema_version="test-1",
    )


def buckets() -> dict[tuple[str, date, int | None], int]:
    """Every stored lag count.

    Returns:
        (broker, night, lag in seconds or ``None``) to its count.
    """
    return {
        (row.broker, row.night, row.lag_seconds): row.count
        for row in IntakeLagCount.objects.all()
    }


class ListAdapter(BrokerAdapter):
    """An adapter that returns a fixed list of polled alerts once."""

    def __init__(self, name: str, items: list[Polled], on_done: Callable) -> None:
        """Hold the alerts.

        Args:
            name: The broker name.
            items: What the first poll returns.
            on_done: Called on the first poll that finds nothing left.
        """
        self.name = name
        self.role = BrokerRole.FULL_STREAM
        self._items = list(items)
        self._on_done = on_done
        self.acked: list[Polled] = []

    def poll(self, timeout: float, max_messages: int = 100) -> list[Polled]:
        """Return the alerts once, then nothing.

        Args:
            timeout: Ignored.
            max_messages: Ignored.

        Returns:
            The alerts on the first call; empty afterwards.
        """
        found, self._items = self._items, []
        if not found:
            self._on_done()
        return found

    def ack(self, batch: Sequence[Polled]) -> None:
        """Note the acknowledgement.

        Args:
            batch: The alerts.
        """
        self.acked.extend(batch)

    def gaps(self) -> list[StreamGapReport]:
        """No gaps.

        Returns:
            Nothing.
        """
        return []

    def close(self) -> None:
        """Nothing to release."""


class LagRecordTests(TestCase):
    """What the engine counts, and under which bucket."""

    def test_lags_count_per_whole_second_under_the_publication_night(self) -> None:
        """Three Babamul alerts published at 23:59 UTC on 12 November commit
        4.2 s, 4.9 s, and 70 s later: two counts at 4 s and one at 70 s,
        under the night of 12 November."""
        for i, lag in enumerate((4.2, 4.9, 70.0)):
            committed = LATE + timedelta(seconds=lag)
            result = process_delivery(
                babamul(OBJECT_ID + i, LATE, LATE + timedelta(seconds=1)),
                THRESHOLDS,
                clock=clock_at(committed),
            )
            self.assertTrue(result.acknowledge)

        self.assertEqual(
            buckets(),
            {("babamul", LATE_NIGHT, 4): 2, ("babamul", LATE_NIGHT, 70): 1},
        )

    def test_publication_at_noon_starts_the_new_night(self) -> None:
        """An alert published at 11:59:59 UTC on 13 November belongs to the
        night of 12 November; one published at 12:00:00 to the night of 13
        November. Both commit 10 s after publication."""
        for i, published in enumerate(
            (
                datetime(2026, 11, 13, 11, 59, 59, tzinfo=UTC),
                datetime(2026, 11, 13, 12, 0, 0, tzinfo=UTC),
            )
        ):
            process_delivery(
                babamul(OBJECT_ID + i, published, published),
                THRESHOLDS,
                clock=clock_at(published + timedelta(seconds=10)),
            )

        self.assertEqual(
            buckets(),
            {
                ("babamul", date(2026, 11, 12), 10): 1,
                ("babamul", date(2026, 11, 13), 10): 1,
            },
        )

    def test_lag_of_two_hours_lands_in_the_overflow_bucket(self) -> None:
        """An alert committed two hours after publication is counted in the
        one shared bucket for lags of one hour or more, stored as 3600 s."""
        process_delivery(
            babamul(OBJECT_ID, LATE, LATE),
            THRESHOLDS,
            clock=clock_at(LATE + timedelta(hours=2)),
        )

        self.assertEqual(buckets(), {("babamul", LATE_NIGHT, 3600): 1})
        self.assertEqual(IntakeLagCount.OVERFLOW_SECONDS, 3600)

    def test_alert_without_publication_time_counts_under_its_receipt_night(
        self,
    ) -> None:
        """An alert with no publication time, received at 11:00 UTC on 13
        November, is counted in the unknown-publication bucket (no lag) of
        the night of 12 November."""
        received = datetime(2026, 11, 13, 11, 0, tzinfo=UTC)
        process_delivery(
            babamul(OBJECT_ID, None, received),
            THRESHOLDS,
            clock=clock_at(received + timedelta(seconds=3)),
        )

        self.assertEqual(buckets(), {("babamul", date(2026, 11, 12), None): 1})

    def test_redelivery_of_stored_work_adds_no_count(self) -> None:
        """The same Babamul alert is processed twice, the second time an hour
        later: its delivery is stored once and counted once, at 5 s."""
        alert = babamul(OBJECT_ID, LATE, LATE)
        process_delivery(alert, THRESHOLDS, clock=clock_at(LATE + timedelta(seconds=5)))
        process_delivery(alert, THRESHOLDS, clock=clock_at(LATE + timedelta(hours=1)))

        self.assertEqual(buckets(), {("babamul", LATE_NIGHT, 5): 1})

    def test_forecast_alert_is_counted(self) -> None:
        """A Fink alert above Fink's thresholds is forecast, and counted once
        under its publication night at 8 s."""
        alert = Delivery(
            dia_object_id=OBJECT_ID,
            detection=Point(
                observed_mjd=FIRST_MJD,
                band="r",
                flux=1000.0,
                flux_err=50.0,
                dia_source_id=OBJECT_ID * 100,
            ),
            previous=(),
            broker="fink",
            role=BrokerRole.QUALIFYING,
            reliability=0.9,
            sn_like_score=0.9,
            published_at=LATE,
            received_at=LATE,
            schema_version="test-1",
        )

        result = process_delivery(
            alert, THRESHOLDS, clock=clock_at(LATE + timedelta(seconds=8))
        )

        self.assertEqual(result.forecast, (OBJECT_ID * 100,))
        self.assertEqual(buckets(), {("fink", LATE_NIGHT, 8): 1})

    def test_engine_intake_skip_is_counted_under_its_publication_night(
        self,
    ) -> None:
        """A Babamul alert whose light curve spans 500 nights cannot become a
        request: it is an intake skip, counted under its publication night
        at 2 s."""
        alert = replace(
            babamul(OBJECT_ID, LATE, LATE),
            previous=(
                Point(
                    observed_mjd=FIRST_MJD - 500,
                    band="r",
                    flux=900.0,
                    flux_err=50.0,
                    dia_source_id=OBJECT_ID * 100 + 99,
                ),
            ),
        )

        result = process_delivery(
            alert, THRESHOLDS, clock=clock_at(LATE + timedelta(seconds=2))
        )

        self.assertEqual(result.skip_reason, "light_curve_span")
        self.assertEqual(buckets(), {("babamul", LATE_NIGHT, 2): 1})

    def test_rolled_back_transaction_leaves_no_counts(self) -> None:
        """Counts flushed inside a transaction that then fails are rolled
        back with it."""
        tally = IntakeLagTally()
        with self.assertRaises(RuntimeError), transaction.atomic():
            tally.add("babamul", LATE, LATE)
            tally.flush(LATE + timedelta(seconds=4))
            self.assertEqual(buckets(), {("babamul", LATE_NIGHT, 4): 1})
            raise RuntimeError("work failed after the counts were applied")

        self.assertEqual(buckets(), {})

    def test_tally_adds_to_existing_counts(self) -> None:
        """Two flushes onto the same bucket add up: 2 then 1 make 3."""
        tally = IntakeLagTally()
        with transaction.atomic():
            tally.add("babamul", LATE, LATE)
            tally.add("babamul", LATE, LATE)
            tally.flush(LATE + timedelta(seconds=4))
        with transaction.atomic():
            tally.add("babamul", LATE, LATE)
            tally.flush(LATE + timedelta(seconds=4))

        self.assertEqual(buckets(), {("babamul", LATE_NIGHT, 4): 3})

    def test_sweep_of_unqualified_deliveries_keeps_the_counts(self) -> None:
        """A Babamul alert's object never qualifies. Seven days and an hour
        later the sweep discards its detection and delivery; its lag count
        stays."""
        process_delivery(
            babamul(OBJECT_ID, LATE, LATE),
            THRESHOLDS,
            clock=clock_at(LATE + timedelta(seconds=6)),
        )

        swept = sweep_pending(LATE + timedelta(days=7, hours=1))

        self.assertEqual(swept.discarded, 1)
        self.assertFalse(Detection.objects.exists())
        self.assertEqual(buckets(), {("babamul", LATE_NIGHT, 6): 1})


class ConsumerLagTests(TestCase):
    """The consumer counts with its own clock, skips included."""

    def test_consumer_counts_deliveries_and_skips_with_its_clock(self) -> None:
        """The consumer's clock reads 23:59:30. A Babamul delivery published
        at 23:59:00 and a skip published at 23:59:10 (received at 23:59:20)
        count 30 s and 20 s under the night of 12 November; a skip with no
        publication time counts in that night's unknown-publication bucket."""
        clock = FakeClock(LATE + timedelta(seconds=30))
        received = LATE + timedelta(seconds=20)
        items = [
            Polled(outcome=babamul(OBJECT_ID, LATE, received), receipt=0),
            Polled(
                outcome=IntakeSkipReport(
                    broker="babamul",
                    reason="missing diaSource",
                    occurred_at=received,
                    raw_identifier="a1",
                    published_at=LATE + timedelta(seconds=10),
                ),
                receipt=1,
            ),
            Polled(
                outcome=IntakeSkipReport(
                    broker="babamul",
                    reason="missing diaSource",
                    occurred_at=received,
                    raw_identifier="a2",
                ),
                receipt=2,
            ),
        ]
        consumer: IntakeConsumer
        adapter = ListAdapter("babamul", items, on_done=lambda: consumer.stop())
        consumer = IntakeConsumer(
            [adapter], THRESHOLDS, intake(), poll_timeout=0.0, clock=clock
        )

        with self.assertLogs("seldon", level="WARNING"):
            consumer.run()

        self.assertEqual(len(adapter.acked), 3)
        self.assertEqual(IntakeSkip.objects.count(), 2)
        self.assertEqual(
            buckets(),
            {
                ("babamul", LATE_NIGHT, 30): 1,
                ("babamul", LATE_NIGHT, 20): 1,
                ("babamul", LATE_NIGHT, None): 1,
            },
        )
