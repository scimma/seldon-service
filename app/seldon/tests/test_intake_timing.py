"""Tests for per-stage intake timing.

Wall time is a fake that only the test moves: each faked stage advances it
by a fixed amount, so every stage's expected total, share of the elapsed
time and per-alert cost is worked out here from those amounts, never from
the code under test. The Kafka adapter runs over the fake consumer from the
adapter tests; the consumer loop over the fake adapter from the consumer
tests, with the real engine, database and checkpoint.
"""

import io
import os
import tempfile
from datetime import timedelta
from pathlib import Path
from typing import Any
from unittest import mock

from confluent_kafka import TopicPartition
from django.core.management import call_command
from django.db import connection
from django.test import SimpleTestCase, TestCase

from seldon import services
from seldon.infrastructure.brokers import kafka as kafka_module
from seldon.intake.timing import Stage, StageTimer, TimingSummary
from seldon.management.commands import consume_alerts
from seldon.management.commands.consume_alerts import IntakeConsumer
from seldon.management.commands.replay_alerts import ReplaySummary, run_replay
from seldon.replay.source import ReplayMode
from seldon.tests.fixtures.broker_alerts import lasair
from seldon.tests.test_broker_adapters import (
    RECEIVED as ADAPTER_RECEIVED,
)
from seldon.tests.test_broker_adapters import (
    FakeConsumer,
    _lasair_messages,
    _lasair_settings,
    _ms,
)
from seldon.tests.test_consume_alerts import (
    THRESHOLDS,
    FakeAdapter,
    FakeStream,
    intake,
)
from seldon.tests.test_replay import (
    REPLAY_ENV,
    T0,
    FakeWall,
    babamul_alert,
    babamul_bytes,
    message,
    replay_of,
    write_recording,
)


class FakeMonotonic:
    """Wall seconds that move only when the test says."""

    def __init__(self) -> None:
        """Start at zero."""
        self.now = 0.0

    def __call__(self) -> float:
        """The current wall time.

        Returns:
            Seconds since the fake started.
        """
        return self.now

    def advance(self, seconds: float) -> None:
        """Move the time on.

        Args:
            seconds: How far.
        """
        self.now += seconds


class TimedConsumer(FakeConsumer):
    """The adapter tests' fake consumer, taking fixed wall time per call."""

    def __init__(self, wall: FakeMonotonic, *args: Any, **kwargs: Any) -> None:
        """Set up the fake partition on fake wall time.

        Args:
            wall: The fake wall time.
            *args: As ``FakeConsumer``.
            **kwargs: As ``FakeConsumer``.
        """
        super().__init__(*args, **kwargs)
        self.wall = wall

    def consume(self, num_messages: int = 1, timeout: float = -1) -> list[Any]:
        """Wait 2 s, then hand out messages.

        Args:
            num_messages: Most messages.
            timeout: Ignored.

        Returns:
            The messages.
        """
        self.wall.advance(2.0)
        return super().consume(num_messages, timeout)

    def commit(
        self,
        message: Any = None,
        offsets: list[TopicPartition] | None = None,
        asynchronous: bool = True,
    ) -> None:
        """Take 0.125 s, then record the commit.

        Args:
            message: Unused.
            offsets: The offsets.
            asynchronous: Ignored.
        """
        self.wall.advance(0.125)
        super().commit(message, offsets, asynchronous)


def adapter_over(
    consumer: FakeConsumer, timer: StageTimer
) -> kafka_module.KafkaBrokerAdapter:
    """A Lasair adapter over a fake consumer, timed by ``timer``.

    Args:
        consumer: The fake consumer.
        timer: The stage timer.

    Returns:
        The adapter.
    """
    return kafka_module.KafkaBrokerAdapter(
        _lasair_settings(),
        consumer=consumer,
        clock=lambda: ADAPTER_RECEIVED,
        timer=timer,
    )


class StageTimerTests(SimpleTestCase):
    """The timer's totals, shares and per-alert costs."""

    def test_summary_reports_each_stage_total_share_and_per_alert_cost(
        self,
    ) -> None:
        """Over 10 s: 4 s of poll wait, a 3 s database stage holding 1 s of
        forecasting, 0.5 s of commit, and 2.5 s in no stage. With two alerts
        the summary gives database 2 s (20%), forecasting 1 s (10%), poll
        wait 4 s (40%, 2,000 ms an alert), commit 0.5 s (5%), other 2.5 s
        (25%), and the shares sum to one."""
        wall = FakeMonotonic()
        timer = StageTimer(wall)
        timer.restart()
        with timer.stage(Stage.POLL_WAIT):
            wall.advance(4.0)
        with timer.stage(Stage.DATABASE):
            wall.advance(1.5)
            with timer.stage(Stage.FORECAST):
                wall.advance(1.0)
            timer.count_forecast()
            wall.advance(0.5)
        with timer.stage(Stage.COMMIT):
            wall.advance(0.5)
        timer.count_commit()
        timer.count_alert()
        timer.count_alert()
        wall.advance(2.5)

        summary = timer.summary()

        self.assertEqual(summary.elapsed_seconds, 10.0)
        self.assertEqual(
            summary.seconds,
            {
                Stage.POLL_WAIT: 4.0,
                Stage.DECODE: 0.0,
                Stage.PARSE: 0.0,
                Stage.DATABASE: 2.0,
                Stage.FORECAST: 1.0,
                Stage.COMMIT: 0.5,
                Stage.OTHER: 2.5,
            },
        )
        self.assertEqual(summary.share(Stage.POLL_WAIT), 0.4)
        self.assertEqual(summary.share(Stage.DATABASE), 0.2)
        self.assertEqual(summary.share(Stage.OTHER), 0.25)
        self.assertAlmostEqual(sum(summary.share(s) for s in Stage), 1.0)
        self.assertEqual(summary.ms_per_alert(Stage.POLL_WAIT), 2000.0)
        self.assertEqual(
            (summary.alerts, summary.commits, summary.forecasts), (2, 1, 1)
        )
        lines = summary.lines()
        self.assertIn("10.0 s", lines[0])
        self.assertIn("2 alerts", lines[0])
        self.assertIn("  poll wait: 4.000 s, 40.0%, 2000.000 ms an alert", lines)


class AdapterTimingTests(SimpleTestCase):
    """The Kafka adapter times its consume, decode, parse and commit calls."""

    def test_poll_and_ack_stages_take_the_injected_amounts(self) -> None:
        """One poll of the two recorded Lasair messages: consume takes 2 s,
        each decode 0.25 s, each parse 0.5 s, and each of the two commits
        0.125 s; 1 s passes outside any stage. Over 4.75 s the summary gives
        poll wait 2 s, decode 0.5 s, parse 1 s, commit 0.25 s for two
        commits, and the remaining 1 s as other."""
        wall = FakeMonotonic()
        timer = StageTimer(wall)
        consumer = TimedConsumer(wall, _lasair_messages(0, _ms(ADAPTER_RECEIVED)))
        real_decode = kafka_module.parser_for("lasair").decode
        real_parse = kafka_module.parse_alert

        def slow_decode(key: Any, value: Any) -> Any:
            wall.advance(0.25)
            return real_decode(key, value)

        def slow_parse(*args: Any, **kwargs: Any) -> Any:
            wall.advance(0.5)
            return real_parse(*args, **kwargs)

        with (
            mock.patch(
                "seldon.infrastructure.brokers.parsers.lasair.decode", slow_decode
            ),
            mock.patch.object(kafka_module, "parse_alert", slow_parse),
        ):
            adapter = adapter_over(consumer, timer)
            timer.restart()
            polled = adapter.poll(timeout=0.1, max_messages=10)
            wall.advance(1.0)
            for item in polled:
                adapter.ack(item)

        self.assertEqual(len(polled), 2)
        summary = timer.summary()
        self.assertEqual(summary.elapsed_seconds, 4.75)
        self.assertEqual(summary.seconds[Stage.POLL_WAIT], 2.0)
        self.assertEqual(summary.seconds[Stage.DECODE], 0.5)
        self.assertEqual(summary.seconds[Stage.PARSE], 1.0)
        self.assertEqual(summary.seconds[Stage.COMMIT], 0.25)
        self.assertEqual(summary.seconds[Stage.OTHER], 1.0)
        self.assertEqual(summary.share(Stage.POLL_WAIT), 2.0 / 4.75)
        self.assertEqual(summary.commits, 2)

    def test_multi_record_message_is_one_timed_commit(self) -> None:
        """One message decoding to two alerts, both acknowledged, is one
        commit call: one 0.125 s commit, counted once."""
        wall = FakeMonotonic()
        timer = StageTimer(wall)
        consumer = TimedConsumer(wall, _lasair_messages(40, _ms(ADAPTER_RECEIVED))[:1])
        two_alerts = [lasair.ALERTS[0]["alert"], lasair.ALERTS[1]["alert"]]
        with mock.patch(
            "seldon.infrastructure.brokers.parsers.lasair.decode",
            return_value=two_alerts,
        ):
            adapter = adapter_over(consumer, timer)
            polled = adapter.poll(timeout=0.1, max_messages=10)
        for item in polled:
            adapter.ack(item)

        self.assertEqual(len(polled), 2)
        self.assertEqual(len(consumer.commits), 1)
        summary = timer.summary()
        self.assertEqual(summary.commits, 1)
        self.assertEqual(summary.seconds[Stage.COMMIT], 0.125)


class ConsumerTimingTests(TestCase):
    """The consumer loop times engine work and logs the summary."""

    def test_forecast_time_is_counted_under_forecasting_not_database(self) -> None:
        """The first recorded Lasair alert qualifies and is forecast once.
        Each database query takes 0.01 s and the model call 3 s: forecasting
        is 3 s for one forecast, and engine database work is 0.01 s for each
        query made while the alert's work was stored, with none of the 3 s.
        """
        wall = FakeMonotonic()
        timer = StageTimer(wall)
        handling = {"on": False, "queries": 0}
        real_forecast = services.forecast
        real_process = consume_alerts.process_delivery

        def slow_forecast(requests: Any) -> Any:
            wall.advance(3.0)
            return real_forecast(requests)

        def counted_process(*args: Any, **kwargs: Any) -> Any:
            handling["on"] = True
            try:
                return real_process(*args, **kwargs)
            finally:
                handling["on"] = False

        def slow_queries(execute, sql, params, many, context):  # noqa: ANN001
            wall.advance(0.01)
            if handling["on"]:
                handling["queries"] += 1
            return execute(sql, params, many, context)

        consumer: IntakeConsumer
        adapter = FakeAdapter(
            FakeStream("lasair", lasair.ALERTS[:1]),
            on_exhausted=lambda: consumer.stop(),
        )
        consumer = IntakeConsumer(
            [adapter], THRESHOLDS, intake(), poll_timeout=0.0, timer=timer
        )
        with (
            mock.patch("seldon.intake.engine.services.forecast", slow_forecast),
            mock.patch(
                "seldon.management.commands.consume_alerts.process_delivery",
                counted_process,
            ),
            connection.execute_wrapper(slow_queries),
            self.assertLogs("seldon.intake.timing", level="INFO"),
        ):
            consumer.run()

        summary = timer.summary()
        self.assertEqual(summary.forecasts, 1)
        self.assertEqual(summary.alerts, 1)
        self.assertEqual(summary.seconds[Stage.FORECAST], 3.0)
        self.assertGreater(handling["queries"], 0)
        self.assertAlmostEqual(
            summary.seconds[Stage.DATABASE], 0.01 * handling["queries"]
        )

    def test_summary_is_logged_at_the_interval_and_once_at_shutdown(self) -> None:
        """Each poll of an idle broker takes 100 s and the interval is
        250 s. Stopping during the fifth poll gives a summary after the
        third cycle (300 s elapsed) and one at shutdown (500 s), no more."""
        wall = FakeMonotonic()
        consumer: IntakeConsumer
        adapter = FakeAdapter(
            FakeStream("babamul", []),
            on_poll=lambda: wall.advance(100.0),
        )
        adapter.on_exhausted = lambda: consumer.stop() if adapter.polls == 5 else None
        consumer = IntakeConsumer(
            [adapter],
            THRESHOLDS,
            intake(),
            poll_timeout=0.0,
            timer=StageTimer(wall),
            timing_interval=250.0,
        )
        with self.assertLogs("seldon.intake.timing", level="INFO") as logs:
            consumer.run()

        headers = [
            r.getMessage() for r in logs.records if "Intake timing" in r.getMessage()
        ]
        self.assertEqual(len(headers), 2)
        self.assertIn("over 300.0 s", headers[0])
        self.assertIn("over 500.0 s", headers[1])


@mock.patch.dict(os.environ, REPLAY_ENV)
class ReplayTimingTests(TestCase):
    """A replay reports the breakdown with its rate."""

    def test_replay_summary_carries_the_breakdown(self) -> None:
        """A maximum-rate replay of two Babamul messages beside empty Fink
        and Lasair recordings, on fake wall time: each empty broker's poll
        waits the whole 0.25 s timeout, so the breakdown shows 0.5 s of poll
        wait, two alerts and a commit for each."""
        wall = FakeWall()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_recording(
                root,
                {
                    "babamul": [
                        message("b", 0, i, T0 + timedelta(seconds=i), babamul_bytes(a))
                        for i, a in enumerate([babamul_alert(0), babamul_alert(1)])
                    ]
                },
                empty_topics={"fink": "f", "lasair": "l"},
            )
            summary = run_replay(
                replay_of(root, ReplayMode.MAX, wall), poll_timeout=0.25
            )

        timing = summary.timing
        self.assertIsNotNone(timing)
        self.assertEqual(timing.seconds[Stage.POLL_WAIT], 0.5)
        self.assertEqual(timing.alerts, 2)
        self.assertEqual(timing.commits, 2)
        self.assertEqual(timing.elapsed_seconds, summary.elapsed_seconds)

    def test_command_prints_the_breakdown_after_the_rate(self) -> None:
        """The command writes the alerts-a-second lines, then the breakdown."""
        timing = TimingSummary(
            elapsed_seconds=10.0,
            seconds={stage: 0.0 for stage in Stage} | {Stage.OTHER: 10.0},
            alerts=5,
            commits=5,
            forecasts=0,
        )
        summary = ReplaySummary(
            alerts={"babamul": 5}, elapsed_seconds=10.0, timing=timing
        )
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_recording(root, {"babamul": [message("b", 0, 0, T0)]})
            with (
                mock.patch("seldon.management.commands.replay_alerts.services"),
                mock.patch("seldon.management.commands.replay_alerts.create_database"),
                mock.patch("seldon.management.commands.replay_alerts.use_database"),
                mock.patch(
                    "seldon.management.commands.replay_alerts.run_replay",
                    return_value=summary,
                ),
            ):
                call_command(
                    "replay_alerts",
                    recording=str(root),
                    database="seldon_replay_unused",
                    stdout=out,
                )

        lines = out.getvalue().splitlines()
        self.assertEqual(lines[: len(summary.lines())], summary.lines())
        self.assertEqual(lines[len(summary.lines()) :], timing.lines())
