"""Wall time spent in each stage of intake, to show where an alert's time goes.

A ``StageTimer`` accumulates monotonic wall time per stage, plus counts of
alerts, broker commits and forecasts. Timing is always on: a stage costs
two clock reads.

Stages nest, and each moment is counted once, under the innermost stage
open at that moment. So forecasting, timed inside the engine's work on a
delivery, is counted under forecasting and not under engine database work.
Whatever no stage covers (loop bookkeeping, sweeps, silence checks, backoff
waits) is reported as other loop overhead, so the stages' shares of the
elapsed time sum to one.

Network fetch is not a stage: inside a live consumer it is hidden in the
poll wait, and a replay has none.
"""

import logging
import time
from collections.abc import Callable
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass
from enum import StrEnum
from types import TracebackType

logger = logging.getLogger("seldon.intake.timing")

#: How often the consumer logs the timing summary, in seconds.
DEFAULT_TIMING_SECONDS = 300.0


class Stage(StrEnum):
    """Where intake time is spent."""

    POLL_WAIT = "poll wait"
    """Inside the Kafka consumer's ``consume`` call."""

    DECODE = "decode"
    """Turning a message's bytes into alert records."""

    PARSE = "parse"
    """Turning an alert record into a delivery or a skip."""

    DATABASE = "engine database"
    """Storing a polled alert's work, forecasting excluded."""

    FORECAST = "forecasting"
    """The model call for each owed detection."""

    COMMIT = "broker commit"
    """The adapter's commit call."""

    OTHER = "other"
    """Elapsed time in no stage above."""


#: The stages a timer measures directly; ``Stage.OTHER`` is the remainder.
MEASURED = tuple(stage for stage in Stage if stage is not Stage.OTHER)


@dataclass(frozen=True)
class TimingSummary:
    """Each stage's time over an elapsed period.

    Attributes:
        elapsed_seconds: Wall time since the timer started.
        seconds: Each stage's total, other loop overhead included.
        alerts: Alerts acknowledged.
        commits: Broker commit calls.
        forecasts: Model calls.
    """

    elapsed_seconds: float
    seconds: dict[Stage, float]
    alerts: int
    commits: int
    forecasts: int

    def share(self, stage: Stage) -> float:
        """A stage's fraction of the elapsed time.

        Args:
            stage: The stage.

        Returns:
            Its share; 0 when no time has passed.
        """
        if self.elapsed_seconds <= 0:
            return 0.0
        return self.seconds[stage] / self.elapsed_seconds

    def ms_per_alert(self, stage: Stage) -> float | None:
        """A stage's milliseconds for each alert acknowledged.

        Args:
            stage: The stage.

        Returns:
            The cost an alert, or ``None`` before any alert.
        """
        if not self.alerts:
            return None
        return 1000 * self.seconds[stage] / self.alerts

    def lines(self) -> list[str]:
        """The summary as text.

        Returns:
            A header line, then one line per stage.
        """
        out = [
            f"Intake timing over {self.elapsed_seconds:.1f} s: {self.alerts} alerts, "
            f"{self.commits} commits, {self.forecasts} forecasts"
        ]
        for stage in Stage:
            line = (
                f"  {stage.value}: {self.seconds[stage]:.3f} s, "
                f"{100 * self.share(stage):.1f}%"
            )
            per_alert = self.ms_per_alert(stage)
            if per_alert is not None:
                line += f", {per_alert:.3f} ms an alert"
            out.append(line)
        return out


class _Span:
    """One open stage; its inner stages' time is taken off its own."""

    __slots__ = ("_timer", "_stage", "_started", "inner")

    def __init__(self, timer: "StageTimer", stage: Stage) -> None:
        """Prepare to time a stage.

        Args:
            timer: The timer to add to.
            stage: The stage.
        """
        self._timer = timer
        self._stage = stage
        self._started = 0.0
        self.inner = 0.0

    def __enter__(self) -> None:
        """Start the stage."""
        self._started = self._timer._monotonic()
        self._timer._open.append(self)

    def __exit__(
        self,
        kind: type[BaseException] | None,
        error: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """End the stage, whether or not it raised.

        Args:
            kind: The exception's type, if one was raised.
            error: The exception, if one was raised.
            traceback: Its traceback.
        """
        timer = self._timer
        elapsed = timer._monotonic() - self._started
        timer._open.pop()
        timer._totals[self._stage] += elapsed - self.inner
        if timer._open:
            timer._open[-1].inner += elapsed


class StageTimer:
    """Accumulates wall time per intake stage."""

    def __init__(self, monotonic: Callable[[], float] = time.monotonic) -> None:
        """Start timing now.

        Args:
            monotonic: Wall seconds, as ``time.monotonic``.
        """
        self._monotonic = monotonic
        self.restart()

    def restart(self) -> None:
        """Forget everything and start timing from now."""
        self._origin = self._monotonic()
        self._last_logged = self._origin
        self._totals = dict.fromkeys(MEASURED, 0.0)
        self._open: list[_Span] = []
        self._alerts = 0
        self._commits = 0
        self._forecasts = 0

    def stage(self, stage: Stage) -> _Span:
        """Time a stage, as a context manager.

        Args:
            stage: The stage; not ``Stage.OTHER``.

        Returns:
            The context manager.
        """
        return _Span(self, stage)

    def count_alert(self) -> None:
        """Count one alert acknowledged."""
        self._alerts += 1

    def count_commit(self) -> None:
        """Count one broker commit call."""
        self._commits += 1

    def count_forecast(self) -> None:
        """Count one model call."""
        self._forecasts += 1

    def summary(self) -> TimingSummary:
        """The totals so far.

        Returns:
            Each stage's time, other loop overhead included, and the counts.
        """
        elapsed = self._monotonic() - self._origin
        seconds = dict(self._totals)
        seconds[Stage.OTHER] = elapsed - sum(self._totals.values())
        return TimingSummary(
            elapsed_seconds=elapsed,
            seconds=seconds,
            alerts=self._alerts,
            commits=self._commits,
            forecasts=self._forecasts,
        )

    def log_summary(self) -> TimingSummary:
        """Log the totals so far.

        Returns:
            The summary logged.
        """
        summary = self.summary()
        self._last_logged = self._monotonic()
        for line in summary.lines():
            logger.info("%s", line)
        return summary

    def log_if_due(self, interval: float) -> None:
        """Log the totals if ``interval`` seconds passed since the last log.

        Args:
            interval: Seconds between summaries.
        """
        if self._monotonic() - self._last_logged >= interval:
            self.log_summary()


def timed(timer: StageTimer | None, stage: Stage) -> AbstractContextManager[None]:
    """Time a stage when there is a timer.

    Args:
        timer: The timer, or ``None`` to time nothing.
        stage: The stage.

    Returns:
        A context manager.
    """
    return nullcontext() if timer is None else timer.stage(stage)
