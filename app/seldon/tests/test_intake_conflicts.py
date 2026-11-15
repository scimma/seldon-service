"""Tests for how batch intake treats concurrent-write conflicts.

A conflict with another consumer process is retried a bounded number of
times; one that keeps recurring, like any other database error, leaves the
batch unacknowledged.
"""

from unittest import mock

from django.db import DatabaseError
from django.test import SimpleTestCase, TestCase

from seldon.intake import engine
from seldon.intake.engine import (
    CONFLICT_ATTEMPTS,
    is_concurrency_conflict,
    process_batch,
)
from seldon.tests.test_consume_alerts import THRESHOLDS, made_up_delivery


class _PostgresError(Exception):
    """A driver error carrying a Postgres SQLSTATE, as psycopg's does."""

    def __init__(self, sqlstate: str) -> None:
        """Hold the code.

        Args:
            sqlstate: The five-character Postgres error code.
        """
        super().__init__(sqlstate)
        self.sqlstate = sqlstate


def _database_error(sqlstate: str | None) -> DatabaseError:
    """A Django database error raised from a driver error.

    Args:
        sqlstate: The driver error's SQLSTATE, or ``None`` for an error with
            no driver cause.

    Returns:
        The error, with its cause set as Django sets it.
    """
    error = DatabaseError("simulated")
    if sqlstate is not None:
        error.__cause__ = _PostgresError(sqlstate)
    return error


class ConflictClassifierTests(SimpleTestCase):
    """Which database errors count as another process's concurrent work."""

    def test_conflict_codes(self) -> None:
        """Deadlock, serialization failure and unique violation are
        conflicts; any other code, or no code, is not."""
        cases = {
            "40P01": True,
            "40001": True,
            "23505": True,
            "23503": False,
            "53300": False,
            None: False,
        }
        for sqlstate, expected in cases.items():
            with self.subTest(sqlstate=sqlstate):
                self.assertIs(
                    is_concurrency_conflict(_database_error(sqlstate)), expected
                )


class ConflictRetryTests(TestCase):
    """A batch's retries on conflict are bounded."""

    def test_recurring_conflict_leaves_the_batch_unacknowledged(self) -> None:
        """A deadlock on every attempt is retried until CONFLICT_ATTEMPTS
        attempts have been made, then the batch is left unacknowledged."""
        failing = mock.Mock(side_effect=_database_error("40P01"))
        with mock.patch.object(engine, "_process_one", failing):
            with self.assertLogs("seldon", level="WARNING") as logs:
                result = process_batch([made_up_delivery(0)], THRESHOLDS)

        self.assertFalse(result.acknowledge)
        self.assertEqual(result.results, ())
        self.assertEqual(failing.call_count, CONFLICT_ATTEMPTS)
        self.assertTrue(
            any("leaving it unacknowledged" in line for line in logs.output)
        )

    def test_other_database_error_is_not_retried(self) -> None:
        """A database error that is not a conflict leaves the batch
        unacknowledged after one attempt."""
        failing = mock.Mock(side_effect=_database_error("53300"))
        with mock.patch.object(engine, "_process_one", failing):
            with self.assertLogs("seldon", level="ERROR"):
                result = process_batch([made_up_delivery(0)], THRESHOLDS)

        self.assertFalse(result.acknowledge)
        self.assertEqual(failing.call_count, 1)
