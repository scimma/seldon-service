"""Tests for where a capture stops reading a partition.

Producer timestamps need not rise with the offset, so the capture reads on
past the window end and records every message stamped inside the window.
"""

import os
from unittest import mock

from django.test import SimpleTestCase

from seldon.management.commands.capture_alerts import END_SLACK_MS
from seldon.replay.recording import read_messages
from seldon.tests.test_capture_alerts import (
    BROKER_ENV,
    END_MS,
    LASAIR_TOPIC,
    START_MS,
    CaptureAlertsCommandTests,
    FakeMessage,
)


@mock.patch.dict(os.environ, BROKER_ENV, clear=True)
class CaptureWindowEndTests(SimpleTestCase):
    """Out-of-order timestamps around the window end."""

    setUp = CaptureAlertsCommandTests.setUp
    factory = CaptureAlertsCommandTests.factory
    run_command = CaptureAlertsCommandTests.run_command

    def test_in_window_message_after_a_later_stamped_one_is_recorded(
        self,
    ) -> None:
        """A message stamped after the window end is followed, at a later
        offset, by one stamped inside it: the later one is still recorded.
        Reading stops at the first message stamped beyond the slack, so a
        message after that is not read even if stamped inside the window."""
        times = [
            START_MS,
            END_MS + 1_000,
            END_MS - 5,
            END_MS + END_SLACK_MS,
            END_MS - 2,
        ]
        messages = [
            FakeMessage(LASAIR_TOPIC, 0, offset, timestamp, None, bytes([offset]))
            for offset, timestamp in enumerate(times)
        ]
        self.retained["lasair"] = {(LASAIR_TOPIC, 0): messages}

        self.run_command("--brokers", "lasair")

        recorded = list(read_messages(self.output, "lasair"))
        self.assertEqual([m.offset for m in recorded], [0, 2])

    def test_message_stamped_before_the_window_is_not_recorded(self) -> None:
        """A message after the window's first offset but stamped before the
        window start is read and left out."""
        times = [START_MS, START_MS - 1_000, START_MS + 1]
        messages = [
            FakeMessage(LASAIR_TOPIC, 0, offset, timestamp, None, bytes([offset]))
            for offset, timestamp in enumerate(times)
        ]
        self.retained["lasair"] = {(LASAIR_TOPIC, 0): messages}

        self.run_command("--brokers", "lasair")

        recorded = list(read_messages(self.output, "lasair"))
        self.assertEqual([m.offset for m in recorded], [0, 2])
