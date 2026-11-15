"""Tests for the intake timing settings read from the environment."""

from unittest import mock

from django.test import SimpleTestCase
from pydantic import ValidationError

from seldon.config.intake import get_consumer_settings, get_intake_settings


class IntakeSettingsTests(SimpleTestCase):
    """The silence period and night window come from ``SELDON_INTAKE_*``."""

    def test_environment_sets_the_silence_period_and_night_window(self) -> None:
        """Values in the environment replace the defaults."""
        env = {
            "SELDON_INTAKE_SILENCE_MINUTES": "45",
            "SELDON_INTAKE_NIGHT_START_UTC_HOUR": "22",
            "SELDON_INTAKE_NIGHT_END_UTC_HOUR": "10",
        }
        with mock.patch.dict("os.environ", env):
            settings = get_intake_settings()

        self.assertEqual(settings.silence_minutes, 45.0)
        self.assertEqual(settings.night_start_utc_hour, 22)
        self.assertEqual(settings.night_end_utc_hour, 10)

    def test_empty_value_means_the_default(self) -> None:
        """An empty variable leaves the one-hour silence period in place."""
        with mock.patch.dict("os.environ", {"SELDON_INTAKE_SILENCE_MINUTES": ""}):
            self.assertEqual(get_intake_settings().silence_minutes, 60.0)


class IntakeBatchSizeTests(SimpleTestCase):
    """``SELDON_INTAKE_BATCH_SIZE`` sets how many alerts one batch holds."""

    def test_batch_size_defaults_to_one_hundred(self) -> None:
        """With nothing set, a batch holds up to 100 alerts."""
        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertEqual(get_intake_settings().batch_size, 100)

    def test_environment_sets_the_batch_size(self) -> None:
        """A value in the environment replaces the default."""
        with mock.patch.dict("os.environ", {"SELDON_INTAKE_BATCH_SIZE": "250"}):
            self.assertEqual(get_intake_settings().batch_size, 250)

    def test_empty_batch_size_means_the_default(self) -> None:
        """An empty variable leaves the default of 100."""
        with mock.patch.dict("os.environ", {"SELDON_INTAKE_BATCH_SIZE": ""}):
            self.assertEqual(get_intake_settings().batch_size, 100)

    def test_non_positive_batch_size_is_rejected(self) -> None:
        """Zero and negative batch sizes are refused."""
        for value in ("0", "-5"):
            with self.subTest(value=value):
                with mock.patch.dict("os.environ", {"SELDON_INTAKE_BATCH_SIZE": value}):
                    with self.assertRaises(ValidationError):
                        get_intake_settings()


class ConsumerProcessesTests(SimpleTestCase):
    """``SELDON_CONSUMER_PROCESSES`` sets how many consumer processes run."""

    def test_processes_default_to_one(self) -> None:
        """With nothing set, one consumer process runs."""
        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertEqual(get_consumer_settings().processes, 1)

    def test_environment_sets_the_process_count(self) -> None:
        """A value in the environment replaces the default."""
        with mock.patch.dict("os.environ", {"SELDON_CONSUMER_PROCESSES": "3"}):
            self.assertEqual(get_consumer_settings().processes, 3)

    def test_empty_process_count_means_the_default(self) -> None:
        """An empty variable leaves the default of one."""
        with mock.patch.dict("os.environ", {"SELDON_CONSUMER_PROCESSES": ""}):
            self.assertEqual(get_consumer_settings().processes, 1)

    def test_non_positive_process_count_is_rejected(self) -> None:
        """Zero and negative process counts are refused."""
        for value in ("0", "-2"):
            with self.subTest(value=value):
                with mock.patch.dict(
                    "os.environ", {"SELDON_CONSUMER_PROCESSES": value}
                ):
                    with self.assertRaises(ValidationError):
                        get_consumer_settings()
