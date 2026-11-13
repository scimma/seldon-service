"""Tests for the intake timing settings read from the environment."""

from unittest import mock

from django.test import SimpleTestCase

from seldon.config.intake import get_intake_settings


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
