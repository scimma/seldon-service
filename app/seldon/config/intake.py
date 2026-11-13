"""Intake timing settings, read from ``SELDON_INTAKE_``-prefixed variables.

One silence period serves two purposes: a broker that delivers nothing for
longer than it during a Rubin night is recorded as a suspected gap, and a
delivery received longer than it after its broker published it counts as a
replay and is left out of the latency percentile.
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class IntakeSettings(BaseSettings):
    """Timing settings shared by the consumer and its reports.

    Attributes:
        silence_minutes: The silence period, in minutes.
        night_start_utc_hour: The UTC hour a Rubin night's observing starts.
        night_end_utc_hour: The UTC hour it ends; earlier than the start
            when the night crosses midnight UTC.
    """

    silence_minutes: float = Field(60.0, gt=0)
    night_start_utc_hour: int = Field(23, ge=0, le=23)
    night_end_utc_hour: int = Field(11, ge=0, le=23)

    model_config = SettingsConfigDict(
        env_prefix="SELDON_INTAKE_", case_sensitive=False, env_ignore_empty=True
    )


def get_intake_settings() -> IntakeSettings:
    """Build the intake settings from the current environment.

    Returns:
        A freshly validated ``IntakeSettings`` instance.
    """
    return IntakeSettings()
