"""The grid every alert forecast is evaluated on.

Six LSST bands, one point a day, from the first observation in the light
curve to 60 days past the last. Rubin alerts carry at most twelve months of
history, so a light curve spanning more than 400 days is refused before any
grid is allocated: one malformed alert cannot exhaust memory.
"""

GRID_BANDS = ("u", "g", "r", "i", "z", "y")
DAYS_PAST_LAST = 60
MAX_SPAN_DAYS = 400.0


class LightCurveSpanError(ValueError):
    """A light curve spans more time than any Rubin alert can carry."""


def forecast_grid(first_mjd: float, last_mjd: float) -> tuple[list[float], list[str]]:
    """Build the daily six-band evaluation grid for a light curve.

    Args:
        first_mjd: Time of the earliest observation, MJD TAI.
        last_mjd: Time of the latest observation, MJD TAI.

    Returns:
        ``eval_times`` and ``eval_bands``, one entry per grid point, band by
        band.

    Raises:
        LightCurveSpanError: If the light curve spans more than 400 days.
    """
    span = last_mjd - first_mjd
    if span > MAX_SPAN_DAYS:
        raise LightCurveSpanError(
            f"Light curve spans {span:.1f} days, more than {MAX_SPAN_DAYS:.0f}."
        )
    n_days = int(span) + DAYS_PAST_LAST + 1
    days = [first_mjd + day for day in range(n_days)]
    eval_times = [time for _ in GRID_BANDS for time in days]
    eval_bands = [band for band in GRID_BANDS for _ in days]
    return eval_times, eval_bands
