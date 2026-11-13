"""Which measurements a detection's forecast sees, and the request it makes.

A forecast uses the detection and the earlier detections its own alert
carried, observed at or before it, each once, sorted by time. Forced
photometry is left out. Rubin fluxes are in nJy, whose AB zero point is
31.4, and LSST band letters are normalized to the checkpoint's lower-case
names; any other band passes through so the model can refuse it by name.
"""

from seldon.domain.intake.delivery import Delivery, Point
from seldon.domain.intake.grid import forecast_grid
from seldon.domain.models.request import ObjectRequest

#: AB zero point of a flux in nJy: 2.5 * log10(3631e9).
LSST_ZERO_POINT = 31.4

#: Observation times are compared at this resolution, in tenths of a second.
#: Some brokers send previous detections as UTC Julian dates without their
#: ``diaSourceId``; converted back to MJD TAI they differ from Rubin's value
#: by float rounding (about a microsecond), never by a tenth of a second.
OBSERVED_KEY_PER_DAY = 864_000

_LSST_BANDS = frozenset("ugrizy")


def observed_key(observed_mjd: float) -> int:
    """Round an MJD TAI observation time to the resolution points match at.

    Args:
        observed_mjd: Observation time, as an MJD in TAI.

    Returns:
        The time in tenths of a second since MJD 0.
    """
    return round(observed_mjd * OBSERVED_KEY_PER_DAY)


def normalize_band(band: str) -> str:
    """Map an LSST band letter in either case to the checkpoint's name.

    Args:
        band: The band as a broker sent it.

    Returns:
        The lower-case LSST letter, or ``band`` unchanged when it is not one.
    """
    lower = band.lower()
    return lower if lower in _LSST_BANDS else band


def light_curve_points(delivery: Delivery) -> tuple[Point, ...]:
    """The measurements a detection's forecast is made from.

    Two measurements in the same band at the same rounded time are the same
    measurement; the copy carrying a ``diaSourceId`` is kept.

    Args:
        delivery: The delivery whose detection is forecast.

    Returns:
        The detection and its earlier detections, each once, sorted by time,
        with bands normalized.
    """
    cutoff = observed_key(delivery.detection.observed_mjd)
    kept: dict[tuple[str, int], Point] = {}
    for point in (delivery.detection, *delivery.previous):
        key_time = observed_key(point.observed_mjd)
        if key_time > cutoff:
            continue
        band = normalize_band(point.band)
        key = (band, key_time)
        if key in kept and kept[key].dia_source_id is not None:
            continue
        kept[key] = Point(
            observed_mjd=point.observed_mjd,
            band=band,
            flux=point.flux,
            flux_err=point.flux_err,
            dia_source_id=point.dia_source_id,
        )
    return tuple(sorted(kept.values(), key=lambda p: p.observed_mjd))


def forecast_request(delivery: Delivery) -> ObjectRequest:
    """Build the forecast request for a delivery's detection.

    Args:
        delivery: The delivery whose detection is forecast.

    Returns:
        A request over the detection's light curve, at the LSST zero point,
        on the daily six-band grid.

    Raises:
        LightCurveSpanError: If the light curve spans more than 400 days.
        ValueError: If the request cannot be constructed.
    """
    points = light_curve_points(delivery)
    eval_times, eval_bands = forecast_grid(
        points[0].observed_mjd, points[-1].observed_mjd
    )
    return ObjectRequest(
        times=[p.observed_mjd for p in points],
        flux=[p.flux for p in points],
        flux_err=[p.flux_err for p in points],
        detected=[True] * len(points),
        bands=[p.band for p in points],
        eval_times=eval_times,
        eval_bands=eval_bands,
        zero_point=LSST_ZERO_POINT,
    )
