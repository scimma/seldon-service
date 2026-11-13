"""Tests for the broker-neutral intake rules: light curve, grid, qualification.

Expected values are hand-computed: a 10-day span gives 71 daily grid points
(days 0 through 70 inclusive) in each of six bands, and LSST fluxes in nJy
have an AB zero point of 2.5 * log10(3631e9) = 31.4. The recorded Fink alert
comes from ``seldon.tests.fixtures.broker_alerts``.
"""

import subprocess
import sys
from datetime import datetime, timezone

from django.test import SimpleTestCase

from seldon import services
from seldon.domain.intake.delivery import BrokerRole, Delivery, Point
from seldon.domain.intake.grid import GRID_BANDS, LightCurveSpanError, forecast_grid
from seldon.domain.intake.light_curve import (
    LSST_ZERO_POINT,
    forecast_request,
    light_curve_points,
)
from seldon.domain.intake.qualification import Thresholds, qualifies
from seldon.tests.fixtures.broker_alerts import fink

RECEIVED = datetime(2026, 11, 13, 0, 5, tzinfo=timezone.utc)
OBJECT_ID = 170019696011640884


def _point(
    mjd: float, band: str = "r", flux: float = 1000.0, source_id: int | None = None
) -> Point:
    """One measurement with a fixed flux error.

    Args:
        mjd: Observation time, MJD TAI.
        band: Band name as a broker sent it.
        flux: Flux in nJy.
        source_id: The ``diaSourceId``, when the broker sent one.

    Returns:
        The point.
    """
    return Point(
        observed_mjd=mjd, band=band, flux=flux, flux_err=25.0, dia_source_id=source_id
    )


def _delivery(
    detection: Point,
    previous: list[Point] | None = None,
    reliability: float | None = 0.9,
    sn_like_score: float | None = 0.8,
    role: BrokerRole = BrokerRole.QUALIFYING,
) -> Delivery:
    """A delivery of one detection from a broker named ``fink``.

    Args:
        detection: The triggering detection.
        previous: Previous measurements the alert carried.
        reliability: The broker's reliability score.
        sn_like_score: The broker's SN-like score.
        role: The broker's configured role.

    Returns:
        The delivery.
    """
    return Delivery(
        dia_object_id=OBJECT_ID,
        detection=detection,
        previous=tuple(previous or ()),
        broker="fink",
        role=role,
        reliability=reliability,
        sn_like_score=sn_like_score,
        published_at=None,
        received_at=RECEIVED,
        schema_version="lsst.v10_0",
        classifier_version="",
    )


class LightCurveTests(SimpleTestCase):
    """The forecast sees the detection and earlier detections, once each."""

    def test_detection_and_three_previous_sorted_at_lsst_zero_point(self) -> None:
        """Four observations, sorted by time, in nJy at zero point 31.4."""
        detection = _point(61010.0, "g", 4000.0, source_id=4)
        previous = [
            _point(61004.0, "r", 2000.0, source_id=2),
            _point(61001.0, "r", 1000.0, source_id=1),
            _point(61007.0, "i", 3000.0, source_id=3),
        ]

        request = forecast_request(_delivery(detection, previous))

        self.assertEqual(list(request.times), [61001.0, 61004.0, 61007.0, 61010.0])
        self.assertEqual(list(request.flux), [1000.0, 2000.0, 3000.0, 4000.0])
        self.assertEqual(list(request.bands), ["r", "r", "i", "g"])
        self.assertTrue(all(request.detected))
        self.assertEqual(request.zero_point, 31.4)
        self.assertEqual(LSST_ZERO_POINT, 31.4)

    def test_previous_detection_listed_twice_appears_once(self) -> None:
        """A repeated measurement is kept once."""
        detection = _point(61010.0, source_id=3)
        repeated = _point(61004.0, source_id=2)

        points = light_curve_points(_delivery(detection, [repeated, repeated]))

        self.assertEqual([p.observed_mjd for p in points], [61004.0, 61010.0])

    def test_same_measurement_with_and_without_identifier_appears_once(self) -> None:
        """A UTC-converted copy without an identifier matches the named one."""
        detection = _point(61010.0, source_id=3)
        named = _point(61004.0, "i", source_id=2)
        converted = _point(61004.0 + 1e-11, "i", source_id=None)

        points = light_curve_points(_delivery(detection, [converted, named]))

        self.assertEqual(len(points), 2)
        self.assertEqual(points[0].dia_source_id, 2)

    def test_previous_detection_after_the_current_one_is_excluded(self) -> None:
        """Nothing observed after the detection is passed to its forecast."""
        detection = _point(61010.0, source_id=3)
        later = _point(61012.0, source_id=4)

        points = light_curve_points(_delivery(detection, [_point(61001.0), later]))

        self.assertEqual([p.observed_mjd for p in points], [61001.0, 61010.0])

    def test_lsst_bands_normalize_to_checkpoint_names(self) -> None:
        """``y`` and ``Y`` both become the checkpoint's ``y``; others pass."""
        detection = _point(61010.0, "Y")
        previous = [_point(61001.0, "y"), _point(61002.0, "G"), _point(61003.0, "w")]

        request = forecast_request(_delivery(detection, previous))

        self.assertEqual(list(request.bands), ["y", "g", "w", "y"])

    def test_recorded_fink_alert_yields_its_detections_in_order(self) -> None:
        """A real alert's detection and previous detections, sorted, in nJy."""
        alert = fink.ALERTS[0]["alert"]
        source = alert["diaSource"]
        detection = Point(
            observed_mjd=source["midpointMjdTai"],
            band=source["band"],
            flux=source["psfFlux"],
            flux_err=source["psfFluxErr"],
            dia_source_id=source["diaSourceId"],
        )
        previous = [
            Point(
                observed_mjd=p["midpointMjdTai"],
                band=p["band"],
                flux=p["psfFlux"],
                flux_err=p["psfFluxErr"],
                dia_source_id=p["diaSourceId"],
            )
            for p in alert["prvDiaSources"]
        ]
        expected_ids = {source["diaSourceId"]} | {
            p["diaSourceId"]
            for p in alert["prvDiaSources"]
            if p["midpointMjdTai"] <= source["midpointMjdTai"]
        }

        points = light_curve_points(_delivery(detection, previous))

        self.assertEqual({p.dia_source_id for p in points}, expected_ids)
        times = [p.observed_mjd for p in points]
        self.assertEqual(times, sorted(times))
        self.assertEqual(points[-1].dia_source_id, source["diaSourceId"])


class GridTests(SimpleTestCase):
    """Six bands, daily, from the first observation to 60 days past the last."""

    def test_ten_day_span_gives_six_bands_of_71_days(self) -> None:
        """Days 0 through 70 inclusive in each band."""
        eval_times, eval_bands = forecast_grid(61000.0, 61010.0)

        self.assertEqual(len(eval_times), 6 * 71)
        self.assertEqual(sorted(set(eval_bands)), sorted(GRID_BANDS))
        self.assertEqual(min(eval_times), 61000.0)
        self.assertEqual(max(eval_times), 61070.0)
        for band in GRID_BANDS:
            self.assertEqual(eval_bands.count(band), 71)

    def test_span_over_400_days_is_rejected_before_a_grid_is_built(self) -> None:
        """A light curve longer than Rubin's alert history is refused."""
        with self.assertRaises(LightCurveSpanError):
            forecast_grid(61000.0, 61400.5)

    def test_light_curve_over_400_days_rejected_at_request(self) -> None:
        """The span check applies to the light curve a delivery carries."""
        delivery = _delivery(_point(61401.0), [_point(61000.0)])

        with self.assertRaises(LightCurveSpanError):
            forecast_request(delivery)

    def test_span_error_is_a_value_error(self) -> None:
        """Intake treats it like any other unbuildable request."""
        self.assertTrue(issubclass(LightCurveSpanError, ValueError))


class SparseForecastTests(SimpleTestCase):
    """A light curve of only a few detections is forecast, marked sparse."""

    def test_four_observations_are_served_with_the_sparse_mark(self) -> None:
        """Four detections still give a forecast, flagged below training."""
        detection = _point(61010.0, "r", 4000.0)
        previous = [
            _point(61001.0, "g", 1500.0),
            _point(61004.0, "r", 2500.0),
            _point(61007.0, "i", 3500.0),
        ]

        (result,) = services.forecast(
            [forecast_request(_delivery(detection, previous))]
        )

        self.assertTrue(result.regime.sparse)
        self.assertFalse(result.regime.in_training_regime)
        self.assertEqual(result.regime.n_observations, 4)
        # First observation at 61001, last at 61010: a 9-day span, so 70 days.
        self.assertEqual(len(result.flux), 6 * 70)


class QualificationTests(SimpleTestCase):
    """One broker's reliability cut and SN-like threshold decide."""

    thresholds = Thresholds(reliability_cut=0.6, sn_like_threshold=0.5)

    def test_scores_at_the_cut_and_threshold_qualify(self) -> None:
        """Meeting both exactly is enough."""
        delivery = _delivery(_point(61010.0), reliability=0.6, sn_like_score=0.5)

        self.assertTrue(qualifies(delivery, self.thresholds))

    def test_reliability_below_the_cut_does_not_qualify(self) -> None:
        """A high SN-like score does not rescue low reliability."""
        delivery = _delivery(_point(61010.0), reliability=0.59, sn_like_score=0.99)

        self.assertFalse(qualifies(delivery, self.thresholds))

    def test_sn_like_score_below_the_threshold_does_not_qualify(self) -> None:
        """High reliability does not rescue a low SN-like score."""
        delivery = _delivery(_point(61010.0), reliability=0.99, sn_like_score=0.49)

        self.assertFalse(qualifies(delivery, self.thresholds))

    def test_missing_sn_like_score_never_qualifies(self) -> None:
        """No score is not a pass, whatever the threshold."""
        delivery = _delivery(_point(61010.0), sn_like_score=None)

        self.assertFalse(qualifies(delivery, Thresholds(0.0, 0.0)))

    def test_full_stream_delivery_never_qualifies(self) -> None:
        """The full-stream source supplies detections but never qualifies one."""
        delivery = _delivery(
            _point(61010.0),
            reliability=1.0,
            sn_like_score=1.0,
            role=BrokerRole.FULL_STREAM,
        )

        self.assertFalse(qualifies(delivery, Thresholds(0.0, 0.0)))


class DjangoFreeTests(SimpleTestCase):
    """The intake rules import without Django or torch."""

    def test_rules_import_without_django_or_torch(self) -> None:
        """A fresh interpreter imports every rule module and neither library."""
        code = (
            "import sys\n"
            "import seldon.domain.intake.delivery, seldon.domain.intake.grid\n"
            "import seldon.domain.intake.light_curve\n"
            "import seldon.domain.intake.qualification\n"
            "print(sorted(m for m in ('django', 'torch') if m in sys.modules))\n"
        )

        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            check=True,
            env={"PYTHONPATH": ".", "PATH": "/usr/local/bin:/usr/bin:/bin"},
        )

        self.assertEqual(result.stdout.strip(), "[]")
