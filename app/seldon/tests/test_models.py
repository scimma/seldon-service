"""Tests for the stored records: forecast identity, round trips, refusals.

These need the compose ``db`` service; run them with
``run/seldonctl slim_dev test``.
"""

from typing import Any

from django.db import IntegrityError, transaction
from django.test import TestCase

from seldon.domain.errors import RejectionCode
from seldon.models import Detection, Forecast, LightCurvePoint, Refusal, RubinObject

CHECKPOINT = "seldon-2.0-roman-elasticc/epoch=1086-val_loss=1.18.ckpt"
NEXT_CHECKPOINT = "seldon-2.1-roman-elasticc/epoch=1200-val_loss=1.10.ckpt"

# Values chosen so that any lossy float or ordering conversion shows: no short
# decimal, a negative flux, a tiny and a large magnitude, and repeated bands.
LIGHT_CURVE = {
    "times": [61000.123456789012, 61001.5, 61003.000000000001],
    "flux": [1234.5678901234567, -12.25, 0.1 + 0.2],
    "flux_err": [1.0 / 3.0, 2.718281828459045, 1e-300],
    "detected": [True, True, True],
    "bands": ["g", "r", "g"],
}
GRID = {
    "eval_times": [61000.123456789012, 61001.123456789012, 61063.0],
    "eval_bands": ["u", "g", "y"],
}
OUTPUTS = {
    "flux": [1.0 / 7.0, 98765.43210987654, -0.0001],
    "flux_err": [0.1, 0.2, 1e20],
    "zero_point_mag": 31.4,
    "parameters": {"t0": 61002.7, "amplitude": 3.141592653589793},
    "class_probabilities": {"SNIa": 0.7000000000000001, "SNII": 0.2999999999999999},
    "predicted_class": "SNIa",
    "latent_mean": [0.0, -1.5, 2.220446049250313e-16],
    "latent_sigma": [1.0, 0.5, 0.25],
}


def forecast_fields(detection: Detection, checkpoint: str) -> dict[str, Any]:
    """Build the fields of a stored forecast for one detection.

    Args:
        detection: The detection the forecast belongs to.
        checkpoint: The checkpoint that produced it.

    Returns:
        Keyword arguments for ``Forecast.objects.create``.
    """
    return {
        "detection": detection,
        "checkpoint": checkpoint,
        "light_curve": LIGHT_CURVE,
        "zero_point": 31.4,
        "grid": GRID,
        "outputs": OUTPUTS,
        "in_training_regime": False,
        "sparse": True,
        "n_observations": 3,
        "min_points": 5,
        "untrained_bands": [],
        "eval_span_days": 62.876543210988,
        "eval_span_in_time_scales": 0.6287654321,
        "time_scale_days": 100.0,
        "library_version": "1.2.0",
        "provenance_zero_point_mag": 27.5,
        "torch_version": "2.9.0+cpu",
        "app_version": "v0.0.0-test",
        "thresholds": {"fink": {"reliability_cut": 0.6, "sn_like_threshold": 0.5}},
    }


class StoredRecordTestCase(TestCase):
    """One Rubin object with one detection, shared by the tests below."""

    @classmethod
    def setUpTestData(cls) -> None:
        """Create the object and its detection."""
        cls.rubin_object = RubinObject.objects.create(dia_object_id=313953456790)
        cls.detection = Detection.objects.create(
            dia_source_id=170032915138363401,
            rubin_object=cls.rubin_object,
            state=Detection.State.OWED,
            observed_mjd=61003.000000000001,
        )


class ForecastIdentityTests(StoredRecordTestCase):
    """A detection is forecast at most once per checkpoint."""

    def test_second_forecast_under_same_checkpoint_is_refused(self) -> None:
        """The database rejects a second forecast for detection and checkpoint."""
        Forecast.objects.create(**forecast_fields(self.detection, CHECKPOINT))

        with self.assertRaises(IntegrityError), transaction.atomic():
            Forecast.objects.create(**forecast_fields(self.detection, CHECKPOINT))

        self.assertEqual(Forecast.objects.filter(detection=self.detection).count(), 1)

    def test_forecast_under_new_checkpoint_is_kept_alongside(self) -> None:
        """After a checkpoint upgrade both forecasts of a detection are stored."""
        Forecast.objects.create(**forecast_fields(self.detection, CHECKPOINT))
        Forecast.objects.create(**forecast_fields(self.detection, NEXT_CHECKPOINT))

        stored = Forecast.objects.filter(detection=self.detection)
        self.assertEqual(
            sorted(stored.values_list("checkpoint", flat=True)),
            [CHECKPOINT, NEXT_CHECKPOINT],
        )


class ForecastRoundTripTests(StoredRecordTestCase):
    """A stored forecast reads back exactly what was passed and returned."""

    def test_light_curve_grid_and_outputs_round_trip_exactly(self) -> None:
        """Arrays, band lists, and maps come back value for value."""
        created = Forecast.objects.create(**forecast_fields(self.detection, CHECKPOINT))

        stored = Forecast.objects.get(pk=created.pk)

        self.assertEqual(stored.light_curve, LIGHT_CURVE)
        self.assertEqual(stored.grid, GRID)
        self.assertEqual(stored.outputs, OUTPUTS)
        self.assertEqual(stored.zero_point, 31.4)
        self.assertIsNotNone(stored.stored_at)


class RefusalTests(StoredRecordTestCase):
    """A model refusal is stored with its code, message, and bands."""

    def test_refusal_stores_code_message_and_unknown_bands(self) -> None:
        """An unknown-band refusal keeps the bands the model could not resolve."""
        created = Refusal.objects.create(
            detection=self.detection,
            checkpoint=CHECKPOINT,
            code=RejectionCode.UNKNOWN_BAND,
            message="Unknown bands: 'w', 'Y'.",
            bands=["w", "Y"],
            app_version="v0.0.0-test",
        )

        stored = Refusal.objects.get(pk=created.pk)

        self.assertEqual(stored.code, RejectionCode.UNKNOWN_BAND.value)
        self.assertEqual(stored.message, "Unknown bands: 'w', 'Y'.")
        self.assertEqual(stored.bands, ["w", "Y"])


class LightCurvePointIdentityTests(TestCase):
    """A measurement is stored once, with or without its ``diaSourceId``.

    Some brokers send previous detections without their ``diaSourceId`` and
    with times converted from UTC Julian dates, so the same measurement can
    arrive with a time that differs from Rubin's by float rounding.
    """

    def test_point_without_source_id_is_stored(self) -> None:
        """A measurement a broker sent without its identifier still saves."""
        point = LightCurvePoint.objects.create(
            dia_object_id=313953456790,
            dia_source_id=None,
            observed_mjd=61357.0157874068,
            band="i",
            flux=1520.5,
            flux_err=40.25,
        )

        self.assertIsNone(LightCurvePoint.objects.get(pk=point.pk).dia_source_id)

    def test_same_measurement_at_rounded_time_is_one_point(self) -> None:
        """Object, band, and time to a tenth of a second identify a point."""
        LightCurvePoint.objects.create(
            dia_object_id=313953456790,
            dia_source_id=170032915138363401,
            observed_mjd=61357.0157874068,
            band="i",
            flux=1520.5,
            flux_err=40.25,
        )
        from_utc_julian_date = 2461357.51535916608 - 2400000.5 + 37.0 / 86400.0

        with self.assertRaises(IntegrityError), transaction.atomic():
            LightCurvePoint.objects.create(
                dia_object_id=313953456790,
                dia_source_id=None,
                observed_mjd=from_utc_julian_date,
                band="i",
                flux=1520.5,
                flux_err=40.25,
            )

    def test_other_band_at_same_time_is_another_point(self) -> None:
        """The same object and time in another band is a separate point."""
        for band in ("i", "z"):
            LightCurvePoint.objects.create(
                dia_object_id=313953456790,
                dia_source_id=None,
                observed_mjd=61357.0157874068,
                band=band,
                flux=1520.5,
                flux_err=40.25,
            )

        self.assertEqual(LightCurvePoint.objects.count(), 2)
