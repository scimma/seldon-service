"""Tests for running a forecast and assembling what callers receive.

Expected values are hand-computed or read from the checkpoint's
``hparams.yaml``: the class names from ``dataset.config.class_map``, the
latent width from ``global_params.latent_dim``, the selection filter from
``dataset.config.min_points``, and the untrained bands from the infinite
weights in ``dataset.config.band_weights`` (per
``docs/solutions/ml-runtime/seldon-servable-bands.md``). A zero point of 25
mag puts flux at ``10 ** (-0.4 * (27.5 - 25)) == 0.1`` of its value at 27.5.
Nothing here takes an expected value from the code under test.
"""

import dataclasses
from collections.abc import Mapping
from importlib import metadata
from typing import Any

import numpy as np
import torch
from django.test import SimpleTestCase

from seldon import services
from seldon.domain.models.forecast import ObjectForecast
from seldon.domain.models.request import ObjectRequest, ZeroPointDeclaration
from seldon.infrastructure.ml.forecast import named_class_probabilities
from seldon.tests.fixtures.hparams import read_hparams

AT_TRAINING = ZeroPointDeclaration.AT_TRAINING_ZERO_POINT
CHECKPOINT = "seldon-2.0-roman-elasticc/epoch=1086-val_loss=1.18.ckpt"


def _request(
    n: int = 40,
    eval_times: list[float] | None = None,
    eval_bands: list[str] | None = None,
    scale: float = 1.0,
    zero_point: object = 27.5,
    peak: float = 15.0,
) -> ObjectRequest:
    """A supernova-like light curve in LSST r and g, one point a day.

    Args:
        n: Observations.
        eval_times: Evaluation times; a 10-point r/g grid by default.
        eval_bands: Evaluation bands, one per time.
        scale: Multiplies flux and flux error, to re-express the zero point.
        zero_point: The request's zero point.
        peak: Day of peak brightness.
    """
    times = np.arange(n, dtype=float)
    flux = 10000.0 * np.exp(-0.5 * ((times - peak) / 6.0) ** 2) + 50.0
    if eval_times is None:
        eval_times = [float(t) for t in np.linspace(0.0, n + 10.0, 10)]
        eval_bands = ["r", "g"] * 5
    return ObjectRequest(
        times=times,
        flux=flux * scale,
        flux_err=(0.05 * flux + 10.0) * scale,
        detected=[True] * n,
        bands=["r", "g"] * (n // 2) + ["r"] * (n % 2),
        eval_times=eval_times,
        eval_bands=eval_bands,
        zero_point=zero_point,
    )


def _forecast_one(request: ObjectRequest) -> ObjectForecast:
    """Forecast one object through the public entry point."""
    (result,) = services.forecast([request])
    return result


class ForecastTests(SimpleTestCase):
    """The public entry point returns every output family, per object."""

    @classmethod
    def setUpClass(cls) -> None:
        """Read the literals the expectations come from."""
        super().setUpClass()
        hparams = read_hparams()
        dataset = hparams["dataset"]["config"]
        cls.class_names = set(dataset["class_map"])
        cls.latent_dim = hparams["global_params"]["latent_dim"]
        cls.min_points = dataset["min_points"]

    def test_flux_reader_also_gets_every_other_family_in_one_call(self) -> None:
        """Covers AE3: flux and uncertainty, and the rest present alongside."""
        result = _forecast_one(_request())

        self.assertEqual(result.flux.shape, (10,))
        self.assertEqual(result.flux_err.shape, (10,))
        self.assertTrue(np.isfinite(result.flux).all())
        self.assertTrue((result.flux_err > 0).all())

        self.assertGreater(len(result.parameters), 0)
        self.assertTrue(all(np.isfinite(v) for v in result.parameters.values()))
        self.assertEqual(set(result.class_probabilities), self.class_names)
        self.assertIn(result.predicted_class, self.class_names)
        self.assertEqual(result.latent_mean.shape, (self.latent_dim,))
        self.assertEqual(result.latent_sigma.shape, (self.latent_dim,))

    def test_empty_request_list_returns_no_forecasts(self) -> None:
        """Zero objects in gives zero forecasts out, not an internal error."""
        self.assertEqual(services.forecast([]), [])

    def test_identical_requests_get_identical_class_predictions(self) -> None:
        """The class comes from the deterministic latent mean, not a sample."""
        first = _forecast_one(_request())
        second = _forecast_one(_request())

        self.assertEqual(first.predicted_class, second.predicted_class)
        self.assertEqual(
            dict(first.class_probabilities), dict(second.class_probabilities)
        )

    def test_class_probabilities_sum_to_one_over_named_classes_only(self) -> None:
        """The unnamed tenth head output is dropped and the rest renormalized."""
        result = _forecast_one(_request())

        self.assertEqual(set(result.class_probabilities), self.class_names)
        self.assertNotIn("KN", result.class_probabilities)
        self.assertAlmostEqual(sum(result.class_probabilities.values()), 1.0, 6)
        self.assertEqual(
            result.predicted_class,
            max(result.class_probabilities, key=result.class_probabilities.get),
        )

    def test_head_matching_its_class_map_drops_nothing(self) -> None:
        """A ten-wide head with ten names keeps all ten, unchanged."""
        head = np.array([[0.05, 0.1, 0.05, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.2]])
        class_map = {f"class{k}": k for k in range(10)}

        (probabilities,) = named_class_probabilities(head, class_map)

        self.assertEqual(set(probabilities), set(class_map))
        for name, index in class_map.items():
            self.assertAlmostEqual(probabilities[name], head[0, index], 12)

    def test_flux_returns_in_the_callers_zero_point(self) -> None:
        """The same photometry at 27.5 and at 25 returns flux in each system."""
        at_training = _forecast_one(_request(zero_point=27.5))
        at_25 = _forecast_one(_request(zero_point=25.0, scale=0.1))

        self.assertEqual(at_training.zero_point_mag, 27.5)
        self.assertEqual(at_25.zero_point_mag, 25.0)
        np.testing.assert_allclose(at_25.flux, 0.1 * at_training.flux, rtol=1e-4)
        np.testing.assert_allclose(
            at_25.flux_err, 0.1 * at_training.flux_err, rtol=1e-4
        )

    def test_sparse_light_curve_is_served_with_the_indicator_set(self) -> None:
        """Five observations are below the training filter: served, flagged."""
        sparse = _forecast_one(_request(n=5, peak=2.0))
        dense = _forecast_one(_request(n=self.min_points))

        self.assertEqual(sparse.flux.shape, (10,))
        self.assertTrue(sparse.regime.sparse)
        self.assertFalse(sparse.regime.in_training_regime)
        self.assertEqual(sparse.regime.n_observations, 5)
        self.assertEqual(sparse.regime.min_points, self.min_points)
        self.assertFalse(dense.regime.sparse)
        self.assertTrue(dense.regime.in_training_regime)

    def test_untrained_band_is_served_with_the_band_axis_flagged(self) -> None:
        """Roman F146 and NIRCam F200W carry infinite weight in hparams.yaml."""
        untrained = _forecast_one(
            _request(eval_times=[5.0, 10.0, 15.0], eval_bands=["r", "F146", "F200W"])
        )
        trained = _forecast_one(
            _request(eval_times=[5.0, 10.0, 15.0], eval_bands=["r", "F062", "g"])
        )

        self.assertEqual(untrained.flux.shape, (3,))
        self.assertEqual(untrained.regime.untrained_bands, ("F146", "F200W"))
        self.assertFalse(untrained.regime.in_training_regime)
        self.assertEqual(trained.regime.untrained_bands, ())
        self.assertTrue(trained.regime.in_training_regime)

    def test_grids_of_different_lengths_each_get_their_own_points(self) -> None:
        """Batched, each object equals its solo forecast; no padding leaks."""
        short = _request(n=34, eval_times=[30.0, 2.0, 12.0], eval_bands=["g", "r", "i"])
        long_times = [float(t) for t in np.linspace(45.0, -3.0, 12)]
        long = _request(
            n=40, peak=20.0, eval_times=long_times, eval_bands=["r", "g", "z"] * 4
        )

        batched = services.forecast([short, long])
        alone = [_forecast_one(short), _forecast_one(long)]

        self.assertEqual(batched[0].flux.shape, (3,))
        self.assertEqual(batched[1].flux.shape, (12,))
        np.testing.assert_array_equal(batched[0].eval_times, [30.0, 2.0, 12.0])
        self.assertEqual(batched[0].eval_bands, ("g", "r", "i"))
        np.testing.assert_array_equal(batched[1].eval_times, long_times)
        for together, solo in zip(batched, alone):
            np.testing.assert_allclose(together.flux, solo.flux, rtol=1e-4)
            np.testing.assert_allclose(together.flux_err, solo.flux_err, rtol=1e-4)

    def test_objects_report_their_own_zero_points(self) -> None:
        """One object at 25 mag and one at 30 mag, in one call."""
        at_25, at_30 = services.forecast(
            [_request(zero_point=25.0), _request(zero_point=30.0)]
        )

        self.assertEqual(at_25.zero_point_mag, 25.0)
        self.assertEqual(at_30.zero_point_mag, 30.0)

    def test_every_result_carries_provenance(self) -> None:
        """Library version, checkpoint, and training zero point, per object."""
        results = services.forecast([_request(), _request(zero_point=AT_TRAINING)])

        for result in results:
            self.assertEqual(
                result.provenance.library_version, metadata.version("seldon_core")
            )
            self.assertEqual(result.provenance.checkpoint, CHECKPOINT)
            self.assertEqual(result.provenance.zero_point_mag, 27.5)
            self.assertEqual(result.provenance.torch_version, torch.__version__)

    def test_no_returned_field_is_a_torch_tensor(self) -> None:
        """Every value reachable from a result is plain Python or numpy."""

        def walk(value: Any) -> None:
            self.assertNotIsInstance(value, torch.Tensor)
            if dataclasses.is_dataclass(value):
                for field in dataclasses.fields(value):
                    walk(getattr(value, field.name))
            elif isinstance(value, Mapping):
                for key, item in value.items():
                    walk(key)
                    walk(item)
            elif isinstance(value, (tuple, list)):
                for item in value:
                    walk(item)

        result = _forecast_one(_request())
        walk(result)
        self.assertIsInstance(result.flux, np.ndarray)
        self.assertIsInstance(result.latent_mean, np.ndarray)
