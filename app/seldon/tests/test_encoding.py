"""Tests for turning validated photometry into the model's input tensors.

Expected values are hand-computed: a normalized time is ``(t - t_first) / 20``
(the checkpoint's ``t_max`` in ``hparams.yaml``), and a zero point of 25 mag
scales flux by ``10 ** (-0.4 * (25 - 27.5)) == 10``. Band indices come from the
sorted filter names in ``hparams.yaml``. The flux normalization is checked by
inverting it with the library's own pipeline, which is what post-processing
uses; nothing here takes an expected value from the code under test.
"""

import math

import numpy as np
import torch
import yaml
from django.test import SimpleTestCase

from seldon import services
from seldon.config.settings import get_settings
from seldon.domain.models.request import ObjectRequest, ZeroPointDeclaration
from seldon.infrastructure.ml.capabilities import read_capabilities
from seldon.infrastructure.ml.encoding import encode_object, encode_objects
from seldon.infrastructure.ml.validation import validate_object

AT_TRAINING = ZeroPointDeclaration.AT_TRAINING_ZERO_POINT
TIME_SCALE = 20.0  # hparams.yaml: dataset.config.flux_stats.t_max


def _band_names() -> list[str]:
    """The checkpoint's band names in index order, read from hparams.yaml."""
    hparams = yaml.safe_load(get_settings().hparams_path.read_text())
    return sorted(hparams["dataset"]["config"]["band_index_map"]["config"]["filedict"])


def _request(
    times: list[float],
    bands: list[str],
    eval_times: list[float],
    eval_bands: list[str],
    flux: list[float] | None = None,
    zero_point: object = AT_TRAINING,
) -> ObjectRequest:
    """An object request with unit flux error unless a test says otherwise."""
    n = len(times)
    flux = flux if flux is not None else [100.0] * n
    return ObjectRequest(
        times=times,
        flux=flux,
        flux_err=[f / 10.0 for f in flux],
        detected=[True] * n,
        bands=bands,
        eval_times=eval_times,
        eval_bands=eval_bands,
        zero_point=zero_point,
    )


class EncodingTests(SimpleTestCase):
    """Validated photometry becomes the six tensors the forward pass reads."""

    @classmethod
    def setUpClass(cls) -> None:
        """Read the real checkpoint's capabilities and flux pipeline once."""
        super().setUpClass()
        loaded = services.loaded_model()
        cls.capabilities = read_capabilities(loaded)
        cls.dataset = loaded.experiment.data.dataset
        cls.index = {name: i for i, name in enumerate(_band_names())}

    def _encode_one(self, request: ObjectRequest):
        """Validate and encode one object as a one-element batch."""
        validated = validate_object(request, self.capabilities)
        return encode_objects([validated], self.capabilities, self.dataset.transform)

    def _unnormalized_flux(self, encoded, row: int = 0) -> tuple[list, list]:
        """Invert the library's flux pipeline on one encoded object's flux."""
        n = encoded.objects[row].n_observations
        x = encoded.batch["x_input"][row, :n].double().numpy()
        flux, flux_err = self.dataset.inverse_transform(x[:, 1], x[:, 2])
        return list(flux), list(flux_err)

    def test_eval_grid_differing_in_values_and_order_restores_caller_order(
        self,
    ) -> None:
        """A shuffled grid at unobserved times is sorted, and restorable."""
        encoded = self._encode_one(
            _request(
                times=[102.0, 100.0, 104.0, 101.0],
                bands=["g", "r", "g", "r"],
                eval_times=[110.0, 95.0, 103.5, 100.0],
                eval_bands=["g", "r", "F062", "i"],
            )
        )
        times = encoded.batch["time_full"][0, :, 0].double().numpy()
        bands = encoded.batch["band_idx_full"][0, :, 0].tolist()
        i = self.index

        # Sorted ascending, each time still with its own band.
        np.testing.assert_allclose(times, [-0.25, 0.0, 0.175, 0.5], atol=1e-6)
        self.assertEqual(bands, [i["r"], i["i"], i["F062"], i["g"]])

        # The recorded permutation restores the caller's order and pairing.
        restore = encoded.objects[0].eval_restore
        np.testing.assert_allclose(times[restore], [0.5, -0.25, 0.175, 0.0], atol=1e-6)
        self.assertEqual(
            [bands[k] for k in restore], [i["g"], i["r"], i["F062"], i["i"]]
        )

    def test_normalized_flux_round_trips_through_the_pipeline_inverse(self) -> None:
        """The model sees normalized flux that post-processing can invert."""
        flux = [50.0, 1000.0, 20000.0]
        encoded = self._encode_one(
            _request(
                times=[0.0, 1.0, 2.0],
                bands=["r", "r", "r"],
                eval_times=[1.5],
                eval_bands=["r"],
                flux=flux,
                zero_point=25.0,
            )
        )
        normalized = encoded.batch["x_input"][0, :, 1].double().numpy()
        recovered, recovered_err = self._unnormalized_flux(encoded)

        self.assertFalse(np.allclose(normalized, [500.0, 10000.0, 200000.0]))
        np.testing.assert_allclose(recovered, [500.0, 10000.0, 200000.0], rtol=1e-5)
        np.testing.assert_allclose(recovered_err, [50.0, 1000.0, 20000.0], rtol=1e-4)

    def test_eval_grid_shorter_than_observations_keeps_pairing(self) -> None:
        """Two evaluation points against five observations, each with its band."""
        encoded = self._encode_one(
            _request(
                times=[0.0, 4.0, 2.0, 1.0, 3.0],
                bands=["r", "g", "r", "g", "r"],
                eval_times=[8.0, 6.0],
                eval_bands=["F062", "z"],
            )
        )
        self.assertEqual(tuple(encoded.batch["time_full"].shape), (1, 2, 1))
        np.testing.assert_allclose(
            encoded.batch["time_full"][0, :, 0].numpy(), [0.3, 0.4], atol=1e-6
        )
        self.assertEqual(
            encoded.batch["band_idx_full"][0, :, 0].tolist(),
            [self.index["z"], self.index["F062"]],
        )

    def test_eval_grid_before_first_observation_is_negative(self) -> None:
        """Times before the earliest observation normalize below zero."""
        encoded = self._encode_one(
            _request(
                times=[105.0, 100.0, 110.0],
                bands=["r", "r", "r"],
                eval_times=[90.0, 100.0, 120.0],
                eval_bands=["r", "r", "r"],
            )
        )
        np.testing.assert_allclose(
            encoded.batch["time_full"][0, :, 0].numpy(), [-0.5, 0.0, 1.0], atol=1e-6
        )
        np.testing.assert_allclose(
            encoded.batch["x_input"][0, :, 0].numpy(), [0.0, 0.25, 0.5], atol=1e-6
        )

    def test_per_observation_zero_point_scales_each_point(self) -> None:
        """An array of zero points scales point by point; a scalar, uniformly.

        The observation with a non-finite zero point is not usable and is
        dropped before encoding.
        """
        per_point = self._encode_one(
            _request(
                times=[0.0, 1.0, 2.0, 3.0],
                bands=["r", "r", "r", "r"],
                eval_times=[1.0],
                eval_bands=["r"],
                zero_point=[25.0, 27.5, math.nan, 30.0],
            )
        )
        self.assertEqual(per_point.objects[0].n_observations, 3)
        flux, flux_err = self._unnormalized_flux(per_point)
        # 100 * 10**(-0.4 * (zp - 27.5)) for zp = 25, 27.5, 30.
        np.testing.assert_allclose(flux, [1000.0, 100.0, 10.0], rtol=1e-5)
        np.testing.assert_allclose(flux_err, [100.0, 10.0, 1.0], rtol=1e-4)

        scalar = self._encode_one(
            _request(
                times=[0.0, 1.0, 2.0],
                bands=["r", "r", "r"],
                eval_times=[1.0],
                eval_bands=["r"],
                zero_point=25.0,
            )
        )
        flux, _ = self._unnormalized_flux(scalar)
        np.testing.assert_allclose(flux, [1000.0, 1000.0, 1000.0], rtol=1e-5)

    def test_detection_flag_outside_zero_and_one_is_rejected(self) -> None:
        """A detection value of 2 never reaches the model's two-row embedding."""
        with self.assertRaises(ValueError) as caught:
            ObjectRequest(
                times=[0.0, 1.0, 2.0],
                flux=[1.0, 1.0, 1.0],
                flux_err=[1.0, 1.0, 1.0],
                detected=[1, 2, 0],
                bands=["r", "r", "r"],
                eval_times=[1.0],
                eval_bands=["r"],
                zero_point=AT_TRAINING,
            )
        self.assertIn("detected", str(caught.exception))

    def test_objects_of_different_lengths_stack_with_a_mask(self) -> None:
        """A 3-point and a 5-point object share one padded batch."""
        short = _request(
            times=[0.0, 1.0, 2.0],
            bands=["r", "r", "r"],
            eval_times=[1.0, 2.0],
            eval_bands=["r", "r"],
        )
        long = _request(
            times=[0.0, 1.0, 2.0, 3.0, 4.0],
            bands=["g", "g", "g", "g", "g"],
            eval_times=[1.0, 2.0, 3.0, 4.0],
            eval_bands=["g", "g", "g", "g"],
        )
        validated = [validate_object(r, self.capabilities) for r in (short, long)]
        encoded = encode_objects(validated, self.capabilities, self.dataset.transform)
        batch = encoded.batch

        self.assertEqual(tuple(batch["x_input"].shape), (2, 5, 4))
        self.assertEqual(batch["x_input"].dtype, torch.float32)
        self.assertEqual(tuple(batch["band_idx"].shape), (2, 5, 1))
        self.assertEqual(batch["band_idx"].dtype, torch.long)
        self.assertEqual(tuple(batch["time_full"].shape), (2, 4, 1))
        self.assertEqual(tuple(batch["band_idx_full"].shape), (2, 4, 1))
        self.assertEqual(batch["pad_mask_part"].dtype, torch.bool)
        self.assertEqual(
            batch["pad_mask_part"].tolist(),
            [[True, True, True, False, False], [True] * 5],
        )
        self.assertEqual(len(batch["class_name"]), 2)
        self.assertEqual([o.n_observations for o in encoded.objects], [3, 5])
        self.assertEqual([o.n_eval for o in encoded.objects], [2, 4])
        self.assertEqual(encoded.pad_value, 0.0)
        self.assertTrue(bool((batch["x_input"][0, 3:] == 0.0).all()))

    def test_one_element_sequence_matches_the_single_object(self) -> None:
        """Batching one object changes nothing but the leading dimension."""
        request = _request(
            times=[3.0, 1.0, 2.0],
            bands=["g", "r", "i"],
            eval_times=[5.0, 0.0],
            eval_bands=["r", "g"],
            zero_point=26.0,
        )
        validated = validate_object(request, self.capabilities)
        single = encode_object(validated, self.capabilities, self.dataset.transform)
        batched = encode_objects([validated], self.capabilities, self.dataset.transform)

        for key, tensor in single.inputs.items():
            if key == "class_name":
                self.assertEqual(batched.batch[key], [tensor])
            else:
                self.assertTrue(torch.equal(batched.batch[key][0], tensor), key)
        self.assertEqual(
            batched.objects[0].eval_restore.tolist(), single.eval_restore.tolist()
        )
