"""Tests for request validation and the rejection contract.

Band expectations come from the checkpoint's ``hparams.yaml`` (the sorted
filter names give the indices, per
``docs/solutions/ml-runtime/seldon-servable-bands.md``), never from the code
under test. The vocabulary is read off the process-wide cached model.
"""

import math
import tempfile
from pathlib import Path

import yaml
from django.test import SimpleTestCase

from seldon import services
from seldon.config.settings import get_settings
from seldon.domain.errors import (
    MissingZeroPointError,
    NoUsableObservationsError,
    UnknownBandError,
)
from seldon.domain.models.request import (
    TRAINING_ZERO_POINT_MAG,
    ObjectRequest,
    ZeroPointDeclaration,
)
from seldon.infrastructure.ml.capabilities import read_capabilities
from seldon.infrastructure.ml.loader import IncompatibleCheckpointError, load_model
from seldon.infrastructure.ml.validation import validate_object
from seldon.tests.fixtures.hparams import band_names_in_index_order

AT_TRAINING = ZeroPointDeclaration.AT_TRAINING_ZERO_POINT


def _request(
    bands: tuple[str, ...] = ("r", "r", "g"),
    eval_bands: tuple[str, ...] = ("r", "g"),
    flux: tuple[float, ...] | None = None,
    zero_point: object = AT_TRAINING,
) -> ObjectRequest:
    """A small, valid object request; each test changes one thing."""
    n = len(bands)
    return ObjectRequest(
        times=[float(i) for i in range(n)],
        flux=flux if flux is not None else [100.0] * n,
        flux_err=[10.0] * n,
        detected=[True] * n,
        bands=bands,
        eval_times=[float(i) for i in range(len(eval_bands))],
        eval_bands=eval_bands,
        zero_point=zero_point,
    )


class ValidationTests(SimpleTestCase):
    """What the checkpoint cannot serve is refused by name; the rest passes."""

    @classmethod
    def setUpClass(cls) -> None:
        """Read the real checkpoint's capabilities once."""
        super().setUpClass()
        cls.capabilities = read_capabilities(services.loaded_model())
        cls.names = band_names_in_index_order()

    def test_missing_zero_point_is_rejected_by_name(self) -> None:
        """A request that states no zero point is refused, naming the omission."""
        with self.assertRaises(MissingZeroPointError) as caught:
            validate_object(_request(zero_point=None), self.capabilities)
        self.assertIn("zero point", str(caught.exception))

    def test_declared_training_zero_point_is_accepted(self) -> None:
        """Asserting the flux is already at 27.5 mag is a valid zero point."""
        self.assertEqual(TRAINING_ZERO_POINT_MAG, 27.5)
        validated = validate_object(_request(), self.capabilities)
        self.assertEqual(validated.request.zero_point, AT_TRAINING)

    def test_non_finite_scalar_zero_point_is_rejected_as_missing(self) -> None:
        """A NaN zero point declares nothing, so it is refused as missing."""
        with self.assertRaises(MissingZeroPointError):
            validate_object(_request(zero_point=math.nan), self.capabilities)

    def test_non_finite_per_observation_zero_point_makes_point_unusable(
        self,
    ) -> None:
        """Finite photometry whose every zero point is NaN has nothing usable."""
        with self.assertRaises(NoUsableObservationsError):
            validate_object(
                _request(zero_point=[math.nan, math.nan, math.nan]),
                self.capabilities,
            )

    def test_unknown_observation_band_is_rejected_by_name(self) -> None:
        """A ZTF-style band name in the photometry is refused and named."""
        with self.assertRaises(UnknownBandError) as caught:
            validate_object(_request(bands=("r", "ZTF_g", "g")), self.capabilities)
        self.assertIn("ZTF_g", str(caught.exception))
        self.assertEqual(caught.exception.bands, ("ZTF_g",))

    def test_unknown_evaluation_band_alone_is_rejected_by_name(self) -> None:
        """Known photometry with an unknown band on the grid is still refused."""
        with self.assertRaises(UnknownBandError) as caught:
            validate_object(_request(eval_bands=("r", "Kp")), self.capabilities)
        self.assertIn("Kp", str(caught.exception))
        self.assertEqual(caught.exception.bands, ("Kp",))

    def test_no_usable_observations_is_rejected(self) -> None:
        """An object whose every point has non-finite flux is refused."""
        with self.assertRaises(NoUsableObservationsError):
            validate_object(
                _request(flux=(math.nan, math.inf, math.nan)), self.capabilities
            )

    def test_sparse_light_curve_is_accepted(self) -> None:
        """Three points in a known band are served, not refused."""
        validated = validate_object(
            _request(bands=("g", "g", "g"), eval_bands=("g",)), self.capabilities
        )
        g = self.names.index("g")
        self.assertEqual(validated.band_indices.tolist(), [g, g, g])
        self.assertEqual(validated.eval_band_indices.tolist(), [g])

    def test_upper_case_single_character_band_resolves(self) -> None:
        """``R`` resolves to the same index as ``r`` (39), on both sides."""
        validated = validate_object(
            _request(bands=("R", "r", "F062"), eval_bands=("R", "F062")),
            self.capabilities,
        )
        r = self.names.index("r")
        self.assertEqual(r, 39)
        f062 = self.names.index("F062")
        self.assertEqual(validated.band_indices.tolist(), [r, r, f062])
        self.assertEqual(validated.eval_band_indices.tolist(), [r, f062])

    def test_each_rejection_carries_its_distinct_documented_code(self) -> None:
        """Every refusal path raises an error whose code is its own category."""
        caught = {}
        for name, request in (
            ("missing_zero_point", _request(zero_point=None)),
            ("unknown_band", _request(eval_bands=("Kp",))),
            ("no_usable_observations", _request(flux=(math.nan,) * 3)),
        ):
            try:
                validate_object(request, self.capabilities)
            except Exception as error:  # noqa: BLE001 - inspecting its code
                caught[name] = error
            else:
                self.fail(f"{name}: request was accepted")

        # The incompatible-checkpoint path, triggered as test_loader does.
        hparams = yaml.safe_load(get_settings().hparams_path.read_text())
        hparams["dataset"]["name"] = "LightCurveDataLoader"
        with tempfile.TemporaryDirectory() as tmp:
            modified = Path(tmp) / "hparams.yaml"
            modified.write_text(yaml.safe_dump(hparams))
            with self.assertRaises(IncompatibleCheckpointError) as load_failure:
                load_model(get_settings().checkpoint_path, modified)
        caught["incompatible_checkpoint"] = load_failure.exception

        # Each error's code equals the documented string for its path.
        codes = {name: getattr(error, "code", None) for name, error in caught.items()}
        self.assertEqual(codes, {name: name for name in caught})
        self.assertEqual(len(set(codes.values())), 4)
