"""Golden-output test: the fixed light curves reproduce their recorded forecast.

R14. A ``seldon_core`` tag bump, a different checkpoint, or a torch upgrade can
change the forecast without breaking anything else: the model still loads,
runs, and returns plausible numbers. This module makes that change visible.
The checkpoint is baked into the image, so this runs everywhere, with no tag
and no skip.

Provenance of the reference values
----------------------------------
``fixtures/reference_outputs.py`` was written by
``fixtures.reference_light_curves.record()`` running in the image with no
network, against the versions it records in ``RECORDED_WITH``. The procedure,
and when to repeat it, is in ``docs/developer/regenerating-reference-outputs.md``.
Never re-record to make this module pass without knowing why it failed.

Tolerance
---------
Set from the smallest change the test must catch, not from the spread:

- Must catch: a relative change of ``SMALLEST_RELATIVE_CHANGE_TO_CATCH``
  (1e-3, about 1 mmag) in any forecast flux or flux error, or an absolute
  change of ``SMALLEST_PROBABILITY_CHANGE_TO_CATCH`` (1e-3) in any class
  probability. That is ten times below the best photometric precision a
  researcher works at (about 1%), so any change a researcher could notice
  fails here.
- Must exceed: the spread between runs that should agree. Repeated passes
  are bit-identical (zero spread). A different torch thread count, or the
  same object forecast alone rather than batched, moves flux and flux error
  by up to about 1.3e-5 relative and probabilities by up to about 2.4e-6.

``RELATIVE_TOLERANCE`` and ``PROBABILITY_TOLERANCE`` (both 1e-4) sit a decade
below the change to catch and about eight times above the measured spread.

R16 rejection paths
-------------------
Covered by existing tests rather than repeated here:

- R4, incompatible checkpoint (``IncompatibleCheckpointError``):
  ``test_loader.CompatibilityGuardTests.test_rejects_dataloader_that_computes_stats_from_data``.
- R8, missing zero point (``MissingZeroPointError``):
  ``test_validation.ValidationTests.test_missing_zero_point_is_rejected_by_name``.
- R11, photometry the checkpoint cannot serve (``UnknownBandError``,
  ``NoUsableObservationsError``):
  ``test_validation.ValidationTests.test_unknown_observation_band_is_rejected_by_name``,
  ``test_unknown_evaluation_band_alone_is_rejected_by_name``, and
  ``test_no_usable_observations_is_rejected``.

``test_validation.ValidationTests.test_each_rejection_carries_its_distinct_documented_code``
raises all four in one place and checks each carries its own code.
"""

import copy
from typing import Any

import numpy as np
import torch
from django.test import SimpleTestCase

from seldon import services
from seldon.domain.models.forecast import ObjectForecast
from seldon.tests.fixtures.reference_light_curves import reference_light_curves
from seldon.tests.fixtures.reference_outputs import RECORDED_WITH, REFERENCE_OUTPUTS

SMALLEST_RELATIVE_CHANGE_TO_CATCH = 1e-3
SMALLEST_PROBABILITY_CHANGE_TO_CATCH = 1e-3
RELATIVE_TOLERANCE = 1e-4
PROBABILITY_TOLERANCE = 1e-4

# A thread count other than the configured one, for the spread measurement.
OTHER_THREAD_COUNT = 4


def _forecast_fixtures() -> dict[str, ObjectForecast]:
    """Forecast every fixture in one batch, as ``record()`` does.

    Returns:
        Each fixture's forecast, keyed by fixture name.
    """
    requests = reference_light_curves()
    return dict(zip(requests, services.forecast(list(requests.values()))))


def _assert_matches(
    result: ObjectForecast, reference: dict[str, Any], label: str
) -> None:
    """Compare one forecast with its recorded reference.

    Args:
        result: The forecast just computed.
        reference: The recorded ``flux``, ``flux_err``, and
            ``class_probabilities``.
        label: Names the fixture in a failure message.

    Raises:
        AssertionError: If any value lies outside its tolerance.
    """
    context = (
        f"{label} moved. Recorded with {RECORDED_WITH}; now running "
        f"{result.provenance}. See docs/developer/regenerating-reference-outputs.md."
    )
    for field in ("flux", "flux_err"):
        np.testing.assert_allclose(
            getattr(result, field),
            reference[field],
            rtol=RELATIVE_TOLERANCE,
            atol=0.0,
            err_msg=f"{field}: {context}",
        )
    probabilities = reference["class_probabilities"]
    if set(result.class_probabilities) != set(probabilities):
        raise AssertionError(f"class names differ: {context}")
    names = sorted(probabilities)
    np.testing.assert_allclose(
        [result.class_probabilities[name] for name in names],
        [probabilities[name] for name in names],
        rtol=0.0,
        atol=PROBABILITY_TOLERANCE,
        err_msg=f"class_probabilities: {context}",
    )


def _largest_deviation(
    results: dict[str, ObjectForecast],
) -> tuple[float, float]:
    """Measure how far forecasts lie from the recorded references.

    Args:
        results: Forecasts keyed by fixture name.

    Returns:
        The largest relative flux or flux-error deviation, and the largest
        absolute class-probability deviation.
    """
    relative, probability = 0.0, 0.0
    for name, reference in REFERENCE_OUTPUTS.items():
        result = results[name]
        for field in ("flux", "flux_err"):
            recorded = np.asarray(reference[field])
            change = np.abs(getattr(result, field) - recorded) / np.abs(recorded)
            relative = max(relative, float(change.max()))
        for label, value in reference["class_probabilities"].items():
            change = abs(result.class_probabilities[label] - value)
            probability = max(probability, change)
    return relative, probability


class ReferenceOutputTests(SimpleTestCase):
    """R14: every fixed light curve reproduces its recorded forecast."""

    @classmethod
    def setUpClass(cls) -> None:
        """Forecast the fixtures once for the whole class."""
        super().setUpClass()
        cls.results = _forecast_fixtures()

    def test_every_fixture_is_covered_by_a_reference(self) -> None:
        """The fixture set and the reference table name the same light curves.

        Guards the comparison below from passing vacuously if either side
        loses an entry.
        """
        self.assertEqual(set(reference_light_curves()), set(REFERENCE_OUTPUTS))
        self.assertEqual(set(self.results), set(REFERENCE_OUTPUTS))
        self.assertGreater(len(REFERENCE_OUTPUTS), 0)

    def test_each_light_curve_reproduces_its_recorded_forecast(self) -> None:
        """Covers R14: flux, flux error, and class probabilities, per fixture."""
        for name, reference in REFERENCE_OUTPUTS.items():
            with self.subTest(fixture=name):
                _assert_matches(self.results[name], reference, name)

    def test_tolerance_is_wider_than_the_run_to_run_spread(self) -> None:
        """A repeat and a run at another thread count both stay inside it."""
        repeated = _largest_deviation(_forecast_fixtures())
        configured = torch.get_num_threads()
        torch.set_num_threads(OTHER_THREAD_COUNT)
        try:
            other_threads = _largest_deviation(_forecast_fixtures())
        finally:
            torch.set_num_threads(configured)

        for relative, probability in (repeated, other_threads):
            self.assertLess(relative, RELATIVE_TOLERANCE)
            self.assertLess(probability, PROBABILITY_TOLERANCE)

    def test_perturbed_reference_value_fails_the_comparison(self) -> None:
        """A change of the smallest size to catch, in any one value, fails."""
        for name, reference in REFERENCE_OUTPUTS.items():
            result = self.results[name]
            for field in ("flux", "flux_err"):
                with self.subTest(fixture=name, field=field):
                    perturbed = copy.deepcopy(reference)
                    perturbed[field][0] *= 1.0 + SMALLEST_RELATIVE_CHANGE_TO_CATCH
                    with self.assertRaises(AssertionError):
                        _assert_matches(result, perturbed, name)
            with self.subTest(fixture=name, field="class_probabilities"):
                perturbed = copy.deepcopy(reference)
                perturbed["class_probabilities"][
                    result.predicted_class
                ] -= SMALLEST_PROBABILITY_CHANGE_TO_CATCH
                with self.assertRaises(AssertionError):
                    _assert_matches(result, perturbed, name)
