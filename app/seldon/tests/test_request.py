"""Tests that ObjectRequest refuses photometry whose fields cannot be paired.

Each refused shape would otherwise reach encoding with times, values, and
bands that do not line up point for point, and the forecast would come back
silently mispaired.
"""

from typing import Any

from django.test import SimpleTestCase

from seldon.domain.models.request import ObjectRequest


def _fields(**overrides: Any) -> dict[str, Any]:
    """Return a well-formed three-point request's fields, with overrides.

    Args:
        **overrides: Fields to replace.

    Returns:
        Keyword arguments for ``ObjectRequest``.
    """
    fields: dict[str, Any] = {
        "times": [0.0, 1.0, 2.0],
        "flux": [100.0, 200.0, 150.0],
        "flux_err": [10.0, 10.0, 10.0],
        "detected": [1, 1, 1],
        "bands": ["r", "g", "r"],
        "eval_times": [3.0, 4.0],
        "eval_bands": ["r", "g"],
        "zero_point": 27.5,
    }
    fields.update(overrides)
    return fields


class ConstructionGuardTests(SimpleTestCase):
    """A request whose fields cannot be paired is refused when built."""

    def test_well_formed_request_is_accepted(self) -> None:
        """The baseline fields build a request, so each refusal below is real."""
        ObjectRequest(**_fields())

    def test_per_observation_fields_of_different_lengths_are_refused(self) -> None:
        """Each per-observation field must have one value per observation."""
        for field, value in [
            ("times", [0.0, 1.0]),
            ("flux", [100.0, 200.0]),
            ("flux_err", [10.0, 10.0]),
            ("detected", [1, 1]),
            ("bands", ["r", "g"]),
        ]:
            with self.subTest(field=field):
                with self.assertRaisesRegex(ValueError, "differ in length"):
                    ObjectRequest(**_fields(**{field: value}))

    def test_eval_times_and_eval_bands_of_different_lengths_are_refused(
        self,
    ) -> None:
        """Every evaluation time needs exactly one evaluation band."""
        with self.assertRaisesRegex(ValueError, "eval_bands"):
            ObjectRequest(**_fields(eval_bands=["r"]))

    def test_multi_dimensional_field_is_refused(self) -> None:
        """A two-dimensional times array is refused, not flattened."""
        with self.assertRaisesRegex(ValueError, "times must be one-dimensional"):
            ObjectRequest(**_fields(times=[[0.0, 1.0, 2.0]]))

    def test_per_observation_zero_point_of_wrong_length_is_refused(self) -> None:
        """A per-observation zero point needs one value per observation."""
        with self.assertRaisesRegex(ValueError, "zero_point has 2 values"):
            ObjectRequest(**_fields(zero_point=[27.5, 25.0]))
