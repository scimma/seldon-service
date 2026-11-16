"""The vocabularies and the version rule."""

import unittest

from seldon_model_contract import CaveatCode, RefusalCode, is_supported
from seldon_model_contract.version import ContractVersion


class VocabularyTests(unittest.TestCase):
    def test_refusal_codes_match_the_service_codes_they_replace(self) -> None:
        self.assertEqual(
            {c.value for c in RefusalCode},
            {
                "missing_zero_point",
                "unknown_band",
                "no_usable_observations",
                "incompatible_checkpoint",
            },
        )

    def test_caveat_codes(self) -> None:
        self.assertIn("excluded_points", {c.value for c in CaveatCode})
        self.assertIn("sparse_light_curve", {c.value for c in CaveatCode})


class VersionTests(unittest.TestCase):
    def test_same_major_older_minor_is_supported(self) -> None:
        self.assertTrue(is_supported("1.0", "1.2"))

    def test_newer_minor_is_not_supported(self) -> None:
        self.assertFalse(is_supported("1.3", "1.2"))

    def test_other_major_is_not_supported(self) -> None:
        self.assertFalse(is_supported("2.0", "1.0"))

    def test_malformed_version_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            ContractVersion.parse("one")


if __name__ == "__main__":
    unittest.main()
