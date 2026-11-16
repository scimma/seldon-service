"""SPEC.md and the Python types name the same codes and kinds.

The specification is the normative text and the types implement it, so a
code added to one and not the other is caught here.
"""

import re
import unittest
from pathlib import Path

from seldon_model_contract import (
    CONTRACT_VERSION,
    CaveatCode,
    InputKind,
    OutputKind,
    RefusalCode,
)

SPEC = Path(__file__).resolve().parents[1] / "SPEC.md"


def section(number: int) -> str:
    text = SPEC.read_text(encoding="utf-8")
    match = re.search(rf"^## {number}\. .*?(?=^## |\Z)", text, re.M | re.S)
    assert match, f"SPEC.md has no section {number}"
    return match.group(0)


def table_codes(text: str) -> set[str]:
    return set(re.findall(r"^\| `([a-z_]+)` \|", text, re.M))


class SpecMatchesTypesTests(unittest.TestCase):
    def test_title_names_the_contract_version(self) -> None:
        first_line = SPEC.read_text(encoding="utf-8").splitlines()[0]
        self.assertIn(f"version {CONTRACT_VERSION}", first_line)

    def test_refusal_table_matches_vocabulary(self) -> None:
        self.assertEqual(table_codes(section(8)), {c.value for c in RefusalCode})

    def test_caveat_table_matches_vocabulary(self) -> None:
        self.assertEqual(table_codes(section(9)), {c.value for c in CaveatCode})

    def test_output_kind_table_matches_types(self) -> None:
        self.assertEqual(table_codes(section(6)), {k.value for k in OutputKind})

    def test_input_kind_table_matches_types(self) -> None:
        self.assertEqual(table_codes(section(5)), {k.value for k in InputKind})


if __name__ == "__main__":
    unittest.main()
