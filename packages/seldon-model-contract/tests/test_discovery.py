"""Loading a package by entry-point name."""

import unittest
from importlib.metadata import EntryPoint
from unittest import mock

from seldon_model_contract import PackageNotFoundError, discovery, load_package


class StubPackage:
    def describe(self):  # noqa: ANN201 - stub
        return None

    def predict(self, packets, requests):  # noqa: ANN001, ANN201 - stub
        return []


STUB = StubPackage()
NOT_A_PACKAGE = {"describe": None}


def fake_entry_points(group: str) -> list[EntryPoint]:
    assert group == discovery.ENTRY_POINT_GROUP
    return [
        EntryPoint("stub", f"{__name__}:STUB", group),
        EntryPoint("stub-class", f"{__name__}:StubPackage", group),
        EntryPoint("not-a-package", f"{__name__}:NOT_A_PACKAGE", group),
    ]


@mock.patch.object(discovery, "entry_points", fake_entry_points)
class LoadPackageTests(unittest.TestCase):
    def test_instance_entry_point_loads(self) -> None:
        self.assertIs(load_package("stub"), STUB)

    def test_class_entry_point_is_instantiated(self) -> None:
        self.assertIsInstance(load_package("stub-class"), StubPackage)

    def test_unregistered_name_lists_installed_names(self) -> None:
        with self.assertRaises(PackageNotFoundError) as caught:
            load_package("missing")
        self.assertIn("stub, stub-class", str(caught.exception))

    def test_entry_point_without_the_two_calls_is_rejected(self) -> None:
        with self.assertRaises(TypeError) as caught:
            load_package("not-a-package")
        self.assertIn("describe() and predict()", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
