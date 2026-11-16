"""How the service finds a model package and what a package must provide.

A package registers one object under the ``seldon.model_packages`` entry-point
group. That object provides ``describe()`` and ``predict()``, as
``ModelPackage`` states. The service loads it by entry-point name.
"""

from collections.abc import Sequence
from importlib.metadata import entry_points
from typing import Protocol, runtime_checkable

from seldon_model_contract.heads import Declaration, HeadRequest
from seldon_model_contract.packets import Packet
from seldon_model_contract.results import ObjectResult

ENTRY_POINT_GROUP = "seldon.model_packages"


@runtime_checkable
class ModelPackage(Protocol):
    """The two calls every model package provides."""

    def describe(self) -> Declaration:
        """Return what the package accepts and which heads it offers."""
        ...

    def predict(
        self, packets: Sequence[Packet], requests: Sequence[HeadRequest]
    ) -> list[ObjectResult]:
        """Return one result per packet, in packet order.

        Each result holds exactly one outcome for every requested head. An
        object's results must not depend on which other packets share the
        batch.

        Args:
            packets: The objects to process. Object ids are unique.
            requests: The heads to run, each with concrete inputs per object.

        Returns:
            One ``ObjectResult`` per packet, in the same order.
        """
        ...


class PackageNotFoundError(LookupError):
    """No package is registered under the requested name."""


def registered_names() -> list[str]:
    """Return the names of every installed model package, sorted."""
    return sorted(ep.name for ep in entry_points(group=ENTRY_POINT_GROUP))


def load_package(name: str) -> ModelPackage:
    """Load the model package registered under ``name``.

    The entry point may name a ``ModelPackage`` instance, or a class or
    factory that returns one when called with no arguments.

    Args:
        name: The entry-point name.

    Returns:
        The package.

    Raises:
        PackageNotFoundError: When no package is registered under ``name``.
        TypeError: When the entry point does not provide a ``ModelPackage``.
    """
    matches = [ep for ep in entry_points(group=ENTRY_POINT_GROUP) if ep.name == name]
    if not matches:
        available = ", ".join(registered_names()) or "none"
        raise PackageNotFoundError(
            f"no model package named {name!r} is installed (installed: {available})"
        )
    target = matches[0].load()
    package = target
    if isinstance(target, type) or (
        not isinstance(target, ModelPackage) and callable(target)
    ):
        package = target()
    if not isinstance(package, ModelPackage):
        raise TypeError(
            f"entry point {name!r} does not provide describe() and predict()"
        )
    return package
