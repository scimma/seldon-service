"""Products on demand from the model package installed in this image.

The service process holds no model. Each model package runs in a worker
image of its own, with its own pinned dependencies, so the only place a
product can be computed on request is inside such an image. This call
resolves the one package installed there and asks it directly, without
storing anything; owed detections reach stored outcomes through the forecast
worker instead.

Nothing here imports Django, so the call works from a plain interpreter in
a worker image.
"""

import logging
import os
from collections.abc import Sequence
from functools import cache

from seldon_model_contract import (
    ModelPackage,
    ObjectResult,
    Packet,
    PackageNotFoundError,
    load_package,
    registered_names,
)

from seldon.domain.contract.inputs import resolve_inputs

logger = logging.getLogger("seldon.services")

#: The environment variable naming the package to use, as the worker reads it.
PACKAGE_VARIABLE = "SELDON_WORKER_PACKAGE"


class NoModelPackageError(LookupError):
    """No single model package can be used in this image."""


@cache
def installed_package() -> ModelPackage:
    """Load this image's model package, once per process.

    The package is the one named by ``SELDON_WORKER_PACKAGE`` when it is
    set, otherwise the only package registered under the contract's entry
    point group.

    Returns:
        The package.

    Raises:
        NoModelPackageError: When the named package is not installed, when
            none is installed, or when several are and none is named.
    """
    name = os.environ.get(PACKAGE_VARIABLE, "").strip()
    if not name:
        names = registered_names()
        if not names:
            raise NoModelPackageError(
                "No model package is installed in this image. Products are "
                "computed only in a worker image, which holds one package."
            )
        if len(names) > 1:
            raise NoModelPackageError(
                f"Several model packages are installed ({', '.join(names)}); "
                f"set {PACKAGE_VARIABLE} to the one to use."
            )
        name = names[0]
    try:
        package = load_package(name)
    except PackageNotFoundError as error:
        raise NoModelPackageError(
            f"Model package {name!r} is not installed in this image ({error})."
        ) from error
    logger.info("Loaded model package %s", name)
    return package


def request_products(
    packets: Sequence[Packet], head_names: Sequence[str]
) -> list[ObjectResult]:
    """Ask this image's model package for products, with default inputs.

    Each head's inputs are its declared defaults, resolved for each packet
    (a time grid anchored on the packet's own first and last points), so a
    caller gives only the photometry::

        from seldon import services
        from seldon_model_contract import Packet, Point

        (result,) = services.request_products(
            [
                Packet(
                    object_id="1",
                    points=(
                        Point("lsst", "r", 61000.0, 1200.0, 30.0, 31.4),
                        Point("lsst", "g", 61002.0, 1500.0, 30.0, 31.4),
                    ),
                )
            ],
            ["flux_forecast", "classification"],
        )
        result.outcomes["classification"]  # a Product or a Refusal

    Read each outcome's caveats before trusting a product. Nothing is
    stored. The first call in a process loads the package.

    Args:
        packets: The objects, each with a unique object id.
        head_names: The heads to run; each must be declared by the package.

    Returns:
        One result per packet, in packet order, as the package returned it.

    Raises:
        NoModelPackageError: When this image has no usable model package.
        ValueError: When a head is not declared by the package, or a packet
            has no points to anchor a time grid.
    """
    package = installed_package()
    declaration = package.describe()
    declared = {head.name: head for head in declaration.heads}
    requests = []
    for name in head_names:
        if name not in declared:
            raise ValueError(
                f"{declaration.package_name} {declaration.package_version} "
                f"declares no head named {name!r} (it declares: "
                f"{', '.join(declared)})."
            )
        requests.append(resolve_inputs(declared[name], {}, packets))
    return package.predict(packets, requests)
