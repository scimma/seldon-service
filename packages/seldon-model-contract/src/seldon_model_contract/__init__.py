"""The contract between the SELDON service and a model package.

The service sends a batch of ``Packet`` values and a list of ``HeadRequest``
values; the package returns one ``ObjectResult`` per packet holding a
``Product`` or ``Refusal`` for each requested head. ``SPEC.md`` beside this
distribution is the normative description; these types are its reference
implementation. Nothing here depends on anything outside the standard library.
"""

from seldon_model_contract.discovery import (
    ENTRY_POINT_GROUP,
    ModelPackage,
    PackageNotFoundError,
    load_package,
    registered_names,
)
from seldon_model_contract.heads import (
    Declaration,
    HeadDeclaration,
    HeadRequest,
    InputKind,
    OutputKind,
    TimeGrid,
)
from seldon_model_contract.packets import Packet, Point
from seldon_model_contract.results import (
    Caveat,
    ObjectResult,
    Outcome,
    Product,
    Refusal,
    outcome_from_dict,
)
from seldon_model_contract.version import (
    CONTRACT_VERSION,
    ContractVersion,
    is_supported,
)
from seldon_model_contract.vocabulary import CaveatCode, RefusalCode

__all__ = [
    "CONTRACT_VERSION",
    "ENTRY_POINT_GROUP",
    "Caveat",
    "CaveatCode",
    "ContractVersion",
    "Declaration",
    "HeadDeclaration",
    "HeadRequest",
    "InputKind",
    "ModelPackage",
    "ObjectResult",
    "Outcome",
    "OutputKind",
    "Packet",
    "PackageNotFoundError",
    "Point",
    "Product",
    "Refusal",
    "RefusalCode",
    "TimeGrid",
    "is_supported",
    "load_package",
    "outcome_from_dict",
    "registered_names",
]
