"""What a package returns: for each object, one outcome per requested head.

An outcome is either a ``Product`` (the head's output, with any caveats) or a
``Refusal`` (no output, with a reason). A package never returns a product it
could not defend: when an input is outside what it can serve, it refuses or
says so in a caveat.

Codes outside the shared vocabulary still load, so a reader keeps what a
newer package said; ``is_known`` reports whether a code is in this version's
vocabulary.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from seldon_model_contract import _checks
from seldon_model_contract.heads import OutputKind
from seldon_model_contract.vocabulary import is_known_caveat, is_known_refusal


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({k: _freeze(v) for k, v in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(v) for v in value)
    return value


def _thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {k: _thaw(v) for k, v in value.items()}
    if isinstance(value, tuple):
        return [_thaw(v) for v in value]
    return value


@dataclass(frozen=True)
class Caveat:
    """A note that a product should be read with care.

    Attributes:
        code: A ``CaveatCode`` value, or a newer code this reader does not know.
        detail: Structured facts behind the caveat (counts, band pairs).
    """

    code: str
    detail: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "detail", _freeze(dict(self.detail)))

    @property
    def is_known(self) -> bool:
        """Whether the code is in this contract version's vocabulary."""
        return is_known_caveat(self.code)

    @classmethod
    def from_dict(cls, data: Any, where: str = "caveat") -> "Caveat":
        """Build a caveat from its JSON form.

        Args:
            data: An object with ``code`` and optional ``detail``.
            where: The caveat's location, for error messages.

        Returns:
            The caveat.

        Raises:
            ValueError: When ``code`` is missing or ``detail`` malformed.
        """
        data = _checks.mapping(data, where)
        return cls(
            code=_checks.text(_checks.required(data, "code", where), f"{where}.code"),
            detail=_checks.mapping(data.get("detail") or {}, f"{where}.detail"),
        )

    def to_dict(self) -> dict[str, Any]:
        """Return the caveat's JSON form."""
        return {"code": self.code, "detail": _thaw(self.detail)}


def _check_payload(kind: OutputKind, payload: Mapping[str, Any], where: str) -> None:
    """Raise ``ValueError`` when ``payload`` does not fit ``kind``."""
    if kind is OutputKind.FLUX_SERIES:
        lengths = set()
        for key in ("times", "bands", "flux", "flux_err"):
            items = _checks.sequence(
                _checks.required(payload, key, where), f"{where}.{key}"
            )
            lengths.add(len(items))
            for i, item in enumerate(items):
                if key == "bands":
                    _checks.pair(item, f"{where}.bands[{i}]")
                else:
                    _checks.number(item, f"{where}.{key}[{i}]")
        if len(lengths) != 1:
            raise ValueError(f"{where}: times, bands, flux and flux_err must align")
        _checks.number(
            _checks.required(payload, "zero_point", where), f"{where}.zero_point"
        )
    elif kind is OutputKind.CLASS_PROBABILITIES:
        probabilities = _checks.mapping(
            _checks.required(payload, "probabilities", where),
            f"{where}.probabilities",
        )
        for name, value in probabilities.items():
            _checks.number(value, f"{where}.probabilities.{name}")
    elif kind is OutputKind.SCALAR:
        _checks.number(_checks.required(payload, "value", where), f"{where}.value")
        _checks.number(
            _checks.required(payload, "uncertainty", where), f"{where}.uncertainty"
        )
    else:
        values = _checks.mapping(
            _checks.required(payload, "values", where), f"{where}.values"
        )
        for name, value in values.items():
            _checks.number(value, f"{where}.values.{name}")


@dataclass(frozen=True)
class Product:
    """A head's output for one object.

    Attributes:
        kind: The output kind, matching the head's declaration.
        payload: The output, shaped as ``kind`` documents. Extra keys are
            allowed and kept.
        caveats: Notes on how to read this product. Empty when none apply.
    """

    kind: OutputKind
    payload: Mapping[str, Any]
    caveats: tuple[Caveat, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", OutputKind(self.kind))
        _check_payload(self.kind, self.payload, f"product[{self.kind.value}]")
        object.__setattr__(self, "payload", _freeze(dict(self.payload)))
        object.__setattr__(self, "caveats", tuple(self.caveats))

    def to_dict(self) -> dict[str, Any]:
        """Return the product's JSON form."""
        return {
            "outcome": "product",
            "kind": self.kind.value,
            "payload": _thaw(self.payload),
            "caveats": [c.to_dict() for c in self.caveats],
        }


@dataclass(frozen=True)
class Refusal:
    """No output for one object and head, with the reason.

    Attributes:
        code: A ``RefusalCode`` value, or a newer code this reader does not know.
        message: A human-readable explanation.
        detail: Structured facts behind the refusal (such as band pairs).
    """

    code: str
    message: str
    detail: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "detail", _freeze(dict(self.detail)))

    @property
    def is_known(self) -> bool:
        """Whether the code is in this contract version's vocabulary."""
        return is_known_refusal(self.code)

    def to_dict(self) -> dict[str, Any]:
        """Return the refusal's JSON form."""
        return {
            "outcome": "refusal",
            "code": self.code,
            "message": self.message,
            "detail": _thaw(self.detail),
        }


Outcome = Product | Refusal


def outcome_from_dict(data: Any, where: str = "outcome") -> Outcome:
    """Build a product or refusal from its JSON form.

    Args:
        data: An object whose ``outcome`` is ``"product"`` or ``"refusal"``.
        where: The outcome's location, for error messages.

    Returns:
        The product or refusal.

    Raises:
        ValueError: When the outcome type is unknown or a field is malformed.
    """
    data = _checks.mapping(data, where)
    outcome = data.get("outcome")
    if outcome == "product":
        try:
            kind = OutputKind(_checks.required(data, "kind", where))
        except ValueError as error:
            raise ValueError(f"{where}.kind is not a known kind") from error
        caveats = _checks.sequence(data.get("caveats") or [], f"{where}.caveats")
        return Product(
            kind=kind,
            payload=_checks.mapping(
                _checks.required(data, "payload", where), f"{where}.payload"
            ),
            caveats=tuple(
                Caveat.from_dict(c, f"{where}.caveats[{i}]")
                for i, c in enumerate(caveats)
            ),
        )
    if outcome == "refusal":
        return Refusal(
            code=_checks.text(_checks.required(data, "code", where), f"{where}.code"),
            message=str(data.get("message") or ""),
            detail=_checks.mapping(data.get("detail") or {}, f"{where}.detail"),
        )
    raise ValueError(f"{where}.outcome must be 'product' or 'refusal'")


@dataclass(frozen=True)
class ObjectResult:
    """Every requested head's outcome for one object.

    Attributes:
        object_id: The packet's ``object_id``.
        outcomes: One outcome per requested head, by head name.
    """

    object_id: str
    outcomes: Mapping[str, Outcome]

    def __post_init__(self) -> None:
        object.__setattr__(self, "outcomes", MappingProxyType(dict(self.outcomes)))

    @classmethod
    def from_dict(cls, data: Any) -> "ObjectResult":
        """Build a result from its JSON form.

        Args:
            data: An object with ``object_id`` and ``outcomes`` (head name to
                outcome).

        Returns:
            The result.

        Raises:
            ValueError: When a field is missing or malformed.
        """
        data = _checks.mapping(data, "result")
        object_id = _checks.text(
            _checks.required(data, "object_id", "result"), "result.object_id"
        )
        where = f"result[{object_id}]"
        outcomes = _checks.mapping(
            _checks.required(data, "outcomes", where), f"{where}.outcomes"
        )
        return cls(
            object_id=object_id,
            outcomes={
                head: outcome_from_dict(value, f"{where}.outcomes.{head}")
                for head, value in outcomes.items()
            },
        )

    def to_dict(self) -> dict[str, Any]:
        """Return the result's JSON form."""
        return {
            "object_id": self.object_id,
            "outcomes": {head: o.to_dict() for head, o in self.outcomes.items()},
        }
