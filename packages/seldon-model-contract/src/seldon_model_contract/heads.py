"""What a package says it can produce, and what the service asks it for.

A package's ``Declaration`` lists the survey and band pairs it accepts and
its decoder heads. Each head names its output kind and the input kinds it
needs, with default inputs the service uses until an operator sets others.

Inputs come in two forms. A head declares *templates* (a time grid relative
to an object's observations, a list of bands); the service resolves them per
object, so the ``HeadRequest`` a package receives carries concrete inputs for
each object. A new input kind is a contract-version change.
"""

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any

from seldon_model_contract import _checks
from seldon_model_contract.version import ContractVersion


class OutputKind(str, Enum):
    """The fixed set of product shapes in this contract version."""

    FLUX_SERIES = "flux_series"
    """Flux and uncertainty at requested (time, band) points. Payload:
    ``times``, ``bands`` (``[survey, band]`` pairs), ``flux`` and
    ``flux_err`` as aligned lists, plus a single ``zero_point`` (the AB
    magnitude of the returned flux)."""

    CLASS_PROBABILITIES = "class_probabilities"
    """Probabilities over named classes. Payload: ``probabilities`` (class
    name to probability)."""

    SCALAR = "scalar"
    """One value with an uncertainty, such as a redshift. Payload: ``value``,
    ``uncertainty``, and an optional ``unit``."""

    NAMED_VALUES = "named_values"
    """Named numbers without uncertainties, such as fitted parameters.
    Payload: ``values`` (name to number)."""


class InputKind(str, Enum):
    """The fixed set of head input kinds in this contract version."""

    TIMES = "times"
    """Times to evaluate at, MJD (TAI). Declared as a ``TimeGrid`` template."""

    BANDS = "bands"
    """``[survey, band]`` pairs to evaluate in. Declared as a list of pairs."""


@dataclass(frozen=True)
class TimeGrid:
    """A grid of times relative to an object's own observations.

    Attributes:
        start_days: Offset of the first time from the first observation.
        days_past_last: How far past the last observation the grid reaches.
        step_days: Spacing between grid times. Must be positive.
    """

    start_days: float
    days_past_last: float
    step_days: float

    def __post_init__(self) -> None:
        if not self.step_days > 0:
            raise ValueError("time_grid.step_days must be positive")

    def resolve(self, first_time: float, last_time: float) -> tuple[float, ...]:
        """Return the concrete grid for an object.

        Args:
            first_time: The object's earliest observation, MJD.
            last_time: The object's latest observation, MJD.

        Returns:
            Grid times from ``first_time + start_days`` through
            ``last_time + days_past_last`` inclusive, ``step_days`` apart.
        """
        start = first_time + self.start_days
        end = last_time + self.days_past_last
        if end < start:
            return ()
        count = int(math.floor((end - start) / self.step_days + 1e-9)) + 1
        return tuple(start + i * self.step_days for i in range(count))

    @classmethod
    def from_dict(cls, data: Any, where: str = "time_grid") -> "TimeGrid":
        """Build a template from its JSON form.

        Args:
            data: An object with ``start_days``, ``days_past_last``, and
                ``step_days``.
            where: The template's location, for error messages.

        Returns:
            The template.

        Raises:
            ValueError: When a field is missing or malformed.
        """
        data = _checks.mapping(data, where)
        return cls(
            start_days=_checks.required_number(data, "start_days", where),
            days_past_last=_checks.number(
                _checks.required(data, "days_past_last", where),
                f"{where}.days_past_last",
            ),
            step_days=_checks.required_number(data, "step_days", where),
        )

    def to_dict(self) -> dict[str, float]:
        """Return the template's JSON form."""
        return {
            "start_days": self.start_days,
            "days_past_last": self.days_past_last,
            "step_days": self.step_days,
        }


def _bands(value: Any, where: str) -> tuple[tuple[str, str], ...]:
    items = _checks.sequence(value, where)
    if not items:
        raise ValueError(f"{where} must name at least one band")
    return tuple(_checks.pair(b, f"{where}[{i}]") for i, b in enumerate(items))


def parse_template(kind: InputKind, value: Any, where: str) -> Any:
    """Parse one input template of the given kind.

    Args:
        kind: The input kind.
        value: Its JSON form.
        where: The location, for error messages.

    Returns:
        A ``TimeGrid`` for ``times``, or a tuple of pairs for ``bands``.

    Raises:
        ValueError: When the value does not fit the kind.
    """
    if kind is InputKind.TIMES:
        return TimeGrid.from_dict(value, where)
    return _bands(value, where)


def _template_to_json(value: Any) -> Any:
    if isinstance(value, TimeGrid):
        return value.to_dict()
    return [list(p) for p in value]


@dataclass(frozen=True)
class HeadDeclaration:
    """One decoder head a package offers.

    Attributes:
        name: The head's name, unique within the package (``"flux_forecast"``).
        output_kind: The shape of the product it returns.
        inputs: Default input templates by kind. Its keys are the input kinds
            the head needs; a head that needs none has an empty mapping.
        description: One sentence for operators and the developer guide.
    """

    name: str
    output_kind: OutputKind
    inputs: Mapping[InputKind, Any] = field(default_factory=dict)
    description: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "output_kind", OutputKind(self.output_kind))
        inputs = {InputKind(kind): value for kind, value in self.inputs.items()}
        object.__setattr__(self, "inputs", MappingProxyType(inputs))

    @classmethod
    def from_dict(cls, data: Any, where: str = "head") -> "HeadDeclaration":
        """Build a head declaration from its JSON form.

        Args:
            data: An object with ``name``, ``output_kind``, ``inputs``, and an
                optional ``description``.
            where: The head's location, for error messages.

        Returns:
            The declaration.

        Raises:
            ValueError: When a field is missing, malformed, or of unknown kind.
        """
        data = _checks.mapping(data, where)
        name = _checks.required_text(data, "name", where)
        where = f"head[{name}]"
        output_kind = _checks.enum_member(
            OutputKind,
            _checks.required(data, "output_kind", where),
            f"{where}.output_kind",
        )
        inputs: dict[InputKind, Any] = {}
        for key, value in _checks.mapping(data.get("inputs") or {}, where).items():
            kind = _checks.enum_member(InputKind, key, f"{where}.inputs.{key}")
            inputs[kind] = parse_template(kind, value, f"{where}.inputs.{key}")
        return cls(
            name=name,
            output_kind=output_kind,
            inputs=inputs,
            description=str(data.get("description") or ""),
        )

    def to_dict(self) -> dict[str, Any]:
        """Return the declaration's JSON form."""
        return {
            "name": self.name,
            "output_kind": self.output_kind.value,
            "inputs": {k.value: _template_to_json(v) for k, v in self.inputs.items()},
            "description": self.description,
        }


@dataclass(frozen=True)
class Declaration:
    """Everything a package says about itself.

    Attributes:
        package_name: The distribution name (``"seldon-reference-model"``).
        package_version: The release version, immutable once published.
        contract_version: The contract version the package implements.
        accepted: The ``(survey, band)`` pairs the package can use. Points
            from any other pair are excluded with a caveat, and an object
            with none of these pairs is refused.
        heads: The heads the package offers, with unique names.
    """

    package_name: str
    package_version: str
    contract_version: str
    accepted: frozenset[tuple[str, str]]
    heads: tuple[HeadDeclaration, ...]

    def __post_init__(self) -> None:
        ContractVersion.parse(self.contract_version)
        names = [h.name for h in self.heads]
        if len(names) != len(set(names)):
            raise ValueError("declaration.heads must have unique names")
        if not self.accepted:
            raise ValueError("declaration.accepted must name at least one pair")

    def head(self, name: str) -> HeadDeclaration:
        """Return the head called ``name``.

        Args:
            name: The head's name.

        Returns:
            Its declaration.

        Raises:
            KeyError: When the package declares no such head.
        """
        for head in self.heads:
            if head.name == name:
                return head
        raise KeyError(name)

    @classmethod
    def from_dict(cls, data: Any) -> "Declaration":
        """Build a declaration from its JSON form.

        Args:
            data: The declaration object.

        Returns:
            The declaration.

        Raises:
            ValueError: When a field is missing or malformed.
        """
        data = _checks.mapping(data, "declaration")
        where = "declaration"
        heads = _checks.sequence(_checks.required(data, "heads", where), "heads")
        accepted = _checks.sequence(
            _checks.required(data, "accepted", where), "accepted"
        )
        return cls(
            package_name=_checks.text(
                _checks.required(data, "package_name", where), "package_name"
            ),
            package_version=_checks.text(
                _checks.required(data, "package_version", where), "package_version"
            ),
            contract_version=_checks.text(
                _checks.required(data, "contract_version", where), "contract_version"
            ),
            accepted=frozenset(
                _checks.pair(p, f"accepted[{i}]") for i, p in enumerate(accepted)
            ),
            heads=tuple(
                HeadDeclaration.from_dict(h, f"heads[{i}]") for i, h in enumerate(heads)
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        """Return the declaration's JSON form, with pairs sorted."""
        return {
            "package_name": self.package_name,
            "package_version": self.package_version,
            "contract_version": self.contract_version,
            "accepted": [list(p) for p in sorted(self.accepted)],
            "heads": [h.to_dict() for h in self.heads],
        }


@dataclass(frozen=True)
class HeadRequest:
    """A request for one head's product, with concrete inputs per object.

    Attributes:
        head: The head's name.
        inputs: For each object id in the batch, the concrete inputs by kind:
            ``times`` as a tuple of MJD values, ``bands`` as a tuple of
            ``(survey, band)`` pairs. Empty for a head that takes no inputs.
    """

    head: str
    inputs: Mapping[str, Mapping[InputKind, tuple[Any, ...]]] = field(
        default_factory=dict
    )

    def __post_init__(self) -> None:
        frozen = {
            object_id: MappingProxyType(dict(values))
            for object_id, values in self.inputs.items()
        }
        object.__setattr__(self, "inputs", MappingProxyType(frozen))

    def for_object(self, object_id: str) -> Mapping[InputKind, tuple[Any, ...]]:
        """Return the concrete inputs for one object, empty when none.

        Args:
            object_id: The object's id.

        Returns:
            Its inputs by kind.
        """
        return self.inputs.get(object_id, MappingProxyType({}))

    @classmethod
    def from_dict(cls, data: Any) -> "HeadRequest":
        """Build a request from its JSON form.

        Args:
            data: An object with ``head`` and ``inputs`` (object id to an
                object of concrete inputs by kind).

        Returns:
            The request.

        Raises:
            ValueError: When a field is missing or malformed.
        """
        data = _checks.mapping(data, "head_request")
        head = _checks.text(
            _checks.required(data, "head", "head_request"), "head_request.head"
        )
        where = f"head_request[{head}]"
        parsed: dict[str, dict[InputKind, tuple[Any, ...]]] = {}
        for object_id, values in _checks.mapping(
            data.get("inputs") or {}, f"{where}.inputs"
        ).items():
            values = _checks.mapping(values, f"{where}.inputs.{object_id}")
            parsed[object_id] = {}
            for key, value in values.items():
                field_name = f"{where}.inputs.{object_id}.{key}"
                kind = _checks.enum_member(InputKind, key, field_name)
                items: Sequence[Any] = _checks.sequence(value, field_name)
                if kind is InputKind.TIMES:
                    parsed[object_id][kind] = tuple(
                        _checks.number(t, f"{field_name}[{i}]")
                        for i, t in enumerate(items)
                    )
                else:
                    parsed[object_id][kind] = _bands(items, field_name)
        return cls(head=head, inputs=parsed)

    def to_dict(self) -> dict[str, Any]:
        """Return the request's JSON form."""
        return {
            "head": self.head,
            "inputs": {
                object_id: {
                    kind.value: [list(v) if isinstance(v, tuple) else v for v in items]
                    for kind, items in values.items()
                }
                for object_id, values in self.inputs.items()
            },
        }
