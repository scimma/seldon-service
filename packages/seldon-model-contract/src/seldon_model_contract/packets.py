"""What the service sends a package: one packet per object.

A packet holds the object's photometry (detections and, where the source
reports them, non-detections), any scalar metadata, and optional
extensions for modalities later contract versions add (spectra, host-galaxy
data). Every point names its survey and band, so a package can tell a ZTF
``g`` point from an LSST ``g`` point and exclude what it does not accept.

A missing zero point is representable (``None``) on purpose: the package, not
the packet, decides that such an object is refused with
``missing_zero_point``.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from seldon_model_contract import _checks


@dataclass(frozen=True)
class Point:
    """One photometric measurement.

    Attributes:
        survey: The survey that measured it, lower case (``"lsst"``, ``"ztf"``).
        band: The band name in that survey's spelling (``"g"``, ``"F062"``).
        time: Observation time, MJD (TAI).
        flux: Flux, in units set by ``zero_point``.
        flux_err: One-sigma flux uncertainty, same units.
        zero_point: AB magnitude zero point of ``flux``, or ``None`` when the
            source did not state one.
        detected: Whether the source reported a detection at this time.
            ``False`` marks a forced or upper-limit measurement.
    """

    survey: str
    band: str
    time: float
    flux: float
    flux_err: float
    zero_point: float | None
    detected: bool = True

    @classmethod
    def from_dict(cls, data: Any, where: str = "point") -> "Point":
        """Build a point from its JSON form.

        Args:
            data: An object with ``survey``, ``band``, ``time``, ``flux``,
                ``flux_err``, and ``zero_point``.
            where: The point's location, for error messages.

        Returns:
            The point.

        Raises:
            ValueError: When a required field is missing or malformed.
        """
        data = _checks.mapping(data, where)
        zero_point = data.get("zero_point")
        detected = data.get("detected", True)
        if not isinstance(detected, bool):
            raise ValueError(f"{where}.detected must be true or false")
        return cls(
            survey=_checks.required_text(data, "survey", where),
            band=_checks.required_text(data, "band", where),
            time=_checks.required_number(data, "time", where),
            flux=_checks.required_number(data, "flux", where),
            flux_err=_checks.required_number(data, "flux_err", where),
            zero_point=(
                None
                if zero_point is None
                else _checks.number(zero_point, f"{where}.zero_point")
            ),
            detected=detected,
        )

    def to_dict(self) -> dict[str, Any]:
        """Return the point's JSON form."""
        return {
            "survey": self.survey,
            "band": self.band,
            "time": self.time,
            "flux": self.flux,
            "flux_err": self.flux_err,
            "zero_point": self.zero_point,
            "detected": self.detected,
        }


@dataclass(frozen=True)
class Packet:
    """Everything the service sends a package about one object.

    Attributes:
        object_id: The service's identifier for the object, unique in a batch.
        points: The object's photometry, in no required order.
        metadata: Scalar facts about the object (strings, numbers, booleans).
        extensions: Additional modalities keyed by name. A package ignores
            names it does not know.
    """

    object_id: str
    points: tuple[Point, ...]
    metadata: Mapping[str, Any] = field(default_factory=dict)
    extensions: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))
        object.__setattr__(self, "extensions", MappingProxyType(dict(self.extensions)))

    @classmethod
    def from_dict(cls, data: Any) -> "Packet":
        """Build a packet from its JSON form.

        Keys other than ``object_id``, ``points``, ``metadata``, and
        ``extensions`` are ignored, so a newer service can add fields an
        older package does not read.

        Args:
            data: The packet object.

        Returns:
            The packet.

        Raises:
            ValueError: When a required field is missing or malformed.
        """
        data = _checks.mapping(data, "packet")
        object_id = _checks.text(
            _checks.required(data, "object_id", "packet"), "packet.object_id"
        )
        where = f"packet[{object_id}]"
        points = _checks.sequence(
            _checks.required(data, "points", where), f"{where}.points"
        )
        metadata = _checks.mapping(data.get("metadata") or {}, f"{where}.metadata")
        for key, value in metadata.items():
            if value is not None and not isinstance(value, (str, int, float, bool)):
                raise ValueError(f"{where}.metadata.{key} must be a scalar")
        return cls(
            object_id=object_id,
            points=tuple(
                Point.from_dict(p, f"{where}.points[{i}]") for i, p in enumerate(points)
            ),
            metadata=metadata,
            extensions=_checks.mapping(
                data.get("extensions") or {}, f"{where}.extensions"
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        """Return the packet's JSON form."""
        return {
            "object_id": self.object_id,
            "points": [p.to_dict() for p in self.points],
            "metadata": dict(self.metadata),
            "extensions": dict(self.extensions),
        }
