"""Small input checks shared by the contract's ``from_dict`` constructors.

Each raises ``ValueError`` naming the field, so a model team or the service
sees which part of a payload is wrong.
"""

import math
from collections.abc import Mapping, Sequence
from typing import Any


def mapping(value: Any, field: str) -> Mapping[str, Any]:
    """Return ``value`` if it is a mapping with string keys.

    Args:
        value: The value to check.
        field: The field name for the error message.

    Returns:
        The value.

    Raises:
        ValueError: When it is not a mapping with string keys.
    """
    if not isinstance(value, Mapping) or not all(isinstance(k, str) for k in value):
        raise ValueError(f"{field} must be an object with string keys")
    return value


def required(data: Mapping[str, Any], key: str, where: str) -> Any:
    """Return ``data[key]``, raising when the key is absent or null.

    Args:
        data: The object being read.
        key: The required key.
        where: The enclosing object's name for the error message.

    Returns:
        The value.

    Raises:
        ValueError: When the key is missing or its value is ``None``.
    """
    if data.get(key) is None:
        raise ValueError(f"{where}.{key} is required")
    return data[key]


def text(value: Any, field: str) -> str:
    """Return ``value`` if it is a non-empty string.

    Args:
        value: The value to check.
        field: The field name for the error message.

    Returns:
        The string.

    Raises:
        ValueError: When it is not a non-empty string.
    """
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a non-empty string")
    return value


def number(value: Any, field: str) -> float:
    """Return ``value`` as a finite float.

    Args:
        value: The value to check. Booleans are rejected.
        field: The field name for the error message.

    Returns:
        The value as a float.

    Raises:
        ValueError: When it is not a finite real number.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be a number")
    if not math.isfinite(value):
        raise ValueError(f"{field} must be finite")
    return float(value)


def sequence(value: Any, field: str) -> Sequence[Any]:
    """Return ``value`` if it is a list or tuple.

    Args:
        value: The value to check.
        field: The field name for the error message.

    Returns:
        The sequence.

    Raises:
        ValueError: When it is not a list or tuple.
    """
    if not isinstance(value, (list, tuple)):
        raise ValueError(f"{field} must be a list")
    return value


def pair(value: Any, field: str) -> tuple[str, str]:
    """Return a ``[survey, band]`` pair as a tuple of two strings.

    Args:
        value: The value to check.
        field: The field name for the error message.

    Returns:
        The pair.

    Raises:
        ValueError: When it is not two non-empty strings.
    """
    items = sequence(value, field)
    if len(items) != 2:
        raise ValueError(f"{field} must be a [survey, band] pair")
    return text(items[0], f"{field}[0]"), text(items[1], f"{field}[1]")
