"""Resolve a head's input templates into concrete inputs for each object.

A head declares its inputs as templates, such as a time grid relative to an
object's own observations, so one setting serves every object. The operator
may replace a template per kind on a head revision; a kind the revision does
not set keeps the package's default. The package then receives concrete
times and bands per object, never a template, so it need not know how the
service anchors a grid.
"""

from collections.abc import Mapping, Sequence
from typing import Any

from seldon_model_contract import HeadDeclaration, HeadRequest, InputKind, Packet
from seldon_model_contract.heads import parse_template


def head_templates(
    head: HeadDeclaration, revision_inputs: Mapping[str, Any]
) -> dict[InputKind, Any]:
    """Return the templates in effect for a head under a revision.

    Args:
        head: The head as its package declares it.
        revision_inputs: The revision's inputs in their JSON form, by kind
            name; ``{}`` keeps every default.

    Returns:
        A template for each input kind the head declares: a ``TimeGrid`` for
        ``times``, a tuple of ``(survey, band)`` pairs for ``bands``.

    Raises:
        ValueError: If the revision sets a kind the head does not take, or a
            template that does not fit its kind.
    """
    templates = dict(head.inputs)
    for key, value in revision_inputs.items():
        kind = InputKind(key)
        if kind not in templates:
            raise ValueError(f"Head {head.name} takes no {key} input.")
        templates[kind] = parse_template(kind, value, f"{head.name}.inputs.{key}")
    return templates


def resolve_inputs(
    head: HeadDeclaration,
    revision_inputs: Mapping[str, Any],
    packets: Sequence[Packet],
) -> HeadRequest:
    """Build a head request with concrete inputs for each packet.

    A time grid resolves against each packet's earliest and latest point;
    a band list is the same for every packet.

    Args:
        head: The head as its package declares it.
        revision_inputs: The revision's inputs in their JSON form, by kind
            name; ``{}`` keeps every default.
        packets: The batch the request goes with.

    Returns:
        The request, keyed by each packet's object id.

    Raises:
        ValueError: If the revision's inputs do not fit the head, or a time
            grid must resolve against a packet with no points.
    """
    templates = head_templates(head, revision_inputs)
    inputs: dict[str, dict[InputKind, tuple[Any, ...]]] = {}
    for packet in packets:
        concrete: dict[InputKind, tuple[Any, ...]] = {}
        for kind, template in templates.items():
            if kind is InputKind.TIMES:
                if not packet.points:
                    raise ValueError(
                        f"Packet {packet.object_id} has no points to anchor "
                        "a time grid."
                    )
                times = [point.time for point in packet.points]
                concrete[kind] = template.resolve(min(times), max(times))
            else:
                concrete[kind] = tuple(template)
        inputs[packet.object_id] = concrete
    return HeadRequest(head=head.name, inputs=inputs)
