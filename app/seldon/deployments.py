"""The registry of model deployments and the operator's head settings.

A worker registers the package release it serves before it claims any work.
Registration is where the service holds a release to its identity: one name
and version must always be the same build (the wheel's sha256), so outcomes
stored under a version can be trusted to come from one artifact, and a
package written for a contract version the service cannot read is turned
away before it produces anything.

Which heads run, and with which inputs, is never decided by a worker. An
operator writes head revisions, which are only ever added, so replicas
cannot overwrite each other and every outcome can name the revision it ran
under. A detection is covered by the revision in effect when it became
owed; re-enabling a head opens a new revision and does not reach back into
the gap.
"""

import json
import logging
from datetime import datetime
from typing import Any

from django.utils import timezone
from seldon_model_contract import (
    CONTRACT_VERSION,
    Declaration,
    HeadDeclaration,
    InputKind,
    TimeGrid,
    is_supported,
)
from seldon_model_contract.heads import parse_template

from seldon.models import HeadRevision, ModelDeployment

logger = logging.getLogger("seldon.deployments")


class DeploymentRefused(Exception):
    """A package release the service will not register."""


def register(declaration: Declaration, digest: str) -> ModelDeployment:
    """Record the release a worker serves, or find it if already recorded.

    Args:
        declaration: The package's declaration.
        digest: The sha256 of the package's wheel, as recorded in the image.

    Returns:
        The deployment for the release.

    Raises:
        DeploymentRefused: When the service does not support the package's
            contract version, when the same name and version are registered
            with a different digest, or when the release has been retired.
    """
    name = declaration.package_name
    version = declaration.package_version
    if not is_supported(declaration.contract_version):
        raise DeploymentRefused(
            f"{name} {version} implements contract "
            f"{declaration.contract_version}, which this service (contract "
            f"{CONTRACT_VERSION}) does not support."
        )
    deployment, created = ModelDeployment.objects.get_or_create(
        package_name=name,
        package_version=version,
        defaults={
            "contract_version": declaration.contract_version,
            "artifact_digest": digest,
            "declaration": declaration.to_dict(),
        },
    )
    if created:
        logger.info("Registered deployment %s with digest %s", deployment, digest)
        return deployment
    if deployment.artifact_digest != digest:
        raise DeploymentRefused(
            f"{name} {version} is already registered with digest "
            f"{deployment.artifact_digest}; this build has digest {digest}. "
            "A release must not change once published; give it a new version."
        )
    if deployment.retired_at is not None:
        raise DeploymentRefused(
            f"{name} {version} was retired at {deployment.retired_at.isoformat()}."
        )
    return deployment


def revision_in_effect(
    deployment: ModelDeployment, head: str, owed_at: datetime
) -> HeadRevision | None:
    """Return the revision that covers a detection owed at ``owed_at``.

    Args:
        deployment: The deployment.
        head: The head's name.
        owed_at: When the detection became owed.

    Returns:
        The latest revision of the head taking effect at or before
        ``owed_at``, or ``None`` when there is none. The detection is due
        for the head only when the revision is enabled.
    """
    return (
        HeadRevision.objects.filter(
            deployment=deployment, head=head, effective_at__lte=owed_at
        )
        .order_by("-effective_at", "-pk")
        .first()
    )


def current_revisions(
    deployment: ModelDeployment, at: datetime | None = None
) -> dict[str, HeadRevision]:
    """Return the revision in effect for each of a deployment's heads.

    Args:
        deployment: The deployment.
        at: The moment to look at; now when omitted.

    Returns:
        Head name to its revision in effect, for heads that have one.
    """
    at = at or timezone.now()
    revisions = (
        HeadRevision.objects.filter(deployment=deployment, effective_at__lte=at)
        .order_by("head", "-effective_at", "-pk")
        .distinct("head")
    )
    return {revision.head: revision for revision in revisions}


def stored_declaration(deployment: ModelDeployment) -> Declaration:
    """Parse the declaration a deployment registered with.

    Args:
        deployment: The deployment.

    Returns:
        Its declaration.

    Raises:
        ValueError: When the stored declaration cannot be parsed, as for a
            deployment from before the model contract.
    """
    try:
        return Declaration.from_dict(deployment.declaration)
    except ValueError as error:
        raise ValueError(
            f"{deployment} has no usable declaration ({error})."
        ) from error


def validate_inputs(declaration: Declaration, head: str, inputs: Any) -> dict:
    """Check head inputs against what the head declares, and normalize them.

    An empty object means the package's own defaults. Otherwise every input
    kind the head declares must be given, and no other, so the inputs never
    mix operator values with defaults.

    Args:
        declaration: The package's declaration.
        head: The head's name.
        inputs: The inputs, as parsed JSON.

    Returns:
        The inputs in their stored JSON form.

    Raises:
        ValueError: When the head is not declared or the inputs do not fit it.
    """
    declared = declared_head(declaration, head)
    if not isinstance(inputs, dict):
        raise ValueError("inputs must be a JSON object")
    if not inputs:
        return {}
    wanted = {kind.value for kind in declared.inputs}
    given = set(inputs)
    if given != wanted:
        raise ValueError(
            f"head {head} takes inputs {sorted(wanted) or 'none'}; "
            f"got {sorted(given)}"
        )
    normalized: dict[str, Any] = {}
    for key, value in inputs.items():
        template = parse_template(InputKind(key), value, f"inputs.{key}")
        if isinstance(template, TimeGrid):
            normalized[key] = template.to_dict()
        else:
            normalized[key] = [list(pair) for pair in template]
    return normalized


def declared_head(declaration: Declaration, head: str) -> HeadDeclaration:
    """Return a head's declaration, refusing a head the package does not list.

    Args:
        declaration: The package's declaration.
        head: The head's name.

    Returns:
        The head's declaration.

    Raises:
        ValueError: When the package declares no such head.
    """
    try:
        return declaration.head(head)
    except KeyError:
        names = ", ".join(h.name for h in declaration.heads)
        raise ValueError(
            f"{declaration.package_name} {declaration.package_version} does not "
            f"declare a head named {head!r} (it declares: {names})."
        ) from None


def set_head(
    deployment: ModelDeployment,
    head: str,
    enabled: bool,
    effective_at: datetime,
    inputs: Any = None,
) -> HeadRevision:
    """Write a new revision enabling or disabling a head.

    Args:
        deployment: The deployment.
        head: The head's name, which the deployment's declaration must list.
        enabled: Whether detections owed from ``effective_at`` are due for it.
        effective_at: The earliest ``owed_at`` the revision covers.
        inputs: The head's input templates as parsed JSON, or ``None`` (or an
            empty object) for the package's defaults. Ignored when disabling.

    Returns:
        The new revision.

    Raises:
        ValueError: When the head is not declared or the inputs do not fit it.
    """
    declaration = stored_declaration(deployment)
    if enabled:
        stored = validate_inputs(declaration, head, {} if inputs is None else inputs)
    else:
        declared_head(declaration, head)
        stored = {}
    revision = HeadRevision.objects.create(
        deployment=deployment,
        head=head,
        enabled=enabled,
        inputs=stored,
        effective_at=effective_at,
    )
    logger.info(
        "%s head %s for %s from %s with inputs %s",
        "Enabled" if enabled else "Disabled",
        head,
        deployment,
        effective_at.isoformat(),
        json.dumps(stored),
    )
    return revision
