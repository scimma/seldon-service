"""The database side of the forecast worker: claims, leases, and outcomes.

Every database step is short and commits on its own, so no lock or
transaction is ever held while a package runs, and replicas of a deployment
(and other deployments) never wait on each other's inference.

A detection is due for a head when the head's revision in effect at the
detection's ``owed_at`` is enabled. Claim rows for due pairs are created in
one set-based statement, and the unique claim per detection, deployment and
head makes a second insert, or a redelivery, a no-op.

A claim is leased by committing a holder, an expiry and one more attempt
before the package is called. A lease that lapses without an outcome,
because its worker crashed, was killed or hung, can be taken again, and the
attempt it used still counts. Lapsed claims are retried one detection at a
time, so a detection that kills its worker cannot take its batchmates' last
attempts with it. A claim found lapsed with every attempt used becomes a
failure without another call.

An outcome is stored only while the claim is still leased by the worker
that ran it, with the same expiry; a late result from a worker whose lease
was taken over is dropped, so a claim never gets two outcomes.
"""

import logging
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

from django.db import connection, transaction
from django.db.models import F, Min, Q
from django.utils import timezone
from seldon_model_contract import Product, Refusal

from seldon.models import (
    Detection,
    HeadRefusal,
    HeadRevision,
    ModelDeployment,
    OutcomeClaim,
    ProductFailure,
)
from seldon.models import Product as StoredProduct

logger = logging.getLogger("seldon.forecasting")

#: Counter keys for what happened to each settled claim.
PRODUCT = "products"
REFUSAL = "refusals"
FAILURE = "failures"
RETRY = "retries"
LOST = "lost"


@dataclass(frozen=True)
class Settlement:
    """What one leased claim's attempt came to.

    Exactly one of ``outcome`` and ``error`` is set.

    Attributes:
        claim: The claim, as leased (its holder and expiry identify the
            lease).
        outcome: The package's product or refusal.
        error: Why the attempt failed, as ``Type: message``.
        light_curve: The packet sent, in its JSON form, for a product.
    """

    claim: OutcomeClaim
    outcome: Product | Refusal | None = None
    error: str | None = None
    light_curve: dict | None = None


@dataclass(frozen=True)
class Leased:
    """The result of one leasing step.

    Attributes:
        claims: The claims now leased to the worker, by detection then head,
            with their detection and revision loaded.
        failed: Claims found lapsed with every attempt used, now failures.
    """

    claims: list[OutcomeClaim]
    failed: int


def heartbeat(deployment: ModelDeployment) -> datetime | None:
    """Record that a worker for the deployment is running.

    Args:
        deployment: The deployment.

    Returns:
        When the deployment was retired, or ``None`` while it is live.
    """
    rows = ModelDeployment.objects.filter(pk=deployment.pk)
    rows.update(last_heartbeat_at=timezone.now())
    return rows.values_list("retired_at", flat=True).get()


def ensure_claims(deployment: ModelDeployment) -> int:
    """Create a due claim for every owed detection and head now owed one.

    The pair is owed when the head's revision in effect at the detection's
    ``owed_at`` (the latest taking effect at or before it) is enabled. Only
    detections owed since the deployment's earliest enabled revision are
    read, and pairs already claimed are skipped.

    Args:
        deployment: The deployment.

    Returns:
        How many claims were created.
    """
    earliest = HeadRevision.objects.filter(
        deployment=deployment, enabled=True
    ).aggregate(earliest=Min("effective_at"))["earliest"]
    if earliest is None:
        return 0
    sql = f"""
        INSERT INTO {OutcomeClaim._meta.db_table}
            (detection_id, deployment_id, head, revision_id, state, attempts,
             leased_by, lease_expires_at, created_at, updated_at)
        SELECT d.id, %(deployment)s, r.head, r.id, %(due)s, 0, '', NULL,
               now(), now()
        FROM {Detection._meta.db_table} d
        CROSS JOIN (
            SELECT DISTINCT head FROM {HeadRevision._meta.db_table}
            WHERE deployment_id = %(deployment)s AND enabled
        ) h
        CROSS JOIN LATERAL (
            SELECT rev.id, rev.head, rev.enabled
            FROM {HeadRevision._meta.db_table} rev
            WHERE rev.deployment_id = %(deployment)s
              AND rev.head = h.head
              AND rev.effective_at <= d.owed_at
            ORDER BY rev.effective_at DESC, rev.id DESC
            LIMIT 1
        ) r
        WHERE d.state = %(owed)s
          AND d.owed_at >= %(earliest)s
          AND r.enabled
          AND NOT EXISTS (
              SELECT 1 FROM {OutcomeClaim._meta.db_table} c
              WHERE c.detection_id = d.id
                AND c.deployment_id = %(deployment)s
                AND c.head = h.head
          )
        ORDER BY d.id, r.head
        ON CONFLICT (detection_id, deployment_id, head) DO NOTHING
    """
    with connection.cursor() as cursor:
        cursor.execute(
            sql,
            {
                "deployment": deployment.pk,
                "due": OutcomeClaim.State.DUE,
                "owed": Detection.State.OWED,
                "earliest": earliest,
            },
        )
        return cursor.rowcount


def lease_batch(
    deployment: ModelDeployment,
    worker_id: str,
    batch_size: int,
    lease_seconds: float,
    attempts: int,
    app_version: str,
) -> Leased:
    """Lease the next due claims of a deployment to a worker.

    Claims another worker holds under an unexpired lease are skipped, as
    are rows another worker is leasing at this moment. Lapsed claims come
    first, one detection at a time; otherwise up to ``batch_size``
    detections are leased with every due head each has. The lease is
    committed before this returns.

    Args:
        deployment: The deployment.
        worker_id: The worker taking the lease.
        batch_size: Most detections to lease.
        lease_seconds: How long the lease lasts.
        attempts: The attempt cap.
        app_version: The service release, for any failure recorded.

    Returns:
        The leased claims and how many lapsed claims became failures.
    """
    now = timezone.now()
    heads = max(1, len(deployment.declaration.get("heads", [])))
    with transaction.atomic():
        candidates = list(
            OutcomeClaim.objects.select_for_update(skip_locked=True)
            .filter(deployment=deployment, state=OutcomeClaim.State.DUE)
            .filter(Q(lease_expires_at__isnull=True) | Q(lease_expires_at__lte=now))
            .order_by(
                F("lease_expires_at").asc(nulls_last=True), "detection_id", "head"
            )[: batch_size * heads]
        )
        exhausted = [c for c in candidates if c.attempts >= attempts]
        for claim in exhausted:
            _fail(claim, _exhausted_reason(claim), app_version)
        live = [c for c in candidates if c.attempts < attempts]
        lapsed = [c for c in live if c.lease_expires_at is not None]
        if lapsed:
            chosen = [c for c in lapsed if c.detection_id == lapsed[0].detection_id]
        else:
            detections = list(dict.fromkeys(c.detection_id for c in live))
            kept = set(detections[:batch_size])
            chosen = [c for c in live if c.detection_id in kept]
        ids = [c.pk for c in chosen]
        if ids:
            OutcomeClaim.objects.filter(pk__in=ids).update(
                attempts=F("attempts") + 1,
                leased_by=worker_id,
                lease_expires_at=now + timedelta(seconds=lease_seconds),
                updated_at=now,
            )
    claims = list(
        OutcomeClaim.objects.select_related("detection__rubin_object", "revision")
        .filter(pk__in=ids)
        .order_by("detection_id", "head")
    )
    return Leased(claims=claims, failed=len(exhausted))


def _exhausted_reason(claim: OutcomeClaim) -> str:
    """Why a claim found with every attempt used is a failure.

    Args:
        claim: The claim.

    Returns:
        The reason.
    """
    if claim.leased_by:
        return (
            f"Lease held by {claim.leased_by} lapsed without an outcome on "
            f"attempt {claim.attempts} (the worker crashed, was killed, or hung)."
        )
    return f"Reached the attempt cap after {claim.attempts} attempts."


def _fail(claim: OutcomeClaim, reason: str, app_version: str) -> None:
    """Record a service-side failure for a claim and close it.

    Args:
        claim: The claim, locked by the caller.
        reason: Why it failed.
        app_version: The service release.
    """
    ProductFailure.objects.create(
        claim=claim, reason=reason, attempts=claim.attempts, app_version=app_version
    )
    _close(claim, OutcomeClaim.State.FAILURE)
    logger.warning(
        "Head %s of deployment %s gave up on diaSource %s after %d attempts: %s",
        claim.head,
        claim.deployment_id,
        claim.detection.dia_source_id,
        claim.attempts,
        reason,
    )


def _close(claim: OutcomeClaim, state: str) -> None:
    """Set a claim's state and clear its lease.

    Args:
        claim: The claim, locked by the caller.
        state: Its new state.
    """
    claim.state = state
    claim.leased_by = ""
    claim.lease_expires_at = None
    claim.save(update_fields=["state", "leased_by", "lease_expires_at", "updated_at"])


def settle(
    settlements: Sequence[Settlement], attempts: int, app_version: str
) -> Counter:
    """Store what each leased claim's attempt came to, and end its lease.

    A product or refusal is stored as returned, unknown codes included. A
    failed attempt with attempts left releases the claim for another try;
    one at the cap becomes a failure. A claim no longer leased exactly as
    it was when the attempt began is left untouched.

    Args:
        settlements: One per leased claim.
        attempts: The attempt cap.
        app_version: The service release.

    Returns:
        How many claims got products, refusals, or failures, were released
        to retry, or were lost to another worker.
    """
    counts: Counter = Counter()
    with transaction.atomic():
        locked = OutcomeClaim.objects.select_for_update().in_bulk(
            [s.claim.pk for s in settlements]
        )
        for settlement in settlements:
            leased = settlement.claim
            claim = locked[leased.pk]
            if (
                claim.state != OutcomeClaim.State.DUE
                or claim.leased_by != leased.leased_by
                or claim.lease_expires_at != leased.lease_expires_at
            ):
                logger.warning(
                    "Dropped the result for diaSource %s head %s: its lease "
                    "lapsed and the claim is now %s%s.",
                    leased.detection.dia_source_id,
                    leased.head,
                    claim.state,
                    f", leased by {claim.leased_by}" if claim.leased_by else "",
                )
                counts[LOST] += 1
                continue
            outcome = settlement.outcome
            if isinstance(outcome, Product):
                _store_product(claim, leased, outcome, settlement, app_version)
                counts[PRODUCT] += 1
            elif isinstance(outcome, Refusal):
                _store_refusal(claim, leased, outcome, app_version)
                counts[REFUSAL] += 1
            elif claim.attempts >= attempts:
                _fail(claim, settlement.error or "unknown error", app_version)
                counts[FAILURE] += 1
            else:
                _close(claim, OutcomeClaim.State.DUE)
                counts[RETRY] += 1
    return counts


def _store_product(
    claim: OutcomeClaim,
    leased: OutcomeClaim,
    product: Product,
    settlement: Settlement,
    app_version: str,
) -> None:
    """Store a product exactly as returned and close its claim.

    Args:
        claim: The locked claim.
        leased: The claim as leased, with its detection loaded.
        product: The package's product.
        settlement: The settlement carrying the packet sent.
        app_version: The service release.
    """
    data = product.to_dict()
    for caveat in product.caveats:
        if not caveat.is_known:
            logger.warning(
                "Stored caveat code %r, outside the contract vocabulary, from "
                "head %s for diaSource %s.",
                caveat.code,
                leased.head,
                leased.detection.dia_source_id,
            )
    StoredProduct.objects.create(
        claim=claim,
        output_kind=data["kind"],
        payload=data["payload"],
        caveats=data["caveats"],
        light_curve=settlement.light_curve or {},
        app_version=app_version,
    )
    _close(claim, OutcomeClaim.State.PRODUCT)


def _store_refusal(
    claim: OutcomeClaim, leased: OutcomeClaim, refusal: Refusal, app_version: str
) -> None:
    """Store a refusal with its code verbatim and close its claim.

    Args:
        claim: The locked claim.
        leased: The claim as leased, with its detection loaded.
        refusal: The package's refusal.
        app_version: The service release.
    """
    if not refusal.is_known:
        logger.warning(
            "Stored refusal code %r, outside the contract vocabulary, from "
            "head %s for diaSource %s.",
            refusal.code,
            leased.head,
            leased.detection.dia_source_id,
        )
    data = refusal.to_dict()
    HeadRefusal.objects.create(
        claim=claim,
        code=data["code"],
        message=data["message"],
        detail=data["detail"],
        app_version=app_version,
    )
    _close(claim, OutcomeClaim.State.REFUSAL)


def has_due(deployment: ModelDeployment) -> bool:
    """Whether any claim of the deployment is still due, leased or not.

    Args:
        deployment: The deployment.

    Returns:
        ``True`` while some claim awaits an outcome.
    """
    return OutcomeClaim.objects.filter(
        deployment=deployment, state=OutcomeClaim.State.DUE
    ).exists()
