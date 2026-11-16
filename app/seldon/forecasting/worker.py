"""The forecast worker loop for one model deployment.

A worker registers the release it serves, then repeats: report in, create
claims for newly owed work, lease a batch, build each detection's packet and
each head's request, call the package, and store the outcomes. The package
runs with no lock or transaction held (see ``seldon.forecasting.store``), so
a slow or hung package delays only its own batch.

When ``predict`` raises for a batch, the batch is split in half and each
half retried, down to single detections, so one bad object costs the
others nothing. The detection that still fails uses up one attempt per
lease; at the cap it is recorded as a failure, kept apart from the
package's own refusals.

A batch can mix detections whose heads ran under different revisions, and
detections that need only some of the batch's heads. A head request carries
concrete inputs per object, so each object's inputs come from its own
claim's revision, and an object with no claim for a head gets the head's
declared defaults; whatever the package returns for it is not stored.
"""

import json
import logging
import os
import socket
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, fields
from typing import Any

from django.conf import settings as django_settings
from seldon_model_contract import (
    Declaration,
    HeadRequest,
    ModelPackage,
    ObjectResult,
    Packet,
)

from seldon import deployments
from seldon.config.worker import WorkerSettings
from seldon.domain.contract.inputs import resolve_inputs
from seldon.forecasting import store
from seldon.forecasting.store import Settlement
from seldon.intake.engine import owed_packet
from seldon.models import ModelDeployment, OutcomeClaim

logger = logging.getLogger("seldon.forecasting")


@dataclass
class BatchSummary:
    """What one batch, or a whole run, came to.

    Attributes:
        detections: Detections leased.
        products: Claims that got a product.
        refusals: Claims that got a refusal.
        failures: Claims recorded as failures.
        retries: Claims released to be tried again.
        lost: Results dropped because another worker took the lease.
        seconds: Wall time spent.
    """

    detections: int = 0
    products: int = 0
    refusals: int = 0
    failures: int = 0
    retries: int = 0
    lost: int = 0
    seconds: float = 0.0

    def add(self, other: "BatchSummary") -> None:
        """Add another summary's counts to this one.

        Args:
            other: The summary to add.
        """
        for item in fields(self):
            setattr(
                self, item.name, getattr(self, item.name) + getattr(other, item.name)
            )


def worker_id() -> str:
    """Name this process as a lease holder.

    Returns:
        ``host:pid``.
    """
    return f"{socket.gethostname()}:{os.getpid()}"


def start(
    package: ModelPackage, settings: WorkerSettings, digest: str | None = None
) -> "ForecastWorker":
    """Register the package's release and build a worker for it.

    Args:
        package: The loaded model package.
        settings: The worker settings.
        digest: The wheel's sha256; ``settings.digest`` when omitted.

    Returns:
        The worker, not yet run.

    Raises:
        ValueError: When no digest is given either way.
        seldon.deployments.DeploymentRefused: When the release is refused.
    """
    digest = digest or settings.digest
    if not digest:
        raise ValueError(
            "No artifact digest: set SELDON_WORKER_DIGEST or pass --digest."
        )
    declaration = package.describe()
    deployment = deployments.register(declaration, digest)
    logger.info("Serving %s (deployment %s)", deployment, deployment.pk)
    return ForecastWorker(package, declaration, deployment, settings)


class ForecastWorker:
    """Claims, runs, and stores owed work for one deployment.

    Attributes:
        package: The model package.
        declaration: What the package declared at start.
        deployment: Its registered deployment.
        settings: The worker settings.
        worker_id: This worker's name as a lease holder.
        sleep: Waits between empty polls; replaceable in tests.
    """

    def __init__(
        self,
        package: ModelPackage,
        declaration: Declaration,
        deployment: ModelDeployment,
        settings: WorkerSettings,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        """Set up a worker for a registered deployment.

        Args:
            package: The model package.
            declaration: What the package declared.
            deployment: Its registered deployment.
            settings: The worker settings.
            sleep: Waits between empty polls.
        """
        self.package = package
        self.declaration = declaration
        self.deployment = deployment
        self.settings = settings
        self.worker_id = worker_id()
        self.sleep = sleep
        self._retired = False

    def run(self, until_idle: bool = False) -> BatchSummary:
        """Work batches until stopped, or until idle.

        Args:
            until_idle: Return once nothing is due or leased for the
                deployment (or it is retired), instead of polling on.

        Returns:
            The totals over every batch run.
        """
        totals = BatchSummary()
        while True:
            summary = self.step()
            if summary is not None:
                totals.add(summary)
                continue
            if until_idle and (self._retired or not store.has_due(self.deployment)):
                return totals
            self.sleep(self.settings.poll_seconds)

    def step(self) -> BatchSummary | None:
        """Report in, then lease, run, and store one batch.

        Returns:
            The batch's summary, or ``None`` when there was nothing to lease
            (or the deployment is retired).
        """
        retired_at = store.heartbeat(self.deployment)
        if retired_at is not None:
            if not self._retired:
                logger.warning(
                    "%s was retired at %s; claiming nothing.",
                    self.deployment,
                    retired_at.isoformat(),
                )
            self._retired = True
            return None
        started = time.monotonic()
        store.ensure_claims(self.deployment)
        leased = store.lease_batch(
            self.deployment,
            self.worker_id,
            self.settings.batch_size,
            self.settings.lease_seconds,
            self.settings.attempts,
            django_settings.APP_VERSION,
        )
        if not leased.claims and not leased.failed:
            return None
        summary = BatchSummary(failures=leased.failed)
        if leased.claims:
            summary.add(self._run_batch(leased.claims))
        summary.seconds = time.monotonic() - started
        logger.info(
            "%s: batch of %d detections: %d products, %d refusals, %d failures, "
            "%d to retry, %d lost to another worker, in %.2fs",
            self.deployment,
            summary.detections,
            summary.products,
            summary.refusals,
            summary.failures,
            summary.retries,
            summary.lost,
            summary.seconds,
        )
        return summary

    def _run_batch(self, claims: Sequence[OutcomeClaim]) -> BatchSummary:
        """Build packets, call the package, and store the outcomes.

        Args:
            claims: The leased claims, grouped by detection.

        Returns:
            The batch's counts.
        """
        groups: dict[int, list[OutcomeClaim]] = {}
        for claim in claims:
            groups.setdefault(claim.detection_id, []).append(claim)
        settlements: list[Settlement] = []
        items: list[tuple[Packet, list[OutcomeClaim]]] = []
        for group in groups.values():
            try:
                items.append((owed_packet(group[0].detection), group))
            except Exception as error:  # an object-specific build failure
                settlements += [Settlement(c, error=describe(error)) for c in group]
        results = self._attempt(items) if items else {}
        for packet, group in items:
            result = results[packet.object_id]
            for claim in group:
                if isinstance(result, Exception):
                    settlements.append(Settlement(claim, error=describe(result)))
                    continue
                outcome = result.outcomes.get(claim.head)
                if outcome is None:
                    settlements.append(
                        Settlement(
                            claim,
                            error=f"The package returned no outcome for head "
                            f"{claim.head}.",
                        )
                    )
                    continue
                settlements.append(
                    Settlement(claim, outcome=outcome, light_curve=packet.to_dict())
                )
        counts = store.settle(
            settlements, self.settings.attempts, django_settings.APP_VERSION
        )
        return BatchSummary(
            detections=len(groups),
            products=counts[store.PRODUCT],
            refusals=counts[store.REFUSAL],
            failures=counts[store.FAILURE],
            retries=counts[store.RETRY],
            lost=counts[store.LOST],
        )

    def _attempt(
        self, items: Sequence[tuple[Packet, list[OutcomeClaim]]]
    ) -> dict[str, ObjectResult | Exception]:
        """Call the package on a batch, splitting it in half when it raises.

        Args:
            items: Each detection's packet and leased claims.

        Returns:
            Each packet's result, or the error that a call holding only
            that packet raised, by object id.
        """
        packets = [packet for packet, _ in items]
        try:
            results = self.package.predict(packets, self._requests(items))
            ids = [result.object_id for result in results]
            expected = [packet.object_id for packet in packets]
            if ids != expected:
                raise ValueError(
                    f"predict returned results for {ids}, not for {expected}."
                )
            return dict(zip(expected, results, strict=True))
        except Exception as error:
            if len(items) == 1:
                logger.warning(
                    "predict failed for diaSource %s: %s",
                    packets[0].object_id,
                    describe(error),
                )
                return {packets[0].object_id: error}
            logger.warning(
                "predict failed for a batch of %d (%s); retrying each half.",
                len(items),
                describe(error),
            )
            half = len(items) // 2
            return {**self._attempt(items[:half]), **self._attempt(items[half:])}

    def _requests(
        self, items: Sequence[tuple[Packet, list[OutcomeClaim]]]
    ) -> list[HeadRequest]:
        """Build one request per head in the batch, with per-object inputs.

        Args:
            items: Each detection's packet and leased claims.

        Returns:
            The head requests, by head name.

        Raises:
            KeyError: When a claimed head is not in the declaration.
            ValueError: When a revision's inputs do not fit its head.
        """
        heads = sorted({claim.head for _, group in items for claim in group})
        requests = []
        for head in heads:
            declared = self.declaration.head(head)
            by_inputs: dict[str, tuple[dict, list[Packet]]] = {}
            for packet, group in items:
                revision_inputs: dict[str, Any] = next(
                    (c.revision.inputs for c in group if c.head == head), {}
                )
                key = json.dumps(revision_inputs, sort_keys=True)
                by_inputs.setdefault(key, (revision_inputs, []))[1].append(packet)
            inputs: dict[str, Any] = {}
            for revision_inputs, packets in by_inputs.values():
                inputs.update(resolve_inputs(declared, revision_inputs, packets).inputs)
            requests.append(HeadRequest(head=head, inputs=inputs))
        return requests


def describe(error: BaseException) -> str:
    """Describe an error as its type and message.

    Args:
        error: The error.

    Returns:
        ``Type: message``.
    """
    return f"{type(error).__name__}: {error}"
