"""Tests for the forecast worker: owed detections in, stored outcomes out.

Small stub packages stand in for a model, so each test says exactly what
the package returns or raises. One test runs the reference package end to
end and skips where it is not installed. These need the compose ``db``
service; run them with ``run/seldonctl slim_dev test``.
"""

import json
import tempfile
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

from django.conf import settings as django_settings
from django.core.management import CommandError, call_command
from django.test import SimpleTestCase, TestCase
from django.utils import timezone
from pydantic import ValidationError
from seldon_model_contract import (
    CONTRACT_VERSION,
    Caveat,
    Declaration,
    HeadDeclaration,
    HeadRequest,
    InputKind,
    ObjectResult,
    Outcome,
    OutputKind,
    Packet,
    Product,
    Refusal,
    TimeGrid,
    load_package,
    registered_names,
)

from seldon import deployments
from seldon.config import worker as worker_config
from seldon.config.worker import WorkerSettings
from seldon.forecasting import store
from seldon.forecasting.worker import ForecastWorker, start
from seldon.intake.engine import owed_packet, process_delivery
from seldon.models import (
    Detection,
    HeadRefusal,
    HeadRevision,
    ModelDeployment,
    OutcomeClaim,
    Product as StoredProduct,
    ProductFailure,
)
from seldon.tests.test_intake_engine import (
    ABOVE,
    OBJECT_ID,
    THRESHOLDS,
    delivery,
    source_id,
)

DIGEST = "sha256:" + "a" * 64
OTHER_DIGEST = "sha256:" + "b" * 64
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
FLUX = "flux_forecast"
CLASSES = "classification"
STUB_GRID = TimeGrid(0.0, 3.0, 1.0)
STUB_BANDS = (("lsst", "g"), ("lsst", "r"))


class StubPackage:
    """A model package whose answers each test chooses.

    By default it returns a flux series on the requested grid and fixed
    class probabilities. ``fail_on`` names objects whose presence makes the
    whole call raise; ``answer`` overrides an object's outcome for a head.

    Attributes:
        calls: The object ids of each ``predict`` call, in call order.
        requests: The head requests of each call.
    """

    def __init__(
        self,
        name: str = "stub-model",
        version: str = "1.0.0",
        fail_on: Sequence[str] = (),
        answer: Callable[[Packet, HeadRequest], Outcome | None] | None = None,
        on_predict: Callable[[Sequence[Packet]], None] | None = None,
    ) -> None:
        """Set up the stub.

        Args:
            name: Its package name.
            version: Its package version.
            fail_on: Object ids that make ``predict`` raise.
            answer: Returns an outcome to use instead of the default, or
                ``None`` to keep it.
            on_predict: Called with the packets at the start of each call.
        """
        self.name = name
        self.version = version
        self.fail_on = set(fail_on)
        self.answer = answer
        self.on_predict = on_predict
        self.calls: list[list[str]] = []
        self.requests: list[list[HeadRequest]] = []

    def describe(self) -> Declaration:
        """Declare a flux head and a classification head.

        Returns:
            The declaration.
        """
        return Declaration(
            package_name=self.name,
            package_version=self.version,
            contract_version=CONTRACT_VERSION,
            accepted=frozenset(STUB_BANDS),
            heads=(
                HeadDeclaration(
                    FLUX,
                    OutputKind.FLUX_SERIES,
                    {InputKind.TIMES: STUB_GRID, InputKind.BANDS: STUB_BANDS},
                ),
                HeadDeclaration(CLASSES, OutputKind.CLASS_PROBABILITIES),
            ),
        )

    def predict(
        self, packets: Sequence[Packet], requests: Sequence[HeadRequest]
    ) -> list[ObjectResult]:
        """Answer each packet, or raise when a ``fail_on`` object is present.

        Args:
            packets: The batch.
            requests: The heads to run.

        Returns:
            One result per packet.

        Raises:
            RuntimeError: When the batch holds a ``fail_on`` object.
        """
        ids = [packet.object_id for packet in packets]
        self.calls.append(ids)
        self.requests.append(list(requests))
        if self.on_predict is not None:
            self.on_predict(packets)
        bad = self.fail_on.intersection(ids)
        if bad:
            raise RuntimeError(f"cannot forecast {sorted(bad)}")
        return [
            ObjectResult(
                packet.object_id,
                {request.head: self.outcome(packet, request) for request in requests},
            )
            for packet in packets
        ]

    def outcome(self, packet: Packet, request: HeadRequest) -> Outcome:
        """One head's default outcome, unless ``answer`` overrides it.

        Args:
            packet: The object.
            request: The head request.

        Returns:
            The outcome.
        """
        if self.answer is not None:
            chosen = self.answer(packet, request)
            if chosen is not None:
                return chosen
        if request.head == CLASSES:
            return Product(
                OutputKind.CLASS_PROBABILITIES, {"probabilities": {"SNIa": 1.0}}
            )
        inputs = request.for_object(packet.object_id)
        grid = [
            (time, list(band))
            for time in inputs[InputKind.TIMES]
            for band in inputs[InputKind.BANDS]
        ]
        return Product(
            OutputKind.FLUX_SERIES,
            {
                "times": [time for time, _ in grid],
                "bands": [band for _, band in grid],
                "flux": [1.0] * len(grid),
                "flux_err": [0.1] * len(grid),
                "zero_point": 31.4,
            },
        )


def owe(count: int, first: int = 0) -> list[Detection]:
    """Make detections owed through intake, one object each.

    Args:
        count: How many.
        first: Offset of the first object from ``OBJECT_ID``.

    Returns:
        The owed detections, in object order.
    """
    owed = []
    for offset in range(first, first + count):
        object_id = OBJECT_ID + offset
        result = process_delivery(
            delivery(0, "fink", ABOVE, object_id=object_id), THRESHOLDS
        )
        assert result.acknowledge
        owed.append(Detection.objects.get(dia_source_id=source_id(0, object_id)))
    return owed


def enable(
    deployment: ModelDeployment,
    head: str,
    at: datetime = EPOCH,
    enabled: bool = True,
    inputs: dict | None = None,
) -> HeadRevision:
    """Write a head revision directly.

    Args:
        deployment: The deployment.
        head: The head's name.
        at: When it takes effect.
        enabled: Whether the head is enabled under it.
        inputs: Its inputs; the package defaults when omitted.

    Returns:
        The revision.
    """
    return HeadRevision.objects.create(
        deployment=deployment,
        head=head,
        enabled=enabled,
        inputs=inputs or {},
        effective_at=at,
    )


def worker_settings(**overrides: Any) -> WorkerSettings:
    """Worker settings for a test, with the stub's digest.

    Args:
        **overrides: Settings to change.

    Returns:
        The settings.
    """
    values: dict[str, Any] = {
        "package": "stub-model",
        "digest": DIGEST,
        "retry_seconds": 0,
    }
    values.update(overrides)
    return WorkerSettings(**values)


def started(package: Any, heads: Sequence[str] = (FLUX, CLASSES), **overrides: Any):
    """Register a package, enable heads from the epoch, and build its worker.

    Args:
        package: The package.
        heads: The heads to enable.
        **overrides: Worker settings to change.

    Returns:
        The worker, not yet run.
    """
    worker = start(package, worker_settings(**overrides))
    for head in heads:
        enable(worker.deployment, head)
    return worker


def claims_of(deployment: ModelDeployment) -> list[OutcomeClaim]:
    """Every claim of a deployment, by detection then head.

    Args:
        deployment: The deployment.

    Returns:
        The claims.
    """
    return list(
        OutcomeClaim.objects.filter(deployment=deployment).order_by(
            "detection__dia_source_id", "head"
        )
    )


class WorkerSettingsTests(SimpleTestCase):
    """Worker settings come from ``SELDON_WORKER_`` variables."""

    def test_defaults_and_prefix(self) -> None:
        env = {"SELDON_WORKER_PACKAGE": "seldon-reference"}
        with (
            mock.patch.dict("os.environ", env, clear=True),
            no_image_digest(),
        ):
            loaded = WorkerSettings()
        self.assertEqual(loaded.package, "seldon-reference")
        self.assertEqual(loaded.batch_size, 8)
        self.assertEqual(loaded.poll_seconds, 1.0)
        self.assertEqual(loaded.attempts, 3)
        self.assertEqual(loaded.lease_seconds, 300)
        self.assertIsNone(loaded.digest)

    def test_values_from_the_environment(self) -> None:
        env = {
            "SELDON_WORKER_PACKAGE": "seldon-reference",
            "SELDON_WORKER_BATCH_SIZE": "4",
            "SELDON_WORKER_DIGEST": DIGEST,
            "SELDON_WORKER_LEASE_SECONDS": "30",
        }
        with mock.patch.dict("os.environ", env, clear=True):
            loaded = WorkerSettings()
        self.assertEqual(
            (loaded.batch_size, loaded.digest, loaded.lease_seconds), (4, DIGEST, 30)
        )

    def test_digest_defaults_to_the_one_recorded_in_the_image(self) -> None:
        """A worker image records its wheel's digest in a file, so a worker
        started with no digest setting registers under it."""
        env = {"SELDON_WORKER_PACKAGE": "seldon-reference"}
        with (
            tempfile.TemporaryDirectory() as directory,
            mock.patch.dict("os.environ", env, clear=True),
        ):
            recorded = Path(directory) / "wheel.sha256"
            recorded.write_text(DIGEST + "\n")
            with mock.patch.object(worker_config, "IMAGE_DIGEST_FILE", recorded):
                self.assertEqual(WorkerSettings().digest, DIGEST)
                with mock.patch.dict(
                    "os.environ", {"SELDON_WORKER_DIGEST": OTHER_DIGEST}
                ):
                    self.assertEqual(WorkerSettings().digest, OTHER_DIGEST)

    def test_package_is_required(self) -> None:
        with mock.patch.dict("os.environ", {}, clear=True):
            with self.assertRaises(ValidationError):
                WorkerSettings()


class OutcomeTests(TestCase):
    """What the worker stores for owed detections."""

    def test_two_enabled_heads_store_one_product_each_under_their_revision(
        self,
    ) -> None:
        """Two owed detections and two enabled heads, the flux head with its
        own grid: four products, each naming its revision, holding the
        packet sent, and the flux one evaluated on the revision's grid."""
        package = StubPackage()
        worker = start(package, worker_settings())
        flux = enable(
            worker.deployment,
            FLUX,
            inputs={
                "times": {"start_days": 0.0, "days_past_last": 1.0, "step_days": 1.0},
                "bands": [["lsst", "r"]],
            },
        )
        classes = enable(worker.deployment, CLASSES)
        owed = owe(2)

        worker.run(until_idle=True)

        claims = claims_of(worker.deployment)
        self.assertEqual(
            [(c.detection_id, c.head, c.revision_id, c.state) for c in claims],
            [
                (d.pk, head, revision.pk, OutcomeClaim.State.PRODUCT)
                for d in owed
                for head, revision in ((CLASSES, classes), (FLUX, flux))
            ],
        )
        for claim in claims:
            product = claim.product
            self.assertEqual(
                product.light_curve, owed_packet(claim.detection).to_dict()
            )
            self.assertEqual(product.app_version, django_settings.APP_VERSION)
            self.assertEqual(product.caveats, [])
            self.assertEqual((claim.attempts, claim.leased_by), (1, ""))
            self.assertIsNone(claim.lease_expires_at)
        flux_product = claims[1].product
        self.assertEqual(flux_product.output_kind, "flux_series")
        self.assertEqual(flux_product.payload["times"], [61000.25, 61001.25])
        self.assertEqual(flux_product.payload["bands"], [["lsst", "r"]] * 2)
        self.assertEqual(claims[0].product.output_kind, "class_probabilities")
        self.assertEqual(claims[0].product.payload, {"probabilities": {"SNIa": 1.0}})
        self.assertEqual(len(package.calls), 1)

    def test_batches_carry_every_due_head_of_each_detection(self) -> None:
        """Five owed detections with a batch size of two: three calls of two,
        two and one objects, each asking for both heads."""
        package = StubPackage()
        worker = started(package, batch_size=2)
        owe(5)

        worker.run(until_idle=True)

        self.assertEqual([len(call) for call in package.calls], [2, 2, 1])
        for requests in package.requests:
            self.assertEqual(sorted(r.head for r in requests), [CLASSES, FLUX])
        self.assertEqual(StoredProduct.objects.count(), 10)

    def test_two_deployments_each_store_one_outcome_and_redelivery_adds_none(
        self,
    ) -> None:
        """Versions 1 and 2 of a package both run on two owed detections: each
        stores one product per detection and head. Lasair then redelivers
        one detection and both run again: nothing more is stored."""
        old = started(StubPackage(version="1.0.0"))
        new = started(StubPackage(version="2.0.0"), digest=OTHER_DIGEST)
        owed = owe(2)

        old.run(until_idle=True)
        new.run(until_idle=True)
        process_delivery(delivery(0, "lasair", ABOVE, object_id=OBJECT_ID), THRESHOLDS)
        old.run(until_idle=True)
        new.run(until_idle=True)

        for worker in (old, new):
            claims = claims_of(worker.deployment)
            self.assertEqual(
                [(c.detection_id, c.head) for c in claims],
                [(d.pk, h) for d in owed for h in (CLASSES, FLUX)],
            )
            self.assertTrue(all(c.state == "product" for c in claims))
        self.assertEqual(StoredProduct.objects.count(), 8)
        self.assertEqual(
            sorted(
                StoredProduct.objects.values_list(
                    "claim__deployment__package_version", flat=True
                ).distinct()
            ),
            ["1.0.0", "2.0.0"],
        )

    def test_failing_object_is_isolated_and_fails_after_three_attempts(self) -> None:
        """One of four objects makes ``predict`` raise. The other three get
        products from the first lease; the bad one is tried alone until its
        third attempt, then gets a failure naming the error, and no
        product."""
        owed = owe(4)
        bad = str(owed[2].dia_source_id)
        package = StubPackage(fail_on=[bad])
        worker = started(package, heads=(CLASSES,))

        with self.assertLogs("seldon.forecasting", level="WARNING"):
            worker.run(until_idle=True)

        claims = {c.detection.dia_source_id: c for c in claims_of(worker.deployment)}
        failed = claims.pop(int(bad))
        self.assertEqual((failed.state, failed.attempts), ("failure", 3))
        self.assertEqual(failed.failure.attempts, 3)
        self.assertEqual(
            failed.failure.reason, f"RuntimeError: cannot forecast ['{bad}']"
        )
        self.assertFalse(StoredProduct.objects.filter(claim=failed).exists())
        self.assertEqual(
            [(c.state, c.attempts) for c in claims.values()], [("product", 1)] * 3
        )
        self.assertEqual(package.calls[0], [str(d.dia_source_id) for d in owed])
        self.assertEqual(sum(call == [bad] for call in package.calls), 3)

    def test_caveats_are_stored_unchanged_and_unknown_codes_kept_and_logged(
        self,
    ) -> None:
        """The flux head returns a known caveat and one outside the
        vocabulary; the classification head refuses with an unknown code.
        Both caveats are stored as returned, the refusal keeps its code,
        and a warning names each unknown code."""
        caveats = (
            Caveat("sparse_light_curve", {"n_observations": 1, "min_points": 3}),
            Caveat("cosmic_weather", {"severity": "high"}),
        )

        def answer(packet: Packet, request: HeadRequest) -> Outcome | None:
            if request.head == CLASSES:
                return Refusal("solar_conjunction", "too close", {"deg": 5})
            payload = {
                "times": [61000.0],
                "bands": [["lsst", "r"]],
                "flux": [1.0],
                "flux_err": [0.1],
                "zero_point": 31.4,
            }
            return Product(OutputKind.FLUX_SERIES, payload, caveats)

        worker = started(StubPackage(answer=answer))
        owe(1)

        with self.assertLogs("seldon.forecasting", level="WARNING") as logs:
            worker.run(until_idle=True)

        product = StoredProduct.objects.get()
        self.assertEqual(
            product.caveats,
            [
                {
                    "code": "sparse_light_curve",
                    "detail": {"n_observations": 1, "min_points": 3},
                },
                {"code": "cosmic_weather", "detail": {"severity": "high"}},
            ],
        )
        refusal = HeadRefusal.objects.get()
        self.assertEqual(
            (refusal.code, refusal.message, refusal.detail, refusal.claim.state),
            ("solar_conjunction", "too close", {"deg": 5}, "refusal"),
        )
        self.assertEqual(refusal.app_version, django_settings.APP_VERSION)
        text = "\n".join(logs.output)
        self.assertIn("cosmic_weather", text)
        self.assertIn("solar_conjunction", text)
        self.assertNotIn("sparse_light_curve", text)

    def test_head_disabled_after_enablement_keeps_its_enabled_window(self) -> None:
        """The flux head is enabled at T0 and disabled at T2. A detection owed
        at T1 is still served; one owed at T3 is not claimed for it, though
        it is for classification."""
        t0 = datetime(2026, 11, 10, tzinfo=UTC)
        package = StubPackage()
        worker = start(package, worker_settings())
        flux = enable(worker.deployment, FLUX, at=t0)
        enable(worker.deployment, FLUX, at=t0 + timedelta(days=2), enabled=False)
        enable(worker.deployment, CLASSES, at=t0)
        during, after = owe(2)
        Detection.objects.filter(pk=during.pk).update(owed_at=t0 + timedelta(days=1))
        Detection.objects.filter(pk=after.pk).update(owed_at=t0 + timedelta(days=3))

        worker.run(until_idle=True)

        self.assertEqual(
            [(c.detection_id, c.head) for c in claims_of(worker.deployment)],
            [(during.pk, CLASSES), (during.pk, FLUX), (after.pk, CLASSES)],
        )
        self.assertEqual(
            OutcomeClaim.objects.get(detection=during, head=FLUX).revision, flux
        )

    def test_detection_owed_before_a_head_was_enabled_is_not_claimed(self) -> None:
        """A head enabled after a detection became owed does not reach back."""
        package = StubPackage()
        worker = start(package, worker_settings())
        (owed,) = owe(1)
        enable(worker.deployment, CLASSES, at=owed.owed_at + timedelta(seconds=1))

        worker.run(until_idle=True)

        self.assertEqual(claims_of(worker.deployment), [])
        self.assertEqual(package.calls, [])

    def test_retired_deployment_claims_nothing(self) -> None:
        """A deployment retired after its worker started claims and stores
        nothing, and its worker still reports in."""
        package = StubPackage()
        worker = started(package)
        owe(2)
        ModelDeployment.objects.filter(pk=worker.deployment.pk).update(
            retired_at=timezone.now()
        )

        worker.run(until_idle=True)

        self.assertFalse(OutcomeClaim.objects.exists())
        self.assertEqual(package.calls, [])
        worker.deployment.refresh_from_db()
        self.assertIsNotNone(worker.deployment.last_heartbeat_at)

    def test_heartbeat_is_recorded(self) -> None:
        worker = started(StubPackage())
        before = timezone.now()

        worker.run(until_idle=True)

        worker.deployment.refresh_from_db()
        self.assertGreaterEqual(worker.deployment.last_heartbeat_at, before)


class RetryDelayTests(TestCase):
    """A failed attempt waits before its retry."""

    def test_failed_attempt_waits_and_each_retry_waits_twice_as_long(self) -> None:
        """With a 60 s retry delay, a claim whose attempt failed is released
        with no holder and is not leased again for 60 s; its next failure
        waits 120 s."""
        owed = owe(1)
        package = StubPackage(fail_on=[str(owed[0].dia_source_id)])
        worker = started(package, heads=(CLASSES,), retry_seconds=60)

        before = timezone.now()
        self.assertIsNotNone(worker.step())
        claim = claims_of(worker.deployment)[0]
        self.assertEqual((claim.state, claim.attempts, claim.leased_by), ("due", 1, ""))
        self.assertGreaterEqual(claim.lease_expires_at, before + timedelta(seconds=60))
        self.assertIsNone(worker.step())
        self.assertEqual(len(package.calls), 1)

        OutcomeClaim.objects.filter(pk=claim.pk).update(
            lease_expires_at=timezone.now() - timedelta(seconds=1)
        )
        before = timezone.now()
        self.assertIsNotNone(worker.step())
        claim.refresh_from_db()
        self.assertEqual((claim.state, claim.attempts), ("due", 2))
        self.assertGreaterEqual(claim.lease_expires_at, before + timedelta(seconds=120))
        self.assertEqual(len(package.calls), 2)


class ClaimScanTests(TestCase):
    """Workers read only recently owed detections between full scans."""

    def test_scan_reads_recent_detections_until_a_revision_is_written(self) -> None:
        """After its first scan, a worker claims only detections owed since
        shortly before the previous scan; a new head revision makes the next
        scan read every owed detection again."""
        worker = started(StubPackage(), heads=(CLASSES,))
        scan = store.ClaimScan()
        day_ago = timezone.now() - timedelta(days=1)
        first = owe(1)
        self.assertEqual(store.ensure_claims(worker.deployment, scan), 1)

        late = owe(1, first=1)
        Detection.objects.filter(pk=late[0].pk).update(owed_at=day_ago)
        owe(1, first=2)
        self.assertEqual(store.ensure_claims(worker.deployment, scan), 1)
        self.assertFalse(OutcomeClaim.objects.filter(detection=late[0]).exists())

        enable(worker.deployment, FLUX)
        self.assertEqual(store.ensure_claims(worker.deployment, scan), 1 + 3)
        self.assertEqual(OutcomeClaim.objects.filter(detection=first[0]).count(), 2)


class LeaseTests(TestCase):
    """Leases that lapse, and outcomes for leases another worker holds."""

    def lapse(self, worker: ForecastWorker, attempts: int) -> None:
        """Make every claim look leased by a worker that died.

        Args:
            worker: The worker whose deployment's claims to change.
            attempts: The attempts they have used.
        """
        store.ensure_claims(worker.deployment)
        OutcomeClaim.objects.filter(deployment=worker.deployment).update(
            leased_by="gone-host:1",
            lease_expires_at=timezone.now() - timedelta(seconds=1),
            attempts=attempts,
        )

    def test_lapsed_claims_are_retried_one_detection_at_a_time(self) -> None:
        """Three detections whose lease lapsed after one attempt are each
        tried alone, with every due head, and stored on attempt two."""
        package = StubPackage()
        worker = started(package)
        owe(3)
        self.lapse(worker, attempts=1)

        worker.run(until_idle=True)

        self.assertEqual([len(call) for call in package.calls], [1, 1, 1])
        self.assertTrue(
            all(
                (c.state, c.attempts) == ("product", 2)
                for c in claims_of(worker.deployment)
            )
        )

    def test_lapsed_claim_at_the_cap_fails_without_another_attempt(self) -> None:
        """A claim whose lease lapsed on its third attempt gets a failure
        saying so, and the package is not called."""
        package = StubPackage()
        worker = started(package, heads=(CLASSES,))
        owe(1)
        self.lapse(worker, attempts=3)

        with self.assertLogs("seldon.forecasting", level="WARNING"):
            worker.run(until_idle=True)

        claim = OutcomeClaim.objects.get()
        self.assertEqual((claim.state, claim.attempts), ("failure", 3))
        self.assertIn("gone-host:1", claim.failure.reason)
        self.assertIn("lapsed", claim.failure.reason)
        self.assertEqual(package.calls, [])

    def test_unexpired_lease_of_another_worker_is_left_alone(self) -> None:
        """While another worker's lease runs, nothing is claimed; with
        ``--until-idle`` the worker waits rather than exits, and takes the
        claim once the lease lapses."""
        package = StubPackage()
        worker = started(package, heads=(CLASSES,), poll_seconds=0.0)
        owe(1)
        store.ensure_claims(worker.deployment)
        OutcomeClaim.objects.update(
            leased_by="busy-host:2",
            lease_expires_at=timezone.now() + timedelta(hours=1),
            attempts=1,
        )
        sleeps: list[float] = []

        def sleep(seconds: float) -> None:
            sleeps.append(seconds)
            OutcomeClaim.objects.update(
                lease_expires_at=timezone.now() - timedelta(seconds=1)
            )

        worker.sleep = sleep
        worker.run(until_idle=True)

        self.assertEqual(len(sleeps), 1)
        claim = OutcomeClaim.objects.get()
        self.assertEqual((claim.state, claim.attempts), ("product", 2))

    def test_outcome_is_not_stored_for_a_lease_another_worker_took(self) -> None:
        """While the package runs, the lease lapses and another worker takes
        it. The late result is dropped and the claim is left as the other
        worker holds it."""
        expires = timezone.now() + timedelta(hours=1)

        def taken_over(packets: Sequence[Packet]) -> None:
            OutcomeClaim.objects.update(
                leased_by="other-host:3", lease_expires_at=expires, attempts=2
            )

        package = StubPackage(on_predict=taken_over)
        worker = started(package, heads=(CLASSES,))
        owe(1)

        with self.assertLogs("seldon.forecasting", level="WARNING") as logs:
            summary = worker.step()

        self.assertEqual(summary.lost, 1)
        claim = OutcomeClaim.objects.get()
        self.assertEqual(
            (claim.state, claim.leased_by, claim.lease_expires_at, claim.attempts),
            ("due", "other-host:3", expires, 2),
        )
        self.assertFalse(StoredProduct.objects.exists())
        self.assertIn("other-host:3", "\n".join(logs.output))


def no_image_digest() -> mock._patch:
    """Hide the digest a worker image records, as in an image without one.

    Returns:
        A patch pointing the recorded-digest file at a path that does not
        exist.
    """
    return mock.patch.object(
        worker_config, "IMAGE_DIGEST_FILE", Path("/nonexistent/wheel.sha256")
    )


def fake_entry_points(package: Any, name: str = "stub-model") -> mock._patch:
    """Patch package discovery so ``name`` loads ``package``.

    Args:
        package: The package to load.
        name: Its entry-point name.

    Returns:
        The patcher.
    """
    entry = SimpleNamespace(name=name, load=lambda: package)
    return mock.patch(
        "seldon_model_contract.discovery.entry_points", return_value=[entry]
    )


class CommandTests(TestCase):
    """The ``forecast_worker`` management command."""

    def test_runs_until_idle_and_logs_each_batch(self) -> None:
        package = StubPackage()
        deployment = deployments.register(package.describe(), DIGEST)
        enable(deployment, CLASSES)
        owe(2)
        env = {"SELDON_WORKER_PACKAGE": "stub-model", "SELDON_WORKER_DIGEST": DIGEST}

        with (
            mock.patch.dict("os.environ", env),
            fake_entry_points(package),
            self.assertLogs("seldon.forecasting", level="INFO") as logs,
        ):
            call_command("forecast_worker", "--until-idle")

        self.assertEqual(StoredProduct.objects.count(), 2)
        batch_lines = [line for line in logs.output if "batch of 2 detections" in line]
        self.assertEqual(len(batch_lines), 1)

    def test_mismatched_digest_stops_the_worker(self) -> None:
        """The release is registered with one digest; a worker built with
        another refuses to start and claims nothing."""
        package = StubPackage()
        deployment = deployments.register(package.describe(), DIGEST)
        enable(deployment, CLASSES)
        owe(1)
        env = {"SELDON_WORKER_PACKAGE": "stub-model"}

        with (
            mock.patch.dict("os.environ", env),
            fake_entry_points(package),
            self.assertRaises(CommandError) as caught,
        ):
            call_command("forecast_worker", "--until-idle", "--digest", OTHER_DIGEST)

        self.assertIn(OTHER_DIGEST, str(caught.exception))
        self.assertFalse(OutcomeClaim.objects.exists())
        self.assertEqual(package.calls, [])

    def test_missing_digest_stops_the_worker(self) -> None:
        env = {"SELDON_WORKER_PACKAGE": "stub-model"}
        with (
            mock.patch.dict("os.environ", env),
            mock.patch.dict("os.environ", {"SELDON_WORKER_DIGEST": ""}),
            no_image_digest(),
            fake_entry_points(StubPackage()),
            self.assertRaises(CommandError) as caught,
        ):
            call_command("forecast_worker", "--until-idle")
        self.assertIn("digest", str(caught.exception))
        self.assertFalse(ModelDeployment.objects.exists())

    def test_unknown_package_stops_the_worker(self) -> None:
        env = {"SELDON_WORKER_PACKAGE": "no-such-model"}
        with (
            mock.patch.dict("os.environ", env),
            fake_entry_points(StubPackage()),
            self.assertRaises(CommandError) as caught,
        ):
            call_command("forecast_worker", "--until-idle", "--digest", DIGEST)
        self.assertIn("no-such-model", str(caught.exception))


REFERENCE = "seldon-reference"


class ReferencePackageTests(TestCase):
    """The reference package, run by the worker on an owed detection."""

    def setUp(self) -> None:
        if REFERENCE not in registered_names():
            self.skipTest(f"{REFERENCE} is not installed in this image")

    def test_reference_package_stores_all_three_products(self) -> None:
        """Night 0 of the intake test object is owed. With all three heads
        enabled, the worker stores a flux series on the default grid (six
        LSST bands, daily, to 60 days past the last point), class
        probabilities, and basis parameters, and no payload carries the
        latent."""
        package = load_package(REFERENCE)
        worker = start(package, worker_settings(package=REFERENCE))
        for head in (FLUX, CLASSES, "basis_parameters"):
            enable(worker.deployment, head)
        process_delivery(delivery(2, "fink", ABOVE), THRESHOLDS)

        worker.run(until_idle=True)

        products = {
            p.claim.head: p
            for p in StoredProduct.objects.select_related("claim").filter(
                claim__deployment=worker.deployment
            )
        }
        self.assertEqual(
            {head: p.output_kind for head, p in products.items()},
            {
                FLUX: "flux_series",
                CLASSES: "class_probabilities",
                "basis_parameters": "named_values",
            },
        )
        flux = products[FLUX].payload
        self.assertEqual(len(flux["times"]), 63 * 6)
        self.assertAlmostEqual(
            sum(products[CLASSES].payload["probabilities"].values()), 1.0, places=4
        )
        self.assertTrue(products["basis_parameters"].payload["values"])
        for product in products.values():
            self.assertNotIn("latent", json.dumps(product.payload))
            self.assertEqual(len(product.light_curve["points"]), 3)
        self.assertFalse(ProductFailure.objects.exists())
