"""Tests for the move of stored forecasts and refusals into outcome tables.

Before the model contract, each detection had at most one forecast or
refusal per checkpoint, stored whole. The contract stores one outcome per
detection, deployment, and head instead. These tests seed a database at the
last schema before that change, migrate forward, and check what the stored
history became.

They migrate the test database back and forth, so they need the compose
``db`` service; run them with ``run/seldonctl slim_dev test``.
"""

import json
from datetime import UTC, datetime, timedelta
from typing import Any

from django.apps.registry import Apps
from django.db import connection
from django.db.migrations.executor import MigrationExecutor
from django.test import TransactionTestCase

BEFORE = [("seldon", "0004_intake_skip_identifier")]
AFTER = [("seldon", "0006_copy_outcomes")]

CHECKPOINT = "seldon-2.0-roman-elasticc/epoch=1086-val_loss=1.18.ckpt"
NEXT_CHECKPOINT = "seldon-2.1-roman-elasticc/epoch=1200-val_loss=1.10.ckpt"
HEADS = {
    "flux_forecast": "flux_series",
    "classification": "class_probabilities",
    "basis_parameters": "named_values",
}

FIRST_STORED = datetime(2026, 11, 10, 3, 0, tzinfo=UTC)
LATER_STORED = datetime(2026, 11, 12, 3, 0, tzinfo=UTC)
REFUSED_AT = datetime(2026, 11, 11, 4, 0, tzinfo=UTC)
FIRST_RECEIVED = datetime(2026, 11, 9, 2, 0, tzinfo=UTC)

LIGHT_CURVE = {
    "times": [61000.5, 61001.5, 61003.0],
    "flux": [1234.5, -12.25, 0.3],
    "flux_err": [1.0, 2.5, 0.5],
    "detected": [True, True, True],
    "bands": ["g", "r", "g"],
}
GRID = {
    "eval_times": [61000.5, 61001.5, 61063.0],
    "eval_bands": ["u", "F062", "F444W"],
}
LATENT_MEAN = [0.123456789, -1.987654321]
LATENT_SIGMA = [0.555555555, 0.444444444]
OUTPUTS = {
    "flux": [1.0 / 7.0, 98765.4, -0.0001],
    "flux_err": [0.1, 0.2, 1e20],
    "zero_point_mag": 31.4,
    "parameters": {"t0": 61002.7, "amplitude": 3.141592653589793},
    "class_probabilities": {"SNIa": 0.7, "SNII": 0.3},
    "predicted_class": "SNIa",
    "latent_mean": LATENT_MEAN,
    "latent_sigma": LATENT_SIGMA,
}
FIRST_THRESHOLDS = {"fink": {"reliability_cut": 0.6, "sn_like_threshold": 0.5}}
LATER_THRESHOLDS = {"fink": {"reliability_cut": 0.9, "sn_like_threshold": 0.8}}
QUALIFYING_EVENT = {
    "dia_source_id": 170032915138363401,
    "broker": "fink",
    "reliability_score": 0.95,
    "sn_like_score": 0.85,
    "reliability_cut": 0.6,
    "sn_like_threshold": 0.5,
}


class CopyOutcomesTests(TransactionTestCase):
    """Old forecasts and refusals become per-head outcomes of retired
    pre-contract deployments, and owed detections gain their owed time and
    qualification snapshot."""

    def setUp(self) -> None:
        """Migrate back to the old schema, seed it, and migrate forward."""
        executor = MigrationExecutor(connection)
        executor.migrate(BEFORE)
        self._seed(executor.loader.project_state(BEFORE).apps)
        executor = MigrationExecutor(connection)
        executor.migrate(AFTER)
        self.apps = executor.loader.project_state(AFTER).apps

    def tearDown(self) -> None:
        """Leave the test database at the latest schema for other tests."""
        executor = MigrationExecutor(connection)
        executor.loader.build_graph()
        executor.migrate(executor.loader.graph.leaf_nodes())

    def _seed(self, apps: Apps) -> None:
        """Store forecasts and refusals as the old intake did.

        One detection was forecast under two checkpoints, the first time
        sparse and touching untrained bands; one was refused; one is owed
        with nothing stored yet; one is pending.

        Args:
            apps: The model registry at the old schema.
        """
        rubin_object = apps.get_model("seldon", "RubinObject").objects.create(
            dia_object_id=313953456790
        )
        Detection = apps.get_model("seldon", "Detection")
        Forecast = apps.get_model("seldon", "Forecast")
        Refusal = apps.get_model("seldon", "Refusal")
        Delivery = apps.get_model("seldon", "Delivery")

        def detection(source_id: int, state: str) -> Any:
            return Detection.objects.create(
                dia_source_id=source_id,
                rubin_object=rubin_object,
                state=state,
                observed_mjd=61003.0,
            )

        self.forecast_detection = detection(170032915138363401, "forecast")
        self.refused_detection = detection(170032915138363402, "refused")
        self.owed_detection = detection(170032915138363403, "owed")
        self.pending_detection = detection(170032915138363404, "pending")

        for broker, received_at in (
            ("lasair", FIRST_RECEIVED + timedelta(minutes=5)),
            ("fink", FIRST_RECEIVED),
        ):
            Delivery.objects.create(
                detection=self.owed_detection,
                broker=broker,
                role="qualifying",
                received_at=received_at,
            )

        def forecast(checkpoint: str, **regime: Any) -> Any:
            return Forecast.objects.create(
                detection=self.forecast_detection,
                checkpoint=checkpoint,
                light_curve=LIGHT_CURVE,
                zero_point=31.4,
                grid=GRID,
                outputs=OUTPUTS,
                eval_span_days=62.5,
                eval_span_in_time_scales=0.625,
                time_scale_days=100.0,
                library_version="1.2.0",
                provenance_zero_point_mag=27.5,
                torch_version="2.9.0+cpu",
                app_version="v0.0.0-test",
                qualifying_event=QUALIFYING_EVENT,
                **regime,
            )

        sparse = forecast(
            CHECKPOINT,
            in_training_regime=False,
            sparse=True,
            n_observations=3,
            min_points=5,
            untrained_bands=["F062", "u"],
            thresholds=FIRST_THRESHOLDS,
        )
        in_regime = forecast(
            NEXT_CHECKPOINT,
            in_training_regime=True,
            sparse=False,
            n_observations=12,
            min_points=5,
            untrained_bands=[],
            thresholds=LATER_THRESHOLDS,
        )
        Forecast.objects.filter(pk=sparse.pk).update(stored_at=FIRST_STORED)
        Forecast.objects.filter(pk=in_regime.pk).update(stored_at=LATER_STORED)

        refusal = Refusal.objects.create(
            detection=self.refused_detection,
            checkpoint=CHECKPOINT,
            code="unknown_band",
            message="Unknown bands: 'w'.",
            bands=["w"],
            app_version="v0.0.0-test",
        )
        Refusal.objects.filter(pk=refusal.pk).update(stored_at=REFUSED_AT)

    def _model(self, name: str) -> Any:
        """Return a model at the migrated schema.

        Args:
            name: The model's name.

        Returns:
            The historical model class.
        """
        return self.apps.get_model("seldon", name)

    def test_each_checkpoint_becomes_a_retired_pre_contract_deployment(
        self,
    ) -> None:
        """Two checkpoints make two retired deployments, each with the three
        heads enabled from the start of time."""
        deployments = self._model("ModelDeployment").objects.order_by("package_version")

        self.assertEqual(
            [d.package_version for d in deployments], [CHECKPOINT, NEXT_CHECKPOINT]
        )
        for deployment in deployments:
            self.assertEqual(deployment.package_name, "seldon-pre-contract")
            self.assertEqual(deployment.artifact_digest, "pre-contract")
            self.assertEqual(deployment.contract_version, "0.0")
            self.assertIsNotNone(deployment.retired_at)
            revisions = deployment.head_revisions.all()
            self.assertEqual(sorted(r.head for r in revisions), sorted(HEADS))
            for revision in revisions:
                self.assertTrue(revision.enabled)
                self.assertEqual(
                    revision.effective_at, datetime(1970, 1, 1, tzinfo=UTC)
                )

    def test_each_forecast_becomes_three_products(self) -> None:
        """Each old forecast gives one product per head, of that head's
        output kind, under its checkpoint's deployment and revision."""
        Product = self._model("Product")
        products = Product.objects.select_related(
            "claim__deployment", "claim__revision"
        )

        self.assertEqual(products.count(), 6)
        for checkpoint in (CHECKPOINT, NEXT_CHECKPOINT):
            stored = products.filter(claim__deployment__package_version=checkpoint)
            self.assertEqual(
                {p.claim.head: p.output_kind for p in stored},
                HEADS,
            )
            for product in stored:
                self.assertEqual(product.claim.state, "product")
                self.assertEqual(product.claim.attempts, 1)
                self.assertEqual(product.claim.detection_id, self.forecast_detection.pk)
                self.assertEqual(product.claim.revision.head, product.claim.head)
                self.assertEqual(
                    product.claim.revision.deployment_id, product.claim.deployment_id
                )
                self.assertEqual(product.app_version, "v0.0.0-test")

        sparse = {
            p.claim.head: p
            for p in products.filter(claim__deployment__package_version=CHECKPOINT)
        }
        self.assertEqual(sparse["flux_forecast"].stored_at, FIRST_STORED)
        self.assertEqual(
            sparse["flux_forecast"].payload,
            {
                "times": GRID["eval_times"],
                "bands": [["lsst", "u"], ["roman", "F062"], ["jwst", "F444W"]],
                "flux": OUTPUTS["flux"],
                "flux_err": OUTPUTS["flux_err"],
                "zero_point": 31.4,
                "eval_span_days": 62.5,
                "eval_span_in_time_scales": 0.625,
            },
        )
        self.assertEqual(
            sparse["classification"].payload,
            {"probabilities": OUTPUTS["class_probabilities"]},
        )
        self.assertEqual(
            sparse["basis_parameters"].payload, {"values": OUTPUTS["parameters"]}
        )

    def test_each_refusal_becomes_three_head_refusals(self) -> None:
        """An old refusal is refused for every head, keeping its code,
        message, and unresolved bands as survey-band pairs."""
        refusals = self._model("HeadRefusal").objects.select_related(
            "claim__deployment"
        )

        self.assertEqual(refusals.count(), 3)
        self.assertEqual(sorted(r.claim.head for r in refusals), sorted(HEADS))
        for refusal in refusals:
            self.assertEqual(refusal.claim.state, "refusal")
            self.assertEqual(refusal.claim.detection_id, self.refused_detection.pk)
            self.assertEqual(refusal.claim.deployment.package_version, CHECKPOINT)
            self.assertEqual(refusal.code, "unknown_band")
            self.assertEqual(refusal.message, "Unknown bands: 'w'.")
            self.assertEqual(refusal.detail, {"pairs": [["lsst", "w"]]})
            self.assertEqual(refusal.stored_at, REFUSED_AT)
        self.assertEqual(self._model("OutcomeClaim").objects.count(), 9)

    def test_sparse_out_of_regime_forecast_carries_its_caveats(self) -> None:
        """The sparse forecast touching untrained bands carries both caveats
        with its original counts and bands; the in-regime one carries none."""
        Product = self._model("Product")

        for product in Product.objects.filter(
            claim__deployment__package_version=CHECKPOINT
        ):
            self.assertEqual(
                product.caveats,
                [
                    {
                        "code": "sparse_light_curve",
                        "detail": {"n_observations": 3, "min_points": 5},
                    },
                    {
                        "code": "untrained_band",
                        "detail": {"bands": [["roman", "F062"], ["lsst", "u"]]},
                    },
                ],
            )
        for product in Product.objects.filter(
            claim__deployment__package_version=NEXT_CHECKPOINT
        ):
            self.assertEqual(product.caveats, [])

    def test_no_migrated_product_contains_latent_data(self) -> None:
        """Neither the latent fields nor their values reach any product."""
        for product in self._model("Product").objects.all():
            stored = json.dumps([product.payload, product.caveats, product.light_curve])
            self.assertNotIn("latent", stored)
            for value in LATENT_MEAN + LATENT_SIGMA:
                self.assertNotIn(repr(value), stored)

    def test_owed_detections_gain_owed_time_and_qualification(self) -> None:
        """A forecast or refused detection is owed from its earliest outcome,
        an owed one from its earliest delivery, a pending one not at all.
        The qualification snapshot comes from the earliest forecast."""
        Detection = self._model("Detection")

        forecast = Detection.objects.get(pk=self.forecast_detection.pk)
        refused = Detection.objects.get(pk=self.refused_detection.pk)
        owed = Detection.objects.get(pk=self.owed_detection.pk)
        pending = Detection.objects.get(pk=self.pending_detection.pk)

        self.assertEqual(forecast.owed_at, FIRST_STORED)
        self.assertEqual(refused.owed_at, REFUSED_AT)
        self.assertEqual(owed.owed_at, FIRST_RECEIVED)
        self.assertIsNone(pending.owed_at)
        self.assertEqual(
            forecast.qualification,
            {"thresholds": FIRST_THRESHOLDS, "qualifying_event": QUALIFYING_EVENT},
        )
        self.assertEqual(pending.qualification, {})
        self.assertEqual(
            [forecast.state, refused.state, owed.state],
            ["forecast", "refused", "owed"],
        )


OWED_STATES = [("seldon", "0007_detection_owed_states")]
LATE_STORED = datetime(2026, 11, 14, 5, 0, tzinfo=UTC)
LATE_REFUSED = datetime(2026, 11, 14, 6, 0, tzinfo=UTC)


def old_forecast(
    apps: Apps, detection: Any, checkpoint: str, stored_at: datetime
) -> Any:
    """Store a forecast as the old intake did, in range and at a given time.

    Args:
        apps: The model registry at a schema that still has forecasts.
        detection: The detection forecast.
        checkpoint: The checkpoint that forecast it.
        stored_at: When it was stored.

    Returns:
        The stored forecast.
    """
    Forecast = apps.get_model("seldon", "Forecast")
    forecast = Forecast.objects.create(
        detection=detection,
        checkpoint=checkpoint,
        light_curve=LIGHT_CURVE,
        zero_point=31.4,
        grid=GRID,
        outputs=OUTPUTS,
        in_training_regime=True,
        sparse=False,
        n_observations=12,
        min_points=5,
        untrained_bands=[],
        eval_span_days=62.5,
        eval_span_in_time_scales=0.625,
        time_scale_days=100.0,
        library_version="1.2.0",
        provenance_zero_point_mag=27.5,
        torch_version="2.9.0+cpu",
        app_version="v0.0.0-test",
        thresholds=FIRST_THRESHOLDS,
        qualifying_event=QUALIFYING_EVENT,
    )
    Forecast.objects.filter(pk=forecast.pk).update(stored_at=stored_at)
    return forecast


class OwedStatesTests(TransactionTestCase):
    """Forecast and refused detections become owed, after any outcome the old
    intake stored once the outcomes were first copied is copied too."""

    def setUp(self) -> None:
        """Copy one forecast, store more outcomes as the old intake would
        have, then migrate to the owed-only states."""
        executor = MigrationExecutor(connection)
        executor.migrate(BEFORE)
        apps = executor.loader.project_state(BEFORE).apps
        Detection = apps.get_model("seldon", "Detection")
        self.rubin_object_id = (
            apps.get_model("seldon", "RubinObject")
            .objects.create(dia_object_id=313953456790)
            .pk
        )
        self.copied = Detection.objects.create(
            dia_source_id=170032915138363401,
            rubin_object_id=self.rubin_object_id,
            state="forecast",
            observed_mjd=61003.0,
        ).pk
        old_forecast(
            apps, Detection.objects.get(pk=self.copied), CHECKPOINT, FIRST_STORED
        )

        executor = MigrationExecutor(connection)
        executor.migrate(AFTER)
        apps = executor.loader.project_state(AFTER).apps
        self._seed_after_copy(apps)

        executor = MigrationExecutor(connection)
        executor.migrate(OWED_STATES)
        self.apps = executor.loader.project_state(OWED_STATES).apps

    def tearDown(self) -> None:
        """Leave the test database at the latest schema for other tests."""
        executor = MigrationExecutor(connection)
        executor.loader.build_graph()
        executor.migrate(executor.loader.graph.leaf_nodes())

    def _seed_after_copy(self, apps: Apps) -> None:
        """Store what old intake, still running, stored after the copy.

        A detection forecast under the copied checkpoint, one refused under
        a checkpoint never seen before, and one pending.

        Args:
            apps: The model registry at the copied schema.
        """
        Detection = apps.get_model("seldon", "Detection")

        def detection(source_id: int, state: str) -> Any:
            return Detection.objects.create(
                dia_source_id=source_id,
                rubin_object_id=self.rubin_object_id,
                state=state,
                observed_mjd=61004.0,
            )

        late = detection(170032915138363405, "forecast")
        self.late_forecast = late.pk
        old_forecast(apps, late, CHECKPOINT, LATE_STORED)
        refused = detection(170032915138363406, "refused")
        self.late_refused = refused.pk
        Refusal = apps.get_model("seldon", "Refusal")
        refusal = Refusal.objects.create(
            detection=refused,
            checkpoint=NEXT_CHECKPOINT,
            code="no_usable_observations",
            message="No usable observations.",
            bands=[],
            app_version="v0.0.0-test",
        )
        Refusal.objects.filter(pk=refusal.pk).update(stored_at=LATE_REFUSED)
        self.pending = detection(170032915138363407, "pending").pk

    def _model(self, name: str) -> Any:
        """Return a model at the migrated schema.

        Args:
            name: The model's name.

        Returns:
            The historical model class.
        """
        return self.apps.get_model("seldon", name)

    def test_forecast_and_refused_detections_become_owed(self) -> None:
        """Every forecast or refused detection is owed, from its earliest
        outcome; the pending one is untouched."""
        Detection = self._model("Detection")
        states = {d.pk: (d.state, d.owed_at) for d in Detection.objects.all()}

        self.assertEqual(
            states,
            {
                self.copied: ("owed", FIRST_STORED),
                self.late_forecast: ("owed", LATE_STORED),
                self.late_refused: ("owed", LATE_REFUSED),
                self.pending: ("pending", None),
            },
        )
        self.assertEqual(
            Detection.objects.get(pk=self.late_forecast).qualification,
            {"thresholds": FIRST_THRESHOLDS, "qualifying_event": QUALIFYING_EVENT},
        )

    def test_outcomes_stored_after_the_first_copy_are_copied_once(self) -> None:
        """The late forecast gets its three products under the existing
        deployment, the late refusal three refusals under a new retired
        deployment, and the forecast copied before gets no second copy."""
        Product = self._model("Product")
        HeadRefusal = self._model("HeadRefusal")
        deployments = self._model("ModelDeployment").objects.order_by("package_version")

        self.assertEqual(
            [d.package_version for d in deployments], [CHECKPOINT, NEXT_CHECKPOINT]
        )
        for deployment in deployments:
            self.assertIsNotNone(deployment.retired_at)
            self.assertEqual(
                sorted(r.head for r in deployment.head_revisions.all()), sorted(HEADS)
            )
        for detection in (self.copied, self.late_forecast):
            products = Product.objects.filter(claim__detection_id=detection)
            self.assertEqual({p.claim.head: p.output_kind for p in products}, HEADS)
            self.assertEqual(products.count(), 3)
            self.assertEqual(
                {p.claim.deployment.package_version for p in products}, {CHECKPOINT}
            )
        refusals = HeadRefusal.objects.filter(claim__detection_id=self.late_refused)
        self.assertEqual(sorted(r.claim.head for r in refusals), sorted(HEADS))
        self.assertEqual(
            {r.claim.deployment.package_version for r in refusals}, {NEXT_CHECKPOINT}
        )
        self.assertEqual({r.stored_at for r in refusals}, {LATE_REFUSED})
        self.assertEqual({r.code for r in refusals}, {"no_usable_observations"})
        self.assertEqual([r.detail for r in refusals], [{}, {}, {}])
        self.assertEqual(self._model("OutcomeClaim").objects.count(), 9)
