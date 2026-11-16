"""Tests for the ``compare_replays`` command.

The command compares two whole databases, so these tests need two. The left
one is the test database itself. The right one is a second database created
from it as a template when the class starts and dropped when it ends. Both
are written through ordinary committed transactions, because the command
reads them over connections of its own. These need the compose ``db``
service; run them with ``run/seldonctl slim_dev test``.

Django's test guard refuses connections to any registered alias a test case
does not list in ``databases``, and that list is checked against settings
before any test runs. So the class adds the right-hand alias and the
command's two runtime aliases to its list once it has started.
"""

import copy
import hashlib
import json
import os
import tempfile
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Any
from unittest import mock

from django.apps import apps
from django.core.management import CommandError, call_command
from django.db import InternalError, connection, connections
from django.test import TransactionTestCase

from seldon import deployments
from seldon.forecasting.worker import start
from seldon.management.commands.compare_replays import (
    COMPARED_ALIASES,
    LEFT_ALIAS,
    read_only_databases,
)
from seldon.management.commands.replay_alerts import (
    live_releases,
    point_at,
    run_replay,
    seed_heads,
)
from seldon.models import (
    Delivery,
    Detection,
    HeadRefusal,
    HeadRevision,
    ModelDeployment,
    OutcomeClaim,
    Product,
    ProductFailure,
    RubinObject,
)
from seldon.replay.source import ReplayMode
from seldon.tests.test_forecast_worker import DIGEST, StubPackage, worker_settings
from seldon.tests.test_replay import (
    REPLAY_ENV,
    FakeWall,
    replay_of,
    write_fixture_recording,
)

RIGHT_ALIAS = "compare_replays_test_right"
LEFT = "default"
REFERENCE = "seldon-reference"
FLUX = "flux_forecast"
CLASSES = "classification"
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def light_curve(points: int = 3) -> dict[str, Any]:
    """Build a small packet, as stored with a product.

    Args:
        points: How many observations it holds.

    Returns:
        The packet in its JSON form.
    """
    return {
        "object_id": "11",
        "points": [
            {
                "survey": "lsst",
                "band": "r",
                "time": 60990.1 + day,
                "flux": 10.0 + day,
                "flux_err": 1.0,
                "zero_point": 31.4,
                "detected": True,
            }
            for day in range(points)
        ],
        "metadata": {},
        "extensions": {},
    }


def flux_payload(flux: float = 12.5) -> dict[str, Any]:
    """Build a flux-series payload.

    Args:
        flux: The first forecast flux value.

    Returns:
        The payload as stored with a product.
    """
    return {
        "times": [61000.0, 61010.0],
        "bands": [["lsst", "r"], ["lsst", "r"]],
        "flux": [flux, 13.25],
        "flux_err": [0.5, 0.75],
        "zero_point": 31.4,
    }


def settle(
    alias: str,
    detection: Detection,
    deployment: ModelDeployment,
    head: str,
    state: str,
    app_version: str,
    stored_at: datetime,
    **fields: Any,
) -> None:
    """Store a claim and its outcome.

    Args:
        alias: The database to write to.
        detection: The detection.
        deployment: The deployment.
        head: The head's name.
        state: ``product``, ``refusal``, or ``failure``.
        app_version: The release recorded on the outcome.
        stored_at: The stored time recorded on it.
        **fields: The outcome's own fields.
    """
    revision = HeadRevision.objects.using(alias).get(deployment=deployment, head=head)
    claim = OutcomeClaim.objects.using(alias).create(
        detection=detection,
        deployment=deployment,
        head=head,
        revision=revision,
        state=state,
        attempts=1,
    )
    model = {"product": Product, "refusal": HeadRefusal, "failure": ProductFailure}[
        state
    ]
    row = model.objects.using(alias).create(
        claim=claim, app_version=app_version, **fields
    )
    model.objects.using(alias).filter(pk=row.pk).update(stored_at=stored_at)


def populate(
    alias: str,
    *,
    app_version: str = "v1.0.0",
    stored_at: datetime = datetime(2026, 11, 14, 4, 0, tzinfo=UTC),
    filler_rows: int = 0,
    forecast_points: int = 3,
    forecast_flux: float = 12.5,
    second_refused: bool = True,
    extra_detection: bool = False,
    caveats: tuple[str, ...] = (),
    refusal_message: str = "no usable observations",
    package_version: str = "1.0.0",
    sn_like_threshold: float = 0.5,
) -> None:
    """Store one drained replay's outcomes for two objects.

    Object 1001 qualified on detection 11, which got a flux product and a
    classification failure; its detection 12 got a flux refusal (or a
    product instead). Object 1002 never qualified; its detection 21 is
    pending.

    Args:
        alias: The database to write to.
        app_version: The release recorded on every outcome.
        stored_at: The stored time recorded on every outcome.
        filler_rows: Objects to create and delete first, so row ids shift.
        forecast_points: Observations in detection 11's packet.
        forecast_flux: Detection 11's first forecast flux.
        second_refused: Refuse detection 12 instead of giving it a product.
        extra_detection: Also store a pending detection 22 of object 1002.
        caveats: Caveat codes on detection 11's product.
        refusal_message: The refusal's message.
        package_version: The deployment's package version.
        sn_like_threshold: Lasair's SN-like threshold in the qualification
            snapshots.
    """
    for filler in range(filler_rows):
        RubinObject.objects.using(alias).create(dia_object_id=-1 - filler)
    RubinObject.objects.using(alias).filter(dia_object_id__lt=0).delete()

    deployment = ModelDeployment.objects.using(alias).create(
        package_name=REFERENCE,
        package_version=package_version,
        contract_version="1.0",
        artifact_digest=DIGEST,
        declaration={},
    )
    for head in (FLUX, CLASSES):
        HeadRevision.objects.using(alias).create(
            deployment=deployment, head=head, enabled=True, effective_at=EPOCH
        )

    event = {
        "dia_source_id": 11,
        "broker": "lasair",
        "reliability_score": 0.9,
        "sn_like_score": 1.0,
        "reliability_cut": 0.6,
        "sn_like_threshold": 0.5,
    }
    qualification = {
        "thresholds": {
            "lasair": {"reliability_cut": 0.6, "sn_like_threshold": sn_like_threshold}
        },
        "qualifying_event": event,
    }
    obj = RubinObject.objects.using(alias).create(dia_object_id=1001)
    first, second = (
        Detection.objects.using(alias).create(
            dia_source_id=dia_source_id,
            rubin_object=obj,
            state="owed",
            observed_mjd=observed_mjd,
            owed_at=stored_at,
            qualification=qualification,
        )
        for dia_source_id, observed_mjd in ((11, 60993.1), (12, 60994.1))
    )
    RubinObject.objects.using(alias).filter(pk=obj.pk).update(
        qualifying_detection=first,
        qualifying_broker="lasair",
        qualifying_reliability_score=0.9,
        qualifying_sn_like_score=1.0,
        qualifying_reliability_cut=0.6,
        qualifying_sn_like_threshold=0.5,
        qualified_at=stored_at,
    )
    quiet = RubinObject.objects.using(alias).create(dia_object_id=1002)
    Detection.objects.using(alias).create(
        dia_source_id=21, rubin_object=quiet, state="pending", observed_mjd=60993.2
    )
    if extra_detection:
        Detection.objects.using(alias).create(
            dia_source_id=22, rubin_object=quiet, state="pending", observed_mjd=60994.2
        )

    common = (app_version, stored_at)
    settle(
        alias,
        first,
        deployment,
        FLUX,
        "product",
        *common,
        output_kind="flux_series",
        payload=flux_payload(forecast_flux),
        caveats=[{"code": code, "detail": {}} for code in caveats],
        light_curve=light_curve(forecast_points),
    )
    settle(
        alias,
        first,
        deployment,
        CLASSES,
        "failure",
        *common,
        reason="RuntimeError: boom",
        attempts=3,
    )
    if second_refused:
        settle(
            alias,
            second,
            deployment,
            FLUX,
            "refusal",
            *common,
            code="no_usable_observations",
            message=refusal_message,
        )
    else:
        settle(
            alias,
            second,
            deployment,
            FLUX,
            "product",
            *common,
            output_kind="flux_series",
            payload=flux_payload(),
            light_curve=light_curve(),
        )


def database_fingerprint(alias: str) -> dict[str, tuple[int, str]]:
    """Count and hash every row of every table the service stores.

    Args:
        alias: The database to read.

    Returns:
        For each table, its row count and a digest of every row's values.
    """
    fingerprint = {}
    models = list(apps.get_app_config("seldon").get_models())
    models.append(Delivery.points.through)
    for model in models:
        rows = list(model.objects.using(alias).order_by("pk").values())
        digest = hashlib.sha256(repr(rows).encode()).hexdigest()
        fingerprint[model._meta.db_table] = (len(rows), digest)
    return fingerprint


class CompareReplaysTests(TransactionTestCase):
    """Two replay databases compared detection by detection."""

    @classmethod
    def setUpClass(cls) -> None:
        """Create the right-hand database from the test database."""
        super().setUpClass()
        cls.left_name = connection.settings_dict["NAME"]
        cls.right_name = f"{cls.left_name}_compare_right"
        connections.close_all()
        quote = connection.ops.quote_name
        with connection._nodb_cursor() as cursor:
            cursor.execute(f"DROP DATABASE IF EXISTS {quote(cls.right_name)}")
            cursor.execute(
                f"CREATE DATABASE {quote(cls.right_name)} "
                f"TEMPLATE {quote(cls.left_name)}"
            )
        right = copy.deepcopy(connections.settings[LEFT])
        right["NAME"] = cls.right_name
        connections.settings[RIGHT_ALIAS] = right
        cls.databases = frozenset(cls.databases | {RIGHT_ALIAS, *COMPARED_ALIASES})

    @classmethod
    def tearDownClass(cls) -> None:
        """Drop the right-hand database and its connection alias."""
        connections[RIGHT_ALIAS].close()
        del connections[RIGHT_ALIAS]
        del connections.settings[RIGHT_ALIAS]
        with connection._nodb_cursor() as cursor:
            cursor.execute(
                f"DROP DATABASE IF EXISTS {connection.ops.quote_name(cls.right_name)}"
            )
        super().tearDownClass()

    def compare(self, *extra: str) -> tuple[dict[str, Any], bool]:
        """Run the command with ``--json`` on the two databases.

        Args:
            *extra: More command-line options.

        Returns:
            The parsed report, and whether the command signalled differences.
        """
        out = StringIO()
        failed = False
        try:
            call_command(
                "compare_replays",
                "--left",
                self.left_name,
                "--right",
                self.right_name,
                "--json",
                *extra,
                stdout=out,
            )
        except CommandError:
            failed = True
        return json.loads(out.getvalue()), failed

    def test_identical_replays_match_despite_stored_at_version_and_ids(self) -> None:
        """Fields that differ by design do not count as differences."""
        populate(LEFT)
        left_pk = RubinObject.objects.using(LEFT).get(dia_object_id=1001).pk
        populate(
            RIGHT_ALIAS,
            app_version="v2.0.0",
            stored_at=datetime(2026, 11, 15, 9, 30, tzinfo=UTC),
            filler_rows=left_pk,
        )
        self.assertNotEqual(
            RubinObject.objects.using(RIGHT_ALIAS).get(dia_object_id=1001).pk, left_pk
        )

        result, failed = self.compare()

        self.assertFalse(failed)
        self.assertEqual(result["differences"], [])
        self.assertEqual(result["only_in_left"], [])
        self.assertEqual(result["only_in_right"], [])
        self.assertEqual(result["matched"]["detections"], 3)
        self.assertEqual(result["matched"]["products"], 1)
        self.assertEqual(result["matched"]["refusals"], 1)
        self.assertEqual(result["matched"]["failures"], 1)
        self.assertEqual(result["matched"]["objects"], 2)

    def test_light_curve_one_point_different_is_reported(self) -> None:
        """A packet one point longer is named with its detection, package,
        version and head."""
        populate(LEFT)
        populate(RIGHT_ALIAS, forecast_points=4)

        result, failed = self.compare()

        self.assertTrue(failed)
        (found,) = result["differences"]
        self.assertEqual(
            {k: found[k] for k in ("dia_source_id", "package_name", "head", "field")},
            {
                "dia_source_id": 11,
                "package_name": REFERENCE,
                "head": FLUX,
                "field": "light_curve",
            },
        )
        self.assertEqual(found["package_version"], "1.0.0")

    def test_product_in_one_and_refusal_in_other_is_reported(self) -> None:
        """A detection's head gave a product on one side and a refusal on
        the other."""
        populate(LEFT)
        populate(RIGHT_ALIAS, second_refused=False)

        result, failed = self.compare()

        self.assertTrue(failed)
        fields = {(d["dia_source_id"], d["field"]) for d in result["differences"]}
        self.assertIn((12, "outcome"), fields)
        outcome = next(d for d in result["differences"] if d["field"] == "outcome")
        self.assertEqual((outcome["left"], outcome["right"]), ("refusal", "product"))
        self.assertEqual(outcome["head"], FLUX)

    def test_detection_in_only_one_database_is_reported(self) -> None:
        """A detection stored only on the right."""
        populate(LEFT)
        populate(RIGHT_ALIAS, extra_detection=True)

        result, failed = self.compare()

        self.assertTrue(failed)
        self.assertEqual(result["only_in_left"], [])
        self.assertEqual(result["only_in_right"], [22])
        self.assertEqual(result["differences"], [])

    def test_text_report_names_the_difference(self) -> None:
        """Without ``--json`` the report is text naming the detection."""
        populate(LEFT)
        populate(RIGHT_ALIAS, forecast_points=4)
        out = StringIO()

        with self.assertRaises(CommandError):
            call_command(
                "compare_replays",
                "--left",
                self.left_name,
                "--right",
                self.right_name,
                stdout=out,
            )

        self.assertIn("11", out.getvalue())
        self.assertIn("light_curve", out.getvalue())

    def test_writes_nothing_to_either_database(self) -> None:
        """Both databases are byte-for-byte unchanged afterwards."""
        populate(LEFT)
        populate(RIGHT_ALIAS, forecast_points=4, extra_detection=True)
        before = (database_fingerprint(LEFT), database_fingerprint(RIGHT_ALIAS))

        self.compare()

        after = (database_fingerprint(LEFT), database_fingerprint(RIGHT_ALIAS))
        self.assertEqual(after, before)

    def test_compared_connections_refuse_writes(self) -> None:
        """A write through the command's connections is refused."""
        with read_only_databases(self.left_name, self.right_name):
            with self.assertRaises(InternalError):
                RubinObject.objects.using(LEFT_ALIAS).create(dia_object_id=9)
        self.assertFalse(RubinObject.objects.using(LEFT).exists())

    def test_output_tolerance_matches_payloads_but_not_light_curves(self) -> None:
        """Payloads within the tolerance match; light curves stay exact."""
        populate(LEFT)
        populate(RIGHT_ALIAS, forecast_flux=12.5 + 1e-7)

        exact, exact_failed = self.compare()
        tolerant, tolerant_failed = self.compare("--output-tolerance", "1e-6")

        self.assertTrue(exact_failed)
        self.assertIn(
            (11, "payload"),
            {(d["dia_source_id"], d["field"]) for d in exact["differences"]},
        )
        self.assertFalse(tolerant_failed)
        self.assertEqual(tolerant["differences"], [])

    def test_output_tolerance_still_fails_on_light_curve(self) -> None:
        """A light-curve difference fails even with an output tolerance."""
        populate(LEFT)
        populate(RIGHT_ALIAS, forecast_points=4, forecast_flux=12.5 + 1e-7)

        result, failed = self.compare("--output-tolerance", "1e-6")

        self.assertTrue(failed)
        fields = {(d["dia_source_id"], d["field"]) for d in result["differences"]}
        self.assertIn((11, "light_curve"), fields)
        self.assertNotIn((11, "payload"), fields)

    def test_object_qualifying_state_difference_is_reported(self) -> None:
        """An object qualified by a different broker is reported."""
        populate(LEFT)
        populate(RIGHT_ALIAS)
        RubinObject.objects.using(RIGHT_ALIAS).filter(dia_object_id=1001).update(
            qualifying_broker="fink"
        )

        result, failed = self.compare()

        self.assertTrue(failed)
        self.assertIn(
            {"dia_object_id": 1001, "field": "qualifying_broker"},
            [
                {"dia_object_id": d.get("dia_object_id"), "field": d["field"]}
                for d in result["object_differences"]
            ],
        )

    def test_caveat_list_difference_is_reported(self) -> None:
        """A product carrying a caveat on one side only is reported."""
        populate(LEFT)
        populate(RIGHT_ALIAS, caveats=("sparse_light_curve",))

        result, failed = self.compare("--output-tolerance", "1")

        self.assertTrue(failed)
        fields = {(d["dia_source_id"], d["field"]) for d in result["differences"]}
        self.assertEqual(fields, {(11, "caveats")})

    def test_refusal_message_difference_is_reported(self) -> None:
        """A refusal with another message is reported, with both messages."""
        populate(LEFT)
        populate(RIGHT_ALIAS, refusal_message="too few points")

        result, failed = self.compare()

        self.assertTrue(failed)
        (found,) = result["differences"]
        self.assertEqual((found["dia_source_id"], found["field"]), (12, "message"))
        self.assertEqual(found["right"], "too few points")

    def test_another_package_version_is_another_key(self) -> None:
        """Outcomes stored under another version are not paired: each of
        the three outcomes is present on one side and absent on the other."""
        populate(LEFT)
        populate(RIGHT_ALIAS, package_version="1.0.1")

        result, failed = self.compare()

        self.assertTrue(failed)
        presence = sorted(
            (d["dia_source_id"], d["package_version"], d["left"], d["right"])
            for d in result["differences"]
        )
        self.assertEqual(len(presence), 6)
        self.assertIn((11, "1.0.0", "product", "absent"), presence)
        self.assertIn((11, "1.0.1", "absent", "product"), presence)
        self.assertIn((12, "1.0.1", "absent", "refusal"), presence)

    def test_qualification_snapshot_difference_is_reported(self) -> None:
        """Detections owed under another threshold are reported."""
        populate(LEFT)
        populate(RIGHT_ALIAS, sn_like_threshold=0.7)

        result, failed = self.compare()

        self.assertTrue(failed)
        fields = {(d["dia_source_id"], d["field"]) for d in result["differences"]}
        self.assertEqual(fields, {(11, "qualification"), (12, "qualification")})

    @mock.patch.dict(os.environ, REPLAY_ENV)
    def test_two_drained_replays_of_one_recording_match(self) -> None:
        """The recorded fixture alerts are replayed into each database with
        every head of the registered release seeded, then drained by a worker
        until idle. The two databases compare with no differences, and some
        detections got products."""
        deployments.register(StubPackage().describe(), DIGEST)
        releases = live_releases()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_fixture_recording(root)
            for name in (self.right_name, self.left_name):
                point_at(name)
                try:
                    seed_heads(releases)
                    run_replay(
                        replay_of(root, ReplayMode.ORDERED, FakeWall()),
                        poll_timeout=0.01,
                    )
                    start(StubPackage(), worker_settings()).run(until_idle=True)
                finally:
                    point_at(self.left_name)

        result, failed = self.compare()

        self.assertFalse(failed, result["differences"])
        self.assertGreater(result["matched"]["products"], 0)
        self.assertFalse(
            OutcomeClaim.objects.using(RIGHT_ALIAS).filter(state="due").exists()
        )
