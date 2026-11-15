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
from datetime import UTC, datetime
from io import StringIO
from typing import Any

from django.apps import apps
from django.core.management import CommandError, call_command
from django.db import InternalError, connection, connections
from django.test import TransactionTestCase

from seldon.management.commands.compare_replays import (
    COMPARED_ALIASES,
    LEFT_ALIAS,
    read_only_databases,
)
from seldon.models import Delivery, Detection, Forecast, Refusal, RubinObject

CHECKPOINT = "seldon-2.0-roman-elasticc/epoch=1086-val_loss=1.18.ckpt"
RIGHT_ALIAS = "compare_replays_test_right"
LEFT = "default"


def light_curve(points: int = 3) -> dict[str, list]:
    """Build a small input light curve.

    Args:
        points: How many observations it holds.

    Returns:
        The light curve as stored with a forecast.
    """
    return {
        "times": [60990.1 + day for day in range(points)],
        "flux": [10.0 + day for day in range(points)],
        "flux_err": [1.0] * points,
        "detected": [True] * points,
        "bands": ["r"] * points,
    }


def outputs(flux: float = 12.5) -> dict[str, Any]:
    """Build model outputs.

    Args:
        flux: The first forecast flux value.

    Returns:
        The outputs as stored with a forecast.
    """
    return {
        "forecast_flux": [flux, 13.25],
        "forecast_flux_err": [0.5, 0.75],
        "predicted_class": "SNIa",
    }


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
) -> None:
    """Store one replay's outcome for two objects.

    Object 1001 qualified on detection 11, which was forecast; its detection
    12 was refused (or forecast instead). Object 1002 never qualified; its
    detection 21 is pending.

    Args:
        alias: The database to write to.
        app_version: The release recorded on forecasts and refusals.
        stored_at: The stored time recorded on forecasts and refusals.
        filler_rows: Objects to create and delete first, so row ids shift.
        forecast_points: Observations in detection 11's light curve.
        forecast_flux: Detection 11's first forecast flux.
        second_refused: Refuse detection 12 instead of forecasting it.
        extra_detection: Also store a pending detection 22 of object 1002.
    """
    for filler in range(filler_rows):
        RubinObject.objects.using(alias).create(dia_object_id=-1 - filler)
    RubinObject.objects.using(alias).filter(dia_object_id__lt=0).delete()

    obj = RubinObject.objects.using(alias).create(dia_object_id=1001)
    first = Detection.objects.using(alias).create(
        dia_source_id=11, rubin_object=obj, state="forecast", observed_mjd=60993.1
    )
    second = Detection.objects.using(alias).create(
        dia_source_id=12,
        rubin_object=obj,
        state="refused" if second_refused else "forecast",
        observed_mjd=60994.1,
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

    event = {
        "dia_source_id": 11,
        "broker": "lasair",
        "reliability_score": 0.9,
        "sn_like_score": 1.0,
        "reliability_cut": 0.6,
        "sn_like_threshold": 0.5,
    }
    store_forecast(
        alias, first, forecast_points, forecast_flux, event, app_version, stored_at
    )
    if second_refused:
        Refusal.objects.using(alias).create(
            detection=second,
            checkpoint=CHECKPOINT,
            code="no_usable_observations",
            message="no usable observations",
            app_version=app_version,
        )
        Refusal.objects.using(alias).update(stored_at=stored_at)
    else:
        store_forecast(alias, second, 3, 12.5, event, app_version, stored_at)


def store_forecast(
    alias: str,
    detection: Detection,
    points: int,
    flux: float,
    event: dict[str, Any],
    app_version: str,
    stored_at: datetime,
) -> None:
    """Store one forecast of a detection.

    Args:
        alias: The database to write to.
        detection: The detection forecast.
        points: Observations in its light curve.
        flux: Its first forecast flux.
        event: The qualifying event stored with it.
        app_version: The release recorded on it.
        stored_at: The stored time recorded on it.
    """
    forecast = Forecast.objects.using(alias).create(
        detection=detection,
        checkpoint=CHECKPOINT,
        light_curve=light_curve(points),
        zero_point=31.4,
        grid={"eval_times": [61000.0, 61010.0], "eval_bands": ["r", "r"]},
        outputs=outputs(flux),
        in_training_regime=True,
        sparse=False,
        n_observations=points,
        min_points=3,
        untrained_bands=[],
        eval_span_days=10.0,
        eval_span_in_time_scales=0.1,
        time_scale_days=100.0,
        library_version="1.2.0",
        provenance_zero_point_mag=27.5,
        torch_version="2.9.0+cpu",
        app_version=app_version,
        thresholds={"lasair": {"reliability_cut": 0.6, "sn_like_threshold": 0.5}},
        qualifying_event=event,
    )
    Forecast.objects.using(alias).filter(pk=forecast.pk).update(stored_at=stored_at)


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
        self.assertEqual(result["matched"]["forecasts"], 1)
        self.assertEqual(result["matched"]["refusals"], 1)
        self.assertEqual(result["matched"]["objects"], 2)

    def test_light_curve_one_point_different_is_reported(self) -> None:
        """A light curve one point longer is named with its detection."""
        populate(LEFT)
        populate(RIGHT_ALIAS, forecast_points=4)

        result, failed = self.compare()

        self.assertTrue(failed)
        fields = {(d["dia_source_id"], d["field"]) for d in result["differences"]}
        self.assertIn((11, "light_curve"), fields)
        self.assertEqual({d["dia_source_id"] for d in result["differences"]}, {11})

    def test_forecast_in_one_and_refused_in_other_is_reported(self) -> None:
        """A detection forecast on one side and refused on the other."""
        populate(LEFT)
        populate(RIGHT_ALIAS, second_refused=False)

        result, failed = self.compare()

        self.assertTrue(failed)
        fields = {(d["dia_source_id"], d["field"]) for d in result["differences"]}
        self.assertIn((12, "outcome"), fields)
        outcome = next(d for d in result["differences"] if d["field"] == "outcome")
        self.assertEqual((outcome["left"], outcome["right"]), ("refused", "forecast"))

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

    def test_output_tolerance_matches_outputs_but_not_light_curves(self) -> None:
        """Outputs within the tolerance match; light curves stay exact."""
        populate(LEFT)
        populate(RIGHT_ALIAS, forecast_flux=12.5 + 1e-7)

        exact, exact_failed = self.compare()
        tolerant, tolerant_failed = self.compare("--output-tolerance", "1e-6")

        self.assertTrue(exact_failed)
        self.assertIn(
            (11, "outputs"),
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
        self.assertNotIn((11, "outputs"), fields)

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
