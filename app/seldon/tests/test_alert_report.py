"""Tests for the ``alert_report`` inspection command.

Every expected number below is worked out by hand from the records each test
builds. Outcomes are stored per deployment and head, so the coverage and
latency reports are per deployment and head too. These need the compose
``db`` service; run them with ``run/seldonctl slim_dev test``.
"""

import hashlib
import json
from contextlib import redirect_stdout
from datetime import UTC, date, datetime, timedelta
from io import StringIO
from types import SimpleNamespace
from typing import Any
from unittest import mock

from django.apps import apps
from django.core.management import CommandError, call_command
from django.test import TestCase

from seldon.models import (
    Delivery,
    Detection,
    HeadRefusal,
    HeadRevision,
    IntakeLagCount,
    IntakeSkip,
    ModelDeployment,
    OutcomeClaim,
    Product,
    ProductFailure,
    RubinObject,
    StreamGap,
)

MJD_EPOCH = datetime(1858, 11, 17, tzinfo=UTC)
TAI_MINUS_UTC = timedelta(seconds=37)
COMMAND = "seldon.management.commands.alert_report"
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
REFERENCE = "seldon-reference"
CLASSES = "classification"
FLUX = "flux_forecast"
OUTPUT_KINDS = {CLASSES: "class_probabilities", FLUX: "flux_series"}
OWED_AT = datetime(2026, 11, 12, 13, 0, tzinfo=UTC)


def mjd_tai(utc: datetime) -> float:
    """Express a UTC time as the MJD in TAI that Rubin would report.

    Args:
        utc: The time, in UTC.

    Returns:
        The MJD of that instant on the TAI scale.
    """
    return (utc + TAI_MINUS_UTC - MJD_EPOCH) / timedelta(days=1)


def at(text: str) -> datetime:
    """Read a UTC time written as ``YYYY-MM-DD HH:MM:SS``.

    Args:
        text: The time.

    Returns:
        The aware datetime.
    """
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


def make_detection(
    rubin_object: RubinObject,
    dia_source_id: int,
    observed: datetime,
    state: str = Detection.State.OWED,
    qualifying_event: dict[str, Any] | None = None,
) -> Detection:
    """Store a detection observed at a UTC time.

    An owed detection is owed at ``OWED_AT``.

    Args:
        rubin_object: The object it belongs to.
        dia_source_id: Its ``diaSourceId``.
        observed: When it was observed, in UTC.
        state: Its state.
        qualifying_event: The qualifying event snapshot taken when it became
            owed.

    Returns:
        The stored detection.
    """
    return Detection.objects.create(
        dia_source_id=dia_source_id,
        rubin_object=rubin_object,
        state=state,
        observed_mjd=mjd_tai(observed),
        owed_at=OWED_AT if state == Detection.State.OWED else None,
        qualification=(
            {"thresholds": {}, "qualifying_event": qualifying_event}
            if qualifying_event
            else {}
        ),
    )


def make_delivery(
    detection: Detection,
    broker: str,
    published: datetime | None,
    received: datetime,
    role: str = Delivery.Role.QUALIFYING,
) -> Delivery:
    """Store one broker's delivery of a detection.

    Args:
        detection: The detection delivered.
        broker: The broker name.
        published: When the broker published it, or ``None``.
        received: When the service received it.
        role: The broker's role.

    Returns:
        The stored delivery.
    """
    return Delivery.objects.create(
        detection=detection,
        broker=broker,
        role=role,
        published_at=published,
        received_at=received,
    )


def make_deployment(
    version: str, retired: bool = False, name: str = REFERENCE
) -> ModelDeployment:
    """Register a deployment.

    Args:
        version: Its package version.
        retired: Whether it is retired.
        name: Its package name.

    Returns:
        The deployment.
    """
    return ModelDeployment.objects.create(
        package_name=name,
        package_version=version,
        contract_version="1.0",
        artifact_digest=f"sha256:{name}-{version}",
        declaration={},
        retired_at=at("2026-11-01 00:00:00") if retired else None,
    )


def enable(deployment: ModelDeployment, head: str) -> HeadRevision:
    """Return the deployment head's revision enabled from the epoch.

    Args:
        deployment: The deployment.
        head: The head's name.

    Returns:
        The revision, created on first use.
    """
    revision, _ = HeadRevision.objects.get_or_create(
        deployment=deployment,
        head=head,
        effective_at=EPOCH,
        defaults={"enabled": True},
    )
    return revision


def make_outcome(
    detection: Detection,
    deployment: ModelDeployment,
    head: str,
    kind: str,
    stored: datetime | None = None,
    *,
    created: datetime | None = None,
    payload: dict[str, Any] | None = None,
    caveats: tuple[str, ...] = (),
) -> OutcomeClaim:
    """Store a claim and, unless it is still due, its outcome.

    Args:
        detection: The detection.
        deployment: The deployment.
        head: The head's name.
        kind: ``product``, ``refusal``, ``failure``, or ``due``.
        stored: When the outcome was stored.
        created: When the claim was created; 30 seconds before ``stored``
            by default.
        payload: A product's payload; fixed class probabilities by default.
        caveats: A product's caveat codes.

    Returns:
        The claim.
    """
    claim = OutcomeClaim.objects.create(
        detection=detection,
        deployment=deployment,
        head=head,
        revision=enable(deployment, head),
        state=kind,
        attempts=0 if kind == "due" else 1,
    )
    if created is None and stored is not None:
        created = stored - timedelta(seconds=30)
    if created is not None:
        OutcomeClaim.objects.filter(pk=claim.pk).update(created_at=created)
    common = {"claim": claim, "app_version": "v0.0.0-test"}
    if kind == "product":
        model = Product
        row = Product.objects.create(
            output_kind=OUTPUT_KINDS[head],
            payload=payload or {"probabilities": {"SNIa": 0.8, "SNII": 0.2}},
            caveats=[{"code": code, "detail": {}} for code in caveats],
            light_curve={},
            **common,
        )
    elif kind == "refusal":
        model = HeadRefusal
        row = HeadRefusal.objects.create(
            code="no_usable_observations", message="no usable observations", **common
        )
    elif kind == "failure":
        model = ProductFailure
        row = ProductFailure.objects.create(
            reason="RuntimeError: boom", attempts=3, **common
        )
    else:
        return claim
    model.objects.filter(pk=row.pk).update(stored_at=stored)
    return claim


def report(*args: str) -> str:
    """Run the report command and capture what it printed.

    Args:
        *args: The subcommand and its arguments.

    Returns:
        The command's standard output.
    """
    out = StringIO()
    call_command("alert_report", *args, stdout=out)
    return out.getvalue()


def report_json(*args: str) -> Any:
    """Run the report command with ``--json`` and parse its output.

    Args:
        *args: The subcommand and its arguments.

    Returns:
        The parsed JSON document.
    """
    return json.loads(report(*args, "--json"))


def database_fingerprint() -> dict[str, tuple[int, str]]:
    """Count and hash every row of every table the service stores.

    Returns:
        For each table, its row count and a digest of every row's values.
    """
    fingerprint = {}
    models = list(apps.get_app_config("seldon").get_models())
    models.append(Delivery.points.through)
    for model in models:
        rows = list(model.objects.order_by("pk").values())
        digest = hashlib.sha256(repr(rows).encode()).hexdigest()
        fingerprint[model._meta.db_table] = (len(rows), digest)
    return fingerprint


def head_rows(night: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    """Index a coverage night's head rows by package version and head.

    Args:
        night: One night of the coverage report.

    Returns:
        ``(package_version, head)`` to the row.
    """
    return {(row["package_version"], row["head"]): row for row in night["heads"]}


class ObjectReportTests(TestCase):
    """One object's outcomes, in observation order."""

    @classmethod
    def setUpTestData(cls) -> None:
        """An object with five outcomes from two deployments, stored out of
        order.

        Its four detections are observed on 1, 2, 3, and 4 November. Under
        1.0.0's classification head the third gets a product first, the
        second is refused, the first gets a product with the sparse caveat,
        and the fourth fails; 1.1.0 gives the fourth a product. A second
        object has a product of its own that must not appear.
        """
        cls.obj = RubinObject.objects.create(
            dia_object_id=1001,
            qualifying_broker="lasair",
            qualifying_reliability_score=0.9,
            qualifying_sn_like_score=1.0,
            qualifying_reliability_cut=0.6,
            qualifying_sn_like_threshold=0.5,
        )
        event = {
            "dia_source_id": 11,
            "broker": "lasair",
            "reliability_score": 0.9,
            "sn_like_score": 1.0,
            "reliability_cut": 0.6,
            "sn_like_threshold": 0.5,
        }
        cls.event = event
        first = make_detection(cls.obj, 11, at("2026-11-01 04:00:00"), "owed", event)
        second = make_detection(cls.obj, 12, at("2026-11-02 04:00:00"), "owed", event)
        third = make_detection(cls.obj, 13, at("2026-11-03 04:00:00"), "owed", event)
        fourth = make_detection(cls.obj, 14, at("2026-11-04 04:00:00"), "owed", event)
        cls.obj.qualifying_detection = first
        cls.obj.save()
        received = at("2026-11-05 00:00:00")
        make_delivery(first, "lasair", received, received)
        make_delivery(first, "babamul", received, received, "full_stream")
        make_delivery(second, "fink", received, received)
        make_delivery(third, "babamul", received, received, "full_stream")
        make_delivery(fourth, "fink", received, received)
        current = make_deployment("1.0.0")
        upgrade = make_deployment("1.1.0")
        make_outcome(third, current, CLASSES, "product", at("2026-11-05 00:01:00"))
        make_outcome(second, current, CLASSES, "refusal", at("2026-11-05 00:02:00"))
        make_outcome(
            first,
            current,
            CLASSES,
            "product",
            at("2026-11-05 00:03:00"),
            payload={"probabilities": {"SNIa": 0.3, "SNII": 0.7}},
            caveats=("sparse_light_curve",),
        )
        make_outcome(fourth, upgrade, CLASSES, "product", at("2026-11-05 00:04:00"))
        make_outcome(fourth, current, CLASSES, "failure", at("2026-11-05 00:05:00"))
        other = RubinObject.objects.create(dia_object_id=2002)
        stray = make_detection(other, 21, at("2026-11-01 05:00:00"))
        make_outcome(stray, current, CLASSES, "product", at("2026-11-05 00:06:00"))

    def test_lists_outcomes_in_observation_order(self) -> None:
        """Five outcomes come back in observation order, then stored order.

        Each record names its detection, deployment and head, its caveat
        codes, its predicted class, the delivering brokers, and the
        qualifying event captured when the detection became owed; the
        refusal carries its code and the failure its reason.
        """
        result = report_json("object", "1001")

        self.assertEqual(result["dia_object_id"], 1001)
        records = result["records"]
        self.assertEqual(
            [(r["kind"], r["dia_source_id"]) for r in records],
            [
                ("product", 11),
                ("refusal", 12),
                ("product", 13),
                ("product", 14),
                ("failure", 14),
            ],
        )
        self.assertEqual(
            [(r["package_version"], r["head"]) for r in records],
            [("1.0.0", CLASSES)] * 3 + [("1.1.0", CLASSES), ("1.0.0", CLASSES)],
        )
        first = records[0]
        self.assertEqual(first["package_name"], REFERENCE)
        self.assertEqual(first["output_kind"], "class_probabilities")
        self.assertEqual(first["caveats"], ["sparse_light_curve"])
        self.assertEqual(first["predicted_class"], "SNII")
        self.assertEqual(first["brokers"], ["babamul", "lasair"])
        self.assertEqual(first["qualifying_event"], self.event)
        self.assertEqual(first["observed_utc"], "2026-11-01T04:00:00+00:00")
        self.assertEqual(first["night"], "2026-10-31")
        refusal = records[1]
        self.assertEqual(refusal["code"], "no_usable_observations")
        self.assertIsNone(refusal["predicted_class"])
        self.assertEqual(records[2]["caveats"], [])
        self.assertEqual(records[2]["predicted_class"], "SNIa")
        self.assertEqual(records[4]["reason"], "RuntimeError: boom")
        self.assertEqual(result["unresolved_detections"], [])

    def test_plain_text_prints_records_in_observation_order(self) -> None:
        """The plain-text listing shows the records in the same order."""
        text = report("object", "1001")

        positions = [text.index(f"diaSource {n}") for n in (11, 12, 13, 14)]
        self.assertEqual(positions, sorted(positions))
        self.assertIn(f"{REFERENCE} 1.1.0", text)
        self.assertIn("sparse_light_curve", text)
        self.assertIn("RuntimeError: boom", text)
        self.assertNotIn("diaSource 21", text)

    def test_unknown_object_is_an_error(self) -> None:
        """Asking for an object the service never received fails clearly."""
        with self.assertRaises(CommandError):
            report("object", "9999")


class CoverageReportTests(TestCase):
    """Coverage of owed detections per deployment head, broker, and night."""

    @classmethod
    def setUpTestData(cls) -> None:
        """Seven detections around the boundary between two nights, and
        three deployments of the classification head.

        The night of 11 November runs from 11 November 12:00 UTC to
        12 November 12:00 UTC. In it: a detection at 11:59 UTC on
        12 November (Fink and Babamul delivered it), one at 23:00 (Lasair),
        one at 03:00 missed (Fink), one at 04:00 (Fink), and one (Lasair) at
        11:59:43 UTC, whose TAI time is already past noon. One at 05:00 is
        pending and counts nowhere. In the night of 12 November: one at
        12:01 UTC (Fink).

        Deployment 1.0.0 gave 31 and 32 products, refused 33, gave up on 37
        (a failure) and has 35 still due. Deployment 1.1.0 runs beside it
        over the same detections: it gave 31 and 33 products and has not
        claimed the rest yet, which are owed it all the same. A retired
        pre-contract deployment gave 31 a product before the cutover.
        """
        obj = RubinObject.objects.create(dia_object_id=3003)
        received = at("2026-11-12 13:00:00")
        rows = [
            (31, "2026-11-12 11:59:00", "owed", ["fink", "babamul"]),
            (32, "2026-11-12 12:01:00", "owed", ["fink"]),
            (33, "2026-11-11 23:00:00", "owed", ["lasair"]),
            (34, "2026-11-12 03:00:00", "missed", ["fink"]),
            (35, "2026-11-12 04:00:00", "owed", ["fink"]),
            (36, "2026-11-12 05:00:00", "pending", ["fink"]),
            (37, "2026-11-12 11:59:43", "owed", ["lasair"]),
        ]
        detections = {}
        for dia_source_id, observed, state, brokers in rows:
            detection = make_detection(obj, dia_source_id, at(observed), state)
            detections[dia_source_id] = detection
            for broker in brokers:
                make_delivery(detection, broker, received, received)
        stored = at("2026-11-12 14:00:00")
        current = make_deployment("1.0.0")
        for dia_source_id, kind in (
            (31, "product"),
            (32, "product"),
            (33, "refusal"),
            (35, "due"),
            (37, "failure"),
        ):
            make_outcome(detections[dia_source_id], current, CLASSES, kind, stored)
        upgrade = make_deployment("1.1.0")
        for dia_source_id in (31, 33):
            make_outcome(detections[dia_source_id], upgrade, CLASSES, "product", stored)
        retired = make_deployment(
            "seldon-2.0/old.ckpt", retired=True, name="seldon-pre-contract"
        )
        make_outcome(detections[31], retired, CLASSES, "product", stored)

    def test_counts_each_deployment_head_per_broker_per_night(self) -> None:
        """Each night's counts per deployment head, per broker and over all.

        Night of 11 November, 1.0.0: Fink delivered two owed detections (one
        product, one still due), share one half; Lasair two (one refused,
        one failed), share one half, the failure in its own column and not
        covered; Babamul one product. Over all brokers four detections, one
        product, one refusal, one failure, one due: share one half.
        1.1.0 over the same four: two products and two due, share one half.
        Night of 12 November: 1.0.0's one product; 1.1.0's one due.
        """
        result = report_json("coverage")

        nights = {n["night"]: n for n in result["nights"]}
        self.assertEqual(sorted(nights), ["2026-11-11", "2026-11-12"])
        earlier = head_rows(nights["2026-11-11"])
        self.assertEqual(sorted(earlier), [("1.0.0", CLASSES), ("1.1.0", CLASSES)])
        current = earlier[("1.0.0", CLASSES)]
        self.assertEqual(current["package_name"], REFERENCE)
        self.assertEqual(
            current["brokers"]["fink"],
            {
                "detections": 2,
                "products": 1,
                "refusals": 0,
                "failures": 0,
                "due": 1,
                "share": 0.5,
            },
        )
        self.assertEqual(
            current["brokers"]["lasair"],
            {
                "detections": 2,
                "products": 0,
                "refusals": 1,
                "failures": 1,
                "due": 0,
                "share": 0.5,
            },
        )
        self.assertEqual(current["brokers"]["babamul"]["share"], 1.0)
        self.assertEqual(
            current["all"],
            {
                "detections": 4,
                "products": 1,
                "refusals": 1,
                "failures": 1,
                "due": 1,
                "share": 0.5,
            },
        )
        self.assertEqual(
            earlier[("1.1.0", CLASSES)]["all"],
            {
                "detections": 4,
                "products": 2,
                "refusals": 0,
                "failures": 0,
                "due": 2,
                "share": 0.5,
            },
        )
        later = head_rows(nights["2026-11-12"])
        self.assertEqual(later[("1.0.0", CLASSES)]["all"]["products"], 1)
        self.assertEqual(later[("1.0.0", CLASSES)]["all"]["share"], 1.0)
        self.assertEqual(later[("1.1.0", CLASSES)]["all"]["due"], 1)
        self.assertEqual(later[("1.1.0", CLASSES)]["all"]["share"], 0.0)

    def test_owed_and_missed_detections_per_broker(self) -> None:
        """The night of 11 November holds four owed detections and one
        missed one; Fink delivered two owed and the missed one."""
        result = report_json("coverage")

        earlier = next(n for n in result["nights"] if n["night"] == "2026-11-11")
        self.assertEqual(earlier["detections"]["all"], {"owed": 4, "missed": 1})
        self.assertEqual(
            earlier["detections"]["brokers"]["fink"], {"owed": 2, "missed": 1}
        )

    def test_retired_deployment_is_left_out(self) -> None:
        """The retired pre-contract deployment's product for 31 appears in
        no night."""
        result = report_json("coverage")

        names = {
            row["package_name"] for night in result["nights"] for row in night["heads"]
        }
        self.assertEqual(names, {REFERENCE})

    def test_night_range_selects_nights(self) -> None:
        """Asking for the night of 12 November alone leaves out the earlier one."""
        result = report_json(
            "coverage", "--from-night", "2026-11-12", "--to-night", "2026-11-12"
        )

        self.assertEqual([n["night"] for n in result["nights"]], ["2026-11-12"])
        self.assertEqual(result["nights"][0]["detections"]["all"]["owed"], 1)

    def test_plain_text_names_each_night_deployment_and_broker(self) -> None:
        """The plain-text table has a row per night, deployment head and
        broker."""
        text = report("coverage")

        self.assertIn("2026-11-11", text)
        self.assertIn(f"{REFERENCE} 1.1.0", text)
        self.assertIn(CLASSES, text)
        self.assertIn("fink", text)
        self.assertIn("50.0%", text)
        self.assertNotIn("seldon-pre-contract", text)


class LatencyReportTests(TestCase):
    """Publish-to-stored latency per deployment head, leaving out replays."""

    @classmethod
    def setUpTestData(cls) -> None:
        """Products of one object, with the silence period at 60 minutes.

        Times are minutes after 12 November 01:00 UTC. Deployment 1.0.0's
        classification head:

        - Qualifying detection 41, qualified by Lasair: Fink published it at
          0 and it arrived at 1; Lasair published it at 2 and it arrived at 3;
          product stored at 6. Measured from Lasair: 4 minutes.
        - Later detection 42: Babamul published at 10, arrived at 10.5;
          Fink published at 11, arrived at 12; stored at 18. Measured from
          the earliest arrival, Babamul's: 8 minutes.
        - Detection 43: published at 20, arrived at 140 (a replay); stored
          at 141. Left out.
        - Detection 44: no publish time; stored at 30. Counted apart.
        - Detection 45: published at 40, arrived at 45; stored at 60:
          20 minutes.
        - Detection 46: published at 48, refused at 50.

        Deployment 1.0.0's flux head was enabled later, back-dated: its
        claim for 46 was made at 400, after the same deployment's refusal,
        and its product stored at 500. A re-forecast; left out.

        Deployment 1.1.0 runs beside it: its product for 41, stored at 9,
        measures 7 minutes from Lasair. 1.0.0's earlier product for 41 does
        not make it a re-forecast.
        """
        start = at("2026-11-12 01:00:00")

        def t(minutes: float) -> datetime:
            return start + timedelta(minutes=minutes)

        obj = RubinObject.objects.create(dia_object_id=4004)
        event = {"dia_source_id": 41, "broker": "lasair"}
        observed = at("2026-11-12 00:00:00")
        current = make_deployment("1.0.0")
        upgrade = make_deployment("1.1.0")

        def detection(dia_source_id: int, hours: int) -> Detection:
            return make_detection(
                obj, dia_source_id, observed + timedelta(hours=hours), "owed", event
            )

        qualifying = detection(41, 0)
        make_delivery(qualifying, "fink", t(0), t(1))
        make_delivery(qualifying, "lasair", t(2), t(3))
        make_outcome(qualifying, current, CLASSES, "product", t(6))
        make_outcome(qualifying, upgrade, CLASSES, "product", t(9))

        later = detection(42, 1)
        make_delivery(later, "babamul", t(10), t(10.5), "full_stream")
        make_delivery(later, "fink", t(11), t(12))
        make_outcome(later, current, CLASSES, "product", t(18))

        replayed = detection(43, 2)
        make_delivery(replayed, "fink", t(20), t(140))
        make_outcome(replayed, current, CLASSES, "product", t(141))

        unpublished = detection(44, 3)
        make_delivery(unpublished, "fink", None, t(25))
        make_outcome(unpublished, current, CLASSES, "product", t(30))

        slow = detection(45, 4)
        make_delivery(slow, "fink", t(40), t(45))
        make_outcome(slow, current, CLASSES, "product", t(60))

        backfilled = detection(46, 5)
        make_delivery(backfilled, "fink", t(48), t(49))
        make_outcome(backfilled, current, CLASSES, "refusal", t(50))
        make_outcome(
            backfilled, current, FLUX, "product", t(500), created=t(400), payload={}
        )

    def heads(self, *args: str) -> dict[tuple[str, str], dict[str, Any]]:
        """Run the latency report with a 60-minute silence period.

        Args:
            *args: More options.

        Returns:
            Its per-head summaries by package version and head.
        """
        with mock.patch.dict("os.environ", {"SELDON_INTAKE_SILENCE_MINUTES": "60"}):
            result = report_json("latency", *args)
        return {(h["package_version"], h["head"]): h for h in result["heads"]}

    def test_percentile_excludes_replayed_deliveries(self) -> None:
        """1.0.0's classification latencies are 4, 8, and 20 minutes; the
        replay is left out.

        By nearest rank the 90th percentile of three values is the third,
        20 minutes; two of three are within 10 minutes. Had the replay's
        121 minutes been counted, the 90th percentile would be 121.
        """
        heads = self.heads()

        current = heads[("1.0.0", CLASSES)]
        self.assertEqual(current["package_name"], REFERENCE)
        self.assertEqual(current["products"], 5)
        self.assertEqual(current["measured"], 3)
        self.assertEqual(
            current["excluded"],
            {"replayed": 1, "no_published_at": 1, "reforecast": 0, "no_delivery": 0},
        )
        self.assertEqual(current["percentile_minutes"], 20.0)
        self.assertEqual(current["within_threshold"], 2)
        self.assertAlmostEqual(current["share_within_threshold"], 2 / 3)
        self.assertEqual(current["by_broker"]["lasair"]["percentile_minutes"], 4.0)
        self.assertEqual(current["by_broker"]["babamul"]["percentile_minutes"], 8.0)
        self.assertEqual(current["by_broker"]["fink"]["percentile_minutes"], 20.0)

    def test_reforecast_is_excluded_within_a_deployment_only(self) -> None:
        """The back-dated flux product is a re-forecast of 1.0.0's own
        earlier outcome; 1.1.0's first product for 41 is measured."""
        heads = self.heads()

        flux = heads[("1.0.0", FLUX)]
        self.assertEqual(flux["products"], 1)
        self.assertEqual(flux["excluded"]["reforecast"], 1)
        self.assertEqual(flux["measured"], 0)
        upgrade = heads[("1.1.0", CLASSES)]
        self.assertEqual(upgrade["excluded"]["reforecast"], 0)
        self.assertEqual(upgrade["measured"], 1)
        self.assertEqual(upgrade["percentile_minutes"], 7.0)

    def test_percentile_and_threshold_are_chosen(self) -> None:
        """The median of 4, 8, and 20 is 8; one of three is within 5 minutes."""
        heads = self.heads("--percentile", "50", "--threshold-minutes", "5")

        self.assertEqual(heads[("1.0.0", CLASSES)]["percentile_minutes"], 8.0)
        self.assertEqual(heads[("1.0.0", CLASSES)]["within_threshold"], 1)

    def test_longer_silence_period_keeps_the_delivery(self) -> None:
        """With a 180-minute silence period the 120-minute arrival is no replay.

        The latencies are then 4, 8, 20, and 121 minutes, and the 90th
        percentile by nearest rank is the fourth, 121.
        """
        with mock.patch.dict("os.environ", {"SELDON_INTAKE_SILENCE_MINUTES": "180"}):
            result = report_json("latency")

        current = next(
            h
            for h in result["heads"]
            if (h["package_version"], h["head"]) == ("1.0.0", CLASSES)
        )
        self.assertEqual(current["measured"], 4)
        self.assertEqual(current["percentile_minutes"], 121.0)

    def test_stored_window_selects_products(self) -> None:
        """Products stored before 01:20 UTC: 1.0.0's first two (4 and 8
        minutes) and 1.1.0's one; the flux head stored none."""
        heads = self.heads("--until", "2026-11-12T01:20:00+00:00")

        self.assertEqual(heads[("1.0.0", CLASSES)]["products"], 2)
        self.assertEqual(heads[("1.0.0", CLASSES)]["percentile_minutes"], 8.0)
        self.assertEqual(heads[("1.1.0", CLASSES)]["products"], 1)
        self.assertNotIn(("1.0.0", FLUX), heads)

    def test_plain_text_states_the_percentile_per_head(self) -> None:
        """The plain-text report names each head and its percentile."""
        with mock.patch.dict("os.environ", {"SELDON_INTAKE_SILENCE_MINUTES": "60"}):
            text = report("latency")

        self.assertIn(f"{REFERENCE} 1.0.0 {CLASSES}", text)
        self.assertIn("90th percentile: 20.0 min", text)
        self.assertIn("66.7%", text)
        self.assertIn("1 re-forecasts", text)


class GapsReportTests(TestCase):
    """Stream gaps per broker, and time since each broker's last delivery."""

    NOW = at("2026-11-12 18:00:00")

    @classmethod
    def setUpTestData(cls) -> None:
        """Gaps for Fink and Lasair, and deliveries at known times.

        Fink has a confirmed gap from 02:00 to 02:30 and a suspected one from
        05:00 to 06:00; Lasair a suspected gap from 03:00 to 04:00. Fink last
        delivered at 17:55 (five minutes before now) and Lasair at 15:00
        (three hours before). Babamul is enabled but never delivered.
        """
        StreamGap.objects.create(
            broker="fink",
            started_at=at("2026-11-12 05:00:00"),
            ended_at=at("2026-11-12 06:00:00"),
            kind="suspected",
        )
        StreamGap.objects.create(
            broker="fink",
            started_at=at("2026-11-12 02:00:00"),
            ended_at=at("2026-11-12 02:30:00"),
            kind="confirmed",
        )
        StreamGap.objects.create(
            broker="lasair",
            started_at=at("2026-11-12 03:00:00"),
            ended_at=at("2026-11-12 04:00:00"),
            kind="suspected",
        )
        obj = RubinObject.objects.create(dia_object_id=5005)
        first = make_detection(obj, 51, at("2026-11-12 01:00:00"), "pending")
        second = make_detection(obj, 52, at("2026-11-12 02:00:00"), "pending")
        make_delivery(first, "fink", None, at("2026-11-12 10:00:00"))
        make_delivery(second, "fink", None, at("2026-11-12 17:55:00"))
        make_delivery(first, "lasair", None, at("2026-11-12 15:00:00"))

    def setUp(self) -> None:
        """Fix the clock and the enabled brokers for each test."""
        brokers = (
            SimpleNamespace(name="fink", enabled=True),
            SimpleNamespace(name="lasair", enabled=True),
            SimpleNamespace(name="babamul", enabled=True),
        )
        for target, value in (
            (f"{COMMAND}.timezone.now", self.NOW),
            (f"{COMMAND}.load_broker_settings", brokers),
        ):
            patcher = mock.patch(target, return_value=value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_lists_gaps_and_time_since_last_delivery(self) -> None:
        """Each broker's gaps in start order, with kind, bounds, and silence."""
        result = report_json("gaps")

        brokers = {b["broker"]: b for b in result["brokers"]}
        self.assertEqual(sorted(brokers), ["babamul", "fink", "lasair"])
        fink = brokers["fink"]
        self.assertEqual(fink["last_delivery_received_at"], "2026-11-12T17:55:00+00:00")
        self.assertEqual(fink["seconds_since_last_delivery"], 300.0)
        self.assertEqual(
            fink["gaps"],
            [
                {
                    "kind": "confirmed",
                    "started_at": "2026-11-12T02:00:00+00:00",
                    "ended_at": "2026-11-12T02:30:00+00:00",
                    "duration_seconds": 1800.0,
                },
                {
                    "kind": "suspected",
                    "started_at": "2026-11-12T05:00:00+00:00",
                    "ended_at": "2026-11-12T06:00:00+00:00",
                    "duration_seconds": 3600.0,
                },
            ],
        )
        self.assertEqual(brokers["lasair"]["seconds_since_last_delivery"], 10800.0)
        self.assertEqual(len(brokers["lasair"]["gaps"]), 1)
        self.assertIsNone(brokers["babamul"]["last_delivery_received_at"])
        self.assertIsNone(brokers["babamul"]["seconds_since_last_delivery"])
        self.assertEqual(brokers["babamul"]["gaps"], [])

    def test_broker_option_narrows_the_listing(self) -> None:
        """Naming Lasair lists Lasair alone."""
        result = report_json("gaps", "--broker", "lasair")

        self.assertEqual([b["broker"] for b in result["brokers"]], ["lasair"])

    def test_plain_text_shows_silence_and_never(self) -> None:
        """Plain text gives each broker's silence, and 'never' for Babamul."""
        text = report("gaps")

        self.assertIn("never", text)
        self.assertIn("5m", text)
        self.assertIn("3h00m", text)
        self.assertIn("suspected", text)
        self.assertIn("confirmed", text)


class SkipsReportTests(TestCase):
    """Intake skips per broker, night, and reason."""

    @classmethod
    def setUpTestData(cls) -> None:
        """Four skips around the noon boundary between two nights.

        Fink skipped two alerts for ``missing_field`` at 11:59 and 03:00 UTC
        on 12 November (night of 11 November) and one for ``light_curve_span``
        at 12:01 (night of 12 November). Lasair skipped one ``parse_error``
        at 13:00 on 12 November.
        """
        for broker, reason, occurred, ident in (
            ("fink", "missing_field", "2026-11-12 11:59:00", "a1"),
            ("fink", "missing_field", "2026-11-12 03:00:00", "a2"),
            ("fink", "light_curve_span", "2026-11-12 12:01:00", "a3"),
            ("lasair", "parse_error", "2026-11-12 13:00:00", ""),
        ):
            IntakeSkip.objects.create(
                broker=broker,
                reason=reason,
                occurred_at=at(occurred),
                raw_identifier=ident,
            )

    def test_counts_per_night_broker_and_reason(self) -> None:
        """Three groups: two Fink skips in the earlier night, two in the later."""
        result = report_json("skips")

        self.assertEqual(
            result["counts"],
            [
                {
                    "night": "2026-11-11",
                    "broker": "fink",
                    "reason": "missing_field",
                    "count": 2,
                },
                {
                    "night": "2026-11-12",
                    "broker": "fink",
                    "reason": "light_curve_span",
                    "count": 1,
                },
                {
                    "night": "2026-11-12",
                    "broker": "lasair",
                    "reason": "parse_error",
                    "count": 1,
                },
            ],
        )
        self.assertNotIn("skips", result)

    def test_list_option_lists_each_skip(self) -> None:
        """With ``--list`` and Fink named, Fink's three skips are listed in time order."""
        result = report_json("skips", "--list", "--broker", "fink")

        self.assertEqual(
            [s["raw_identifier"] for s in result["skips"]], ["a2", "a1", "a3"]
        )
        self.assertEqual(len(result["counts"]), 2)


class IntakeLagReportTests(TestCase):
    """Publication-to-commit lag per broker per publication night."""

    @classmethod
    def setUpTestData(cls) -> None:
        """Counts for two brokers on the night of 12 November and one more night.

        Fink: two alerts at 4 s and one at 70 s on 12 November, and one at
        5 s on 13 November. Lasair on 12 November: one at 30 s, one in the
        bucket for an hour or more, and two without a publication time.
        """
        n12, n13 = date(2026, 11, 12), date(2026, 11, 13)
        for broker, night, lag, count in (
            ("fink", n12, 4, 2),
            ("fink", n12, 70, 1),
            ("fink", n13, 5, 1),
            ("lasair", n12, 30, 1),
            ("lasair", n12, 3600, 1),
            ("lasair", n12, None, 2),
        ):
            IntakeLagCount.objects.create(
                broker=broker, night=night, lag_seconds=lag, count=count
            )

    def test_figures_per_broker_per_night(self) -> None:
        """Fink on 12 November: 3 alerts; nearest-rank median is the 2nd
        value (4 s), the 90th percentile the 3rd (70 s), maximum 70 s, all
        within 10 minutes. Lasair: 2 measured alerts (30 s and over an hour)
        and 2 unknown; median the 1st (30 s), 90th percentile and maximum in
        the overflow bucket, half within 10 minutes."""
        result = report_json("intake-lag")

        self.assertEqual(result["overflow_seconds"], 3600)
        self.assertEqual(
            result["rows"],
            [
                {
                    "night": "2026-11-12",
                    "broker": "fink",
                    "count": 3,
                    "median_seconds": 4,
                    "p90_seconds": 70,
                    "max_seconds": 70,
                    "over_one_hour": 0,
                    "within_10_minutes": 3,
                    "share_within_10_minutes": 1.0,
                    "unknown_publication": 0,
                },
                {
                    "night": "2026-11-12",
                    "broker": "lasair",
                    "count": 2,
                    "median_seconds": 30,
                    "p90_seconds": 3600,
                    "max_seconds": 3600,
                    "over_one_hour": 1,
                    "within_10_minutes": 1,
                    "share_within_10_minutes": 0.5,
                    "unknown_publication": 2,
                },
                {
                    "night": "2026-11-13",
                    "broker": "fink",
                    "count": 1,
                    "median_seconds": 5,
                    "p90_seconds": 5,
                    "max_seconds": 5,
                    "over_one_hour": 0,
                    "within_10_minutes": 1,
                    "share_within_10_minutes": 1.0,
                    "unknown_publication": 0,
                },
            ],
        )

    def test_night_range_selects_nights(self) -> None:
        """From 13 November on, only Fink's night of 13 November is left; up
        to 12 November, only the two rows of 12 November."""
        later = report_json("intake-lag", "--from-night", "2026-11-13")
        earlier = report_json("intake-lag", "--to-night", "2026-11-12")

        self.assertEqual(
            [(r["night"], r["broker"]) for r in later["rows"]],
            [("2026-11-13", "fink")],
        )
        self.assertEqual(
            [(r["night"], r["broker"]) for r in earlier["rows"]],
            [("2026-11-12", "fink"), ("2026-11-12", "lasair")],
        )

    def test_plain_text_gives_the_same_figures_as_json(self) -> None:
        """Each text row names its night and broker and shows the JSON
        figures: seconds, with a statistic in the overflow bucket shown as
        more than one hour, and the share as a percentage."""
        result = report_json("intake-lag")
        lines = report("intake-lag").splitlines()

        def text(seconds: int) -> str:
            return ">1h" if seconds == result["overflow_seconds"] else f"{seconds}s"

        for row in result["rows"]:
            with self.subTest(night=row["night"], broker=row["broker"]):
                expected = [
                    row["night"],
                    row["broker"],
                    str(row["count"]),
                    text(row["median_seconds"]),
                    text(row["p90_seconds"]),
                    text(row["max_seconds"]),
                    f"{row['share_within_10_minutes'] * 100:.1f}%",
                    str(row["unknown_publication"]),
                ]
                self.assertIn(expected, [line.split() for line in lines])
        lasair = next(
            line.split() for line in lines if line.startswith("2026-11-12  lasair")
        )
        self.assertEqual(lasair[5], ">1h")

    def test_help_states_nights_are_publication_nights(self) -> None:
        """The subcommand's help says nights are grouped by publication night."""
        out = StringIO()
        with redirect_stdout(out), self.assertRaises(SystemExit):
            call_command("alert_report", "intake-lag", "--help")
        self.assertIn("publication night", " ".join(out.getvalue().split()))


class ReadOnlyTests(TestCase):
    """No report changes anything stored."""

    @classmethod
    def setUpTestData(cls) -> None:
        """One object with a product, a refusal, a failure, a gap, a skip,
        and a lag count."""
        obj = RubinObject.objects.create(dia_object_id=6006)
        event = {"dia_source_id": 61, "broker": "fink"}
        first = make_detection(obj, 61, at("2026-11-12 01:00:00"), "owed", event)
        second = make_detection(obj, 62, at("2026-11-12 02:00:00"), "owed", event)
        obj.qualifying_detection = first
        obj.qualifying_broker = "fink"
        obj.save()
        for detection in (first, second):
            make_delivery(
                detection, "fink", at("2026-11-12 03:00:00"), at("2026-11-12 03:01:00")
            )
        deployment = make_deployment("1.0.0")
        make_outcome(first, deployment, CLASSES, "product", at("2026-11-12 03:05:00"))
        make_outcome(second, deployment, CLASSES, "refusal", at("2026-11-12 03:06:00"))
        make_outcome(second, deployment, FLUX, "failure", at("2026-11-12 03:07:00"))
        StreamGap.objects.create(
            broker="fink",
            started_at=at("2026-11-12 04:00:00"),
            ended_at=at("2026-11-12 05:00:00"),
            kind="suspected",
        )
        IntakeSkip.objects.create(
            broker="fink", reason="missing_field", occurred_at=at("2026-11-12 06:00:00")
        )
        IntakeLagCount.objects.create(
            broker="fink", night=date(2026, 11, 12), lag_seconds=60, count=2
        )

    def test_every_subcommand_leaves_the_database_unchanged(self) -> None:
        """Row counts and contents of every table match before and after."""
        invocations = [
            ("object", "6006"),
            ("coverage",),
            ("latency",),
            ("gaps",),
            ("skips", "--list"),
            ("intake-lag",),
        ]
        for args in invocations:
            for output in ((), ("--json",)):
                with self.subTest(args=args + output):
                    before = database_fingerprint()
                    report(*args, *output)
                    self.assertEqual(database_fingerprint(), before)
