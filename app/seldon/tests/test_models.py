"""Tests for the stored records: outcome identity, round trips, refusals.

These need the compose ``db`` service; run them with
``run/seldonctl slim_dev test``.
"""

from datetime import UTC, date, datetime, timedelta

from django.db import IntegrityError, transaction
from django.test import TestCase

from seldon.models import (
    ConsumerLastCommit,
    Detection,
    HeadRefusal,
    HeadRevision,
    IntakeLagCount,
    LightCurvePoint,
    ModelDeployment,
    OutcomeClaim,
    Product,
    RubinObject,
)

# Values chosen so that any lossy float or ordering conversion shows: no short
# decimal, a negative flux, a tiny and a large magnitude, and repeated bands.
LIGHT_CURVE = {
    "object_id": "170032915138363401",
    "points": [
        {"time": 61000.123456789012, "flux": 1234.5678901234567, "band": "g"},
        {"time": 61001.5, "flux": -12.25, "band": "r"},
        {"time": 61003.000000000001, "flux": 0.1 + 0.2, "band": "g"},
    ],
}
PAYLOAD = {
    "times": [61000.123456789012, 61001.123456789012, 61063.0],
    "bands": [["lsst", "u"], ["lsst", "g"], ["lsst", "y"]],
    "flux": [1.0 / 7.0, 98765.43210987654, -0.0001],
    "flux_err": [0.1, 0.2, 1e20],
    "zero_point": 31.4,
}
CAVEATS = [{"code": "sparse_light_curve", "detail": {"n_observations": 3}}]


class StoredRecordTestCase(TestCase):
    """One Rubin object with one detection, shared by the tests below."""

    @classmethod
    def setUpTestData(cls) -> None:
        """Create the object and its detection."""
        cls.rubin_object = RubinObject.objects.create(dia_object_id=313953456790)
        cls.detection = Detection.objects.create(
            dia_source_id=170032915138363401,
            rubin_object=cls.rubin_object,
            state=Detection.State.OWED,
            observed_mjd=61003.000000000001,
        )


def claim_for(detection: Detection, head: str = "flux_forecast") -> OutcomeClaim:
    """Store a deployment, a head revision, and a claim for a detection.

    Args:
        detection: The detection.
        head: The head's name.

    Returns:
        The claim.
    """
    deployment = ModelDeployment.objects.create(
        package_name="seldon-reference-model",
        package_version="2.0.0",
        contract_version="1.0",
        artifact_digest="sha256:0123",
        declaration={},
    )
    revision = HeadRevision.objects.create(
        deployment=deployment,
        head=head,
        enabled=True,
        effective_at=datetime(2026, 11, 16, tzinfo=UTC),
    )
    return OutcomeClaim.objects.create(
        detection=detection, deployment=deployment, head=head, revision=revision
    )


class ProductRoundTripTests(StoredRecordTestCase):
    """A stored product reads back exactly what was sent and returned."""

    def test_packet_payload_and_caveats_round_trip_exactly(self) -> None:
        """Arrays, band lists, and maps come back value for value."""
        created = Product.objects.create(
            claim=claim_for(self.detection),
            output_kind="flux_series",
            payload=PAYLOAD,
            caveats=CAVEATS,
            light_curve=LIGHT_CURVE,
            app_version="v0.0.0-test",
        )

        stored = Product.objects.get(pk=created.pk)

        self.assertEqual(stored.light_curve, LIGHT_CURVE)
        self.assertEqual(stored.payload, PAYLOAD)
        self.assertEqual(stored.caveats, CAVEATS)
        self.assertIsNotNone(stored.stored_at)


class HeadRefusalTests(StoredRecordTestCase):
    """A head's refusal is stored with its code, message, and detail."""

    def test_refusal_keeps_a_code_outside_the_vocabulary_verbatim(self) -> None:
        """A code the contract does not define is kept as given."""
        created = HeadRefusal.objects.create(
            claim=claim_for(self.detection),
            code="solar_conjunction",
            message="Too close to the Sun.",
            detail={"bands": [["lsst", "w"]]},
            app_version="v0.0.0-test",
        )

        stored = HeadRefusal.objects.get(pk=created.pk)

        self.assertEqual(stored.code, "solar_conjunction")
        self.assertEqual(stored.message, "Too close to the Sun.")
        self.assertEqual(stored.detail, {"bands": [["lsst", "w"]]})


class OutcomeClaimIdentityTests(StoredRecordTestCase):
    """A detection has at most one outcome per deployment and head."""

    def test_second_claim_for_the_same_head_is_refused(self) -> None:
        """The database rejects a second claim for the detection, deployment,
        and head, but keeps a claim for another head alongside."""
        deployment = ModelDeployment.objects.create(
            package_name="seldon-reference-model",
            package_version="2.0.0",
            contract_version="1.0",
            artifact_digest="sha256:0123",
            declaration={},
        )
        revisions = {
            head: HeadRevision.objects.create(
                deployment=deployment,
                head=head,
                enabled=True,
                effective_at=datetime(2026, 11, 16, tzinfo=UTC),
            )
            for head in ("flux_forecast", "classification")
        }

        def claim(head: str) -> OutcomeClaim:
            return OutcomeClaim.objects.create(
                detection=self.detection,
                deployment=deployment,
                head=head,
                revision=revisions[head],
            )

        claim("flux_forecast")
        with self.assertRaises(IntegrityError), transaction.atomic():
            claim("flux_forecast")
        claim("classification")

        self.assertEqual(
            OutcomeClaim.objects.filter(detection=self.detection).count(), 2
        )


class LightCurvePointIdentityTests(TestCase):
    """A measurement is stored once, with or without its ``diaSourceId``.

    Some brokers send previous detections without their ``diaSourceId`` and
    with times converted from UTC Julian dates, so the same measurement can
    arrive with a time that differs from Rubin's by float rounding.
    """

    def test_point_without_source_id_is_stored(self) -> None:
        """A measurement a broker sent without its identifier still saves."""
        point = LightCurvePoint.objects.create(
            dia_object_id=313953456790,
            dia_source_id=None,
            observed_mjd=61357.0157874068,
            band="i",
            flux=1520.5,
            flux_err=40.25,
        )

        self.assertIsNone(LightCurvePoint.objects.get(pk=point.pk).dia_source_id)

    def test_same_measurement_at_rounded_time_is_one_point(self) -> None:
        """Object, band, and time to a tenth of a second identify a point."""
        LightCurvePoint.objects.create(
            dia_object_id=313953456790,
            dia_source_id=170032915138363401,
            observed_mjd=61357.0157874068,
            band="i",
            flux=1520.5,
            flux_err=40.25,
        )
        from_utc_julian_date = 2461357.51535916608 - 2400000.5 + 37.0 / 86400.0

        with self.assertRaises(IntegrityError), transaction.atomic():
            LightCurvePoint.objects.create(
                dia_object_id=313953456790,
                dia_source_id=None,
                observed_mjd=from_utc_julian_date,
                band="i",
                flux=1520.5,
                flux_err=40.25,
            )

    def test_other_band_at_same_time_is_another_point(self) -> None:
        """The same object and time in another band is a separate point."""
        for band in ("i", "z"):
            LightCurvePoint.objects.create(
                dia_object_id=313953456790,
                dia_source_id=None,
                observed_mjd=61357.0157874068,
                band=band,
                flux=1520.5,
                flux_err=40.25,
            )

        self.assertEqual(LightCurvePoint.objects.count(), 2)


class IntakeLagCountIdentityTests(TestCase):
    """One lag count per broker, night, and lag, the unknown lag included."""

    def test_second_row_for_the_same_bucket_is_refused(self) -> None:
        """The database rejects a second row for Fink's 4 s bucket of the
        night of 12 November, but keeps Lasair's alongside."""
        night = date(2026, 11, 12)
        IntakeLagCount.objects.create(broker="fink", night=night, lag_seconds=4)

        with self.assertRaises(IntegrityError), transaction.atomic():
            IntakeLagCount.objects.create(broker="fink", night=night, lag_seconds=4)
        IntakeLagCount.objects.create(broker="lasair", night=night, lag_seconds=4)

        self.assertEqual(IntakeLagCount.objects.count(), 2)

    def test_second_row_for_the_unknown_publication_bucket_is_refused(
        self,
    ) -> None:
        """The unknown-publication bucket (no lag) is one row per broker and
        night too: a second is rejected."""
        night = date(2026, 11, 12)
        IntakeLagCount.objects.create(broker="fink", night=night, lag_seconds=None)

        with self.assertRaises(IntegrityError), transaction.atomic():
            IntakeLagCount.objects.create(broker="fink", night=night, lag_seconds=None)

        self.assertEqual(IntakeLagCount.objects.count(), 1)


class ConsumerLastCommitTests(TestCase):
    """One last-commit time per broker and consumer instance."""

    def test_record_updates_the_instance_row_and_latest_spans_instances(
        self,
    ) -> None:
        """Instance a records Babamul at 01:00 and again at 01:10; instance b
        records it at 01:05. Instance a keeps one row, now 01:10, and
        Babamul's latest commit across instances is 01:10. Lasair, which no
        instance committed, has none."""
        one = datetime(2026, 11, 13, 1, 0, tzinfo=UTC)
        ConsumerLastCommit.record("babamul", "host-a-1", one)
        ConsumerLastCommit.record("babamul", "host-b-2", one + timedelta(minutes=5))
        ConsumerLastCommit.record("babamul", "host-a-1", one + timedelta(minutes=10))

        self.assertEqual(
            ConsumerLastCommit.objects.get(instance="host-a-1").committed_at,
            one + timedelta(minutes=10),
        )
        self.assertEqual(ConsumerLastCommit.objects.count(), 2)
        self.assertEqual(
            ConsumerLastCommit.latest(["babamul", "lasair"]),
            {"babamul": one + timedelta(minutes=10)},
        )

    def test_second_row_for_the_same_instance_is_refused(self) -> None:
        """The database rejects a second row for one broker and instance."""
        moment = datetime(2026, 11, 13, 1, 0, tzinfo=UTC)
        ConsumerLastCommit.objects.create(
            broker="fink", instance="host-a-1", committed_at=moment
        )

        with self.assertRaises(IntegrityError), transaction.atomic():
            ConsumerLastCommit.objects.create(
                broker="fink", instance="host-a-1", committed_at=moment
            )
