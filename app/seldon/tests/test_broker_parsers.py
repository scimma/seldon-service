"""Tests for the per-broker parsers that turn alerts into deliveries.

Every expected value is read straight from the recorded alerts in
``seldon.tests.fixtures.broker_alerts`` (or computed from them here), never
from the parser under test. No network is used: decoding tests encode their
own small Avro messages with ``fastavro``.
"""

import copy
import io
import json
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

import fastavro
from django.test import SimpleTestCase

from seldon.domain.intake.delivery import BrokerRole, Delivery
from seldon.infrastructure.brokers.base import IntakeSkipReport, ParseContext
from seldon.infrastructure.brokers.parsers import babamul as babamul_parser
from seldon.infrastructure.brokers.parsers import fink as fink_parser
from seldon.infrastructure.brokers.parsers import parse_alert
from seldon.tests.fixtures.broker_alerts import babamul, fink, lasair

RECEIVED = datetime(2026, 11, 13, 1, 0, tzinfo=timezone.utc)

#: Seconds TAI was ahead of UTC on the night the alerts were recorded.
TAI_MINUS_UTC_SECONDS = 37


def _context(message: dict[str, Any], broker: str, role: BrokerRole) -> ParseContext:
    """The context a recorded message arrived in.

    Args:
        message: One recorded message.
        broker: The configured broker name.
        role: The broker's configured role.

    Returns:
        The parse context, with the recorded Kafka timestamp as publish time.
    """
    published = datetime.fromtimestamp(
        message["kafka_timestamp_ms"] / 1000, tz=timezone.utc
    )
    return ParseContext(
        broker=broker,
        role=role,
        topic=message["topic"],
        published_at=published,
        received_at=RECEIVED,
    )


class FinkParserTests(SimpleTestCase):
    """Fink alerts carry the detection, its history, and SuperNNova's score."""

    def test_recorded_alerts_parse_to_deliveries(self) -> None:
        """Each recorded Fink alert gives its diaSource as the detection, its
        prvDiaSources (with identifiers) as the history, the diaSource's
        reliability, and the SuperNNova SN-vs-others score, never the -1
        early-SN Ia placeholder."""
        for message in fink.ALERTS:
            alert = message["alert"]
            source = alert["diaSource"]
            with self.subTest(dia_source_id=source["diaSourceId"]):
                delivery = parse_alert(
                    "fink", alert, _context(message, "fink", BrokerRole.QUALIFYING)
                )
                self.assertIsInstance(delivery, Delivery)
                self.assertEqual(delivery.dia_object_id, source["diaObjectId"])
                self.assertEqual(delivery.dia_source_id, source["diaSourceId"])
                self.assertEqual(
                    delivery.detection.observed_mjd, source["midpointMjdTai"]
                )
                self.assertEqual(delivery.detection.band, source["band"])
                self.assertEqual(delivery.detection.flux, source["psfFlux"])
                self.assertEqual(delivery.detection.flux_err, source["psfFluxErr"])
                self.assertEqual(delivery.reliability, source["reliability"])
                self.assertEqual(
                    delivery.sn_like_score, alert["clf"]["snnSnVsOthers_score"]
                )
                self.assertNotEqual(
                    delivery.sn_like_score, alert["clf"]["earlySNIa_score"]
                )
                self.assertEqual(len(delivery.previous), len(alert["prvDiaSources"]))
                self.assertEqual(
                    {p.dia_source_id for p in delivery.previous},
                    {p["diaSourceId"] for p in alert["prvDiaSources"]},
                )
                self.assertEqual(delivery.broker, "fink")
                self.assertIs(delivery.role, BrokerRole.QUALIFYING)
                self.assertEqual(
                    delivery.published_at,
                    datetime.fromtimestamp(
                        message["kafka_timestamp_ms"] / 1000, tz=timezone.utc
                    ),
                )
                self.assertEqual(delivery.received_at, RECEIVED)
                self.assertEqual(delivery.schema_version, alert["lsst_schema_version"])
                self.assertEqual(
                    delivery.classifier_version, alert["fink_science_version"]
                )

    def test_forced_photometry_is_left_out(self) -> None:
        """The recorded alert carries forced photometry, yet no history point
        is one of its prvDiaForcedSources fluxes."""
        message = fink.ALERTS[0]
        alert = message["alert"]
        self.assertTrue(alert["prvDiaForcedSources"])
        delivery = parse_alert(
            "fink", alert, _context(message, "fink", BrokerRole.QUALIFYING)
        )
        forced = {
            (p["midpointMjdTai"], p["psfFlux"]) for p in alert["prvDiaForcedSources"]
        }
        for point in delivery.previous:
            self.assertNotIn((point.observed_mjd, point.flux), forced)

    def test_missing_sn_like_score_is_an_intake_skip(self) -> None:
        """A Fink alert whose clf section lacks snnSnVsOthers_score is skipped,
        and the skip names that field and carries the diaSourceId."""
        message = copy.deepcopy(fink.ALERTS[0])
        del message["alert"]["clf"]["snnSnVsOthers_score"]
        outcome = parse_alert(
            "fink", message["alert"], _context(message, "fink", BrokerRole.QUALIFYING)
        )
        self.assertIsInstance(outcome, IntakeSkipReport)
        self.assertIn("snnSnVsOthers_score", outcome.reason)
        self.assertLessEqual(len(outcome.reason), 64)
        self.assertEqual(outcome.broker, "fink")
        self.assertEqual(
            outcome.raw_identifier,
            str(message["alert"]["diaSource"]["diaSourceId"]),
        )
        self.assertEqual(outcome.occurred_at, RECEIVED)

    def test_uncomputed_score_is_not_a_probability(self) -> None:
        """A SuperNNova score of -1 (Fink's placeholder for not computed) is
        delivered as no score at all, not as a probability."""
        message = copy.deepcopy(fink.ALERTS[0])
        message["alert"]["clf"]["snnSnVsOthers_score"] = -1.0
        delivery = parse_alert(
            "fink", message["alert"], _context(message, "fink", BrokerRole.QUALIFYING)
        )
        self.assertIsInstance(delivery, Delivery)
        self.assertIsNone(delivery.sn_like_score)

    def test_message_decodes_with_the_schema_in_its_key(self) -> None:
        """A Fink message whose key is the writer schema as JSON and whose
        value is schemaless Avro decodes to the record that was written."""
        schema = {
            "type": "record",
            "name": "alert",
            "fields": [
                {"name": "diaSourceId", "type": "long"},
                {"name": "lsst_schema_version", "type": "string"},
            ],
        }
        record = {
            "diaSourceId": 171202760056766576,
            "lsst_schema_version": "lsst.v11_2",
        }
        buffer = io.BytesIO()
        fastavro.schemaless_writer(buffer, fastavro.parse_schema(schema), record)
        decoded = fink_parser.decode(json.dumps(schema).encode(), buffer.getvalue())
        self.assertEqual(decoded, [record])


class LasairParserTests(SimpleTestCase):
    """Lasair filter messages: latest diaSource, Sherlock class as SN signal."""

    def test_recorded_alerts_parse_to_deliveries(self) -> None:
        """Each recorded Lasair message gives the latest entry of
        diaSourcesList as the detection, the other entries as history, that
        entry's reliability, and an SN-like score of 1.0 for class SN."""
        for message in lasair.ALERTS:
            body = message["alert"]
            sources = body["alert"]["diaSourcesList"]
            latest = max(sources, key=lambda s: s["midpointMjdTai"])
            with self.subTest(dia_source_id=latest["diaSourceId"]):
                self.assertEqual(body["classification"], "SN")
                delivery = parse_alert(
                    "lasair", body, _context(message, "lasair", BrokerRole.QUALIFYING)
                )
                self.assertIsInstance(delivery, Delivery)
                self.assertEqual(delivery.dia_object_id, body["diaObjectId"])
                self.assertEqual(delivery.dia_source_id, latest["diaSourceId"])
                self.assertEqual(
                    delivery.detection.observed_mjd, latest["midpointMjdTai"]
                )
                self.assertEqual(delivery.detection.flux, latest["psfFlux"])
                self.assertEqual(delivery.detection.flux_err, latest["psfFluxErr"])
                self.assertEqual(delivery.detection.band, latest["band"])
                self.assertEqual(delivery.reliability, latest["reliability"])
                self.assertEqual(delivery.sn_like_score, 1.0)
                self.assertEqual(len(delivery.previous), len(sources) - 1)
                self.assertEqual(
                    {p.dia_source_id for p in delivery.previous},
                    {s["diaSourceId"] for s in sources} - {latest["diaSourceId"]},
                )

    def test_class_decides_the_sn_like_score(self) -> None:
        """Sherlock class NT scores 1.0 like SN; any other class, such as VS
        (variable star), scores 0.0."""
        for classification, expected in (("NT", 1.0), ("VS", 0.0), ("AGN", 0.0)):
            message = copy.deepcopy(lasair.ALERTS[0])
            message["alert"]["classification"] = classification
            with self.subTest(classification=classification):
                delivery = parse_alert(
                    "lasair",
                    message["alert"],
                    _context(message, "lasair", BrokerRole.QUALIFYING),
                )
                self.assertEqual(delivery.sn_like_score, expected)

    def test_missing_class_is_an_intake_skip(self) -> None:
        """A Lasair message without its classification column is skipped, and
        the skip names the field."""
        message = copy.deepcopy(lasair.ALERTS[0])
        del message["alert"]["classification"]
        outcome = parse_alert(
            "lasair",
            message["alert"],
            _context(message, "lasair", BrokerRole.QUALIFYING),
        )
        self.assertIsInstance(outcome, IntakeSkipReport)
        self.assertIn("classification", outcome.reason)


class BabamulParserTests(SimpleTestCase):
    """Babamul: the full stream, with unidentified UTC-Julian-date history."""

    def test_recorded_alerts_parse_to_deliveries(self) -> None:
        """Each recorded Babamul alert gives its candidate as the detection
        with its reliability, no SN-like score, and every prv_candidates
        entry as history with no identifier and its UTC Julian date turned
        into MJD TAI (37 s ahead of UTC)."""
        for message in babamul.ALERTS:
            alert = message["alert"]
            candidate = alert["candidate"]
            with self.subTest(dia_source_id=candidate["diaSourceId"]):
                delivery = parse_alert(
                    "babamul",
                    alert,
                    _context(message, "babamul", BrokerRole.FULL_STREAM),
                )
                self.assertIsInstance(delivery, Delivery)
                self.assertEqual(delivery.dia_object_id, candidate["diaObjectId"])
                self.assertEqual(delivery.dia_source_id, candidate["diaSourceId"])
                self.assertEqual(
                    delivery.detection.observed_mjd, candidate["midpointMjdTai"]
                )
                self.assertEqual(delivery.detection.flux, candidate["psfFlux"])
                self.assertEqual(delivery.reliability, candidate["reliability"])
                self.assertIsNone(delivery.sn_like_score)
                self.assertIs(delivery.role, BrokerRole.FULL_STREAM)
                self.assertEqual(len(delivery.previous), len(alert["prv_candidates"]))
                for point, sent in zip(
                    sorted(delivery.previous, key=lambda p: p.observed_mjd),
                    sorted(alert["prv_candidates"], key=lambda p: p["jd"]),
                ):
                    self.assertIsNone(point.dia_source_id)
                    self.assertAlmostEqual(
                        point.observed_mjd,
                        sent["jd"] - 2400000.5 + TAI_MINUS_UTC_SECONDS / 86400,
                        places=9,
                    )
                    self.assertEqual(point.flux, sent["psfFlux"])
                    self.assertEqual(point.band, sent["band"])

    def test_converted_history_lands_on_rubin_time(self) -> None:
        """The first recorded alert's history repeats its own candidate as a
        UTC Julian date; converted, it lands within a tenth of a second of
        the candidate's midpointMjdTai, which it would miss by 37 s without
        the leap-second offset."""
        alert = babamul.ALERTS[0]["alert"]
        delivery = parse_alert(
            "babamul",
            alert,
            _context(babamul.ALERTS[0], "babamul", BrokerRole.FULL_STREAM),
        )
        mjd = alert["candidate"]["midpointMjdTai"]
        nearest = min(delivery.previous, key=lambda p: abs(p.observed_mjd - mjd))
        self.assertLess(abs(nearest.observed_mjd - mjd) * 86400, 0.1)

    def test_missing_required_field_is_an_intake_skip(self) -> None:
        """A Babamul alert whose candidate lacks its reliability, or its
        diaSourceId, is skipped with a reason naming that field."""
        for field in ("reliability", "diaSourceId"):
            message = copy.deepcopy(babamul.ALERTS[0])
            del message["alert"]["candidate"][field]
            with self.subTest(field=field):
                outcome = parse_alert(
                    "babamul",
                    message["alert"],
                    _context(message, "babamul", BrokerRole.FULL_STREAM),
                )
                self.assertIsInstance(outcome, IntakeSkipReport)
                self.assertIn(f"candidate.{field}", outcome.reason)

    def test_message_decodes_as_an_avro_container(self) -> None:
        """A Babamul message, one Avro object container file, decodes to the
        record it holds."""
        schema = {
            "type": "record",
            "name": "alert",
            "fields": [{"name": "objectId", "type": "string"}],
        }
        record = {"objectId": "171202759669317685"}
        buffer = io.BytesIO()
        fastavro.writer(buffer, fastavro.parse_schema(schema), [record])
        self.assertEqual(babamul_parser.decode(None, buffer.getvalue()), [record])


class MalformedValueTests(SimpleTestCase):
    """A value no stored record can hold becomes an intake skip, never an
    exception that stops the consumer."""

    def _lasair_outcome(self, change: Callable[[dict[str, Any]], None]) -> Any:
        """Parse the first recorded Lasair message after altering it.

        Args:
            change: Edits the decoded alert in place.

        Returns:
            What parsing produced.
        """
        message = copy.deepcopy(lasair.ALERTS[0])
        change(message["alert"])
        return parse_alert(
            "lasair",
            message["alert"],
            _context(message, "lasair", BrokerRole.QUALIFYING),
        )

    def test_infinite_identifier_from_json_is_a_skip(self) -> None:
        """Lasair sends JSON, where ``Infinity`` and ``1e400`` decode to an
        infinite float; an infinite diaObjectId is skipped."""
        outcome = self._lasair_outcome(
            lambda alert: alert.update(diaObjectId=json.loads("1e400"))
        )

        self.assertIsInstance(outcome, IntakeSkipReport)

    def test_infinite_observation_time_is_a_skip(self) -> None:
        """An infinite midpoint time on a detection is skipped."""

        def infinite_time(alert: dict[str, Any]) -> None:
            for source in alert["alert"]["diaSourcesList"]:
                source["midpointMjdTai"] = float("inf")

        self.assertIsInstance(self._lasair_outcome(infinite_time), IntakeSkipReport)

    def test_light_curve_of_the_wrong_shape_is_a_skip(self) -> None:
        """A diaSourcesList sent as an object rather than a list is skipped."""
        outcome = self._lasair_outcome(
            lambda alert: alert["alert"].update(diaSourcesList={"0": {}})
        )

        self.assertIsInstance(outcome, IntakeSkipReport)

    def test_identifier_beyond_64_bits_is_a_skip(self) -> None:
        """JSON integers are unbounded; a diaObjectId past 2**63 - 1 cannot be
        stored and is skipped."""
        outcome = self._lasair_outcome(lambda alert: alert.update(diaObjectId=2**63))

        self.assertIsInstance(outcome, IntakeSkipReport)

    def test_band_name_longer_than_stored_is_a_skip(self) -> None:
        """A band name longer than the stored eight characters is skipped."""

        def long_band(alert: dict[str, Any]) -> None:
            for source in alert["alert"]["diaSourcesList"]:
                source["band"] = "verylongband"

        self.assertIsInstance(self._lasair_outcome(long_band), IntakeSkipReport)

    def test_overlong_schema_version_is_shortened_to_fit(self) -> None:
        """Fink's schema version is kept, cut to the 64 characters stored."""
        message = copy.deepcopy(fink.ALERTS[0])
        message["alert"]["lsst_schema_version"] = "v" * 100

        outcome = parse_alert(
            "fink",
            message["alert"],
            _context(message, "fink", BrokerRole.QUALIFYING),
        )

        self.assertIsInstance(outcome, Delivery)
        self.assertEqual(outcome.schema_version, "v" * 64)


class DroppedHistoryPointTests(SimpleTestCase):
    """An unusable earlier point is left out of the light curve, visibly."""

    def test_unusable_history_point_is_dropped_and_warned_about(self) -> None:
        """A Fink alert whose first previous detection has no flux is still a
        delivery, one point shorter, and the drop is logged as a warning so a
        broker that stops sending a field shows up in the logs."""
        message = copy.deepcopy(fink.ALERTS[0])
        history = message["alert"]["prvDiaSources"]
        del history[0]["psfFlux"]

        with self.assertLogs("seldon.brokers.parsers", "WARNING") as logs:
            outcome = parse_alert(
                "fink",
                message["alert"],
                _context(message, "fink", BrokerRole.QUALIFYING),
            )

        self.assertIsInstance(outcome, Delivery)
        self.assertEqual(len(outcome.previous), len(history) - 1)
        self.assertIn("psfFlux", logs.output[0])
