"""Read-only reports on alert intake: object history, coverage, latency, gaps.

Each subcommand reads the stored records and prints plain text, or JSON with
``--json``. None of them writes anything.

Nights follow Rubin's dayObs, as the SCiMMA crossmatch service does: a night
runs from 12:00 UTC to 12:00 UTC and is named by the date at its start.
A detection's night comes from its observation time, an MJD in TAI, turned
into UTC by subtracting the 37-second TAI-UTC offset in force since 2017.
"""

import json
import logging
import math
from argparse import ArgumentParser, ArgumentTypeError
from collections import Counter, defaultdict
from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta
from typing import Any

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Max
from django.utils import timezone
from pydantic import ValidationError

from seldon.config.brokers import load_broker_settings
from seldon.config.intake import get_intake_settings
from seldon.domain.intake.time_scales import TAI_MINUS_UTC_SECONDS
from seldon.models import (
    Delivery,
    Detection,
    Forecast,
    IntakeSkip,
    Refusal,
    RubinObject,
    StreamGap,
)

logger = logging.getLogger("seldon.reports")

#: MJD zero, as a UTC time.
MJD_EPOCH = datetime(1858, 11, 17, tzinfo=UTC)

TAI_MINUS_UTC = timedelta(seconds=TAI_MINUS_UTC_SECONDS)

#: Hours after midnight UTC that a night starts (and the previous one ends).
NIGHT_START = timedelta(hours=12)

#: Detection states that mean a detection was owed a forecast.
OWED_STATES = (
    Detection.State.FORECAST,
    Detection.State.REFUSED,
    Detection.State.MISSED,
    Detection.State.OWED,
)

COVERAGE_HELP = (
    "Coverage per broker per night. For each night and each broker: the "
    "detections that broker delivered and that were owed a forecast (state "
    "forecast, refused, missed, or still owed; pending detections are left "
    "out), counted by how they ended, and the share that ended in a stored "
    "forecast or a recorded refusal. A detection belongs to the night of its "
    "observation time (MJD TAI, less 37 s, in UTC); a night runs from 12:00 "
    "UTC to 12:00 UTC and is named by its start date, so a detection at "
    "11:59 UTC is in the earlier night. A detection several brokers "
    "delivered counts once for each; the 'all' row counts it once."
)

LATENCY_HELP = (
    "Latency from the broker publishing the triggering delivery to the "
    "forecast being stored. The triggering delivery is the qualifying "
    "broker's delivery when the forecast detection is the object's "
    "qualifying event, otherwise the detection's earliest-received delivery. "
    "Left out and counted apart: replayed deliveries (received more than the "
    "silence period after publication), deliveries without a publish time, "
    "and re-forecasts (a detection forecast or refused earlier under another "
    "checkpoint, whose triggering redelivery is not stored). The percentile "
    "is by nearest rank."
)


def mjd_to_utc(mjd_tai: float) -> datetime:
    """Turn an MJD on the TAI scale into a UTC time.

    Args:
        mjd_tai: The MJD, in TAI.

    Returns:
        The aware UTC datetime.
    """
    return MJD_EPOCH + timedelta(days=mjd_tai) - TAI_MINUS_UTC


def utc_to_mjd(utc: datetime) -> float:
    """Turn a UTC time into an MJD on the TAI scale.

    Args:
        utc: The aware time.

    Returns:
        The MJD, in TAI.
    """
    return (utc + TAI_MINUS_UTC - MJD_EPOCH) / timedelta(days=1)


def night_of(moment: datetime) -> date:
    """Name the night a moment falls in.

    Args:
        moment: An aware time.

    Returns:
        The date at the start of the 12:00-to-12:00 UTC night holding it.
    """
    return (moment.astimezone(UTC) - NIGHT_START).date()


def night_start(night: date) -> datetime:
    """Give the moment a night starts.

    Args:
        night: The night's name.

    Returns:
        12:00 UTC on that date.
    """
    return datetime(night.year, night.month, night.day, tzinfo=UTC) + NIGHT_START


def parse_night(text: str) -> date:
    """Read a night name given on the command line.

    Args:
        text: The night as ``YYYY-MM-DD``.

    Returns:
        The date.

    Raises:
        ArgumentTypeError: If the text is not a date.
    """
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise ArgumentTypeError(f"not a night (YYYY-MM-DD): {text!r}") from exc


def parse_moment(text: str) -> datetime:
    """Read a time given on the command line; a naive time is taken as UTC.

    Args:
        text: An ISO 8601 time.

    Returns:
        The aware time.

    Raises:
        ArgumentTypeError: If the text is not an ISO 8601 time.
    """
    try:
        moment = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ArgumentTypeError(f"not an ISO 8601 time: {text!r}") from exc
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return moment


def percentile_value(values: list[float], percentile: float) -> float | None:
    """Take a percentile by nearest rank.

    Args:
        values: The values, in any order.
        percentile: The percentile, above 0 and at most 100.

    Returns:
        The smallest value with at least ``percentile`` percent of the values
        at or below it, or ``None`` when there are no values.
    """
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, math.ceil(percentile / 100 * len(ordered)))
    return ordered[rank - 1]


def iso(moment: datetime | None) -> str | None:
    """Write a time as ISO 8601 in UTC.

    Args:
        moment: The time, or ``None``.

    Returns:
        The text, or ``None``.
    """
    return None if moment is None else moment.astimezone(UTC).isoformat()


def share_text(share: float | None) -> str:
    """Write a share as a percentage.

    Args:
        share: A fraction, or ``None`` when nothing was counted.

    Returns:
        The percentage to one decimal, or ``-``.
    """
    return "-" if share is None else f"{share * 100:.1f}%"


def duration_text(seconds: float | None) -> str:
    """Write a duration compactly.

    Args:
        seconds: The duration, or ``None`` for something that never happened.

    Returns:
        ``never``, ``<n>s``, ``<n>m``, ``<h>h<mm>m``, or ``<d>d<hh>h``.
    """
    if seconds is None:
        return "never"
    whole = int(round(seconds))
    if abs(whole) < 60:
        return f"{whole}s"
    minutes = whole // 60
    if minutes < 60:
        return f"{minutes}m"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h{minutes:02d}m"
    days, hours = divmod(hours, 24)
    return f"{days}d{hours:02d}h"


class Command(BaseCommand):
    """Print read-only reports on alert intake."""

    help = (
        "Read-only reports on alert intake: one object's forecasts, coverage "
        "per broker per night, publish-to-stored latency, stream gaps, and "
        "intake skips. Nights run 12:00 to 12:00 UTC, named by their start "
        "date. Add --json to any report for JSON output."
    )

    def add_arguments(self, parser: ArgumentParser) -> None:
        """Declare the subcommands and their options.

        Args:
            parser: The command's argument parser.
        """
        reports = parser.add_subparsers(dest="report", required=True)

        obj = reports.add_parser(
            "object",
            help="Every forecast and refusal of one Rubin object, in "
            "observation order.",
        )
        obj.add_argument("dia_object_id", type=int, help="Rubin diaObjectId.")

        coverage = reports.add_parser(
            "coverage", help=COVERAGE_HELP, description=COVERAGE_HELP
        )
        self._add_night_range(coverage)

        latency = reports.add_parser(
            "latency", help=LATENCY_HELP, description=LATENCY_HELP
        )
        latency.add_argument(
            "--percentile",
            type=float,
            default=90.0,
            help="Percentile to report, above 0 and at most 100 (default 90).",
        )
        latency.add_argument(
            "--threshold-minutes",
            type=float,
            default=10.0,
            help="Report the share stored within this many minutes (default 10).",
        )
        latency.add_argument(
            "--since",
            type=parse_moment,
            help="Only forecasts stored at or after this ISO 8601 time "
            "(UTC if no offset).",
        )
        latency.add_argument(
            "--until",
            type=parse_moment,
            help="Only forecasts stored before this ISO 8601 time.",
        )

        gaps = reports.add_parser(
            "gaps",
            help="Confirmed and suspected stream gaps per broker, with the "
            "time since each broker's last new delivery.",
        )
        gaps.add_argument(
            "--broker",
            action="append",
            help="Only this broker; repeat for several.",
        )
        gaps.add_argument(
            "--since",
            type=parse_moment,
            help="Only gaps ending at or after this ISO 8601 time.",
        )

        skips = reports.add_parser(
            "skips",
            help="Intake skips counted per night, broker, and reason.",
        )
        self._add_night_range(skips)
        skips.add_argument(
            "--broker",
            action="append",
            help="Only this broker; repeat for several.",
        )
        skips.add_argument(
            "--list",
            action="store_true",
            help="Also list each skip.",
        )

        for subparser in (obj, coverage, latency, gaps, skips):
            subparser.add_argument(
                "--json", action="store_true", help="Print JSON instead of text."
            )

    @staticmethod
    def _add_night_range(parser: ArgumentParser) -> None:
        """Add the first-night and last-night options.

        Args:
            parser: A subcommand's parser.
        """
        parser.add_argument(
            "--from-night",
            type=parse_night,
            help="First night to include, YYYY-MM-DD.",
        )
        parser.add_argument(
            "--to-night",
            type=parse_night,
            help="Last night to include, YYYY-MM-DD.",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        """Build the chosen report and print it.

        Args:
            *args: Unused.
            **options: The parsed options.

        Raises:
            CommandError: If an option is out of range or the object is
                unknown.
        """
        builders = {
            "object": (self._object_report, self._print_object),
            "coverage": (self._coverage_report, self._print_coverage),
            "latency": (self._latency_report, self._print_latency),
            "gaps": (self._gaps_report, self._print_gaps),
            "skips": (self._skips_report, self._print_skips),
        }
        build, render = builders[options["report"]]
        result = build(options)
        if options["json"]:
            self.stdout.write(json.dumps(result, indent=2))
        else:
            render(result)

    def _object_report(self, options: dict[str, Any]) -> dict[str, Any]:
        """Collect one object's forecasts and refusals in observation order.

        Args:
            options: The parsed options.

        Returns:
            The report.

        Raises:
            CommandError: If the object was never received.
        """
        try:
            rubin_object = RubinObject.objects.select_related(
                "qualifying_detection"
            ).get(dia_object_id=options["dia_object_id"])
        except RubinObject.DoesNotExist as exc:
            raise CommandError(
                f"No diaObject {options['dia_object_id']} has been received."
            ) from exc

        detections = list(
            rubin_object.detections.order_by("observed_mjd", "dia_source_id")
        )
        brokers: dict[int, list[str]] = defaultdict(list)
        for detection_id, broker in Delivery.objects.filter(
            detection__in=detections
        ).values_list("detection_id", "broker"):
            brokers[detection_id].append(broker)

        forecasts = defaultdict(list)
        for forecast in Forecast.objects.filter(detection__in=detections).order_by(
            "stored_at", "pk"
        ):
            forecasts[forecast.detection_id].append(forecast)
        refusals = defaultdict(list)
        for refusal in Refusal.objects.filter(detection__in=detections).order_by(
            "stored_at", "pk"
        ):
            refusals[refusal.detection_id].append(refusal)

        records = []
        unresolved = []
        for detection in detections:
            observed = mjd_to_utc(detection.observed_mjd)
            common = {
                "dia_source_id": detection.dia_source_id,
                "observed_mjd": detection.observed_mjd,
                "observed_utc": iso(observed),
                "night": night_of(observed).isoformat(),
                "brokers": sorted(brokers[detection.pk]),
            }
            outcomes = [("forecast", f) for f in forecasts[detection.pk]] + [
                ("refusal", r) for r in refusals[detection.pk]
            ]
            outcomes.sort(key=lambda item: (item[1].stored_at, item[0]))
            if not outcomes:
                unresolved.append({**common, "state": detection.state})
            for kind, outcome in outcomes:
                records.append(
                    {**common, "kind": kind, **self._outcome_fields(kind, outcome)}
                )

        qualifying = None
        if rubin_object.qualifying_detection is not None:
            qualifying = {
                **rubin_object.qualifying_event(),
                "qualified_at": iso(rubin_object.qualified_at),
            }
        return {
            "dia_object_id": rubin_object.dia_object_id,
            "qualifying_event": qualifying,
            "records": records,
            "unresolved_detections": unresolved,
        }

    @staticmethod
    def _outcome_fields(kind: str, outcome: Forecast | Refusal) -> dict[str, Any]:
        """Describe one forecast or refusal.

        Args:
            kind: ``forecast`` or ``refusal``.
            outcome: The stored forecast or refusal.

        Returns:
            Its checkpoint, stored time, regime mark, predicted class,
            qualifying event, and, for a refusal, its code and message.
        """
        fields: dict[str, Any] = {
            "checkpoint": outcome.checkpoint,
            "stored_at": iso(outcome.stored_at),
            "app_version": outcome.app_version,
        }
        if kind == "forecast":
            fields.update(
                {
                    "in_training_regime": outcome.in_training_regime,
                    "sparse": outcome.sparse,
                    "n_observations": outcome.n_observations,
                    "untrained_bands": outcome.untrained_bands,
                    "predicted_class": (outcome.outputs or {}).get("predicted_class"),
                    "qualifying_event": outcome.qualifying_event or None,
                    "code": None,
                    "message": None,
                }
            )
        else:
            fields.update(
                {
                    "in_training_regime": None,
                    "sparse": None,
                    "n_observations": None,
                    "untrained_bands": [],
                    "predicted_class": None,
                    "qualifying_event": None,
                    "code": outcome.code,
                    "message": outcome.message,
                }
            )
        return fields

    def _print_object(self, result: dict[str, Any]) -> None:
        """Print an object report as text.

        Args:
            result: The report.
        """
        self.stdout.write(f"diaObject {result['dia_object_id']}")
        event = result["qualifying_event"]
        if event is None:
            self.stdout.write("  not qualified")
        else:
            self.stdout.write(
                f"  qualified on diaSource {event['dia_source_id']} by "
                f"{event['broker']} (reliability {event['reliability_score']} "
                f"vs cut {event['reliability_cut']}, SN-like "
                f"{event['sn_like_score']} vs threshold "
                f"{event['sn_like_threshold']}) at {event['qualified_at']}"
            )
        if not result["records"]:
            self.stdout.write("  no forecasts or refusals")
        for record in result["records"]:
            self.stdout.write(
                f"  diaSource {record['dia_source_id']}  "
                f"observed {record['observed_utc']} (night {record['night']})  "
                f"{record['kind']}  checkpoint {record['checkpoint']}"
            )
            if record["kind"] == "forecast":
                regime = (
                    "in regime"
                    if record["in_training_regime"]
                    else "out of regime"
                    + (" (sparse)" if record["sparse"] else "")
                    + (
                        f" (untrained bands {','.join(record['untrained_bands'])})"
                        if record["untrained_bands"]
                        else ""
                    )
                )
                self.stdout.write(
                    f"      {regime}, n_observations {record['n_observations']}, "
                    f"class {record['predicted_class']}"
                )
                event = record["qualifying_event"] or {}
                self.stdout.write(
                    f"      qualifying event diaSource "
                    f"{event.get('dia_source_id')} by {event.get('broker')}"
                )
            else:
                self.stdout.write(f"      {record['code']}: {record['message']}")
            self.stdout.write(
                f"      delivered by {', '.join(record['brokers']) or '-'}; "
                f"stored {record['stored_at']}"
            )
        for detection in result["unresolved_detections"]:
            self.stdout.write(
                f"  diaSource {detection['dia_source_id']}  observed "
                f"{detection['observed_utc']} (night {detection['night']})  "
                f"{detection['state']}, no forecast or refusal; delivered by "
                f"{', '.join(detection['brokers']) or '-'}"
            )

    def _coverage_report(self, options: dict[str, Any]) -> dict[str, Any]:
        """Count owed detections by how they ended, per night and broker.

        Args:
            options: The parsed options.

        Returns:
            The report.
        """
        detections = Detection.objects.filter(state__in=OWED_STATES)
        if options["from_night"] is not None:
            detections = detections.filter(
                observed_mjd__gte=utc_to_mjd(night_start(options["from_night"]))
            )
        if options["to_night"] is not None:
            end = night_start(options["to_night"] + timedelta(days=1))
            detections = detections.filter(observed_mjd__lt=utc_to_mjd(end))

        per_broker: dict[date, dict[str, Counter]] = defaultdict(
            lambda: defaultdict(Counter)
        )
        overall: dict[date, Counter] = defaultdict(Counter)
        seen: set[int] = set()
        rows = Delivery.objects.filter(detection__in=detections).values_list(
            "detection_id", "detection__observed_mjd", "detection__state", "broker"
        )
        for detection_id, observed_mjd, state, broker in rows:
            night = night_of(mjd_to_utc(observed_mjd))
            per_broker[night][broker][state] += 1
            if detection_id not in seen:
                seen.add(detection_id)
                overall[night][state] += 1

        nights = []
        for night in sorted(overall):
            nights.append(
                {
                    "night": night.isoformat(),
                    "brokers": {
                        broker: self._coverage_counts(counts)
                        for broker, counts in sorted(per_broker[night].items())
                    },
                    "all": self._coverage_counts(overall[night]),
                }
            )
        return {
            "definition": COVERAGE_HELP,
            "tai_minus_utc_seconds": TAI_MINUS_UTC.total_seconds(),
            "nights": nights,
        }

    @staticmethod
    def _coverage_counts(counts: Counter) -> dict[str, Any]:
        """Summarize one night's owed detections for one broker or all.

        Args:
            counts: Detections by state.

        Returns:
            The total, each state's count, and the share forecast or refused.
        """
        total = sum(counts.values())
        done = counts[Detection.State.FORECAST] + counts[Detection.State.REFUSED]
        return {
            "detections": total,
            "forecast": counts[Detection.State.FORECAST],
            "refused": counts[Detection.State.REFUSED],
            "missed": counts[Detection.State.MISSED],
            "owed": counts[Detection.State.OWED],
            "share": done / total if total else None,
        }

    def _print_coverage(self, result: dict[str, Any]) -> None:
        """Print a coverage report as a table.

        Args:
            result: The report.
        """
        header = (
            f"{'night':<10}  {'broker':<12} {'detections':>10} {'forecast':>8} "
            f"{'refused':>7} {'missed':>6} {'owed':>6} {'covered':>8}"
        )
        self.stdout.write(header)
        if not result["nights"]:
            self.stdout.write("(no owed detections)")
        for night in result["nights"]:
            rows = list(night["brokers"].items()) + [("all", night["all"])]
            for broker, counts in rows:
                self.stdout.write(
                    f"{night['night']:<10}  {broker:<12} {counts['detections']:>10} "
                    f"{counts['forecast']:>8} {counts['refused']:>7} "
                    f"{counts['missed']:>6} {counts['owed']:>6} "
                    f"{share_text(counts['share']):>8}"
                )
        self.stdout.write(
            "detections: delivered and owed a forecast (not pending); owed: "
            "still owed; covered: share ending in a forecast or a refusal; "
            "all: each detection once."
        )

    def _latency_report(self, options: dict[str, Any]) -> dict[str, Any]:
        """Measure publish-to-stored latency of forecasts outside replays.

        Args:
            options: The parsed options.

        Returns:
            The report.

        Raises:
            CommandError: If the percentile or threshold is out of range.
        """
        percentile = options["percentile"]
        if not 0 < percentile <= 100:
            raise CommandError("--percentile must be above 0 and at most 100.")
        threshold = timedelta(minutes=options["threshold_minutes"])
        if threshold <= timedelta(0):
            raise CommandError("--threshold-minutes must be positive.")
        silence = timedelta(minutes=get_intake_settings().silence_minutes)

        forecasts = Forecast.objects.select_related("detection")
        if options["since"] is not None:
            forecasts = forecasts.filter(stored_at__gte=options["since"])
        if options["until"] is not None:
            forecasts = forecasts.filter(stored_at__lt=options["until"])
        forecasts = list(forecasts.order_by("stored_at", "pk"))
        detection_ids = {forecast.detection_id for forecast in forecasts}

        deliveries: dict[int, list[Delivery]] = defaultdict(list)
        for delivery in Delivery.objects.filter(
            detection_id__in=detection_ids
        ).order_by("received_at", "pk"):
            deliveries[delivery.detection_id].append(delivery)
        earlier_outcomes: dict[int, list[datetime]] = defaultdict(list)
        for model in (Forecast, Refusal):
            for detection_id, stored_at in model.objects.filter(
                detection_id__in=detection_ids
            ).values_list("detection_id", "stored_at"):
                earlier_outcomes[detection_id].append(stored_at)

        excluded = Counter(
            {"replayed": 0, "no_published_at": 0, "reforecast": 0, "no_delivery": 0}
        )
        latencies: list[float] = []
        by_broker: dict[str, list[float]] = defaultdict(list)
        for forecast in forecasts:
            if any(
                stored < forecast.stored_at
                for stored in earlier_outcomes[forecast.detection_id]
            ):
                excluded["reforecast"] += 1
                continue
            trigger = self._triggering_delivery(
                forecast, deliveries[forecast.detection_id]
            )
            if trigger is None:
                excluded["no_delivery"] += 1
            elif trigger.published_at is None:
                excluded["no_published_at"] += 1
            elif trigger.received_at - trigger.published_at > silence:
                excluded["replayed"] += 1
            else:
                minutes = (forecast.stored_at - trigger.published_at) / timedelta(
                    minutes=1
                )
                latencies.append(minutes)
                by_broker[trigger.broker].append(minutes)

        summary = self._latency_summary(latencies, percentile, threshold)
        return {
            "definition": LATENCY_HELP,
            "percentile": percentile,
            "threshold_minutes": options["threshold_minutes"],
            "silence_minutes": silence / timedelta(minutes=1),
            "since": iso(options["since"]),
            "until": iso(options["until"]),
            "forecasts": len(forecasts),
            "excluded": dict(excluded),
            **summary,
            "by_broker": {
                broker: self._latency_summary(values, percentile, threshold)
                for broker, values in sorted(by_broker.items())
            },
        }

    @staticmethod
    def _triggering_delivery(
        forecast: Forecast, deliveries: list[Delivery]
    ) -> Delivery | None:
        """Find the delivery that triggered a forecast.

        Args:
            forecast: The forecast, with its detection loaded.
            deliveries: The detection's deliveries, earliest received first.

        Returns:
            The qualifying broker's delivery when the detection is the
            qualifying event recorded with the forecast, otherwise the
            earliest-received delivery; ``None`` if there is none.
        """
        event = forecast.qualifying_event or {}
        if event.get("dia_source_id") == forecast.detection.dia_source_id:
            for delivery in deliveries:
                if delivery.broker == event.get("broker"):
                    return delivery
        return deliveries[0] if deliveries else None

    @staticmethod
    def _latency_summary(
        latencies: list[float], percentile: float, threshold: timedelta
    ) -> dict[str, Any]:
        """Summarize a set of latencies.

        Args:
            latencies: Latencies in minutes.
            percentile: The percentile to take.
            threshold: The latency to count shares within.

        Returns:
            The count measured, the percentile in minutes, the count and
            share at or within the threshold, and the largest latency.
        """
        limit = threshold / timedelta(minutes=1)
        within = sum(1 for value in latencies if value <= limit)
        return {
            "measured": len(latencies),
            "percentile_minutes": percentile_value(latencies, percentile),
            "within_threshold": within,
            "share_within_threshold": within / len(latencies) if latencies else None,
            "max_minutes": max(latencies) if latencies else None,
        }

    def _print_latency(self, result: dict[str, Any]) -> None:
        """Print a latency report as text.

        Args:
            result: The report.
        """
        excluded = result["excluded"]
        rank = f"{result['percentile']:g}th percentile"
        window = ""
        if result["since"] or result["until"]:
            window = (
                f" stored from {result['since'] or 'the start'} to "
                f"{result['until'] or 'now'}"
            )
        self.stdout.write(f"Forecasts{window}: {result['forecasts']}")
        self.stdout.write(f"  measured: {result['measured']}")
        self.stdout.write(
            f"  left out: {excluded['replayed']} replayed (received more than "
            f"{result['silence_minutes']:g} min after publication), "
            f"{excluded['no_published_at']} without a publish time, "
            f"{excluded['reforecast']} re-forecasts, "
            f"{excluded['no_delivery']} without a delivery"
        )
        self.stdout.write(self._latency_line(rank, result, result["threshold_minutes"]))
        for broker, summary in result["by_broker"].items():
            self.stdout.write(
                f"  triggered by {broker}: {summary['measured']} measured; "
                + self._latency_line(rank, summary, result["threshold_minutes"])
            )

    @staticmethod
    def _latency_line(rank: str, summary: dict[str, Any], threshold: float) -> str:
        """Describe a latency summary in one line.

        Args:
            rank: The percentile's name.
            summary: The summary.
            threshold: The threshold, in minutes.

        Returns:
            The line.
        """
        value = summary["percentile_minutes"]
        value_text = "-" if value is None else f"{value:.1f} min"
        return (
            f"{rank}: {value_text}; within {threshold:g} min: "
            f"{summary['within_threshold']} "
            f"({share_text(summary['share_within_threshold'])})"
        )

    def _gaps_report(self, options: dict[str, Any]) -> dict[str, Any]:
        """List stream gaps and each broker's time since its last delivery.

        Args:
            options: The parsed options.

        Returns:
            The report.
        """
        now = timezone.now()
        gaps = StreamGap.objects.order_by("started_at", "pk")
        if options["since"] is not None:
            gaps = gaps.filter(ended_at__gte=options["since"])
        last_delivery = {
            broker: received
            for broker, received in Delivery.objects.values("broker")
            .order_by("broker")
            .annotate(last=Max("received_at"))
            .values_list("broker", "last")
        }
        names = (
            set(last_delivery)
            | set(gaps.values_list("broker", flat=True))
            | set(_enabled_brokers())
        )
        if options["broker"]:
            names = set(options["broker"])

        by_broker: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for gap in gaps.filter(broker__in=names):
            by_broker[gap.broker].append(
                {
                    "kind": gap.kind,
                    "started_at": iso(gap.started_at),
                    "ended_at": iso(gap.ended_at),
                    "duration_seconds": (gap.ended_at - gap.started_at).total_seconds(),
                }
            )
        brokers = []
        for name in sorted(names):
            received = last_delivery.get(name)
            brokers.append(
                {
                    "broker": name,
                    "last_delivery_received_at": iso(received),
                    "seconds_since_last_delivery": (
                        None if received is None else (now - received).total_seconds()
                    ),
                    "gaps": by_broker[name],
                }
            )
        return {"now": iso(now), "since": iso(options["since"]), "brokers": brokers}

    def _print_gaps(self, result: dict[str, Any]) -> None:
        """Print a gaps report as text.

        Args:
            result: The report.
        """
        self.stdout.write(f"As of {result['now']}")
        if not result["brokers"]:
            self.stdout.write("(no brokers have delivered or are enabled)")
        for broker in result["brokers"]:
            since = duration_text(broker["seconds_since_last_delivery"])
            last = broker["last_delivery_received_at"]
            self.stdout.write(
                f"{broker['broker']}: last new delivery "
                + (f"{since} ago ({last})" if last else since)
                + f"; {len(broker['gaps'])} gap(s)"
            )
            for gap in broker["gaps"]:
                self.stdout.write(
                    f"  {gap['kind']:<9} {gap['started_at']} to {gap['ended_at']} "
                    f"({duration_text(gap['duration_seconds'])})"
                )

    def _skips_report(self, options: dict[str, Any]) -> dict[str, Any]:
        """Count intake skips per night, broker, and reason.

        Args:
            options: The parsed options.

        Returns:
            The report, with each skip listed when ``--list`` was given.
        """
        skips = IntakeSkip.objects.order_by("occurred_at", "pk")
        if options["from_night"] is not None:
            skips = skips.filter(occurred_at__gte=night_start(options["from_night"]))
        if options["to_night"] is not None:
            end = night_start(options["to_night"] + timedelta(days=1))
            skips = skips.filter(occurred_at__lt=end)
        if options["broker"]:
            skips = skips.filter(broker__in=options["broker"])

        counts: Counter = Counter()
        listed = []
        for skip in skips:
            night = night_of(skip.occurred_at).isoformat()
            counts[(night, skip.broker, skip.reason)] += 1
            if options["list"]:
                listed.append(
                    {
                        "night": night,
                        "broker": skip.broker,
                        "reason": skip.reason,
                        "occurred_at": iso(skip.occurred_at),
                        "raw_identifier": skip.raw_identifier,
                    }
                )
        result: dict[str, Any] = {
            "counts": [
                {"night": night, "broker": broker, "reason": reason, "count": count}
                for (night, broker, reason), count in sorted(counts.items())
            ]
        }
        if options["list"]:
            result["skips"] = listed
        return result

    def _print_skips(self, result: dict[str, Any]) -> None:
        """Print a skips report as text.

        Args:
            result: The report.
        """
        self.stdout.write(f"{'night':<10}  {'broker':<12} {'reason':<24} {'count':>6}")
        if not result["counts"]:
            self.stdout.write("(no intake skips)")
        for row in result["counts"]:
            self.stdout.write(
                f"{row['night']:<10}  {row['broker']:<12} {row['reason']:<24} "
                f"{row['count']:>6}"
            )
        for skip in result.get("skips", []):
            self.stdout.write(
                f"  {skip['occurred_at']}  {skip['broker']}  {skip['reason']}  "
                f"{skip['raw_identifier'] or '(no identifier)'}"
            )


def _enabled_brokers() -> Iterable[str]:
    """Name the brokers the consumer is configured to read.

    Returns:
        Each enabled broker's name; none if the settings cannot be read,
        which is logged.
    """
    try:
        return [broker.name for broker in load_broker_settings() if broker.enabled]
    except ValidationError:
        logger.warning("Broker settings could not be read; listing seen brokers only")
        return []
