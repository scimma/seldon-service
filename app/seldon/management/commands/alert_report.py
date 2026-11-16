"""Read-only reports on alert intake: history, coverage, latency, gaps, lag.

Each subcommand reads the stored records and prints plain text, or JSON with
``--json``. None of them writes anything.

Outcomes belong to a model deployment and one of its heads, so coverage and
latency are reported per deployment and head: two releases running side by
side over the same detections are counted apart, and a failure (the service
giving up) is counted in its own column, never as a product or a refusal.

Nights follow Rubin's dayObs, as the SCiMMA crossmatch service does: a night
runs from 12:00 UTC to 12:00 UTC and is named by the date at its start.
A detection's night comes from its observation time, an MJD in TAI, turned
into UTC by subtracting the 37-second TAI-UTC offset in force since 2017.
"""

import json
import logging
import math
from argparse import ArgumentParser, ArgumentTypeError
from bisect import bisect_right
from collections import Counter, defaultdict
from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta
from typing import Any

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Max
from django.utils import timezone
from pydantic import ValidationError
from seldon_model_contract import OutputKind

from seldon.config.brokers import load_broker_settings
from seldon.config.intake import get_intake_settings
from seldon.domain.intake.time_scales import TAI_MINUS_UTC_SECONDS
from seldon.models import (
    NIGHT_START,
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
    night_of,
)

logger = logging.getLogger("seldon.reports")

#: MJD zero, as a UTC time.
MJD_EPOCH = datetime(1858, 11, 17, tzinfo=UTC)

TAI_MINUS_UTC = timedelta(seconds=TAI_MINUS_UTC_SECONDS)

#: Detection states counted in coverage: owed, or qualified too late.
COVERED_STATES = (Detection.State.OWED, Detection.State.MISSED)

#: Claim states that settle an outcome, with the name each is counted under.
SETTLED = {
    OutcomeClaim.State.PRODUCT: "products",
    OutcomeClaim.State.REFUSAL: "refusals",
    OutcomeClaim.State.FAILURE: "failures",
}

COVERAGE_HELP = (
    "Coverage per deployment head, per broker, per night. For each night: "
    "the owed and missed detections each broker delivered (pending "
    "detections are left out). Then for each head of each deployment that is "
    "not retired: the owed detections due an outcome from it (it has a claim "
    "for them, or its head revision in effect when they became owed is "
    "enabled), counted by how they ended (product, refusal, failure, or still "
    "due) and the share that ended in a product or a refusal; failures are "
    "not covered. A detection belongs to the night of its observation time "
    "(MJD TAI, less 37 s, in UTC); a night runs from 12:00 UTC to 12:00 UTC "
    "and is named by its start date, so a detection at 11:59 UTC is in the "
    "earlier night. A detection several brokers delivered counts once for "
    "each; the 'all' row counts it once."
)

LATENCY_HELP = (
    "Latency from the broker publishing the triggering delivery to a product "
    "being stored, per deployment head. The triggering delivery is the "
    "qualifying broker's delivery when the detection was the object's "
    "qualifying event when it became owed, otherwise the detection's "
    "earliest-received delivery. Left out and counted apart: replayed "
    "deliveries (received more than the silence period after publication), "
    "deliveries without a publish time, and re-forecasts (the same deployment "
    "had already stored an outcome for the detection before this product was "
    "owed, as when a head is enabled later with an earlier start; another "
    "deployment's earlier outcome does not count). The percentile is by "
    "nearest rank."
)

#: Lag within which an alert counts as taken in on time, in seconds.
INTAKE_LAG_TARGET_SECONDS = 600

INTAKE_LAG_HELP = (
    "Lag from a broker publishing an alert to the consumer committing its "
    "work, per broker per night, from the intake-lag record (kept after the "
    "pending sweep deletes deliveries). Nights are grouped by publication "
    "night (12:00 to 12:00 UTC, named by its start date); an alert without a "
    "publication time is counted apart, under the night it was received. "
    "For each broker and night: alerts measured, median, 90th percentile "
    "(nearest rank) and maximum lag to one second, and the share within 10 "
    "minutes. Lags of an hour or more share one bucket, shown as more than "
    "one hour (>1h)."
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


def settled_outcome(claim: OutcomeClaim) -> Product | HeadRefusal | ProductFailure:
    """Return the product, refusal, or failure that settled a claim.

    Args:
        claim: A claim no longer due.

    Returns:
        Its outcome.
    """
    if claim.state == OutcomeClaim.State.PRODUCT:
        return claim.product
    if claim.state == OutcomeClaim.State.REFUSAL:
        return claim.refusal
    return claim.failure


def predicted_class(product: Product) -> str | None:
    """Name the most probable class of a class-probabilities product.

    Args:
        product: The product.

    Returns:
        The class with the highest probability, or ``None`` for a product of
        another kind or with no probabilities.
    """
    if product.output_kind != OutputKind.CLASS_PROBABILITIES:
        return None
    probabilities = (product.payload or {}).get("probabilities") or {}
    return max(probabilities, key=probabilities.get) if probabilities else None


class Command(BaseCommand):
    """Print read-only reports on alert intake."""

    help = (
        "Read-only reports on alert intake: one object's outcomes, coverage "
        "per deployment head per broker per night, publish-to-stored latency "
        "per deployment head, stream gaps, and intake skips. Nights run 12:00 to 12:00 UTC, named by their start "
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
            help="Every product, refusal and failure of one Rubin object, by "
            "deployment and head, in observation order.",
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
            help="Only products stored at or after this ISO 8601 time "
            "(UTC if no offset).",
        )
        latency.add_argument(
            "--until",
            type=parse_moment,
            help="Only products stored before this ISO 8601 time.",
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

        intake_lag = reports.add_parser(
            "intake-lag", help=INTAKE_LAG_HELP, description=INTAKE_LAG_HELP
        )
        self._add_night_range(intake_lag)

        for subparser in (obj, coverage, latency, gaps, skips, intake_lag):
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
            "intake-lag": (self._intake_lag_report, self._print_intake_lag),
        }
        build, render = builders[options["report"]]
        result = build(options)
        if options["json"]:
            self.stdout.write(json.dumps(result, indent=2))
        else:
            render(result)

    def _object_report(self, options: dict[str, Any]) -> dict[str, Any]:
        """Collect one object's outcomes in observation order.

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

        claims: dict[int, list[OutcomeClaim]] = defaultdict(list)
        for claim in (
            OutcomeClaim.objects.filter(detection__in=detections, state__in=SETTLED)
            .select_related("deployment", "product", "refusal", "failure")
            .order_by("pk")
        ):
            claims[claim.detection_id].append(claim)

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
            settled = sorted(
                claims[detection.pk], key=lambda c: (settled_outcome(c).stored_at, c.pk)
            )
            if not settled:
                unresolved.append({**common, "state": detection.state})
            event = (detection.qualification or {}).get("qualifying_event") or None
            for claim in settled:
                records.append(
                    {
                        **common,
                        **self._outcome_fields(claim),
                        "qualifying_event": event,
                    }
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
    def _outcome_fields(claim: OutcomeClaim) -> dict[str, Any]:
        """Describe one settled outcome.

        Args:
            claim: The claim, with its deployment and outcome loaded.

        Returns:
            Its kind, deployment, head, stored time, release, and, for a
            product, its output kind, caveat codes and predicted class; for a
            refusal, its code and message; for a failure, its reason.
        """
        outcome = settled_outcome(claim)
        fields: dict[str, Any] = {
            "kind": claim.state,
            "package_name": claim.deployment.package_name,
            "package_version": claim.deployment.package_version,
            "head": claim.head,
            "stored_at": iso(outcome.stored_at),
            "app_version": outcome.app_version,
            "output_kind": None,
            "caveats": [],
            "predicted_class": None,
            "code": None,
            "message": None,
            "reason": None,
        }
        if isinstance(outcome, Product):
            fields["output_kind"] = outcome.output_kind
            fields["caveats"] = [caveat.get("code") for caveat in outcome.caveats]
            fields["predicted_class"] = predicted_class(outcome)
        elif isinstance(outcome, HeadRefusal):
            fields["code"] = outcome.code
            fields["message"] = outcome.message
        else:
            fields["reason"] = outcome.reason
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
            self.stdout.write("  no products, refusals or failures")
        for record in result["records"]:
            self.stdout.write(
                f"  diaSource {record['dia_source_id']}  "
                f"observed {record['observed_utc']} (night {record['night']})  "
                f"{record['kind']}  {record['package_name']} "
                f"{record['package_version']} {record['head']}"
            )
            if record["kind"] == OutcomeClaim.State.PRODUCT:
                caveats = ", ".join(record["caveats"]) or "none"
                line = f"      {record['output_kind']}, caveats {caveats}"
                if record["predicted_class"] is not None:
                    line += f", class {record['predicted_class']}"
                self.stdout.write(line)
            elif record["kind"] == OutcomeClaim.State.REFUSAL:
                self.stdout.write(f"      {record['code']}: {record['message']}")
            else:
                self.stdout.write(f"      failed: {record['reason']}")
            event = record["qualifying_event"] or {}
            self.stdout.write(
                f"      qualifying event diaSource "
                f"{event.get('dia_source_id')} by {event.get('broker')}"
            )
            self.stdout.write(
                f"      delivered by {', '.join(record['brokers']) or '-'}; "
                f"stored {record['stored_at']}"
            )
        for detection in result["unresolved_detections"]:
            self.stdout.write(
                f"  diaSource {detection['dia_source_id']}  observed "
                f"{detection['observed_utc']} (night {detection['night']})  "
                f"{detection['state']}, no outcome yet; delivered by "
                f"{', '.join(detection['brokers']) or '-'}"
            )

    def _coverage_report(self, options: dict[str, Any]) -> dict[str, Any]:
        """Count owed detections by outcome, per night, deployment head, broker.

        Args:
            options: The parsed options.

        Returns:
            The report.
        """
        detections = Detection.objects.filter(state__in=COVERED_STATES)
        if options["from_night"] is not None:
            detections = detections.filter(
                observed_mjd__gte=utc_to_mjd(night_start(options["from_night"]))
            )
        if options["to_night"] is not None:
            end = night_start(options["to_night"] + timedelta(days=1))
            detections = detections.filter(observed_mjd__lt=utc_to_mjd(end))

        found: dict[int, tuple[date, str, datetime | None]] = {}
        brokers_of: dict[int, list[str]] = defaultdict(list)
        rows = Delivery.objects.filter(detection__in=detections).values_list(
            "detection_id",
            "detection__observed_mjd",
            "detection__state",
            "detection__owed_at",
            "broker",
        )
        for detection_id, observed_mjd, state, owed_at, broker in rows:
            found.setdefault(
                detection_id, (night_of(mjd_to_utc(observed_mjd)), state, owed_at)
            )
            brokers_of[detection_id].append(broker)

        lifecycle: dict[date, dict[str, Counter]] = defaultdict(
            lambda: defaultdict(Counter)
        )
        lifecycle_all: dict[date, Counter] = defaultdict(Counter)
        for detection_id, (night, state, _) in found.items():
            for broker in brokers_of[detection_id]:
                lifecycle[night][broker][state] += 1
            lifecycle_all[night][state] += 1

        outcomes = self._outcomes_by_head(detections, found)
        live = {
            deployment.pk: deployment
            for deployment in ModelDeployment.objects.filter(retired_at__isnull=True)
        }
        per_head: dict[date, dict[tuple[int, str], dict[str, Counter]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(Counter))
        )
        per_head_all: dict[date, dict[tuple[int, str], Counter]] = defaultdict(
            lambda: defaultdict(Counter)
        )
        for key, by_detection in outcomes.items():
            for detection_id, kind in by_detection.items():
                night = found[detection_id][0]
                for broker in brokers_of[detection_id]:
                    per_head[night][key][broker][kind] += 1
                per_head_all[night][key][kind] += 1

        def order(key: tuple[int, str]) -> tuple[str, str, str]:
            deployment = live[key[0]]
            return deployment.package_name, deployment.package_version, key[1]

        nights = []
        for night in sorted(lifecycle_all):
            heads = []
            for key in sorted(per_head_all[night], key=order):
                deployment = live[key[0]]
                heads.append(
                    {
                        "package_name": deployment.package_name,
                        "package_version": deployment.package_version,
                        "head": key[1],
                        "brokers": {
                            broker: self._coverage_counts(counts)
                            for broker, counts in sorted(per_head[night][key].items())
                        },
                        "all": self._coverage_counts(per_head_all[night][key]),
                    }
                )
            nights.append(
                {
                    "night": night.isoformat(),
                    "detections": {
                        "brokers": {
                            broker: self._lifecycle_counts(counts)
                            for broker, counts in sorted(lifecycle[night].items())
                        },
                        "all": self._lifecycle_counts(lifecycle_all[night]),
                    },
                    "heads": heads,
                }
            )
        return {
            "definition": COVERAGE_HELP,
            "tai_minus_utc_seconds": TAI_MINUS_UTC.total_seconds(),
            "nights": nights,
        }

    @staticmethod
    def _outcomes_by_head(
        detections: Any, found: dict[int, tuple[date, str, datetime | None]]
    ) -> dict[tuple[int, str], dict[int, str]]:
        """Say how each owed detection stands with each live deployment head.

        A detection is due a head's outcome when the deployment has a claim
        for it, or when the head's revision in effect when the detection
        became owed is enabled and no worker has claimed it yet.

        Args:
            detections: The detections reported on, as a query.
            found: The delivered ones, as night, state and owed time.

        Returns:
            For each (deployment id, head), each due detection's id to
            ``products``, ``refusals``, ``failures``, or ``due``.
        """
        outcomes: dict[tuple[int, str], dict[int, str]] = defaultdict(dict)
        claims = OutcomeClaim.objects.filter(
            deployment__retired_at__isnull=True, detection__in=detections
        ).values_list("deployment_id", "head", "detection_id", "state")
        for deployment_id, head, detection_id, state in claims:
            if detection_id in found:
                outcomes[(deployment_id, head)][detection_id] = SETTLED.get(
                    state, "due"
                )

        timelines: dict[tuple[int, str], tuple[list[datetime], list[bool]]] = (
            defaultdict(lambda: ([], []))
        )
        revisions = (
            HeadRevision.objects.filter(deployment__retired_at__isnull=True)
            .order_by("deployment_id", "head", "effective_at", "pk")
            .values_list("deployment_id", "head", "effective_at", "enabled")
        )
        for deployment_id, head, effective_at, enabled in revisions:
            times, states = timelines[(deployment_id, head)]
            times.append(effective_at)
            states.append(enabled)
        for key, (times, states) in timelines.items():
            claimed = outcomes[key]
            for detection_id, (_, state, owed_at) in found.items():
                if (
                    state != Detection.State.OWED
                    or owed_at is None
                    or detection_id in claimed
                ):
                    continue
                in_effect = bisect_right(times, owed_at) - 1
                if in_effect >= 0 and states[in_effect]:
                    claimed[detection_id] = "due"
        return {key: value for key, value in outcomes.items() if value}

    @staticmethod
    def _lifecycle_counts(counts: Counter) -> dict[str, int]:
        """Count one night's owed and missed detections for a broker or all.

        Args:
            counts: Detections by state.

        Returns:
            The owed and missed counts.
        """
        return {
            "owed": counts[Detection.State.OWED],
            "missed": counts[Detection.State.MISSED],
        }

    @staticmethod
    def _coverage_counts(counts: Counter) -> dict[str, Any]:
        """Summarize one night's due detections for one head and broker or all.

        Args:
            counts: Detections by outcome.

        Returns:
            The total, each outcome's count, and the share that ended in a
            product or a refusal.
        """
        total = sum(counts.values())
        covered = counts["products"] + counts["refusals"]
        return {
            "detections": total,
            "products": counts["products"],
            "refusals": counts["refusals"],
            "failures": counts["failures"],
            "due": counts["due"],
            "share": covered / total if total else None,
        }

    def _print_coverage(self, result: dict[str, Any]) -> None:
        """Print a coverage report as two tables.

        Args:
            result: The report.
        """
        self.stdout.write(f"{'night':<10}  {'broker':<12} {'owed':>6} {'missed':>6}")
        if not result["nights"]:
            self.stdout.write("(no owed detections)")
        for night in result["nights"]:
            detections = night["detections"]
            rows = list(detections["brokers"].items()) + [("all", detections["all"])]
            for broker, counts in rows:
                self.stdout.write(
                    f"{night['night']:<10}  {broker:<12} {counts['owed']:>6} "
                    f"{counts['missed']:>6}"
                )
        self.stdout.write("")
        self.stdout.write(
            f"{'night':<10}  {'deployment':<28} {'head':<16} {'broker':<12} "
            f"{'detections':>10} {'products':>8} {'refused':>7} {'failed':>6} "
            f"{'due':>6} {'covered':>8}"
        )
        if not any(night["heads"] for night in result["nights"]):
            self.stdout.write("(no detections due to a live deployment)")
        for night in result["nights"]:
            for head in night["heads"]:
                label = f"{head['package_name']} {head['package_version']}"
                rows = list(head["brokers"].items()) + [("all", head["all"])]
                for broker, counts in rows:
                    self.stdout.write(
                        f"{night['night']:<10}  {label:<28} {head['head']:<16} "
                        f"{broker:<12} {counts['detections']:>10} "
                        f"{counts['products']:>8} {counts['refusals']:>7} "
                        f"{counts['failures']:>6} {counts['due']:>6} "
                        f"{share_text(counts['share']):>8}"
                    )
        self.stdout.write(
            "owed/missed: delivered detections owed, or qualified too late; "
            "detections: owed detections due to the deployment head; due: no "
            "outcome yet; covered: share ending in a product or a refusal "
            "(failures are not covered); all: each detection once; retired "
            "deployments are left out."
        )

    def _latency_report(self, options: dict[str, Any]) -> dict[str, Any]:
        """Measure publish-to-stored latency of products outside replays.

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

        products = Product.objects.select_related(
            "claim__detection", "claim__deployment"
        ).only(
            "stored_at",
            "claim__head",
            "claim__created_at",
            "claim__detection__dia_source_id",
            "claim__detection__qualification",
            "claim__deployment__package_name",
            "claim__deployment__package_version",
        )
        if options["since"] is not None:
            products = products.filter(stored_at__gte=options["since"])
        if options["until"] is not None:
            products = products.filter(stored_at__lt=options["until"])
        products = list(products.order_by("stored_at", "pk"))
        detection_ids = {product.claim.detection_id for product in products}

        deliveries: dict[int, list[Delivery]] = defaultdict(list)
        for delivery in Delivery.objects.filter(
            detection_id__in=detection_ids
        ).order_by("received_at", "pk"):
            deliveries[delivery.detection_id].append(delivery)
        earlier_outcomes: dict[tuple[int, int], list[datetime]] = defaultdict(list)
        for model in (Product, HeadRefusal, ProductFailure):
            for detection_id, deployment_id, stored_at in model.objects.filter(
                claim__detection_id__in=detection_ids
            ).values_list("claim__detection_id", "claim__deployment_id", "stored_at"):
                earlier_outcomes[(detection_id, deployment_id)].append(stored_at)

        groups: dict[tuple[int, str], dict[str, Any]] = {}
        for product in products:
            claim = product.claim
            group = groups.setdefault(
                (claim.deployment_id, claim.head),
                {
                    "deployment": claim.deployment,
                    "products": 0,
                    "excluded": Counter(
                        {
                            "replayed": 0,
                            "no_published_at": 0,
                            "reforecast": 0,
                            "no_delivery": 0,
                        }
                    ),
                    "latencies": [],
                    "by_broker": defaultdict(list),
                },
            )
            group["products"] += 1
            excluded = group["excluded"]
            if any(
                stored < claim.created_at
                for stored in earlier_outcomes[
                    (claim.detection_id, claim.deployment_id)
                ]
            ):
                excluded["reforecast"] += 1
                continue
            trigger = self._triggering_delivery(
                claim.detection, deliveries[claim.detection_id]
            )
            if trigger is None:
                excluded["no_delivery"] += 1
            elif trigger.published_at is None:
                excluded["no_published_at"] += 1
            elif trigger.received_at - trigger.published_at > silence:
                excluded["replayed"] += 1
            else:
                minutes = (product.stored_at - trigger.published_at) / timedelta(
                    minutes=1
                )
                group["latencies"].append(minutes)
                group["by_broker"][trigger.broker].append(minutes)

        heads = []
        for (_, head), group in sorted(
            groups.items(),
            key=lambda item: (
                item[1]["deployment"].package_name,
                item[1]["deployment"].package_version,
                item[0][1],
            ),
        ):
            heads.append(
                {
                    "package_name": group["deployment"].package_name,
                    "package_version": group["deployment"].package_version,
                    "head": head,
                    "products": group["products"],
                    "excluded": dict(group["excluded"]),
                    **self._latency_summary(group["latencies"], percentile, threshold),
                    "by_broker": {
                        broker: self._latency_summary(values, percentile, threshold)
                        for broker, values in sorted(group["by_broker"].items())
                    },
                }
            )
        return {
            "definition": LATENCY_HELP,
            "percentile": percentile,
            "threshold_minutes": options["threshold_minutes"],
            "silence_minutes": silence / timedelta(minutes=1),
            "since": iso(options["since"]),
            "until": iso(options["until"]),
            "heads": heads,
        }

    @staticmethod
    def _triggering_delivery(
        detection: Detection, deliveries: list[Delivery]
    ) -> Delivery | None:
        """Find the delivery that triggered a detection's outcomes.

        Args:
            detection: The detection.
            deliveries: Its deliveries, earliest received first.

        Returns:
            The qualifying broker's delivery when the detection was the
            object's qualifying event when it became owed, otherwise the
            earliest-received delivery; ``None`` if there is none.
        """
        event = (detection.qualification or {}).get("qualifying_event") or {}
        if event.get("dia_source_id") == detection.dia_source_id:
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
        """Print a latency report as text, one block per deployment head.

        Args:
            result: The report.
        """
        rank = f"{result['percentile']:g}th percentile"
        window = ""
        if result["since"] or result["until"]:
            window = (
                f" stored from {result['since'] or 'the start'} to "
                f"{result['until'] or 'now'}"
            )
        self.stdout.write(f"Products{window}, per deployment head:")
        if not result["heads"]:
            self.stdout.write("  (no products)")
        for head in result["heads"]:
            excluded = head["excluded"]
            self.stdout.write(
                f"{head['package_name']} {head['package_version']} "
                f"{head['head']}: {head['products']} products"
            )
            self.stdout.write(f"  measured: {head['measured']}")
            self.stdout.write(
                f"  left out: {excluded['replayed']} replayed (received more "
                f"than {result['silence_minutes']:g} min after publication), "
                f"{excluded['no_published_at']} without a publish time, "
                f"{excluded['reforecast']} re-forecasts, "
                f"{excluded['no_delivery']} without a delivery"
            )
            self.stdout.write(
                "  " + self._latency_line(rank, head, result["threshold_minutes"])
            )
            for broker, summary in head["by_broker"].items():
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

    def _intake_lag_report(self, options: dict[str, Any]) -> dict[str, Any]:
        """Summarize publication-to-commit lag per publication night and broker.

        Args:
            options: The parsed options.

        Returns:
            The report.
        """
        rows = IntakeLagCount.objects.all()
        if options["from_night"] is not None:
            rows = rows.filter(night__gte=options["from_night"])
        if options["to_night"] is not None:
            rows = rows.filter(night__lte=options["to_night"])

        histograms: dict[tuple[date, str], Counter] = {}
        unknown: Counter = Counter()
        for night, broker, lag, count in rows.values_list(
            "night", "broker", "lag_seconds", "count"
        ):
            histogram = histograms.setdefault((night, broker), Counter())
            if lag is None:
                unknown[(night, broker)] += count
            else:
                histogram[lag] += count

        result_rows = []
        for night, broker in sorted(histograms):
            histogram = histograms[(night, broker)]
            measured = sum(histogram.values())
            within = sum(
                count
                for lag, count in histogram.items()
                if lag <= INTAKE_LAG_TARGET_SECONDS
            )
            result_rows.append(
                {
                    "night": night.isoformat(),
                    "broker": broker,
                    "count": measured,
                    "median_seconds": histogram_percentile(histogram, 50),
                    "p90_seconds": histogram_percentile(histogram, 90),
                    "max_seconds": max(histogram) if histogram else None,
                    "over_one_hour": histogram[IntakeLagCount.OVERFLOW_SECONDS],
                    "within_10_minutes": within,
                    "share_within_10_minutes": within / measured if measured else None,
                    "unknown_publication": unknown[(night, broker)],
                }
            )
        return {
            "definition": INTAKE_LAG_HELP,
            "overflow_seconds": IntakeLagCount.OVERFLOW_SECONDS,
            "rows": result_rows,
        }

    def _print_intake_lag(self, result: dict[str, Any]) -> None:
        """Print an intake-lag report as a table.

        Args:
            result: The report.
        """
        overflow = result["overflow_seconds"]

        def lag_text(seconds: int | None) -> str:
            if seconds is None:
                return "-"
            return ">1h" if seconds >= overflow else f"{seconds}s"

        self.stdout.write(
            f"{'night':<10}  {'broker':<12} {'alerts':>8} {'median':>7} "
            f"{'p90':>7} {'max':>7} {'<=10min':>8} {'unknown':>8}"
        )
        if not result["rows"]:
            self.stdout.write("(no intake-lag counts)")
        for row in result["rows"]:
            self.stdout.write(
                f"{row['night']:<10}  {row['broker']:<12} {row['count']:>8} "
                f"{lag_text(row['median_seconds']):>7} "
                f"{lag_text(row['p90_seconds']):>7} "
                f"{lag_text(row['max_seconds']):>7} "
                f"{share_text(row['share_within_10_minutes']):>8} "
                f"{row['unknown_publication']:>8}"
            )
        self.stdout.write(
            "night: publication night; alerts: alerts with a publication time; "
            "lags from publication to commit, >1h meaning an hour or more; "
            "unknown: alerts without a publication time, by receipt night."
        )


def histogram_percentile(histogram: Counter, percentile: float) -> int | None:
    """Take a percentile by nearest rank from counts per value.

    Gives the same value as ``percentile_value`` over the values the
    histogram counts, without expanding them.

    Args:
        histogram: Value to how many times it occurs.
        percentile: The percentile, above 0 and at most 100.

    Returns:
        The smallest value with at least ``percentile`` percent of the
        counts at or below it, or ``None`` when nothing is counted.
    """
    total = sum(histogram.values())
    if not total:
        return None
    rank = max(1, math.ceil(percentile / 100 * total))
    seen = 0
    for value in sorted(histogram):
        seen += histogram[value]
        if seen >= rank:
            return value
    return None


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
