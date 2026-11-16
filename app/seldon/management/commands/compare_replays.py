"""Compare two replay databases detection by detection, without writing.

Two replays of the same retained traffic, in the same order of receipt,
each drained by the same model releases, should owe the same detections
for the same reasons and store the same outcomes from the same packets.
This command checks that. For each detection, keyed by ``diaSourceId``, it
compares its state and the qualification snapshot taken when it became
owed, and its outcomes keyed by package name, package version and head: the
outcome's kind (product, refusal, failure, or still due); for a product its
output kind, payload, caveat list and the packet it was given; for a
refusal its code, message and detail. A failure is compared by kind only,
because its reason can name the worker that held the lease. For each
object, keyed by ``diaObjectId``, it compares whether it qualified and, if
so, the qualifying detection, broker, scores, cut and threshold.

Fields that differ between runs by design are left out: when rows were
stored or became owed (``stored_at``, ``owed_at``, ``qualified_at``), the
release that stored them (``app_version``), attempts and leases, and row
and foreign-key ids. An outcome still due on one side means that side's
replay was not drained.

Both databases are on the configured database server and are opened with
the default database's credentials, as two connection aliases registered for
the run, with every transaction read-only. Rows are streamed in key order on
both sides, so tens of thousands of detections are compared without loading
either database whole.
"""

import copy
import json
import logging
from argparse import ArgumentParser
from collections.abc import Iterator
from contextlib import contextmanager
from itertools import groupby
from typing import Any

from django.core.management.base import BaseCommand, CommandError
from django.db import DEFAULT_DB_ALIAS, connections

from seldon.models import Detection, OutcomeClaim, RubinObject

logger = logging.getLogger("seldon.reports")

#: Connection aliases registered for the run, one per database.
LEFT_ALIAS = "compare_replays_left"
RIGHT_ALIAS = "compare_replays_right"
COMPARED_ALIASES = (LEFT_ALIAS, RIGHT_ALIAS)

#: Rows fetched per round trip from each server-side cursor.
CHUNK_SIZE = 2000

#: Detection fields compared exactly, besides its outcomes.
DETECTION_FIELDS = ("state", "qualification")

#: Outcome fields read, as (column, reported name), for each kind. A
#: product's ``payload`` is compared within the output tolerance; every
#: other field exactly.
OUTCOME_FIELDS = {
    "product": (
        ("product__output_kind", "output_kind"),
        ("product__payload", "payload"),
        ("product__caveats", "caveats"),
        ("product__light_curve", "light_curve"),
    ),
    "refusal": (
        ("refusal__code", "code"),
        ("refusal__message", "message"),
        ("refusal__detail", "detail"),
    ),
    "failure": (),
    "due": (),
}

#: Every outcome column read, in order.
OUTCOME_COLUMNS = tuple(
    column for fields in OUTCOME_FIELDS.values() for column, _ in fields
)

#: Outcome kinds counted as compared and matched.
OUTCOME_KINDS = ("products", "refusals", "failures")

#: Object fields compared when both sides qualified the object, with the
#: names they are reported under.
OBJECT_FIELDS = (
    ("qualifying_detection__dia_source_id", "qualifying_detection"),
    ("qualifying_broker", "qualifying_broker"),
    ("qualifying_reliability_score", "qualifying_reliability_score"),
    ("qualifying_sn_like_score", "qualifying_sn_like_score"),
    ("qualifying_reliability_cut", "qualifying_reliability_cut"),
    ("qualifying_sn_like_threshold", "qualifying_sn_like_threshold"),
)


@contextmanager
def read_only_databases(left: str, right: str) -> Iterator[None]:
    """Register the two databases as read-only connection aliases.

    Each alias copies the default database's settings, with its own database
    name and every transaction read-only. Both are closed and removed on
    exit.

    Args:
        left: The left database's name.
        right: The right database's name.

    Yields:
        Nothing; the aliases exist while the context is open.
    """
    base = connections.settings[DEFAULT_DB_ALIAS]
    for alias, name in zip(COMPARED_ALIASES, (left, right)):
        settings = copy.deepcopy(base)
        settings["NAME"] = name
        options = dict(settings.get("OPTIONS") or {})
        options["options"] = " ".join(
            filter(
                None, [options.get("options"), "-c default_transaction_read_only=on"]
            )
        )
        settings["OPTIONS"] = options
        connections.settings[alias] = settings
    try:
        yield
    finally:
        for alias in COMPARED_ALIASES:
            connections[alias].close()
            del connections[alias]
            del connections.settings[alias]


def same(left: Any, right: Any, tolerance: float) -> bool:
    """Compare two JSON values, numbers within an absolute tolerance.

    Structure, keys, strings and booleans must match exactly.

    Args:
        left: One value.
        right: The other.
        tolerance: The largest absolute difference allowed between numbers.

    Returns:
        Whether the values match.
    """
    if isinstance(left, bool) or isinstance(right, bool):
        return type(left) is type(right) and left == right
    if isinstance(left, int | float) and isinstance(right, int | float):
        return abs(left - right) <= tolerance
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(
            same(left[key], right[key], tolerance) for key in left
        )
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(
            same(a, b, tolerance) for a, b in zip(left, right)
        )
    return left == right


def outcome_groups(alias: str) -> Iterator[tuple[int, dict[tuple, dict]]]:
    """Stream each detection's outcomes, in ``diaSourceId`` order.

    Args:
        alias: The connection alias to read.

    Yields:
        ``(dia_source_id, {(package_name, package_version, head): outcome})``
        for each detection with an outcome, where the outcome holds its
        ``state`` and the fields its kind is compared on.
    """
    rows = (
        OutcomeClaim.objects.using(alias)
        .order_by(
            "detection__dia_source_id",
            "deployment__package_name",
            "deployment__package_version",
            "head",
        )
        .values_list(
            "detection__dia_source_id",
            "deployment__package_name",
            "deployment__package_version",
            "head",
            "state",
            *OUTCOME_COLUMNS,
        )
        .iterator(chunk_size=CHUNK_SIZE)
    )
    for dia_source_id, group in groupby(rows, key=lambda row: row[0]):
        outcomes = {}
        for _, name, version, head, state, *values in group:
            columns = dict(zip(OUTCOME_COLUMNS, values, strict=True))
            outcomes[(name, version, head)] = {
                "state": state,
                **{
                    field: columns[column]
                    for column, field in OUTCOME_FIELDS.get(state, ())
                },
            }
        yield dia_source_id, outcomes


def detection_records(alias: str) -> Iterator[tuple[int, dict[str, Any]]]:
    """Stream each detection with its outcomes.

    Args:
        alias: The connection alias to read.

    Yields:
        ``(dia_source_id, record)`` in ``diaSourceId`` order, where the
        record holds the detection's compared fields and its outcomes by
        package name, version and head.
    """
    detections = (
        Detection.objects.using(alias)
        .order_by("dia_source_id")
        .values_list("dia_source_id", *DETECTION_FIELDS)
        .iterator(chunk_size=CHUNK_SIZE)
    )
    outcomes = outcome_groups(alias)
    next_outcomes = next(outcomes, None)
    for dia_source_id, *values in detections:
        record: dict[str, Any] = dict(zip(DETECTION_FIELDS, values, strict=True))
        record["outcomes"] = {}
        while next_outcomes is not None and next_outcomes[0] <= dia_source_id:
            if next_outcomes[0] == dia_source_id:
                record["outcomes"] = next_outcomes[1]
            next_outcomes = next(outcomes, None)
        yield dia_source_id, record


def object_records(alias: str) -> Iterator[tuple[int, dict[str, Any]]]:
    """Stream each object's qualifying state.

    Args:
        alias: The connection alias to read.

    Yields:
        ``(dia_object_id, {reported field: value})`` in ``diaObjectId`` order.
    """
    columns = [column for column, _ in OBJECT_FIELDS]
    names = [name for _, name in OBJECT_FIELDS]
    rows = (
        RubinObject.objects.using(alias)
        .order_by("dia_object_id")
        .values_list("dia_object_id", *columns)
        .iterator(chunk_size=CHUNK_SIZE)
    )
    for dia_object_id, *values in rows:
        yield dia_object_id, dict(zip(names, values, strict=True))


def merge_join(
    left: Iterator[tuple[int, Any]], right: Iterator[tuple[int, Any]]
) -> Iterator[tuple[int, Any, Any]]:
    """Pair two key-ordered streams by key.

    Args:
        left: ``(key, value)`` pairs in ascending key order.
        right: The same, from the other database.

    Yields:
        ``(key, left value or None, right value or None)`` in key order.
    """
    a = next(left, None)
    b = next(right, None)
    while a is not None or b is not None:
        if b is None or (a is not None and a[0] < b[0]):
            yield a[0], a[1], None
            a = next(left, None)
        elif a is None or b[0] < a[0]:
            yield b[0], None, b[1]
            b = next(right, None)
        else:
            yield a[0], a[1], b[1]
            a = next(left, None)
            b = next(right, None)


def difference(
    key: str, key_value: int, field: str, left: Any, right: Any, **extra: Any
) -> dict[str, Any]:
    """Describe one difference.

    Values are included only when they are scalars, so packets and
    payloads do not flood the report.

    Args:
        key: ``dia_source_id`` or ``dia_object_id``.
        key_value: The detection or object identifier.
        field: The field that differs.
        left: Its left value.
        right: Its right value.
        **extra: Further identifying entries, such as the package and head.

    Returns:
        The difference as a JSON-ready mapping.
    """
    entry: dict[str, Any] = {key: key_value, **extra, "field": field}
    if not isinstance(left, dict | list) and not isinstance(right, dict | list):
        entry["left"] = left
        entry["right"] = right
    return entry


def count_differences(result: dict[str, Any]) -> int:
    """Count every difference and every key found on one side only.

    Args:
        result: The comparison.

    Returns:
        The total; zero when the databases match.
    """
    return sum(
        len(result[key])
        for key in (
            "differences",
            "only_in_left",
            "only_in_right",
            "object_differences",
            "objects_only_in_left",
            "objects_only_in_right",
        )
    )


class Command(BaseCommand):
    """Compare two replay databases without writing to either."""

    help = (
        "Compare two drained replay databases on the configured server, "
        "detection by detection: state and qualification snapshot, and each "
        "outcome keyed by package, version and head (its kind; a product's "
        "payload, caveats and packet; a refusal's code, message and detail), "
        "and each object's qualifying state. Stored times, release versions "
        "and row ids are ignored. Prints matched counts and every difference, "
        "and exits non-zero on any difference. Opens both databases read-only."
    )

    def add_arguments(self, parser: ArgumentParser) -> None:
        """Declare the options.

        Args:
            parser: The command's argument parser.
        """
        parser.add_argument("--left", required=True, help="First database name.")
        parser.add_argument("--right", required=True, help="Second database name.")
        parser.add_argument(
            "--output-tolerance",
            type=float,
            default=0.0,
            help="Compare numbers in product payloads within this absolute "
            "tolerance (default 0, exact). Packets, caveats and every other "
            "field are always compared exactly.",
        )
        parser.add_argument(
            "--json", action="store_true", help="Print JSON instead of text."
        )

    def handle(self, *args: Any, **options: Any) -> None:
        """Compare the two databases and print the result.

        Args:
            *args: Unused.
            **options: The parsed options.

        Raises:
            CommandError: If the databases differ in any compared way.
        """
        logger.info(
            "Comparing replay databases %s and %s", options["left"], options["right"]
        )
        with read_only_databases(options["left"], options["right"]):
            result = self._compare(options["output_tolerance"])
        result["left"] = options["left"]
        result["right"] = options["right"]
        result["output_tolerance"] = options["output_tolerance"]
        if options["json"]:
            self.stdout.write(json.dumps(result, indent=2))
        else:
            self._print(result)
        total = count_differences(result)
        if total:
            raise CommandError(f"{total} differences between the two databases.")

    def _compare(self, tolerance: float) -> dict[str, Any]:
        """Walk both databases in key order and collect differences.

        Args:
            tolerance: Absolute tolerance for numbers in product payloads.

        Returns:
            Counts compared and matched, differences, and keys found on one
            side only.
        """
        compared = dict.fromkeys(("detections", *OUTCOME_KINDS, "objects"), 0)
        matched = dict(compared)
        differences: list[dict[str, Any]] = []
        only_in_left: list[int] = []
        only_in_right: list[int] = []

        pairs = merge_join(
            detection_records(LEFT_ALIAS), detection_records(RIGHT_ALIAS)
        )
        for dia_source_id, left, right in pairs:
            if right is None:
                only_in_left.append(dia_source_id)
                continue
            if left is None:
                only_in_right.append(dia_source_id)
                continue
            found = self._compare_detection(
                dia_source_id, left, right, tolerance, compared, matched
            )
            compared["detections"] += 1
            matched["detections"] += not found
            differences.extend(found)

        object_differences: list[dict[str, Any]] = []
        objects_only_in_left: list[int] = []
        objects_only_in_right: list[int] = []
        pairs = merge_join(object_records(LEFT_ALIAS), object_records(RIGHT_ALIAS))
        for dia_object_id, left, right in pairs:
            if right is None:
                objects_only_in_left.append(dia_object_id)
                continue
            if left is None:
                objects_only_in_right.append(dia_object_id)
                continue
            found = self._compare_object(dia_object_id, left, right)
            compared["objects"] += 1
            matched["objects"] += not found
            object_differences.extend(found)

        return {
            "compared": compared,
            "matched": matched,
            "differences": differences,
            "only_in_left": only_in_left,
            "only_in_right": only_in_right,
            "object_differences": object_differences,
            "objects_only_in_left": objects_only_in_left,
            "objects_only_in_right": objects_only_in_right,
        }

    @staticmethod
    def _compare_detection(
        dia_source_id: int,
        left: dict[str, Any],
        right: dict[str, Any],
        tolerance: float,
        compared: dict[str, int],
        matched: dict[str, int],
    ) -> list[dict[str, Any]]:
        """Compare one detection found in both databases.

        Args:
            dia_source_id: The detection's ``diaSourceId``.
            left: Its left record.
            right: Its right record.
            tolerance: Absolute tolerance for numbers in product payloads.
            compared: Counts compared, updated for each kind of outcome.
            matched: Counts matched, updated for each kind of outcome.

        Returns:
            Its differences.
        """
        found = [
            difference("dia_source_id", dia_source_id, field, left[field], right[field])
            for field in DETECTION_FIELDS
            if left[field] != right[field]
        ]
        for key in sorted(left["outcomes"].keys() | right["outcomes"].keys()):
            name, version, head = key
            where = {"package_name": name, "package_version": version, "head": head}
            ours = left["outcomes"].get(key)
            theirs = right["outcomes"].get(key)
            ours_state = "absent" if ours is None else ours["state"]
            theirs_state = "absent" if theirs is None else theirs["state"]
            if ours_state != theirs_state:
                found.append(
                    difference(
                        "dia_source_id",
                        dia_source_id,
                        "outcome",
                        ours_state,
                        theirs_state,
                        **where,
                    )
                )
                continue
            differing = [
                field
                for _, field in OUTCOME_FIELDS[ours_state]
                if not (
                    same(ours[field], theirs[field], tolerance)
                    if field == "payload"
                    else ours[field] == theirs[field]
                )
            ]
            kind = f"{ours_state}s"
            if kind in compared:
                compared[kind] += 1
                matched[kind] += not differing
            found.extend(
                difference(
                    "dia_source_id",
                    dia_source_id,
                    field,
                    ours[field],
                    theirs[field],
                    **where,
                )
                for field in differing
            )
        return found

    @staticmethod
    def _compare_object(
        dia_object_id: int, left: dict[str, Any], right: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Compare one object's qualifying state found in both databases.

        Args:
            dia_object_id: The object's ``diaObjectId``.
            left: Its left qualifying fields.
            right: Its right qualifying fields.

        Returns:
            Its differences: whether it qualified, or, when both qualified
            it, each qualifying field that differs.
        """
        qualified = (
            left["qualifying_detection"] is not None,
            right["qualifying_detection"] is not None,
        )
        if qualified[0] != qualified[1]:
            return [difference("dia_object_id", dia_object_id, "qualified", *qualified)]
        return [
            difference("dia_object_id", dia_object_id, name, left[name], right[name])
            for _, name in OBJECT_FIELDS
            if left[name] != right[name]
        ]

    def _print(self, result: dict[str, Any]) -> None:
        """Print the comparison as text.

        Args:
            result: The comparison.
        """
        write = self.stdout.write
        write(f"Left:  {result['left']}")
        write(f"Right: {result['right']}")
        if result["output_tolerance"]:
            write(f"Payloads compared within {result['output_tolerance']:g}.")
        for kind in ("detections", *OUTCOME_KINDS, "objects"):
            write(
                f"{kind.capitalize()}: {result['compared'][kind]} in both, "
                f"{result['matched'][kind]} identical."
            )
        for label, ids in (
            ("Detections only in left", result["only_in_left"]),
            ("Detections only in right", result["only_in_right"]),
            ("Objects only in left", result["objects_only_in_left"]),
            ("Objects only in right", result["objects_only_in_right"]),
        ):
            if ids:
                write(f"{label} ({len(ids)}): {', '.join(map(str, ids))}")
        for entry in result["differences"] + result["object_differences"]:
            if "dia_source_id" in entry:
                where = f"detection {entry['dia_source_id']}"
            else:
                where = f"object {entry['dia_object_id']}"
            if "head" in entry:
                where += (
                    f" [{entry['package_name']} {entry['package_version']} "
                    f"{entry['head']}]"
                )
            line = f"{where}: {entry['field']} differs"
            if "left" in entry:
                line += f" (left {entry['left']!r}, right {entry['right']!r})"
            write(line)
        total = count_differences(result)
        write("No differences." if not total else f"{total} differences.")
