"""Replay a recording of broker traffic into a separate database.

The recording (written by ``capture_alerts``) is fed through the real Kafka
adapter and the real intake loop, with ``seldon.replay.source``'s
file-backed consumer in place of the network client and its replay clock in
place of the system clock, so receipt times, the sweep and silence checks
all run on replay time. No broker connection is ever opened, and commits
are recorded in memory only, so the live consumer groups are untouched.

The live database is never written. Before any query the command refuses a
``--database`` name equal to the configured one; it then creates that
database on the same server (refusing one that exists unless ``--replace``
is given), migrates it, and switches the default connection to it.

Several replay processes can replay one recording into one database
together, as several consumer processes share the live partitions: give
each the same ``--process-count`` and its own ``--process-index``. Each
then takes only the recorded partitions assigned to its index (see
``Recording.share``). Process 0 creates and migrates the database as above;
the others wait for it to exist and be fully migrated, and never create or
replace it.

Every recorded broker is replayed with its configured parser, role and
thresholds, whether or not it is enabled for live intake. The command exits
once every message is released and acknowledged, and logs the alerts
taken, the wall time, and alerts a second, in total and per broker, then
writes the wall time spent in each intake stage, timed on the replay's
wall clock.

Intake only owes detections; it never runs a model. With ``--seed-heads
all``, process 0 copies each model release registered in the configured
database and not retired (its name, version, digest and declaration) into
the replay database, then enables every head each release declares there,
effective from the epoch, so every detection the replay owes is due for
every head whenever it became owed. A replay is complete only once each
deployment's worker has run ``forecast_worker --until-idle`` against the
replay database; until then its outcomes are partial and ``compare_replays``
reports them as still due.
"""

import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from django.conf import settings
from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError, CommandParser
from django.db import DEFAULT_DB_ALIAS, connections
from django.db.migrations.executor import MigrationExecutor

from seldon import deployments
from seldon.config.brokers import BrokerSettings, load_broker_settings
from seldon.config.intake import get_intake_settings
from seldon.infrastructure.brokers.base import Polled
from seldon.infrastructure.brokers.kafka import KafkaBrokerAdapter
from seldon.intake.timing import StageTimer, TimingSummary
from seldon.management.commands.consume_alerts import (
    DEFAULT_MAX_MESSAGES,
    DEFAULT_POLL_TIMEOUT,
    DEFAULT_SWEEP_MINUTES,
    IntakeConsumer,
    qualifying_thresholds,
)
from seldon.models import HeadRevision, ModelDeployment
from seldon.replay.recording import read_manifest
from seldon.management.commands.capture_alerts import parse_utc
from seldon.replay.source import Replay, ReplayMode, load_recording

logger = logging.getLogger("seldon.replay")


class _CountingAdapter(KafkaBrokerAdapter):
    """The Kafka adapter, counting the alerts it acknowledges."""

    def __init__(
        self,
        settings: BrokerSettings,
        consumer: Any,
        clock: Callable[[], datetime],
        timer: StageTimer | None = None,
    ) -> None:
        """Subscribe through the given consumer.

        Args:
            settings: The broker's settings.
            consumer: The file-backed consumer.
            clock: The replay clock.
            timer: Times the adapter's stages.
        """
        self.acknowledged = 0
        super().__init__(settings, consumer=consumer, clock=clock, timer=timer)

    def ack(self, batch: Sequence[Polled]) -> None:
        """Acknowledge and count a batch of alerts.

        Args:
            batch: Consecutive alerts this adapter returned from ``poll``.
        """
        super().ack(batch)
        self.acknowledged += len(batch)


@dataclass(frozen=True)
class ReplaySummary:
    """What a replay took in.

    Attributes:
        alerts: Alerts acknowledged per broker.
        elapsed_seconds: Wall time the replay ran.
        timing: Wall time spent in each intake stage, if timed.
    """

    alerts: dict[str, int]
    elapsed_seconds: float
    timing: TimingSummary | None = None

    def rate(self, count: int) -> float:
        """A count per second of wall time.

        Args:
            count: Alerts.

        Returns:
            Alerts a second; 0 when no time passed.
        """
        return count / self.elapsed_seconds if self.elapsed_seconds > 0 else 0.0

    def lines(self) -> list[str]:
        """The summary as text.

        Returns:
            A total line, then one line per broker.
        """
        total = sum(self.alerts.values())
        out = [
            f"Replayed {total} alerts in {self.elapsed_seconds:.1f} s: "
            f"{self.rate(total):.1f} alerts a second"
        ]
        for broker, count in self.alerts.items():
            out.append(f"  {broker}: {count} alerts, {self.rate(count):.1f} a second")
        return out


def run_replay(
    replay: Replay,
    *,
    poll_timeout: float = DEFAULT_POLL_TIMEOUT,
    sweep_interval: timedelta = timedelta(minutes=DEFAULT_SWEEP_MINUTES),
) -> ReplaySummary:
    """Run the intake loop over a replay until it is drained and acknowledged.

    Writes go to whatever database the default connection uses.

    Args:
        replay: The replay to read.
        poll_timeout: Longest wait after a cycle in which no broker returned
            an alert, in wall seconds; it ends early when the next message
            is due.
        sweep_interval: How often to sweep pending detections, replay time.

    Returns:
        The alerts taken, the wall time it took, and the time per stage.
    """
    by_name = {s.name: s for s in load_broker_settings()}
    chosen = [by_name[b].model_copy(update={"enabled": True}) for b in replay.brokers]
    timer = StageTimer(replay.monotonic)
    adapters = [
        _CountingAdapter(
            s, consumer=replay.consumer(s.name), clock=replay.clock, timer=timer
        )
        for s in chosen
    ]
    consumer = IntakeConsumer(
        adapters,
        qualifying_thresholds(chosen),
        get_intake_settings(),
        poll_timeout=poll_timeout,
        max_messages=DEFAULT_MAX_MESSAGES,
        sweep_interval=sweep_interval,
        clock=replay.clock,
        before_cycle=replay.before_cycle,
        finished=replay.finished,
        timer=timer,
        idle_wait=replay.idle_wait,
    )
    started = replay.monotonic()
    consumer.run()
    return ReplaySummary(
        alerts={a.name: a.acknowledged for a in adapters},
        elapsed_seconds=replay.monotonic() - started,
        timing=timer.summary(),
    )


def create_database(name: str, replace: bool) -> None:
    """Create a database on the default connection's server.

    A database being replaced is dropped with ``FORCE``, which ends every
    connection to it. A replay process that attached to the old database
    before process 0 replaced it therefore fails at once, rather than
    writing into a database that is about to be dropped or holding the
    drop up.

    Args:
        name: The database.
        replace: Whether to drop one that already exists.

    Raises:
        CommandError: If it exists and ``replace`` is false.
    """
    connection = connections[DEFAULT_DB_ALIAS]
    quoted = connection.ops.quote_name(name)
    with connection._nodb_cursor() as cursor:
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", [name])
        if cursor.fetchone() is not None:
            if not replace:
                raise CommandError(
                    f"Database {name} exists; pass --replace to drop and recreate it"
                )
            logger.warning("Dropping database %s to replace it", name)
            cursor.execute(f"DROP DATABASE {quoted} WITH (FORCE)")
        cursor.execute(f"CREATE DATABASE {quoted}")


def point_at(name: str) -> None:
    """Point the default connection at a database.

    Args:
        name: The database.
    """
    connection = connections[DEFAULT_DB_ALIAS]
    if connection.settings_dict["NAME"] == name:
        return
    connection.close()
    connection.settings_dict["NAME"] = name
    settings.DATABASES[DEFAULT_DB_ALIAS]["NAME"] = name


def use_database(name: str) -> None:
    """Point the default connection at a database and migrate it.

    Args:
        name: The database.
    """
    point_at(name)
    call_command("migrate", database=DEFAULT_DB_ALIAS, interactive=False, verbosity=0)


#: Longest a replay process other than process 0 waits for the database.
DATABASE_WAIT_SECONDS = 300.0


def wait_for_database(
    name: str,
    timeout: float = DATABASE_WAIT_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Wait for another replay process to create and migrate a database.

    Once the database exists the default connection points at it, and the
    wait ends when no migration is left to apply.

    Args:
        name: The database.
        timeout: Longest wait, in seconds.
        sleep: Waits a number of seconds between checks.

    Raises:
        CommandError: If it is not ready within the timeout.
    """
    connection = connections[DEFAULT_DB_ALIAS]
    deadline = time.monotonic() + timeout
    while True:
        with connection._nodb_cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", [name])
            exists = cursor.fetchone() is not None
        if exists:
            point_at(name)
            executor = MigrationExecutor(connection)
            if not executor.migration_plan(executor.loader.graph.leaf_nodes()):
                return
        if time.monotonic() >= deadline:
            raise CommandError(
                f"Database {name} is not created and migrated; start process 0"
            )
        sleep(1.0)


#: When seeded heads take effect: before any detection can have become owed.
SEED_EFFECTIVE_AT = datetime(1970, 1, 1, tzinfo=UTC)


def live_releases() -> list[ModelDeployment]:
    """Read the model releases registered and not retired.

    Call it while the default connection still points at the configured
    database, before switching to the replay database.

    Returns:
        The deployments, by package name and version.
    """
    return list(
        ModelDeployment.objects.filter(retired_at__isnull=True).order_by(
            "package_name", "package_version"
        )
    )


def seed_heads(releases: Sequence[ModelDeployment]) -> list[HeadRevision]:
    """Enable every declared head of every live release in this database.

    Each release is first registered here under its name, version, digest
    and declaration, unless one of that name and version already is; a
    worker built from the same artifact then registers as the same
    deployment. Every deployment here that is not retired then gets an
    enabled revision with the package's default inputs for each head it
    declares, effective from the epoch.

    Args:
        releases: Releases read from the configured database.

    Returns:
        The revisions written.
    """
    for release in releases:
        ModelDeployment.objects.get_or_create(
            package_name=release.package_name,
            package_version=release.package_version,
            defaults={
                "contract_version": release.contract_version,
                "artifact_digest": release.artifact_digest,
                "declaration": release.declaration,
            },
        )
    revisions = []
    for deployment in live_releases():
        try:
            declaration = deployments.stored_declaration(deployment)
        except ValueError as error:
            logger.warning("Not seeding %s: %s", deployment, error)
            continue
        for head in declaration.heads:
            revisions.append(
                deployments.set_head(deployment, head.name, True, SEED_EFFECTIVE_AT)
            )
    return revisions


class Command(BaseCommand):
    """Replay a recording into a separate database."""

    help = (
        "Feed a recording of broker traffic through the real adapter and "
        "intake loop on a replay clock, into a new database; never into the "
        "configured one, and never over the network. Intake only owes "
        "detections: a replay is complete only after each deployment's worker "
        "has run forecast_worker --until-idle against the replay database."
    )

    def add_arguments(self, parser: CommandParser) -> None:
        """Add the recording, database and mode options.

        Args:
            parser: The command's argument parser.
        """
        parser.add_argument(
            "--recording", required=True, help="The recording directory."
        )
        parser.add_argument(
            "--database",
            required=True,
            help="The database to create and replay into; never the configured one.",
        )
        parser.add_argument(
            "--replace",
            action="store_true",
            help="Drop and recreate the database if it exists.",
        )
        parser.add_argument(
            "--mode",
            choices=[m.value for m in ReplayMode],
            default=ReplayMode.PACED.value,
            help=(
                "paced: on the replay clock at --speed; max: everything at once; "
                "ordered: one broker run at a time, for comparing outcomes."
            ),
        )
        parser.add_argument(
            "--speed",
            type=float,
            default=1.0,
            help="Speed-up of a paced replay.",
        )
        parser.add_argument(
            "--brokers",
            nargs="+",
            help="Replay only these recorded brokers; all of them by default.",
        )
        parser.add_argument(
            "--from",
            dest="window_from",
            type=parse_utc,
            help=(
                "Replay only messages published at or after this ISO 8601 "
                "time, starting the replay there; the recording's start by "
                "default."
            ),
        )
        parser.add_argument(
            "--to",
            dest="window_to",
            type=parse_utc,
            help=(
                "Replay only messages published before this ISO 8601 time; "
                "to the recording's end by default."
            ),
        )
        parser.add_argument(
            "--process-count",
            type=int,
            default=1,
            help=(
                "How many replay processes share the recording and the "
                "database; each takes its share of the recorded partitions."
            ),
        )
        parser.add_argument(
            "--process-index",
            type=int,
            default=0,
            help=(
                "This process's index, 0 to --process-count minus 1. Process 0 "
                "creates the database (honouring --replace); the others wait "
                "for it."
            ),
        )
        parser.add_argument(
            "--seed-heads",
            choices=["all"],
            help=(
                "all: copy each model release registered in the configured "
                "database and not retired into the replay database, and enable "
                "every head it declares from the epoch, so every detection the "
                "replay owes is due for every head. Process 0 seeds; the "
                "others ignore it."
            ),
        )

    def handle(self, *args: Any, **options: Any) -> None:
        """Guard the live database, prepare the replay database, and replay.

        Args:
            *args: Unused.
            **options: The parsed options.

        Raises:
            CommandError: If the database is the configured one or exists,
                the recording is missing or lacks a named broker, or the
                speed is not positive.
        """
        name = options["database"]
        if name == settings.DATABASES[DEFAULT_DB_ALIAS]["NAME"]:
            raise CommandError(
                f"Refusing to replay into the configured database {name}"
            )
        root = Path(options["recording"])
        manifest = read_manifest(root)
        if manifest is None:
            raise CommandError(f"No recording in {root}")
        brokers = options["brokers"]
        missing = sorted(set(brokers or ()) - set(manifest["brokers"]))
        if missing:
            raise CommandError(f"The recording holds nothing for {', '.join(missing)}")
        if options["speed"] <= 0:
            raise CommandError("--speed must be positive")
        index, count = options["process_index"], options["process_count"]
        if count < 1 or not 0 <= index < count:
            raise CommandError(
                "--process-count must be positive and --process-index from 0 "
                "to one less than it"
            )
        try:
            recording = (
                load_recording(root, brokers)
                .window(options.get("window_from"), options.get("window_to"))
                .share(index, count)
            )
        except ValueError as error:
            raise CommandError(str(error))
        if index == 0:
            releases = live_releases() if options["seed_heads"] else None
            create_database(name, options["replace"])
            use_database(name)
            if releases is not None:
                self._seed(releases)
        else:
            wait_for_database(name)
        logger.info(
            "Replaying %s into database %s as process %d of %d",
            root,
            name,
            index,
            count,
        )

        replay = Replay(
            recording,
            ReplayMode(options["mode"]),
            run_size=DEFAULT_MAX_MESSAGES,
            speed=options["speed"],
        )
        summary = run_replay(replay)
        for line in summary.lines():
            logger.info("%s", line)
            self.stdout.write(line)
        # The intake loop has already logged the breakdown at its end.
        if summary.timing is not None:
            for line in summary.timing.lines():
                self.stdout.write(line)

    def _seed(self, releases: Sequence[ModelDeployment]) -> None:
        """Seed every head of each live release and say what was seeded.

        Args:
            releases: Releases read from the configured database.
        """
        revisions = seed_heads(releases)
        if not revisions:
            line = (
                "No registered deployment that is not retired declares a head; "
                "no heads seeded."
            )
            logger.warning("%s", line)
            self.stdout.write(line)
            return
        seeded: dict[str, list[str]] = {}
        for revision in revisions:
            seeded.setdefault(str(revision.deployment), []).append(revision.head)
        for deployment, heads in seeded.items():
            line = f"Seeded {deployment}: {', '.join(heads)} from the epoch"
            logger.info("%s", line)
            self.stdout.write(line)
