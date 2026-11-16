"""Stored records: objects, detections, deployments, outcomes, gaps, lag.

Identity follows Rubin: an object is a ``diaObjectId`` and a detection is a
``diaSourceId``. An outcome is identified by its detection, the model
deployment, and the head that produced it, and the database enforces that
triple as unique, so a detection gets at most one product, refusal, or
failure per deployment head however many brokers deliver it and however
many workers run.

Packets and products are stored as JSON, exactly as sent to or returned
from the model package. Caveats are a JSON list with an index, so products
to read with care can be queried by caveat code.
"""

from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from typing import Any

from django.contrib.postgres.indexes import GinIndex
from django.db import connection, models

from seldon.domain.intake.light_curve import observed_key

#: Hours after midnight UTC that a night starts (and the previous one ends).
NIGHT_START = timedelta(hours=12)


def night_of(moment: datetime) -> date:
    """Name the night a moment falls in.

    Nights follow Rubin's dayObs: a night runs from 12:00 UTC to 12:00 UTC
    and is named by the date at its start.

    Args:
        moment: An aware time.

    Returns:
        The date at the start of the 12:00-to-12:00 UTC night holding it.
    """
    return (moment.astimezone(UTC) - NIGHT_START).date()


class RubinObject(models.Model):
    """A Rubin ``diaObject`` and the event that qualified it, if any.

    The qualifying fields stay empty until a detection of the object
    qualifies. The broker's reliability cut and SN-like threshold are copied
    in as they were in effect at that moment, so later setting changes do not
    rewrite why the object qualified.

    Attributes:
        dia_object_id: Rubin's ``diaObjectId``.
        qualifying_detection: The detection that qualified the object.
        qualifying_broker: The broker whose scores qualified it.
        qualifying_reliability_score: That broker's reliability score for the
            qualifying detection.
        qualifying_sn_like_score: That broker's SN-like score for it.
        qualifying_reliability_cut: The broker's reliability cut in effect.
        qualifying_sn_like_threshold: The broker's SN-like threshold in effect.
        qualified_at: When the service found the object qualified.
    """

    dia_object_id = models.BigIntegerField(unique=True)
    qualifying_detection = models.OneToOneField(
        "Detection",
        null=True,
        blank=True,
        on_delete=models.PROTECT,
        related_name="qualified_object",
    )
    qualifying_broker = models.CharField(max_length=64, blank=True)
    qualifying_reliability_score = models.FloatField(null=True, blank=True)
    qualifying_sn_like_score = models.FloatField(null=True, blank=True)
    qualifying_reliability_cut = models.FloatField(null=True, blank=True)
    qualifying_sn_like_threshold = models.FloatField(null=True, blank=True)
    qualified_at = models.DateTimeField(null=True, blank=True)

    def qualifying_event(self) -> dict:
        """The qualifying event as captured when a detection becomes owed.

        Returns:
            The qualifying ``diaSourceId``, broker, its two scores, and its
            cut and threshold in effect.
        """
        return {
            "dia_source_id": self.qualifying_detection.dia_source_id,
            "broker": self.qualifying_broker,
            "reliability_score": self.qualifying_reliability_score,
            "sn_like_score": self.qualifying_sn_like_score,
            "reliability_cut": self.qualifying_reliability_cut,
            "sn_like_threshold": self.qualifying_sn_like_threshold,
        }

    def __str__(self) -> str:
        """Name the object by its Rubin identifier.

        Returns:
            The ``diaObjectId`` as text.
        """
        return f"diaObject {self.dia_object_id}"


class Detection(models.Model):
    """A Rubin ``diaSource`` the service has received.

    A detection is pending until its object qualifies, then owed or missed
    (a pending detection whose object never qualifies is discarded). What
    each model deployment made of an owed detection is not a detection
    state: it is an outcome per deployment and head (a product, a refusal,
    or a service-side failure), at most one each.

    Attributes:
        dia_source_id: Rubin's ``diaSourceId``.
        rubin_object: The object the detection belongs to.
        state: Where the detection stands: pending (delivered, not owed
            yet), owed, or missed.
        observed_mjd: The detection's observation time, as an MJD.
        owed_at: When the detection became owed, on the wall clock (not a
            replay's clock), so deployments agree on which head settings
            cover it; empty until then.
        qualification: The ``thresholds`` and ``qualifying_event`` in effect
            when the detection became owed, so later setting changes or a
            later qualifying event do not rewrite why it was owed.
    """

    class State(models.TextChoices):
        """Where a detection stands on its way to being owed."""

        PENDING = "pending"
        OWED = "owed"
        MISSED = "missed"

    dia_source_id = models.BigIntegerField(unique=True)
    rubin_object = models.ForeignKey(
        RubinObject, on_delete=models.PROTECT, related_name="detections"
    )
    state = models.CharField(max_length=16, choices=State.choices)
    observed_mjd = models.FloatField()
    owed_at = models.DateTimeField(null=True, blank=True, db_index=True)
    qualification = models.JSONField(default=dict)

    class Meta:
        indexes = [models.Index(fields=["state"])]

    def __str__(self) -> str:
        """Name the detection by its Rubin identifier.

        Returns:
            The ``diaSourceId`` as text.
        """
        return f"diaSource {self.dia_source_id}"


class LightCurvePoint(models.Model):
    """One ``diaSource`` measurement, stored once however many alerts carry it.

    A measurement is identified by its object, band, and observation time,
    because some brokers send previous detections without their
    ``diaSourceId``. The identifier is kept whenever an alert carries it.

    Attributes:
        dia_object_id: Rubin's ``diaObjectId`` for the measured object.
        dia_source_id: Rubin's ``diaSourceId``, when the alert carried it.
        observed_mjd: Observation time, as an MJD in TAI, as received.
        observed_key: ``observed_mjd`` rounded to the resolution points
            match at; set on save.
        band: The band, as normalized by intake.
        flux: Measured flux.
        flux_err: One-sigma flux uncertainty, same units as ``flux``.
    """

    dia_object_id = models.BigIntegerField()
    dia_source_id = models.BigIntegerField(unique=True, null=True, blank=True)
    observed_mjd = models.FloatField()
    observed_key = models.BigIntegerField(editable=False)
    band = models.CharField(max_length=8)
    flux = models.FloatField()
    flux_err = models.FloatField()

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["dia_object_id", "band", "observed_key"],
                name="unique_point_per_object_band_time",
            )
        ]

    def save(self, *args: Any, **kwargs: Any) -> None:
        """Set the rounded observation time, then save.

        Args:
            *args: Passed to ``Model.save``.
            **kwargs: Passed to ``Model.save``.
        """
        self.observed_key = observed_key(self.observed_mjd)
        super().save(*args, **kwargs)


class Delivery(models.Model):
    """One broker's delivery of a detection.

    A redelivery from the same broker does not add a row: the first delivery,
    with its times, is the one kept.

    Attributes:
        detection: The detection delivered.
        broker: The configured broker name.
        role: Whether the broker delivered as a qualifying broker or as the
            full-stream source.
        reliability_score: The broker's reliability score, when it sent one.
        sn_like_score: The broker's SN-like score, when it sent one.
        points: The light-curve points the alert carried, so the light curve
            it delivered can be rebuilt exactly.
        published_at: When the broker published the alert.
        received_at: When the service received it.
        schema_version: The broker's alert schema version.
        classifier_version: The broker's classifier version, when it gave one.
    """

    class Role(models.TextChoices):
        """How a broker's deliveries are used."""

        QUALIFYING = "qualifying"
        FULL_STREAM = "full_stream"

    detection = models.ForeignKey(
        Detection, on_delete=models.PROTECT, related_name="deliveries"
    )
    broker = models.CharField(max_length=64)
    role = models.CharField(max_length=16, choices=Role.choices)
    reliability_score = models.FloatField(null=True, blank=True)
    sn_like_score = models.FloatField(null=True, blank=True)
    points = models.ManyToManyField(LightCurvePoint, related_name="deliveries")
    published_at = models.DateTimeField(null=True, blank=True)
    received_at = models.DateTimeField()
    schema_version = models.CharField(max_length=64, blank=True)
    classifier_version = models.CharField(max_length=128, blank=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["detection", "broker"], name="unique_delivery_per_broker"
            )
        ]


class ModelDeployment(models.Model):
    """One model package release the service runs or has run.

    A release is identified by its package name and version; the artifact
    digest pins the exact build, so a different build under the same name
    and version is refused rather than mixed into the same outcomes.
    Outcomes stored before the model contract belong to retired deployments
    named ``seldon-pre-contract``, one per checkpoint, with the digest
    ``pre-contract``.

    Attributes:
        package_name: The package's distribution name.
        package_version: The package's version (for a pre-contract
            deployment, the checkpoint it stood for).
        contract_version: The contract version the package implements.
        artifact_digest: The sha256 of the package's wheel.
        declaration: The package's declaration, as it described itself.
        registered_at: When the deployment was first registered.
        last_heartbeat_at: When a worker for it last reported in.
        retired_at: When it stopped being owed work; empty while live.
    """

    package_name = models.CharField(max_length=128)
    package_version = models.CharField(max_length=255)
    contract_version = models.CharField(max_length=16)
    artifact_digest = models.CharField(max_length=128)
    declaration = models.JSONField()
    registered_at = models.DateTimeField(auto_now_add=True)
    last_heartbeat_at = models.DateTimeField(null=True, blank=True)
    retired_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["package_name", "package_version"],
                name="unique_deployment_per_release",
            )
        ]

    def __str__(self) -> str:
        """Name the deployment by its release.

        Returns:
            The package name and version.
        """
        return f"{self.package_name} {self.package_version}"


class HeadRevision(models.Model):
    """One operator change to a deployment head's settings.

    Revisions are only ever added. The revision in effect for a detection
    is the latest one for the deployment and head whose ``effective_at`` is
    at or before the detection's ``owed_at``; the detection is owed an
    outcome from that head only when that revision is enabled.

    Attributes:
        deployment: The deployment the head belongs to.
        head: The head's name, as the package declares it.
        enabled: Whether the head is owed outcomes under this revision.
        inputs: The head's input templates, or empty for the package's own
            defaults.
        effective_at: The earliest ``owed_at`` the revision covers.
        created_at: When the revision was written.
    """

    deployment = models.ForeignKey(
        ModelDeployment, on_delete=models.PROTECT, related_name="head_revisions"
    )
    head = models.CharField(max_length=64)
    enabled = models.BooleanField()
    inputs = models.JSONField(default=dict)
    effective_at = models.DateTimeField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [
            models.Index(
                fields=["deployment", "head", "effective_at"],
                name="seldon_revision_in_effect",
            )
        ]


class OutcomeClaim(models.Model):
    """One detection's outcome slot for one deployment head.

    The database allows one claim per detection, deployment, and head, so a
    detection gets at most one product, refusal, or failure from each head
    however many workers run. A worker leases the claim before predicting.

    Attributes:
        detection: The detection owed the outcome.
        deployment: The deployment that owes it.
        head: The head's name.
        revision: The head revision the outcome was owed under.
        state: Due, or which kind of outcome was stored.
        attempts: How many times a worker has tried it.
        leased_by: The worker holding the lease, if any.
        lease_expires_at: When the current lease lapses.
        created_at: When the claim was created.
        updated_at: When it last changed.
    """

    class State(models.TextChoices):
        """Whether an outcome is still due, and which kind was stored."""

        DUE = "due"
        PRODUCT = "product"
        REFUSAL = "refusal"
        FAILURE = "failure"

    detection = models.ForeignKey(
        Detection, on_delete=models.PROTECT, related_name="outcome_claims"
    )
    deployment = models.ForeignKey(ModelDeployment, on_delete=models.PROTECT)
    head = models.CharField(max_length=64)
    revision = models.ForeignKey(HeadRevision, on_delete=models.PROTECT)
    state = models.CharField(max_length=16, choices=State.choices, default=State.DUE)
    attempts = models.IntegerField(default=0)
    leased_by = models.CharField(max_length=128, blank=True)
    lease_expires_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["detection", "deployment", "head"],
                name="unique_outcome_per_head",
            )
        ]
        indexes = [
            models.Index(fields=["deployment", "state"], name="seldon_claim_state"),
            # The lease query's order, so it reads only the rows it leases.
            models.Index(
                fields=["deployment", "lease_expires_at", "detection", "head"],
                condition=models.Q(state="due"),
                name="seldon_claim_lease",
            ),
        ]


class Product(models.Model):
    """What one head returned for one detection.

    Attributes:
        claim: The claim it settles.
        output_kind: The contract output kind of ``payload``.
        payload: The head's output, as the package returned it.
        caveats: Why to read it with care: a list of ``code`` and
            ``detail`` objects.
        light_curve: The packet the package was given.
        app_version: The service release that stored it.
        stored_at: When it was stored.
    """

    claim = models.OneToOneField(
        OutcomeClaim, on_delete=models.PROTECT, related_name="product"
    )
    output_kind = models.CharField(max_length=32)
    payload = models.JSONField()
    caveats = models.JSONField(default=list)
    light_curve = models.JSONField()
    app_version = models.CharField(max_length=128)
    stored_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [GinIndex(fields=["caveats"], name="seldon_product_caveats")]


class HeadRefusal(models.Model):
    """A head's refusal to produce an outcome for one detection, and why.

    Attributes:
        claim: The claim it settles.
        code: The refusal code, kept verbatim even when outside the
            contract's vocabulary.
        message: The refusal's human-readable reason.
        detail: The code's structured detail.
        app_version: The service release that stored it.
        stored_at: When it was stored.
    """

    claim = models.OneToOneField(
        OutcomeClaim, on_delete=models.PROTECT, related_name="refusal"
    )
    code = models.CharField(max_length=64)
    message = models.TextField()
    detail = models.JSONField(default=dict)
    app_version = models.CharField(max_length=128)
    stored_at = models.DateTimeField(auto_now_add=True)


class ProductFailure(models.Model):
    """A service-side failure to get an outcome for one detection and head.

    Kept apart from refusals: a refusal is the package's answer, a failure
    is the service giving up after repeated errors.

    Attributes:
        claim: The claim it settles.
        reason: The last error's message.
        attempts: How many attempts were made.
        app_version: The service release that stored it.
        stored_at: When it was stored.
    """

    claim = models.OneToOneField(
        OutcomeClaim, on_delete=models.PROTECT, related_name="failure"
    )
    reason = models.TextField()
    attempts = models.IntegerField()
    app_version = models.CharField(max_length=128)
    stored_at = models.DateTimeField(auto_now_add=True)


class IntakeSkip(models.Model):
    """An alert intake could not turn into a forecast request.

    Attributes:
        broker: The configured broker name.
        reason: Why the alert was skipped.
        occurred_at: When it was skipped.
        raw_identifier: The alert's identifier as received, when one could
            be read.
    """

    broker = models.CharField(max_length=64)
    reason = models.CharField(max_length=64)
    occurred_at = models.DateTimeField()
    raw_identifier = models.CharField(max_length=255, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=["broker", "occurred_at"]),
            models.Index(
                fields=["broker", "raw_identifier"], name="seldon_skip_identifier"
            ),
        ]


class StreamGap(models.Model):
    """A period a broker's stream could not be read back.

    Attributes:
        broker: The configured broker name.
        started_at: Start of the period.
        ended_at: End of the period.
        kind: Confirmed (the transport shows messages were lost) or
            suspected (downtime or silence that may have lost messages).
    """

    class Kind(models.TextChoices):
        """How sure the service is that messages were lost."""

        CONFIRMED = "confirmed"
        SUSPECTED = "suspected"

    broker = models.CharField(max_length=64)
    started_at = models.DateTimeField()
    ended_at = models.DateTimeField()
    kind = models.CharField(max_length=16, choices=Kind.choices)


class IntakeLagCount(models.Model):
    """How many alerts one broker published in one night took a given lag.

    Each alert whose work is newly stored (a new delivery or a new intake
    skip) adds one count, in the same transaction as that work. The lag runs
    from the broker publishing the alert to the consumer committing its
    work, in whole seconds rounded down. The night is the alert's
    publication night (see ``night_of``). These rows outlive the deliveries
    the pending sweep deletes.

    Two values of ``lag_seconds`` are reserved:

    - ``OVERFLOW_SECONDS`` (3600) is the one bucket shared by every lag of an
      hour or more;
    - ``None`` is the unknown-publication bucket: alerts without a
      publication time, counted under the night they were received in.

    A lag can be negative when the broker's clock runs ahead of the
    consumer's; it is stored as measured.

    Attributes:
        broker: The configured broker name.
        night: The publication night (receipt night when unknown).
        lag_seconds: The lag in whole seconds, ``OVERFLOW_SECONDS`` for an
            hour or more, or ``None`` when the publication time is unknown.
        count: How many alerts fell in this bucket.
    """

    #: The bucket shared by every lag of an hour or more, in seconds.
    OVERFLOW_SECONDS = 3600

    broker = models.CharField(max_length=64)
    night = models.DateField()
    lag_seconds = models.IntegerField(null=True, blank=True)
    count = models.PositiveIntegerField(default=0)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["broker", "night", "lag_seconds"],
                name="unique_intake_lag_bucket",
                nulls_distinct=False,
            )
        ]

    @classmethod
    def bucket(
        cls,
        broker: str,
        published_at: datetime | None,
        received_at: datetime,
        committed_at: datetime,
    ) -> tuple[str, date, int | None]:
        """Name the bucket one alert counts in.

        Args:
            broker: The configured broker name.
            published_at: When the broker published it, if known.
            received_at: When the service received it.
            committed_at: When its work is committed.

        Returns:
            The broker, the night, and the lag in seconds (or ``None``).
        """
        if published_at is None:
            return broker, night_of(received_at), None
        lag = (committed_at - published_at) // timedelta(seconds=1)
        return broker, night_of(published_at), min(lag, cls.OVERFLOW_SECONDS)

    @classmethod
    def add_counts(cls, counts: Mapping[tuple[str, date, int | None], int]) -> None:
        """Add counts to their buckets, one upsert per bucket in sorted order.

        Call it inside the transaction that stores the counted alerts' work,
        so a rollback drops the counts too. The fixed order keeps concurrent
        consumers from deadlocking on these rows.

        Args:
            counts: Bucket (broker, night, lag or ``None``) to count to add.
        """
        table = connection.ops.quote_name(cls._meta.db_table)
        sql = (
            f"INSERT INTO {table} (broker, night, lag_seconds, count) "
            "VALUES (%s, %s, %s, %s) "
            "ON CONFLICT (broker, night, lag_seconds) "
            f"DO UPDATE SET count = {table}.count + EXCLUDED.count"
        )
        ordered = sorted(
            counts.items(),
            key=lambda item: (
                item[0][0],
                item[0][1],
                item[0][2] is None,
                item[0][2] or 0,
            ),
        )
        with connection.cursor() as cursor:
            for (broker, night, lag), count in ordered:
                cursor.execute(sql, [broker, night, lag, count])


class ConsumerLastCommit(models.Model):
    """When one consumer instance last committed a batch from one broker.

    Several consumer processes share each broker's partitions, so a process
    may hold none of a broker's partitions and hear nothing from it while
    the others take its alerts. The silence check therefore reads each
    broker's latest commit across every instance here, not one process's
    own activity. Each batch updates its instance's row inside the batch's
    transaction, just before it commits.

    Attributes:
        broker: The configured broker name.
        instance: The consumer instance: host name and process id.
        committed_at: When that instance's latest batch from the broker
            committed, on the consumer's clock.
    """

    broker = models.CharField(max_length=64)
    instance = models.CharField(max_length=255)
    committed_at = models.DateTimeField()

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["broker", "instance"], name="unique_last_commit_per_instance"
            )
        ]

    @classmethod
    def record(cls, broker: str, instance: str, committed_at: datetime) -> None:
        """Set an instance's last-commit time for a broker.

        Call it inside the batch's transaction, so a rollback drops it too.

        Args:
            broker: The configured broker name.
            instance: The consumer instance.
            committed_at: When the batch commits.
        """
        cls.objects.bulk_create(
            [cls(broker=broker, instance=instance, committed_at=committed_at)],
            update_conflicts=True,
            unique_fields=["broker", "instance"],
            update_fields=["committed_at"],
        )

    @classmethod
    def latest(cls, brokers: list[str]) -> dict[str, datetime]:
        """Each broker's latest commit across every instance.

        Args:
            brokers: The configured broker names to look up.

        Returns:
            Broker name to its latest commit time; a broker no instance has
            committed is absent.
        """
        rows = (
            cls.objects.filter(broker__in=brokers)
            .values("broker")
            .annotate(latest=models.Max("committed_at"))
        )
        return {row["broker"]: row["latest"] for row in rows}
