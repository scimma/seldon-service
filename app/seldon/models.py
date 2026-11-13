"""Stored records for alert intake: objects, detections, forecasts, and gaps.

Identity follows Rubin: an object is a ``diaObjectId`` and a detection is a
``diaSourceId``. A forecast is identified by its detection and the checkpoint
that produced it, and the database enforces that pair as unique, so a
detection is forecast at most once per checkpoint however many brokers
deliver it and however many consumer processes run.

Array data (light curves, evaluation grids, model outputs) is stored as JSON,
exactly as passed to or returned from the forecast call. The regime fields
are columns so sparse and out-of-regime forecasts can be queried.
"""

from django.db import models

from seldon.domain.errors import RejectionCode


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

    def __str__(self) -> str:
        """Name the object by its Rubin identifier.

        Returns:
            The ``diaObjectId`` as text.
        """
        return f"diaObject {self.dia_object_id}"


class Detection(models.Model):
    """A Rubin ``diaSource`` the service has received.

    Attributes:
        dia_source_id: Rubin's ``diaSourceId``.
        rubin_object: The object the detection belongs to.
        state: Where the detection stands: pending (delivered, not owed a
            forecast yet), owed, forecast, refused, or missed.
        observed_mjd: The detection's observation time, as an MJD.
    """

    class State(models.TextChoices):
        """Where a detection stands on its way to a forecast."""

        PENDING = "pending"
        OWED = "owed"
        FORECAST = "forecast"
        REFUSED = "refused"
        MISSED = "missed"

    dia_source_id = models.BigIntegerField(unique=True)
    rubin_object = models.ForeignKey(
        RubinObject, on_delete=models.PROTECT, related_name="detections"
    )
    state = models.CharField(max_length=16, choices=State.choices)
    observed_mjd = models.FloatField()

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

    Attributes:
        dia_source_id: Rubin's ``diaSourceId`` for the measurement.
        observed_mjd: Observation time, as an MJD.
        band: The band, as normalized by intake.
        flux: Measured flux.
        flux_err: One-sigma flux uncertainty, same units as ``flux``.
    """

    dia_source_id = models.BigIntegerField(unique=True)
    observed_mjd = models.FloatField()
    band = models.CharField(max_length=8)
    flux = models.FloatField()
    flux_err = models.FloatField()


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


class Forecast(models.Model):
    """A stored forecast of one detection under one checkpoint.

    The input fields are exactly what was passed to the forecast call; the
    output, regime, and provenance fields are what it returned.

    Attributes:
        detection: The detection forecast.
        checkpoint: The checkpoint, as ``<checkpoint directory>/<file>``.
        light_curve: The input light curve: ``times``, ``flux``,
            ``flux_err``, ``detected``, and ``bands`` lists.
        zero_point: The zero point supplied with the light curve: a number,
            one number per observation, or a declaration's value.
        grid: The evaluation grid: ``eval_times`` and ``eval_bands`` lists.
        outputs: Every output family the call returned (forecast flux and
            uncertainty, output zero point, parameters, class probabilities,
            predicted class, latent mean and sigma).
        in_training_regime: ``False`` when the forecast is sparse or touches
            an untrained band.
        sparse: Fewer usable observations than training admitted.
        n_observations: Usable observations the model conditioned on.
        min_points: The training selection filter's minimum.
        untrained_bands: Bands with no training observations that the
            forecast touched.
        eval_span_days: Latest minus earliest evaluation time, in days.
        eval_span_in_time_scales: ``eval_span_days`` over the training time
            scale.
        time_scale_days: The training time scale, in days.
        library_version: The ``seldon_core`` version.
        provenance_zero_point_mag: The zero point of the model's flux scale.
        torch_version: The torch version.
        app_version: The service release that stored the forecast.
        thresholds: Each broker's reliability cut and SN-like threshold in
            effect when the forecast was stored, by broker name.
        stored_at: When the forecast was stored.
    """

    detection = models.ForeignKey(
        Detection, on_delete=models.PROTECT, related_name="forecasts"
    )
    checkpoint = models.CharField(max_length=255)

    light_curve = models.JSONField()
    zero_point = models.JSONField()
    grid = models.JSONField()
    outputs = models.JSONField()

    in_training_regime = models.BooleanField()
    sparse = models.BooleanField()
    n_observations = models.IntegerField()
    min_points = models.IntegerField()
    untrained_bands = models.JSONField(default=list)
    eval_span_days = models.FloatField()
    eval_span_in_time_scales = models.FloatField()
    time_scale_days = models.FloatField()

    library_version = models.CharField(max_length=64)
    provenance_zero_point_mag = models.FloatField()
    torch_version = models.CharField(max_length=64)
    app_version = models.CharField(max_length=128)
    thresholds = models.JSONField(default=dict)
    stored_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["detection", "checkpoint"],
                name="unique_forecast_per_checkpoint",
            )
        ]


class Refusal(models.Model):
    """A detection the model refused to forecast, and why.

    Attributes:
        detection: The detection refused.
        checkpoint: The checkpoint that refused it.
        code: The rejection code, from the adapter's public set.
        message: The refusal's human-readable reason.
        bands: The unresolved band names, for an unknown-band refusal.
        app_version: The service release that stored the refusal.
        stored_at: When the refusal was stored.
    """

    detection = models.ForeignKey(
        Detection, on_delete=models.PROTECT, related_name="refusals"
    )
    checkpoint = models.CharField(max_length=255)
    code = models.CharField(
        max_length=32, choices=[(code.value, code.name) for code in RejectionCode]
    )
    message = models.TextField()
    bands = models.JSONField(default=list)
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
        indexes = [models.Index(fields=["broker", "occurred_at"])]


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
