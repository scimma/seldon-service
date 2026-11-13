"""Broker settings, read from ``SELDON_<BROKER>_``-prefixed environment variables.

One class per chosen broker: Fink and the service's own Lasair filter
qualify detections; Babamul is the full stream. Each holds the broker's
role, connection, credentials, topics, reliability cut, and SN-like
threshold. Settings are read once at startup, so a threshold change applies
on restart and forward only.

Credentials (username, password, consumer group, bootstrap servers) are
``SecretStr``: they never appear in the settings' ``repr``, ``str``, or
``model_dump`` output, and are revealed only when the Kafka client
configuration is built. An empty variable means unset.
"""

from typing import ClassVar, Self

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from seldon.domain.intake.delivery import BrokerRole
from seldon.domain.intake.qualification import Thresholds


class BrokerSettings(BaseSettings):
    """Settings common to every broker.

    Attributes:
        name: The configured broker name; it also selects the parser.
        enabled: Whether the consumer reads this broker.
        role: Qualifying (its scores can qualify a detection) or full-stream.
        bootstrap_servers: Kafka bootstrap servers, comma-separated.
        group_id: Kafka consumer group. For Fink it is the auth identity.
        username: SASL username, where the broker uses SASL.
        password: SASL password, where the broker uses SASL.
        security_protocol: Kafka ``security.protocol``.
        sasl_mechanism: Kafka ``sasl.mechanism``, where SASL is used.
        topics: Topics to read, comma-separated; an entry starting with ``^``
            is a regular expression over topic names.
        reliability_cut: Lowest reliability score that qualifies.
        sn_like_threshold: Lowest SN-like score that qualifies.
        max_partition_fetch_bytes: Kafka ``max.partition.fetch.bytes``, when
            the library default is too large for the link.
    """

    name: ClassVar[str]

    enabled: bool = False
    role: BrokerRole = BrokerRole.QUALIFYING
    bootstrap_servers: SecretStr | None = None
    group_id: SecretStr | None = None
    username: SecretStr | None = None
    password: SecretStr | None = None
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str | None = None
    topics: str = ""
    reliability_cut: float | None = Field(None, ge=0.0, le=1.0)
    sn_like_threshold: float | None = Field(None, ge=0.0, le=1.0)
    max_partition_fetch_bytes: int | None = Field(None, gt=0)

    model_config = SettingsConfigDict(
        case_sensitive=False,
        env_ignore_empty=True,
        # A validation error would otherwise echo the raw input, credentials
        # included.
        hide_input_in_errors=True,
    )

    @property
    def topic_list(self) -> list[str]:
        """The configured topics.

        Returns:
            Each non-empty comma-separated entry of ``topics``, stripped.
        """
        return [t.strip() for t in self.topics.split(",") if t.strip()]

    @property
    def thresholds(self) -> Thresholds | None:
        """The cut and threshold a qualifying broker applies.

        Returns:
            The thresholds in effect, or ``None`` for a full-stream broker.
        """
        if self.role is not BrokerRole.QUALIFYING:
            return None
        return Thresholds(
            reliability_cut=self.reliability_cut,
            sn_like_threshold=self.sn_like_threshold,
        )

    @model_validator(mode="after")
    def _check_enabled(self) -> Self:
        """Require what an enabled broker needs to connect and qualify.

        Messages name fields only, never their values.

        Returns:
            The validated settings.

        Raises:
            ValueError: If an enabled broker lacks a required setting.
        """
        if not self.enabled:
            return self
        missing = [
            field
            for field in ("bootstrap_servers", "group_id")
            if getattr(self, field) is None
        ]
        if not self.topic_list:
            missing.append("topics")
        if self.role is BrokerRole.QUALIFYING:
            missing += [
                field
                for field in ("reliability_cut", "sn_like_threshold")
                if getattr(self, field) is None
            ]
        if self.security_protocol.upper().startswith("SASL"):
            missing += [
                field
                for field in ("username", "password", "sasl_mechanism")
                if getattr(self, field) is None
            ]
        if missing:
            raise ValueError(
                f"{self.name} is enabled but missing: {', '.join(missing)}"
            )
        return self


class FinkSettings(BrokerSettings):
    """Fink's SN selection topic; the group id is the LSST livestream identity.

    LSST livestream credentials set no password: Kafka uses only the
    bootstrap servers and the group id. ``username`` is kept for the
    record and is not sent to Kafka.
    """

    name: ClassVar[str] = "fink"

    topics: str = "fink_most_likely_sn_lsst"
    reliability_cut: float | None = Field(0.6, ge=0.0, le=1.0)
    sn_like_threshold: float | None = Field(0.5, ge=0.0, le=1.0)
    # Larger fetches stall past librdkafka's timeout on a slow link to Fink.
    max_partition_fetch_bytes: int | None = Field(131_072, gt=0)

    model_config = SettingsConfigDict(env_prefix="SELDON_FINK_")


class LasairSettings(BrokerSettings):
    """The service's own Lasair filter: no credentials, any group id.

    Its SN-like signal is categorical (Sherlock class SN or NT scores 1.0),
    so the default threshold is 1.0.
    """

    name: ClassVar[str] = "lasair"

    bootstrap_servers: SecretStr | None = SecretStr("lasair-lsst-kafka.lsst.ac.uk:9092")
    group_id: SecretStr | None = SecretStr("seldon-service")
    topics: str = "lasair_366c0efe0df"
    reliability_cut: float | None = Field(0.6, ge=0.0, le=1.0)
    sn_like_threshold: float | None = Field(1.0, ge=0.0, le=1.0)

    model_config = SettingsConfigDict(env_prefix="SELDON_LASAIR_")


class BabamulSettings(BrokerSettings):
    """Babamul, the full stream: every ``babamul.lsst.*`` topic, SASL/SCRAM.

    The consumer group must start with ``<username>-``.
    """

    name: ClassVar[str] = "babamul"

    role: BrokerRole = BrokerRole.FULL_STREAM
    bootstrap_servers: SecretStr | None = SecretStr("kaboom.caltech.edu:9093")
    security_protocol: str = "SASL_PLAINTEXT"
    sasl_mechanism: str | None = "SCRAM-SHA-512"
    topics: str = r"^babamul\.lsst\..*"

    model_config = SettingsConfigDict(env_prefix="SELDON_BABAMUL_")

    @model_validator(mode="after")
    def _check_group_prefix(self) -> Self:
        """Require the consumer group Babamul accepts for this user.

        Returns:
            The validated settings.

        Raises:
            ValueError: If both are set and the group does not start with
                ``<username>-``.
        """
        if self.username is None or self.group_id is None:
            return self
        prefix = f"{self.username.get_secret_value()}-"
        if not self.group_id.get_secret_value().startswith(prefix):
            raise ValueError("babamul group_id must start with '<username>-'")
        return self


def load_broker_settings() -> tuple[FinkSettings, LasairSettings, BabamulSettings]:
    """Build every broker's settings from the current environment.

    Returns:
        Fink's, Lasair's, and Babamul's settings, enabled or not.

    Raises:
        pydantic.ValidationError: If a ``SELDON_<BROKER>_*`` variable holds a
            value its field rejects, or an enabled broker lacks a setting.
    """
    return FinkSettings(), LasairSettings(), BabamulSettings()
