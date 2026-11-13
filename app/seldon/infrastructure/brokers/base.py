"""What every broker adapter offers its caller, as plain values.

An adapter polls its broker for deliveries, lets the caller acknowledge each
one once it is safely handled, and reports stream gaps. Intake skips and
gaps are reported as values; the adapter writes nothing to the database.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

from seldon.domain.intake.delivery import BrokerRole, Delivery

#: Longest skip reason the intake-skip record holds.
MAX_REASON_LENGTH = 64


class GapKind(Enum):
    """How sure the service is that messages were lost."""

    CONFIRMED = "confirmed"
    """The transport shows messages were lost."""

    SUSPECTED = "suspected"
    """Downtime or silence that may have lost messages."""


@dataclass(frozen=True)
class StreamGapReport:
    """A period a broker's stream could not be read back.

    Attributes:
        broker: The configured broker name.
        started_at: Start of the period: the last message handled before it.
        ended_at: End of the period: the first message available after it.
        kind: Confirmed or suspected.
    """

    broker: str
    started_at: datetime
    ended_at: datetime
    kind: GapKind


@dataclass(frozen=True)
class IntakeSkipReport:
    """An alert intake could not turn into a delivery.

    Attributes:
        broker: The configured broker name.
        reason: Why it was skipped, at most 64 characters.
        occurred_at: When it was skipped.
        raw_identifier: The alert's identifier as received, or its place in
            the stream when no identifier could be read; empty when neither.
    """

    broker: str
    reason: str
    occurred_at: datetime
    raw_identifier: str = ""


@dataclass(frozen=True)
class ParseContext:
    """What a parser needs besides the alert itself.

    Attributes:
        broker: The configured broker name.
        role: The broker's configured role.
        topic: The topic the alert arrived on.
        published_at: When the broker published it, when known.
        received_at: When the service received it.
    """

    broker: str
    role: BrokerRole
    topic: str
    published_at: datetime | None
    received_at: datetime


class MissingFieldError(Exception):
    """A field an alert must carry is absent or empty.

    Attributes:
        field: The field's dotted path in the alert.
    """

    def __init__(self, field: str) -> None:
        """Name the missing field.

        Args:
            field: The field's dotted path in the alert.
        """
        super().__init__(f"missing {field}")
        self.field = field


@dataclass(frozen=True)
class Polled:
    """One polled alert: a delivery or an intake skip, and how to ack it.

    Both outcomes are acknowledged once the caller has handled them (stored
    the forecast or the skip); until then the broker may deliver them again.

    Attributes:
        outcome: The parsed delivery, or why the alert was skipped.
        receipt: Transport-specific acknowledgement handle; opaque to callers.
    """

    outcome: Delivery | IntakeSkipReport
    receipt: Any


class BrokerAdapter(ABC):
    """A broker's stream, turned into broker-neutral deliveries.

    Attributes:
        name: The configured broker name.
        role: The broker's configured role.
    """

    name: str
    role: BrokerRole

    @abstractmethod
    def poll(self, timeout: float, max_messages: int = 100) -> list[Polled]:
        """Wait for alerts and parse them.

        Args:
            timeout: Longest wait, in seconds.
            max_messages: Most messages to take in one call.

        Returns:
            The polled alerts in stream order; empty when none arrived.
        """

    @abstractmethod
    def ack(self, polled: Polled) -> None:
        """Mark an alert handled, so the broker does not deliver it again.

        Acknowledge in poll order: acknowledging one alert also covers every
        earlier alert from the same partition.

        Args:
            polled: An alert this adapter returned from ``poll``.
        """

    @abstractmethod
    def gaps(self) -> list[StreamGapReport]:
        """Gaps found since the last call.

        Returns:
            Each gap once, in the order found.
        """

    @abstractmethod
    def close(self) -> None:
        """Release the connection."""
