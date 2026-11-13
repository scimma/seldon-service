"""Broker adapters: each chosen broker's stream as broker-neutral deliveries.

``build_adapters`` builds one adapter per enabled broker from the
``SELDON_<BROKER>_*`` settings. Each adapter polls for ``Polled`` alerts
(a ``Delivery`` or an ``IntakeSkipReport``), acknowledges them, and reports
``StreamGapReport`` values; the caller stores skips and gaps.
"""

from collections.abc import Iterable

from seldon.config.brokers import BrokerSettings, load_broker_settings
from seldon.infrastructure.brokers.base import (
    BrokerAdapter,
    GapKind,
    IntakeSkipReport,
    Polled,
    StreamGapReport,
)
from seldon.infrastructure.brokers.kafka import KafkaBrokerAdapter

__all__ = [
    "BrokerAdapter",
    "GapKind",
    "IntakeSkipReport",
    "KafkaBrokerAdapter",
    "Polled",
    "StreamGapReport",
    "build_adapters",
]


def build_adapters(
    settings: Iterable[BrokerSettings] | None = None,
) -> list[BrokerAdapter]:
    """Connect to every enabled broker.

    Every chosen broker is read over Kafka.

    Args:
        settings: Broker settings; read from the environment when omitted.

    Returns:
        One subscribed adapter per enabled broker.

    Raises:
        pydantic.ValidationError: If the environment's broker settings are
            invalid.
    """
    chosen = load_broker_settings() if settings is None else settings
    return [KafkaBrokerAdapter(s) for s in chosen if s.enabled]
