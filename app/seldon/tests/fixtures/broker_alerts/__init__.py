"""Real broker alerts, recorded from live LSST streams, for parser tests.

Each broker module exposes ``ALERTS``: a list of recorded messages, each a
dict with the ``topic`` it arrived on, the broker's ``kafka_timestamp_ms``
(its publish time), and the decoded ``alert`` exactly as the broker's client
library returns it. Image cutouts are removed; identifiers are kept. Nothing
in them is personal or credential-bearing.

The recordings were made on 2026-11-13 from the night of 2026-11-12 (Rubin
dayObs 20261112). What each broker's messages carry is written up in
``docs/solutions/broker-intake/lsst-broker-alert-fields.md``.
"""

import json
from pathlib import Path
from typing import Any

_HERE = Path(__file__).resolve().parent


def load(broker: str) -> list[dict[str, Any]]:
    """Read one broker's recorded messages.

    Args:
        broker: The recording's file stem, such as ``"fink"``.

    Returns:
        The recorded messages, in recording order.
    """
    with open(_HERE / f"{broker}.json", encoding="utf-8") as handle:
        return json.load(handle)
