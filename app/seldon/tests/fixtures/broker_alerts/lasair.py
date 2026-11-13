"""Two Lasair messages from the service's own filter, topic
``lasair_366c0efe0df``.

The filter selects Sherlock class ``SN`` or ``NT`` with
``objects.latestR >= 0.6`` and sends full alerts. Recorded on 2026-11-13;
these two were produced when the filter was first saved, so their latest
detections are older than a live alert's would be. Each carries the selected
columns at top level and, under ``alert``, ``diaSourcesList`` (every field,
including ``diaSourceId`` and ``reliability``) and ``diaForcedSourcesList``
(with ``diaForcedSourceId``). Messages are plain JSON.
"""

from seldon.tests.fixtures.broker_alerts import load

ALERTS = load("lasair")
