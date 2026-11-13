"""Two Fink alerts from ``fink_most_likely_sn_lsst``, Fink's SuperNNova and
CATS supernova selection.

Decoded with ``fink-client`` 12.3.0 (Avro, schema carried in the message key)
and recorded on 2026-11-13. Both carry ``diaSource`` with ``reliability``,
``prvDiaSources`` with ``diaSourceId`` on every point, ``prvDiaForcedSources``
with ``diaForcedSourceId``, and classifier scores under ``clf``:
``snnSnVsOthers_score``, ``cats_score``, ``cats_class``, and
``earlySNIa_score``, which is ``-1`` when Fink did not compute it.
"""

from seldon.tests.fixtures.broker_alerts import load

ALERTS = load("fink")
