"""Two Babamul alerts from ``babamul.lsst.no-ztf-match.hosted``.

Decoded with ``fastavro`` (one Avro container per message) and recorded on
2026-11-13 from the night of 2026-11-12. ``candidate`` is the triggering
diaSource, with ``diaSourceId``, ``reliability``, and ``midpointMjdTai``.
Previous detections (``prv_candidates``) and forced photometry
(``fp_hists``) carry no identifiers, and their times are UTC Julian dates
(``jd``): Rubin's MJD TAI is ``jd - 2400000.5 + 37 / 86400``. The first alert
has no forced photometry; the second has 21 forced points.
"""

from seldon.tests.fixtures.broker_alerts import load

ALERTS = load("babamul")
