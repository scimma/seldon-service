---
title: LSST broker alerts - what Fink, Lasair, and Babamul actually carry
date: 2026-11-13
module: broker-intake
problem_type: data-contract
tags: [lsst, rubin, brokers, fink, lasair, babamul, kafka, zero-point, latency]
---

# LSST broker alerts - what Fink, Lasair, and Babamul actually carry

Finding from the broker probe that opens `docs/plans/2026-11-12-0629-feat-alert-intake-and-filtering-plan.md`.
Evidence is real alerts from the night of 2026-11-12 (Rubin dayObs 20261112),
read on 2026-11-13. The probe scripts were throwaway and never entered the
repository. Recorded alerts are in `app/seldon/tests/fixtures/broker_alerts/`.

## Verdict

Intake can be built on three free sources:

| Role | Source | Why |
|---|---|---|
| Qualifying | Fink, topic `fink_most_likely_sn_lsst` | Carries reliability, SN-like scores (SuperNNova, CATS), and a light curve with an identifier on every point. |
| Qualifying | Lasair, the service's own filter (topic `lasair_366c0efe0df`) | Carries reliability and a light curve with identifiers; the SN-like signal is Sherlock's host-context class. |
| Full stream | Babamul, every `babamul.lsst.*` topic including `stellar` | Delivered every sampled Fink detection and every Lasair SN/NT object, about 2 minutes after observation, before either qualifying broker. |

None of the plan's stop conditions holds:

- **Qualifying brokers:** two exist, Fink and Lasair.
- **Zero point:** confirmed.
- **CPU:** one process has about three times the forecasting capacity the measured peak needs.

Pitt-Google also qualifies, but it is billed, so it is not used.
AMPEL (no reliability score), ANTARES (light curve only by HTTP), and
ALeRCE (no LSST stream) are not used; see the plan's broker probe section.

## Flux units and zero point

Rubin's schema gives `psfFlux` and `psfFluxErr` in **nJy** for both
DiaSource and DiaForcedSource ([sdm-schemas, APDB](https://sdm-schemas.lsst.io/apdb.html)).
The AB zero point for nJy is 2.5 log10(3631 x 10^9) = **31.4**. AMPEL
independently tags every LSST point with `zp: 31.4, zpsys: "ab"`.

## Field names

### Fink (`fink_most_likely_sn_lsst`)

- **Transport:** Kafka, Avro. The writer schema is JSON in the message key, which `fink-client` reads.
- **Triggering detection:** `diaSource`, carrying `diaSourceId`, `diaObjectId`, `midpointMjdTai`, `band`, `psfFlux`, `psfFluxErr`, `reliability` and `reliabilityVersion` (`0.3`).
- **Previous detections:** `prvDiaSources`, the same fields with `diaSourceId` on every point.
- **Forced photometry:** `prvDiaForcedSources`, with `diaForcedSourceId`.
- **SN-like scores:**
  - `clf.snnSnVsOthers_score` (SuperNNova).
  - `clf.cats_score` and `clf.cats_class` (CATS). The class was `11` on every alert; which label code 11 is has not been confirmed.
  - `clf.earlySNIa_score` is `-1` when not computed. It must never be read as a probability.
- **Distributions in a 300-alert sample:**
  - SuperNNova: 0.715 to 0.97, median 0.807.
  - CATS: 0.90 to 1.0, median 0.987.
  - reliability: 0.504 to 1.0, median 0.908.
- **Light curve size:** a median of 14 previous detections, at most 30.
- **Bands:** `i`, `z` and `r` were seen; the names are plain lower-case letters.
- **Message size:** a median of 115 KB, because each message carries three image cutouts.

### Lasair (service filter, full-alert mode)

- **Transport:** Kafka at `lasair-lsst-kafka.lsst.ac.uk:9092`, JSON, no credentials. The group ID is any string.
- **Filter:**
  - selects `sherlock_classifications.classification IN ('SN', 'NT')` and `objects.latestR >= 0.6`;
  - runs on every new alert, so an object whose class is SN or NT is delivered again at each new detection while its latest reliability passes.
- **Top-level fields:** the selected columns, namely `diaObjectId`, `ra`, `decl`, `classification`, `latestR`, `medianR`, `nDiaSources`, `nSourcesGood`, the first and last detection times, `separationArcsec`, `z` and `photoZ`.
- **Light curve:**
  - `alert.diaSourcesList`: full diaSources with `diaSourceId` and `reliability`.
  - `alert.diaForcedSourcesList`: carries `diaForcedSourceId`.
  - Lasair names the declination `decl`, not `dec`.
- **Lite mode:** Lasair's lite-lightcurve mode drops all point identifiers and must not be used.
- **Triggering detection:** none is named. It is the latest entry of `diaSourcesList`.

### Babamul (all `babamul.lsst.*` topics)

- **Transport:** Kafka at `kaboom.caltech.edu:9093`, SASL_PLAINTEXT with SCRAM-SHA-512. The consumer group must start with `<username>-`. Each message is one Avro container.
- **Triggering detection:** `candidate`, carrying `diaSourceId`, `diaObjectId`, `midpointMjdTai`, `band`, `psfFlux`, `psfFluxErr`, `reliability` and `reliabilityVersion`. Babamul publishes only reliability 0.5 and above.
- **Previous detections** (`prv_candidates`: `band`, `jd`, `psfFlux`, `psfFluxErr`, `ra`, `dec`) and **forced photometry** (`fp_hists`: `band`, `jd`, `psfFlux`, `psfFluxErr`):
  - they carry no identifiers;
  - their time is a **UTC** Julian date. Rubin's MJD TAI is `jd - 2400000.5 + 37 / 86400`. Without the 37 seconds, every converted point is 37 s early.
- **Point identity:** light-curve points are stored keyed on object, band and time rounded to 0.1 s, so the same measurement from Babamul and from a broker that names it lands on one row. Rounding absorbs the remaining microsecond of conversion error.
- **No class score**, only descriptive `properties`.
- **The `stellar` topics must be read.** Of 1,514 Lasair SN/NT objects active that night, Babamul routed 1,129 to its `stellar` topics and 385 to `hosted` or `hostless`. The two `unknown` topics were empty.

## Volumes, latency, and lag (night of 2026-11-12)

| Source | Alerts | Objects | Size | Published after observation |
|---|---|---|---|---|
| Fink `fink_most_likely_sn_lsst` | 3,390 (2,955 of them in the hour from 00:00 UTC) | about 2,000 (estimated from a 300-alert sample) | median 115 KB | median 5.6 min, p90 10.3 min |
| Lasair SN/NT (a public filter matching the service's) | 2,511 | 1,514 | median 32 KB in full-alert mode | median 3.6 min, p90 4.9 min |
| Babamul, all topics | 59,036 | 23,633 | median 18 KB, 1.3 GB in all | median 2.0 min, p99 2.6 min |

- **Babamul delivered all 300 sampled Fink detections**, a median of 3.8 minutes before Fink (p90 8.3 min).
- **Fink and Lasair select mostly different objects:** only 48 of the 173 objects in the Fink sample were Lasair SN/NT.
- **The service's own Lasair filter had no live night yet** when this was written. The figures above come from a public filter with the same cut.

**Reading Fink from this host is bandwidth-bound.** The link gave about 0.11 MB/s.

- **Fetch sizes:** with `max.partition.fetch.bytes` above about 2 MB, fetch requests exceed librdkafka's 60-second timeout and nothing arrives. With 128 KB per partition, reads run at about 0.7 alerts a second.
- **The burst is at that limit:** 2,955 alerts in one hour, at 115 KB each, comes to about 0.09 MB/s, so a burst is read about as fast as it is published.
- **A deployment host should measure its own bandwidth to Fink** (IJCLab, France) before relying on this.

## Forecast cost and the steady-state rate

Single-object forecasts were run inside the service image at one CPU with one
torch thread, on the daily six-band grid that runs from the first observation
to 60 days past the last:

| Points | Span (days) | Grid points | Seconds |
|---|---|---|---|
| 3 | 10 | 426 | 0.10 |
| 30 | 10 | 426 | 0.12 |
| 30 | 365 | 2,556 | 0.30 |
| 256 | 365 | 2,556 | 0.41 |
| 300 | 10 | 426 | 0.26 |
| 300 | 365 | 2,556 | 0.45 |

**Light curves are not truncated.**
- A 300-point request reports 300 observations used.
- `seldon_core`'s 256-point and 400-point limits belong to its training data loader, which the service does not use for inference; the service encodes every usable point itself.

**Steady-state forecast rate:**
- **Last night's actual load:** the union of Lasair SN/NT objects and the sampled Fink objects (1,639 objects) had 4,259 detections in Babamul's stream, 3,474 of them in the peak hour.
- **Scaled to Fink's full night:** about 6,000 to 8,000 forecasts a night, with a peak of 4,000 to 6,000 in an hour.
- **Why this is close to steady state:** both qualifying brokers re-deliver every new detection of the objects they select, so the active qualified set is already counted in these figures.
- **Capacity:** at 0.1 to 0.45 s per forecast, one process handles about 8,000 to 36,000 an hour, so the 10-minute latency target holds with about three times headroom at the measured peak.

**Storage:**
- A stored forecast's grid and outputs take about 25 KB of JSON for a short span and about 150 KB for a 365-day span.
- At about 40 KB typical and 7,000 forecasts a night, that is about 280 MB a night, or roughly 70 GB over a 250-night season.

## Proposed thresholds

| Broker | Reliability cut | SN-like threshold |
|---|---|---|
| Fink | the detection's `reliability` >= 0.6 (drops under 10% of the sample) | `snnSnVsOthers_score` >= 0.5. This never binds: the topic's own cut admitted nothing below 0.715. It is recorded so the value in effect is stored. |
| Lasair | the triggering detection's `reliability` >= 0.6, matching the filter's `latestR` cut | Sherlock class `SN` or `NT`, which is categorical. A matching class can be scored 1.0 against a threshold of 1.0. |
| Babamul | none: the full stream never qualifies a detection | none |

## Client notes

- **`fink-client` 12.3.0:** the `finkctl` Kafka configuration uses only the bootstrap server and group ID. No password is set for LSST livestream credentials.
- **`hop-client` 0.13.0 on Python 3.13:** it needs `six` installed explicitly. This matters only if AMPEL is added later.
- **Avro decoding:** `confluent-kafka` and `fastavro` are enough for Babamul and Fink (Fink with its key-carried schema). Lasair needs only `json`.

## Open after this probe

- **Lasair:** measure the service filter's live latency and nightly volume once it has run through a Rubin night.
- **Fink CATS:** confirm which class `cats_class` 11 denotes.
- **Lasair "SN" objects that Babamul routes to `stellar`:** three quarters of Lasair's SN/NT objects. Sherlock's host-context class was accepted as Lasair's SN-like signal for now and marked for review. This disagreement is the first thing that review should look at.
