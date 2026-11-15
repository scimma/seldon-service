# Grounding: alert consumer intake throughput

Extraction only. Quotes are verbatim (... marks omitted text), with file:line pointers as of 2026-11-15. This file is untracked; it is only as durable as the next commit.

## 0. Prior art

- `docs/grounding/alert-intake-and-filtering.md:5-7` (section 1): "Searched tracked files for kafka, hopskotch, scimma, broker, consumer ... No code, config, or dependency hits." That file predates the consumer code.
- `docs/solutions/` (broker-intake, ml-runtime) has no note on intake rate, batching, or the consumer loop. Grep for throughput, batch, poll, latency, capacity, max_messages finds quantitative content only in the plan and the live-check report.

## 1. Consumer loop (app/seldon/management/commands/consume_alerts.py)

`consume_alerts.py:3-7`: "It loads the model once, then polls every enabled broker in turn, hands each delivery to the intake engine, and acknowledges the broker message only after the engine reports its result committed."

`consume_alerts.py:50-54`: `DEFAULT_POLL_TIMEOUT = 1.0`, `DEFAULT_MAX_MESSAGES = 100` ("#: Most alerts taken from one broker in one poll.").

`consume_alerts.py:237-253` (one cycle): sweep and gap checks, then `for broker in brokers:` polls `broker.adapter.poll(self._poll_timeout, self._max_messages)` only `if not broker.queue:`, then `if not self._drain(broker): return False`.

`consume_alerts.py:264-270` (drain, one alert at a time):
```
while broker.queue and not self.stopping:
    polled = broker.queue[0]
    if not self._handle(polled.outcome):
        return False
    broker.adapter.ack(polled)
    broker.queue.popleft()
```
`consume_alerts.py:294-297`: `result = process_delivery(outcome, self._thresholds)`, then `if not result.acknowledge: self._reset_connection()`.

`consume_alerts.py:9-15`: "On a database error the delivery is left unacknowledged and retried ... before anything later from that broker is read: a Kafka consumer does not redeliver within a session, so skipping ahead would lose it."

`consume_alerts.py:209`: one `_Broker` per adapter. The command file has no thread, process pool, or per-broker worker.

## 2. Per-delivery processing and storage (app/seldon/intake/engine.py)

`engine.py:3-10`: "...decides which of the object's detections are now owed a forecast, forecasts each one in its own call, and stores every forecast or refusal in the same transaction as the detection's state change."

`engine.py:12-16` (pending hold): "A detection waits (pending) until its object qualifies on a detection observed at or before it. Every pending detection is held for seven days from its first delivery: ..."

`engine.py:165-168`: `checkpoint = services.loaded_model().checkpoint_id`, then `with transaction.atomic(): return _process(delivery, thresholds, checkpoint)`.

`engine.py:280-281`: "The object's row is locked first, so deliveries of one object from several consumers are applied one at a time."

`engine.py:291-307` (queries per delivery):
```
rubin_object, _ = RubinObject.objects.select_for_update().get_or_create(dia_object_id=delivery.dia_object_id)
detection, _ = Detection.objects.get_or_create(dia_source_id=delivery.dia_source_id, defaults={...})
_record_delivery(delivery, detection)
if delivery.role is BrokerRole.QUALIFYING:
    ...
```
`engine.py:309-318`: replay check via `Forecast.objects.filter(detection=detection, checkpoint=checkpoint).exists()` and `Refusal.objects.filter(...).exists()`.

`engine.py:320-331`: `_owe_pending(...)` when the object is qualified; then `owed = rubin_object.detections.filter(state=Detection.State.OWED).order_by("observed_mjd")` and `for owed_detection in owed: state = _forecast_detection(...)`.

`engine.py:344-345`: "A redelivery from the same broker changes nothing: the first delivery, with its times and points, is kept."
`engine.py:351-369`: `StoredDelivery.objects.get_or_create(detection=detection, broker=delivery.broker, ...)`; on create, `stored.points.add(*_stored_points(...))`.

`engine.py:372-377`: "...The object's stored measurements are read in one query and new ones written in one."
`engine.py:433-436`: `LightCurvePoint.objects.bulk_create(new)`; `LightCurvePoint.objects.bulk_update(filled, ["dia_source_id"])`.

`engine.py:475-495` (`_owe_pending`): one query over the object's PENDING detections with `annotate(first_received=Min("deliveries__received_at"))`, then `detection.save(update_fields=["state"])` per detection.

## 3. Kafka adapter (app/seldon/infrastructure/brokers/kafka.py)

`kafka.py:3-5`: "Offsets are committed only when the caller acknowledges an alert (``enable.auto.commit`` is off), so an alert is never lost between receipt and storage."

`kafka.py:95-101`: `"enable.auto.commit": False`, `"auto.offset.reset": "latest"`.
`app/seldon/config/brokers.py:145`: `max_partition_fetch_bytes: int | None = Field(131_072, gt=0)`

`kafka.py:285`: `messages = self._consumer.consume(num_messages=max_messages, timeout=timeout)`.

`kafka.py:345-370` (ack): "A message can carry several alerts; its offset is committed only when its last alert is acknowledged, so a crash before then redelivers all of them." The commit is `self._consumer.commit(offsets=[TopicPartition(...)], asynchronous=False)`.

## 4. Plan (docs/plans/2026-11-12-0629-feat-alert-intake-and-filtering-plan.md)

`plan.md:16`: "**Means:** A long-running consumer process takes alerts from qualifying brokers and one full-stream source, applies the qualification rule itself, forecasts one detection per call, and stores each result in Postgres (KTD2, KTD3, KTD4)."

`plan.md:18` (stop condition): "Stop and report rather than building around it if ... one process at the measured per-forecast cost cannot meet the 10-minute latency target at U1's estimated steady-state forecast rate."

`plan.md:48`: "**A full-stream source supplies later detections.** SN-score topics only carry detections that pass the broker's own filter, so later detections of a qualified object may never arrive from them."

`plan.md:67` (R15): "The service also consumes at least one source that delivers every LSST detection, so later detections of qualified objects reach it; such a source never qualifies a detection."

`plan.md:108` (success criterion): "90% of forecasts outside a post-outage replay are stored within 10 minutes of the broker publishing the delivery that triggered them..."

`plan.md:128` (deferred): "A watch-list or reduced-volume full-stream source, if U1 or the first weeks of running show the full stream is too heavy for one process."

`plan.md:171` (pre-U1 probe): "Read speed: reading from Fink was slow, with fetch timeouts and 15 to 30 alerts in 90 seconds ... U1 measures sustained throughput against the latency target."

`plan.md:212` (KTD2, fragment): "One process polls every broker in a synchronous loop; inference stays synchronous, as in the foundation plan."

`plan.md:215` (KTD5, fragment): "A forecast or refusal is committed before the broker message is acknowledged, so a crash in between causes redelivery, never a second forecast."

`plan.md:331` and `:334` (U1): measure "the full-stream source's message rate over one night"; "Record whether one process at the measured per-forecast cost meets the 10-minute latency target at that rate..."

`plan.md:314`: "**Two processes each hold a model.** `app` and `consumer` each load the checkpoint (about 2 GB resident apiece), so the compose host needs memory for both."

`plan.md:443-444` (U6): "2. Loop over the configured adapters, pass each delivery to the engine, and acknowledge only what the engine reports committed."

The plan records no measured message rate or per-forecast cost; those are U1 outputs.

## 5. Live-check report (docs/reports/2026-11-15-live-check-night-2026-11-14.md)

`report.md:8`: "**The consumer could not keep up with Babamul.** Babamul sent almost twice the alerts of any night measured so far. The consumer read them at about 9 a second, fell up to three hours behind, and the delay spread to Fink."

`report.md:87-91` (publication to receipt): Babamul median 106 min, p90 172, max 185; Fink median 7.1 min, p90 15.4; Lasair median 3.3 min, p90 4.9.

`report.md:105`: "**The consumer's intake ceiling:** from 01:00 to 04:00 UTC it took in an almost constant 32,000 to 33,000 Babamul alerts an hour, about 9 a second. That flat rate is the limit of the single consumer process."

`report.md:106`: "**Babamul published faster than that:** ... reached the consumer a median of 106 minutes after publication. The backlog cleared at about 05:36."

`report.md:107`: "**Forecasting is not the limit.** 6,433 forecasts at 0.1 to 0.45 seconds each is under an hour of work over the night. The cost is in reading each alert and storing its light-curve points."

`report.md:110`: "**Fink:** all brokers are read in one loop, so Fink's alerts also waited."

`report.md:147-150` (follow-up 1): "**Consumer throughput.** ... Raise intake well above 9 alerts a second, so a busy night cannot delay the qualifying brokers. Options:" "**Separate process:** read the full stream in its own consumer process." "**Cheaper storage:** make storing each Babamul alert cheaper, for example by inserting in batches." "**Fewer points stored:** keep light-curve points only for objects that could still qualify."

## 6. Compose resources

`docker/docker-compose.yml:29-41` (base `consumer`): "Alert intake: reads the brokers and stores forecasts. Same image and thread budget as app; it loads the model once and polls synchronously." `restart: unless-stopped`; `stop_grace_period: 30s`; no `cpus`, `mem_limit`, `deploy`, `replicas`, or `scale` key.

`docker/docker-compose.dev.yaml:43-48` (dev override): `cpus: ${SELDON_TORCH_THREADS:-1}`.

`env/.env.default:12`: "compose service uses it as its `cpus:` limit and as OMP_NUM_THREADS."

## 7. Not found

- No batch-size setting for consume, no async commit, no worker pool, and no second process for the same broker in the code or docs.
- No per-night throughput requirement in `docs/` beyond the live-check follow-up and the plan's 10-minute latency criterion.
- Not read by instruction: `env/.env.dev` and `docker/secrets/`. Docker was not run.
