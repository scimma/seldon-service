# Alert intake throughput acceptance (2026-11-15)

**Summary.**
- **Result:** four batched consumer processes kept every broker within the 10-minute lag target through the 2026-11-14 burst replayed at 2.1 times its real rate, which is the sizing target of twice the peak.
- **Worst lag:** 70 s, for Babamul. At the 90th percentile every broker stayed under 30 s.
- **Correctness:** every replayed alert was stored exactly once. No detection was forecast twice and none was left owed.
- **Not covered:** the Fink link limit recorded in the [cost breakdown](2026-11-15-intake-cost-breakdown.md). Replays read Fink from a local recording, so they cannot show it.

## Method

- **Traffic:** the local recording of all three brokers for 2026-11-13 12:00 UTC to 2026-11-15 12:00 UTC, narrowed to publication times 2026-11-14 23:30 UTC to 2026-11-15 02:30 UTC. That window holds the burst that left the live consumer three hours behind.
- **Pacing:** each alert was released at its recorded publication time divided by 2.1, so the three-hour window ran in about 77 minutes and the burst arrived at a little over twice its real rate.
- **Processes:** four `replay_alerts` processes ran from the branch head (commit `f5b79f7`), with `--process-index 0..3 --process-count 4`, under the development compose stack. They shared the recording's partitions the way four live consumers share a group's partitions, and wrote to one database.
- **Lag:** lag is measured on the replay clock, from each alert's publication time to the commit that stored its work. It is read with `alert_report intake-lag` from the intake-lag record.
- **Broker commits:** a replay keeps broker commits in memory. The batched path makes about one synchronous commit per partition per batch, about 3 ms per alert at the live commit latency, so this is a small gap.

## Results

| Process | Alerts | Babamul | Lasair | Fink |
|---|---|---|---|---|
| 0 | 16,046 | 14,278 | 1,106 | 662 |
| 1 | 19,637 | 17,942 | 1,020 | 675 |
| 2 | 74,474 | 72,970 | 1,091 | 413 |
| 3 | 2,244 | 290 | 1,541 | 413 |
| **Total** | **112,401** | **105,480** | **4,758** | **2,163** |

Process 2 held Babamul's `no-ztf-match.stellar` partition, which carries most of Babamul's traffic. Even so, it kept up.

| Broker | Alerts | Median lag | 90th percentile | Maximum | Within 10 min |
|---|---|---|---|---|---|
| Babamul | 105,480 | 0 s | 19 s | 70 s | 100.0% |
| Fink | 2,163 | 2 s | 18 s | 41 s | 100.0% |
| Lasair | 4,758 | 11 s | 28 s | 57 s | 100.0% |

| Invariant | Value |
|---|---|
| Deliveries stored | 112,401 (one per replayed alert) |
| Detections | 105,480 |
| Forecasts | 6,214 |
| Detections forecast more than once | 0 |
| Detections still owed a forecast | 0 |
| Refusals, intake skips, stream gaps | 0, 0, 0 |

## Limits of this check

- **Single partition:** Babamul puts each topic's data on partition 0, so one process carries most of the burst. More processes will not raise the ceiling on that topic. Batching is what keeps the one process fast enough. If Babamul's traffic grows beyond twice the 2026-11-14 burst, this is the first limit it will reach.
- **Fink's link:** this is a property of the network path, not of the consumer, and remains as recorded in the cost breakdown.
- **Live confirmation:** the success criteria still need confirming on a real night after deployment, with `alert_report intake-lag`.
