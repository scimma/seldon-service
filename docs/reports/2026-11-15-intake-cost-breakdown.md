# Alert intake cost breakdown (2026-11-15)

**Summary.**
- **Why 2026-11-14 fell behind:** the consumer that fell three hours behind Babamul on 2026-11-14 spent most of each alert waiting on the network: a synchronous broker commit per alert, plus a second's wait on each quiet broker per cycle. Those two costs, together with database work and forecasting, account almost exactly for the 9 alerts a second seen live.
- **What batching does:** it cuts broker commits about 23-fold and removes the waits. Database work per alert falls from 11.2 ms to 8.1 ms.
- **What is left:** one batched process should reach about 45 alerts a second live. The target of 150 a second, plus a 25% margin, therefore needs about four or five processes.
- **The Fink limit:** the link to Fink cannot carry twice the 2026-11-14 Fink burst, whatever the consumer does.

## Method

- **Traffic:** every broker's retained traffic for 2026-11-13 12:00 UTC to 2026-11-15 12:00 UTC was copied to local recordings with `capture_alerts`, under its own group ids and without commits.
- **Replays:** maximum-rate replays of the Babamul and Lasair recording ran through the real adapter, consumer loop and engine with `replay_alerts`, under the development compose stack's one-CPU limit and with one torch thread. Each replay went into its own database.
  - **Baseline:** the branch before batching (commit `ceaf0aa`).
  - **Batched:** the branch with batching (commit `2d996be`).
  - Both were stopped after about 15 minutes, which covered the same first 8,600 alerts.
- **Live commit latency:** a replay keeps broker commits in memory, so it cannot show their cost. It was measured separately, with 20 synchronous commits per broker under a throwaway group id.
- **What a maximum-rate replay distorts:** it releases each broker's traffic at once and the loop takes the brokers in turn, so the first alerts are half Lasair. Every Lasair alert qualifies, which makes half the replayed alerts forecasts, against about 6% on a real night. Stage costs per alert are therefore read separately for database work and per forecast, and combined below at the live mix.

## Network links

| Broker | Messages | Bytes | Sustained fetch | Needed at twice the 2026-11-14 peak |
|---|---|---|---|---|
| Babamul | 154,398 | 2.83 GB | 59-85 MB/s | 2.7 MB/s |
| Lasair | 4,874 | 103 MB | 1.5 MB/s | 0.084 MB/s |
| Fink | 3,157 | 592 MB (187 KB a message) | 0.143 MB/s | about 0.19 MB/s |

**Fink's link is the binding external limit.**
- **What it carries:** at 187 KB a message, 0.143 MB/s is about 2,750 alerts an hour.
- **What the target needs:** twice the 2026-11-14 Fink burst is about 3,650 an hour (twice 1,824). An hour-long burst of that size would leave Fink about 20 minutes behind, from the link alone.
- **Today's real burst** fits within the link.
- **The earlier estimate:** the probe's figure of 0.12 MB/s assumed 115 KB a message, which understated what is needed.

| Broker | Synchronous commit, median of 20 |
|---|---|
| Babamul | 72.7 ms |
| Fink | 114.3 ms |
| Lasair | 113.5 ms |

## Stage costs

| Stage | Baseline | Batched |
|---|---|---|
| Alerts in the window | 8,600 | 8,600 |
| Broker commits | 8,600 | 372 |
| Database work, per alert | 11.2 ms | 8.1 ms |
| Forecasting, per forecast | about 195 ms | about 197 ms |
| Decode and parse, per alert | 0.20 ms | 0.20 ms |
| Poll wait in the replay | negligible | negligible |

## Live cost per alert at the 2026-11-14 mix

About 5.5% of alerts were forecast during the 2026-11-14 burst hour.

| Cost per alert | Baseline (live) | Batched (projected) |
|---|---|---|
| Broker commit | 73 ms (one per alert) | about 3 ms (one per partition per batch) |
| Waits on two quiet brokers | about 20 ms (1 s each per 100 alerts) | none |
| Database work | 11 ms | 8 ms |
| Forecasting | about 11 ms | about 11 ms |
| **Total** | **about 115 ms, about 9 a second** | **about 22 ms, about 45 a second** |

**The baseline column reproduces what was observed live:** 9 alerts a second, flat, for three hours on 2026-11-14.

## Consequences

- **Multiple processes are needed.** One batched process falls well short of 190 alerts a second, the target plus a 25% margin, so the plan's multi-process step is required. About four or five processes are needed, each loading about 2 GB of model memory.
- **Forecasting load:** at 150 alerts a second and a 5.5% forecast share, forecasting alone needs about 1.6 CPU-seconds every second.
- **The Fink link:** it is recorded as a known external limit. Fink could lag up to about 20 minutes during an hour-long burst at twice the 2026-11-14 level. Lasair, Babamul and today's actual Fink volume are unaffected.
