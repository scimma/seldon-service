# Running the alert consumer

The alert consumer is a long-running process that reads Rubin alerts from
broker streams. It decides which detections are likely supernovae, forecasts
each one with SELDON, and stores the result in the service's Postgres
database. This guide covers:

- **Setup:** where the credentials go, how the brokers are configured, and how to start it.
- **Reports:** how to read the record of what it did.
- **Capture and replay:** how to copy broker traffic to local files and replay it into a separate database to measure intake.

Nothing the consumer produces leaves the service. Forecasts are stored only so
the team can inspect them now and score them later.

## How it works

The consumer reads two kinds of stream:

| Role | Broker | What it sends |
|---|---|---|
| Qualifying | Fink, topic `fink_most_likely_sn_lsst` | Rubin's reliability score, SuperNNova's SN-vs-others score, and the light curve |
| Qualifying | Lasair, the service's own filter | Reliability, Sherlock's host-context class (`SN` or `NT`), and the light curve |
| Full stream | Babamul, every `babamul.lsst.*` topic | Every detection, with reliability and the light curve, but no class score |

**How a detection is handled:**

- **It qualifies** when one qualifying broker's reliability score and SN-like score both meet that broker's own cut and threshold.
- **After that, every later detection of the same object is forecast**, whichever broker delivers it and whatever its scores. This is what the full stream is for.
- **Detections that arrive before their object qualifies are held for seven days.** They are forecast if the object qualifies within that time, and recorded as missed if it qualifies too late.
- **A forecast uses the light curve carried by the alert that delivered the detection**: the detection itself and the earlier detections in that alert. Forced photometry is left out.
- **The forecast grid** covers all six LSST bands, one point a day, from the first detection to 60 days past the latest.
- **Each detection is forecast once per checkpoint**, however many brokers deliver it.

The findings behind these choices are in
[docs/solutions/broker-intake/lsst-broker-alert-fields.md](../solutions/broker-intake/lsst-broker-alert-fields.md).

## Before you start

- **The image:** you need `seldon-service:latest` (see "Building the image" in the [README](../../README.md)).
- **Credentials:** you need credentials for each broker you enable.
  - **Fink:** register at https://forms.gle/2td4jysT4e9pkf889. Fink's reply gives a username, a group ID, and a bootstrap server.
  - **Babamul:** a Babamul account's Kafka username and password.
  - **Lasair:** no Kafka credentials. You need the topic of a filter you own on https://lasair.lsst.ac.uk, saved in full-alert mode. Lite mode drops the identifiers intake needs.

## Where the credentials go

Every broker setting is an environment variable named
`SELDON_<BROKER>_<SETTING>`:

- **Non-secret defaults** are in `env/.env.default`, which is committed.
- **Credentials go only in `env/.env.dev`**, which git ignores.
- **An empty value means "use the default".**

Put this in `env/.env.dev`, with your own values:

```shell
SELDON_FINK_ENABLED = true
SELDON_FINK_BOOTSTRAP_SERVERS = <host:port from Fink>
SELDON_FINK_GROUP_ID = <group id from Fink>
SELDON_FINK_USERNAME = <username from Fink>

SELDON_LASAIR_ENABLED = true
# SELDON_LASAIR_TOPICS defaults to the service's filter; set it if you use another.

SELDON_BABAMUL_ENABLED = true
SELDON_BABAMUL_USERNAME = <Babamul username>
SELDON_BABAMUL_PASSWORD = <Babamul password>
SELDON_BABAMUL_GROUP_ID = <Babamul username>-seldon-consumer
```

**Rules for these values:**

- **Babamul group ID:** it must start with the username and a hyphen. Babamul refuses any other.
- **Secrecy:** the servers, group IDs, usernames, and passwords are held as secrets. They never appear in logs or in a printed settings object.
- **Enabling brokers:** each broker is off until its `ENABLED` is `true`.

## Broker settings

| Setting (after `SELDON_<BROKER>_`) | Meaning | Defaults |
|---|---|---|
| `ENABLED` | Read this broker at all | `false` for every broker |
| `ROLE` | `qualifying` or `full_stream` | Fink and Lasair `qualifying`, Babamul `full_stream` |
| `TOPICS` | Comma-separated topics; an entry starting with `^` is a regular expression | see `env/.env.default` |
| `RELIABILITY_CUT` | Lowest reliability score that qualifies | Fink and Lasair 0.6 |
| `SN_LIKE_THRESHOLD` | Lowest SN-like score that qualifies | Fink 0.5 (SuperNNova); Lasair 1.0 (`SN` or `NT` counts as 1.0, any other class as 0.0) |
| `MAX_PARTITION_FETCH_BYTES` | Kafka fetch size per partition | Fink 131072, because Fink's messages are large and larger fetches time out on slow links |

**Intake timing** (`SELDON_INTAKE_*`):

- **The silence period** (`SILENCE_MINUTES`, default 60) does two jobs:
  - **Suspected gaps:** a broker that delivers nothing for longer than it during the Rubin night is recorded as a suspected gap. The night is `NIGHT_START_UTC_HOUR` (default 23) to `NIGHT_END_UTC_HOUR` (default 11), in UTC.
  - **Replays:** a delivery received longer than this after its broker published it counts as a replay, and is left out of the latency figure.

**Intake batching** (`SELDON_INTAKE_BATCH_SIZE`, default 100):

- **What it does:** the consumer stores up to this many consecutive alerts from one broker in one database transaction, then commits that broker's read position once per partition.
- **Its upper bound:** a batch never holds more alerts than one poll returns (`consume_alerts --max-messages`, also 100 by default).
- **Why it matters:** a synchronous broker commit per alert, and a wait on each quiet broker, were most of the live cost per alert before batching. Brokers are now polled without waiting, and the consumer waits once only after a cycle in which no broker returned anything. The measurements are in [docs/reports/2026-11-15-intake-cost-breakdown.md](../reports/2026-11-15-intake-cost-breakdown.md).

**Consumer processes** (`SELDON_CONSUMER_PROCESSES`, default 1):

- **What it does:** compose starts this many `consumer` replicas in the same consumer groups, and Kafka spreads each broker's partitions among them.
- **Memory:** each process loads the model, about 2 GB. Check the host's memory before raising the count.
- **CPU:** each replica has its own CPU limit equal to `SELDON_TORCH_THREADS`. Keep the two equal; see [docs/solutions/ml-runtime/seldon-cpu-viability.md](../solutions/ml-runtime/seldon-cpu-viability.md).
- **Shared work:** replicas apply migrations one at a time under a Postgres advisory lock (`migrate_locked`). The pending sweep and the silence check run in only one process at a time, under another advisory lock.
- **How many:** one batched process is expected to take about 45 alerts a second. The intake target of 150 a second, with a margin, needs about four or five.

**Stage timing** (`consume_alerts --timing-seconds`, default 300): the consumer logs the wall time spent in each intake stage (poll wait, decode, parse, database work, forecasting, broker commit) at this interval and once at shutdown.

### Changing a threshold

Thresholds are read when the consumer starts. To change one:

1. Edit the variable in `env/.env.dev`.
2. Restart the consumer:

   ```console
   $ run/seldonctl slim_dev down
   $ run/seldonctl slim_dev up
   ```

**What a change affects:**

- **Only alerts processed after the restart.** Detections already qualified are not re-judged.
- **Every forecast stores the thresholds in effect when it was made**, along with its object's qualifying event (broker, scores, cut, and threshold). So you can always see which settings produced a forecast.

## Starting and stopping

```console
$ run/seldonctl slim_dev up      # builds the image, starts app, db, and consumer
$ run/seldonctl slim_dev logs    # follows app and consumer logs
$ run/seldonctl slim_dev down
```

**On start**, the consumer:

1. applies database migrations;
2. loads the model once;
3. starts polling every enabled broker.

If the model cannot be loaded, it exits before reading any alert.

**While it runs:**

- **Acknowledgement:** it acknowledges a message only after the forecast, refusal, or skip is committed, with the rest of its batch. A crash or restart therefore causes the broker to redeliver the message, never to produce a second forecast.
- **Database errors:** it retries the same alert, with backoff of up to a minute, before reading anything newer from that broker.
- **Stopping:** SIGTERM (as sent by `down`) stops it after the batch in hand.
- **Restarts:** the `consumer` service restarts unless you stop it.

**On first start**, each broker is read from its newest message onward. Earlier history is not replayed and is not counted as a gap.

## Reading the reports

`alert_report` is read-only. Run it inside the stack:

```console
$ docker compose --file docker/docker-compose.yml --file docker/docker-compose.dev.yaml \
    --env-file env/.env.default --env-file env/.env.dev --profile slim_dev \
    exec consumer python manage.py alert_report <subcommand> [options]
```

**Nights** run from 12:00 to 12:00 UTC, as Rubin's dayObs does, and are named
by the date they start. Every subcommand takes `--json`, which prints JSON
instead of text.

### One object's forecasts

```console
... alert_report object 170019696011640884
```

**What it lists:** every forecast and refusal for that Rubin `diaObjectId`, in observation order. For each one:

- the `diaSourceId` and observation time;
- the checkpoint that produced it;
- the regime mark, including `sparse` when the light curve is shorter than the checkpoint's training minimum;
- the predicted class;
- the brokers that delivered it.

It also shows the object's qualifying event.

### Coverage per broker per night

```console
... alert_report coverage --from-night 2026-11-12 --to-night 2026-11-13
```

**What it counts:** for each night and broker, the detections that broker delivered and that were owed a forecast. They are split by how they ended:

- **forecast** or **refused**: the forecast call ran;
- **missed**: delivered more than seven days before the object qualified;
- **owed**: not finished yet.

**What it reports:** the share that ended forecast or refused. An "all" row counts each detection once.

### Latency

```console
... alert_report latency --percentile 90 --threshold-minutes 10
```

**What it measures:** the time from the triggering broker's publication to the stored forecast.

- **Triggering delivery:** for a qualifying detection, the qualifying broker's delivery. For a later detection of a qualified object, its first delivery.
- **Reported:** the chosen percentile and the share within the threshold, overall and per broker.
- **Left out of the percentile and counted separately:** replays, deliveries without a publish time, and re-forecasts after a checkpoint change.
- **A known overstatement:** a later detection whose first delivery came before its object qualified is measured from that earlier delivery. Its latency includes the wait for qualification.

### Gaps

```console
... alert_report gaps
```

**What it lists:** stream gaps per broker, with their start and end, and the time since each broker's last delivery. A broker that has gone quiet shows here even before a gap is recorded.

There are two kinds of gap:

- **Confirmed:** the broker's retention passed the consumer's last committed position, so alerts were lost.
- **Suspected:** the broker was silent for longer than the silence period during the Rubin night.

### Intake skips

```console
... alert_report skips --from-night 2026-11-12 --list
```

**What it lists:** counts of intake skips per broker, night, and reason. With `--list`, each skip is printed with its identifier.

**What a skip is:** an alert that could not become a forecast request. It may be missing a required field or carry no usable light curve, or its light curve may span more than 400 days. A skip is not a low score, and a broker whose message format changes shows up here instead of looking quiet.

**What a refusal is, by contrast:** the model itself declined the request, for example because of a band it does not know. Refusals appear in the object and coverage reports.

### Intake lag

```console
... alert_report intake-lag --from-night 2026-11-14
```

**What it measures:** the time from a broker publishing an alert to the consumer committing its work, for every alert taken in, not only those that were forecast.

**What it reports**, per broker and per publication night:

- the alerts measured;
- the median, 90th percentile and maximum lag, to one second. Lags of an hour or more share one bucket, shown as `>1h`;
- the share within 10 minutes;
- the alerts without a publication time, counted apart under the night they were received.

**Why it differs from the latency report:** latency covers forecasts only, from the triggering delivery. Intake lag shows whether the consumer as a whole is keeping up with each broker. Its counts are kept after the pending sweep deletes old deliveries.

## Capturing and replaying broker traffic

Intake can be measured offline. `capture_alerts` copies a window of each
broker's retained traffic to local files, and `replay_alerts` feeds a copy
through the real adapter and intake loop into a separate database. Two
replays can then be compared with `compare_replays`.

**Safety rules:**

- **Own group ids, no commits:** a capture reads each broker under a capture group id of its own (Babamul's still starts with the username) and commits nothing, so the live consumer groups are untouched.
- **No network in a replay:** a replay never opens a broker connection, and its commits are kept in memory.
- **Never the configured database:** a replay refuses a `--database` equal to the configured one, and refuses one that exists unless `--replace` is given.
- **Run heavy replays outside Rubin night hours (23:00 to 11:00 UTC), or in a separate stack.** A replay shares the host and the Postgres server with live intake, so a maximum-rate replay during the night slows the live consumer.

The commands below use `run --rm`, so each one gets a container of its own
with the consumer's CPU limit instead of sharing a live consumer's:

```console
$ docker compose --file docker/docker-compose.yml --file docker/docker-compose.dev.yaml \
    --env-file env/.env.default --env-file env/.env.dev --profile slim_dev \
    run --rm consumer python manage.py <command> [options]
```

### Capturing

```console
... capture_alerts --from 2026-11-13T12:00 --to 2026-11-15T12:00
```

**What it does:** reads every partition of each enabled broker's topics from the first message at or after `--from` until `--to`, or until the end of what the broker held when the capture started. `--brokers` limits it to some of them.

**Where it writes:** `/app/var/recordings/<window>` by default (`app/var/recordings/` on the host, which git ignores), or `--output`. A directory that already holds a recording is refused unless `--replace` is given.

**What it logs:** per broker, the messages, bytes, time taken, and sustained fetch rate in MB/s beside the rate the broker needs at the intake target. A broker read slower than that is logged as a warning.

**Retention limits the window.** A capture holds only what the brokers still retain, so capture a night soon after it ends.

### Replaying

```console
... replay_alerts --recording /app/var/recordings/<window> --database seldon_replay_a --mode max
```

**What it does:** loads the model, creates and migrates the named database on the same server, and replays the recording into it. Every recorded broker is replayed with its configured parser, role and thresholds, whether or not it is enabled live. `--brokers` replays only some of them.

**Modes** (`--mode`):

- **`paced`** (the default): messages are released on a replay clock at their recorded publication times, sped up by `--speed` (default 1).
- **`max`**: every message is released at once, to find the ceiling. The loop takes the brokers in turn, so the first alerts are a different mix from a real night's.
- **`ordered`**: messages are released one broker run at a time in publication order, each run only after the one before is committed. Use it when two replays' outcomes are to be compared.

**A window of the recording** (`--from`, `--to`): replays only the messages published in that window, and starts the replay at `--from`, so a replay of a busy hour does not first wait through the hours before it. Give each time with its offset, for example `2026-11-14T03:00+00:00`.

**What it prints:** the alerts taken, the wall time and alerts a second, in total and per broker, then the time spent in each intake stage.

### Replaying with several processes

Several replay processes can replay one recording into one database, as
several consumer processes share the live partitions. Give each the same
`--process-count` and its own `--process-index`:

```console
... replay_alerts --recording /app/var/recordings/<window> --database seldon_replay_b \
    --mode max --process-count 4 --process-index 0
... replay_alerts --recording /app/var/recordings/<window> --database seldon_replay_b \
    --mode max --process-count 4 --process-index 1
```

**How the work is split:** each process takes its share of the recorded partitions, so together they release every message once.

**Who creates the database:** process 0 creates and migrates it (honouring `--replace`). The others wait up to five minutes for it to exist and be fully migrated, so start process 0 first.

**Memory and CPU:** each process loads the model (about 2 GB) and has its own CPU limit, as live consumer processes do.

### Comparing two replays

```console
... compare_replays --left seldon_replay_a --right seldon_replay_b
```

**What it checks:** two replays of the same traffic should forecast or refuse the same detections from the same light curves. For each detection it compares the outcome, and for each forecast the light curve, zero point, grid, outputs, regime, thresholds and qualifying event; for each object, whether and how it qualified. Fields that differ by design, such as storage times, are left out.

**Options:** `--output-tolerance` compares numbers in model outputs within an absolute tolerance (default 0, exact). `--json` prints JSON instead of text.

**It is read-only:** both databases are opened with read-only transactions.
