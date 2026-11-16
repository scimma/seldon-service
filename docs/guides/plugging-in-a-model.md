# Plugging in a model

This guide takes a model team from cutting a release to its products running
on live Rubin alerts. It is written first for the SELDON team's next release;
it applies to any team that ships a package against the same contract.

The contract itself is in
[`packages/seldon-model-contract/SPEC.md`](../../packages/seldon-model-contract/SPEC.md).
This guide cites it by section and does not repeat it. Read SPEC.md first; it
is short. The worked example throughout is the reference package in
[`packages/seldon-reference-model/`](../../packages/seldon-reference-model/),
which serves today's pinned checkpoint through the contract.

The path, end to end:

1. You build a package that implements `describe` and `predict`.
2. You run the conformance kit in your CI.
3. You hand over a tagged, pip-installable release.
4. The service team builds a worker image for it, the worker registers, and
   the operators replay a recording, compare, enable heads, and run it beside
   the previous version.

## 1. Package structure and the entry point

A package is an ordinary Python distribution. The service imports it; it
never calls a network endpoint of yours. The only dependency you need on the
service side is `seldon-model-contract`, which uses the standard library only,
so it cannot conflict with your torch, numpy, or anything else.

The reference package's layout:

```text
packages/seldon-reference-model/
  pyproject.toml
  src/seldon_reference_model/
    package.py        # the object the entry point names: describe() and predict()
    runtime.py        # model loading, settings, thread pinning
    checkpoints/...   # the model files, shipped inside the package
  tests/
    fixtures/         # golden light curves and their recorded outputs
    test_package.py   # the package through describe() and predict()
```

Register one object under the `seldon.model_packages` entry-point group
(SPEC.md section 2). The reference package's `pyproject.toml` does it like
this:

```toml
[project]
name = "seldon-reference-model"
version = "2.0.0"
requires-python = ">=3.11"
dependencies = ["seldon-model-contract>=1.0,<2", "numpy", "omegaconf", "pyyaml"]

[project.entry-points."seldon.model_packages"]
seldon-reference = "seldon_reference_model.package:ReferencePackage"
```

Two names matter, and they differ:

- The **entry-point name** (`seldon-reference`) is what the conformance kit
  and the worker's `SELDON_WORKER_PACKAGE` use to load your package.
- The **package name** is the `package_name` your `describe()` returns (the
  reference returns its distribution name, `seldon-reference-model`). It is
  what `model_heads`, the reports, and `/healthz` show.

The entry point may name an instance, or a class or factory that the service
calls with no arguments. `ReferencePackage` is a class. Load the model lazily
(the reference package loads it on the first call, once per process) so that
`describe()` stays cheap.

The package provides two calls, whose signatures are `ModelPackage` in
`seldon_model_contract`:

```python
def describe(self) -> Declaration: ...
def predict(self, packets, requests) -> list[ObjectResult]: ...
```

`predict` returns exactly one `ObjectResult` per packet, in order, holding one
outcome per requested head (SPEC.md section 7). Raise only for a failure of
the package itself; a problem with one object is that object's refusal.

## 2. Declaring heads, inputs, output kinds, and accepted pairs

`describe()` returns a `Declaration` (SPEC.md section 4). It names:

- `package_name`, `package_version`, and `contract_version` (use the
  `CONTRACT_VERSION` constant from `seldon_model_contract`);
- `accepted`: every `(survey, band)` pair the package can use;
- `heads`: one `HeadDeclaration` per decoder head.

Survey and band are one identity. A ZTF `g` and an LSST `g` are different
filters (SPEC.md section 3), so declare `("ztf", "g")` and `("lsst", "g")`
separately. The reference package derives its accepted pairs from the
checkpoint's band list; see `accepted_pairs` in
`src/seldon_reference_model/package.py`.

Each head names an output kind and the inputs it takes by default:

```python
from seldon_model_contract import (
    CONTRACT_VERSION, Declaration, HeadDeclaration, InputKind, OutputKind, TimeGrid,
)

Declaration(
    package_name="my-model",
    package_version="1.0.0",
    contract_version=CONTRACT_VERSION,
    accepted=frozenset({("lsst", "g"), ("lsst", "r"), ("ztf", "g")}),
    heads=(
        HeadDeclaration(
            "flux_forecast",
            OutputKind.FLUX_SERIES,
            {
                InputKind.TIMES: TimeGrid(
                    start_days=0.0, days_past_last=60.0, step_days=1.0
                ),
                InputKind.BANDS: (("lsst", "g"), ("lsst", "r")),
            },
            "Forecast flux and uncertainty at requested times and bands.",
        ),
        HeadDeclaration(
            "classification",
            OutputKind.CLASS_PROBABILITIES,
            description="Probability of each transient class.",
        ),
    ),
)
```

- **Output kinds** are `flux_series`, `class_probabilities`, `scalar`, and
  `named_values`; SPEC.md section 6 gives each payload.
- **Inputs** are templates (SPEC.md section 5). The service resolves a
  template for each object and sends your package concrete values: a list of
  MJD times, and a list of `[survey, band]` pairs. A head with no inputs takes
  only the packet. A new kind of input needs a new contract version.
- **Head names are unique.** Declaring a head does not run it. An operator
  enables it later (section 6), and may replace the default inputs.
- **Embeddings are not products.** Latent vectors and other internals stay
  inside the package and never appear in payloads (SPEC.md section 6).

## 3. Refusals and caveats

The package, not the service, decides when to refuse an object and which
caveats to attach, because only the model team knows a release's validated
range. The rules are in SPEC.md:

- section 8 lists the refusal codes and what each means;
- section 9 lists the caveat codes and their `detail` fields;
- section 10 says what to do with points from pairs you do not accept: use
  the accepted points and add an `excluded_points` caveat, or refuse with
  `unknown_band` when none remain;
- section 11 says an unknown code is kept by the service but fails the
  conformance kit, so propose new codes as a minor contract version rather
  than inventing them.

The service stores every refusal and caveat it receives and never strips one.
A product that hides a reason you could not defend is the one thing the
contract forbids (SPEC.md section 9). If an input is outside your validated
range, say so in a caveat or refuse.

## 4. Running the conformance kit in your CI

The kit loads an installed package by entry-point name and checks it against
SPEC.md using sample packets from Rubin, ZTF, and Roman, plus mixed packets
that include points from pairs your package does not accept.

In a clean virtual environment, install the contract distribution and your
package, then run the kit:

```console
$ python -m venv .venv && . .venv/bin/activate
$ pip install /path/to/seldon-service/packages/seldon-model-contract
$ pip install .                       # your package, from its own checkout
$ python -m seldon_model_contract.conformance my-model
```

`my-model` is your entry-point name. Options:

- `--max-batch N`: the largest batch size used to test batch independence
  (default 32).
- `--json`: print the report as JSON instead of text.

Exit codes:

| Code | Meaning |
|---|---|
| 0 | Every check passed. |
| 1 | At least one check failed; the report names each failure. |
| 2 | The package could not be loaded (not installed, wrong entry-point name, or an error on import). |

Make CI fail on a non-zero exit. The kit checks, among other things, batch
independence (SPEC.md section 7), the refusal and caveat vocabularies
(sections 8, 9, and 11), and that points from pairs you do not accept are
excluded or refused with a reason (section 10).

For the reference package, the same check runs in the worker image through
`run/seldonctl slim_dev worker-test`.

Run your own tests as well. The reference package pins golden outputs for
three fixed light curves in `packages/seldon-reference-model/tests/` and checks
them through `predict` at a relative tolerance of 1e-4; do the same for your
release so that a dependency bump cannot move your numbers unnoticed. See
[Regenerating the reference outputs](../developer/regenerating-reference-outputs.md).

## 5. What to hand over

Hand over a **tagged release that `pip` can install**:

- a source tree that builds a wheel with `pip wheel --no-deps`, with the
  entry point in its `pyproject.toml`;
- the model files inside the package (as the reference package ships its
  checkpoint), or a stated way for the worker image to obtain them;
- every dependency your package needs listed in `pyproject.toml`, with the
  heavy ones (torch and the like) named in your hand-off note, because the
  service team installs those in the worker image at an exact version;
- a passing conformance run (and your own test run) for that tag.

**The version never changes after release.** The service records the sha256
of your wheel when it first registers `package_name` at `package_version`.
If it later sees a different wheel under the same name and version, the worker
refuses to start (SPEC.md section 4). A fix is a new version, never a rebuilt
release. This is what lets every stored output say exactly which model made it.

Version numbers are yours to choose. Use a new one for any change that can
move an output, including a dependency bump.

## 6. What the service does next

You do not need to do these steps, but you should know them, because they
are the path from your tag to live alerts. The operator, usually the service
team, does them. Commands run from the repository root.

**Build the worker image.** The reference package lives in this repository
under `packages/`. For a new release, its source goes into `packages/<name>/`
and the image builds with that directory name as `MODEL_PACKAGE`:

```console
$ GH_BUILD_TOKEN=$(gh auth token) docker buildx build --target worker \
    --build-context packages=packages \
    --build-arg MODEL_PACKAGE=<name> \
    --secret id=gh_token,env=GH_BUILD_TOKEN \
    -t seldon-forecaster-<name> app/
```

The worker image holds the service code and exactly one model package, with
its own pinned torch. The build records the wheel's sha256 in the image.
(The `gh_token` secret is for the private `seldon_core` repository, which the
reference package needs; the build requires it today.)

**The worker registers on start.** `python manage.py forecast_worker` loads
the package named by `SELDON_WORKER_PACKAGE` (its entry-point name), registers
its declaration under the image's digest, and begins to work. It refuses to
start on a digest that differs from the one registered for the same name and
version, on a contract major version the service does not support, or for a
retired release. Check it with:

```console
$ docker compose --file docker/docker-compose.yml --file docker/docker-compose.dev.yaml \
    --env-file env/.env.default --env-file env/.env.dev --profile slim_dev \
    exec app python manage.py model_heads list
```

All the later `manage.py` commands in this guide run the same way, in place
of `model_heads list`; `...` below stands for the compose prefix above.

**Replay a recording with the heads seeded, then drain.** A replay shows what
the release does on real traffic without touching live data. `replay_alerts`
only owes detections (it runs no model). With `--seed-heads all` it copies
each registered release that is not retired into the replay database and
enables every declared head there from the epoch, so every owed detection is
due for every head. The worker then drains the replay database; point it at
that database with `DATABASE_NAME`:

```console
... run --rm consumer python manage.py replay_alerts \
    --recording /app/var/recordings/<window> --database seldon_replay_next \
    --mode ordered --seed-heads all
... run --rm -e DATABASE_NAME=seldon_replay_next forecaster-<name> \
    python manage.py forecast_worker --until-idle
```

`forecast_worker --until-idle` exits when nothing is due or leased for that
deployment. Run it once per deployment's image. See
[Running the alert consumer](running-the-alert-consumer.md) for capturing a
recording and for the replay options.

**Compare against a run of the previous version.** `compare_replays` checks
that two replays of the same recording stored the same outcomes, detection by
detection, keyed by package, version, and head. Use it to confirm that the
previous version's outcomes are identical with and without the new version
beside it. Replay the recording and drain the previous version alone first,
before the new release registers (a seeded replay copies every release that is
registered and not retired), into one database. Then do the seeded replay
above, with both versions drained and the same replay options, into another:

```console
... run --rm consumer python manage.py compare_replays \
    --left seldon_replay_prev --right seldon_replay_next
```

It prints matched counts and every difference, and exits non-zero on any
difference. Expect the previous version's outcomes to match and the new
version's own outcomes to appear as differences, since the first run has none
of them. An outcome reported as still due means that deployment was not
drained. `--output-tolerance` compares numbers in product payloads within an
absolute tolerance (default 0, exact); `--json` prints JSON. To see how the
new version's outputs differ from the old for one object, read
`alert_report object <diaObjectId>`, which lists every product, refusal, and
failure by deployment and head.

**Enable heads for live alerts.** A declared head does nothing on live alerts
until an operator enables it. Check outputs from the replay first, then:

```console
... exec app python manage.py model_heads enable <package_name> <head>
... exec app python manage.py model_heads enable <package_name> <head> \
    --version <package_version> \
    --inputs '{"times": {"start_days": 0, "days_past_last": 60, "step_days": 1}, "bands": [["lsst", "r"]]}'
```

`<package_name>` is the name your `describe()` returns. Without `--version`
the command acts on the one deployment of that name that is not retired. A
head covers detections that become owed at or after its `--from` time (default
now); enabling later does not backfill the gap. `model_heads disable` and
`model_heads retire` stop a head or a whole release.

**Run it beside the previous version.** Each deployed version gets its own
forecaster service. `docker/docker-compose.dev.yaml` carries a commented
second forecaster (`forecaster-seldon-next`) to copy: give it its own image,
its own `MODEL_PACKAGE`, and its own `SELDON_WORKER_PACKAGE`. Both forecasters
read the same owed detections, each forecasts each detection at most once, and
every stored output names the package version and contract version that
produced it. `alert_report coverage`, `latency`, and `object` show each
deployment and head separately, and `/healthz` shows each deployment's
heartbeat age.

## 7. Memory and threads

- **Memory:** budget about 2 GB per worker process. The reference package
  measures 1.6 to 1.8 GB resident (the model loads once per process); see
  [docs/solutions/ml-runtime/seldon-cpu-viability.md](../solutions/ml-runtime/seldon-cpu-viability.md).
  Replicas of one forecaster each load the model, so multiply by
  `SELDON_FORECASTER_REPLICAS`. Tell the service team your model's footprint
  in the hand-off note.
- **Threads:** torch's default thread pool is sized to the host's cores, and a
  CPU quota then makes forecasts 15 to 40 times slower. Pin the pool to the
  container's CPU limit. The reference package reads `SELDON_TORCH_THREADS`
  before its first load (`runtime.py`), and the compose files set
  `OMP_NUM_THREADS`, `SELDON_TORCH_THREADS`, and the container's `cpus:`
  limit from one value. If your package uses a thread pool, honour the same
  variable.

## First switch to the contract

This section is for the operator doing the one-time cutover of an existing
deployment from the old in-service model to the reference package. Follow the
order. A fresh database needs none of it: enable heads normally.

Before the cutover, the consumer forecast detections itself. After it, intake
only records which detections are owed, and a forecaster turns them into
outputs, but only for heads an operator has enabled. The steps below keep
detections that become owed between the migration and head enablement from
being missed.

1. **Apply the migrations.** The consumer's entrypoint runs `migrate_locked`
   on start, so starting the new stack does it. To run it by hand:

   ```console
   ... run --rm consumer python manage.py migrate_locked
   ```

   Migration `0005_model_contract_schema` creates the contract's tables; the
   later migrations copy existing forecasts and refusals into them under a
   retired `seldon-pre-contract` deployment and drop the old tables. The time
   migration 0005 was applied is the start of the switch-over: from then on,
   intake owes detections instead of forecasting them.

2. **Start the reference forecaster** so it registers its deployment:

   ```console
   $ run/seldonctl slim_dev up
   ```

   (`run/seldonctl slim_dev up` builds and starts `app`, `consumer`,
   `forecaster-seldon-reference`, and `db`.) Confirm the registration:

   ```console
   ... exec app python manage.py model_heads list
   ```

   It should list `seldon-reference-model 2.0.0` with its three heads
   (`flux_forecast`, `classification`, `basis_parameters`) as `not enabled`.

3. **Read the time migration 0005 was applied.** From the repository root:

   ```console
   ... exec -T app python manage.py shell -c "from django.db.migrations.recorder import MigrationRecorder as R; print(R.Migration.objects.get(app='seldon', name='0005_model_contract_schema').applied.isoformat())"
   ```

   The last line of the output is the time, for example
   `2026-11-16T14:03:21.482910+00:00`. It comes from the `applied` column of
   the `django_migrations` table, row `app = 'seldon'`,
   `name = '0005_model_contract_schema'`. Use it exactly as printed; `--from`
   needs a time with an offset.

4. **Enable each reference head from that time**, so detections owed during
   the switch-over are covered:

   ```console
   ... exec app python manage.py model_heads enable seldon-reference-model flux_forecast --from <time>
   ... exec app python manage.py model_heads enable seldon-reference-model classification --from <time>
   ... exec app python manage.py model_heads enable seldon-reference-model basis_parameters --from <time>
   ```

   Each prints `Enabled <head> for <deployment> from <time>.` The forecaster
   then works through every detection owed since that time.

5. **Check coverage.** `alert_report coverage` shows, per head, how many owed
   detections ended in a product or refusal and how many are still due.
   Watch the "still due" count fall to zero.

Enabling a head later without `--from` starts it at the moment of the command
and leaves detections owed before then without an outcome from that head.
