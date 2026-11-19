# SELDON Service

## SELDON: Supernove Explosions Learned by Deep Neural Networks

This service turns Rubin alerts, and photometry you give it, into SELDON
products: forecast light curves with uncertainties, class probabilities, and
the model's basis parameters, each with caveats that say whether the request
is inside what the model was trained on. It runs on CPU in ordinary
containers, with the checkpoint baked into the model's image: no GPU, no HPC
allocation, no training data.

The service does not hold a model. It speaks a versioned contract
([packages/seldon-model-contract/SPEC.md](packages/seldon-model-contract/SPEC.md))
to model packages, and each model release runs in a worker image of its own.
Today's pinned SELDON checkpoint is served by the reference package in
`packages/seldon-reference-model/`. To bring a new release, see
**[docs/guides/plugging-in-a-model.md](docs/guides/plugging-in-a-model.md)**.

There is no web interface, REST API, or MCP interface yet. Products on demand
are a Python call, `seldon.services.request_products`, that works only inside
a worker image.

## Running a forecast

See **[docs/guides/running-a-forecast.md](docs/guides/running-a-forecast.md)**
for the call, a worked example you can run against the worker image, the
refusal codes, and the bands the reference package accepts.

In short:

```python
from seldon import services
from seldon_model_contract import Packet, Point

(result,) = services.request_products(
    [Packet("my-object", (
        Point("lsst", "r", 61000.0, 1200.0, 30.0, 31.4),  # survey, band, MJD, flux, flux_err, zero point
        # ...
    ))],
    ["flux_forecast", "classification"],
)
result.outcomes["classification"]  # a Product with caveats, or a Refusal
```

The reference package accepts LSST, Roman WFI, and JWST NIRCam filters only;
see
[docs/solutions/ml-runtime/seldon-servable-bands.md](docs/solutions/ml-runtime/seldon-servable-bands.md).

## Forecasting live Rubin alerts

The `consumer` service reads alerts from the Fink, Lasair, and Babamul
brokers and records which detections of each likely supernova are owed
products. It runs no model. A forecaster (one per deployed model release, see
below) turns owed detections into stored products, and `alert_report` shows
what both did. See
**[docs/guides/running-the-alert-consumer.md](docs/guides/running-the-alert-consumer.md)**
for credentials, broker settings, starting the stack, and reading the reports,
and for capturing broker traffic and replaying it to measure intake offline.

Upgrading an existing deployment from the old in-service model? Follow
"First switch to the contract" in
[docs/guides/plugging-in-a-model.md](docs/guides/plugging-in-a-model.md#first-switch-to-the-contract).

## Building the images

`app/Dockerfile` builds two images, which `docker/docker-compose.dev.yaml`
builds from `app/` with `packages/` as the named build context `packages`:

- **`service`** (`seldon-service:latest`): the Django service, run as `app` and
  `consumer`. It has no torch, no `seldon_core`, and no model package; it
  carries only the standard-library contract package.
- **`worker`** (`seldon-forecaster-reference:latest`): the service plus exactly
  one model package, chosen by the `MODEL_PACKAGE` build argument (a
  directory under `packages/`), with its own pinned torch. It runs
  `python manage.py forecast_worker`, and the wheel's sha256 that the worker
  registers under is recorded in the image.

The worker image installs `seldon_core` from a private GitHub repository, so
building it needs a GitHub token with read access to it. The token is passed
as a BuildKit secret and is not stored in any image layer. The service image
needs no token.

The development profile builds and starts the stack in one step:

```console
$ run/seldonctl slim_dev up           # build and start; reads the token from `gh auth token`
$ run/seldonctl slim_dev logs         # follow the app, consumer, and forecaster logs
$ run/seldonctl slim_dev test         # run the service test suite in the running app container
$ run/seldonctl slim_dev worker-test  # run the model-package tests in the running forecaster
$ run/seldonctl slim_dev down         # stop
```

`up` uses `GH_BUILD_TOKEN` if it is set, and otherwise takes the token from
the `gh` CLI, so you need to be logged in with `gh auth login` first. To build
each image without compose:

```console
$ docker buildx build --target service --build-context packages=packages \
    -t seldon-service app/
$ GH_BUILD_TOKEN=$(gh auth token) docker buildx build --target worker \
    --build-context packages=packages \
    --build-arg MODEL_PACKAGE=seldon-reference-model \
    --secret id=gh_token,env=GH_BUILD_TOKEN \
    -t seldon-forecaster-reference app/
```

## Running a forecaster

Each deployed model release runs as a `forecaster-<name>` compose service
(`forecaster-seldon-reference` for the reference package). It registers its
release on start, then works through the detections that are owed, for the
heads an operator has enabled:

```console
$ docker compose --file docker/docker-compose.yml --file docker/docker-compose.dev.yaml \
    --env-file env/.env.default --env-file env/.env.dev --profile slim_dev \
    exec app python manage.py model_heads list
```

`model_heads enable|disable|retire` change which heads run. `SELDON_FORECASTER_REPLICAS`
sets how many replicas share a forecaster's work, and the `SELDON_WORKER_*`
settings in `env/.env.default` set its batch size, polling, and retry limits.
A second version runs beside the first as a second forecaster service; see
[docs/guides/plugging-in-a-model.md](docs/guides/plugging-in-a-model.md).

Once running, `http://127.0.0.1:8000/healthz` answers 200 while the database
is reachable and 503 when it is not. Its body lists each deployment that is
not retired, with its package version, contract version, and the age of its
last heartbeat, so a forecaster that has stopped shows there.

Keep `SELDON_TORCH_THREADS` (in `env/.env.default`) equal to the container's
CPU limit; the dev profile uses it as the `cpus:` limit. Each forecaster process
needs about 2 GB of memory. See "The thread trap" in
[docs/solutions/ml-runtime/seldon-cpu-viability.md](docs/solutions/ml-runtime/seldon-cpu-viability.md).

## References

Wu, Jiezhong, Jack O'Brien, Jennifer Li, M. S. Krafczyk, Ved G. Shah, Amanda R.
Wasserman, Daniel W. Apley, Gautham Narayan, and Noelle I. Samia. "SELDON:
Supernova Explosions Learned by Deep ODE Networks." In *Proceedings of the AAAI
Conference on Artificial Intelligence*, vol. 40, no. 32, pp. 26922-26930. 2026.
GitHub: <https://github.com/skai-institute/seldon>
