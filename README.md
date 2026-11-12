# SELDON Service

## SELDON: Supernove Explosions Learned by Deep Neural Networks

This service serves forecasts from a trained SELDON model. Give it a
transient's photometry and it returns the forecast light curve with
uncertainties, class probabilities, the model's basis parameters and latent,
a flag saying whether the request is inside what the model was trained on,
and a provenance record. It runs on CPU in an ordinary container, with the
checkpoint baked into the image: no GPU, no HPC allocation, no training data.

Today the service is a Python call, `seldon.services.forecast`, inside the
image. There is no web interface, REST API, or MCP interface yet.

## Running a forecast

See **[docs/guides/running-a-forecast.md](docs/guides/running-a-forecast.md)**
for the call, a worked example you can run against the image, the rejection
codes, and the bands this checkpoint knows.

In short:

```python
from seldon import services
from seldon.domain.models.request import ObjectRequest

(result,) = services.forecast([
    ObjectRequest(
        times=[...], flux=[...], flux_err=[...], detected=[...], bands=[...],
        eval_times=[...], eval_bands=[...],
        zero_point=27.5,  # AB zero point of your flux; required
    )
])
result.flux, result.flux_err, result.predicted_class
result.regime.in_training_regime  # check before trusting the numbers
```

The checkpoint knows LSST, Roman WFI, and JWST NIRCam filters only; see
[docs/solutions/ml-runtime/seldon-servable-bands.md](docs/solutions/ml-runtime/seldon-servable-bands.md).

## Building the image

The image installs `seldon_core` from a private GitHub repository, so the
build needs a GitHub token with read access to it. The token is passed as a
BuildKit secret and is not stored in any image layer.

The development profile builds and starts the service in one step:

```console
$ run/seldonctl slim_dev up      # build and start; reads the token from `gh auth token`
$ run/seldonctl slim_dev logs    # follow the app log
$ run/seldonctl slim_dev test    # run the test suite in the running container
$ run/seldonctl slim_dev down    # stop
```

`up` uses `GH_BUILD_TOKEN` if it is set, and otherwise takes the token from
the `gh` CLI, so you need to be logged in with `gh auth login` first. To build
without compose:

```console
$ GH_BUILD_TOKEN=$(gh auth token) docker build \
    --secret id=gh_token,env=GH_BUILD_TOKEN -t seldon-service app/
```

Once running, `http://127.0.0.1:8000/healthz` answers 503 while the model is
loading and 200 once it has loaded, with the library and checkpoint in the
response.

Keep `SELDON_TORCH_THREADS` (in `env/.env.default`) equal to the container's
CPU limit; the dev profile uses it as the `cpus:` limit. See "The thread trap"
in [docs/solutions/ml-runtime/seldon-cpu-viability.md](docs/solutions/ml-runtime/seldon-cpu-viability.md).
