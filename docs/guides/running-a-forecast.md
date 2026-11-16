# Running a forecast

This guide shows how to turn a light curve into SELDON products on demand,
with a model worker image. It runs on an ordinary CPU container: no GPU, no
HPC allocation, and no training data. The checkpoint is baked into the image.

The call is a Python function, `seldon.services.request_products`. It works
only inside a worker image, because the service image holds no model: each
model package runs in a worker image of its own. There is no web form, REST
API, or MCP interface yet; the only HTTP endpoint is `/healthz`. Live alerts
reach stored products through the forecast worker instead; see
[Running the alert consumer](running-the-alert-consumer.md).

## Before you start

You need the reference model's worker image, `seldon-forecaster-reference:latest`.
Building it needs read access to the private `seldon_core` repository; see
"Building the images" in the [README](../../README.md). Check that you have it:

```console
$ docker images seldon-forecaster-reference
```

## The call

```python
from seldon import services
from seldon_model_contract import Packet, Point

results = services.request_products(packets, ["flux_forecast", "classification"])
```

It takes a list of `Packet` values, one per object, and the names of the heads
to run, and returns a list of `ObjectResult` values in the same order. Nothing
is stored. The objects run as one batch. The first call in a process loads the
model, which takes a few seconds and about 1.7 GB of memory; later calls in
the same process reuse it.

The package is the one named by `SELDON_WORKER_PACKAGE`, or the only package
installed in the image. Each head runs with the inputs the package declares as
its defaults, resolved for each packet: the reference package's flux forecast
covers the six LSST bands, one point a day, from the first observation to 60
days past the last. To use other inputs, call the package directly through
`seldon_model_contract.load_package`. The call raises:

- `NoModelPackageError` when the image has no usable package (as in the
  service image);
- `ValueError` when a head is not one the package declares, or a packet has
  no points.

### What you supply: `Packet` and `Point`

A packet is one object. Each of its points carries the survey and band
together, so a ZTF `g` is never read as an LSST `g`:

| `Point` field | What it is |
|---|---|
| `survey` | Lower case: `lsst`, `ztf`, `roman`, and so on. |
| `band` | The band in that survey's own spelling, such as `g` or `F062`. |
| `time` | MJD (TAI). |
| `flux`, `flux_err` | Flux and its one-sigma uncertainty, in the units the zero point sets. |
| `zero_point` | The AB magnitude zero point of `flux`, or `None` if unknown (a package that needs it refuses the object). |
| `detected` | `True` (the default) for a detection, `False` for a forced or upper-limit measurement. |

The format is specified in
[SPEC.md](../../packages/seldon-model-contract/SPEC.md), section 3.

### What comes back: `ObjectResult`

`result.outcomes` maps each requested head to one outcome:

- a `Product`, with `kind`, `payload`, and `caveats`;
- or a `Refusal`, with `code`, `message`, and `detail`.

The reference package's three heads:

| Head | Output kind | Payload |
|---|---|---|
| `flux_forecast` | `flux_series` | `times`, `bands`, `flux`, `flux_err` on the requested grid (`flux[k]` is at `times[k]` in `bands[k]`), and `zero_point` (the AB zero point of the returned flux), plus `eval_span_days` and `eval_span_in_time_scales` |
| `classification` | `class_probabilities` | `probabilities`: probability per class; nine classes: `ILOT`, `PISN`, `SLSN`, `SNII`, `SNIIb`, `SNIIn`, `SNIa`, `SNIax`, `SNIb/c` |
| `basis_parameters` | `named_values` | `values`: the decoder's basis parameters `global_offset`, `global_amplitude`, `redshift` |

The latent never leaves the package.

**Read the caveats before trusting a product.** They replace the old regime
indicator. SPEC.md section 9 lists them:

- `sparse_light_curve`: fewer usable observations than the model was trained
  on (`min_points` is 32). The model still forecasts, even from one
  observation, but treat it with caution.
- `untrained_band`: a band in your observations or the evaluation grid
  contributed no training data. Forecasting in it is an extrapolation in
  wavelength.
- `excluded_points`: some points were left out because the package does not
  accept their survey and band pair.

## Worked example

Save this as `request_example.py`. It builds a synthetic supernova-like light
curve of 36 observations in LSST g, r, and i at zero point 27.5, requests two
heads, and then shows a refusal.

```python
import numpy as np

from seldon import services
from seldon_model_contract import Packet, Point, Product

# A synthetic supernova-like light curve: 36 observations, 2 days apart,
# cycling through LSST g, r, i. Flux is at an AB zero point of 27.5.
days = np.arange(36) * 2.0
bands = ["g", "r", "i"] * 12
peak = {"g": 5000.0, "r": 6000.0, "i": 5500.0}
rise, fall, t0 = 3.0, 20.0, 15.0
shape = np.exp(-(days - t0) / fall) / (1 + np.exp(-(days - t0) / rise))
flux = np.array([peak[b] for b in bands]) * shape
flux_err = np.sqrt(np.abs(flux)) + 20.0


def points(zero_point):
    return tuple(
        Point("lsst", b, 61000.0 + float(d), float(f), float(e), zero_point)
        for d, b, f, e in zip(days, bands, flux, flux_err)
    )


good = Packet("supernova", points(27.5))
no_zero_point = Packet("no-zero-point", points(None))
unknown_survey = Packet(
    "unknown-survey",
    tuple(Point("someother", "g", 61000.0 + float(d), 100.0, 10.0, 27.5) for d in days),
)

results = services.request_products(
    [good, no_zero_point, unknown_survey], ["flux_forecast", "classification"]
)

for result in results:
    print(result.object_id)
    for head, outcome in result.outcomes.items():
        if isinstance(outcome, Product):
            codes = [c.code for c in outcome.caveats]
            print(f"  {head}: product ({outcome.kind.value}), caveats {codes}")
        else:
            print(f"  {head}: refused, {outcome.code} - {outcome.message}")

forecast = results[0].outcomes["flux_forecast"].payload
print("grid points:", len(forecast["times"]), "zero point:", forecast["zero_point"])
print(results[0].outcomes["classification"].payload["probabilities"])
```

Run it in the image. The script is fed on standard input, so nothing needs
mounting, and `--network=none` shows the call needs no network access:

```console
$ docker run --rm -i --network=none seldon-forecaster-reference:latest python - < request_example.py 2>/dev/null
```

`2>/dev/null` hides library warnings on standard error. The run takes about
ten seconds, most of it loading the model. The `seldon_core` library also
prints a line beginning `Log Flux is True` to standard output when the model
loads; it is not part of the result. Things to notice in the output:

- The first packet returns a product for both heads. Its 36 observations are
  above the 32-point minimum and every band is trained, so it has no caveats.
- The second packet is refused with `missing_zero_point` for both heads.
- The third is refused with `unknown_band`, naming the pair in `detail`.
- One refused object does not affect the others: each packet gets its own
  outcome for each head.

To use your own photometry, replace the points. To read a file from your
machine, mount its directory read-only, for example `-v "$PWD/data:/data:ro"`,
and open it from `/data` in the script.

## When an object is refused

A refusal is a normal result, not an exception. Its `code` comes from the
contract's vocabulary (SPEC.md section 8); match on the code, not the message
text, which may change.

| `code` | Meaning | What to do |
|---|---|---|
| `missing_zero_point` | A point the package needs has no zero point. | Give the AB zero point of your flux. |
| `unknown_band` | No point is in a survey and band pair the package accepts; `detail.pairs` lists them. | Convert the photometry to a pair the package accepts (see below), or drop those points. |
| `no_usable_observations` | Points are present but none can condition the model. | Check for NaN or infinite values in your input. |
| `incompatible_checkpoint` | The package cannot serve this request with the model it carries. | This is a deployment problem, not a problem with your request. Report it to whoever runs the service. |

If a packet holds some accepted points and some not, the package uses the
accepted ones and adds an `excluded_points` caveat (SPEC.md section 10).

## Which bands

The reference package (`seldon-2.0-roman-elasticc`) accepts 43 survey and band
pairs, from three instruments only:

- **LSST:** `lsst` with `u g r i z y`
- **Roman WFI:** `roman` with `F062 F087 F106 F129 F146 F158 F184 F213`
- **JWST NIRCam:** `jwst` with 29 filters, `F070W` through `F480M` (for
  example `F150W`, `F200W`, `F444W`)

Of these, 12 carry training data: the six LSST bands and Roman `F062 F087
F106 F129 F158 F184`. Roman `F146`, `F213`, and every JWST band are served
but carry the `untrained_band` caveat.

There are no ZTF, ATLAS, Pan-STARRS, Swift, or Johnson-Cousins pairs. Because
survey and band travel together, ZTF `g` photometry is refused with
`unknown_band` rather than silently read as LSST `g`, which the checkpoint
alone could not detect (see
[docs/solutions/ml-runtime/seldon-servable-bands.md](../solutions/ml-runtime/seldon-servable-bands.md)).

Names are matched exactly: `F062` works, `f062` and `R` are refused. The full
table, with indices and training weights, is in the servable-bands note above.

## Performance

Set the torch thread count to the container's CPU limit. The worker image
defaults to one thread (`OMP_NUM_THREADS=1`, `SELDON_TORCH_THREADS=1`). If you
give the container more CPUs, raise both to match, for example
`--cpus=4 -e OMP_NUM_THREADS=4 -e SELDON_TORCH_THREADS=4`. Leaving torch at its
default under a CPU quota makes forecasts 15-40x slower; see "The thread trap"
in
[docs/solutions/ml-runtime/seldon-cpu-viability.md](../solutions/ml-runtime/seldon-cpu-viability.md).

To add a model of your own, see [Plugging in a model](plugging-in-a-model.md).
