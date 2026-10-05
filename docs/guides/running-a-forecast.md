# Running a forecast

This guide shows how to turn a light curve into a SELDON forecast with the
`seldon-service` image. It runs on an ordinary CPU container: no GPU, no HPC
allocation, and no training data. The checkpoint is baked into the image.

What exists today is a Python call inside the image. There is no web form,
REST API, or MCP interface yet; the only HTTP endpoint is `/healthz`.

## Before you start

You need the image `seldon-service:latest`. Building it needs read access to
the private `seldon_core` repository; see "Building the image" in the
[README](../../README.md). Check that you have it:

```console
$ docker images seldon-service
```

## The call

There is one call:

```python
from seldon import services
from seldon.domain.models.request import ObjectRequest

results = services.forecast([request_1, request_2, ...])
```

It takes a list of `ObjectRequest` values, one per object, and returns a list
of `ObjectForecast` values in the same order. The objects run as one batch.
The first call in a process loads the model, which takes a few seconds and
about 1.7 GB of memory; later calls in the same process reuse it.

It is plain Python. You do not need to set up Django to call it.

### What you supply: `ObjectRequest`

| Field | What it is |
|---|---|
| `times` | Observation times, in days. Any origin; order does not matter. |
| `flux` | Observed flux, at the zero point you give in `zero_point`. |
| `flux_err` | One-sigma flux uncertainty, same units as `flux`. |
| `detected` | Per observation: `1`/`True` if detected, `0`/`False` if not. |
| `bands` | Per observation: the band name (see "Which bands" below). |
| `eval_times` | Times you want the forecast at, in days, same origin as `times`. |
| `eval_bands` | The band to forecast in at each of `eval_times`. |
| `zero_point` | The AB magnitude zero point of `flux`. Required; see below. |

`times`, `flux`, `flux_err`, `detected`, and `bands` must all have the same
length, and `eval_times` and `eval_bands` must match each other. A mismatch
raises a plain `ValueError` when you build the `ObjectRequest`, before any
forecast runs. Lists and numpy arrays both work.

`zero_point` has no default, because guessing it silently scales every
forecast. Give one of:

- a float: one AB zero point for the whole object, for example `27.5` or `25.0`;
- an array with one zero point per observation;
- `ZeroPointDeclaration.AT_TRAINING_ZERO_POINT` (from
  `seldon.domain.models.request`), to state that your flux is already at the
  model's training zero point of 27.5 mag (`TRAINING_ZERO_POINT_MAG`).

Leaving it out is refused with the code `missing_zero_point`.

An observation is used only if its time, flux, flux error, and (when you gave
one per observation) zero point are all finite. Other observations are
dropped silently; `regime.n_observations` tells you how many were used.

### What comes back: `ObjectForecast`

Values are numpy arrays and plain Python; no torch tensors.

| Field | What it is |
|---|---|
| `eval_times`, `eval_bands` | Your evaluation grid, in your order. |
| `flux`, `flux_err` | Forecast flux and one-sigma uncertainty on that grid: `flux[k]` is at `eval_times[k]` in `eval_bands[k]`. |
| `zero_point_mag` | The AB zero point `flux` is expressed in (see below). |
| `predicted_class` | The most probable class name. |
| `class_probabilities` | Probability per class, by name; sums to 1. Nine classes: `ILOT`, `PISN`, `SLSN`, `SNII`, `SNIIb`, `SNIIn`, `SNIa`, `SNIax`, `SNIb/c`. |
| `parameters` | The decoder's basis parameters: `global_offset`, `global_amplitude`, `redshift`. |
| `latent_mean`, `latent_sigma` | The 6-dimensional latent the forecast is decoded from, and its per-dimension standard deviation. |
| `regime` | Whether this request is inside what the model was trained on. **Read it before trusting the numbers.** |
| `provenance` | Which library, checkpoint, zero point, and torch version produced the values. |

`zero_point_mag` is the zero point you gave if you gave a single float.
Otherwise (a per-observation array, or the training-zero-point declaration)
it is 27.5. Always read it rather than assuming: forecast fluxes at different
zero points are not comparable.

**The regime indicator** (`regime`) has these fields:

- `in_training_regime`: `False` if the forecast is sparse or uses an
  untrained band. This is the one to check.
- `sparse`: fewer usable observations than `min_points` (32). The model never
  trained on light curves this short. It still returns a forecast, even from a
  single observation, but treat it with caution.
- `n_observations`, `min_points`: the usable count and the threshold.
- `untrained_bands`: bands in your observations or evaluation grid that
  contributed no training data. The model has their transmission curves and
  will forecast in them, but that is an extrapolation in wavelength.
- `eval_span_days`, `eval_span_in_time_scales`, `time_scale_days`: how long
  your evaluation grid is, in days and in units of the training time scale
  (20 days). Reported for your judgment; these never clear
  `in_training_regime` on their own.

**The provenance record** (`provenance`) has `library_version` (the
`seldon_core` version), `checkpoint` (directory and file), `zero_point_mag`
(the model's flux scale, 27.5), and `torch_version`. Keep it with any result
you save; a different library, checkpoint, or torch build can change the
numbers.

## Worked example

Save this as `forecast_example.py`. It builds a synthetic supernova-like light
curve of 36 observations in LSST g, r, and i at zero point 27.5, forecasts r
and g at three later times, then shows what a refusal looks like.

```python
import numpy as np

from seldon import services
from seldon.domain.errors import SeldonRejection
from seldon.domain.models.request import ObjectRequest

# A synthetic supernova-like light curve: 36 observations, 2 days apart,
# cycling through LSST g, r, i. Flux is at an AB zero point of 27.5.
times = np.arange(36) * 2.0
bands = ["g", "r", "i"] * 12
peak = {"g": 5000.0, "r": 6000.0, "i": 5500.0}
rise, fall, t0 = 3.0, 20.0, 15.0
shape = np.exp(-(times - t0) / fall) / (1 + np.exp(-(times - t0) / rise))
flux = np.array([peak[b] for b in bands]) * shape
flux_err = np.sqrt(np.abs(flux)) + 20.0

request = ObjectRequest(
    times=times,
    flux=flux,
    flux_err=flux_err,
    detected=[1] * 36,
    bands=bands,
    eval_times=[75.0, 80.0, 85.0, 75.0, 80.0, 85.0],
    eval_bands=["r", "r", "r", "g", "g", "g"],
    zero_point=27.5,
)

(result,) = services.forecast([request])

print("forecast on the requested grid (flux at zero point", result.zero_point_mag, "):")
for t, b, f, e in zip(result.eval_times, result.eval_bands, result.flux, result.flux_err):
    print(f"  t={t:5.1f}  {b}  flux={f:8.1f} +/- {e:6.1f}")
print("predicted class:", result.predicted_class)
print("class probabilities:")
for name, p in sorted(result.class_probabilities.items(), key=lambda kv: -kv[1]):
    print(f"  {name:7s} {p:.3f}")
print("parameters:", {k: round(v, 3) for k, v in result.parameters.items()})
print("regime:", result.regime)
print("provenance:", result.provenance)

# The same photometry with no zero point is refused, with a category code.
try:
    services.forecast([ObjectRequest(
        times=times, flux=flux, flux_err=flux_err, detected=[1] * 36,
        bands=bands, eval_times=[75.0], eval_bands=["r"],
    )])
except SeldonRejection as rejection:
    print("refused:", rejection.code.value, "-", rejection)
```

Run it in the image. The script is fed on standard input, so nothing needs
mounting, and `--network=none` shows the forecast needs no network access:

```console
$ docker run --rm -i --network=none seldon-service:latest python - < forecast_example.py 2>/dev/null
```

`2>/dev/null` hides library warnings on standard error (two `UserWarning`
lines about empty dataset splits and an `Initialized SELDON with 9 classes`
line). They are harmless. The run takes about ten seconds, most of it loading
the model. Output:

```text
Log Flux is True, max log: 5.367475894613145
forecast on the requested grid (flux at zero point 27.5 ):
  t= 75.0  r  flux=   270.3 +/-   60.2
  t= 80.0  r  flux=   225.5 +/-   52.9
  t= 85.0  r  flux=   192.5 +/-   47.9
  t= 75.0  g  flux=   174.5 +/-   49.7
  t= 80.0  g  flux=   150.3 +/-   45.3
  t= 85.0  g  flux=   130.9 +/-   42.0
predicted class: SNIa
class probabilities:
  SNIa    0.695
  SNIIn   0.214
  SNIb/c  0.036
  SLSN    0.028
  SNII    0.027
  SNIIb   0.000
  SNIax   0.000
  ILOT    0.000
  PISN    0.000
parameters: {'global_offset': 0.445, 'global_amplitude': 10258707.0, 'redshift': 0.025}
regime: RegimeIndicator(in_training_regime=True, sparse=False, n_observations=36, min_points=32, untrained_bands=(), eval_span_days=10.0, eval_span_in_time_scales=0.5, time_scale_days=20.0)
provenance: Provenance(library_version='1.2.0', checkpoint='seldon-2.0-roman-elasticc/epoch=1086-val_loss=1.18.ckpt', zero_point_mag=27.5, torch_version='2.9.0+cpu')
refused: missing_zero_point - The request states no flux zero point. Give the AB magnitude zero point of the flux (one value, or one per observation), or declare AT_TRAINING_ZERO_POINT if the flux is already at the model's 27.5 mag zero point.
```

The first line, `Log Flux is True, ...`, is printed to standard output by the
`seldon_core` library when the model loads. It is not part of the forecast.

The output is deterministic: the same image and input give the same numbers.
Things to notice:

- 36 usable observations is above `min_points` (32) and every band is
  trained, so `in_training_regime` is `True`.
- `flux` is in the order of `eval_bands` as given (r, r, r, g, g, g), not
  sorted.
- With fewer than 32 observations the same call still returns a forecast, but
  with `sparse=True` and `in_training_regime=False`.

To use your own photometry, replace `times`, `flux`, `flux_err`, `detected`,
`bands`, the evaluation grid, and `zero_point`, keeping the rest of the
script. To read a file from your machine, mount its directory read-only, for
example `-v "$PWD/data:/data:ro"`, and open it from `/data` in the script.

## When a request is refused

A request the checkpoint cannot serve raises `SeldonRejection` (from
`seldon.domain.errors`). Its `code` is one of four `RejectionCode` values;
match on the code, not the message text, which may change.

| `code` | Meaning | What to do |
|---|---|---|
| `missing_zero_point` | The request gave no `zero_point`. | Give the AB zero point of your flux, or declare `AT_TRAINING_ZERO_POINT` if the flux is already at 27.5. |
| `unknown_band` | A band in `bands` or `eval_bands` is not one the checkpoint knows. The exception's `bands` attribute lists the offending names as you gave them. | Convert the photometry to an LSST, Roman WFI, or JWST NIRCam filter, or drop those points. |
| `no_usable_observations` | No observation has a finite time, flux, flux error, and zero point. | Check for NaN or infinite values in your input. |
| `incompatible_checkpoint` | The checkpoint the service is configured with cannot be served. This is a deployment problem, not a problem with your request. | Report it to whoever runs the service. |

Two things are deliberately **not** refused: a sparse light curve (even a
single observation) and a band with no training data. Both are served, with
the regime indicator set.

In a batch, one bad object refuses the whole call: no forecasts come back, and
the error does not say which object it was. If you submit many objects, check
them yourself first or call `forecast` per object.

## Which bands

This checkpoint (`seldon-2.0-roman-elasticc`) knows 43 band names, from three
instruments only:

- **LSST:** `u g r i z y`
- **Roman WFI:** `F062 F087 F106 F129 F146 F158 F184 F213`
- **JWST NIRCam:** 29 filters, `F070W` through `F480M` (for example `F150W`,
  `F200W`, `F444W`)

Of these, 12 carry training data: the six LSST bands and Roman `F062 F087
F106 F129 F158 F184`. Roman `F146`, `F213`, and every JWST band are served
but reported in `regime.untrained_bands`.

There are no ZTF, ATLAS, Pan-STARRS, Swift, or Johnson-Cousins bands. A name
like `ztfg` or `V` is refused with `unknown_band`. Sending ZTF g photometry
labelled `g` is not refused, because `g` is a known name, but the model treats
it as LSST g and nothing can detect the relabelling. Convert photometry from
other systems before submitting.

Single-letter names are case-insensitive (`R` is read as `r`). Longer names
must match exactly: `F062` works, `f062` is refused.

The full table, with indices and training weights, is in
[docs/solutions/ml-runtime/seldon-servable-bands.md](../solutions/ml-runtime/seldon-servable-bands.md).

## Performance

Set the torch thread count to the container's CPU limit. The image defaults to
one thread (`OMP_NUM_THREADS=1`, `SELDON_TORCH_THREADS=1`). If you give the
container more CPUs, raise both to match, for example
`--cpus=4 -e OMP_NUM_THREADS=4 -e SELDON_TORCH_THREADS=4`. A plain
`python` script like the example above is governed by `OMP_NUM_THREADS`;
`SELDON_TORCH_THREADS` is applied when the Django app starts (the server, the
test runner, `manage.py shell`). Leaving torch at its default under a CPU
quota makes forecasts 15-40x slower; see "The thread trap" in
[docs/solutions/ml-runtime/seldon-cpu-viability.md](../solutions/ml-runtime/seldon-cpu-viability.md).
