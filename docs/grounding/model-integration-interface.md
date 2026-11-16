# Grounding: model-integration interface (Data / Encoder / Decoder Heads)
Extraction from main (426a917). No file names an Encoder / Decoder Heads /
queue contract; one model (SELDON) exists; the nearest thing is the "adapter" below.

## 1. Existing abstraction: one adapter, one model, no plug-in seam
docs/plans/2026-08-26-0512-feat-model-serving-foundation-plan.md:33
> A thin adapter in this repository that loads a pinned SELDON checkpoint and returns forecasts on a caller-supplied evaluation grid, running CPU-only with no training data present.

plan:47
> **The adapter returns every output family and lets the caller select.** ... a single forward pass computes all of them

app/seldon/services.py:35-36,51-52
```python
@lru_cache(maxsize=1)
def loaded_model() -> LoadedModel:
    settings = get_settings()
    return load_model(settings.checkpoint_path, settings.hparams_path)
```
app/seldon/services.py:136-137
```python
    capabilities, provenance = _model_description()
    return forecast_objects(requests, loaded_model(), capabilities, provenance)
```
app/seldon/domain/services/forecast_service.py:93-97
```python
    validated = validate_objects(requests, capabilities)
    encoded = encode_objects(
        validated, capabilities, loaded.experiment.data.dataset.transform
    )
    outputs = run_forecast(loaded.experiment, encoded, capabilities.class_map)
```

## 2. Checkpoint loading
app/seldon/infrastructure/ml/loader.py:1-3
> Load a SELDON checkpoint into a ready experiment on CPU, with no training data.
> A checkpoint directory holds ``hparams.yaml`` and a Lightning checkpoint file.

loader.py:58-72: `LoadedModel(experiment: Any, checkpoint_path: Path, hparams_path: Path, library_version: str)`
loader.py:183-188
> def load_model(checkpoint_path: Path, hparams_path: Path) -> LoadedModel:
> Each call builds a new experiment, and the library's data module starts a ``multiprocessing.Manager`` process each time; call it once per process

app/Dockerfile:27: seldon_core installed from `github.com/skai-institute/Transient-Foundation-Model.git@${SELDON_CORE_REF}`

app/requirements.txt:1-2
> # torch is installed separately in the Dockerfile from the PyTorch CPU index
> # (torch==2.9.0+cpu). Never list it here

## 3. Inputs (Data)
app/seldon/domain/models/request.py:87-94
```python
    times: np.ndarray
    flux: np.ndarray
    flux_err: np.ndarray
    detected: np.ndarray
    bands: tuple[str, ...]
    eval_times: np.ndarray
    eval_bands: tuple[str, ...]
    zero_point: ZeroPoint = None
```
request.py:7-10
> The flux zero point has no default. A caller states it, per object or per observation, or declares ``ZeroPointDeclaration.AT_TRAINING_ZERO_POINT``

app/seldon/domain/models/capabilities.py:48-53 (ModelCapabilities): `band_index`, `band_weights`, `flux_stats`, `time_scale`, `class_map: Mapping[str, int]`, `min_points`

## 4. Encoding and batching
app/seldon/infrastructure/ml/encoding.py:3-6,13-15
> Per object, encoding drops unusable observations, rescales flux from the caller's zero point to ``TRAINING_ZERO_POINT_MAG``, applies the checkpoint's own flux normalization, anchors times at the earliest usable observation and divides by the checkpoint's time scale ...
> A sequence of objects is zero-padded into one batch. The observation side carries a mask; the evaluation side carries none, and a padded band index of 0 is a real band

app/seldon/infrastructure/ml/forecast.py:110-115
```python
    with torch.inference_mode():
        out = experiment(encoded.batch)
        flux, flux_err = experiment.post_process_prediction(
            out["reconstructed"], out["error"]
        )
        head = experiment.model.predict_class(out["z_mean"], probs=True)
```
app/seldon/intake/engine.py:791: `(result,) = services.forecast([request])`
app/seldon/config/intake.py:36: `batch_size: int = Field(100, gt=0)` (polled alerts, not model inputs). No model-input queue found in app/.

## 5. Outputs
app/seldon/domain/models/forecast.py:84-95 (ObjectForecast)
```python
    eval_times, eval_bands, flux, flux_err, zero_point_mag: float,
    parameters: Mapping[str, float], class_probabilities: Mapping[str, float],
    predicted_class: str, latent_mean: np.ndarray, latent_sigma: np.ndarray,
    regime: RegimeIndicator, provenance: Provenance
```
forecast.py:1-8
> Every output family the forward pass computes is here, so a caller reads the fields it wants and ignores the rest. ... no torch tensor crosses this boundary.

plan:67
> A forecast returns reconstructed flux with uncertainty on the requested grid, the interpretable basis parameters, the class prediction with probabilities, and the latent representation.

"redshift" does not occur in app/, CONCEPTS.md, or the plan's output requirement.

## 6. Provenance, refusals, regime
app/seldon/domain/models/provenance.py:25-28: `library_version: str`, `checkpoint: str`, `zero_point_mag: float`, `torch_version: str`
app/seldon/domain/errors.py:47-50,33-35
```python
    MISSING_ZERO_POINT = "missing_zero_point"
    UNKNOWN_BAND = "unknown_band"
    NO_USABLE_OBSERVATIONS = "no_usable_observations"
    INCOMPATIBLE_CHECKPOINT = "incompatible_checkpoint"
```
> The codes are a public contract: add one only when a new kind of refusal cannot be told apart from these.

forecast.py:46-53 (RegimeIndicator): `in_training_regime`, `sparse`, `n_observations`, `min_points`, `untrained_bands`, `eval_span_days`, `eval_span_in_time_scales`, `time_scale_days`

## 7. Django tables
app/seldon/models.py:278-298 (Forecast): `checkpoint = CharField(255)`, `light_curve = JSONField()`, `zero_point = JSONField()`, `grid = JSONField()`, `outputs = JSONField()`, `in_training_regime`, `library_version`, `torch_version`, `app_version`
models.py:327-332 (Refusal): `checkpoint`, `code`, `message`, `bands = JSONField(default=list)`
models.py:159-165 (LightCurvePoint): `dia_object_id`, `dia_source_id`, `observed_mjd`, `band = CharField(max_length=8)`, `flux`, `flux_err`
models.py:216-224 (Delivery): `broker`, `role`, `reliability_score`, `sn_like_score`, `points` (M2M), `schema_version`, `classifier_version`
CONCEPTS.md:24
> Each detection is forecast at most once per model checkpoint, however many brokers deliver it.
No model-version table; `checkpoint` is a CharField on Forecast and Refusal.

## 8. Light curve and bands after intake
app/seldon/domain/intake/light_curve.py:3-7
> Forced photometry is left out. Rubin fluxes are in nJy, whose AB zero point is 31.4, and LSST band letters are normalized to the checkpoint's lower-case names; any other band passes through so the model can refuse it by name.

light_curve.py:98-107: `ObjectRequest(times=[p.observed_mjd ...], ..., detected=[True] * len(points), ..., zero_point=LSST_ZERO_POINT)`
app/seldon/domain/intake/grid.py:1-4
> Six LSST bands, one point a day, from the first observation in the light curve to 60 days past the last.

docs/solutions/ml-runtime/seldon-servable-bands.md:9-10
> knows **43 band names, and they denote LSST, Roman WFI, and JWST NIRCam filters only.** Only **12 of them carry training weight**
servable-bands.md:16-18
> The model has no way to tell where photometry came from: the band name is the only thing that selects the transmission curve. A ZTF `g` point sent as `g` is treated as LSST `g`, and that relabelling **cannot be detected**

## 9. CPU runtime limits
docs/solutions/ml-runtime/seldon-cpu-viability.md:12-15
> **Yes.** The latent neural-ODE path runs correctly CPU-only and clears the 60-second bar by roughly two orders of magnitude - provided torch's thread count matches the container's CPU limit.

cpu-viability.md (table; cols 1 CPU 8 threads / 1 CPU 1 thread / 2 CPUs 2 threads, seconds)
> | **Padded batch of 8** (5, 10, 30, 50, 100, 200, 256, 400 points) | **30.4** | **0.8** | **0.5** |

cpu-viability.md:55-56
> Peak resident memory (`VmHWM`) is 1.6-1.8 GB per process. That bounds workers per container: budget about 2 GB each.

## 10. Output surfaces
README.md:13: "There is no web interface, REST API, or MCP interface yet."
STRATEGY.md:91-92
> Hopskotch broadcast plus the web, REST, and MCP interfaces, all as views of one forecast output.

app/seldon/views.py:32: only `healthz` exists; payload `"model": asdict(identity) if ready else None`. No Hopskotch code outside STRATEGY.md and docs.
Related, not quoted: docs/guides/running-a-forecast.md; docs/developer/regenerating-reference-outputs.md; app/seldon/infrastructure/brokers/parsers/
