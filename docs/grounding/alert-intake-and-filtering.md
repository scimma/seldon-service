# Grounding: alert intake and filtering

Extraction only. Quotes are verbatim, with file:line pointers. This file is untracked; it is only as durable as the next commit.

## 1. Consumption code: what exists

Searched tracked files for kafka, hopskotch, scimma, broker, consumer, lasair, antares, alert, topic, rubin, lsst, stomp, alerce, fink, diaSource, magzp. No code, config, or dependency hits. Hits are prose only (STRATEGY.md, plan and solution docs).

- `app/requirements.txt:5-10` lists coverage, Django 5.2.6, gunicorn, lightning 2.5.2, pydantic 2.9.2, pydantic-settings 2.5.2. No pyproject.toml exists.
- `app/seldon_project/urls.py:8`: `path("healthz", views.healthz, name="healthz"),` is the only route.
- `docs/guides/running-a-forecast.md:7-8`: "What exists today is a Python call inside the image. There is no web form, REST API, or MCP interface yet; the only HTTP endpoint is `/healthz`."

## 2. Product framing (STRATEGY.md)

- `STRATEGY.md:18-22`: "We add to what brokers emit rather than compete with them. As part of the SCiMMA ecosystem, the service takes alerts from several Rubin brokers, enriches them with model forecasts, and sends the results back into a shared ecosystem instead of being one more destination ... Forecasts on the alert stream come first; forecasts on photometry a researcher brings remain a secondary path."
- `STRATEGY.md:47-50`: "Filtering uses what brokers and SCiMMA services already provide. The service does not train its own classifier." / "The service publishes forecasts. Ranking and prioritizing candidates is left to consumers."
- `STRATEGY.md:51-54`: "Never return a forecast just to avoid an input error. An honest failure beats a quiet forecast nobody can defend, and where a forecast falls outside the model's validated range, the caveat travels with it - on the broadcast stream as well as in single responses."
- `STRATEGY.md:70-77` (track "Alert intake and filtering"): "Consuming several Rubin brokers, selecting likely transients using what brokers and SCiMMA services already provide, and turning alert light curves into model input. Submitted photometry comes in here as a second path ... a researcher with ZTF, ATLAS, Pan-STARRS, or Swift data cannot yet be served correctly."
- `STRATEGY.md:63-66` (metrics): "**Alert coverage and latency** - share of filtered alerts that get a forecast, and time from an alert arriving to its forecast going out"; "**Downstream use** - distinct consumers of the forecasts, through Hopskotch subscribers and web/REST/MCP callers".
- `STRATEGY.md:92-93` (distribution): "Hopskotch broadcast plus the web, REST, and MCP interfaces, all as views of one forecast output."

## 3. Forecast call and ObjectRequest

- `app/seldon/services.py:91-92`: `def forecast(requests: Sequence[ObjectRequest]) -> list[ObjectForecast]:`. Body at `services.py:136-137` calls `forecast_objects(requests, loaded_model(), capabilities, provenance)`.
- `app/seldon/domain/models/request.py:87-94` (ObjectRequest fields):
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
- `request.py:7-10`: "The flux zero point has no default. A caller states it, per object or per observation, or declares ``ZeroPointDeclaration.AT_TRAINING_ZERO_POINT`` to assert the flux is already at ``TRAINING_ZERO_POINT_MAG``. Leaving it out is representable (``None``) only so validation can refuse it by name."
- `app/seldon/domain/models/provenance.py:10`: `TRAINING_ZERO_POINT_MAG = 27.5`
- `request.py:98-101`: "The flag indexes a two-row embedding, so only 0 and 1 are flags"; `raise ValueError("detected must hold only 0, 1, True, or False.")`.
- `app/seldon/infrastructure/ml/validation.py:87-100` (usable observations): "This is the one definition of "usable": validation refuses an object with none, and encoding drops the rest. An observation is usable when its time, flux, and flux error are finite and, for a per-observation zero point, its zero point is finite too."
- `docs/guides/running-a-forecast.md:67-69`: "An observation is used only if its time, flux, flux error, and (when you gave one per observation) zero point are all finite. Other observations are dropped silently; `regime.n_observations` tells you how many were used."
- Documented example at `app/seldon/services.py:101-112` uses `bands=["r", "g", "r"],  # LSST, Roman WFI, or NIRCam names` and `zero_point=27.5,  # AB magnitude zero point of the flux`.

## 4. ObjectForecast output

- `docs/guides/running-a-forecast.md:75-85` (table of fields): `eval_times, eval_bands`; `flux, flux_err` ("`flux[k]` is at `eval_times[k]` in `eval_bands[k]`"); `zero_point_mag`; `predicted_class`; `class_probabilities` ("Nine classes: `ILOT`, `PISN`, `SLSN`, `SNII`, `SNIIb`, `SNIIn`, `SNIa`, `SNIax`, `SNIb/c`."); `parameters`; `latent_mean, latent_sigma`; `regime`; `provenance`.
- `running-a-forecast.md:87-90`: "`zero_point_mag` is the zero point you gave if you gave a single float. Otherwise ... it is 27.5. Always read it rather than assuming: forecast fluxes at different zero points are not comparable."
- `app/seldon/domain/models/forecast.py:57`: `class ObjectForecast:`, docstring "One object's forecast, with every output family from a single pass."

## 5. Rejection codes

- `app/seldon/domain/errors.py:16-22`:
  ```python
  class RejectionCode(str, Enum):
      MISSING_ZERO_POINT = "missing_zero_point"
      UNKNOWN_BAND = "unknown_band"
      NO_USABLE_OBSERVATIONS = "no_usable_observations"
      INCOMPATIBLE_CHECKPOINT = "incompatible_checkpoint"
  ```
- `errors.py:1-7`: "The codes are a public contract: add one only when a new kind of refusal cannot be told apart from these."
- `services.py:129-134` (Raises): "A sparse light curve or an untrained band is served, with the regime indicator set, not refused."
- `running-a-forecast.md:244-246`: "In a batch, one bad object refuses the whole call: no forecasts come back, and the error does not say which object it was."
- `docs/plans/2026-08-26-0512-feat-model-serving-foundation-plan.md:69` (R11): "Photometry that fits the shape R5 names but that the checkpoint cannot serve is rejected with an error naming the reason ... A sparse light curve is not one of them - it is served, with R10's indication set."

## 6. Regime flag

- `app/seldon/domain/services/forecast_service.py:44-55`:
  ```python
  usable = usable_observations(request)
  n_observations = int(usable.sum())
  sparse = n_observations < capabilities.min_points
  ...
  in_training_regime=not sparse and not untrained,
  ```
- `forecast_service.py:31-35`: "Sparsity: fewer usable observations than ``min_points``, the training selection filter ... Bands: any usable observation's band or evaluation band whose training weight is infinite. Span: ... reported and never flagged."
- `app/seldon/domain/models/forecast.py:18-26` (RegimeIndicator): "Two axes can put a forecast outside that regime, and either one clears ``in_training_regime``: too few usable observations, or a band that carried no training observations."
- `docs/guides/running-a-forecast.md:94-98`: "`in_training_regime`: `False` if the forecast is sparse or uses an untrained band. This is the one to check." / "`sparse`: fewer usable observations than `min_points` (32). ... It still returns a forecast, even from a single observation, but treat it with caution."
- `app/checkpoints/seldon-2.0-roman-elasticc/hparams.yaml:155`: `    min_points: 32`

## 7. Observation floor and 256-point cap

- Code refuses only at zero usable observations: `validation.py:160` `if not usable_observations(request).any():`.
- `docs/solutions/ml-runtime/seldon-cpu-viability.md:68-74`: "A light curve of **1, 2, 3, or 5** observations produces a finite forecast with no error. The floor is therefore one usable observation ... **No R11 amendment is needed.** Whether a forecast from one point is *useful* is R10's concern (its observation-count axis compares against the training `min_points` of 32), not a rejection."
- 256 cap: `seldon-cpu-viability.md:47`: "The 60-second bar is never crossed, including past the training-time cap of 256 points."
- `docs/plans/...-plan.md:254` (U1 step 3): "The checkpoint records a training-time cap of 256 and nothing rejects a longer request, so the sweep must reach past it."
- No enforcing max-length check in non-test `app/seldon` code.

## 8. Band vocabulary

- `docs/solutions/ml-runtime/seldon-servable-bands.md:15-19`: "The checkpoint `seldon-2.0-roman-elasticc` ... knows **43 band names, and they denote LSST, Roman WFI, and JWST NIRCam filters only.** Only **12 of them carry training weight**."
- `seldon-servable-bands.md:21-26`: "There are no ZTF, ATLAS, Pan-STARRS, Swift, or Johnson-Cousins names. ... A ZTF `g` point sent as `g` is treated as LSST `g`, and that relabelling **cannot be detected** by the service or the model."
- `seldon-servable-bands.md:92-95`: "An untrained band can still be evaluated ... That forecast is an extrapolation in wavelength."
- `running-a-forecast.md:253-260`: LSST `u g r i z y`; Roman WFI `F062 F087 F106 F129 F146 F158 F184 F213`; JWST NIRCam 29 filters `F070W` through `F480M`. "Of these, 12 carry training data ... Roman `F146`, `F213`, and every JWST band are served but reported in `regime.untrained_bands`."
- `running-a-forecast.md:262-269`: "A name like `ztfg` or `V` is refused with `unknown_band`." / "Single-letter names are case-insensitive (`R` is read as `r`). Longer names must match exactly: `F062` works, `f062` is refused."
- Plan R17 (`...-plan.md:71`): "Caller-supplied band names are matched against the checkpoint's vocabulary after normalizing single-character names to lower case, and matched as given otherwise."

## 9. Settings, env, Django

- `app/seldon/config/settings.py:35-47`: `checkpoint_path`, `hparams_path`, `torch_threads: int = Field(1, ge=1)`, `warmup: bool = Field(False)`; `model_config = SettingsConfigDict(env_prefix="SELDON_", ...)`.
- `settings.py:1-5`: "Model-related settings, read from ``SELDON_``-prefixed environment variables. Django's own settings stay in ``seldon_project.settings``."
- `app/seldon_project/settings.py:33-35`: `DEBUG = os.environ.get("DJANGO_DEBUG", "false").lower() == "true"`; `ALLOWED_HOSTS = os.environ.get("DJANGO_ALLOWED_HOSTS", "*").split(",")`
- `seldon_project/settings.py:51`: `DATABASES: dict = {}`. `seldon_project/settings.py:67-69`: `"seldon"` logger level from `SELDON_LOG_LEVEL`, default `"INFO"`.
- `env/.env.default:1-13`: `DJANGO_DEBUG =`, `DJANGO_ALLOWED_HOSTS = *`, `WEB_APP_PORT = 8000`, `WEB_APP_HOST = app`, `SELDON_IMAGE = seldon-service:latest`, `SELDON_TORCH_THREADS = 1`. No broker or stream variables.
- Layout: `app/manage.py`; `app/seldon_project/` (settings, urls, wsgi); `app/seldon/` with `views.py` (healthz only), `services.py` (`@lru_cache` `loaded_model()` at `services.py:35-36`), `apps.py`, `config/settings.py`, `domain/` (Django-free models, errors, `services/forecast_service.py`), `infrastructure/ml/` (`loader.py`, `encoding.py`, `forecast.py`, `validation.py`, `capabilities.py`), `tests/`.
- `services.py:3-4`: "Nothing here imports Django, so the same seams serve a view, a management command, or a process with no request cycle at all."

## 10. Docker, run, tooling

- `app/Dockerfile:9-10`: `ARG TORCH_VERSION=2.9.0+cpu`, `ARG SELDON_CORE_REF=v1.2.0`. `:7` `FROM python:3.13-slim AS deps`.
- `Dockerfile:25-27`: `RUN --mount=type=secret,id=gh_token,required=true \` installs `seldon_core` from a private git repo at `@${SELDON_CORE_REF}`.
- `Dockerfile:35-39`: ENV `TORCH_COMPILE_DISABLE=1`, `MPLBACKEND=Agg`, `OMP_NUM_THREADS=1`, `SELDON_TORCH_THREADS=1`.
- `docker/docker-compose.yml:8-21`: single service `app`, `command: bash entrypoints/docker-entrypoint.app.sh`, profile `slim_dev`. No broker or Kafka service.
- `docker/docker-compose.dev.yaml:25`: `cpus: ${SELDON_TORCH_THREADS:-1}`; `:29-31`: `DEV_MODE=1`, `APP_VERSION=...`, `SELDON_LOG_LEVEL=DEBUG`.
- `app/entrypoints/docker-entrypoint.app.sh:10-13`: `if [[ $DEV_MODE == 1 ]]; then exec python manage.py runserver ...` else `exec gunicorn seldon_project.wsgi ...`.
- `run/seldonctl:16-25`: only profile `slim_dev`. `:40-53` actions `up`, `logs`, `down`, `test`; `:73-80`: `coverage run manage.py test seldon.tests -v 2`.
- `README.md:12-13`: "Today the service is a Python call, `seldon.services.forecast`, inside the image. There is no web interface, REST API, or MCP interface yet."

## 11. Sibling SCiMMA service patterns (per docs)

- `...-plan.md:170`: "The sibling Astrodash service, whose layered domain and inference split, CPU-only torch install, and container tooling are the closest local precedent."
- `...-plan.md:237`: "**The sibling service is a pattern source, not a dependency.** Nothing here imports from it."
- `...-plan.md:227`: "The service targets Python 3.13 and Django 5.2, matching the sibling service."
- `...-plan.md:271`: "Mirror the sibling service's two-settings split: Django settings for the framework, a prefixed pydantic-settings class for everything model-related."
- No repo doc describes Hopskotch, broker, or crossmatch service patterns beyond `STRATEGY.md:66,92`.

## 12. Not found

- Kafka, Hopskotch, STOMP, or SCiMMA client code or config.
- Broker alert parsing or schema (Rubin, ANTARES, Lasair, ALeRCE, Fink), diaSource/diaObject fields, or magzp-derived zero points.
- Alert-level filter logic (reliability, SNR, classifier score cuts).
- A 256-point enforcement check.
