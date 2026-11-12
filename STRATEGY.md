---
name: SELDON Service
last_updated: 2026-11-12
---

# SELDON Service Strategy

## Purpose

Each new model, and each new version of SELDON, gets stuck in code repositories and
papers and never runs on live alerts. Getting from a trained checkpoint to forecasts
on live alerts takes stream intake, filtering, reliable model serving, and
distribution - operational work that each model team would otherwise rebuild from
scratch.

## Positioning

We add to what brokers emit rather than compete with them. As part of the SCiMMA
ecosystem, the service takes alerts from several Rubin brokers, enriches them with
model forecasts, and sends the results back into a shared ecosystem instead of being
one more destination - so that a new model, or a new SELDON version, reaches live
alerts by plugging in here instead of building its own pipeline. Forecasts on the
alert stream come first; forecasts on photometry a researcher brings remain a
secondary path. Supernovae come first, but the service is not limited to them: as
SELDON extends to TDEs, kilonovae, and AGN flares, so does the service.

## Users

**Primary:** Model developers such as the SELDON team - on the day they cut a new
release, they use SELDON Service to get forecasts from that version onto live alerts
within days rather than months, to run it alongside the previous version for
comparison, and to have its outputs cited and used without operating anything
themselves.

**Secondary:** The follow-up advocate - they use SELDON Service to get a defensible
forecast for the one object they're making the case for, before it peaks, whether
that forecast already exists from the alert stream or comes from photometry they
bring.

## Boundaries

- No retraining or model evolution in this repo. Every model - SELDON first, others'
  models too - is used at a pinned release from the team that owns it.
- New model capabilities arrive here as things to serve, not as work to build here.
  Classification already arrived this way: it is live in the pinned checkpoint, so
  the service exposes it.
- Filtering uses what brokers and SCiMMA services already provide. The service does
  not train its own classifier.
- The service publishes forecasts. Ranking and prioritizing candidates is left to
  consumers.
- Never return a forecast just to avoid an input error. An honest failure beats a
  quiet forecast nobody can defend, and where a forecast falls outside the model's
  validated range, the caveat travels with it - on the broadcast stream as well as in
  single responses.

_Resist a change when:_ it would require building or changing a model rather than
serving one, it would put the service in competition with brokers or consumers
instead of adding to alerts, or it would trade an honest failure for a forecast
nobody could defend.

## Key metrics

- **Release-to-live lag** - time from a model release tag to that version's forecasts going out on live alerts; measured from release tags and deployment records.
- **Forecast accuracy** - how well a forecast predicted the photometry that later alerts for the same object brought in; computed by the service from its stored forecasts and later alerts.
- **Alert coverage and latency** - share of filtered alerts that get a forecast, and time from an alert arriving to its forecast going out; from service logs or metrics.
- **Downstream use** - distinct consumers of the forecasts, through Hopskotch subscribers and web/REST/MCP callers; counted by the service and the Hopskotch topic.

## Tracks

### Alert intake and filtering

Consuming several Rubin brokers, selecting likely transients using what brokers and
SCiMMA services already provide, and turning alert light curves into model input.
Submitted photometry comes in here as a second path, along with mapping other
instruments' bands onto what a model knows - the pinned checkpoint knows only LSST,
Roman, and JWST filters, so a researcher with ZTF, ATLAS, Pan-STARRS, or Swift data
cannot yet be served correctly.

_Why it serves the approach:_ Enriching what brokers emit starts with taking it in
from more than one of them, so a model is not tied to any one broker's pipeline.

### Model integration

The contract a model plugs into, pinned versions, versions running side by side, and
provenance on every output. SELDON first; models from other teams are in scope.

_Why it serves the approach:_ This is the primary user's job - a new release reaches
live alerts by plugging in, not by building a pipeline.

### Distribution

Hopskotch broadcast plus the web, REST, and MCP interfaces, all as views of one
forecast output.

_Why it serves the approach:_ Sending results back into the shared ecosystem is the
positioning; without distribution the service is one more destination.

### Serving and operations

Packaging each model at its pinned release, running it reliably on CPU, capacity, and
uptime.

_Why it serves the approach:_ This is the operational work no model team should have
to rebuild.
