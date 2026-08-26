---
name: SELDON Service
last_updated: 2026-08-26
---

# SELDON Service Strategy

## Purpose

The people best positioned to act on a pre-peak transient forecast - researchers
advocating for or arranging follow-up on another telescope - are the ones least able
to run SELDON. The model is gated behind an HPC allocation, a cluster environment,
and a training codebase, so the forecast that would justify the observation is not
available at the moment the decision has to be made.

## Positioning

We take whatever photometry the researcher already has - any telescope, any
assembly - rather than owning a stream, so that getting a SELDON forecast requires
nothing but the data in hand. When breadth of ingest competes with output polish or
interface work, ingest wins.

## Users

**Primary:** The follow-up advocate - they use SELDON Service to turn the
handful of early points they already have into a defensible forecast of what the
object will do next, before it peaks, so they can make the case for another
telescope's time.

**Secondary:** The follow-up implementer - someone with time already allocated,
triaging tonight's queue across many candidates at once.

## Key metrics

- **Ingest success rate** - fraction of submitted light curves that produce a forecast rather than an error; measured in service logs. This is the direct scoreboard for bring-your-own-light-curve.
- **Time from arriving with data to holding a forecast** - wall-clock for a first-time user, from showing up with photometry to having a forecast in hand. Effectively infinite before this service existed.
- **Forecasts that fed a real follow-up request** - count of forecasts that went on to support an observing proposal or an actual observation; instrumented in the service rather than collected by survey.

## Tracks

### Ingest and format coverage

Photometry schemas, bandpass handling for instruments beyond Rubin, validation, and
error messaging when something cannot be parsed.

_Why it serves the approach:_ This is the bring-your-own-light-curve bet made
concrete - every format we cannot accept is a researcher we cannot serve.

### Invocation surfaces

The web interface, the REST API, and the MCP interface as three faces of one
contract, so that humans, tools, and AI agents all invoke the same inference.

_Why it serves the approach:_ Low friction has to hold for whoever shows up, and
increasingly the thing asking for a forecast is a scheduler or an agent rather than
a person.

### Serving and operations

Packaging `seldon_core` at a pinned tag, running inference somewhere reachable
without institutional credentials, uptime, and capacity - including batch volume for
the implementer.

_Why it serves the approach:_ Reachability is the barrier the service exists to
remove; a model behind a VPN and an allocation is no more accessible than a model in
a paper.

## Boundaries

- No retraining or model evolution in this repo. SELDON evolves elsewhere; this service consumes it at a pinned release tag.
- New model capabilities arrive here as products to serve - not as work to be built
  here. Classification has already arrived this way: it is live in the pinned
  checkpoint, so the service exposes it rather than treating it as future work.
- Never return a forecast to avoid an ingest error. An honest failure beats a quiet, undefendable forecast, and where a forecast is returned outside the model's validated regime, the caveat travels with it.

_Resist a change when:_ it would require changing the model rather than the service,
or when it would trade an honest ingest failure for a forecast the advocate could not
defend.
