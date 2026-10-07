---
title: SELDON forecasts on CPU - viability and observation floor
date: 2026-10-05
module: ml-runtime
problem_type: performance
tags: [seldon, cpu, torch, threads, latency, checkpoint]
---

# SELDON forecasts on CPU - viability and observation floor

Finding from U1 of `docs/plans/2026-08-26-0512-feat-model-serving-foundation-plan.md`.
The spike that produced it was throwaway and is deleted in U2.

## Verdict

**Yes.** The latent neural-ODE path runs correctly CPU-only and clears the 60-second
bar by roughly two orders of magnitude - provided torch's thread count matches the
container's CPU limit. With the thread count left at torch's default it still clears
the bar, but runs 15-40x slower than it should.

## Conditions

- Image: `python:3.13-slim`, `torch==2.9.0+cpu` from the PyTorch CPU index,
  `lightning==2.5.2`, `seldon_core` at `v1.2.0` (commit `0daf669`).
- Compilation **disabled** (`TORCH_COMPILE_DISABLE=1`); this is the mode measured.
  No compiled mode was qualified.
- `--network=none`, checkpoint mounted read-only, no training data present.
- Host: 8 cores. Limits applied with `docker run --cpus=N --cpuset-cpus=0-(N-1)`.
- Checkpoint: `epoch=1086-val_loss=1.18.ckpt` with its `hparams.yaml`
  (`FlexibleLightCurveDataLoader`, `SELDONEnergyClassifierExperiment3`).
- Inputs: synthetic SN-like light curves in the six LSST bands at the training
  zero point; evaluation grid of N+64 points spanning 150 days.

## Measurements (seconds, wall clock, after one warm-up forecast)

| Observations | 1 CPU, 8 threads (default) | 1 CPU, 1 thread | 2 CPUs, 2 threads |
|---|---|---|---|
| 3 | 0.40 | 0.08 | 0.05 |
| 5 | 0.40 | 0.08 | 0.05 |
| 10 | 0.31 | 0.09 | 0.05 |
| 30 | 0.80 | 0.10 | 0.06 |
| 100 | 1.40 | 0.14 | 0.10 |
| 256 | 3.50 | 0.23 | 0.19 |
| 400 | 5.00 | 0.32 | 0.26 |
| **Padded batch of 8** (5, 10, 30, 50, 100, 200, 256, 400 points) | **30.4** | **0.8** | **0.5** |

- The 60-second bar is never crossed, including past the training-time cap of 256
  points. There is no observation count at which it stops clearing in this range.
- Cost split: checkpoint load (model, data module, experiment, weights) takes
  0.8-1.0 s once per process. Every forecast above is forward pass plus
  post-processing.
- Peak resident memory (`VmHWM`) is 1.6-1.8 GB per process. That bounds workers per
  container: budget about 2 GB each.

## The thread trap

Under `--cpus=1` the container still sees all 8 host cores, so torch sizes its
intra-op pool to 8 threads. The CFS quota then throttles those 8 threads onto one
CPU's worth of time, and the result is 15-40x slower than a single thread. The
padded batch goes from 0.8 s to 30.4 s - within 2x of the bar, from a run that
should have been trivially fast.

U2 must therefore pin the thread count to the container's CPU limit (for example
`OMP_NUM_THREADS`, or `torch.set_num_threads` at startup, set from the same value as
the Kubernetes CPU limit). Leaving it at the default makes latency depend on the
node's core count rather than the pod's limit, which is invisible in local testing.

## Observation floor

A light curve of **1, 2, 3, or 5** observations produces a finite forecast with no
error. The floor is therefore one usable observation, which is exactly R11's
existing rule (reject only zero usable observations). **No R11 amendment is needed.**
Whether a forecast from one point is *useful* is R10's concern (its observation-count
axis compares against the training `min_points` of 32), not a rejection.

## State-dict key sets

Loading through `torch.load(..., map_location="cpu", weights_only=True)` followed by
`experiment.load_state_dict(state_dict, strict=False)` reports:

- missing keys: **none**
- unexpected keys: **none**

So U3's allowlist is the empty set. This differs from KTD6's premise that
`strict=False` is required because the class embedding is aliased under a second
name: on this load path, at this tag, every key matches. U3 can keep `strict=False`
and assert both sets are empty, which is equivalent to strict loading but keeps the
error message under the adapter's control.

## Other observations for later units

- **Determinism (U7, U8):** two forward passes on the same batch return bit-identical
  reconstructions. No seeding is needed.
- **Class head width (U7):** `predict_class(z_mean, probs=True)` returns shape
  `[1, 10]` summing to 1, while the class map names 9 classes. This confirms the
  unnamed-output drop rule R9 now states.
- **Filter paths (U3):** 172 training-time filter paths appear in `hparams.yaml`, which
  is 43 bands times the four sites R3 names. All resolved against the installed
  package's `TAE/data/` directory.
- **Forward-pass outputs (U7):** `reconstructed` and `error` are `[batch, n_eval, 1]`;
  `z_mean`, `z_var`, `z_sample`, `z_sigma`, `z` are `[batch, 6]`; `parameters`,
  `class_idx`, `class_emb`, `memory`, `encoding_sequence`, `decoder_inputs` and `elbo`
  are also returned.
- **Time scale (U6, R10):** `flux_stats.t_max` is `20.0` and is the divisor the notebook
  uses for time normalization.
- **Benign warnings:** with no data present, the data module's `random_split` warns
  that the train and validation splits are empty. This is expected on the no-data
  path.
