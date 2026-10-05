# Regenerating the reference outputs

`app/seldon/tests/test_reference_outputs.py` is the golden-output test (R14). It forecasts
three fixed light curves and compares the results with values recorded once and checked in.
If a library, checkpoint, or runtime change moves the forecast, this test fails, so the
change is visible instead of passing quietly.

| File | What it holds |
|---|---|
| `app/seldon/tests/fixtures/reference_light_curves.py` | The three synthetic light curves, plus `record()`, which writes the table |
| `app/seldon/tests/fixtures/reference_outputs.py` | The recorded table (`REFERENCE_OUTPUTS`) and the versions it was recorded against (`RECORDED_WITH`). Generated; do not edit by hand |
| `app/seldon/tests/test_reference_outputs.py` | The comparison, the tolerance, and the tests that check the tolerance |

## What is pinned

The test pins each fixture's forecast `flux` and `flux_err` on its evaluation grid, plus
its `class_probabilities`. The fixtures are deterministic and use no random numbers:

- `lsst_well_sampled`: 48 points in LSST g, r, i. This is above the 32-point training
  selection filter. The request declares that its flux is already at the training zero
  point.
- `lsst_sparse`: 6 points in LSST r and g, at 27.5 mag. It is below the filter, so it is
  served with the sparse flag set.
- `roman_wfi`: 36 points in Roman WFI F062, F106, and F158, at 26.0 mag. The forecast grid
  also asks for F087, F129, and F184. The pinned values therefore cover the rescale to the
  training zero point and back.

All three are forecast together in a single batch, both when recorded and when tested.

## Current table: recorded against

| Input | Value |
|---|---|
| `seldon_core` | 1.2.0 (tag v1.2.0) |
| Checkpoint | `seldon-2.0-roman-elasticc/epoch=1086-val_loss=1.18.ckpt` |
| torch | 2.9.0+cpu |
| torch threads | 1 (`SELDON_TORCH_THREADS` default) |
| Environment | `seldon-service:latest` image, `--network=none`, no data volume |

`RECORDED_WITH` in `reference_outputs.py` holds the same values. When the comparison
fails, its message prints them next to the provenance of the current run.

## Tolerance

Flux and flux error are compared with a relative tolerance of `1e-4`. Class probabilities
are compared with an absolute tolerance of `1e-4`. The tolerance sits between two bounds:

- **The smallest change it must catch** is a relative change of `1e-3` in any flux or flux
  error (about 1 mmag), or a change of `1e-3` in any class probability. The best
  photometric precision a researcher works with is about 1%, ten times larger. So any
  change a researcher could notice fails the test, with a factor of ten to spare. Any real model
  change (retrained weights, different normalization, a changed padding value) moves the
  numbers by far more than this.
- **The spread it must exceed** is the variation between runs that should agree. This was
  measured in the image:
  - repeated forward passes are bit-identical (spread 0);
  - running with 4 torch threads instead of 1 changes flux and flux error by up to 1.3e-5
    relative and probabilities by up to 8e-8;
  - forecasting a fixture on its own instead of in the batch changes flux by up to 1.0e-5
    relative and probabilities by up to 2.4e-6.

  These differences come from float32 summation order and say nothing about the model.

`1e-4` is ten times below the change the test must catch and about eight times above the
largest measured spread. `test_tolerance_is_wider_than_the_run_to_run_spread` reruns the
spread measurement (a repeat, and a run at 4 threads) on every test run.
`test_perturbed_reference_value_fails_the_comparison` checks that a `1e-3` change to any
single value fails.

## When to regenerate

Regenerate only on purpose, and only after one of these:

- a `seldon_core` tag bump. The library is a research codebase with a young release
  process, so treat a bump as a change that requires a new recording, never as a routine
  dependency update;
- a checkpoint change. The checkpoint and the library are pinned independently, so either
  one alone counts;
- a torch version bump;
- a deliberate change to a fixture in `reference_light_curves.py`.

Never regenerate just to make a failing test pass. If the test fails and none of the
changes above was intended, the failure is a regression. Find its cause first.

## How to regenerate

1. Rebuild the image with the new pin (`run/seldonctl slim_dev up` builds it). The library,
   torch, and checkpoint come from the image. The bind mount below only supplies the code.
2. From the repository root, record into the working tree:

   ```bash
   docker run --rm --network=none \
     --user "$(id -u):$(id -g)" -e HOME=/tmp -e USER=seldon -e PYTHONDONTWRITEBYTECODE=1 \
     -v "$PWD/app:/app" seldon-service:latest \
     python manage.py shell -c \
     "from seldon.tests.fixtures.reference_light_curves import record; record()"
   black app/seldon/tests/fixtures/reference_outputs.py
   ```

   `manage.py shell` sets up Django, which sets torch's thread count the same way the test
   runner does. `--user` keeps the rewritten file owned by you. `USER` is needed because
   torch looks up a user name, and a container user without one fails that lookup.

3. Run the suite the same way:

   ```bash
   docker run --rm --network=none \
     --user "$(id -u):$(id -g)" -e HOME=/tmp -e USER=seldon -e PYTHONDONTWRITEBYTECODE=1 \
     -v "$PWD/app:/app" seldon-service:latest \
     python manage.py test seldon.tests -v 2
   ```

## Before committing new values

- `git diff app/seldon/tests/fixtures/reference_outputs.py`. Check that `RECORDED_WITH`
  names the versions you meant to move to, and only those.
- Read how far the numbers moved and decide whether that size of change is expected. A
  torch bump that moves values by more than about `1e-4` relative, or that changes a
  fixture's predicted class, is not float noise. Find out why before you accept it.
- Check that the regime flags still make sense: `lsst_sparse` should still be served with
  the sparse flag set. This file does not pin the flags, but `test_forecast.py` covers them.
- Update the "recorded against" table above. If you remeasured the spread, update the
  spread figures as well.
- In the commit message, say what changed, which pin moved, and by how much the numbers
  moved.
