"""The fixed light curves the golden-output test pins, and their recorder.

Every light curve is synthetic and fully determined by the code below: no
randomness, no file, no network. Changing any number here changes the input,
so the recorded outputs in ``reference_outputs.py`` must be re-recorded with
it (``docs/developer/regenerating-reference-outputs.md``).

The three cover the paths a researcher's request takes:

- ``lsst_well_sampled``: 48 points in LSST g, r, i, above the training
  selection filter (32 points), declared already at the training zero point.
- ``lsst_sparse``: 6 points in LSST r and g, below the filter, so it is
  served with the sparse indicator set, at a stated 27.5 mag zero point.
- ``roman_wfi``: 36 points in the trained Roman WFI bands F062, F106, F158,
  at a 26.0 mag zero point, so the rescale to the training zero point and
  back is inside what is pinned.

Recording runs through Django so torch's thread count is set exactly as it
is in the test runner::

    python manage.py shell -c \\
        "from seldon.tests.fixtures.reference_light_curves import record; record()"
"""

import logging
import pprint
from pathlib import Path

import numpy as np

from seldon.domain.models.request import ObjectRequest, ZeroPointDeclaration

logger = logging.getLogger(__name__)

REFERENCE_OUTPUTS_PATH = Path(__file__).resolve().parent / "reference_outputs.py"


def _bazin(times: np.ndarray, peak_flux: float, t0: float) -> np.ndarray:
    """A supernova-like rise and decline (Bazin et al. 2009), plus a floor.

    Args:
        times: Times, in days.
        peak_flux: Amplitude of the transient.
        t0: Reference time of the rise, in days.

    Returns:
        Flux at each time.
    """
    rise, fall = 3.0, 18.0
    shape = np.exp(-(times - t0) / fall) / (1.0 + np.exp(-(times - t0) / rise))
    return peak_flux * shape + 20.0


def _lsst_well_sampled() -> ObjectRequest:
    """48 points cycling g, r, i every 1.5 days, at the training zero point."""
    times = np.arange(48, dtype=float) * 1.5
    bands = ["g", "r", "i"] * 16
    colour = {"g": 0.8, "r": 1.0, "i": 0.9}
    flux = np.array(
        [
            _bazin(np.array([t]), 30000.0 * colour[b], 15.0)[0]
            for t, b in zip(times, bands)
        ]
    )
    return ObjectRequest(
        times=times,
        flux=flux,
        flux_err=0.03 * flux + 15.0,
        detected=flux > 100.0,
        bands=bands,
        eval_times=[-5.0, 10.0, 20.0, 20.0, 20.0, 40.0, 75.0, 90.0],
        eval_bands=["r", "g", "g", "r", "i", "r", "i", "z"],
        zero_point=ZeroPointDeclaration.AT_TRAINING_ZERO_POINT,
    )


def _lsst_sparse() -> ObjectRequest:
    """6 points in r and g over two weeks, below the training filter."""
    times = np.array([0.0, 2.0, 5.0, 7.0, 11.0, 14.0])
    bands = ["r", "g", "r", "g", "r", "g"]
    flux = _bazin(times, 8000.0, 3.0)
    return ObjectRequest(
        times=times,
        flux=flux,
        flux_err=0.05 * flux + 25.0,
        detected=[True] * 6,
        bands=bands,
        eval_times=[1.0, 6.0, 12.0, 20.0, 30.0, 30.0],
        eval_bands=["r", "g", "r", "g", "r", "g"],
        zero_point=27.5,
    )


def _roman_wfi() -> ObjectRequest:
    """36 points cycling F062, F106, F158 every 2 days, at 26.0 mag."""
    times = np.arange(36, dtype=float) * 2.0
    bands = ["F062", "F106", "F158"] * 12
    colour = {"F062": 1.0, "F106": 0.85, "F158": 0.7}
    # 26.0 mag puts flux at 10 ** (-0.4 * (27.5 - 26.0)) of its 27.5 value.
    scale = 10.0 ** (-0.4 * 1.5)
    flux = scale * np.array(
        [
            _bazin(np.array([t]), 20000.0 * colour[b], 20.0)[0]
            for t, b in zip(times, bands)
        ]
    )
    return ObjectRequest(
        times=times,
        flux=flux,
        flux_err=0.04 * flux + 5.0,
        detected=[True] * 36,
        bands=bands,
        eval_times=[0.0, 18.0, 30.0, 30.0, 30.0, 60.0, 80.0],
        eval_bands=["F062", "F087", "F062", "F129", "F184", "F158", "F106"],
        zero_point=26.0,
    )


def reference_light_curves() -> dict[str, ObjectRequest]:
    """Build the fixed light curves, keyed by name.

    Returns:
        A fresh request per name, in a stable order.
    """
    return {
        "lsst_well_sampled": _lsst_well_sampled(),
        "lsst_sparse": _lsst_sparse(),
        "roman_wfi": _roman_wfi(),
    }


def record(path: Path = REFERENCE_OUTPUTS_PATH) -> None:
    """Forecast every fixture and write the outputs as a Python module.

    Run it only from a process that has set up Django (``manage.py shell``),
    so torch's thread count matches the test runner's.

    Args:
        path: The module to (over)write.
    """
    import torch

    from seldon import services

    names = list(reference_light_curves())
    results = services.forecast(list(reference_light_curves().values()))
    outputs = {
        name: {
            "flux": [float(v) for v in result.flux],
            "flux_err": [float(v) for v in result.flux_err],
            "class_probabilities": {
                k: float(v) for k, v in sorted(result.class_probabilities.items())
            },
        }
        for name, result in zip(names, results)
    }
    provenance = results[0].provenance
    recorded_with = {
        "library_version": provenance.library_version,
        "checkpoint": provenance.checkpoint,
        "torch_version": provenance.torch_version,
        "torch_threads": torch.get_num_threads(),
    }
    path.write_text(
        '"""Recorded outputs for the fixtures in ``reference_light_curves.py``.\n\n'
        "Generated by ``reference_light_curves.record()``; do not edit by hand.\n"
        "See docs/developer/regenerating-reference-outputs.md.\n"
        '"""\n\n'
        f"RECORDED_WITH = {pprint.pformat(recorded_with, sort_dicts=False)}\n\n"
        f"REFERENCE_OUTPUTS = {pprint.pformat(outputs, sort_dicts=False)}\n"
    )
    logger.info("Recorded %d reference outputs to %s", len(outputs), path)
