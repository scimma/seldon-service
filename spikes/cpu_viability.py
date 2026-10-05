"""Throwaway U1 spike: does SELDON forecast on CPU within 60 seconds?

Loads the checkpoint by the shortest path that works (following the author's
notebook, not the eventual adapter design), then times forecasts over a sweep
of observation counts, a padded mixed-length batch, and sparse inputs. Prints
one JSON document with every measurement the U1 finding needs.

Run inside the spike image with the checkpoint directory mounted at /ckpt.
Deleted in U2.
"""

import json
import os
import sys
import time
import traceback
from pathlib import Path

import numpy as np
import psutil
import torch
from omegaconf import OmegaConf

from seldon_core.datasets.dataset_factory import get_module as get_dataset
from seldon_core.experiments.factory import get_module as get_experiment
from seldon_core.models.factory import get_module as get_model

CKPT_DIR = Path(os.environ.get("CKPT_DIR", "/ckpt"))
TRAINING_ZP = 27.5  # from the author's notebook; recorded in neither artifact
LIMIT = 60.0
OLD_PREFIX = "/projects/ncsa/caps/skai/software/Transient-Foundation-Model/TAE/data/"
TRAINED_LSST = ["u", "g", "r", "i", "z", "y"]


def rss_mb() -> float:
    return psutil.Process().memory_info().rss / 2**20


def repoint_filters(node, data_dir: Path) -> int:
    """Rewrite every training-time filter path in place; return how many."""
    count = 0
    if isinstance(node, dict):
        for key, value in node.items():
            if isinstance(value, str) and value.startswith(OLD_PREFIX):
                node[key] = str(data_dir / value[len(OLD_PREFIX) :])
                count += 1
            else:
                count += repoint_filters(value, data_dir)
    elif isinstance(node, list):
        for item in node:
            count += repoint_filters(item, data_dir)
    return count


def load(result: dict):
    import TAE

    data_dir = Path(TAE.__file__).parent / "data"
    hparams = CKPT_DIR / "hparams.yaml"
    ckpt_file = next((CKPT_DIR / "checkpoints").glob("*.ckpt"))
    config = OmegaConf.to_container(OmegaConf.load(hparams))
    result["filter_paths_rewritten"] = repoint_filters(config, data_dir)
    result["filter_data_dir"] = str(data_dir)

    ds = config["dataset"]["config"]
    ds["setup_from_data"] = False
    ds["num_workers"] = 0
    ds["pin_memory"] = False
    ds["augmentations"] = ["FullSample"]

    t0 = time.perf_counter()
    model = get_model(config["model_params"]["name"], config["model_params"]["config"])
    data = get_dataset(config["dataset"]["name"], ds)
    data.band_array = []  # library defect: the no-data branch reads it unset
    data.setup()
    experiment = get_experiment(
        config["exp_params"]["name"],
        {"model": model, "data": data, **config["exp_params"]["config"]},
    )
    checkpoint = torch.load(ckpt_file, map_location="cpu", weights_only=True)
    keys = experiment.load_state_dict(checkpoint["state_dict"], strict=False)
    experiment.eval()
    result["load_seconds"] = time.perf_counter() - t0
    result["missing_keys"] = sorted(keys.missing_keys)
    result["unexpected_keys"] = sorted(keys.unexpected_keys)
    result["class_map"] = dict(experiment.class_map)
    result["num_bands"] = len(data.band_index_map)
    result["rss_after_load_mb"] = rss_mb()
    return experiment


def synthetic_curve(n: int, rng: np.random.Generator) -> dict:
    """An SN-like rise and fall in trained LSST bands at the training ZP."""
    mjd = np.sort(rng.uniform(60000.0, 60120.0, n))
    peak = 60040.0
    shape = np.exp(-0.5 * ((mjd - peak) / 15.0) ** 2) + 0.02
    flux = 5000.0 * shape
    flux_err = np.full(n, 50.0)
    flux = flux + rng.normal(0.0, 50.0, n)
    band = np.array([TRAINED_LSST[i % len(TRAINED_LSST)] for i in range(n)])
    return {"mjd": mjd, "flux": flux, "flux_err": flux_err, "band": band}


def encode(experiment, curve: dict, n_eval: int) -> dict:
    """Notebook-equivalent encoding, with the eval-grid sort fixed."""
    t_scale = experiment.data.dataset.flux_stats["t_max"]
    order = np.argsort(curve["mjd"])
    mjd = curve["mjd"][order]
    mjd_eval = np.linspace(mjd[0], mjd[0] + 150.0, n_eval)
    band_eval = np.array([TRAINED_LSST[i % 6] for i in range(n_eval)])
    t0 = mjd[0] / t_scale
    t = torch.from_numpy(mjd / t_scale).float() - t0
    t_eval = torch.from_numpy(mjd_eval / t_scale).float() - t0
    flux, flux_err = experiment.data.dataset.transform(
        torch.from_numpy(curve["flux"][order]).float(),
        torch.from_numpy(curve["flux_err"][order]).float(),
    )
    det = torch.from_numpy((curve["flux"][order] / curve["flux_err"][order] > 4)).float()
    bim = experiment.data.band_index_map
    return {
        "x_input": torch.stack([t, flux, flux_err, det], dim=1),
        "time_full": t_eval.unsqueeze(-1),
        "band_idx": torch.tensor([bim[b] for b in curve["band"][order]]).unsqueeze(-1),
        "band_idx_full": torch.tensor([bim[b] for b in band_eval]).unsqueeze(-1),
        "pad_mask_part": torch.ones(len(mjd), dtype=torch.bool),
        "class_name": "SNIa",
    }


def stack(items: list) -> dict:
    n_in = max(i["x_input"].shape[0] for i in items)
    n_full = max(i["time_full"].shape[0] for i in items)
    out = {}
    for key in items[0]:
        if not isinstance(items[0][key], torch.Tensor):
            out[key] = [i[key] for i in items]
            continue
        target = n_full if "full" in key else n_in
        padded = []
        for i in items:
            x = i[key]
            pad = [0, 0] * (x.dim() - 1) + [0, target - x.shape[0]]
            padded.append(torch.nn.functional.pad(x, pad, value=0))
        out[key] = torch.stack(padded, dim=0)
    return out


def forecast(experiment, batch: dict) -> tuple[float, dict]:
    t0 = time.perf_counter()
    with torch.inference_mode():
        out = experiment(batch)
        experiment.post_process_prediction(out["reconstructed"], out["error"])
    return time.perf_counter() - t0, out


def main() -> None:
    torch.manual_seed(0)
    rng = np.random.default_rng(0)
    result = {
        "torch_version": torch.__version__,
        "cuda_available": torch.cuda.is_available(),
        "torch_threads": torch.get_num_threads(),
        "visible_cpus": len(os.sched_getaffinity(0)),
        "cpu_quota": open("/sys/fs/cgroup/cpu.max").read().strip(),
        "compile_disabled": os.environ.get("TORCH_COMPILE_DISABLE"),
    }
    experiment = load(result)

    # Warm-up pass so the sweep measures steady state, then record it too.
    warm, _ = forecast(experiment, stack([encode(experiment, synthetic_curve(30, rng), 64)]))
    result["first_forecast_seconds"] = warm

    sweep = {}
    for n in [3, 5, 10, 30, 100, 256, 400]:
        batch = stack([encode(experiment, synthetic_curve(n, rng), n + 64)])
        seconds, out = forecast(experiment, batch)
        sweep[n] = {"seconds": seconds, "clears_bar": seconds < LIMIT}
        if n == 30:
            result["output_keys"] = sorted(out.keys())
            result["output_shapes"] = {
                k: list(v.shape) for k, v in out.items() if isinstance(v, torch.Tensor)
            }
            result["deterministic"] = bool(
                torch.equal(out["reconstructed"], forecast(experiment, batch)[1]["reconstructed"])
            )
            with torch.inference_mode():
                probs = experiment.model.predict_class(out["z_mean"], probs=True)
            result["class_probs_shape"] = list(probs.shape)
            result["class_probs_sum"] = float(probs.sum())
    result["single_object_sweep"] = sweep

    mixed_lengths = [5, 30, 100, 256, 400, 10, 50, 200]
    items = [encode(experiment, synthetic_curve(n, rng), n + 64) for n in mixed_lengths]
    seconds, _ = forecast(experiment, stack(items))
    result["mixed_batch"] = {
        "lengths": mixed_lengths,
        "seconds_total": seconds,
        "seconds_per_object": seconds / len(items),
        "clears_bar": seconds < LIMIT,
    }

    floor = {}
    for n in [1, 2, 3, 5]:
        try:
            batch = stack([encode(experiment, synthetic_curve(n, rng), 64)])
            _, out = forecast(experiment, batch)
            floor[n] = {"ok": bool(torch.isfinite(out["reconstructed"]).all())}
        except Exception as exc:  # the spike records the break, it does not handle it
            floor[n] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"[:300]}
    result["sparse_floor"] = floor

    result["peak_rss_mb"] = rss_mb()
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmHWM"):
                result["vm_hwm"] = line.split(":", 1)[1].strip()
    json.dump(result, sys.stdout, indent=2, default=str)
    print()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        sys.exit(1)
