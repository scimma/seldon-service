---
title: SELDON servable bands - what the checkpoint's band names mean
date: 2026-10-05
module: ml-runtime
problem_type: data-contract
tags: [seldon, bands, filters, checkpoint, lsst, roman, jwst]
---

# SELDON servable bands - what the checkpoint's band names mean

Finding from U4 of `docs/plans/2026-08-26-0512-feat-model-serving-foundation-plan.md`.

## Verdict

The checkpoint `seldon-2.0-roman-elasticc` (`epoch=1086-val_loss=1.18.ckpt`) knows **43
band names, and they denote LSST, Roman WFI, and JWST NIRCam filters only.** Only **12 of
them carry training weight**: the six LSST bands (`u g r i z y`) and six of the eight Roman
WFI bands (`F062 F087 F106 F129 F158 F184`). Roman `F146` and `F213` and all 29 JWST
NIRCam bands have infinite weight, meaning they contributed no training observations.

There are **no ZTF, ATLAS, Pan-STARRS, Swift, or Johnson-Cousins names**. The model has no
way to tell where photometry came from: the band name is the only thing that selects the
transmission curve. A ZTF `g` point sent as `g` is treated as LSST `g`, and that
relabelling **cannot be detected** by the service or the model. Photometry from any other
system must be converted to one of these filters by the caller, or it is outside what this
checkpoint was trained on.

## How indices are assigned

The library builds each band map with `BandPass.from_files`, which sorts the filter
dictionary's keys and numbers them in that order. Python sorts uppercase before lowercase,
so the Roman and JWST names (`F...`) take indices 0-36 and the LSST names take 37-42. `r`
is index **39**. The library's legacy hardcoded map (`BAND_IDX_MAP`, `legacy_band_index_map`,
where `r` is 1) disagrees with every index below and must not be used with this checkpoint.

The same filter dictionary is passed separately to the data module
(`experiment.data.band_index_map`), the encoder's band embedding
(`experiment.model.encoder.embedder.band_embedding.bandpass`), and the decoder's bandpass
embedding (`experiment.model.decoder.band_emb.bandpass`; `bandpass_embedding` in
`hparams.yaml`). `seldon.infrastructure.ml.capabilities.read_capabilities` reads the
vocabulary off the data module and raises `BandVocabularyMismatchError` if the other two
disagree or the highest index has no row in the decoder's embedding.

## Band table

| Index | Band name | Instrument | Filter file | Trained (weight) |
|---|---|---|---|---|
| 0 | `F062` | Roman WFI | `Roman_WFI.F062.dat` | yes (2.197) |
| 1 | `F070W` | JWST NIRCam | `jwst_nircam_f070w.dat` | no (inf) |
| 2 | `F087` | Roman WFI | `Roman_WFI.F087.dat` | yes (1.858) |
| 3 | `F090W` | JWST NIRCam | `jwst_nircam_f090w.dat` | no (inf) |
| 4 | `F106` | Roman WFI | `Roman_WFI.F106.dat` | yes (1.154) |
| 5 | `F115W` | JWST NIRCam | `jwst_nircam_f115w.dat` | no (inf) |
| 6 | `F129` | Roman WFI | `Roman_WFI.F129.dat` | yes (1.166) |
| 7 | `F140M` | JWST NIRCam | `jwst_nircam_f140m.dat` | no (inf) |
| 8 | `F146` | Roman WFI | `Roman_WFI.F146.dat` | no (inf) |
| 9 | `F150W` | JWST NIRCam | `jwst_nircam_f150w.dat` | no (inf) |
| 10 | `F150W2` | JWST NIRCam | `jwst_nircam_f150w2.dat` | no (inf) |
| 11 | `F158` | Roman WFI | `Roman_WFI.F158.dat` | yes (3.195) |
| 12 | `F162M` | JWST NIRCam | `jwst_nircam_f162m.dat` | no (inf) |
| 13 | `F164N` | JWST NIRCam | `jwst_nircam_f164n.dat` | no (inf) |
| 14 | `F182M` | JWST NIRCam | `jwst_nircam_f182m.dat` | no (inf) |
| 15 | `F184` | Roman WFI | `Roman_WFI.F184.dat` | yes (3.533) |
| 16 | `F187N` | JWST NIRCam | `jwst_nircam_f187n.dat` | no (inf) |
| 17 | `F200W` | JWST NIRCam | `jwst_nircam_f200w.dat` | no (inf) |
| 18 | `F210M` | JWST NIRCam | `jwst_nircam_f210m.dat` | no (inf) |
| 19 | `F212N` | JWST NIRCam | `jwst_nircam_f212n.dat` | no (inf) |
| 20 | `F213` | Roman WFI | `Roman_WFI.F213.dat` | no (inf) |
| 21 | `F250M` | JWST NIRCam | `jwst_nircam_f250m.dat` | no (inf) |
| 22 | `F277W` | JWST NIRCam | `jwst_nircam_f277w.dat` | no (inf) |
| 23 | `F300M` | JWST NIRCam | `jwst_nircam_f300m.dat` | no (inf) |
| 24 | `F322W2` | JWST NIRCam | `jwst_nircam_f322w2.dat` | no (inf) |
| 25 | `F323N` | JWST NIRCam | `jwst_nircam_f323n.dat` | no (inf) |
| 26 | `F335M` | JWST NIRCam | `jwst_nircam_f335m.dat` | no (inf) |
| 27 | `F356W` | JWST NIRCam | `jwst_nircam_f356w.dat` | no (inf) |
| 28 | `F360M` | JWST NIRCam | `jwst_nircam_f360m.dat` | no (inf) |
| 29 | `F405N` | JWST NIRCam | `jwst_nircam_f405n.dat` | no (inf) |
| 30 | `F410M` | JWST NIRCam | `jwst_nircam_f410m.dat` | no (inf) |
| 31 | `F430M` | JWST NIRCam | `jwst_nircam_f430m.dat` | no (inf) |
| 32 | `F444W` | JWST NIRCam | `jwst_nircam_f444w.dat` | no (inf) |
| 33 | `F460M` | JWST NIRCam | `jwst_nircam_f460m.dat` | no (inf) |
| 34 | `F466N` | JWST NIRCam | `jwst_nircam_f466n.dat` | no (inf) |
| 35 | `F470N` | JWST NIRCam | `jwst_nircam_f470n.dat` | no (inf) |
| 36 | `F480M` | JWST NIRCam | `jwst_nircam_f480m.dat` | no (inf) |
| 37 | `g` | LSST | `total_g.dat` | yes (1.348) |
| 38 | `i` | LSST | `total_i.dat` | yes (0.506) |
| 39 | `r` | LSST | `total_r.dat` | yes (0.526) |
| 40 | `u` | LSST | `total_u.dat` | yes (1.898) |
| 41 | `y` | LSST | `total_y.dat` | yes (0.552) |
| 42 | `z` | LSST | `total_z.dat` | yes (0.578) |

An untrained band can still be evaluated: the model has its transmission curve and will
produce a forecast in it. That forecast is an extrapolation in wavelength, which is why the
regime indicator (R10) distinguishes trained from untrained bands rather than refusing
them.

## How this table was produced

Generated, not typed. A throwaway script (not committed) ran inside the dev container
(`seldon_core` v1.2.0, `torch 2.9.0+cpu`). It loaded the baked-in checkpoint through
`seldon.services.loaded_model()` and took index, band name, and weight from
`seldon.infrastructure.ml.capabilities.read_capabilities` (`band_index`, `band_weights`).
It took each filter file name from `dataset.config.band_index_map.config.filedict` in the
checkpoint's `hparams.yaml`, and mapped the file-name prefix to the instrument:
`total_*.dat` is LSST, `jwst_nircam_*.dat` is JWST NIRCam, and `Roman_WFI.*.dat` is Roman
WFI. Weights are `dataset.config.band_weights` from `hparams.yaml`, keyed by index.
`seldon/tests/test_capabilities.py` asserts the trained set and `r` -> 39 against
`hparams.yaml` independently.
