# SELDON Model Contract, version 1.0

This document is the normative description of how the SELDON service and a model package work together. The Python types in `seldon_model_contract` implement it; where this text and the types disagree, report it as a bug.

The service takes alerts from Rubin brokers, decides which detections are owed a product, and stores and distributes the products. A model package, shipped by the team that owns the model, turns photometry into products. The package never talks to brokers, databases, or consumers; the service never looks inside the model.

## 1. Versioning

- A contract version is `<major>.<minor>`, currently `1.0`.
- A minor version adds things an older reader can ignore: a vocabulary code, an optional field, a new output kind a package may declare.
- A major version changes something an older reader would misread.
- The service runs a package only when the major versions match and the package's minor version is not newer than the service's. It refuses to start a package that fails this rule.

## 2. Discovery

A package is a Python distribution that registers one object under the entry-point group `seldon.model_packages`:

```toml
[project.entry-points."seldon.model_packages"]
my-model = "my_model.package:PACKAGE"
```

The object is either a package instance or a class or factory that returns one when called with no arguments. A package provides two calls, `describe()` and `predict()`.

## 3. Packets

The service sends one packet per object. In JSON:

```json
{
  "object_id": "170141183460469231731687303715884105727",
  "points": [
    {"survey": "lsst", "band": "g", "time": 61000.12, "flux": 1834.2, "flux_err": 41.0, "zero_point": 31.4, "detected": true}
  ],
  "metadata": {"ra": 150.11, "dec": -30.27},
  "extensions": {}
}
```

- `object_id` is unique within a batch.
- Each point carries `survey` (lower case, such as `lsst`, `ztf`, `roman`), `band` (in that survey's own spelling, such as `g` or `F062`), `time` (MJD, TAI), `flux` and `flux_err` (in the units its zero point sets), `zero_point` (the AB magnitude zero point of `flux`), and an optional `detected` (default `true`; `false` marks a forced or upper-limit measurement).
- `zero_point` may be `null` when the source did not state one. The package, not the packet, decides what that means; a package that needs it refuses the object with `missing_zero_point`.
- A point missing `survey`, `band`, `time`, `flux`, or `flux_err`, or carrying a non-finite number, is malformed.
- `metadata` holds scalar facts about the object.
- `extensions` holds additional modalities (spectra, host-galaxy data) that later contract versions define. A package ignores names it does not know, and a reader ignores top-level keys it does not know.
- Points arrive in no required order.

The survey and band together identify a filter. A package must never treat a point from one survey as if it came from another survey with the same band name.

## 4. Declaration

`describe()` returns the package's declaration:

```json
{
  "package_name": "seldon-reference-model",
  "package_version": "2.0.0",
  "contract_version": "1.0",
  "accepted": [["lsst", "g"], ["lsst", "r"]],
  "heads": [
    {
      "name": "flux_forecast",
      "output_kind": "flux_series",
      "inputs": {
        "times": {"start_days": 0.0, "days_past_last": 60.0, "step_days": 1.0},
        "bands": [["lsst", "u"], ["lsst", "g"]]
      },
      "description": "Flux at requested times and bands."
    },
    {"name": "classification", "output_kind": "class_probabilities", "inputs": {}}
  ]
}
```

- `package_version` identifies an immutable release. Rebuilding a release with different code under the same version is not allowed; the service detects it and refuses.
- `accepted` lists every `[survey, band]` pair the package can use.
- `heads` lists the decoder heads, with unique names. Each names an output kind (section 6) and its inputs as defaults (section 5). The service runs a head only after an operator enables it, and may replace the default inputs.

## 5. Head inputs

v1 defines two input kinds.

| Kind | Declared as (template) | Sent as (concrete, per object) |
|---|---|---|
| `times` | `{"start_days", "days_past_last", "step_days"}`: a grid from the object's first observation plus `start_days` through its last observation plus `days_past_last`, inclusive, `step_days` apart | a list of MJD values |
| `bands` | a list of `[survey, band]` pairs | the same list |

The service resolves templates for each object, so a package always receives concrete inputs. A head with no inputs takes only the packet. Adding an input kind is a contract-version change.

## 6. Output kinds

| Kind | Payload |
|---|---|
| `flux_series` | `times`, `bands` (`[survey, band]` pairs), `flux`, `flux_err` as aligned lists, plus `zero_point` (AB magnitude of the returned flux) |
| `class_probabilities` | `probabilities`: class name to probability |
| `scalar` | `value`, `uncertainty`, and an optional `unit` |
| `named_values` | `values`: name to number |

A payload may carry extra keys; readers keep them. Embeddings, latent vectors, and other model internals are not products and do not appear in payloads.

## 7. predict

```text
predict(packets, requests) -> results
```

- `packets` is a batch of packets; `requests` lists the heads to run. Each request names a head and carries concrete inputs for each object id:

  ```json
  {"head": "flux_forecast", "inputs": {"<object_id>": {"times": [61000.0, 61001.0], "bands": [["lsst", "g"]]}}}
  ```

- The package returns exactly one result per packet, in packet order. Each result holds exactly one outcome for every requested head:

  ```json
  {
    "object_id": "<object_id>",
    "outcomes": {
      "flux_forecast": {"outcome": "product", "kind": "flux_series", "payload": {}, "caveats": []},
      "classification": {"outcome": "refusal", "code": "unknown_band", "message": "...", "detail": {}}
    }
  }
  ```

- **Per-object outcomes.** A problem with one object becomes that object's refusal. It never fails the batch or affects other objects.
- **Batch independence.** An object's results agree, within relative 1e-4 for fluxes and absolute 1e-4 for probabilities, whichever other objects share its batch and whatever the batch size. The service chooses batch size and composition. (Population-level inference, where results depend on other objects, needs a later major version.)
- **Determinism.** The same inputs give the same results within the same tolerance.
- **Exceptions.** Raise only for a failure of the package itself (out of memory, a bug). The service retries a failing batch in smaller pieces and records a service-side failure for an object that keeps failing.

## 8. Refusals

A refusal means no product for that object and head. Its `code` comes from this vocabulary:

| Code | Meaning |
|---|---|
| `missing_zero_point` | A point the package needs has no zero point. |
| `unknown_band` | No point is in a survey and band pair the package accepts. |
| `no_usable_observations` | Points are present but none can condition the model. |
| `incompatible_checkpoint` | The package cannot serve this request with the model it carries. |

`message` explains it for people; `detail` carries structured facts.

## 9. Caveats

A caveat means the product is returned but should be read with care. Each has a `code` and structured `detail`:

| Code | Meaning | Detail |
|---|---|---|
| `sparse_light_curve` | Fewer usable observations than the model was trained on. | `n_observations`, `min_points` |
| `untrained_band` | An observed or requested band carried no training data. | `bands`: `[survey, band]` pairs |
| `eval_span_beyond_training` | Requested times reach past the spans the model was trained on. | `eval_span_days` |
| `excluded_points` | Points from pairs the package does not accept were left out. | `pairs`: `[survey, band]` pairs, `n_points` |

The package decides which caveats apply. It must not return a product that hides a reason it could not defend; when an input is outside its validated range, it says so in a caveat or refuses.

## 10. Points the package does not accept

When a packet holds points from survey and band pairs outside `accepted`:

- if accepted points remain, the package uses only those and adds an `excluded_points` caveat naming the excluded pairs;
- if none remain, it refuses the object with `unknown_band`, naming the pairs in `detail.pairs`.

## 11. Unknown codes

A reader keeps a refusal or caveat code it does not know, as given. The service stores it verbatim and logs a warning. The conformance kit fails a package that emits a code outside this version's vocabulary; propose new codes as a minor version instead.

## 12. Conformance

`python -m seldon_model_contract.conformance <entry-point-name>` checks an installed package against this document and exits non-zero on any failure. Run it in the package's CI before handing over a release.
