"""The checks the conformance kit runs against an installed package.

Each check exercises one rule of ``SPEC.md`` through the package's own
``describe`` and ``predict``, using the sample packets under ``samples/`` and
variants derived from them. A check that cannot run because an earlier one
failed is reported as failed with the reason, never skipped silently.
"""

import json
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from importlib import resources
from typing import Any

from seldon_model_contract.heads import Declaration, HeadRequest, InputKind, TimeGrid
from seldon_model_contract.packets import Packet
from seldon_model_contract.results import ObjectResult, Outcome, Product, Refusal
from seldon_model_contract.version import CONTRACT_VERSION, is_supported
from seldon_model_contract.vocabulary import CaveatCode, RefusalCode

from seldon_model_contract.conformance.report import Report

REL_TOL = 1e-4
ABS_TOL = 1e-4
SAMPLE_FILES = ("rubin.json", "ztf.json", "roman.json", "mixed.json")
FALLBACK_UNACCEPTED = ("conformance-kit", "unknown")


def load_samples() -> list[Packet]:
    """Return every bundled sample packet, in file order."""
    folder = resources.files("seldon_model_contract.conformance") / "samples"
    packets: list[Packet] = []
    for name in SAMPLE_FILES:
        data = json.loads((folder / name).read_text(encoding="utf-8"))
        packets.extend(Packet.from_dict(p) for p in data)
    return packets


def requests_for(
    declaration: Declaration, packets: Sequence[Packet]
) -> list[HeadRequest]:
    """Build a request for every declared head, resolving default inputs.

    Args:
        declaration: The package's declaration.
        packets: The batch the requests are for.

    Returns:
        One request per head, with concrete inputs for every packet.
    """
    requests = []
    for head in declaration.heads:
        inputs: dict[str, dict[InputKind, tuple[Any, ...]]] = {}
        for packet in packets:
            times = [p.time for p in packet.points]
            concrete: dict[InputKind, tuple[Any, ...]] = {}
            for kind, template in head.inputs.items():
                if isinstance(template, TimeGrid):
                    concrete[kind] = (
                        template.resolve(min(times), max(times)) if times else ()
                    )
                else:
                    concrete[kind] = tuple(template)
            inputs[packet.object_id] = concrete
        requests.append(HeadRequest(head.name, inputs))
    return requests


def _variant(packet: Packet, index: int) -> Packet:
    """Return a distinct, deterministic copy of ``packet`` for batch checks."""
    scale = 1.0 + 0.05 * index
    shift = 0.25 * index
    return replace(
        packet,
        object_id=f"{packet.object_id}-v{index}",
        points=tuple(
            replace(
                p, time=p.time + shift, flux=p.flux * scale, flux_err=p.flux_err * scale
            )
            for p in packet.points
        ),
    )


def _close(a: Any, b: Any) -> bool:
    """Whether two JSON-like values agree within the contract's tolerance."""
    if isinstance(a, bool) or isinstance(b, bool):
        return a == b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return math.isclose(a, b, rel_tol=REL_TOL, abs_tol=ABS_TOL)
    if isinstance(a, Mapping) and isinstance(b, Mapping):
        return a.keys() == b.keys() and all(_close(a[k], b[k]) for k in a)
    if isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)):
        return len(a) == len(b) and all(_close(x, y) for x, y in zip(a, b))
    return a == b


def outcomes_agree(a: Outcome, b: Outcome) -> bool:
    """Whether two outcomes agree within the contract's tolerance.

    Args:
        a: One outcome.
        b: The other.

    Returns:
        ``True`` when both are refusals with the same code, or both are
        products of the same kind with the same caveat codes and payloads
        that agree within tolerance.
    """
    if isinstance(a, Refusal) or isinstance(b, Refusal):
        return isinstance(a, Refusal) and isinstance(b, Refusal) and a.code == b.code
    return (
        a.kind == b.kind
        and sorted(c.code for c in a.caveats) == sorted(c.code for c in b.caveats)
        and _close(a.payload, b.payload)
    )


class IncompleteResultsError(AssertionError):
    """A predict call did not return exactly what the contract requires."""


def completeness_problem(
    declaration: Declaration, packets: Sequence[Packet], results: Sequence[ObjectResult]
) -> str | None:
    """Describe how ``results`` break the one-outcome-per-head rule, if they do.

    Args:
        declaration: The package's declaration; every head was requested.
        packets: The batch that was sent.
        results: What ``predict`` returned for it.

    Returns:
        ``None`` when there is one result per packet in packet order, each
        with exactly one outcome per declared head and products of the
        declared kind; otherwise a message naming the first problem.
    """
    if [r.object_id for r in results] != [p.object_id for p in packets]:
        return (
            f"batch of {len(packets)}: results are not one per packet in packet order"
        )
    names = {h.name for h in declaration.heads}
    for result in results:
        for head in declaration.heads:
            outcome = result.outcomes.get(head.name)
            if outcome is None:
                return f"{result.object_id}: no outcome for head {head.name!r}"
            if isinstance(outcome, Product) and outcome.kind != head.output_kind:
                return (
                    f"{result.object_id}: head {head.name!r} returned "
                    f"{outcome.kind.value}, declared {head.output_kind.value}"
                )
        extra = set(result.outcomes) - names
        if extra:
            return f"{result.object_id}: outcomes for unrequested heads {sorted(extra)}"
    return None


class Run:
    """One conformance run: the package, its declaration, and the report."""

    def __init__(self, package: Any, report: Report, max_batch: int) -> None:
        self.package = package
        self.report = report
        self.max_batch = max_batch
        self.declaration: Declaration | None = None

    def predict(self, packets: Sequence[Packet]) -> list[ObjectResult]:
        """Call the package for ``packets`` with every head requested.

        Every call is held to the completeness rule, so a package cannot drop
        objects or heads at batch sizes only one check uses.

        Raises:
            IncompleteResultsError: When the results break that rule.
        """
        assert self.declaration is not None
        results = list(
            self.package.predict(packets, requests_for(self.declaration, packets))
        )
        problem = completeness_problem(self.declaration, packets, results)
        if problem:
            raise IncompleteResultsError(problem)
        return results

    def check_declaration(self) -> bool:
        """The declaration is well formed and its contract version supported."""
        try:
            declaration = self.package.describe()
            if not isinstance(declaration, Declaration):
                raise TypeError("describe() must return a Declaration")
            again = Declaration.from_dict(json.loads(json.dumps(declaration.to_dict())))
        except Exception as error:  # noqa: BLE001 - any package error fails the check
            self.report.add("declaration", False, f"{type(error).__name__}: {error}")
            return False
        if not is_supported(declaration.contract_version, CONTRACT_VERSION):
            self.report.add(
                "declaration",
                False,
                f"contract version {declaration.contract_version} is not supported "
                f"by this kit ({CONTRACT_VERSION})",
            )
            return False
        if again.to_dict() != declaration.to_dict():
            self.report.add("declaration", False, "declaration does not survive JSON")
            return False
        if not declaration.heads:
            self.report.add("declaration", False, "the package declares no heads")
            return False
        self.declaration = declaration
        self.report.add(
            "declaration",
            True,
            f"{declaration.package_name} {declaration.package_version}, "
            f"{len(declaration.heads)} heads, {len(declaration.accepted)} accepted pairs",
        )
        return True

    def _guarded(self, name: str, body: Callable[[], str | None]) -> None:
        """Run a check, turning an exception or a failure message into FAIL."""
        try:
            failure = body()
        except Exception as error:  # noqa: BLE001 - any package error fails the check
            self.report.add(name, False, f"{type(error).__name__}: {error}")
            return
        self.report.add(name, failure is None, failure or "ok")

    def check_completeness(self, samples: Sequence[Packet]) -> None:
        """One result per packet, in order, with one outcome per requested head."""

        def body() -> str | None:
            self.predict(samples)
            return None

        self._guarded("completeness", body)

    def check_vocabulary(self, samples: Sequence[Packet]) -> None:
        """Every refusal and caveat code is in the contract vocabulary."""

        def body() -> str | None:
            for result in self.predict(samples):
                for head, outcome in result.outcomes.items():
                    if isinstance(outcome, Refusal):
                        unknown = [] if outcome.is_known else [outcome.code]
                    else:
                        unknown = [c.code for c in outcome.caveats if not c.is_known]
                    if unknown:
                        return f"{result.object_id}/{head}: codes {unknown} are not in the vocabulary"
            return None

        self._guarded("vocabulary", body)

    def _unaccepted_pair(self) -> tuple[str, str]:
        assert self.declaration is not None
        for pair in (("ztf", "g"), ("ztf", "r"), FALLBACK_UNACCEPTED):
            if pair not in self.declaration.accepted:
                return pair
        raise AssertionError("no unaccepted pair available")

    def _accepted_base(self, samples: Sequence[Packet]) -> Packet | None:
        assert self.declaration is not None
        best = None
        for packet in samples:
            kept = tuple(
                p
                for p in packet.points
                if (p.survey, p.band) in self.declaration.accepted
            )
            if kept and (best is None or len(kept) > len(best.points)):
                best = replace(
                    packet, object_id=f"{packet.object_id}-accepted", points=kept
                )
        return best

    def check_exclusion(self, samples: Sequence[Packet]) -> None:
        """Unaccepted points are excluded with a caveat; only-unaccepted objects are refused."""

        def body() -> str | None:
            base = self._accepted_base(samples)
            if base is None:
                return "no sample has points in an accepted pair"
            survey, band = self._unaccepted_pair()
            foreign = tuple(
                replace(p, survey=survey, band=band, flux=p.flux * 3.0)
                for p in base.points[: max(3, len(base.points) // 3)]
            )
            mixed = replace(base, object_id="mixed", points=base.points + foreign)
            only_foreign = replace(base, object_id="only-foreign", points=foreign)
            clean, dirty, alien = self.predict([base, mixed, only_foreign])
            for head, outcome in dirty.outcomes.items():
                expected = clean.outcomes[head]
                if isinstance(outcome, Product):
                    excluded = [
                        c
                        for c in outcome.caveats
                        if c.code == CaveatCode.EXCLUDED_POINTS
                    ]
                    named = excluded and [survey, band] in [
                        list(p) for p in excluded[0].detail.get("pairs", ())
                    ]
                    if not named:
                        return f"{head}: product for a packet with {survey} {band} points has no excluded_points caveat naming them"
                    if excluded[0].detail.get("n_points") != len(foreign):
                        return f"{head}: excluded_points caveat reports n_points={excluded[0].detail.get('n_points')!r}, expected {len(foreign)}"
                    stripped = replace(
                        outcome,
                        caveats=tuple(
                            c
                            for c in outcome.caveats
                            if c.code != CaveatCode.EXCLUDED_POINTS
                        ),
                    )
                    if not outcomes_agree(stripped, expected):
                        return f"{head}: {survey} {band} points changed the product, so they were not excluded"
                elif not (
                    isinstance(expected, Refusal) and expected.code == outcome.code
                ):
                    return f"{head}: refused a packet that still has accepted points ({outcome.code}); it should use them and add an excluded_points caveat"
            for head, outcome in alien.outcomes.items():
                if not (
                    isinstance(outcome, Refusal)
                    and outcome.code == RefusalCode.UNKNOWN_BAND
                ):
                    return f"{head}: a packet with only {survey} {band} points was not refused with unknown_band"
                if [survey, band] not in [
                    list(p) for p in outcome.detail.get("pairs", ())
                ]:
                    return f"{head}: unknown_band refusal does not name {survey} {band} in detail.pairs"
            return None

        self._guarded("exclusion", body)

    def check_batch_independence(self, samples: Sequence[Packet]) -> None:
        """An object's results do not depend on its batch or batch size."""

        def body() -> str | None:
            pool = [
                _variant(samples[i % len(samples)], i) for i in range(self.max_batch)
            ]
            alone = {p.object_id: self.predict([p])[0] for p in pool[:8]}
            for size in sorted({8, self.max_batch}):
                batch = list(reversed(pool[:size]))
                for result in self.predict(batch):
                    reference = alone.get(result.object_id)
                    if reference is None:
                        continue
                    for head, outcome in result.outcomes.items():
                        if not outcomes_agree(outcome, reference.outcomes[head]):
                            return f"{result.object_id}/{head} differs in a batch of {size}"
            return None

        self._guarded("batch_independence", body)

    def check_json_round_trip(self, samples: Sequence[Packet]) -> None:
        """Results survive a JSON round trip."""

        def body() -> str | None:
            for result in self.predict(samples):
                again = ObjectResult.from_dict(json.loads(json.dumps(result.to_dict())))
                if again.to_dict() != result.to_dict():
                    return f"{result.object_id}: result does not survive JSON"
            return None

        self._guarded("json_round_trip", body)

    def check_determinism(self, samples: Sequence[Packet]) -> None:
        """The same inputs give the same results."""

        def body() -> str | None:
            first, second = self.predict(samples), self.predict(samples)
            for a, b in zip(first, second):
                for head, outcome in a.outcomes.items():
                    if not outcomes_agree(outcome, b.outcomes[head]):
                        return (
                            f"{a.object_id}/{head} differs between two identical calls"
                        )
            return None

        self._guarded("determinism", body)


def run_checks(package: Any, report: Report, max_batch: int = 32) -> Report:
    """Run every check against ``package`` and fill ``report``.

    Args:
        package: The loaded model package.
        report: The report to fill.
        max_batch: The largest batch size to test for batch independence.

    Returns:
        The filled report.
    """
    run = Run(package, report, max(8, max_batch))
    if not run.check_declaration():
        return report
    samples = load_samples()
    run.check_completeness(samples)
    run.check_vocabulary(samples)
    run.check_exclusion(samples)
    run.check_batch_independence(samples)
    run.check_json_round_trip(samples)
    run.check_determinism(samples)
    return report


__all__ = ["load_samples", "outcomes_agree", "requests_for", "run_checks"]
