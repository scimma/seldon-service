"""The conformance kit: check an installed model package against SPEC.md.

Run it in a package's CI before handing over a release::

    python -m seldon_model_contract.conformance <entry-point-name>

It exits 0 when every check passes, 1 when any fails, and 2 when the package
cannot be loaded.
"""

from typing import Any

from seldon_model_contract.conformance.checks import (
    load_samples,
    outcomes_agree,
    requests_for,
    run_checks,
)
from seldon_model_contract.conformance.report import CheckResult, Report


def check_package(package: Any, name: str = "package", max_batch: int = 32) -> Report:
    """Run every conformance check against a loaded package.

    Args:
        package: An object providing ``describe()`` and ``predict()``.
        name: The name to show in the report.
        max_batch: The largest batch size to test for batch independence.

    Returns:
        The report.
    """
    return run_checks(package, Report(name), max_batch)


__all__ = [
    "CheckResult",
    "Report",
    "check_package",
    "load_samples",
    "outcomes_agree",
    "requests_for",
]
