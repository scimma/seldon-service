"""Command-line entry point for the conformance kit."""

import argparse
import json
import sys
from dataclasses import asdict

from seldon_model_contract.conformance import check_package
from seldon_model_contract.discovery import load_package


def main(argv: list[str] | None = None) -> int:
    """Load a package by entry-point name, check it, and print the report.

    Args:
        argv: Command-line arguments; ``sys.argv[1:]`` when ``None``.

    Returns:
        0 when every check passes, 1 when any fails, 2 when the package
        cannot be loaded.
    """
    parser = argparse.ArgumentParser(
        prog="python -m seldon_model_contract.conformance",
        description="Check an installed model package against the SELDON model contract.",
    )
    parser.add_argument(
        "package", help="the package's seldon.model_packages entry-point name"
    )
    parser.add_argument(
        "--max-batch",
        type=int,
        default=32,
        help="largest batch size to test for batch independence (default 32)",
    )
    parser.add_argument("--json", action="store_true", help="print the report as JSON")
    args = parser.parse_args(argv)
    try:
        package = load_package(args.package)
    except Exception as error:  # noqa: BLE001 - any load failure is exit 2
        print(f"cannot load package: {error}", file=sys.stderr)
        return 2
    report = check_package(package, args.package, args.max_batch)
    if args.json:
        print(
            json.dumps(
                {
                    "package": report.package,
                    "passed": report.passed,
                    "checks": [asdict(r) for r in report.results],
                },
                indent=2,
            )
        )
    else:
        print(report.render())
    return 0 if report.passed else 1


if __name__ == "__main__":
    sys.exit(main())
