"""The conformance kit's findings and how they are printed."""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class CheckResult:
    """One check's outcome.

    Attributes:
        name: A short, stable name for the check.
        passed: Whether the package met it.
        message: What was checked, or what failed and for which object.
    """

    name: str
    passed: bool
    message: str


@dataclass
class Report:
    """Every check run against one package.

    Attributes:
        package: The entry-point name the kit loaded.
        results: Check outcomes in the order they ran.
    """

    package: str
    results: list[CheckResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        """Whether every check passed."""
        return all(r.passed for r in self.results)

    def add(self, name: str, passed: bool, message: str) -> None:
        """Record one check's outcome.

        Args:
            name: The check's name.
            passed: Whether it passed.
            message: What it found.
        """
        self.results.append(CheckResult(name, passed, message))

    def render(self) -> str:
        """Return the report as plain text, one line per check."""
        lines = [f"Conformance report for {self.package!r}"]
        for result in self.results:
            status = "PASS" if result.passed else "FAIL"
            lines.append(f"  {status}  {result.name}: {result.message}")
        failed = sum(not r.passed for r in self.results)
        lines.append(
            "Result: conforms"
            if failed == 0
            else f"Result: {failed} of {len(self.results)} checks failed"
        )
        return "\n".join(lines)
