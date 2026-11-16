"""The contract's version and the rule for whether two versions are compatible.

A version is ``"<major>.<minor>"``. Minor versions add things (a vocabulary
code, an optional field) that an older reader can ignore; a major version
changes something an older reader would misread. The service accepts a
package only when the major versions match and the package's minor version
is no newer than the service knows.
"""

from dataclasses import dataclass

CONTRACT_VERSION = "1.0"


@dataclass(frozen=True, order=True)
class ContractVersion:
    """A parsed contract version.

    Attributes:
        major: Incremented for changes an older reader would misread.
        minor: Incremented for additions an older reader can ignore.
    """

    major: int
    minor: int

    @classmethod
    def parse(cls, text: str) -> "ContractVersion":
        """Parse ``"<major>.<minor>"``.

        Args:
            text: The version string.

        Returns:
            The parsed version.

        Raises:
            ValueError: When ``text`` is not two non-negative integers joined
                by a dot.
        """
        parts = text.split(".") if isinstance(text, str) else []
        if len(parts) != 2 or not all(p.isdigit() for p in parts):
            raise ValueError(f"contract version must look like '1.0', got {text!r}")
        return cls(int(parts[0]), int(parts[1]))

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}"


def is_supported(package_version: str, service_version: str = CONTRACT_VERSION) -> bool:
    """Whether a service at ``service_version`` can serve a package's version.

    Args:
        package_version: The contract version the package declares.
        service_version: The contract version the service implements.

    Returns:
        ``True`` when the majors match and the package's minor is not newer.

    Raises:
        ValueError: When either version cannot be parsed.
    """
    package = ContractVersion.parse(package_version)
    service = ContractVersion.parse(service_version)
    return package.major == service.major and package.minor <= service.minor
