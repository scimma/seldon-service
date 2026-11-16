"""Forecast worker settings, read from ``SELDON_WORKER_``-prefixed variables.

A worker image holds the service plus exactly one model package, so the
package to load and the digest of its wheel are fixed when the image is
built and passed in here; the digest is what registration holds the release
to. Which heads run is not a worker setting: operators write head
revisions, so replicas of one deployment can never disagree about it.

The batch size is how many detections go to the package in one ``predict``
call. The lease is how long a worker may hold claimed work before another
replica may take it over, so it must exceed the slowest batch; the attempt
cap counts every lease, whether it ended in an error or lapsed because the
worker crashed or hung. An empty variable means the default.
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class WorkerSettings(BaseSettings):
    """Settings for one ``forecast_worker`` process.

    Attributes:
        package: The entry-point name of the model package to load.
        batch_size: Most detections sent to the package in one call;
            positive.
        poll_seconds: How long to wait after finding no work; not negative.
        attempts: How many leases a claim gets before it is recorded as a
            failure; positive.
        lease_seconds: How long a lease lasts before another replica may
            take the work over; positive.
        digest: The sha256 of the package's wheel, recorded in the image;
            the command's ``--digest`` overrides it.
    """

    package: str = Field(min_length=1)
    batch_size: int = Field(8, gt=0)
    poll_seconds: float = Field(1.0, ge=0)
    attempts: int = Field(3, gt=0)
    lease_seconds: float = Field(300, gt=0)
    digest: str | None = None

    model_config = SettingsConfigDict(
        env_prefix="SELDON_WORKER_", case_sensitive=False, env_ignore_empty=True
    )


def get_worker_settings() -> WorkerSettings:
    """Build the worker settings from the current environment.

    Returns:
        A freshly validated ``WorkerSettings`` instance.

    Raises:
        pydantic.ValidationError: If ``SELDON_WORKER_PACKAGE`` is unset or a
            variable holds a value its field rejects.
    """
    return WorkerSettings()
