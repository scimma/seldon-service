"""The checkpoint's ``hparams.yaml``, read directly for tests.

Tests take expected values from here rather than from the adapter, so a
mistake in the adapter's reading of the file cannot hide in its own tests.
"""

from typing import Any

import yaml

from seldon.config.settings import get_settings


def read_hparams() -> dict[str, Any]:
    """Parse the configured checkpoint's ``hparams.yaml``.

    Returns:
        The parsed file.
    """
    return yaml.safe_load(get_settings().hparams_path.read_text())


def band_names_in_index_order() -> list[str]:
    """Return the checkpoint's band names in index order.

    The library numbers bands by sorting the filter dictionary's keys.

    Returns:
        The band names, sorted.
    """
    dataset = read_hparams()["dataset"]["config"]
    return sorted(dataset["band_index_map"]["config"]["filedict"])
