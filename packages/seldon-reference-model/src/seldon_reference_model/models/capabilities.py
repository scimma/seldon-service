"""What a loaded checkpoint can serve, read off the model without inference.

These are plain values: no torch types and no library objects, so a view, a
serializer, or a regime check can hold them without touching the model.
"""

import math
from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class FluxStats:
    """The flux normalization statistics recorded at training time.

    Attributes:
        min: Minimum training flux.
        max: Maximum training flux.
        mean: Mean training flux.
        std: Standard deviation of training flux.
    """

    min: float
    max: float
    mean: float
    std: float


@dataclass(frozen=True)
class ModelCapabilities:
    """Everything the adapter needs to know about a checkpoint before a forecast.

    Attributes:
        band_index: Band name to the index the model uses for it, in index
            order. Indices come from sorting the checkpoint's filter names, so
            they are read off the model, never assumed.
        band_weights: Band name to its training-time loss weight. A weight is
            infinite exactly where a band contributed no training
            observations.
        flux_stats: The flux normalization statistics.
        time_scale: The training-time time normalization divisor, in days. It
            is a scale, not a bound on the evaluation grid's span.
        class_map: Class name to class index, as the checkpoint records it.
        min_points: The training-time selection filter: light curves with
            fewer observations were never trained on.
    """

    band_index: Mapping[str, int]
    band_weights: Mapping[str, float]
    flux_stats: FluxStats
    time_scale: float
    class_map: Mapping[str, int]
    min_points: int

    @property
    def trained_bands(self) -> frozenset[str]:
        """Return the bands that carried training observations.

        Returns:
            The names of bands whose training weight is finite.
        """
        return frozenset(
            band for band, weight in self.band_weights.items() if math.isfinite(weight)
        )

    @property
    def untrained_bands(self) -> frozenset[str]:
        """Return the bands the model knows by name but never saw data in.

        Returns:
            The names of bands whose training weight is infinite.
        """
        return frozenset(self.band_weights) - self.trained_bands
