"""Read a loaded checkpoint's capabilities and provenance, with no inference.

The band vocabulary lives in three separately built objects: the data
module's band index map, the encoder's band embedding, and the decoder's
bandpass embedding. They agree only because ``hparams.yaml`` passes the same
filter dictionary to each, and the library sorts its keys to assign indices.
The library also ships a legacy hardcoded map that disagrees with every
checkpoint built this way, so the vocabulary is read off the model and the
three copies are checked against each other here.

Nothing here imports Django and nothing here runs a forward pass.
"""

from collections.abc import Mapping
from types import MappingProxyType

import torch

from seldon.domain.models.capabilities import FluxStats, ModelCapabilities
from seldon.domain.models.provenance import TRAINING_ZERO_POINT_MAG, Provenance
from seldon.infrastructure.ml.loader import IncompatibleCheckpointError, LoadedModel


class BandVocabularyMismatchError(IncompatibleCheckpointError):
    """The model's band vocabularies disagree, so a band index is ambiguous."""


def check_band_vocabularies(
    data: Mapping[str, int],
    encoder: Mapping[str, int],
    decoder: Mapping[str, int],
    decoder_embedding_size: int,
) -> None:
    """Refuse a model whose three band vocabularies are not one vocabulary.

    Args:
        data: The data module's band name to index map.
        encoder: The encoder band embedding's map.
        decoder: The decoder bandpass embedding's map.
        decoder_embedding_size: How many rows the decoder's band embedding has.

    Raises:
        BandVocabularyMismatchError: If the encoder's or decoder's map differs
            from the data module's, or the highest index has no row in the
            decoder's embedding.
    """
    for name, other in (("encoder", encoder), ("decoder", decoder)):
        if dict(other) != dict(data):
            differing = sorted(
                band
                for band in set(data) | set(other)
                if data.get(band) != other.get(band)
            )
            raise BandVocabularyMismatchError(
                f"The {name}'s band vocabulary differs from the data module's "
                f"for bands {differing}; a band index would mean different "
                "filters on the two sides of the model."
            )
    highest = max(data.values())
    if highest >= decoder_embedding_size:
        raise BandVocabularyMismatchError(
            f"Band index {highest} has no row in the decoder's band embedding "
            f"of {decoder_embedding_size} rows."
        )


def read_capabilities(loaded: LoadedModel) -> ModelCapabilities:
    """Read what the checkpoint can serve off a loaded model.

    Args:
        loaded: A model from ``load_model``.

    Returns:
        The band vocabulary in index order, per-band training weights, flux
        statistics, time scale, class map, and training selection filter.

    Raises:
        BandVocabularyMismatchError: If the model's band vocabularies disagree.
    """
    experiment = loaded.experiment
    data = experiment.data
    decoder_embedding = experiment.model.decoder.band_emb
    band_index = dict(data.band_index_map)
    check_band_vocabularies(
        band_index,
        experiment.model.encoder.embedder.band_embedding.bandpass,
        decoder_embedding.bandpass,
        decoder_embedding.emb.num_embeddings,
    )

    ordered = dict(sorted(band_index.items(), key=lambda item: item[1]))
    stats = data.flux_stats
    return ModelCapabilities(
        band_index=MappingProxyType(ordered),
        band_weights=MappingProxyType(
            {band: float(data.band_weights[index]) for band, index in ordered.items()}
        ),
        flux_stats=FluxStats(
            min=float(stats["min"]),
            max=float(stats["max"]),
            mean=float(stats["mean"]),
            std=float(stats["std"]),
        ),
        time_scale=float(data.t_max),
        class_map=MappingProxyType(dict(data.class_map)),
        min_points=int(data.min_points),
    )


def read_provenance(loaded: LoadedModel) -> Provenance:
    """Build the provenance record for forecasts from a loaded model.

    Args:
        loaded: A model from ``load_model``.

    Returns:
        The library version, checkpoint identity, training zero point, and
        resolved torch version.
    """
    return Provenance(
        library_version=loaded.library_version,
        checkpoint=f"{loaded.hparams_path.parent.name}/{loaded.checkpoint_path.name}",
        zero_point_mag=TRAINING_ZERO_POINT_MAG,
        torch_version=torch.__version__,
    )
