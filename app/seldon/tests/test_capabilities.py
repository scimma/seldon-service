"""Tests for capability introspection and provenance.

Expected values come from the checkpoint's ``hparams.yaml`` and from the
library's legacy band map, never from the code under test. The model is the
process-wide cached one from ``seldon.services``, as in ``test_loader``.
"""

import math
from contextlib import ExitStack
from unittest import mock

import yaml
from django.test import SimpleTestCase

from seldon import services
from seldon.config.settings import get_settings
from seldon.infrastructure.ml.capabilities import (
    BandVocabularyMismatchError,
    check_band_vocabularies,
    read_capabilities,
    read_provenance,
)

# The six LSST and six Roman WFI bands with finite training weight. Roman
# F146 and F213 and every JWST NIRCam band carry none (U4 finding).
EXPECTED_TRAINED_BANDS = frozenset(
    {"u", "g", "r", "i", "z", "y", "F062", "F087", "F106", "F129", "F158", "F184"}
)


def _hparams() -> dict:
    """The checkpoint's ``hparams.yaml``, parsed independently of the adapter."""
    return yaml.safe_load(get_settings().hparams_path.read_text())


class BandVocabularyTests(SimpleTestCase):
    """The band vocabulary is the model's own, not the library's legacy map."""

    def test_vocabulary_is_alphabetical_and_not_the_legacy_map(self) -> None:
        """Indices follow the sorted filter names, so ``r`` is 39, not 1."""
        from seldon_core.datasets.bandpasses import BAND_IDX_MAP

        filedict = _hparams()["dataset"]["config"]["band_index_map"]["config"][
            "filedict"
        ]

        band_index = read_capabilities(services.loaded_model()).band_index

        self.assertEqual(list(band_index), sorted(filedict))
        self.assertEqual(list(band_index.values()), list(range(len(filedict))))
        self.assertEqual(band_index["r"], 39)
        self.assertNotEqual(band_index["r"], BAND_IDX_MAP["r"])

    def test_vocabularies_agree_and_a_mismatch_is_rejected(self) -> None:
        """The three maps on the model agree; a swapped or oversized one fails."""
        model = services.loaded_model().experiment.model
        data = dict(services.loaded_model().experiment.data.band_index_map)
        encoder = dict(model.encoder.embedder.band_embedding.bandpass)
        decoder = dict(model.decoder.band_emb.bandpass)
        size = model.decoder.band_emb.emb.num_embeddings
        self.assertEqual(data, encoder)
        self.assertEqual(data, decoder)
        check_band_vocabularies(data, encoder, decoder, size)

        swapped = dict(decoder, r=decoder["g"], g=decoder["r"])
        with self.subTest("decoder disagrees"):
            with self.assertRaises(BandVocabularyMismatchError) as caught:
                check_band_vocabularies(data, encoder, swapped, size)
            self.assertIn("decoder", str(caught.exception))
        with self.subTest("index beyond the decoder's embedding"):
            with self.assertRaises(BandVocabularyMismatchError):
                check_band_vocabularies(data, encoder, decoder, len(data) - 1)


class TrainingSupportTests(SimpleTestCase):
    """Bands with infinite training weight are reported as untrained."""

    def test_infinite_weight_bands_are_untrained(self) -> None:
        """Exactly the bands hparams.yaml weights finitely count as trained."""
        dataset = _hparams()["dataset"]["config"]
        names = sorted(dataset["band_index_map"]["config"]["filedict"])
        finite = {
            names[index]
            for index, weight in dataset["band_weights"].items()
            if math.isfinite(weight)
        }
        self.assertEqual(finite, EXPECTED_TRAINED_BANDS)

        capabilities = read_capabilities(services.loaded_model())

        self.assertEqual(capabilities.trained_bands, EXPECTED_TRAINED_BANDS)
        self.assertEqual(
            capabilities.untrained_bands, frozenset(names) - EXPECTED_TRAINED_BANDS
        )
        self.assertEqual(capabilities.min_points, dataset["min_points"])
        self.assertEqual(capabilities.time_scale, dataset["flux_stats"]["t_max"])
        self.assertEqual(capabilities.class_map, dataset["class_map"])
        self.assertEqual(capabilities.flux_stats.std, dataset["flux_stats"]["std"])


class ProvenanceTests(SimpleTestCase):
    """A forecast can be traced to library, checkpoint, zero point, and torch."""

    def test_provenance_names_every_input(self) -> None:
        """No field of the provenance record is empty."""
        provenance = read_provenance(services.loaded_model())

        self.assertTrue(provenance.library_version)
        self.assertTrue(provenance.checkpoint)
        self.assertEqual(provenance.zero_point_mag, 27.5)
        self.assertTrue(provenance.torch_version)


class NoInferenceTests(SimpleTestCase):
    """Capability and provenance reads never run the network."""

    def test_reads_need_no_forward_pass(self) -> None:
        """Every module's ``forward`` raises, yet both reads succeed."""
        loaded = services.loaded_model()
        with ExitStack() as stack:
            for module in loaded.experiment.modules():
                stack.enter_context(
                    mock.patch.object(
                        module,
                        "forward",
                        side_effect=AssertionError("forward pass during a read"),
                    )
                )
            capabilities = read_capabilities(loaded)
            provenance = read_provenance(loaded)

        self.assertEqual(len(capabilities.band_index), 43)
        self.assertTrue(provenance.torch_version)
