"""Where a forecast came from: library, checkpoint, zero point, and runtime."""

from dataclasses import dataclass

# The AB magnitude zero point the training fluxes are expressed in. Neither
# pinned artifact records it: seldon_core v1.2.0 does not, and hparams.yaml
# carries only a flux divisor (``zero_point_flux: 1``). It comes from the
# author's ``Tutorial.ipynb``, which rescales input photometry with
# ``10**(-0.4 * (zero_point_mag - 27.5))``.
TRAINING_ZERO_POINT_MAG = 27.5


@dataclass(frozen=True)
class Provenance:
    """The pinned inputs that together determine a forecast's values.

    Attributes:
        library_version: The installed ``seldon_core`` version.
        checkpoint: The checkpoint, as ``<checkpoint directory>/<file>``.
        zero_point_mag: The magnitude zero point of the model's flux scale.
        torch_version: The resolved torch version; a different runtime can
            move floating-point outputs.
    """

    library_version: str
    checkpoint: str
    zero_point_mag: float
    torch_version: str
