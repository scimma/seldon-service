"""Whether one broker's scores qualify a detection for forecasting.

A detection qualifies when a single qualifying broker's reliability score
and SN-like score for it both meet that broker's own cut and threshold. A
missing score never qualifies, and a full-stream source never qualifies.
"""

from dataclasses import dataclass

from seldon.domain.intake.delivery import BrokerRole, Delivery


@dataclass(frozen=True)
class Thresholds:
    """One broker's qualification settings.

    Attributes:
        reliability_cut: The lowest reliability score that passes.
        sn_like_threshold: The lowest SN-like score that passes.
    """

    reliability_cut: float
    sn_like_threshold: float


def qualifies(delivery: Delivery, thresholds: Thresholds) -> bool:
    """Decide whether a delivery qualifies its detection.

    Args:
        delivery: One broker's delivery of the detection.
        thresholds: That broker's cut and threshold in effect.

    Returns:
        ``True`` when the broker is qualifying and both scores are present and
        meet their thresholds.
    """
    if delivery.role is not BrokerRole.QUALIFYING:
        return False
    if delivery.reliability is None or delivery.sn_like_score is None:
        return False
    return (
        delivery.reliability >= thresholds.reliability_cut
        and delivery.sn_like_score >= thresholds.sn_like_threshold
    )
