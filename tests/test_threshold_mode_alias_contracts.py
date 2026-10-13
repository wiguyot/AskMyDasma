"""Contract tests: all threshold-mode normalizers accept the documented aliases."""

from __future__ import annotations

from dasma_kafka.common.threshold_modes import (
    normalize_deepant_threshold_mode,
    normalize_hstree_threshold_mode,
    normalize_iforest_threshold_mode,
    normalize_kitnet_threshold_mode,
    normalize_mdragpoint_threshold_mode,
)


def test_percentile_internal_services_accept_campaign_p99_alias() -> None:
    normalizers = (
        normalize_hstree_threshold_mode,
        normalize_deepant_threshold_mode,
        normalize_mdragpoint_threshold_mode,
    )

    for normalize in normalizers:
        assert normalize("p99") == "percentile"
        assert normalize(" P99 ") == "percentile"
        assert normalize("percentile") == "percentile"


def test_p99_internal_services_accept_percentile_alias() -> None:
    normalizers = (
        normalize_iforest_threshold_mode,
        normalize_kitnet_threshold_mode,
    )

    for normalize in normalizers:
        assert normalize("p99") == "p99"
        assert normalize("percentile") == "p99"
        assert normalize(" Percentile ") == "p99"


def test_all_threshold_mode_normalizers_preserve_welford_modes() -> None:
    normalizers = (
        normalize_hstree_threshold_mode,
        normalize_deepant_threshold_mode,
        normalize_mdragpoint_threshold_mode,
        normalize_iforest_threshold_mode,
        normalize_kitnet_threshold_mode,
    )

    for normalize in normalizers:
        assert normalize("welford") == "welford"
        assert normalize("welford-windowed") == "welford-windowed"