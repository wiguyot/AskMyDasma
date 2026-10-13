"""Compute the expected services of the current campaign for the admin console.

The classification mirrors scripts/web/campaign_config.py (compose_services,
MODEL_COMPOSE_SERVICES, repeatability/reproducibility rules) by importing that
module, exactly like CampaignService._load_campaign_config_module does. The
frontend never re-implements these rules.
"""

from __future__ import annotations

import importlib.util
import logging
import sys
from pathlib import Path
from types import ModuleType

from ..models import ServiceGroup, ServiceStatusView

logger = logging.getLogger(__name__)

# Admin-only presentation metadata: compose service -> (group, role, permanent).
# Groups follow docs/planning/askmydasma-admin-redesign.md section 5.
_SERVICE_META: dict[str, tuple[ServiceGroup, str, bool]] = {
    "kafka1": ("Infrastructure", "Kafka broker 1", True),
    "kafka2": ("Infrastructure", "Kafka broker 2", True),
    "kafka3": ("Infrastructure", "Kafka broker 3", True),
    "schema-registry": ("Infrastructure", "Avro schemas", True),
    "init-topic": ("Infrastructure", "Topic initialization", False),
    "producer": ("Infrastructure", "Source data producer", True),
    "influxdb": ("Infrastructure", "Time series storage", True),
    "influxdb-init-buckets": ("Infrastructure", "InfluxDB buckets setup", False),
    "influxdb-apply-template": ("Infrastructure", "InfluxDB dashboard template", False),
    "askmydasma-web": ("Infrastructure", "Backend and UI", True),
    "hstree-analysis": ("Detectors", "HSTree anomaly detection", True),
    "iforest-analysis": ("Detectors", "IForest anomaly detection", True),
    "kitnet-analysis": ("Detectors", "KitNET anomaly detection", True),
    "deepant-analysis": ("Detectors", "DeepAnT anomaly detection", True),
    "mdragstream-point-analysis": ("Detectors", "mDragStream Point-based detection", True),
    "explainability": ("Explainability", "SHAP explanations V1", True),
    "explainability-v2": ("Explainability", "SHAP explanations V2", True),
    "explainability-shap-diff": ("Explainability", "V1/V2 SHAP comparison", True),
    "repeatability-v1": ("Validation", "Repeatability instance 1", True),
    "repeatability-v2": ("Validation", "Repeatability instance 2", True),
    "repeatability-diff": ("Validation", "Repeatability output diff", True),
    "reproducibility-export": ("Validation", "Reproducibility CSV export", True),
    "consensus-analysis": ("Observability", "Consensus metrics", True),
    "build-metrics": ("Observability", "Topic metrics builder", True),
    "telegraf": ("Observability", "Host metrics collector", True),
    "grafana": ("Observability", "Dashboards", True),
}

# Presentation order inside each group: expected services first, then the
# others; within each subset the admin card order is kept.
_GROUP_ORDER: tuple[ServiceGroup, ...] = (
    "Infrastructure",
    "Detectors",
    "Explainability",
    "Validation",
    "Observability",
)

# Known compose services not covered by _SERVICE_META (viz utilities run on
# demand or inside containers; they stay visible only if Docker reports them).
_FALLBACK_ROLE = "Support service"
_FALLBACK_GROUP: ServiceGroup = "Infrastructure"


def _load_campaign_config(scripts_web_dir: Path) -> ModuleType:
    """Import campaign_config.py as a module, mirroring the orchestrator loader."""
    spec = importlib.util.spec_from_file_location(
        "askmydasma_expected_services_config",
        scripts_web_dir / "campaign_config.py",
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load campaign config from {scripts_web_dir}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _campaign_config_dict(campaign) -> dict | None:
    """Build the raw campaign config dict used by compose_services()."""
    from ..campaign_config_contract import campaign_request_to_config_payload

    if campaign is None:
        return None
    try:
        return campaign_request_to_config_payload(
            _request_from_campaign_view(campaign),
            selected_groups=campaign.selected_groups,
            excluded_fields=campaign.excluded_fields,
        )
    except Exception as exc:
        logger.warning("[expected-services] cannot build campaign config: %s", exc)
        return None


def _request_from_campaign_view(campaign):
    """Rebuild a launch-request-like object from the persisted CampaignView."""
    from ..models import CampaignLaunchRequest

    return CampaignLaunchRequest(
        dataset_path=campaign.dataset_path,
        mode=campaign.mode,
        selected_groups=campaign.selected_groups,
        excluded_fields=campaign.excluded_fields,
        selected_models=campaign.selected_models,
        hstree_estimator=campaign.hstree_estimator,
        hstree_threshold_window=campaign.hstree_threshold_window,
        hstree_welford_window=campaign.hstree_welford_window,
        hstree_anomaly_k_sigma=campaign.hstree_anomaly_k_sigma,
        hstree_anomaly_warmup_window=campaign.hstree_anomaly_warmup_window,
        hstree_initial_window_size=campaign.hstree_initial_window_size,
        hstree_window_size=campaign.hstree_window_size,
        hstree_num_trees=campaign.hstree_num_trees,
        hstree_variant=campaign.hstree_variant,
        hstree_max_depth=campaign.hstree_max_depth,
        iforest_estimator=campaign.iforest_estimator,
        iforest_threshold_window=campaign.iforest_threshold_window,
        iforest_welford_window=campaign.iforest_welford_window,
        iforest_anomaly_k_sigma=campaign.iforest_anomaly_k_sigma,
        iforest_anomaly_warmup_window=campaign.iforest_anomaly_warmup_window,
        iforest_initial_window_size=campaign.iforest_initial_window_size,
        iforest_window_size=campaign.iforest_window_size,
        iforest_n_estimators=campaign.iforest_n_estimators,
        iforest_max_samples=campaign.iforest_max_samples,
        iforest_contamination=campaign.iforest_contamination,
        iforest_refit_every=campaign.iforest_refit_every,
        kitnet_estimator=campaign.kitnet_estimator,
        kitnet_threshold_window=campaign.kitnet_threshold_window,
        kitnet_welford_window=campaign.kitnet_welford_window,
        kitnet_threshold_percentile=campaign.kitnet_threshold_percentile,
        kitnet_anomaly_k_sigma=campaign.kitnet_anomaly_k_sigma,
        kitnet_anomaly_warmup_window=campaign.kitnet_anomaly_warmup_window,
        kitnet_max_size_ae=campaign.kitnet_max_size_ae,
        kitnet_grace_feature_mapping=campaign.kitnet_grace_feature_mapping,
        kitnet_grace_anomaly_detector=campaign.kitnet_grace_anomaly_detector,
        kitnet_learning_rate=campaign.kitnet_learning_rate,
        kitnet_hidden_ratio=campaign.kitnet_hidden_ratio,
        kitnet_clip=campaign.kitnet_clip,
        deepant_estimator=campaign.deepant_estimator,
        deepant_threshold_window=campaign.deepant_threshold_window,
        deepant_welford_window=campaign.deepant_welford_window,
        deepant_threshold_percentile=campaign.deepant_threshold_percentile,
        deepant_anomaly_k_sigma=campaign.deepant_anomaly_k_sigma,
        deepant_anomaly_warmup_window=campaign.deepant_anomaly_warmup_window,
        deepant_lookback_size=campaign.deepant_lookback_size,
        deepant_initial_window_size=campaign.deepant_initial_window_size,
        deepant_refit_every=campaign.deepant_refit_every,
        deepant_window_size=campaign.deepant_window_size,
        deepant_filters=campaign.deepant_filters,
        deepant_kernel_size=campaign.deepant_kernel_size,
        deepant_dropout=campaign.deepant_dropout,
        deepant_lr=campaign.deepant_lr,
        deepant_epochs_initial=campaign.deepant_epochs_initial,
        deepant_epochs_refit=campaign.deepant_epochs_refit,
        deepant_batch_size=campaign.deepant_batch_size,
        deepant_max_group_workers=campaign.deepant_max_group_workers,
        mdragpoint_verdict_mode=campaign.mdragpoint_verdict_mode,
        mdragpoint_estimator=campaign.mdragpoint_estimator,
        mdragpoint_threshold_window=campaign.mdragpoint_threshold_window,
        mdragpoint_welford_window=campaign.mdragpoint_welford_window,
        mdragpoint_threshold_percentile=campaign.mdragpoint_threshold_percentile,
        mdragpoint_anomaly_k_sigma=campaign.mdragpoint_anomaly_k_sigma,
        mdragpoint_anomaly_warmup_window=campaign.mdragpoint_anomaly_warmup_window,
        mdragpoint_dynamic_threshold=campaign.mdragpoint_dynamic_threshold,
        mdragpoint_lof_init=campaign.mdragpoint_lof_init,
        mdragpoint_n_init=campaign.mdragpoint_n_init,
        mdragpoint_max_nb_clusters=campaign.mdragpoint_max_nb_clusters,
        mdragpoint_max_cluster_size=campaign.mdragpoint_max_cluster_size,
        mdragpoint_lof_neighbors=campaign.mdragpoint_lof_neighbors,
        hstree_repeatability=campaign.hstree_repeatability,
        iforest_repeatability=campaign.iforest_repeatability,
        kitnet_repeatability=campaign.kitnet_repeatability,
        deepant_repeatability=campaign.deepant_repeatability,
        mdragpoint_repeatability=campaign.mdragpoint_repeatability,
        hstree_reproducibility=campaign.hstree_reproducibility,
        iforest_reproducibility=campaign.iforest_reproducibility,
        kitnet_reproducibility=campaign.kitnet_reproducibility,
        deepant_reproducibility=campaign.deepant_reproducibility,
        mdragpoint_reproducibility=campaign.mdragpoint_reproducibility,
        reproducibility_source_role=campaign.reproducibility_source_role,
        reproducibility_campaign_id=campaign.reproducibility_campaign_id,
        explainability_nsamples=campaign.explainability_nsamples,
        explainability_v2_nsamples=campaign.explainability_v2_nsamples,
        explainability_model=campaign.explainability_model,
        explainability_mode=campaign.explainability_mode,
        explainability_method=campaign.explainability_method,
        explainability_v2_model=campaign.explainability_v2_model,
        explainability_v2_mode=campaign.explainability_v2_mode,
        explainability_v2_method=campaign.explainability_v2_method,
    )


def _always_expected() -> set[str]:
    """Infrastructure and web services the console itself depends on."""
    return {
        "kafka1",
        "kafka2",
        "kafka3",
        "schema-registry",
        "influxdb",
        "askmydasma-web",
    }


def expected_service_names(
    campaign,
    scripts_web_dir: Path,
    config_module: ModuleType | None = None,
) -> dict[str, dict] | None:
    """Return {compose_service: metadata} for the campaign's expected services.

    Returns None when the expectation cannot be computed (no campaign, config
    module unavailable or config invalid); the caller then emits expected=None
    entries instead of guessing.
    """
    try:
        module = config_module or _load_campaign_config(scripts_web_dir)
        config = _campaign_config_dict(campaign)
        if config is None:
            return None
        compose = set(module.compose_services(config))
        expected = _always_expected() | compose
        if compose:
            # Production also relies on Grafana and Telegraf for observability.
            expected |= {"grafana", "telegraf"}
        return {
            name: {
                "expected": True,
                "role": _SERVICE_META.get(name, (_FALLBACK_GROUP, _FALLBACK_ROLE, True))[1],
                "group": _SERVICE_META.get(name, (_FALLBACK_GROUP, _FALLBACK_ROLE, True))[0],
                "permanent": _SERVICE_META.get(name, (_FALLBACK_GROUP, _FALLBACK_ROLE, True))[2],
            }
            for name in expected
        }
    except Exception as exc:
        logger.warning("[expected-services] computation failed: %s", exc)
        return None


def merge_services(
    observed: list[ServiceStatusView],
    expected: dict[str, dict] | None,
) -> list[ServiceStatusView]:
    """Merge Docker observations with campaign expectations.

    - Expected services without an observed container are inserted with
      state="not_started" and health="not_configured" so their absence stays
      visible.
    - Observed services inherit expected/role/group/permanent when known.
    - Groups and ordering follow the admin presentation rules.
    """
    by_name: dict[str, ServiceStatusView] = {}
    for service in observed:
        if service.name:
            by_name[service.name] = service

    merged: list[ServiceStatusView] = []
    if expected is None:
        # Expectations unknown: keep observations, mark everything unknown.
        for service in observed:
            service.expected = None
            merged.append(service)
        return merged

    names = set(by_name) | set(expected)
    for name in names:
        meta = _SERVICE_META.get(name)
        group = meta[0] if meta else _FALLBACK_GROUP
        role = meta[1] if meta else _FALLBACK_ROLE
        permanent = meta[2] if meta else True
        observed_service = by_name.get(name)
        is_expected = name in expected
        if observed_service is None:
            if not is_expected:
                # Unknown observed service: never synthesize a placeholder.
                continue
            merged.append(ServiceStatusView(
                name=name,
                container_name=None,
                state="not_started",
                health="not_configured",
                status="not_started",
                expected=True,
                role=role,
                group=group,
                permanent=permanent,
            ))
        else:
            observed_service.expected = is_expected
            observed_service.role = role
            observed_service.group = group
            observed_service.permanent = permanent
            merged.append(observed_service)

    return _sort_services(merged)


def _sort_services(services: list[ServiceStatusView]) -> list[ServiceStatusView]:
    group_rank = {group: rank for rank, group in enumerate(_GROUP_ORDER)}
    return sorted(
        services,
        key=lambda s: (
            group_rank.get(s.group or _FALLBACK_GROUP, len(_GROUP_ORDER)),
            0 if (s.expected or s.permanent is not False) else 1,
            s.name or "",
        ),
    )
