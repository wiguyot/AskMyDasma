"""Shared AskMyDasma campaign JSON contract helpers."""

from __future__ import annotations

from typing import Any


STRUCTURED_TOP_LEVEL_FIELDS = {
    "dataset_path",
    "mode",
    "selected_groups",
    "excluded_fields",
    "selected_models",
    "models",
    "explainability",
    "reproducibility",
}

MODEL_FIELD_MAP: dict[str, dict[str, str]] = {
    "hstree": {
        "estimator": "hstree_estimator",
        "threshold_window": "hstree_threshold_window",
        "welford_window": "hstree_welford_window",
        "anomaly_k_sigma": "hstree_anomaly_k_sigma",
        "anomaly_warmup_window": "hstree_anomaly_warmup_window",
        "initial_window_size": "hstree_initial_window_size",
        "window_size": "hstree_window_size",
        "num_trees": "hstree_num_trees",
        "variant": "hstree_variant",
        "max_depth": "hstree_max_depth",
        "repeatability": "hstree_repeatability",
        "reproducibility": "hstree_reproducibility",
    },
    "iforest": {
        "estimator": "iforest_estimator",
        "threshold_window": "iforest_threshold_window",
        "welford_window": "iforest_welford_window",
        "anomaly_k_sigma": "iforest_anomaly_k_sigma",
        "anomaly_warmup_window": "iforest_anomaly_warmup_window",
        "initial_window_size": "iforest_initial_window_size",
        "window_size": "iforest_window_size",
        "n_estimators": "iforest_n_estimators",
        "max_samples": "iforest_max_samples",
        "contamination": "iforest_contamination",
        "refit_every": "iforest_refit_every",
        "repeatability": "iforest_repeatability",
        "reproducibility": "iforest_reproducibility",
    },
    "kitnet": {
        "estimator": "kitnet_estimator",
        "threshold_window": "kitnet_threshold_window",
        "welford_window": "kitnet_welford_window",
        "threshold_percentile": "kitnet_threshold_percentile",
        "anomaly_k_sigma": "kitnet_anomaly_k_sigma",
        "anomaly_warmup_window": "kitnet_anomaly_warmup_window",
        "max_size_ae": "kitnet_max_size_ae",
        "grace_feature_mapping": "kitnet_grace_feature_mapping",
        "grace_anomaly_detector": "kitnet_grace_anomaly_detector",
        "learning_rate": "kitnet_learning_rate",
        "hidden_ratio": "kitnet_hidden_ratio",
        "clip": "kitnet_clip",
        "repeatability": "kitnet_repeatability",
        "reproducibility": "kitnet_reproducibility",
    },
    "deepant": {
        "estimator": "deepant_estimator",
        "threshold_window": "deepant_threshold_window",
        "welford_window": "deepant_welford_window",
        "threshold_percentile": "deepant_threshold_percentile",
        "anomaly_k_sigma": "deepant_anomaly_k_sigma",
        "anomaly_warmup_window": "deepant_anomaly_warmup_window",
        "lookback_size": "deepant_lookback_size",
        "initial_window_size": "deepant_initial_window_size",
        "refit_every": "deepant_refit_every",
        "window_size": "deepant_window_size",
        "filters": "deepant_filters",
        "kernel_size": "deepant_kernel_size",
        "dropout": "deepant_dropout",
        "lr": "deepant_lr",
        "epochs_initial": "deepant_epochs_initial",
        "epochs_refit": "deepant_epochs_refit",
        "batch_size": "deepant_batch_size",
        "max_group_workers": "deepant_max_group_workers",
        "repeatability": "deepant_repeatability",
        "reproducibility": "deepant_reproducibility",
    },
    "mdragpoint": {
        "verdict_mode": "mdragpoint_verdict_mode",
        "estimator": "mdragpoint_estimator",
        "threshold_window": "mdragpoint_threshold_window",
        "welford_window": "mdragpoint_welford_window",
        "threshold_percentile": "mdragpoint_threshold_percentile",
        "anomaly_k_sigma": "mdragpoint_anomaly_k_sigma",
        "anomaly_warmup_window": "mdragpoint_anomaly_warmup_window",
        "dynamic_threshold": "mdragpoint_dynamic_threshold",
        "lof_init": "mdragpoint_lof_init",
        "n_init": "mdragpoint_n_init",
        "max_nb_clusters": "mdragpoint_max_nb_clusters",
        "max_cluster_size": "mdragpoint_max_cluster_size",
        "lof_neighbors": "mdragpoint_lof_neighbors",
        "repeatability": "mdragpoint_repeatability",
        "reproducibility": "mdragpoint_reproducibility",
    },
}

EXPLAINABILITY_FIELD_MAP = {
    "nsamples": "explainability_nsamples",
    "v2_nsamples": "explainability_v2_nsamples",
    "model": "explainability_model",
    "mode": "explainability_mode",
    "method": "explainability_method",
    "v2_model": "explainability_v2_model",
    "v2_mode": "explainability_v2_mode",
    "v2_method": "explainability_v2_method",
}

REPRODUCIBILITY_FIELD_MAP = {
    "source_role": "reproducibility_source_role",
    "campaign_id": "reproducibility_campaign_id",
}


def campaign_request_to_config_payload(
    request: Any,
    *,
    selected_groups: list[str],
    excluded_fields: list[str],
) -> dict[str, Any]:
    """Convert a validated campaign request object to structured runtime JSON."""
    models: dict[str, dict[str, Any]] = {}
    for model_name, field_map in MODEL_FIELD_MAP.items():
        model_config: dict[str, Any] = {}
        for structured_key, request_attr in field_map.items():
            model_config[structured_key] = getattr(request, request_attr)
        models[model_name] = model_config

    return {
        "dataset_path": request.dataset_path,
        "mode": request.mode,
        "selected_groups": list(selected_groups),
        "excluded_fields": list(excluded_fields),
        "selected_models": list(request.selected_models),
        "models": models,
        "explainability": {
            "nsamples": request.explainability_nsamples,
            "v2_nsamples": request.explainability_v2_nsamples,
            "model": request.explainability_model,
            "mode": request.explainability_mode,
            "method": request.explainability_method,
            "v2_model": request.explainability_v2_model,
            "v2_mode": request.explainability_v2_mode,
            "v2_method": request.explainability_v2_method,
        },
        "reproducibility": {
            "source_role": request.reproducibility_source_role,
            "campaign_id": request.reproducibility_campaign_id,
        },
    }


def flatten_structured_campaign(raw: dict[str, Any]) -> dict[str, Any]:
    """Convert structured campaign JSON to the flat request/runtime contract."""
    if not {"models", "explainability", "reproducibility"} & set(raw):
        return raw

    unknown_top_level = sorted(set(raw) - STRUCTURED_TOP_LEVEL_FIELDS)
    if unknown_top_level:
        raise ValueError(f"unknown campaign config field(s): {', '.join(unknown_top_level)}")

    flattened: dict[str, Any] = {
        key: value
        for key, value in raw.items()
        if key not in {"models", "explainability", "reproducibility"}
    }

    models = raw.get("models", {})
    if models is None:
        models = {}
    if not isinstance(models, dict):
        raise ValueError("campaign config field 'models' must be an object")

    for model_name, model_config in models.items():
        if model_name not in MODEL_FIELD_MAP:
            raise ValueError(f"unknown campaign model config: {model_name}")
        if not isinstance(model_config, dict):
            raise ValueError(f"campaign model config '{model_name}' must be an object")

        field_map = MODEL_FIELD_MAP[model_name]
        unknown_model_fields = sorted(set(model_config) - set(field_map))
        if unknown_model_fields:
            raise ValueError(
                f"unknown campaign field(s) for model {model_name}: {', '.join(unknown_model_fields)}"
            )
        for structured_key, flat_key in field_map.items():
            if structured_key in model_config:
                flattened[flat_key] = model_config[structured_key]

    explainability = raw.get("explainability", {})
    if explainability is None:
        explainability = {}
    if not isinstance(explainability, dict):
        raise ValueError("campaign config field 'explainability' must be an object")

    unknown_explainability_fields = sorted(set(explainability) - set(EXPLAINABILITY_FIELD_MAP))
    if unknown_explainability_fields:
        raise ValueError(
            "unknown campaign field(s) for explainability: "
            + ", ".join(unknown_explainability_fields)
        )
    for structured_key, flat_key in EXPLAINABILITY_FIELD_MAP.items():
        if structured_key in explainability:
            flattened[flat_key] = explainability[structured_key]

    reproducibility = raw.get("reproducibility", {})
    if reproducibility is None:
        reproducibility = {}
    if not isinstance(reproducibility, dict):
        raise ValueError("campaign config field 'reproducibility' must be an object")

    unknown_reproducibility_fields = sorted(set(reproducibility) - set(REPRODUCIBILITY_FIELD_MAP))
    if unknown_reproducibility_fields:
        raise ValueError(
            "unknown campaign field(s) for reproducibility: "
            + ", ".join(unknown_reproducibility_fields)
        )
    for structured_key, flat_key in REPRODUCIBILITY_FIELD_MAP.items():
        if structured_key in reproducibility:
            flattened[flat_key] = reproducibility[structured_key]

    return flattened
