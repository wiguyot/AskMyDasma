#!/usr/bin/env python3
"""Read an AskMyDasma campaign JSON and emit shell env or action metadata."""

from __future__ import annotations

import argparse
import json
import re
import shlex
import sys
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from dasma_kafka.web.campaign_config_contract import flatten_structured_campaign
from dasma_kafka.iforest_analysis.helpers import parse_iforest_contamination, parse_iforest_max_samples
from dasma_kafka.iforest_analysis.model import validate_iforest_params


DEFAULTS: dict[str, Any] = {
    "dataset_path": "",
    "mode": "batch",
    "selected_groups": [],
    "excluded_fields": [],
    "selected_models": ["hstree", "iforest", "kitnet"],
    "hstree_estimator": "p99",
    "hstree_threshold_window": 500,
    "hstree_welford_window": 2048,
    "hstree_anomaly_k_sigma": 3.0,
    "hstree_anomaly_warmup_window": 100,
    "hstree_initial_window_size": 256,
    "hstree_window_size": 256,
    "hstree_num_trees": 25,
    "hstree_variant": "paper",
    "hstree_max_depth": 15,
    "iforest_estimator": "native",
    "iforest_threshold_window": 500,
    "iforest_welford_window": 2048,
    "iforest_anomaly_k_sigma": 3.0,
    "iforest_anomaly_warmup_window": 100,
    "iforest_initial_window_size": 512,
    "iforest_window_size": 2048,
    "iforest_n_estimators": 100,
    "iforest_max_samples": "256",
    "iforest_contamination": "auto",
    "iforest_refit_every": 128,
    "kitnet_estimator": "welford-windowed",
    "kitnet_threshold_window": 1000,
    "kitnet_welford_window": 1000,
    "kitnet_threshold_percentile": 99.0,
    "kitnet_anomaly_k_sigma": 3.0,
    "kitnet_anomaly_warmup_window": 100,
    "kitnet_max_size_ae": 10,
    "kitnet_grace_feature_mapping": 1000,
    "kitnet_grace_anomaly_detector": 1000,
    "kitnet_learning_rate": 0.1,
    "kitnet_hidden_ratio": 0.75,
    "kitnet_clip": 10.0,
    "deepant_estimator": "welford-windowed",
    "deepant_threshold_window": 500,
    "deepant_welford_window": 2048,
    "deepant_threshold_percentile": 99.0,
    "deepant_anomaly_k_sigma": 3.0,
    "deepant_anomaly_warmup_window": 100,
    "deepant_lookback_size": 100,
    "deepant_initial_window_size": 512,
    "deepant_refit_every": 128,
    "deepant_window_size": 2048,
    "deepant_filters": 32,
    "deepant_kernel_size": 2,
    "deepant_dropout": 0.25,
    "deepant_lr": 0.001,
    "deepant_epochs_initial": 10,
    "deepant_epochs_refit": 5,
    "deepant_batch_size": 32,
    "deepant_max_group_workers": 2,
    "mdragpoint_verdict_mode": "internal",
    "mdragpoint_estimator": "p99",
    "mdragpoint_threshold_window": 500,
    "mdragpoint_welford_window": 900,
    "mdragpoint_threshold_percentile": 99.0,
    "mdragpoint_anomaly_k_sigma": 3.0,
    "mdragpoint_anomaly_warmup_window": 100,
    "mdragpoint_dynamic_threshold": True,
    "mdragpoint_lof_init": True,
    "mdragpoint_n_init": 1000,
    "mdragpoint_max_nb_clusters": 50,
    "mdragpoint_max_cluster_size": 30,
    "mdragpoint_lof_neighbors": 20,
    "hstree_repeatability": False,
    "iforest_repeatability": False,
    "kitnet_repeatability": False,
    "deepant_repeatability": False,
    "mdragpoint_repeatability": False,
    "hstree_reproducibility": False,
    "iforest_reproducibility": False,
    "kitnet_reproducibility": False,
    "deepant_reproducibility": False,
    "mdragpoint_reproducibility": False,
    "reproducibility_source_role": "",
    "reproducibility_campaign_id": "",
    "explainability_nsamples": 100,
    "explainability_v2_nsamples": 100,
    "explainability_model": None,
    "explainability_mode": None,
    "explainability_method": None,
    "explainability_v2_model": None,
    "explainability_v2_mode": None,
    "explainability_v2_method": None,
}

VALID_MODES = {"batch", "realtime", "realtime-x5", "realtime-x20", "realtime-x100"}
VALID_ESTIMATORS = {"p99", "welford", "welford-windowed"}
VALID_IFOREST_ESTIMATORS = VALID_ESTIMATORS | {"native"}
VALID_HSTREE_VARIANTS = {"pysad", "paper"}
VALID_MDRAGPOINT_VERDICT_MODES = {"internal", "external"}
VALID_SHAP_NSAMPLES = {100, 1000, 1500, 2000}
VALID_EXPLAINABILITY_MODELS = {"hstree", "iforest", "kitnet"}
VALID_EXPLAINABILITY_MODES = {"simple", "reference"}
VALID_EXPLAINABILITY_METHODS = {"kernel", "tree"}
KNOWN_MODELS = {
    "hstree",
    "iforest",
    "kitnet",
    "deepant",
    "mdragpoint",
    "explainability",
    "explainability-v2",
}

MODEL_COMPOSE_SERVICES: dict[str, str] = {
    "hstree": "hstree-analysis",
    "iforest": "iforest-analysis",
    "kitnet": "kitnet-analysis",
    "deepant": "deepant-analysis",
    "mdragpoint": "mdragstream-point-analysis",
    "explainability": "explainability",
    "explainability-v2": "explainability-v2",
}

MODEL_COMPOSE_SERVICE_ORDER = (
    "hstree",
    "iforest",
    "kitnet",
    "deepant",
    "mdragpoint",
    "explainability",
    "explainability-v2",
)

# Single source of truth for the repeatability test mapping: for the tested
# model, which compose image to run, which script entrypoint to use and which
# compared fields the diff service must read. The four compared fields are
# "<field_prefix>_score", "_status", "_threshold" and "_anomaly".
REPEATABILITY_MODELS: dict[str, dict[str, str]] = {
    "hstree": {
        "image": "dasma-kafka-anomaly-light:0.1.0",
        "script": "runtime/detectors/hstree/hstree_analysis.py",
        "field_prefix": "hstree",
        "extra_fields": "",
    },
    "iforest": {
        "image": "dasma-kafka-anomaly-sklearn:0.1.0",
        "script": "runtime/detectors/iforest/iforest_analysis.py",
        "field_prefix": "iforest",
        "extra_fields": "",
    },
    "kitnet": {
        "image": "dasma-kafka-anomaly-light:0.1.0",
        "script": "runtime/detectors/kitnet/kitnet_analysis_pysad_cpu.py",
        "field_prefix": "kitnet",
        "extra_fields": "",
    },
    "deepant": {
        "image": "dasma-kafka-deepant:0.1.0",
        "script": "runtime/detectors/deepant/deepant_analysis.py",
        "field_prefix": "deepant",
        "extra_fields": "",
    },
    "mdragpoint": {
        "image": "dasma-kafka-anomaly-sklearn:0.1.0",
        "script": "runtime/detectors/mdragpoint/mdragstream_point_analysis.py",
        "field_prefix": "mdragpoint",
        "extra_fields": "mdragpoint_internal_r",
    },
}

REPEATABILITY_MODEL_KEYS = tuple(REPEATABILITY_MODELS)
REPEATABILITY_COMPOSE_SERVICES = ("repeatability-v1", "repeatability-v2", "repeatability-diff")

# Reproducibility (phase 2) reuses the five canonical detection-model keys:
# explainability services never carry a reproducibility flag.
REPRODUCIBILITY_MODEL_KEYS = REPEATABILITY_MODEL_KEYS
VALID_REPRODUCIBILITY_SOURCE_ROLES = ("source-1", "source-2")
_REPRODUCIBILITY_CAMPAIGN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
REPRODUCIBILITY_COMPOSE_SERVICE = "reproducibility-export"
REPRODUCIBILITY_OUTPUT_ROOT = "/app/data/reproducibility"
SHAP_DIFF_COMPOSE_SERVICE = "explainability-shap-diff"

# Mirror of validation/reproducibility/export_topic_to_csv.py MODEL_VIZ_TOPICS (P2-3).
# The duplication is deliberate and locked by a mirror test.
REPRODUCIBILITY_VIZ_TOPICS = {
    "hstree": "hstree-analysis-viz",
    "iforest": "iforest-analysis-viz",
    "kitnet": "kitnet-analysis-viz",
    "deepant": "deepant-analysis-viz",
    "mdragpoint": "mdragstream-point-analysis-viz",
}

CORE_COMPOSE_SERVICES = ("consensus-analysis", "build-metrics", "telegraf")
PRODUCER_COMPOSE_SERVICE = "producer"
SETUP_COMPOSE_SERVICE = "init-topic"
ALL_RUNTIME_COMPOSE_SERVICES = (
    PRODUCER_COMPOSE_SERVICE,
    *MODEL_COMPOSE_SERVICES.values(),
    *REPEATABILITY_COMPOSE_SERVICES,
    REPRODUCIBILITY_COMPOSE_SERVICE,
    SHAP_DIFF_COMPOSE_SERVICE,
    *CORE_COMPOSE_SERVICES,
    SETUP_COMPOSE_SERVICE,
)

RESET_TOPICS = (
    "source-data",
    "source-data-viz",
    "hstree-analysis",
    "hstree-analysis-viz",
    "iforest-analysis",
    "iforest-analysis-viz",
    "kitnet-analysis",
    "kitnet-analysis-viz",
    "deepant-analysis",
    "deepant-analysis-viz",
    "mdragstream-point-analysis",
    "mdragstream-point-analysis-viz",
    "explainability",
    "explainability-viz",
    "explainability-v2",
    "explainability-v2-viz",
    "consensus-analysis",
    "topic-metrics",
    "repeatability-v1",
    "repeatability-v1-viz",
    "repeatability-v2",
    "repeatability-v2-viz",
    "repeatability-diff",
)

RESET_SCHEMA_SUBJECTS = (
    "source-data-value",
    "hstree-analysis-value",
    "iforest-analysis-value",
    "kitnet-analysis-value",
    "deepant-analysis-value",
    "mdragstream-point-analysis-value",
    "explainability-value",
    "explainability-v2-value",
    "repeatability-v1-value",
    "repeatability-v2-value",
)


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    return [item.strip() for item in str(value).split(",") if item.strip()]


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _as_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _normalize_shap_nsamples(value: Any) -> int:
    parsed = _as_int(value, 100)
    return parsed if parsed in VALID_SHAP_NSAMPLES else 100


def _validate_explainability(config: dict[str, Any]) -> None:
    for service, prefix in (("explainability", "explainability"), ("explainability-v2", "explainability_v2")):
        if service not in config["selected_models"]:
            continue
        model_key, mode_key, method_key = (f"{prefix}_{suffix}" for suffix in ("model", "mode", "method"))
        model, mode, method = config[model_key], config[mode_key], config[method_key]
        label = service
        if model not in VALID_EXPLAINABILITY_MODELS:
            raise ValueError(f"{label}: le champ '{model_key}' est obligatoire lorsque le service est sélectionné (valeurs : hstree, iforest, kitnet)")
        if mode not in VALID_EXPLAINABILITY_MODES:
            raise ValueError(f"{label}: le champ '{mode_key}' est obligatoire lorsque le service est sélectionné (valeurs : simple, reference)")
        if method not in VALID_EXPLAINABILITY_METHODS:
            raise ValueError(f"{label}: le champ '{method_key}' est obligatoire lorsque le service est sélectionné (valeurs : kernel, tree)")
        if mode == "reference" and model not in config["selected_models"]:
            raise ValueError(f"{label}: le mode 'reference' avec le modèle '{model}' exige que le détecteur '{model}' soit sélectionné")
        if method == "tree" and model != "iforest":
            raise ValueError(f"{label}: la méthode 'tree' n'est disponible que pour le modèle 'iforest'")
        if model == "hstree" and method == "kernel":
            continue
        if model == "iforest" and method in {"kernel", "tree"}:
            continue
        if model == "kitnet" and method == "kernel":
            continue
        raise ValueError(f"{label}: la combinaison modèle='{model}', méthode='{method}' sera disponible en phase B")


def _explainability_source_topic(model: Any, mode: Any) -> str:
    if not model or not mode:
        return ""
    return "source-data" if mode == "simple" else f"{model}-analysis"


def load_campaign_dict(raw: Any) -> dict[str, Any]:
    """Normalize and validate a raw campaign mapping.

    This is the single validation authority for campaign JSON: the CLI
    (`load_campaign`) and the AskMyDasma service (before persisting) both go
    through it, so a request bypassing the interface is rejected before launch.
    """
    if not isinstance(raw, dict):
        raise ValueError("campaign config must be a JSON object")

    raw = flatten_structured_campaign(raw)
    unknown = sorted(set(raw) - set(DEFAULTS))
    if unknown:
        raise ValueError(f"unknown campaign config field(s): {', '.join(unknown)}")

    config = dict(DEFAULTS)
    config.update(raw)

    if config["hstree_variant"] not in VALID_HSTREE_VARIANTS:
        raise ValueError(
            f"hstree: variante inconnue '{config['hstree_variant']}' (valeurs : pysad, paper)"
        )

    config["selected_groups"] = _as_list(config["selected_groups"])
    config["excluded_fields"] = _as_list(config["excluded_fields"])
    selected_models = _as_list(config["selected_models"])
    unknown_models = sorted(set(selected_models) - KNOWN_MODELS)
    if unknown_models:
        raise ValueError(f"unknown selected model(s): {', '.join(unknown_models)}")
    config["selected_models"] = selected_models

    if config["mode"] not in VALID_MODES:
        raise ValueError("mode must be batch, realtime, realtime-x5, realtime-x20 or realtime-x100")

    for key, default in (
        ("hstree_estimator", "p99"),
        ("iforest_estimator", "native"),
        ("kitnet_estimator", "welford-windowed"),
        ("deepant_estimator", "welford-windowed"),
        ("mdragpoint_estimator", "p99"),
    ):
        allowed = VALID_IFOREST_ESTIMATORS if key == "iforest_estimator" else VALID_ESTIMATORS
        if config[key] not in allowed:
            config[key] = default

    if config["mdragpoint_verdict_mode"] in VALID_ESTIMATORS:
        config["mdragpoint_estimator"] = config["mdragpoint_verdict_mode"]
        config["mdragpoint_verdict_mode"] = "internal"
    elif config["mdragpoint_verdict_mode"] not in VALID_MDRAGPOINT_VERDICT_MODES:
        config["mdragpoint_verdict_mode"] = "internal"

    int_defaults = {key: value for key, value in DEFAULTS.items() if isinstance(value, int) and not isinstance(value, bool)}
    float_defaults = {key: value for key, value in DEFAULTS.items() if isinstance(value, float)}
    bool_defaults = {key: value for key, value in DEFAULTS.items() if isinstance(value, bool)}

    for key, default in int_defaults.items():
        config[key] = _as_int(config[key], default)
    for key, default in float_defaults.items():
        config[key] = _as_float(config[key], default)
    for key in bool_defaults:
        config[key] = _as_bool(config[key])

    explains_hstree = any(
        service in config["selected_models"] and config[model_key] == "hstree"
        for service, model_key in (
            ("explainability", "explainability_model"),
            ("explainability-v2", "explainability_v2_model"),
        )
    )
    if "hstree" in config["selected_models"] or explains_hstree:
        initial_window = config["hstree_initial_window_size"]
        model_window = config["hstree_window_size"]
        if initial_window != model_window:
            raise ValueError(
                f"hstree: la fenêtre d'amorçage ({initial_window}) doit être égale à la fenêtre du modèle "
                f"({model_window}) : fenêtre de référence du papier"
            )

    explains_iforest = any(
        service in config["selected_models"] and config[model_key] == "iforest"
        for service, model_key in (
            ("explainability", "explainability_model"),
            ("explainability-v2", "explainability_v2_model"),
        )
    )
    if "iforest" in config["selected_models"] or explains_iforest:
        raw_max_samples = raw.get("iforest_max_samples", DEFAULTS["iforest_max_samples"])
        config["iforest_max_samples"] = parse_iforest_max_samples(str(raw_max_samples))
        contamination = parse_iforest_contamination(str(config["iforest_contamination"]))
        config["iforest_contamination"] = "auto" if contamination == "auto" else f"{contamination:.2f}"
        validate_iforest_params(
            config["iforest_initial_window_size"],
            config["iforest_window_size"],
            config["iforest_max_samples"],
            config["iforest_n_estimators"],
            config["iforest_refit_every"],
        )

    active_repeatability = [
        model for model in REPEATABILITY_MODEL_KEYS if config[f"{model}_repeatability"]
    ]
    if len(active_repeatability) > 1:
        raise ValueError(
            "repeatability test is exclusive: at most one model can have "
            f"repeatability enabled, got {', '.join(active_repeatability)}"
        )
    if active_repeatability:
        model = active_repeatability[0]
        if model not in config["selected_models"]:
            raise ValueError(
                f"repeatability is enabled for '{model}' but the model is not "
                "in selected_models"
            )
        # Exclusive dedicated test: selected_models must be literally [M] — no
        # other model and no duplicated entry. Any other model, including
        # explainability services that wait for a topic deliberately replaced
        # by the pair, is a conflicting selection.
        if config["selected_models"] != [model]:
            unexpected = {entry for entry in config["selected_models"] if entry != model}
            detail = (
                f"conflicting selected model(s): {', '.join(sorted(unexpected))}"
                if unexpected
                else f"duplicated selected_models entries: {config['selected_models']}"
            )
            raise ValueError(
                "repeatability test is exclusive: the campaign must select "
                f"exactly ['{model}']; {detail}"
            )

    # Reproducibility (phase 2): configuration-only contract. The normal model
    # service of the flagged model stays selected and is never replaced here.
    active_reproducibility = [
        model for model in REPRODUCIBILITY_MODEL_KEYS if config[f"{model}_reproducibility"]
    ]
    if active_reproducibility:
        if len(active_reproducibility) > 1:
            raise ValueError(
                "reproducibility test is exclusive: at most one model can have "
                f"reproducibility enabled, got {', '.join(active_reproducibility)}"
            )
        model = active_reproducibility[0]
        if config["selected_models"] != [model]:
            unexpected = {entry for entry in config["selected_models"] if entry != model}
            detail = (
                f"conflicting selected model(s): {', '.join(sorted(unexpected))}"
                if unexpected
                else f"duplicated selected_models entries: {config['selected_models']}"
            )
            raise ValueError(
                "reproducibility test is exclusive: the campaign must select "
                f"exactly ['{model}']; {detail}"
            )
        if active_repeatability:
            raise ValueError(
                "reproducibility and repeatability are mutually exclusive: "
                f"repeatability is enabled for '{active_repeatability[0]}' while "
                f"reproducibility is enabled for '{model}'"
            )
        role = str(config.get("reproducibility_source_role") or "")
        if role not in VALID_REPRODUCIBILITY_SOURCE_ROLES:
            raise ValueError(
                "reproducibility source role must be exactly 'source-1' or "
                f"'source-2', got {role!r}"
            )
        campaign_id = str(config.get("reproducibility_campaign_id") or "").strip()
        if not campaign_id:
            raise ValueError(
                "reproducibility campaign-id is required and cannot be empty"
            )
        if not _REPRODUCIBILITY_CAMPAIGN_ID_PATTERN.match(campaign_id):
            raise ValueError(
                "reproducibility campaign-id must be a portable directory name "
                "(alphanumeric, dot, underscore or hyphen; starting with an "
                "alphanumeric character; at most 128 characters); got "
                f"{campaign_id!r}"
            )
        config["reproducibility_campaign_id"] = campaign_id

    config["explainability_nsamples"] = _normalize_shap_nsamples(config["explainability_nsamples"])
    config["explainability_v2_nsamples"] = _normalize_shap_nsamples(config["explainability_v2_nsamples"])
    _validate_explainability(config)
    return config


def load_campaign(path: str | Path) -> dict[str, Any]:
    return load_campaign_dict(json.loads(Path(path).read_text(encoding="utf-8")))


def mode_to_transformation(mode: str) -> str:
    return mode if mode in {"realtime", "realtime-x5", "realtime-x20", "realtime-x100"} else ""


def model_enabled(config: dict[str, Any], model: str) -> bool:
    selected = set(config["selected_models"])
    if model in {"explainability", "explainability-v2"}:
        return model in selected
    return model in selected


def repeatability_model(config: dict[str, Any]) -> str | None:
    """Return the single model with an active repeatability test, or ``None``.

    ``load_campaign`` already validated exclusivity, selection and the
    explainability conflict; this helper only resolves the active model.
    """
    active = [model for model in REPEATABILITY_MODEL_KEYS if config.get(f"{model}_repeatability")]
    return active[0] if len(active) == 1 else None


def reproducibility_model(config: dict[str, Any]) -> str | None:
    """Return the single model with an active reproducibility test, or ``None``.

    ``load_campaign`` already validated exclusivity and selection; this helper
    only resolves the active model. The normal service of that model stays
    selected and is never replaced.
    """
    active = [
        model for model in REPRODUCIBILITY_MODEL_KEYS if config.get(f"{model}_reproducibility")
    ]
    return active[0] if len(active) == 1 else None


def compose_services(config: dict[str, Any]) -> list[str]:
    repeat_model = repeatability_model(config)
    repro_model = reproducibility_model(config)
    services: list[str] = []
    for model in MODEL_COMPOSE_SERVICE_ORDER:
        if not model_enabled(config, model):
            continue
        if model == repeat_model:
            # Exclusive repeatability test: the normal service of the tested
            # model is replaced by the generic pair and the diff service.
            services.extend(REPEATABILITY_COMPOSE_SERVICES)
        else:
            services.append(MODEL_COMPOSE_SERVICES[model])
            if model == repro_model:
                # Reproducibility keeps the normal model running and adds the
                # CSV exporter next to it.
                services.append(REPRODUCIBILITY_COMPOSE_SERVICE)
    services.extend(CORE_COMPOSE_SERVICES)
    # The precomputed comparison is meaningful only when both explainability
    # streams are active. It remains absent for V1-only or V2-only campaigns.
    if model_enabled(config, "explainability") and model_enabled(config, "explainability-v2"):
        services.append(SHAP_DIFF_COMPOSE_SERVICE)
    return services


def build_env(config: dict[str, Any], *, wait_for_start: bool = False) -> dict[str, str]:
    repeat_model = repeatability_model(config)
    repeat_row = REPEATABILITY_MODELS[repeat_model or "hstree"]
    repeat_prefix = repeat_row["field_prefix"]
    # Reproducibility (phase 2): configuration-only contract. Without an active
    # flag the four runtime variables stay neutral even if stray role/id values
    # are present; the normal model service of M is never replaced here.
    repro_model = next(
        (model for model in REPRODUCIBILITY_MODEL_KEYS if config.get(f"{model}_reproducibility")),
        "",
    )
    repro_role = str(config.get("reproducibility_source_role") or "")
    repro_campaign_id = str(config.get("reproducibility_campaign_id") or "")
    if not repro_model:
        repro_role = ""
        repro_campaign_id = ""
        repro_source_topic = ""
        repro_group_id = ""
        repro_image = ""
        repro_campaign_parameters_json = "{}"
    else:
        # Normal reproducibility campaign: the exporter consumes the viz topic
        # of M while the normal model keeps running.
        repro_source_topic = REPRODUCIBILITY_VIZ_TOPICS[repro_model]
        repro_group_id = f"reproducibility-export-{repro_campaign_id}-{repro_role}"
        repro_image = REPEATABILITY_MODELS[repro_model]["image"]
        model_parameters = {
            key: config[key]
            for key in sorted(config)
            if key.startswith(f"{repro_model}_")
            and key not in (f"{repro_model}_repeatability", f"{repro_model}_reproducibility")
        }
        campaign_parameters = {
            "dataset_path": config["dataset_path"],
            "mode": config["mode"],
            "selected_groups": config["selected_groups"],
            "excluded_fields": config["excluded_fields"],
            "model_parameters": model_parameters,
        }
        repro_campaign_parameters_json = json.dumps(
            campaign_parameters, ensure_ascii=False, sort_keys=True, allow_nan=False
        )
    env = {
        "DATA_SOURCE_FILE": str(config["dataset_path"]),
        "DASMA_PRODUCER_TRANSFORMATION": mode_to_transformation(str(config["mode"])),
        "DASMA_SELECTED_GROUPS": ",".join(config["selected_groups"]),
        "DASMA_EXCLUDED_FIELDS": ",".join(config["excluded_fields"]),
        "DASMA_SELECTED_MODELS": ",".join(config["selected_models"]),
        "DASMA_COMPOSE_SERVICES": " ".join(compose_services(config)),
        "DASMA_HSTREE_ENABLED": "1" if model_enabled(config, "hstree") else "0",
        "DASMA_IFOREST_ENABLED": "1" if model_enabled(config, "iforest") else "0",
        "DASMA_KITNET_ENABLED": "1" if model_enabled(config, "kitnet") else "0",
        "DASMA_DEEPANT_ENABLED": "1" if model_enabled(config, "deepant") else "0",
        "DASMA_MDRAGPOINT_ENABLED": "1" if model_enabled(config, "mdragpoint") else "0",
        "DASMA_EXPLAINABILITY_ENABLED": "1" if model_enabled(config, "explainability") else "0",
        "DASMA_EXPLAINABILITY_V2_ENABLED": "1" if model_enabled(config, "explainability-v2") else "0",
        "DASMA_SHAP_DIFF_ENABLED": (
            "1"
            if model_enabled(config, "explainability") and model_enabled(config, "explainability-v2")
            else "0"
        ),
        "DASMA_CONSENSUS_ENABLED": "1",
        "HSTREE_THRESHOLD_MODE": str(config["hstree_estimator"]),
        "HSTREE_THRESHOLD_WINDOW": str(config["hstree_threshold_window"]),
        "HSTREE_WELFORD_WINDOW": str(config["hstree_welford_window"]),
        "HSTREE_ANOMALY_K_SIGMA": str(config["hstree_anomaly_k_sigma"]),
        "HSTREE_ANOMALY_MIN_SAMPLES": str(config["hstree_anomaly_warmup_window"]),
        "HSTREE_INITIAL_WINDOW_SIZE": str(config["hstree_initial_window_size"]),
        "HSTREE_WINDOW_SIZE": str(config["hstree_window_size"]),
        "HSTREE_NUM_TREES": str(config["hstree_num_trees"]),
        "HSTREE_VARIANT": str(config["hstree_variant"]),
        "HSTREE_MAX_DEPTH": str(config["hstree_max_depth"]),
        "IFOREST_THRESHOLD_MODE": str(config["iforest_estimator"]),
        "IFOREST_THRESHOLD_WINDOW": str(config["iforest_threshold_window"]),
        "IFOREST_WELFORD_WINDOW": str(config["iforest_welford_window"]),
        "IFOREST_ANOMALY_K_SIGMA": str(config["iforest_anomaly_k_sigma"]),
        "IFOREST_ANOMALY_MIN_SAMPLES": str(config["iforest_anomaly_warmup_window"]),
        "IFOREST_INITIAL_WINDOW_SIZE": str(config["iforest_initial_window_size"]),
        "IFOREST_WINDOW_SIZE": str(config["iforest_window_size"]),
        "IFOREST_N_ESTIMATORS": str(config["iforest_n_estimators"]),
        "IFOREST_MAX_SAMPLES": str(config["iforest_max_samples"]),
        "IFOREST_CONTAMINATION": str(config["iforest_contamination"]),
        "IFOREST_REFIT_EVERY": str(config["iforest_refit_every"]),
        "KITNET_THRESHOLD_MODE": str(config["kitnet_estimator"]),
        "KITNET_THRESHOLD_WINDOW": str(config["kitnet_threshold_window"]),
        "KITNET_WELFORD_WINDOW": str(config["kitnet_welford_window"]),
        "KITNET_THRESHOLD_PERCENTILE": str(config["kitnet_threshold_percentile"]),
        "KITNET_ANOMALY_K_SIGMA": str(config["kitnet_anomaly_k_sigma"]),
        "KITNET_ANOMALY_WARMUP_WINDOW": str(config["kitnet_anomaly_warmup_window"]),
        "KITNET_ANOMALY_MIN_SAMPLES": str(config["kitnet_anomaly_warmup_window"]),
        "KITNET_MAX_SIZE_AE": str(config["kitnet_max_size_ae"]),
        "KITNET_GRACE_FEATURE_MAPPING": str(config["kitnet_grace_feature_mapping"]),
        "KITNET_GRACE_ANOMALY_DETECTOR": str(config["kitnet_grace_anomaly_detector"]),
        "KITNET_LEARNING_RATE": str(config["kitnet_learning_rate"]),
        "KITNET_HIDDEN_RATIO": str(config["kitnet_hidden_ratio"]),
        "KITNET_CLIP": str(config["kitnet_clip"]),
        "DEEPANT_THRESHOLD_MODE": str(config["deepant_estimator"]),
        "DEEPANT_THRESHOLD_WINDOW": str(config["deepant_threshold_window"]),
        "DEEPANT_WELFORD_WINDOW": str(config["deepant_welford_window"]),
        "DEEPANT_THRESHOLD_PERCENTILE": str(config["deepant_threshold_percentile"]),
        "DEEPANT_ANOMALY_K_SIGMA": str(config["deepant_anomaly_k_sigma"]),
        "DEEPANT_ANOMALY_MIN_SAMPLES": str(config["deepant_anomaly_warmup_window"]),
        "DEEPANT_LOOKBACK_SIZE": str(config["deepant_lookback_size"]),
        "DEEPANT_INITIAL_WINDOW_SIZE": str(config["deepant_initial_window_size"]),
        "DEEPANT_REFIT_EVERY": str(config["deepant_refit_every"]),
        "DEEPANT_WINDOW_SIZE": str(config["deepant_window_size"]),
        "DEEPANT_FILTERS": str(config["deepant_filters"]),
        "DEEPANT_KERNEL_SIZE": str(config["deepant_kernel_size"]),
        "DEEPANT_DROPOUT": str(config["deepant_dropout"]),
        "DEEPANT_LR": str(config["deepant_lr"]),
        "DEEPANT_EPOCHS_INITIAL": str(config["deepant_epochs_initial"]),
        "DEEPANT_EPOCHS_REFIT": str(config["deepant_epochs_refit"]),
        "DEEPANT_BATCH_SIZE": str(config["deepant_batch_size"]),
        "DEEPANT_MAX_GROUP_WORKERS": str(config["deepant_max_group_workers"]),
        "MDRAGPOINT_THRESHOLD_MODE": str(config["mdragpoint_estimator"]),
        "MDRAGPOINT_VERDICT_MODE": str(config["mdragpoint_verdict_mode"]),
        "MDRAGPOINT_THRESHOLD_WINDOW": str(config["mdragpoint_threshold_window"]),
        "MDRAGPOINT_WELFORD_WINDOW": str(config["mdragpoint_welford_window"]),
        "MDRAGPOINT_THRESHOLD_PERCENTILE": str(config["mdragpoint_threshold_percentile"]),
        "MDRAGPOINT_ANOMALY_K_SIGMA": str(config["mdragpoint_anomaly_k_sigma"]),
        "MDRAGPOINT_ANOMALY_MIN_SAMPLES": str(config["mdragpoint_anomaly_warmup_window"]),
        "MDRAGPOINT_DYNAMIC_THRESHOLD": "True" if config["mdragpoint_dynamic_threshold"] else "False",
        "MDRAGPOINT_LOF_INIT": "True" if config["mdragpoint_lof_init"] else "False",
        "MDRAGPOINT_N_INIT": str(config["mdragpoint_n_init"]),
        "MDRAGPOINT_MAX_NB_CLUSTERS": str(config["mdragpoint_max_nb_clusters"]),
        "MDRAGPOINT_MAX_CLUSTER_SIZE": str(config["mdragpoint_max_cluster_size"]),
        "MDRAGPOINT_LOF_NEIGHBORS": str(config["mdragpoint_lof_neighbors"]),
        "EXPLAINABILITY_SHAP_NSAMPLES": str(config["explainability_nsamples"]),
        "EXPLAINABILITY_V2_SHAP_NSAMPLES": str(config["explainability_v2_nsamples"]),
        "EXPLAINABILITY_MODEL": str(config["explainability_model"] or ""),
        "EXPLAINABILITY_MODE": str(config["explainability_mode"] or ""),
        "EXPLAINABILITY_METHOD": str(config["explainability_method"] or ""),
        "EXPLAINABILITY_SOURCE_TOPIC": _explainability_source_topic(config["explainability_model"], config["explainability_mode"]),
        "EXPLAINABILITY_V2_MODEL": str(config["explainability_v2_model"] or ""),
        "EXPLAINABILITY_V2_MODE": str(config["explainability_v2_mode"] or ""),
        "EXPLAINABILITY_V2_METHOD": str(config["explainability_v2_method"] or ""),
        "EXPLAINABILITY_V2_SOURCE_TOPIC": _explainability_source_topic(config["explainability_v2_model"], config["explainability_v2_mode"]),
        # Repeatability values, derived from the single REPEATABILITY_MODELS
        # mapping. Without an active repeatability flag they resolve to the
        # hstree row with the kill switch off, which is exactly the neutral
        # configuration the compose interpolations expect.
        "DASMA_REPEATABILITY_ENABLED": "1" if repeat_model else "0",
        "REPEATABILITY_IMAGE": repeat_row["image"],
        "REPEATABILITY_SCRIPT": repeat_row["script"],
        "REPEATABILITY_MODEL_ID": repeat_prefix,
        "REPEATABILITY_SCORE_FIELD": f"{repeat_prefix}_score",
        "REPEATABILITY_STATUS_FIELD": f"{repeat_prefix}_status",
        "REPEATABILITY_THRESHOLD_FIELD": f"{repeat_prefix}_threshold",
        "REPEATABILITY_ANOMALY_FIELD": f"{repeat_prefix}_anomaly",
        "REPEATABILITY_EXTRA_FIELDS": repeat_row["extra_fields"],
        "REPEATABILITY_TIMEOUT_SEC": "30",
        "REPEATABILITY_RETENTION_SEC": "300",
        "ANALYSE_SEED": "42",
        "REPEATABILITY_VIZ_TOPIC_A": "repeatability-v1-viz",
        "REPEATABILITY_VIZ_TOPIC_B": "repeatability-v2-viz",
        "REPEATABILITY_DEST_TOPIC": "repeatability-diff",
        "REPEATABILITY_GROUP_ID": "repeatability-diff-consumer",
        # Reproducibility (phase 2) runtime variables: emitted for every
        # campaign. P2-4 wires them to the Compose/exporter bilateral contract.
        "DASMA_REPRODUCIBILITY_ENABLED": "1" if repro_model else "0",
        "REPRODUCIBILITY_MODEL_ID": repro_model,
        "REPRODUCIBILITY_SOURCE_ROLE": repro_role,
        "REPRODUCIBILITY_CAMPAIGN_ID": repro_campaign_id,
        "REPRODUCIBILITY_SOURCE_TOPIC": repro_source_topic,
        "REPRODUCIBILITY_GROUP_ID": repro_group_id,
        "REPRODUCIBILITY_OUTPUT_ROOT": REPRODUCIBILITY_OUTPUT_ROOT,
        "REPRODUCIBILITY_IDLE_EXIT_SEC": "15",
        "REPRODUCIBILITY_STARTUP_TIMEOUT_SEC": "300",
        "REPRODUCIBILITY_IMAGE": repro_image,
        "REPRODUCIBILITY_CAMPAIGN_PARAMETERS_JSON": repro_campaign_parameters_json,
    }
    if repeat_model is not None:
        # Exclusive test: the tested model's normal service and the consensus
        # stay idle so nothing perturbs the repeatability measurement.
        env[f"DASMA_{repeat_model.upper()}_ENABLED"] = "0"
        env["DASMA_CONSENSUS_ENABLED"] = "0"
    if wait_for_start:
        env["DASMA_PRODUCER_WAIT_FOR_START"] = "1"
        env["DASMA_PRODUCER_START_SIGNAL_FILE"] = "/tmp/dasma-producer-start.signal"
    return env


def build_action_payload(config: dict[str, Any], action: str) -> dict[str, Any]:
    return {
        "status": "ok",
        "action": action,
        **config,
    }


def print_exports(config: dict[str, Any], *, wait_for_start: bool) -> None:
    for key, value in build_env(config, wait_for_start=wait_for_start).items():
        print(f"export {key}={shlex.quote(value)}")


def print_words(values: tuple[str, ...] | list[str]) -> None:
    for value in values:
        print(value)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    export_parser = subparsers.add_parser("export-env")
    export_parser.add_argument("config_path")
    export_parser.add_argument("--wait-for-start", action="store_true")

    payload_parser = subparsers.add_parser("payload")
    payload_parser.add_argument("config_path")
    payload_parser.add_argument("--action", required=True)

    subparsers.add_parser("all-runtime-services")
    subparsers.add_parser("reset-topics")
    subparsers.add_parser("reset-schema-subjects")

    args = parser.parse_args(argv)

    if args.command == "all-runtime-services":
        print_words(ALL_RUNTIME_COMPOSE_SERVICES)
        return 0

    if args.command == "reset-topics":
        print_words(RESET_TOPICS)
        return 0

    if args.command == "reset-schema-subjects":
        print_words(RESET_SCHEMA_SUBJECTS)
        return 0

    try:
        config = load_campaign(args.config_path)
    except Exception as exc:
        print(json.dumps({"status": "error", "error": "invalid_campaign_config", "message": str(exc)}), file=sys.stderr)
        return 1

    if args.command == "export-env":
        print_exports(config, wait_for_start=args.wait_for_start)
        return 0

    if args.command == "payload":
        print(json.dumps(build_action_payload(config, args.action), ensure_ascii=False))
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
