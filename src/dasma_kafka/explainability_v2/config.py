"""Configuration and constants for the explainability-v2 service.

This is a fine, isolated copy of the regular explainability configuration. The
runtime logic is identical to the baseline; only the v2 identity differs:
destination topics, consumer group, and the Avro record name. Regular keeps its
own `src/dasma_kafka/explainability/config.py` untouched.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import Optional, Set

from dasma_kafka.kitnet_analysis.helpers import (
    parse_grace_anomaly_detector,
    parse_grace_feature_mapping,
)
from dasma_kafka.iforest_analysis.helpers import parse_iforest_contamination, parse_iforest_max_samples
from dasma_kafka.iforest_analysis.model import validate_iforest_params


@dataclass
class ExplainabilityV2Config:
    """Centralized configuration for the explainability-v2 service."""

    bootstrap_servers: str = ""
    source_topic: str = ""
    explained_model: str = ""
    explainability_mode: str = ""
    explanation_method: str = ""
    dest_topic: str = "explainability-v2"
    dest_topic_json: str = ""
    schema_registry_url: str = ""
    group_id: str = "explainability-v2-consumer"
    log_level: str = "INFO"

    hstree_window_size: int = 256
    hstree_num_trees: int = 25
    hstree_max_depth: int = 15
    hstree_initial_window_size: int = 256
    hstree_seed: int = 42
    hstree_variant: str = "paper"

    shap_background_size: int = 100
    shap_nsamples: int = 100
    shap_enabled: bool = True
    explainability_seed: int = 42

    iforest_initial_window_size: int = 512
    iforest_window_size: int = 2048
    iforest_n_estimators: int = 100
    iforest_max_samples: str | int = 256
    iforest_refit_every: int = 128
    iforest_contamination: str = "auto"

    kitnet_max_size_ae: int = 10
    kitnet_grace_feature_mapping: int | None = None
    kitnet_grace_anomaly_detector: int = 50000
    kitnet_learning_rate: float = 0.1
    kitnet_hidden_ratio: float = 0.75

    @classmethod
    def from_environment(cls) -> "ExplainabilityV2Config":
        """Load configuration from environment variables."""

        def get_env(
            name: str,
            default: Optional[str] = None,
            required: bool = False,
        ) -> str:
            value = os.getenv(name, default)
            if required and value is None:
                print(
                    f"[explainability-v2] ERROR: missing environment variable: {name}",
                    file=sys.stderr,
                )
                sys.exit(1)
            return value

        def kitnet_env(primary: str, alias: str) -> Optional[str]:
            value = os.getenv(primary)
            return os.getenv(alias) if value in (None, "") else value

        return cls(
            bootstrap_servers=get_env("BOOTSTRAP_SERVERS", required=True),
            source_topic=get_env("KAFKA_SOURCE_TOPIC", ""),
            explained_model=get_env("EXPLAINABILITY_MODEL", ""),
            explainability_mode=get_env("EXPLAINABILITY_MODE", ""),
            explanation_method=get_env("EXPLAINABILITY_METHOD", ""),
            dest_topic=get_env("KAFKA_DEST_TOPIC", "explainability-v2"),
            dest_topic_json=get_env(
                "KAFKA_DEST_TOPIC_JSON",
                os.getenv("KAFKA_DEST_TOPIC_JSNON", ""),
            ),
            schema_registry_url=get_env("SCHEMA_REGISTRY_URL", required=True),
            group_id=get_env("EXPLAINABILITY_GROUP_ID", "explainability-v2-consumer"),
            log_level=get_env("LOG_LEVEL", "INFO"),
            hstree_window_size=int(get_env("HSTREE_WINDOW_SIZE", "256")),
            hstree_num_trees=int(get_env("HSTREE_NUM_TREES", "25")),
            hstree_max_depth=int(get_env("HSTREE_MAX_DEPTH", "15")),
            hstree_initial_window_size=int(get_env("HSTREE_INITIAL_WINDOW_SIZE", "256")),
            hstree_seed=int(get_env("ANALYSE_SEED", "42")),
            hstree_variant=get_env("HSTREE_VARIANT", "paper"),
            shap_background_size=int(get_env("SHAP_BACKGROUND_SIZE", "100")),
            shap_nsamples=int(get_env("SHAP_NSAMPLES", "100")),
            shap_enabled=get_env("SHAP_ENABLED", "true").lower() == "true",
            explainability_seed=int(get_env("EXPLAINABILITY_SEED", "42")),
            iforest_initial_window_size=int(get_env("IFOREST_INITIAL_WINDOW_SIZE", "512")),
            iforest_window_size=int(get_env("IFOREST_WINDOW_SIZE", "2048")),
            iforest_n_estimators=int(get_env("IFOREST_N_ESTIMATORS", "100")),
            iforest_max_samples=get_env("IFOREST_MAX_SAMPLES", "256"),
            iforest_refit_every=int(get_env("IFOREST_REFIT_EVERY", "128")),
            iforest_contamination=get_env("IFOREST_CONTAMINATION", "auto"),
            kitnet_max_size_ae=int(get_env("KITNET_MAX_SIZE_AE", "10")),
            kitnet_grace_feature_mapping=parse_grace_feature_mapping(
                kitnet_env("KITNET_GRACE_FEATURE_MAPPING", "KITNET_FM_GRACE_PERIOD")
            ),
            kitnet_grace_anomaly_detector=parse_grace_anomaly_detector(
                kitnet_env("KITNET_GRACE_ANOMALY_DETECTOR", "KITNET_AD_GRACE_PERIOD"),
                50000,
            ),
            kitnet_learning_rate=float(get_env("KITNET_LEARNING_RATE", "0.1")),
            kitnet_hidden_ratio=float(get_env("KITNET_HIDDEN_RATIO", "0.75")),
        )


_default_hstree_exclude = "timestamp"
_hstree_exclude = os.getenv("HSTREE_EXCLUDE_FIELDS", _default_hstree_exclude)
_hstree_exclude_set = {col.strip() for col in _hstree_exclude.split(",") if col.strip()}

EXCLUDED_COLUMNS: Set[str] = _hstree_exclude_set | {
    "hstree_score",
    "hstree_status",
    "hstree_anomaly",
    "hstree_threshold",
    "_topic",
    "_partition",
    "_offset",
    "_key",
}

_iforest_exclude = os.getenv(
    "IFOREST_EXCLUDE_FIELDS",
    "source_event_id,timestamp,Group,group,Cycle,LabviewTime,time",
)
IFOREST_EXCLUDE_FIELDS: Set[str] = {
    col.strip() for col in _iforest_exclude.split(",") if col.strip()
} | {
    "iforest_score",
    "iforest_status",
    "iforest_anomaly",
    "iforest_threshold",
    "_topic",
    "_partition",
    "_offset",
    "_key",
}

_kitnet_exclude = os.getenv(
    "KITNET_EXCLUDE_FIELDS",
    "source_event_id,timestamp,Group,group,name,time",
)
KITNET_EXCLUDE_FIELDS: Set[str] = {
    col.strip() for col in _kitnet_exclude.split(",") if col.strip()
} | {
    "kitnet_score",
    "kitnet_status",
    "kitnet_anomaly",
    "kitnet_threshold",
    "_topic",
    "_partition",
    "_offset",
    "_key",
}


EXPLAINABILITY_V2_VALUE_SCHEMA_STR = """
{
  "type": "record",
  "name": "ExplainabilityV2Event",
  "namespace": "dasma.kafka",
  "fields": [
    {"name": "timestamp", "type": ["null", "string"], "default": null},
    {"name": "Group", "type": ["null", "string"], "default": null},
    {"name": "hstree_score", "type": ["null", "double"], "default": null},

    {"name": "features", "type": ["null", {"type": "map", "values": "double"}], "default": null},

    {"name": "shap_values", "type": ["null", {"type": "map", "values": "double"}], "default": null},
    {"name": "shap_base_value", "type": ["null", "double"], "default": null},
    {"name": "shap_computation_time_ms", "type": ["null", "double"], "default": null},

    {"name": "local_hstree_score", "type": ["null", "double"], "default": null},

    {"name": "source_event_id", "type": ["null", "string"], "default": null},
    {"name": "hstree_status", "type": ["null", "string"], "default": null},
    {"name": "hstree_anomaly", "type": ["null", "boolean"], "default": null},
    {"name": "hstree_threshold", "type": ["null", "double"], "default": null},
    {"name": "explanation_status", "type": ["null", "string"], "default": null},
    {"name": "explanation_error_code", "type": ["null", "string"], "default": null},

    {"name": "explanation_profile", "type": ["null", "string"], "default": null},
    {"name": "shap_nsamples", "type": ["null", "int"], "default": null},
    {"name": "shap_background_size", "type": ["null", "int"], "default": null},
    {"name": "explainability_seed", "type": ["null", "long"], "default": null},
    {"name": "hstree_model_version", "type": ["null", "string"], "default": null},
    {"name": "hstree_model_fingerprint", "type": ["null", "string"], "default": null},
    {"name": "feature_fingerprint", "type": ["null", "string"], "default": null}
    ,{"name": "explained_model", "type": ["null", "string"], "default": null}
    ,{"name": "explainability_mode", "type": ["null", "string"], "default": null}
    ,{"name": "explanation_method", "type": ["null", "string"], "default": null}
    ,{"name": "local_score", "type": ["null", "double"], "default": null}
    ,{"name": "reference_model", "type": ["null", "string"], "default": null}
    ,{"name": "reference_score", "type": ["null", "double"], "default": null}
    ,{"name": "reference_status", "type": ["null", "string"], "default": null}
    ,{"name": "reference_anomaly", "type": ["null", "boolean"], "default": null}
    ,{"name": "reference_threshold", "type": ["null", "double"], "default": null}
    ,{"name": "model_version", "type": ["null", "string"], "default": null}
    ,{"name": "model_fingerprint", "type": ["null", "string"], "default": null}
    ,{"name": "explained_value", "type": ["null", "double"], "default": null}
    ,{"name": "explanation_output", "type": ["null", "string"], "default": null}
  ]
}
"""


def validate_explainability_selection(config: ExplainabilityV2Config) -> None:
    """Validate the active Phase B model/Kernel selection before Kafka use."""
    if config.explained_model not in {"hstree", "iforest", "kitnet"}:
        raise ValueError("EXPLAINABILITY_MODEL must be 'hstree', 'iforest' or 'kitnet'")
    if config.explainability_mode not in {"simple", "reference"}:
        raise ValueError("EXPLAINABILITY_MODE must be 'simple' or 'reference'")
    if config.explanation_method not in {"kernel", "tree"}:
        raise ValueError("EXPLAINABILITY_METHOD must be 'kernel' or 'tree'")
    if config.explanation_method == "tree" and config.explained_model != "iforest":
        raise ValueError("EXPLAINABILITY_METHOD 'tree' requires 'iforest'")
    if config.hstree_variant not in {"pysad", "paper"}:
        raise ValueError("HSTREE_VARIANT must be 'pysad' or 'paper'")
    if not config.source_topic:
        raise ValueError("KAFKA_SOURCE_TOPIC is required when explainability is enabled")
    if config.explained_model == "iforest":
        psi = parse_iforest_max_samples(str(config.iforest_max_samples))
        parse_iforest_contamination(config.iforest_contamination)
        validate_iforest_params(
            config.iforest_initial_window_size,
            config.iforest_window_size,
            psi,
            config.iforest_n_estimators,
            config.iforest_refit_every,
        )

# Explanation status vocabulary fixed by the explainability-v2 sub-plan (OC-02).
# OC-01 only declares it; the runtime attribution is implemented in OC-02.
EXPLANATION_STATUSES = frozenset(
    {"disabled", "warmup", "background_not_ready", "computed", "failed", "model_mismatch"}
)

# Computation metadata (OC-03): the explanation profile of v2 is exhaustive and
# the local HSTree mirror version is fixed, so two outputs can be compared by
# configuration fingerprints.
EXPLANATION_PROFILE = "exhaustive"
HSTREE_MODEL_VERSION = "local-hstree-v3"
