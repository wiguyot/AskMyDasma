"""Data schemas for the AskMyDasma backend."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

CampaignMode = Literal["batch", "realtime", "realtime-x5", "realtime-x20", "realtime-x100"]
EstimatorMode = Literal["p99", "welford", "welford-windowed"]
IForestEstimatorMode = Literal["native", "p99", "welford", "welford-windowed"]
MdragPointVerdictMode = Literal["internal", "external"]
# Allowed KernelSHAP sample budgets exposed in the UI for explainability v1/v2.
# Default 100 preserves the historical baseline behavior.
ShapNsamples = Literal[100, 1000, 1500, 2000]
ExplainabilityModel = Literal["hstree", "iforest", "kitnet"]
ExplainabilityMode = Literal["simple", "reference"]
ExplainabilityMethod = Literal["kernel", "tree"]
HSTreeVariant = Literal["pysad", "paper"]


PlatformStatus = Literal[
    "waiting_for_launch",
    "preparing_platform",
    "ready_for_production",
    "processing_data",
    "stopping_platform",
    "resetting_platform",
    "error",
]


CampaignStatus = Literal[
    "idle",
    "configuring",
    "starting",
    "running",
    "stopping",
    "resetting",
    "reseted",
    "error",
]


# Explainability v2 campaign completion states. `complete` is a cautious
# conclusion: it is only derived from a known Kafka consumer lag plus a
# finished upstream plus enough published v2 results (OC-11).
ExplainabilityV2State = Literal["processing", "complete", "failed"]
ExplainabilityV1State = Literal["processing", "complete", "failed"]


DEFAULT_SELECTED_MODELS: list[str] = ["hstree", "iforest", "kitnet"]
# DeepAnT is opt-in by default (heavier service, longer warmup, torch install).
# Activate it from the UI checkbox or by setting DASMA_SELECTED_MODELS.


class CampaignView(BaseModel):
    """View of the current campaign."""

    dataset_path: str
    mode: CampaignMode
    selected_groups: list[str] = Field(default_factory=list)
    excluded_fields: list[str] = Field(default_factory=list)
    selected_models: list[str] = Field(default_factory=lambda: list(DEFAULT_SELECTED_MODELS))
    hstree_estimator: EstimatorMode = "p99"
    hstree_threshold_window: int = 500
    hstree_welford_window: int = 2048
    hstree_anomaly_k_sigma: float = 3.0
    hstree_anomaly_warmup_window: int = 100
    hstree_initial_window_size: int = 256
    hstree_window_size: int = 256
    hstree_num_trees: int = 25
    hstree_variant: HSTreeVariant = "paper"
    hstree_max_depth: int = 15
    iforest_estimator: IForestEstimatorMode = "native"
    iforest_threshold_window: int = 500
    iforest_welford_window: int = 2048
    iforest_anomaly_k_sigma: float = 3.0
    iforest_anomaly_warmup_window: int = 100
    iforest_initial_window_size: int = 512
    iforest_window_size: int = 2048
    iforest_n_estimators: int = 100
    iforest_max_samples: str = "256"
    iforest_contamination: str = "auto"
    iforest_refit_every: int = 128
    kitnet_estimator: EstimatorMode = "welford-windowed"
    kitnet_threshold_window: int = 1000
    kitnet_welford_window: int = 1000
    kitnet_threshold_percentile: float = 99.0
    kitnet_anomaly_k_sigma: float = 3.0
    kitnet_anomaly_warmup_window: int = 100
    kitnet_max_size_ae: int = 10
    kitnet_grace_feature_mapping: int = 1000
    kitnet_grace_anomaly_detector: int = 1000
    kitnet_learning_rate: float = 0.1
    kitnet_hidden_ratio: float = 0.75
    kitnet_clip: float = 10.0
    deepant_estimator: EstimatorMode = "welford-windowed"
    deepant_threshold_window: int = 500
    deepant_welford_window: int = 2048
    deepant_threshold_percentile: float = 99.0
    deepant_anomaly_k_sigma: float = 3.0
    deepant_anomaly_warmup_window: int = 100
    deepant_lookback_size: int = 100
    deepant_initial_window_size: int = 512
    deepant_refit_every: int = 128
    deepant_window_size: int = 2048
    deepant_filters: int = 32
    deepant_kernel_size: int = 2
    deepant_dropout: float = 0.25
    deepant_lr: float = 0.001
    deepant_epochs_initial: int = 10
    deepant_epochs_refit: int = 5
    deepant_batch_size: int = 32
    deepant_max_group_workers: int = 2
    mdragpoint_verdict_mode: MdragPointVerdictMode = "internal"
    mdragpoint_estimator: EstimatorMode = "p99"
    mdragpoint_threshold_window: int = 500
    mdragpoint_welford_window: int = 900
    mdragpoint_threshold_percentile: float = 99.0
    mdragpoint_anomaly_k_sigma: float = 3.0
    mdragpoint_anomaly_warmup_window: int = 100
    mdragpoint_dynamic_threshold: bool = True
    mdragpoint_lof_init: bool = True
    mdragpoint_n_init: int = 1000
    mdragpoint_max_nb_clusters: int = 50
    mdragpoint_max_cluster_size: int = 30
    mdragpoint_lof_neighbors: int = 20
    hstree_repeatability: bool = False
    iforest_repeatability: bool = False
    kitnet_repeatability: bool = False
    deepant_repeatability: bool = False
    mdragpoint_repeatability: bool = False
    hstree_reproducibility: bool = False
    iforest_reproducibility: bool = False
    kitnet_reproducibility: bool = False
    deepant_reproducibility: bool = False
    mdragpoint_reproducibility: bool = False
    reproducibility_source_role: str = ""
    reproducibility_campaign_id: str = ""
    explainability_nsamples: ShapNsamples = 100
    explainability_v2_nsamples: ShapNsamples = 100
    explainability_model: ExplainabilityModel | None = None
    explainability_mode: ExplainabilityMode | None = None
    explainability_method: ExplainabilityMethod | None = None
    explainability_v2_model: ExplainabilityModel | None = None
    explainability_v2_mode: ExplainabilityMode | None = None
    explainability_v2_method: ExplainabilityMethod | None = None
    status: CampaignStatus
    created_at: str | None = None
    started_at: str | None = None
    updated_at: str | None = None


class CampaignLaunchRequest(BaseModel):
    """Minimal payload used to launch a campaign."""

    dataset_path: str
    mode: CampaignMode = "realtime"
    selected_groups: list[str] = Field(default_factory=list)
    excluded_fields: list[str] = Field(default_factory=list)
    selected_models: list[str] = Field(default_factory=lambda: list(DEFAULT_SELECTED_MODELS))
    hstree_estimator: EstimatorMode = "p99"
    hstree_threshold_window: int = 500
    hstree_welford_window: int = Field(default=2048, ge=1)
    hstree_anomaly_k_sigma: float = Field(default=3.0, gt=0)
    hstree_anomaly_warmup_window: int = Field(default=100, ge=2)
    hstree_initial_window_size: int = Field(default=256, ge=1)
    hstree_window_size: int = Field(default=256, ge=64)
    hstree_num_trees: int = Field(default=25, ge=1)
    hstree_variant: HSTreeVariant = "paper"
    hstree_max_depth: int = Field(default=15, ge=1)
    iforest_estimator: IForestEstimatorMode = "native"
    iforest_threshold_window: int = 500
    iforest_welford_window: int = Field(default=2048, ge=1)
    iforest_anomaly_k_sigma: float = Field(default=3.0, gt=0)
    iforest_anomaly_warmup_window: int = Field(default=100, ge=2)
    iforest_initial_window_size: int = Field(default=512, ge=1)
    iforest_window_size: int = Field(default=2048, ge=64)
    iforest_n_estimators: int = Field(default=100, ge=1)
    iforest_max_samples: str = "256"
    iforest_contamination: str = "auto"
    iforest_refit_every: int = Field(default=128, ge=1)
    kitnet_estimator: EstimatorMode = "welford-windowed"
    kitnet_threshold_window: int = 1000
    kitnet_welford_window: int = Field(default=1000, ge=1)
    kitnet_threshold_percentile: float = Field(default=99.0, gt=0, lt=100)
    kitnet_anomaly_k_sigma: float = Field(default=3.0, gt=0)
    kitnet_anomaly_warmup_window: int = Field(default=100, ge=2)
    kitnet_max_size_ae: int = Field(default=10, ge=1)
    kitnet_grace_feature_mapping: int = Field(default=1000, ge=0)
    kitnet_grace_anomaly_detector: int = Field(default=1000, ge=0)
    kitnet_learning_rate: float = Field(default=0.1, gt=0)
    kitnet_hidden_ratio: float = Field(default=0.75, gt=0, le=1)
    kitnet_clip: float = Field(default=10.0, gt=0)
    deepant_estimator: EstimatorMode = "welford-windowed"
    deepant_threshold_window: int = Field(default=500, ge=50)
    deepant_welford_window: int = Field(default=2048, ge=1)
    deepant_threshold_percentile: float = Field(default=99.0, gt=0, lt=100)
    deepant_anomaly_k_sigma: float = Field(default=3.0, gt=0)
    deepant_anomaly_warmup_window: int = Field(default=100, ge=2)
    deepant_lookback_size: int = Field(default=100, ge=8)
    deepant_initial_window_size: int = Field(default=512, ge=8)
    deepant_refit_every: int = Field(default=128, ge=1)
    deepant_window_size: int = Field(default=2048, ge=64)
    deepant_filters: int = Field(default=32, ge=1)
    deepant_kernel_size: int = Field(default=2, ge=1)
    deepant_dropout: float = Field(default=0.25, ge=0, lt=1)
    deepant_lr: float = Field(default=0.001, gt=0)
    deepant_epochs_initial: int = Field(default=10, ge=1)
    deepant_epochs_refit: int = Field(default=5, ge=1)
    deepant_batch_size: int = Field(default=32, ge=1)
    deepant_max_group_workers: int = Field(default=2, ge=1)
    mdragpoint_verdict_mode: MdragPointVerdictMode = "internal"
    mdragpoint_estimator: EstimatorMode = "p99"
    mdragpoint_threshold_window: int = Field(default=500, ge=50)
    mdragpoint_welford_window: int = Field(default=900, ge=1)
    mdragpoint_threshold_percentile: float = Field(default=99.0, gt=0, lt=100)
    mdragpoint_anomaly_k_sigma: float = Field(default=3.0, gt=0)
    mdragpoint_anomaly_warmup_window: int = Field(default=100, ge=2)
    mdragpoint_dynamic_threshold: bool = True
    mdragpoint_lof_init: bool = True
    mdragpoint_n_init: int = Field(default=1000, ge=2)
    mdragpoint_max_nb_clusters: int = Field(default=50, ge=1)
    mdragpoint_max_cluster_size: int = Field(default=30, ge=1)
    mdragpoint_lof_neighbors: int = Field(default=20, ge=1)
    hstree_repeatability: bool = False
    iforest_repeatability: bool = False
    kitnet_repeatability: bool = False
    deepant_repeatability: bool = False
    mdragpoint_repeatability: bool = False
    hstree_reproducibility: bool = False
    iforest_reproducibility: bool = False
    kitnet_reproducibility: bool = False
    deepant_reproducibility: bool = False
    mdragpoint_reproducibility: bool = False
    reproducibility_source_role: str = ""
    reproducibility_campaign_id: str = ""
    explainability_nsamples: ShapNsamples = 100
    explainability_v2_nsamples: ShapNsamples = 100
    explainability_model: ExplainabilityModel | None = None
    explainability_mode: ExplainabilityMode | None = None
    explainability_method: ExplainabilityMethod | None = None
    explainability_v2_model: ExplainabilityModel | None = None
    explainability_v2_mode: ExplainabilityMode | None = None
    explainability_v2_method: ExplainabilityMethod | None = None


class ActionResultView(BaseModel):
    """Standard result of a control action."""

    status: str
    action: str
    details: str | None = None
    # Set only when the caller explicitly forced the action past the
    # explainability-v2 guard. A forced action never claims completeness.
    forced: bool = False
    campaign_incomplete: bool = False


ServiceGroup = Literal[
    "Infrastructure",
    "Detectors",
    "Explainability",
    "Validation",
    "Observability",
]


class ServiceStatusView(BaseModel):
    """Simplified view of a Docker service.

    The admin-specific fields (expected/role/group/permanent) are optional so
    existing consumers of /api/services keep working unchanged. expected is
    true/false/null (null = the backend cannot decide); permanent=false marks
    one-shot initialization services.
    """

    name: str | None = None
    container_name: str | None = None
    state: str = "unknown"
    health: str = "unknown"
    status: str = "unknown"
    expected: bool | None = None
    role: str | None = None
    group: ServiceGroup | None = None
    permanent: bool | None = None


class PlatformStatusView(BaseModel):
    """Summary view of platform state."""

    platform_status: PlatformStatus
    campaign_status: CampaignStatus
    grafana_available: bool


class ProgressItemView(BaseModel):
    """Simplified progress state of an output pipeline."""

    label: str
    topic: str
    produced_records: int = 0
    total_records: int = 0
    progress_percent: float = 0.0


class ExplainabilityV2ProgressView(BaseModel):
    """Dedicated completion view of explainability-v2 (OC-11).

    `state` stays null when v2 is not part of the campaign: no v2 state is
    fabricated for a campaign that does not enable it. Unknown observations
    are reported as `processing` with a readable reason, never as `complete`.
    """

    selected: bool = False
    state: ExplainabilityV2State | None = None
    reason: str | None = None
    upstream_records: int | None = None
    published_records: int | None = None
    consumer_lag: int | None = None


class ExplainabilityV1ProgressView(BaseModel):
    """Dedicated completion view of explainability V1 (V1-04c).

    Same shape as the v2 view: `state` stays null when V1 is not part of the
    campaign, and ambiguous observations are reported as `processing` with a
    readable reason, never as `complete`. Completeness is operational and
    at least once: it infers the contiguous commits from V1-03 (commit only
    after both Avro and JSON confirmations); it never claims exactly once.
    """

    selected: bool = False
    state: ExplainabilityV1State | None = None
    reason: str | None = None
    upstream_records: int | None = None
    published_records: int | None = None
    consumer_lag: int | None = None


class CampaignProgressView(BaseModel):
    """Global progress view of the current campaign."""

    dataset_path: str | None = None
    total_records: int = 0
    items: list[ProgressItemView] = Field(default_factory=list)
    explainability_v2: ExplainabilityV2ProgressView | None = None
    explainability_v1: ExplainabilityV1ProgressView | None = None


class DatasetPresetView(BaseModel):
    """Prefill associated with an explicitly supported dataset."""

    label: str | None = None
    selected_groups: list[str] = Field(default_factory=list)
    excluded_fields: list[str] = Field(default_factory=list)
    source: str = "docker-compose"


class DatasetsView(BaseModel):
    """List of available datasets."""

    datasets: list[str] = Field(default_factory=list)
    presets: dict[str, DatasetPresetView] = Field(default_factory=dict)


class DatasetTimeRangeView(BaseModel):
    """Detected time summary for a source dataset."""

    dataset_path: str
    row_count: int = 0
    mode: str = "source"
    time_column: str | None = None
    start: str | None = None
    end: str | None = None
    duration_seconds: int | None = None
    status: str = "not_detected"


class DatasetPreviewView(BaseModel):
    """Preview of columns and first rows of a source dataset."""

    dataset_path: str
    columns: list[str] = Field(default_factory=list)
    sample_rows: list[dict[str, str | None]] = Field(default_factory=list)
    max_rows: int = 10
    status: str = "empty"


class GrafanaUrlView(BaseModel):
    """Grafana URL exposed to the frontend."""

    url: str
