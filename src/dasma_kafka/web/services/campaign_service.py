"""Summary service for the current campaign state."""

from __future__ import annotations

import importlib.util
import json
import logging
import sys
from pathlib import Path

from fastapi import HTTPException

from ..campaign_state import CampaignState, utc_now_iso
from ..models import (
    ActionResultView,
    CampaignLaunchRequest,
    CampaignProgressView,
    CampaignView,
    ExplainabilityV1ProgressView,
    ExplainabilityV2ProgressView,
    PlatformStatusView,
)
from ..campaign_config_contract import (
    campaign_request_to_config_payload,
    flatten_structured_campaign,
)
from ..orchestration.campaign_orchestrator import CampaignOrchestrator
from ..settings import WebSettings
from .dataset_service import DatasetService
from .docker_service import DockerService
from .grafana_service import GrafanaService
from .progress_service import ProgressService


class CampaignService:
    """Expose high-level views related to the current campaign."""

    def __init__(
        self,
        state: CampaignState,
        settings: WebSettings,
        dataset_service: DatasetService,
        docker_service: DockerService,
        grafana_service: GrafanaService,
        progress_service: ProgressService,
        orchestrator: CampaignOrchestrator | None = None,
    ):
        self.state = state
        self.settings = settings
        self.dataset_service = dataset_service
        self.docker_service = docker_service
        self.grafana_service = grafana_service
        self.progress_service = progress_service
        self._orchestrator = orchestrator or CampaignOrchestrator(
            project_root=settings.project_root,
            scripts_web_dir=settings.web_scripts_dir,
            host_project_root=settings.host_project_root,
        )

    def get_current_campaign(self) -> CampaignView | None:
        return self.state.get()

    def get_campaign_progress(self) -> CampaignProgressView:
        return self.progress_service.get_campaign_progress(self.state.get())

    def _explainability_v2_guard(self, action: str, force: bool) -> ExplainabilityV2ProgressView | None:
        """Backward-compatible alias of the combined V1+V2 guard (V1-04c)."""
        guard = self._explainability_guard(action, force)
        if isinstance(guard, ExplainabilityV2ProgressView):
            return guard
        return None

    def _explainability_guard(self, action: str, force: bool):
        """Return the blocking progress view (V1 or V2) for the action, or None.

        stop is refused while any selected explainability model is processing;
        reset/stop-and-reset are refused while it is processing or failed, so
        an incomplete or failed campaign is never wiped silently. The
        orchestrator is never called before this guard decides. force=true is
        always an explicit user request and bypasses the guard.
        """
        if force:
            return None
        campaign = self.state.get()
        views = (
            self.progress_service.get_explainability_v1_progress(campaign),
            self.progress_service.get_explainability_v2_progress(campaign),
        )
        for progress in views:
            if not progress.selected:
                continue
            if action == "stop":
                blocked = progress.state == "processing"
            else:
                blocked = progress.state in {"processing", "failed"}
            if blocked:
                return progress
        return None

    @staticmethod
    def _explainability_model_label(progress) -> str:
        if isinstance(progress, ExplainabilityV1ProgressView):
            return "explainability"
        return "explainability-v2"

    def _raise_explainability_v2_conflict(self, action: str, progress) -> None:
        label = self._explainability_model_label(progress)
        raise HTTPException(
            status_code=409,
            detail={
                "state": progress.state,
                "reason": progress.reason,
                "message": (
                    f"{action} refused: {label} is {progress.state} "
                    f"({progress.reason}). Retry with force=true to proceed; "
                    "the campaign will remain incomplete."
                ),
            },
        )


    @property
    def _campaign_config_path(self) -> Path:
        return self.settings.tmp_dir / "current-campaign.json"

    def _load_campaign_config_module(self):
        """Load scripts/web/campaign_config.py, mirroring the orchestrator loader."""
        module_path = self.settings.web_scripts_dir / "campaign_config.py"
        spec = importlib.util.spec_from_file_location("askmydasma_campaign_config", module_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot load campaign config from {module_path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        return module

    def _write_campaign_config(
        self,
        request: CampaignLaunchRequest,
        *,
        selected_groups: list[str],
        excluded_fields: list[str],
    ) -> Path:
        """Persist the validated campaign request as the runtime source of truth."""
        config_path = self._campaign_config_path
        config_path.parent.mkdir(parents=True, exist_ok=True)
        payload = campaign_request_to_config_payload(
            request,
            selected_groups=selected_groups,
            excluded_fields=excluded_fields,
        )
        # Reject invalid campaigns before anything is persisted: an API request
        # bypassing the interface must fail here, not later during orchestration
        # with an invalid current-campaign.json on disk.
        self._load_campaign_config_module().load_campaign_dict(payload)
        config_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return config_path

    @staticmethod
    def _request_from_campaign_config(payload: dict[str, object]) -> CampaignLaunchRequest:
        """Build a validated launch request from the structured runtime JSON."""
        return CampaignLaunchRequest(**flatten_structured_campaign(payload))

    @staticmethod
    def _campaign_from_request(
        request: CampaignLaunchRequest,
        *,
        selected_groups: list[str],
        excluded_fields: list[str],
        status: str,
        created_at: str | None,
        started_at: str | None,
        updated_at: str | None,
    ) -> CampaignView:
        return CampaignView(
            dataset_path=request.dataset_path,
            mode=request.mode,
            selected_groups=selected_groups,
            excluded_fields=excluded_fields,
            selected_models=request.selected_models,
            hstree_estimator=request.hstree_estimator,
            hstree_threshold_window=request.hstree_threshold_window,
            hstree_welford_window=request.hstree_welford_window,
            hstree_anomaly_k_sigma=request.hstree_anomaly_k_sigma,
            hstree_anomaly_warmup_window=request.hstree_anomaly_warmup_window,
            hstree_initial_window_size=request.hstree_initial_window_size,
            hstree_window_size=request.hstree_window_size,
            hstree_num_trees=request.hstree_num_trees,
            hstree_variant=request.hstree_variant,
            hstree_max_depth=request.hstree_max_depth,
            iforest_estimator=request.iforest_estimator,
            iforest_threshold_window=request.iforest_threshold_window,
            iforest_welford_window=request.iforest_welford_window,
            iforest_anomaly_k_sigma=request.iforest_anomaly_k_sigma,
            iforest_anomaly_warmup_window=request.iforest_anomaly_warmup_window,
            iforest_initial_window_size=request.iforest_initial_window_size,
            iforest_window_size=request.iforest_window_size,
            iforest_n_estimators=request.iforest_n_estimators,
            iforest_max_samples=request.iforest_max_samples,
            iforest_contamination=request.iforest_contamination,
            iforest_refit_every=request.iforest_refit_every,
            kitnet_estimator=request.kitnet_estimator,
            kitnet_threshold_window=request.kitnet_threshold_window,
            kitnet_welford_window=request.kitnet_welford_window,
            kitnet_threshold_percentile=request.kitnet_threshold_percentile,
            kitnet_anomaly_k_sigma=request.kitnet_anomaly_k_sigma,
            kitnet_anomaly_warmup_window=request.kitnet_anomaly_warmup_window,
            kitnet_max_size_ae=request.kitnet_max_size_ae,
            kitnet_grace_feature_mapping=request.kitnet_grace_feature_mapping,
            kitnet_grace_anomaly_detector=request.kitnet_grace_anomaly_detector,
            kitnet_learning_rate=request.kitnet_learning_rate,
            kitnet_hidden_ratio=request.kitnet_hidden_ratio,
            kitnet_clip=request.kitnet_clip,
            deepant_estimator=request.deepant_estimator,
            deepant_threshold_window=request.deepant_threshold_window,
            deepant_welford_window=request.deepant_welford_window,
            deepant_threshold_percentile=request.deepant_threshold_percentile,
            deepant_anomaly_k_sigma=request.deepant_anomaly_k_sigma,
            deepant_anomaly_warmup_window=request.deepant_anomaly_warmup_window,
            deepant_lookback_size=request.deepant_lookback_size,
            deepant_initial_window_size=request.deepant_initial_window_size,
            deepant_refit_every=request.deepant_refit_every,
            deepant_window_size=request.deepant_window_size,
            deepant_filters=request.deepant_filters,
            deepant_kernel_size=request.deepant_kernel_size,
            deepant_dropout=request.deepant_dropout,
            deepant_lr=request.deepant_lr,
            deepant_epochs_initial=request.deepant_epochs_initial,
            deepant_epochs_refit=request.deepant_epochs_refit,
            deepant_batch_size=request.deepant_batch_size,
            deepant_max_group_workers=request.deepant_max_group_workers,
            mdragpoint_verdict_mode=request.mdragpoint_verdict_mode,
            mdragpoint_estimator=request.mdragpoint_estimator,
            mdragpoint_threshold_window=request.mdragpoint_threshold_window,
            mdragpoint_welford_window=request.mdragpoint_welford_window,
            mdragpoint_threshold_percentile=request.mdragpoint_threshold_percentile,
            mdragpoint_anomaly_k_sigma=request.mdragpoint_anomaly_k_sigma,
            mdragpoint_anomaly_warmup_window=request.mdragpoint_anomaly_warmup_window,
            mdragpoint_dynamic_threshold=request.mdragpoint_dynamic_threshold,
            mdragpoint_lof_init=request.mdragpoint_lof_init,
            mdragpoint_n_init=request.mdragpoint_n_init,
            mdragpoint_max_nb_clusters=request.mdragpoint_max_nb_clusters,
            mdragpoint_max_cluster_size=request.mdragpoint_max_cluster_size,
            mdragpoint_lof_neighbors=request.mdragpoint_lof_neighbors,
            hstree_repeatability=request.hstree_repeatability,
            iforest_repeatability=request.iforest_repeatability,
            kitnet_repeatability=request.kitnet_repeatability,
            deepant_repeatability=request.deepant_repeatability,
            mdragpoint_repeatability=request.mdragpoint_repeatability,
            hstree_reproducibility=request.hstree_reproducibility,
            iforest_reproducibility=request.iforest_reproducibility,
            kitnet_reproducibility=request.kitnet_reproducibility,
            deepant_reproducibility=request.deepant_reproducibility,
            mdragpoint_reproducibility=request.mdragpoint_reproducibility,
            reproducibility_source_role=request.reproducibility_source_role,
            reproducibility_campaign_id=request.reproducibility_campaign_id,
            explainability_nsamples=request.explainability_nsamples,
            explainability_v2_nsamples=request.explainability_v2_nsamples,
            explainability_model=request.explainability_model,
            explainability_mode=request.explainability_mode,
            explainability_method=request.explainability_method,
            explainability_v2_model=request.explainability_v2_model,
            explainability_v2_mode=request.explainability_v2_mode,
            explainability_v2_method=request.explainability_v2_method,
            status=status,
            created_at=created_at,
            started_at=started_at,
            updated_at=updated_at,
        )

    def restore_from_campaign_config(self) -> CampaignView | None:
        """Restore in-memory campaign state from the persisted runtime JSON."""
        if self.state.get() is not None:
            return self.state.get()

        config_path = self._campaign_config_path
        if not config_path.exists():
            return None

        try:
            payload = json.loads(config_path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("campaign config must be a JSON object")
            request = self._request_from_campaign_config(payload)
        except Exception as exc:
            logging.warning("Unable to restore campaign from %s: %s", config_path, exc)
            return None

        now = utc_now_iso()
        status = "configuring"
        started_at = None
        platform_status = "ready_for_production"

        try:
            services = self.docker_service.get_services_status()
            config_module = self._load_campaign_config_module()
            runtime_services = set(config_module.compose_services(request.model_dump()))
            runtime_services.add("producer")
            if any(
                service.name in runtime_services and service.status in {"healthy", "running"}
                for service in services
            ):
                status = "running"
                started_at = now
                platform_status = "processing_data"
        except Exception as exc:
            logging.warning("Unable to infer campaign runtime status: %s", exc)

        campaign = self._campaign_from_request(
            request,
            selected_groups=request.selected_groups,
            excluded_fields=request.excluded_fields,
            status=status,
            created_at=now,
            started_at=started_at,
            updated_at=now,
        )
        self.state.set(campaign)
        self.state.set_platform_status(platform_status)
        return campaign

    @staticmethod
    def _load_json_output(stdout: str) -> dict[str, object]:
        """Extract the last JSON object emitted by a control script."""
        for line in reversed(stdout.splitlines()):
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("{") and stripped.endswith("}"):
                return json.loads(stripped)
        return json.loads(stdout or "{}")

    @staticmethod
    def _ensure_script_dataset_matches(payload: dict[str, object], dataset_path: str) -> None:
        """Fail fast if a control script reports a different dataset than requested."""
        script_dataset_path = payload.get("dataset_path")
        if script_dataset_path is not None and script_dataset_path != dataset_path:
            raise RuntimeError(
                f"Control script used dataset {script_dataset_path!r}, expected {dataset_path!r}"
            )

    def get_platform_status(self) -> PlatformStatusView:
        services = self.docker_service.get_services_status()
        campaign = self.state.get()

        platform_status = self.state.get_platform_status()
        if any(service.status == "error" for service in services):
            platform_status = "error"

        campaign_status = campaign.status if campaign is not None else "idle"
        grafana_available = any(
            service.name == "grafana" and service.status in {"healthy", "running"}
            for service in services
        )

        return PlatformStatusView(
            platform_status=platform_status,
            campaign_status=campaign_status,
            grafana_available=grafana_available,
        )

    def launch_campaign(self, request: CampaignLaunchRequest) -> CampaignView:
        try:
            self.dataset_service.ensure_dataset_exists(request.dataset_path)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=f"Dataset not found: {exc.args[0]}") from exc

        self.state.set_platform_status("preparing_platform")
        try:
            preset = self.dataset_service.get_dataset_preset(request.dataset_path)
            selected_groups = request.selected_groups or (preset.selected_groups if preset is not None else [])
            excluded_fields = request.excluded_fields or (preset.excluded_fields if preset is not None else [])
            config_path = self._write_campaign_config(
                request,
                selected_groups=selected_groups,
                excluded_fields=excluded_fields,
            )
            payload = self._orchestrator.launch_campaign(config_path)
            self._ensure_script_dataset_matches(payload, request.dataset_path)
            now = utc_now_iso()
            campaign = self._campaign_from_request(
                request,
                selected_groups=selected_groups,
                excluded_fields=excluded_fields,
                status="configuring",
                created_at=now,
                started_at=None,
                updated_at=now,
            )
            self.state.set(campaign)
            self.state.set_platform_status("ready_for_production")
            return campaign
        except Exception:
            self.state.set_platform_status("error")
            raise

    def start_production(self, request: CampaignLaunchRequest) -> CampaignView:
        campaign = self.state.get()
        if campaign is None:
            raise HTTPException(status_code=400, detail="No campaign available to start")
        if campaign.status != "configuring":
            raise HTTPException(status_code=400, detail="Campaign is not ready for production")

        try:
            self.dataset_service.ensure_dataset_exists(request.dataset_path)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=f"Dataset not found: {exc.args[0]}") from exc

        preset = self.dataset_service.get_dataset_preset(request.dataset_path)
        selected_groups = request.selected_groups or (preset.selected_groups if preset is not None else [])
        excluded_fields = request.excluded_fields or (preset.excluded_fields if preset is not None else [])
        campaign.dataset_path = request.dataset_path
        campaign.mode = request.mode
        campaign.selected_groups = selected_groups
        campaign.excluded_fields = excluded_fields
        campaign.selected_models = request.selected_models
        campaign.hstree_estimator = request.hstree_estimator
        campaign.hstree_threshold_window = request.hstree_threshold_window
        campaign.hstree_welford_window = request.hstree_welford_window
        campaign.hstree_anomaly_k_sigma = request.hstree_anomaly_k_sigma
        campaign.hstree_anomaly_warmup_window = request.hstree_anomaly_warmup_window
        campaign.hstree_initial_window_size = request.hstree_initial_window_size
        campaign.hstree_window_size = request.hstree_window_size
        campaign.hstree_num_trees = request.hstree_num_trees
        campaign.hstree_variant = request.hstree_variant
        campaign.hstree_max_depth = request.hstree_max_depth
        campaign.iforest_estimator = request.iforest_estimator
        campaign.iforest_threshold_window = request.iforest_threshold_window
        campaign.iforest_welford_window = request.iforest_welford_window
        campaign.iforest_anomaly_k_sigma = request.iforest_anomaly_k_sigma
        campaign.iforest_anomaly_warmup_window = request.iforest_anomaly_warmup_window
        campaign.iforest_initial_window_size = request.iforest_initial_window_size
        campaign.iforest_window_size = request.iforest_window_size
        campaign.iforest_n_estimators = request.iforest_n_estimators
        campaign.iforest_max_samples = request.iforest_max_samples
        campaign.iforest_contamination = request.iforest_contamination
        campaign.iforest_refit_every = request.iforest_refit_every
        campaign.kitnet_estimator = request.kitnet_estimator
        campaign.kitnet_threshold_window = request.kitnet_threshold_window
        campaign.kitnet_welford_window = request.kitnet_welford_window
        campaign.kitnet_threshold_percentile = request.kitnet_threshold_percentile
        campaign.kitnet_anomaly_k_sigma = request.kitnet_anomaly_k_sigma
        campaign.kitnet_anomaly_warmup_window = request.kitnet_anomaly_warmup_window
        campaign.kitnet_max_size_ae = request.kitnet_max_size_ae
        campaign.kitnet_grace_feature_mapping = request.kitnet_grace_feature_mapping
        campaign.kitnet_grace_anomaly_detector = request.kitnet_grace_anomaly_detector
        campaign.kitnet_learning_rate = request.kitnet_learning_rate
        campaign.kitnet_hidden_ratio = request.kitnet_hidden_ratio
        campaign.kitnet_clip = request.kitnet_clip
        campaign.deepant_estimator = request.deepant_estimator
        campaign.deepant_threshold_window = request.deepant_threshold_window
        campaign.deepant_welford_window = request.deepant_welford_window
        campaign.deepant_threshold_percentile = request.deepant_threshold_percentile
        campaign.deepant_anomaly_k_sigma = request.deepant_anomaly_k_sigma
        campaign.deepant_anomaly_warmup_window = request.deepant_anomaly_warmup_window
        campaign.deepant_lookback_size = request.deepant_lookback_size
        campaign.deepant_initial_window_size = request.deepant_initial_window_size
        campaign.deepant_refit_every = request.deepant_refit_every
        campaign.deepant_window_size = request.deepant_window_size
        campaign.deepant_filters = request.deepant_filters
        campaign.deepant_kernel_size = request.deepant_kernel_size
        campaign.deepant_dropout = request.deepant_dropout
        campaign.deepant_lr = request.deepant_lr
        campaign.deepant_epochs_initial = request.deepant_epochs_initial
        campaign.deepant_epochs_refit = request.deepant_epochs_refit
        campaign.deepant_batch_size = request.deepant_batch_size
        campaign.deepant_max_group_workers = request.deepant_max_group_workers
        campaign.mdragpoint_verdict_mode = request.mdragpoint_verdict_mode
        campaign.mdragpoint_estimator = request.mdragpoint_estimator
        campaign.mdragpoint_threshold_window = request.mdragpoint_threshold_window
        campaign.mdragpoint_welford_window = request.mdragpoint_welford_window
        campaign.mdragpoint_threshold_percentile = request.mdragpoint_threshold_percentile
        campaign.mdragpoint_anomaly_k_sigma = request.mdragpoint_anomaly_k_sigma
        campaign.mdragpoint_anomaly_warmup_window = request.mdragpoint_anomaly_warmup_window
        campaign.mdragpoint_dynamic_threshold = request.mdragpoint_dynamic_threshold
        campaign.mdragpoint_lof_init = request.mdragpoint_lof_init
        campaign.mdragpoint_n_init = request.mdragpoint_n_init
        campaign.mdragpoint_max_nb_clusters = request.mdragpoint_max_nb_clusters
        campaign.mdragpoint_max_cluster_size = request.mdragpoint_max_cluster_size
        campaign.mdragpoint_lof_neighbors = request.mdragpoint_lof_neighbors
        campaign.explainability_nsamples = request.explainability_nsamples
        campaign.explainability_v2_nsamples = request.explainability_v2_nsamples
        campaign.explainability_model = request.explainability_model
        campaign.explainability_mode = request.explainability_mode
        campaign.explainability_method = request.explainability_method
        campaign.explainability_v2_model = request.explainability_v2_model
        campaign.explainability_v2_mode = request.explainability_v2_mode
        campaign.explainability_v2_method = request.explainability_v2_method
        campaign.status = "starting"
        campaign.updated_at = utc_now_iso()
        self.state.set(campaign)
        try:
            config_path = self._write_campaign_config(
                request,
                selected_groups=selected_groups,
                excluded_fields=excluded_fields,
            )
            payload = self._orchestrator.start_production(config_path)
            self._ensure_script_dataset_matches(payload, request.dataset_path)

            now = utc_now_iso()
            campaign.status = "running"
            if campaign.started_at is None:
                campaign.started_at = now
            campaign.updated_at = now
            self.state.set_platform_status("processing_data")
            return self.state.set(campaign)
        except Exception:
            campaign.status = "configuring"
            campaign.updated_at = utc_now_iso()
            self.state.set(campaign)
            self.state.set_platform_status("error")
            raise

    def stop_campaign(self, force: bool = False) -> ActionResultView:
        guard = self._explainability_guard("stop", force)
        if guard is not None:
            self._raise_explainability_v2_conflict("stop", guard)
        self.state.set_platform_status("stopping_platform")
        try:
            self._orchestrator.stop_campaign()

            campaign = self.state.get()
            if campaign is not None:
                campaign.status = "stopping"
                campaign.updated_at = utc_now_iso()
                self.state.set(campaign)

            self.state.set_platform_status("waiting_for_launch")
            return ActionResultView(
                status="ok", action="stop_campaign", forced=force, campaign_incomplete=force
            )
        except Exception:
            self.state.set_platform_status("error")
            raise

    def reset_campaign(self, force: bool = False) -> ActionResultView:
        guard = self._explainability_guard("reset", force)
        if guard is not None:
            self._raise_explainability_v2_conflict("reset", guard)
        self.state.set_platform_status("resetting_platform")
        try:
            payload = self._orchestrator.reset_campaign()

            campaign = self.state.get()
            if campaign is not None:
                campaign.status = "reseted"
                campaign.updated_at = utc_now_iso()
                self.state.set(campaign)

            self.state.set_platform_status("waiting_for_launch")
            return ActionResultView(
                status=payload.get("status", "ok"),
                action=payload.get("action", "reset_campaign"),
                details=payload.get("details"),
                forced=force,
                campaign_incomplete=force,
            )
        except Exception:
            self.state.set_platform_status("error")
            raise

    def stop_and_reset_for_new_campaign(self, force: bool = False) -> ActionResultView:
        guard = self._explainability_guard("reset", force)
        if guard is not None:
            self._raise_explainability_v2_conflict("stop_and_reset", guard)
        self.state.set_platform_status("stopping_platform")
        try:
            self._orchestrator.stop_campaign()

            campaign = self.state.get()
            if campaign is not None:
                campaign.status = "stopping"
                campaign.updated_at = utc_now_iso()
                self.state.set(campaign)

            self.state.set_platform_status("resetting_platform")
            payload = self._orchestrator.reset_campaign()

            if campaign is not None:
                campaign.status = "reseted"
                campaign.updated_at = utc_now_iso()
                self.state.set(campaign)

            self.state.clear()
            self.state.set_platform_status("waiting_for_launch")
            return ActionResultView(
                status=payload.get("status", "ok"),
                action="stop_and_reset_for_new_campaign",
                details=payload.get("details"),
                forced=force,
                campaign_incomplete=force,
            )
        except Exception:
            self.state.set_platform_status("error")
            raise

    def relaunch_campaign(self) -> CampaignView:
        campaign = self.state.get()
        if campaign is None:
            raise HTTPException(status_code=400, detail="No campaign available to relaunch")

        try:
            launch_request = CampaignLaunchRequest(
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
            self.launch_campaign(launch_request)
            return self.start_production(launch_request)
        except Exception:
            self.state.set_platform_status("error")
            raise
