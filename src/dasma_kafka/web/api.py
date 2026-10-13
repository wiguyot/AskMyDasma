"""HTTP routes for the AskMyDasma v1 backend."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from .campaign_state import CampaignState
from .models import (
    ActionResultView,
    CampaignProgressView,
    CampaignLaunchRequest,
    CampaignView,
    DatasetPreviewView,
    DatasetTimeRangeView,
    DatasetsView,
    GrafanaUrlView,
    PlatformStatusView,
    ServiceStatusView,
)
from .services.campaign_service import CampaignService
from .services.dataset_service import DatasetService
from .services.docker_service import DockerService
from .services.grafana_service import GrafanaService
from .services.progress_service import ProgressService


def build_api_router(
    state: CampaignState,
    dataset_service: DatasetService,
    docker_service: DockerService,
    grafana_service: GrafanaService,
    progress_service: ProgressService,
    *,
    restore_campaign: bool = True,
) -> APIRouter:
    """Build the v1 API router."""
    router = APIRouter(prefix="/api")
    campaign_service = CampaignService(
        state=state,
        settings=dataset_service.settings,
        dataset_service=dataset_service,
        docker_service=docker_service,
        grafana_service=grafana_service,
        progress_service=progress_service,
    )
    if restore_campaign:
        campaign_service.restore_from_campaign_config()

    @router.get("/status", response_model=PlatformStatusView)
    def get_status() -> PlatformStatusView:
        return campaign_service.get_platform_status()

    @router.get("/services", response_model=list[ServiceStatusView])
    def get_services() -> list[ServiceStatusView]:
        return docker_service.get_services_status(state.get())

    @router.get("/datasets", response_model=DatasetsView)
    def get_datasets() -> DatasetsView:
        return dataset_service.list_datasets()

    @router.get("/dataset-time-range", response_model=DatasetTimeRangeView)
    def get_dataset_time_range(dataset_path: str) -> DatasetTimeRangeView:
        return dataset_service.get_dataset_time_range(dataset_path)

    @router.get("/dataset-preview", response_model=DatasetPreviewView)
    def get_dataset_preview(dataset_path: str) -> DatasetPreviewView:
        return dataset_service.get_dataset_preview(dataset_path)

    @router.get("/grafana-url", response_model=GrafanaUrlView)
    def get_grafana_url() -> GrafanaUrlView:
        return grafana_service.get_url()

    @router.get("/current-campaign", response_model=CampaignView | None)
    def get_current_campaign() -> CampaignView | None:
        return campaign_service.get_current_campaign()

    @router.get("/campaign-progress", response_model=CampaignProgressView)
    def get_campaign_progress() -> CampaignProgressView:
        return campaign_service.get_campaign_progress()

    @router.post("/campaigns/launch", response_model=CampaignView)
    def launch_campaign(request: CampaignLaunchRequest) -> CampaignView:
        try:
            return campaign_service.launch_campaign(request)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.post("/campaigns/start-production", response_model=CampaignView)
    def start_production(request: CampaignLaunchRequest) -> CampaignView:
        try:
            return campaign_service.start_production(request)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.post("/campaigns/stop", response_model=ActionResultView)
    def stop_campaign(force: bool = False) -> ActionResultView:
        return campaign_service.stop_campaign(force=force)

    @router.post("/campaigns/reset", response_model=ActionResultView)
    def reset_campaign(force: bool = False) -> ActionResultView:
        return campaign_service.reset_campaign(force=force)

    @router.post("/campaigns/stop-and-reset", response_model=ActionResultView)
    def stop_and_reset_for_new_campaign(force: bool = False) -> ActionResultView:
        return campaign_service.stop_and_reset_for_new_campaign(force=force)

    @router.post("/campaigns/relaunch", response_model=CampaignView)
    def relaunch_campaign() -> CampaignView:
        try:
            return campaign_service.relaunch_campaign()
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    return router
