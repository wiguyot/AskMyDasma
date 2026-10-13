"""AskMyDasma FastAPI application v1."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from .api import build_api_router
from .campaign_state import CampaignState
from .services.dataset_service import DatasetService
from .services.docker_service import DockerService
from .services.grafana_service import GrafanaService
from .services.progress_service import ProgressService
from .settings import load_settings


def create_app(*, restore_campaign: bool = True) -> FastAPI:
    """Create the v1 FastAPI application."""
    settings = load_settings()
    state = CampaignState()
    dataset_service = DatasetService(settings)
    docker_service = DockerService(settings)
    grafana_service = GrafanaService(settings)
    progress_service = ProgressService(settings)

    app = FastAPI(title="AskMyDasma Backend", version="0.1.0")
    app.include_router(
        build_api_router(
            state=state,
            dataset_service=dataset_service,
            docker_service=docker_service,
            grafana_service=grafana_service,
            progress_service=progress_service,
            restore_campaign=restore_campaign,
        )
    )

    admin_frontend_dir = settings.frontend_dir / "admin"
    if admin_frontend_dir.exists():
        app.mount(
            "/askmydasma/admin",
            StaticFiles(directory=admin_frontend_dir, html=True),
            name="askmydasma-admin",
        )

    if settings.frontend_dir.exists():
        app.mount(
            "/askmydasma",
            StaticFiles(directory=settings.frontend_dir, html=True),
            name="askmydasma",
        )

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


app = create_app()
