"""Service for accessing simplified Docker state."""

from __future__ import annotations

import json

from ..models import ServiceStatusView
from ..orchestration.compose_runner import ComposeRunner
from ..settings import WebSettings
from .expected_services import expected_service_names, merge_services


class DockerService:
    """Query Docker Compose service state."""

    def __init__(self, settings: WebSettings):
        self.settings = settings
        self._runner = ComposeRunner(settings.project_root, host_project_root=settings.host_project_root)

    def get_services_status(self, campaign=None) -> list[ServiceStatusView]:
        raw = self._runner.ps().strip()
        services: list[ServiceStatusView] = []
        if raw:
            for line in raw.splitlines():
                item = json.loads(line)
                state = (item.get("State") or "").lower()
                health = (item.get("Health") or "").lower()
                health_view = health or "not_configured"

                if state == "running" and health == "healthy":
                    status = "healthy"
                elif state == "running":
                    status = "running"
                elif state in {"created", "restarting"}:
                    status = "starting"
                elif state in {"exited", "dead", "removing"}:
                    status = "stopped"
                elif not state:
                    status = "unknown"
                else:
                    status = "error"

                services.append(ServiceStatusView(
                    name=item.get("Service"),
                    container_name=item.get("Name"),
                    state=state or "unknown",
                    health=health_view,
                    status=status,
                ))

        expected = expected_service_names(campaign, self.settings.web_scripts_dir)
        return merge_services(services, expected)
