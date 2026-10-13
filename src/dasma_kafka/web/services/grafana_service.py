"""Grafana access service."""

from __future__ import annotations

from ..models import GrafanaUrlView
from ..settings import WebSettings


class GrafanaService:
    """Expose simple Grafana-related information."""

    def __init__(self, settings: WebSettings):
        self.settings = settings

    def get_url(self) -> GrafanaUrlView:
        return GrafanaUrlView(url=self.settings.grafana_url)
