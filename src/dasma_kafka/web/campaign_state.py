"""In-memory state for the AskMyDasma campaign."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from .models import CampaignView, PlatformStatus


def utc_now_iso() -> str:
    """Return a simple UTC ISO-8601 timestamp."""
    return datetime.now(timezone.utc).isoformat()


@dataclass
class CampaignState:
    """In-memory storage for a single active campaign."""

    current_campaign: CampaignView | None = None
    platform_status: PlatformStatus = "waiting_for_launch"

    def get(self) -> CampaignView | None:
        return self.current_campaign

    def get_platform_status(self) -> PlatformStatus:
        return self.platform_status

    def set(self, campaign: CampaignView) -> CampaignView:
        self.current_campaign = campaign
        return campaign

    def clear(self) -> None:
        self.current_campaign = None
        self.platform_status = "waiting_for_launch"

    def set_platform_status(self, status: PlatformStatus) -> PlatformStatus:
        self.platform_status = status
        return status
