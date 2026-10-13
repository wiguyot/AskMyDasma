"""AskMyDasma web backend configuration."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path


@dataclass(frozen=True)
class WebSettings:
    """Paths and URLs used by the web backend."""

    project_root: Path
    scripts_dir: Path
    web_scripts_dir: Path
    data_dir: Path
    tmp_dir: Path
    frontend_dir: Path
    # Real host path of the project root. Equals project_root when running on
    # the host. In container mode, project_root is /project while
    # host_project_root is the actual host path (e.g. /Users/.../dasma-kafka).
    # ComposeRunner passes this as --project-directory so Docker resolves
    # volume mounts against host paths, not container-internal paths.
    host_project_root: Path
    grafana_url: str = "http://localhost:3000"


def _repo_root() -> Path:
    """Fallback: derive project root from this file's location in the repo layout."""
    return Path(__file__).resolve().parents[3]


def load_settings() -> WebSettings:
    """Build backend settings from environment variables, with repo-layout fallback.

    Environment variables (all optional):
      ASKMYDASMA_PROJECT_ROOT      — absolute path to the project root (in-container view)
      ASKMYDASMA_HOST_PROJECT_ROOT — real host path of the project root (for docker compose)
      ASKMYDASMA_SCRIPTS_DIR       — path to scripts/
      ASKMYDASMA_WEB_SCRIPTS_DIR   — path to scripts/web/
      ASKMYDASMA_DATA_DIR          — path to data/
      ASKMYDASMA_TMP_DIR           — path to .tmp/
      ASKMYDASMA_FRONTEND_DIR      — path to frontend/askmydasma/
      ASKMYDASMA_GRAFANA_URL       — Grafana base URL
    """
    project_root = Path(os.environ["ASKMYDASMA_PROJECT_ROOT"]) if "ASKMYDASMA_PROJECT_ROOT" in os.environ else _repo_root()
    host_project_root = Path(os.environ["ASKMYDASMA_HOST_PROJECT_ROOT"]) if "ASKMYDASMA_HOST_PROJECT_ROOT" in os.environ else project_root

    scripts_dir = Path(os.environ["ASKMYDASMA_SCRIPTS_DIR"]) if "ASKMYDASMA_SCRIPTS_DIR" in os.environ else project_root / "scripts"
    web_scripts_dir = Path(os.environ["ASKMYDASMA_WEB_SCRIPTS_DIR"]) if "ASKMYDASMA_WEB_SCRIPTS_DIR" in os.environ else scripts_dir / "web"
    data_dir = Path(os.environ["ASKMYDASMA_DATA_DIR"]) if "ASKMYDASMA_DATA_DIR" in os.environ else project_root / "data"
    tmp_dir = Path(os.environ["ASKMYDASMA_TMP_DIR"]) if "ASKMYDASMA_TMP_DIR" in os.environ else project_root / ".tmp"
    frontend_dir = Path(os.environ["ASKMYDASMA_FRONTEND_DIR"]) if "ASKMYDASMA_FRONTEND_DIR" in os.environ else project_root / "frontend" / "askmydasma"
    grafana_url = os.environ.get("ASKMYDASMA_GRAFANA_URL", "http://localhost:3000")

    return WebSettings(
        project_root=project_root,
        host_project_root=host_project_root,
        scripts_dir=scripts_dir,
        web_scripts_dir=web_scripts_dir,
        data_dir=data_dir,
        tmp_dir=tmp_dir,
        frontend_dir=frontend_dir,
        grafana_url=grafana_url,
    )
