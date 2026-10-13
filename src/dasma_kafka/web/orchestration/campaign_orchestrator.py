"""Python replacement for the bash campaign control scripts."""

from __future__ import annotations

import importlib.util
import logging
import sys
import time
from pathlib import Path
from types import ModuleType

import httpx

from .compose_runner import ComposeRunner, ComposeError

logger = logging.getLogger(__name__)

_LAUNCH_IMAGES = (
    "init-topic",
    "hstree-analysis",
    "iforest-analysis",
    "kitnet-analysis",
    "deepant-analysis",
    "mdragstream-point-analysis",
    "explainability",
    "explainability-v2",
    # Repeatability services: the pair resolves its model image from the
    # campaign env (REPEATABILITY_IMAGE), the diff service reuses kafka-tools.
    "repeatability-v1",
    "repeatability-v2",
    "repeatability-diff",
    "reproducibility-export",
    "consensus-analysis",
    "build-metrics",
    "telegraf",
)

_RESET_IMAGES = ("init-topic", "influxdb", "schema-registry", "kafka1")

_PRODUCER_START_SIGNAL = "/tmp/dasma-producer-start.signal"
_PRODUCER_READY_RETRIES = 30
_PRODUCER_READY_SLEEP = 1.0


def _load_campaign_config(scripts_web_dir: Path) -> ModuleType:
    """Import campaign_config.py as a module from scripts/web/."""
    spec = importlib.util.spec_from_file_location(
        "campaign_config",
        scripts_web_dir / "campaign_config.py",
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load campaign_config from {scripts_web_dir}")
    module = importlib.util.module_from_spec(spec)
    sys.modules["campaign_config"] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


class CampaignOrchestrator:
    """Python equivalent of the bash campaign control scripts.

    Replaces launch_campaign.sh, start_production.sh, stop_campaign.sh,
    reset_campaign.sh and relaunch_campaign.sh with direct Python + ComposeRunner
    calls. campaign_config.py is imported as a module to avoid subprocess eval.
    """

    def __init__(
        self,
        project_root: Path,
        scripts_web_dir: Path,
        host_project_root: Path | None = None,
        schema_registry_url: str = "http://localhost:8081",
    ) -> None:
        self.project_root = project_root
        self._runner = ComposeRunner(project_root, host_project_root=host_project_root or project_root)
        self._cc = _load_campaign_config(scripts_web_dir)
        self._schema_registry_url = schema_registry_url

    # ------------------------------------------------------------------
    # launch_campaign.sh
    # ------------------------------------------------------------------

    def launch_campaign(self, config_path: Path) -> dict:
        """Prepare topics and images for a campaign. Returns action payload."""
        config = self._cc.load_campaign(config_path)

        data_file = self.project_root / config["dataset_path"]
        if not data_file.exists():
            raise FileNotFoundError(f"Dataset not found: {data_file}")

        # Build the campaign env once, before image preparation: the repeatability
        # pair resolves its image (REPEATABILITY_IMAGE) from this environment.
        env = self._cc.build_env(config)

        logger.info("[launch] ensuring runtime images")
        self._runner.pull_missing(*_LAUNCH_IMAGES, env=env)

        logger.info("[launch] ensuring kafka topics")
        self._runner.up("init-topic", force_recreate=True, env=env)

        return self._cc.build_action_payload(config, "launch_campaign")

    # ------------------------------------------------------------------
    # start_production.sh
    # ------------------------------------------------------------------

    def start_production(self, config_path: Path) -> dict:
        """Start runtime services and trigger the producer. Returns action payload."""
        config = self._cc.load_campaign(config_path)

        data_file = self.project_root / config["dataset_path"]
        if not data_file.exists():
            raise FileNotFoundError(f"Dataset not found: {data_file}")

        env = self._cc.build_env(config, wait_for_start=True)

        services = self._cc.compose_services(config)
        logger.info("[production] starting runtime services: %s", services)
        if services:
            self._runner.up(*services, force_recreate=True, env=env)

        logger.info("[production] creating producer container")
        self._runner.up("producer", force_recreate=True, env=env)

        logger.info("[production] waiting for producer readiness")
        signal_file = env.get("DASMA_PRODUCER_START_SIGNAL_FILE", _PRODUCER_START_SIGNAL)

        for _ in range(_PRODUCER_READY_RETRIES):
            try:
                self._runner.exec("producer", "sh", "-lc", f'touch "{signal_file}"')
                return self._cc.build_action_payload(config, "start_production")
            except ComposeError:
                time.sleep(_PRODUCER_READY_SLEEP)

        raise RuntimeError("producer_not_ready: producer container is not ready to receive the start signal")

    # ------------------------------------------------------------------
    # stop_campaign.sh
    # ------------------------------------------------------------------

    def stop_campaign(self) -> dict:
        """Stop and remove all runtime campaign containers."""
        services = list(self._cc.ALL_RUNTIME_COMPOSE_SERVICES)
        logger.info("[stop] stopping campaign containers: %s", services)
        self._runner.stop(*services)
        self._runner.rm(*services)
        return {"status": "ok", "action": "stop_campaign"}

    # ------------------------------------------------------------------
    # reset_campaign.sh
    # ------------------------------------------------------------------

    def reset_campaign(self) -> dict:
        """Reset Kafka topics, Schema Registry subjects and InfluxDB storage."""
        logger.info("[reset] ensuring reset prerequisites")
        self._runner.pull_missing(*_RESET_IMAGES)

        logger.info("[reset] ensuring kafka readiness")
        self._runner.up("kafka1", "kafka2", "kafka3", "schema-registry", wait=True, wait_timeout=240)

        topics = list(self._cc.RESET_TOPICS)
        logger.info("[reset] removing kafka topics: %s", topics)
        for topic in topics:
            try:
                self._runner.exec(
                    "kafka1", "bash", "-lc",
                    f"/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --delete --if-exists --topic '{topic}'",
                )
            except ComposeError as exc:
                logger.warning("[reset] topic delete failed (ignored): %s", exc)

        logger.info("[reset] recreating kafka topics")
        self._runner.up("init-topic", force_recreate=True)

        subjects = list(self._cc.RESET_SCHEMA_SUBJECTS)
        logger.info("[reset] removing schema registry subjects: %s", subjects)
        for subject in subjects:
            self._delete_schema_subject(subject)

        logger.info("[reset] resetting influxdb storage")
        self._runner.stop("influxdb", "influxdb-init-buckets", "influxdb-apply-template")
        self._runner.rm("influxdb", "influxdb-init-buckets", "influxdb-apply-template")
        self._drop_influxdb_volume()

        logger.info("[reset] recreating influxdb")
        self._runner.up("influxdb", wait=True, wait_timeout=240)

        logger.info("[reset] initializing influxdb buckets")
        self._runner.up("influxdb-init-buckets", force_recreate=True)

        logger.info("[reset] applying influxdb template")
        self._runner.up("influxdb-apply-template", force_recreate=True)

        return {
            "status": "ok",
            "action": "reset_campaign",
            "details": "topics recreated, influxdb volume reset, schema registry subjects removed",
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _delete_schema_subject(self, subject: str) -> None:
        base = self._schema_registry_url.rstrip("/")
        for url in (
            f"{base}/subjects/{subject}?permanent=true",
            f"{base}/subjects/{subject}",
        ):
            try:
                httpx.delete(url, timeout=5.0)
            except Exception as exc:
                logger.debug("[reset] schema subject delete %s: %s (ignored)", subject, exc)

    def _drop_influxdb_volume(self) -> None:
        import subprocess
        project_name = self.project_root.name
        volume = f"{project_name}_influxdb-data"
        subprocess.run(["docker", "volume", "rm", "-f", volume], capture_output=True)
