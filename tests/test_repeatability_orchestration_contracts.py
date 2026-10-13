"""Contracts for repeatability orchestration and reset wiring (step 6).

The repeatability services must be launchable (image resolution with the
campaign environment), stoppable (all-runtime list) and resettable (Kafka
topics and the two Avro schema subjects) — all statically, without starting
any Docker daemon, broker or registry.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest import TestCase
from unittest.mock import MagicMock, patch

from src.dasma_kafka.web.orchestration.campaign_orchestrator import (
    CampaignOrchestrator,
    _LAUNCH_IMAGES,
)
from src.dasma_kafka.web.orchestration.compose_runner import ComposeRunner
from scripts.web.campaign_config import (
    ALL_RUNTIME_COMPOSE_SERVICES,
    RESET_SCHEMA_SUBJECTS,
    RESET_TOPICS,
    REPEATABILITY_COMPOSE_SERVICES,
)

ROOT = Path(__file__).resolve().parents[1]
TRIO = ("repeatability-v1", "repeatability-v2", "repeatability-diff")
REPEATABILITY_TOPICS = (
    "repeatability-v1",
    "repeatability-v1-viz",
    "repeatability-v2",
    "repeatability-v2-viz",
    "repeatability-diff",
)
REPEATABILITY_SUBJECTS = ("repeatability-v1-value", "repeatability-v2-value")


class LaunchImagesContractTest(TestCase):
    def test_trio_in_launch_images_once(self) -> None:
        for service in TRIO:
            self.assertEqual(_LAUNCH_IMAGES.count(service), 1, service)

    def test_trio_in_launch_shell_script_once(self) -> None:
        script = (ROOT / "scripts" / "web" / "launch_campaign.sh").read_text(encoding="utf-8")
        # Join the backslash-continued pull_missing_images.sh invocation.
        block = script.split("pull_missing_images.sh", 1)[1].split("\n\n", 1)[0]
        invocation = block.replace("\\\n", " ")
        for service in TRIO:
            self.assertEqual(invocation.count(service), 1, service)
        # The legacy service list must be preserved, not reordered.
        for legacy in ("init-topic", "hstree-analysis", "explainability-v2", "consensus-analysis", "telegraf"):
            self.assertIn(legacy, invocation)

    def test_trio_in_all_runtime_services_once(self) -> None:
        for service in TRIO:
            self.assertEqual(ALL_RUNTIME_COMPOSE_SERVICES.count(service), 1, service)
        self.assertEqual(len(ALL_RUNTIME_COMPOSE_SERVICES), len(set(ALL_RUNTIME_COMPOSE_SERVICES)))
        self.assertEqual(REPEATABILITY_COMPOSE_SERVICES, TRIO)


class OrchestratorLaunchTest(TestCase):
    def _make_orchestrator(self, project_root: Path) -> CampaignOrchestrator:
        orchestrator = CampaignOrchestrator(
            project_root=project_root,
            scripts_web_dir=ROOT / "scripts" / "web",
        )
        orchestrator._runner = MagicMock(spec=ComposeRunner)
        return orchestrator

    def _write_iforest_repeatability_campaign(self, project_root: Path) -> Path:
        (project_root / "data").mkdir(parents=True, exist_ok=True)
        (project_root / "data" / "source.csv").write_text("col\n1\n", encoding="utf-8")
        config_path = project_root / "campaign.json"
        config_path.write_text(
            json.dumps({
                "dataset_path": "data/source.csv",
                "selected_models": ["iforest"],
                "models": {"iforest": {"repeatability": True}},
            }),
            encoding="utf-8",
        )
        return config_path

    def test_iforest_repeatability_launch_resolves_campaign_env(self) -> None:
        """launch_campaign must build_env() before pull_missing and reuse it."""
        with tempfile.TemporaryDirectory() as tmp:
            project_root = Path(tmp)
            config_path = self._write_iforest_repeatability_campaign(project_root)
            orchestrator = self._make_orchestrator(project_root)

            payload = orchestrator.launch_campaign(config_path)
            self.assertEqual(payload["status"], "ok")

            pull_call = orchestrator._runner.pull_missing.call_args
            args = pull_call.args
            env = pull_call.kwargs["env"]
            for service in TRIO:
                self.assertIn(service, args)
            self.assertEqual(env["DASMA_REPEATABILITY_ENABLED"], "1")
            self.assertEqual(env["REPEATABILITY_IMAGE"], "dasma-kafka-anomaly-sklearn:0.1.0")
            self.assertEqual(
                env["REPEATABILITY_SCRIPT"],
                "runtime/detectors/iforest/iforest_analysis.py",
            )

            # The very same env object must be reused for init-topic.
            up_call = orchestrator._runner.up.call_args
            self.assertEqual(up_call.args, ("init-topic",))
            self.assertTrue(up_call.kwargs["force_recreate"])
            self.assertIs(up_call.kwargs["env"], env)

    def test_stop_campaign_stops_and_removes_trio(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            orchestrator = self._make_orchestrator(Path(tmp))

            orchestrator.stop_campaign()

            stop_args = orchestrator._runner.stop.call_args.args
            rm_args = orchestrator._runner.rm.call_args.args
            for service in TRIO:
                self.assertIn(service, stop_args)
                self.assertIn(service, rm_args)
            self.assertEqual(len(stop_args), len(set(stop_args)))
            self.assertEqual(len(rm_args), len(set(rm_args)))


class ResetListsContractTest(TestCase):
    def test_reset_topics_contain_repeatability_once(self) -> None:
        for topic in REPEATABILITY_TOPICS:
            self.assertEqual(RESET_TOPICS.count(topic), 1, topic)

    def test_reset_schema_subjects_contain_only_avro_pair(self) -> None:
        for subject in REPEATABILITY_SUBJECTS:
            self.assertEqual(RESET_SCHEMA_SUBJECTS.count(subject), 1, subject)
        for absent in (
            "repeatability-diff-value",
            "repeatability-v1-viz-value",
            "repeatability-v2-viz-value",
        ):
            self.assertNotIn(absent, RESET_SCHEMA_SUBJECTS)


class HistoricalScriptsContractTest(TestCase):
    def test_stop_campaign_stays_helper_driven(self) -> None:
        script = (ROOT / "scripts" / "web" / "stop_campaign.sh").read_text(encoding="utf-8")
        self.assertIn("all-runtime-services", script)
        self.assertNotIn("repeatability", script)

    def test_reset_campaign_stays_helper_driven(self) -> None:
        script = (ROOT / "scripts" / "web" / "reset_campaign.sh").read_text(encoding="utf-8")
        self.assertIn("reset-topics", script)
        self.assertIn("reset-schema-subjects", script)
        self.assertNotIn("repeatability", script)
