"""Contracts for the reproducibility orchestration and UI wiring (P2-4).

The exporter service is wired to a normal campaign: Compose service, bilateral
build_env/Compose contract, campaign service lists, orchestration paths and the
reproducibility exclusivity rules in the interface. No broker, no container, no
real CSV.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
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
    build_env,
    compose_services,
    load_campaign_dict,
)

ROOT = Path(__file__).resolve().parents[1]
EXPORTER_SERVICE = "reproducibility-export"
MODELS = ("hstree", "iforest", "kitnet", "deepant", "mdragpoint")
NORMAL_SERVICES = {
    "hstree": "hstree-analysis",
    "iforest": "iforest-analysis",
    "kitnet": "kitnet-analysis",
    "deepant": "deepant-analysis",
    "mdragpoint": "mdragstream-point-analysis",
}
REPEATABILITY_SERVICES = ("repeatability-v1", "repeatability-v2", "repeatability-diff")


def _repro_config(model: str, role: str = "source-1", campaign_id: str = "campaign-42") -> dict:
    return load_campaign_dict({
        "dataset_path": "data/source.csv",
        "mode": "batch",
        "selected_models": [model],
        f"{model}_reproducibility": True,
        "reproducibility_source_role": role,
        "reproducibility_campaign_id": campaign_id,
    })


def _compose_service_section(service_name: str) -> str:
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    marker = f"  {service_name}:"
    section = compose.split(marker, 1)[1]
    next_service = re.search(r"\n  [A-Za-z0-9_-]+:", section)
    return section if next_service is None else section[: next_service.start()]


class ComposeServiceContractTest(TestCase):
    """Family 1: the exact Compose service."""

    def setUp(self) -> None:
        self.section = _compose_service_section(EXPORTER_SERVICE)

    def test_service_identity_and_runtime(self) -> None:
        self.assertIn('image: dasma-kafka-kafka-tools:0.1.0', self.section)
        self.assertIn(f"container_name: {EXPORTER_SERVICE}", self.section)
        self.assertIn('entrypoint: ["python", "/app/validation/reproducibility/export_topic_to_csv.py"]', self.section)
        self.assertIn('restart: "no"', self.section)
        self.assertIn("working_dir: /app", self.section)
        self.assertIn("networks: [kafka-net]", self.section)
        self.assertIn("init-topic:", self.section)
        self.assertIn("service_completed_successfully", self.section)

    def test_volumes_have_dedicated_writable_mount(self) -> None:
        # Do not mount a read-only /app parent above the writable output: Docker
        # Desktop exposes that nested bind as read-only at runtime. The
        # exporter needs only its own package, which remains read-only.
        self.assertIn(
            "- ./validation/reproducibility:/app/validation/reproducibility:ro",
            self.section,
        )
        self.assertNotIn("- ./:/app:ro", self.section)
        self.assertIn("- ./data/reproducibility:/app/data/reproducibility", self.section)
        # The dedicated mount must not be read-only.
        self.assertNotIn(
            "- ./data/reproducibility:/app/data/reproducibility:ro", self.section
        )

    def test_environment_has_the_twelve_variables_with_coherent_defaults(self) -> None:
        expected = (
            'BOOTSTRAP_SERVERS: "kafka1:9092,kafka2:9092,kafka3:9092"',
            'DASMA_ENABLED: "${DASMA_REPRODUCIBILITY_ENABLED:-0}"',
            'REPRODUCIBILITY_MODEL_ID: "${REPRODUCIBILITY_MODEL_ID:-}"',
            'REPRODUCIBILITY_SOURCE_ROLE: "${REPRODUCIBILITY_SOURCE_ROLE:-}"',
            'REPRODUCIBILITY_CAMPAIGN_ID: "${REPRODUCIBILITY_CAMPAIGN_ID:-}"',
            'REPRODUCIBILITY_SOURCE_TOPIC: "${REPRODUCIBILITY_SOURCE_TOPIC:-}"',
            'REPRODUCIBILITY_GROUP_ID: "${REPRODUCIBILITY_GROUP_ID:-}"',
            'REPRODUCIBILITY_OUTPUT_ROOT: "${REPRODUCIBILITY_OUTPUT_ROOT:-/app/data/reproducibility}"',
            'REPRODUCIBILITY_IDLE_EXIT_SEC: "${REPRODUCIBILITY_IDLE_EXIT_SEC:-15}"',
            'REPRODUCIBILITY_STARTUP_TIMEOUT_SEC: "${REPRODUCIBILITY_STARTUP_TIMEOUT_SEC:-300}"',
            'REPRODUCIBILITY_IMAGE: "${REPRODUCIBILITY_IMAGE:-}"',
            'REPRODUCIBILITY_CAMPAIGN_PARAMETERS_JSON: "${REPRODUCIBILITY_CAMPAIGN_PARAMETERS_JSON:-}"',
        )
        for line in expected:
            self.assertIn(line, self.section)

    def test_service_creates_no_topic_or_schema_resource(self) -> None:
        self.assertNotIn("TOPIC_NAME", self.section)
        self.assertNotIn("kafka-topics", self.section)


class BilateralRuntimeContractTest(TestCase):
    """Family 2: build_env emission and lossless Compose rendering."""

    def test_neutral_campaign_emits_eleven_neutral_variables(self) -> None:
        env = build_env(load_campaign_dict({
            "dataset_path": "data/source.csv",
            "selected_models": ["hstree", "iforest", "kitnet"],
        }))
        self.assertEqual(env["DASMA_REPRODUCIBILITY_ENABLED"], "0")
        self.assertEqual(env["REPRODUCIBILITY_MODEL_ID"], "")
        self.assertEqual(env["REPRODUCIBILITY_SOURCE_ROLE"], "")
        self.assertEqual(env["REPRODUCIBILITY_CAMPAIGN_ID"], "")
        self.assertEqual(env["REPRODUCIBILITY_SOURCE_TOPIC"], "")
        self.assertEqual(env["REPRODUCIBILITY_GROUP_ID"], "")
        self.assertEqual(env["REPRODUCIBILITY_OUTPUT_ROOT"], "/app/data/reproducibility")
        self.assertEqual(env["REPRODUCIBILITY_IDLE_EXIT_SEC"], "15")
        self.assertEqual(env["REPRODUCIBILITY_STARTUP_TIMEOUT_SEC"], "300")
        self.assertEqual(env["REPRODUCIBILITY_IMAGE"], "")
        self.assertEqual(env["REPRODUCIBILITY_CAMPAIGN_PARAMETERS_JSON"], "{}")

    def test_active_campaign_emits_exact_runtime_contract(self) -> None:
        env = build_env(_repro_config("hstree"))
        self.assertEqual(env["DASMA_REPRODUCIBILITY_ENABLED"], "1")
        self.assertEqual(env["REPRODUCIBILITY_MODEL_ID"], "hstree")
        self.assertEqual(env["REPRODUCIBILITY_SOURCE_ROLE"], "source-1")
        self.assertEqual(env["REPRODUCIBILITY_CAMPAIGN_ID"], "campaign-42")
        self.assertEqual(env["REPRODUCIBILITY_SOURCE_TOPIC"], "hstree-analysis-viz")
        self.assertEqual(
            env["REPRODUCIBILITY_GROUP_ID"], "reproducibility-export-campaign-42-source-1"
        )
        self.assertEqual(env["REPRODUCIBILITY_OUTPUT_ROOT"], "/app/data/reproducibility")
        self.assertEqual(env["REPRODUCIBILITY_IDLE_EXIT_SEC"], "15")
        self.assertEqual(env["REPRODUCIBILITY_STARTUP_TIMEOUT_SEC"], "300")
        self.assertEqual(env["REPRODUCIBILITY_IMAGE"], "dasma-kafka-anomaly-light:0.1.0")

        parameters = json.loads(env["REPRODUCIBILITY_CAMPAIGN_PARAMETERS_JSON"])
        self.assertEqual(parameters["dataset_path"], "data/source.csv")
        self.assertEqual(parameters["mode"], "batch")
        self.assertEqual(parameters["selected_groups"], [])
        self.assertEqual(parameters["excluded_fields"], [])
        model_parameters = parameters["model_parameters"]
        self.assertIn("hstree_num_trees", model_parameters)
        self.assertEqual(model_parameters["hstree_num_trees"], 25)
        self.assertIsInstance(model_parameters["hstree_anomaly_k_sigma"], float)
        self.assertNotIn("hstree_repeatability", model_parameters)
        self.assertNotIn("hstree_reproducibility", model_parameters)
        for key in model_parameters:
            self.assertTrue(key.startswith("hstree_"), key)
        json.dumps(parameters, allow_nan=False)

    def test_topic_table_mirrors_the_p2_3_exporter(self) -> None:
        from validation.reproducibility.export_topic_to_csv import MODEL_VIZ_TOPICS

        from scripts.web.campaign_config import REPRODUCIBILITY_VIZ_TOPICS

        self.assertEqual(REPRODUCIBILITY_VIZ_TOPICS, MODEL_VIZ_TOPICS)

    def test_compose_config_renders_the_twelve_variables_losslessly(self) -> None:
        if shutil.which("docker") is None:
            self.skipTest("docker CLI is not available")
        env = build_env(_repro_config("hstree"))
        env["REPRODUCIBILITY_CAMPAIGN_PARAMETERS_JSON"] = json.dumps(
            {
                "dataset_path": "data/essai-café.csv",
                "mode": "batch",
                "selected_groups": ["G1"],
                "excluded_fields": [],
                "model_parameters": {"hstree_num_trees": 75},
            },
            ensure_ascii=False,
            sort_keys=True,
            allow_nan=False,
        )
        env["REPRODUCIBILITY_MODEL_ID"] = "hstree"
        env["REPRODUCIBILITY_SOURCE_ROLE"] = "source-2"
        env["REPRODUCIBILITY_CAMPAIGN_ID"] = "campaign-42"
        env["REPRODUCIBILITY_SOURCE_TOPIC"] = "hstree-analysis-viz"
        env["REPRODUCIBILITY_GROUP_ID"] = "reproducibility-export-campaign-42-source-2"
        env["REPRODUCIBILITY_IMAGE"] = "dasma-kafka-anomaly-light:0.1.0"
        env["DASMA_REPRODUCIBILITY_ENABLED"] = "1"

        result = subprocess.run(
            ["docker", "compose", "-f", str(ROOT / "docker-compose.yml"),
             "--project-directory", str(ROOT), "config", "--format", "json",
             EXPORTER_SERVICE],
            capture_output=True, text=True, env={**os.environ, **env}, check=True,
        )
        rendered = json.loads(result.stdout)["services"][EXPORTER_SERVICE]["environment"]
        self.assertEqual(rendered["DASMA_ENABLED"], "1")
        self.assertEqual(rendered["REPRODUCIBILITY_MODEL_ID"], "hstree")
        self.assertEqual(rendered["REPRODUCIBILITY_SOURCE_ROLE"], "source-2")
        self.assertEqual(rendered["REPRODUCIBILITY_CAMPAIGN_ID"], "campaign-42")
        self.assertEqual(rendered["REPRODUCIBILITY_SOURCE_TOPIC"], "hstree-analysis-viz")
        self.assertEqual(
            rendered["REPRODUCIBILITY_GROUP_ID"], "reproducibility-export-campaign-42-source-2"
        )
        self.assertEqual(rendered["REPRODUCIBILITY_OUTPUT_ROOT"], "/app/data/reproducibility")
        self.assertEqual(rendered["REPRODUCIBILITY_IDLE_EXIT_SEC"], "15")
        self.assertEqual(rendered["REPRODUCIBILITY_STARTUP_TIMEOUT_SEC"], "300")
        self.assertEqual(rendered["REPRODUCIBILITY_IMAGE"], "dasma-kafka-anomaly-light:0.1.0")
        # Unicode JSON with quotes survives the compose interpolation.
        parameters = json.loads(rendered["REPRODUCIBILITY_CAMPAIGN_PARAMETERS_JSON"])
        self.assertEqual(parameters["dataset_path"], "data/essai-café.csv")
        self.assertEqual(parameters["model_parameters"]["hstree_num_trees"], 75)


class CampaignNormalityTest(TestCase):
    """Family 3: reproducibility keeps the normal model running."""

    def test_each_model_keeps_its_normal_service_active(self) -> None:
        for model in MODELS:
            config = _repro_config(model)
            env = build_env(config)
            services = compose_services(config)

            self.assertEqual(env["DASMA_REPRODUCIBILITY_ENABLED"], "1", model)
            self.assertEqual(env[f"DASMA_{model.upper()}_ENABLED"], "1", model)
            self.assertEqual(env["DASMA_CONSENSUS_ENABLED"], "1", model)
            self.assertEqual(env["DASMA_REPEATABILITY_ENABLED"], "0", model)
            self.assertEqual(
                services,
                [
                    NORMAL_SERVICES[model],
                    EXPORTER_SERVICE,
                    "consensus-analysis",
                    "build-metrics",
                    "telegraf",
                ],
                model,
            )
            for repeatability_service in REPEATABILITY_SERVICES:
                self.assertNotIn(repeatability_service, services, model)

    def test_ordinary_campaign_services_unchanged(self) -> None:
        config = load_campaign_dict({
            "dataset_path": "data/source.csv",
            "selected_models": ["hstree", "iforest", "kitnet"],
        })
        self.assertEqual(
            compose_services(config),
            [
                "hstree-analysis",
                "iforest-analysis",
                "kitnet-analysis",
                "consensus-analysis",
                "build-metrics",
                "telegraf",
            ],
        )


class OrchestrationWiringTest(TestCase):
    """Family 4: images, runtime lists, launch/stop paths, helper-driven scripts."""

    def test_exporter_listed_once_everywhere(self) -> None:
        self.assertEqual(_LAUNCH_IMAGES.count(EXPORTER_SERVICE), 1)
        self.assertEqual(ALL_RUNTIME_COMPOSE_SERVICES.count(EXPORTER_SERVICE), 1)
        self.assertEqual(
            len(ALL_RUNTIME_COMPOSE_SERVICES), len(set(ALL_RUNTIME_COMPOSE_SERVICES))
        )
        script = (ROOT / "scripts" / "web" / "launch_campaign.sh").read_text(encoding="utf-8")
        block = script.split("pull_missing_images.sh", 1)[1].split("\n\n", 1)[0]
        invocation = block.replace("\\\n", " ")
        self.assertEqual(invocation.count(EXPORTER_SERVICE), 1)

    def test_stop_campaign_covers_exporter_without_duplicates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            orchestrator = CampaignOrchestrator(
                project_root=Path(tmp_str),
                scripts_web_dir=ROOT / "scripts" / "web",
            )
            orchestrator._runner = MagicMock(spec=ComposeRunner)

            orchestrator.stop_campaign()

            stop_args = orchestrator._runner.stop.call_args.args
            rm_args = orchestrator._runner.rm.call_args.args
            self.assertIn(EXPORTER_SERVICE, stop_args)
            self.assertIn(EXPORTER_SERVICE, rm_args)
            self.assertEqual(len(stop_args), len(set(stop_args)))

    def test_launch_campaign_passes_campaign_env_for_reproducibility(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            project_root = Path(tmp_str)
            (project_root / "data").mkdir()
            (project_root / "data" / "source.csv").write_text("col\n1\n", encoding="utf-8")
            config_path = project_root / "campaign.json"
            config_path.write_text(
                json.dumps({
                    "dataset_path": "data/source.csv",
                    "selected_models": ["iforest"],
                    "models": {"iforest": {"reproducibility": True}},
                    "reproducibility": {
                        "source_role": "source-2",
                        "campaign_id": "essai-cta",
                    },
                }),
                encoding="utf-8",
            )
            orchestrator = CampaignOrchestrator(
                project_root=project_root,
                scripts_web_dir=ROOT / "scripts" / "web",
            )
            orchestrator._runner = MagicMock(spec=ComposeRunner)

            orchestrator.launch_campaign(config_path)

            pull_call = orchestrator._runner.pull_missing.call_args
            env = pull_call.kwargs["env"]
            self.assertIn(EXPORTER_SERVICE, pull_call.args)
            self.assertEqual(env["DASMA_REPRODUCIBILITY_ENABLED"], "1")
            self.assertEqual(env["REPRODUCIBILITY_MODEL_ID"], "iforest")
            self.assertEqual(env["REPRODUCIBILITY_SOURCE_ROLE"], "source-2")
            self.assertEqual(env["REPRODUCIBILITY_CAMPAIGN_ID"], "essai-cta")
            self.assertEqual(env["REPRODUCIBILITY_SOURCE_TOPIC"], "iforest-analysis-viz")
            up_call = orchestrator._runner.up.call_args
            self.assertIs(up_call.kwargs["env"], env)

    def test_stop_and_reset_scripts_stay_helper_driven(self) -> None:
        stop = (ROOT / "scripts" / "web" / "stop_campaign.sh").read_text(encoding="utf-8")
        reset = (ROOT / "scripts" / "web" / "reset_campaign.sh").read_text(encoding="utf-8")
        self.assertIn("all-runtime-services", stop)
        self.assertNotIn("reproducibility", stop)
        self.assertIn("reset-topics", reset)
        self.assertIn("reset-schema-subjects", reset)
        self.assertNotIn("reproducibility", reset)
        self.assertNotIn("data/reproducibility", reset)


class ResetPreservationTest(TestCase):
    """Family 5: reset never deletes the reproducibility evidence."""

    def test_reset_lists_have_no_reproducibility_resource(self) -> None:
        for topic in RESET_TOPICS:
            self.assertNotIn("reproducibility", topic)
        for subject in RESET_SCHEMA_SUBJECTS:
            self.assertNotIn("reproducibility", subject)

    def test_reset_campaign_script_never_touches_data_repeatability(self) -> None:
        script = (ROOT / "scripts" / "web" / "reset_campaign.sh").read_text(encoding="utf-8")
        self.assertNotIn("repeatability", script)


class ReproducibilityUiContractTest(TestCase):
    """Family 6: interface exclusivity rules, statically asserted."""

    def setUp(self) -> None:
        self.js = (ROOT / "frontend" / "askmydasma" / "app-user.js").read_text(encoding="utf-8")

    def _helper(self, name: str) -> str:
        return self.js.split(f"function {name}(", 1)[1].split("\n}", 1)[0]

    def test_helpers_exist_and_cover_both_families(self) -> None:
        self.assertIn("function reproducibilityBoxes()", self.js)
        self.assertIn("function enforceSingleReproducibility(", self.js)
        self.assertIn("function enforceReproducibilityExclusivity(", self.js)
        helpers = self.js.split("function modelSelectorBoxes()", 1)[1].split(
            "function updateInteractiveState()", 1
        )[0]
        for reference in (
            "els.reproducibilityHstree",
            "els.reproducibilityMdragpoint",
            "els.modelHstree",
            "els.modelIforest",
            "els.modelKitnet",
            "els.modelDeepant",
            "els.modelExplainability",
            "els.modelExplainabilityV2",
        ):
            self.assertIn(reference, helpers)

    def test_repro_exclusivity_unchecks_repeat_and_other_selectors(self) -> None:
        helper = self._helper("enforceReproducibilityExclusivity")
        self.assertIn("if (entry.box) entry.box.checked = false;", helper)
        self.assertIn("if (entry.modelBox) entry.modelBox.checked = false;", helper)
        self.assertIn("if (entry.modelBox) entry.modelBox.checked = true;", helper)
        # Every repeatability flag is unchecked by a reproducibility campaign.
        self.assertIn("for (const entry of repeatabilityBoxes())", helper)
        self.assertIn("if (entry.box) entry.box.checked = false;", helper)

    def test_repeat_exclusivity_unchecks_reproducibility_flags(self) -> None:
        helper = self._helper("enforceRepeatabilityExclusivity")
        self.assertIn("for (const entry of reproducibilityBoxes())", helper)
        self.assertIn("if (entry.box) entry.box.checked = false;", helper)

    def test_availability_freezes_selectors_and_role_in_dedicated_modes(self) -> None:
        state_block = self.js.split("function updateInteractiveState()", 1)[1].split("\n}", 1)[0]
        self.assertIn(
            "entry.box.disabled = locked || activeReproducibility !== null || (!modelChecked && entry.model !== activeRepeatability)",
            state_block,
        )
        self.assertIn(
            "entry.box.disabled = locked || activeRepeatability !== null || (!modelChecked && entry.model !== activeReproducibility)",
            state_block,
        )
        self.assertIn(
            "entry.box.disabled = locked || activeRepeatability !== null || activeReproducibility !== null",
            state_block,
        )
        self.assertIn("els.reproducibilitySourceRole.disabled = locked || activeReproducibility === null;", state_block)
        self.assertIn("els.reproducibilityCampaignId.disabled = locked || activeReproducibility === null;", state_block)

    def test_repro_listener_normalizes_and_sync_reapplies_state(self) -> None:
        self.assertIn("enforceReproducibilityExclusivity(entry.model)", self.js)
        sync_block = self.js.split("function syncFormFromCampaign", 1)[1].split("\n}", 1)[0]
        self.assertIn("!reproducibilityDirty", sync_block)
        self.assertIn("updateInteractiveState()", sync_block)

    def test_ordinary_campaign_visuals_unchanged(self) -> None:
        state_block = self.js.split("function updateInteractiveState()", 1)[1].split("\n}", 1)[0]
        # No reproducibility mode: repeatability and model availability keep the
        # historical rules (locked / model-checked), nothing is force-disabled.
        self.assertIn(
            "entry.box.disabled = locked || activeReproducibility !== null || (!modelChecked && entry.model !== activeRepeatability)",
            state_block,
        )
        self.assertIn(
            "entry.box.disabled = locked || activeRepeatability !== null || (!modelChecked && entry.model !== activeReproducibility)",
            state_block,
        )

    def test_wrapper_and_settings_visibility_follow_the_global_mode(self) -> None:
        state_block = self.js.split("function updateInteractiveState()", 1)[1].split("\n}", 1)[0]
        self.assertIn('const wrapperHidden = validationMode !== "reproducibility" || !modelChecked;', state_block)
        self.assertIn("entry.wrapper.hidden = wrapperHidden;", state_block)
        self.assertIn('const settingsHidden = validationMode !== "reproducibility";', state_block)
        self.assertIn("els.reproducibilitySettings.hidden = settingsHidden;", state_block)

    def test_sync_deduces_the_global_mode_without_overriding_dirty_state(self) -> None:
        sync_block = self.js.split("function syncFormFromCampaign", 1)[1].split("\n}", 1)[0]
        self.assertIn("!validationModeDirty", sync_block)
        self.assertIn("!reproducibilityDirty &&", sync_block)
        self.assertIn("Boolean(campaign.reproducibility_source_role)", sync_block)
        self.assertIn("Boolean(campaign.reproducibility_campaign_id)", sync_block)
        # The deduction runs before the flag restoration blocks.
        self.assertLess(
            sync_block.index("applyValidationModeBoxes("),
            sync_block.index("reproducibilityBindings"),
        )

    def test_global_mode_checkboxes_are_frozen_while_campaign_runs(self) -> None:
        state_block = self.js.split("function updateInteractiveState()", 1)[1].split("\n}", 1)[0]
        self.assertIn("els.validationModeRepeatability.disabled = locked;", state_block)
        self.assertIn("els.validationModeReproducibility.disabled = locked;", state_block)
