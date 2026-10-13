"""Unit tests for the AskMyDasma campaign service."""

from __future__ import annotations

import json
import re
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from fastapi import HTTPException

from src.dasma_kafka.web.campaign_state import CampaignState
from src.dasma_kafka.web.dataset_presets import DEFAULT_EXCLUDED_FIELDS
from src.dasma_kafka.web.models import (
    CampaignLaunchRequest,
    CampaignView,
    ExplainabilityV1ProgressView,
    ExplainabilityV2ProgressView,
    ServiceStatusView,
)
from src.dasma_kafka.web.orchestration.campaign_orchestrator import CampaignOrchestrator
from src.dasma_kafka.web.services.campaign_service import CampaignService
from src.dasma_kafka.web.services.dataset_service import DatasetService
from src.dasma_kafka.web.services.docker_service import DockerService
from src.dasma_kafka.web.services.grafana_service import GrafanaService
from src.dasma_kafka.web.services.progress_service import ProgressService
from src.dasma_kafka.web.settings import WebSettings


class CampaignServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        project_root = Path("/tmp/askmydasma")
        # web_scripts_dir points at the real scripts/web so the service can
        # load campaign_config.py and validate campaigns before persistence.
        real_scripts_web = Path(__file__).resolve().parents[1] / "scripts" / "web"
        self.settings = WebSettings(
            project_root=project_root,
            scripts_dir=project_root / "scripts",
            web_scripts_dir=real_scripts_web,
            data_dir=project_root / "data",
            tmp_dir=project_root / ".tmp",
            host_project_root=project_root,
            frontend_dir=project_root / "frontend" / "askmydasma",
        )
        self.state = CampaignState()
        self.dataset_service = DatasetService(self.settings)
        self.docker_service = DockerService(self.settings)
        self.grafana_service = GrafanaService(self.settings)
        self.progress_service = ProgressService(self.settings)

        self.mock_orchestrator = MagicMock(spec=CampaignOrchestrator)
        self.mock_orchestrator.launch_campaign.return_value = {"status": "ok", "action": "launch_campaign"}
        self.mock_orchestrator.start_production.return_value = {"status": "ok", "action": "start_production"}
        self.mock_orchestrator.stop_campaign.return_value = {"status": "ok", "action": "stop_campaign"}
        self.mock_orchestrator.reset_campaign.return_value = {
            "status": "ok",
            "action": "reset_campaign",
            "details": "topics recreated and influxdb buckets cleared",
        }

        self.service = CampaignService(
            state=self.state,
            settings=self.settings,
            dataset_service=self.dataset_service,
            docker_service=self.docker_service,
            grafana_service=self.grafana_service,
            progress_service=self.progress_service,
            orchestrator=self.mock_orchestrator,
        )

    def _config_path(self) -> Path:
        return self.settings.tmp_dir / "current-campaign.json"

    def _assert_config_written(self) -> dict:
        """Read and return the persisted campaign config, asserting it exists."""
        config_path = self._config_path()
        self.assertTrue(config_path.exists(), "campaign config was not written")
        return json.loads(config_path.read_text(encoding="utf-8"))

    def _write_campaign_config(self, payload: dict) -> None:
        config_path = self._config_path()
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(json.dumps(payload), encoding="utf-8")

    # ------------------------------------------------------------------
    # restore
    # ------------------------------------------------------------------

    @patch.object(DockerService, "get_services_status")
    def test_restore_from_campaign_config_restores_configuring_campaign(self, get_services_status) -> None:
        get_services_status.return_value = []
        self._write_campaign_config(
            {
                "dataset_path": "data/source-1000.csv",
                "mode": "batch",
                "selected_groups": ["1A"],
                "excluded_fields": ["timestamp"],
                "selected_models": ["hstree"],
            }
        )

        campaign = self.service.restore_from_campaign_config()

        self.assertIsNotNone(campaign)
        self.assertEqual(campaign.dataset_path, "data/source-1000.csv")
        self.assertEqual(campaign.status, "configuring")

    @patch.object(DockerService, "get_services_status")
    def test_restore_from_campaign_config_marks_campaign_running_when_producer_runs(self, get_services_status) -> None:
        get_services_status.return_value = [
            ServiceStatusView(
                name="producer",
                container_name="producer",
                state="running",
                health="healthy",
                status="healthy",
            )
        ]
        self._write_campaign_config(
            {
                "dataset_path": "data/source-1000.csv",
                "mode": "batch",
                "selected_groups": [],
                "excluded_fields": [],
                "selected_models": ["hstree"],
            }
        )

        campaign = self.service.restore_from_campaign_config()

        self.assertIsNotNone(campaign)
        self.assertEqual(campaign.status, "running")

    @patch.object(DockerService, "get_services_status")
    def test_restore_detects_selected_runtime_after_producer_finishes(self, get_services_status) -> None:
        self._write_campaign_config({
            "dataset_path": "data/source-1000.csv",
            "selected_models": ["hstree"],
        })
        for service_name, expected_status in (("hstree-analysis", "running"), ("iforest-analysis", "configuring")):
            with self.subTest(service=service_name):
                self.service.state = CampaignState()
                get_services_status.return_value = [
                    ServiceStatusView(name="producer", state="exited", status="stopped"),
                    ServiceStatusView(name=service_name, state="running", status="running"),
                ]
                campaign = self.service.restore_from_campaign_config()
                self.assertEqual(campaign.status, expected_status)
                self.assertEqual(self.service.state.get_platform_status(),
                                 "processing_data" if expected_status == "running" else "ready_for_production")

    @patch.object(DockerService, "get_services_status")
    def test_restore_from_campaign_config_ignores_invalid_json(self, get_services_status) -> None:
        get_services_status.return_value = []
        config_path = self._config_path()
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text("not valid json", encoding="utf-8")

        campaign = self.service.restore_from_campaign_config()

        self.assertIsNone(campaign)
        get_services_status.assert_not_called()

    @patch.object(DockerService, "get_services_status", return_value=[])
    @patch.object(DatasetService, "ensure_dataset_exists")
    def test_relaunch_rejects_legacy_hstree_window_mismatch_without_rewriting_config(
        self, ensure_dataset_exists, get_services_status
    ) -> None:
        legacy_config = {
            "dataset_path": "data/source-1000.csv",
            "mode": "batch",
            "selected_models": ["hstree"],
            "hstree_initial_window_size": 128,
            "hstree_window_size": 2048,
        }
        self._write_campaign_config(legacy_config)
        restored = self.service.restore_from_campaign_config()
        self.assertIsNotNone(restored)
        self.assertEqual(restored.hstree_initial_window_size, 128)
        self.assertEqual(restored.hstree_window_size, 2048)

        expected_message = (
            "hstree: la fenêtre d'amorçage (128) doit être égale à la fenêtre du modèle "
            "(2048) : fenêtre de référence du papier"
        )
        with self.assertRaisesRegex(ValueError, re.escape(expected_message)):
            self.service.relaunch_campaign()

        ensure_dataset_exists.assert_called_once_with("data/source-1000.csv")
        get_services_status.assert_called_once()
        self.mock_orchestrator.launch_campaign.assert_not_called()
        self.assertEqual(self._assert_config_written(), legacy_config)

    @patch.object(DockerService, "get_services_status", return_value=[])
    @patch.object(DatasetService, "ensure_dataset_exists")
    def test_relaunch_rejects_legacy_iforest_parameters_without_rewriting_config(
        self, ensure_dataset_exists, get_services_status
    ) -> None:
        legacy_config = {
            "dataset_path": "data/source-1000.csv",
            "mode": "batch",
            "selected_models": ["iforest"],
            "iforest_initial_window_size": 128,
            "iforest_window_size": 2048,
            "iforest_max_samples": "512",
        }
        self._write_campaign_config(legacy_config)
        restored = self.service.restore_from_campaign_config()
        self.assertIsNotNone(restored)
        expected_message = (
            "iforest: la fenêtre d'amorçage (128) doit être au moins égale à la taille de sous-échantillon ψ (512) : "
            "sinon scikit-learn réduit ψ et le sous-échantillonnage du papier disparaît"
        )
        with self.assertRaisesRegex(ValueError, re.escape(expected_message)):
            self.service.relaunch_campaign()

        ensure_dataset_exists.assert_called_once_with("data/source-1000.csv")
        get_services_status.assert_called_once()
        self.mock_orchestrator.launch_campaign.assert_not_called()
        self.assertEqual(self._assert_config_written(), legacy_config)

    @patch.object(DockerService, "get_services_status")
    @patch.object(DatasetService, "ensure_dataset_exists")
    def test_repeatability_flag_survives_launch_view_persist_and_restore(
        self, ensure_dataset_exists, get_services_status
    ) -> None:
        """Launch → view → persisted JSON → restore keeps the repeatability flag."""
        get_services_status.return_value = []
        request = CampaignLaunchRequest(
            dataset_path="data/source-1000.csv",
            mode="batch",
            selected_models=["hstree"],
            hstree_repeatability=True,
        )

        campaign = self.service.launch_campaign(request)
        self.assertIs(campaign.hstree_repeatability, True)
        self.assertIs(campaign.iforest_repeatability, False)

        config = self._assert_config_written()
        self.assertIs(config["models"]["hstree"]["repeatability"], True)

        # Simulate a backend restart: fresh state, restore from the JSON.
        self.service.state.clear()
        restored = self.service.restore_from_campaign_config()
        self.assertIsNotNone(restored)
        self.assertIs(restored.hstree_repeatability, True)
        self.assertIs(restored.iforest_repeatability, False)
        self.assertIs(restored.mdragpoint_repeatability, False)

    @patch.object(DatasetService, "ensure_dataset_exists")
    def test_launch_campaign_persists_all_repeatability_flags(self, ensure_dataset_exists) -> None:
        request = CampaignLaunchRequest(
            dataset_path="data/source-1000.csv",
            mode="batch",
            selected_models=["kitnet"],
            kitnet_repeatability=True,
        )

        campaign = self.service.launch_campaign(request)

        config = self._assert_config_written()
        self.assertIs(config["models"]["kitnet"]["repeatability"], True)
        self.assertIs(config["models"]["hstree"]["repeatability"], False)
        self.assertIs(config["models"]["mdragpoint"]["repeatability"], False)
        self.assertIs(campaign.kitnet_repeatability, True)

    @patch.object(DatasetService, "ensure_dataset_exists")
    def test_launch_campaign_rejects_inconsistent_repeatability_before_persistence(
        self, ensure_dataset_exists
    ) -> None:
        """An API request bypassing the UI must fail before persistence/launch."""
        config_path = self._config_path()
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.unlink(missing_ok=True)

        request = CampaignLaunchRequest(
            dataset_path="data/source-1000.csv",
            mode="batch",
            selected_models=["hstree", "iforest"],
            hstree_repeatability=True,
        )

        with self.assertRaises(ValueError) as ctx:
            self.service.launch_campaign(request)

        self.assertIn("exclusive", str(ctx.exception))
        self.assertFalse(self._config_path().exists(), "no campaign JSON must be persisted")
        self.mock_orchestrator.launch_campaign.assert_not_called()
        self.assertEqual(self.state.get_platform_status(), "error")

    @patch.object(DatasetService, "ensure_dataset_exists")
    def test_reproducibility_campaign_survives_launch_persist_restore_relaunch(
        self, ensure_dataset_exists
    ) -> None:
        """P2-1 path: form → request → structured JSON → view → restore → relaunch."""
        request = CampaignLaunchRequest(
            dataset_path="data/source-1000.csv",
            mode="batch",
            selected_models=["iforest"],
            iforest_reproducibility=True,
            reproducibility_source_role="source-2",
            reproducibility_campaign_id="essai-cta",
        )

        campaign = self.service.launch_campaign(request)
        self.assertTrue(campaign.iforest_reproducibility)
        self.assertEqual(campaign.reproducibility_source_role, "source-2")
        self.assertEqual(campaign.reproducibility_campaign_id, "essai-cta")

        config = self._assert_config_written()
        self.assertIs(config["models"]["iforest"]["reproducibility"], True)
        self.assertEqual(
            config["reproducibility"],
            {"source_role": "source-2", "campaign_id": "essai-cta"},
        )

        # Simulated backend restart: fresh state, restore from the JSON.
        self.service.state.clear()
        restored = self.service.restore_from_campaign_config()
        self.assertIsNotNone(restored)
        self.assertTrue(restored.iforest_reproducibility)
        self.assertEqual(restored.reproducibility_source_role, "source-2")
        self.assertEqual(restored.reproducibility_campaign_id, "essai-cta")

        # Relaunch keeps flag, role and id through the rebuilt request and env.
        self.service.relaunch_campaign()
        relaunched_config = json.loads(self._config_path().read_text(encoding="utf-8"))
        self.assertIs(relaunched_config["models"]["iforest"]["reproducibility"], True)
        self.assertEqual(relaunched_config["reproducibility"]["source_role"], "source-2")
        self.assertEqual(relaunched_config["reproducibility"]["campaign_id"], "essai-cta")

    @patch.object(DatasetService, "ensure_dataset_exists")
    def test_launch_campaign_rejects_invalid_reproducibility_before_persistence(
        self, ensure_dataset_exists
    ) -> None:
        config_path = self._config_path()
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.unlink(missing_ok=True)

        request = CampaignLaunchRequest(
            dataset_path="data/source-1000.csv",
            mode="batch",
            selected_models=["hstree"],
            hstree_reproducibility=True,
            reproducibility_source_role="",
        )

        with self.assertRaises(ValueError) as ctx:
            self.service.launch_campaign(request)

        self.assertIn("source role", str(ctx.exception))
        self.assertFalse(config_path.exists(), "no campaign JSON must be persisted")
        self.mock_orchestrator.launch_campaign.assert_not_called()

    # ------------------------------------------------------------------
    # launch_campaign
    # ------------------------------------------------------------------

    @patch.object(DatasetService, "ensure_dataset_exists")
    def test_launch_campaign_updates_in_memory_state(self, ensure_dataset_exists) -> None:
        request = CampaignLaunchRequest(dataset_path="data/source-1000.csv", mode="batch")

        campaign = self.service.launch_campaign(request)

        ensure_dataset_exists.assert_called_once_with("data/source-1000.csv")
        self.mock_orchestrator.launch_campaign.assert_called_once_with(self._config_path())
        self.assertEqual(campaign.status, "configuring")
        self.assertEqual(campaign.dataset_path, "data/source-1000.csv")
        self.assertIsNotNone(self.state.get())
        self.assertIsNone(campaign.started_at)
        self.assertEqual(self.state.get_platform_status(), "ready_for_production")

    @patch.object(DatasetService, "ensure_dataset_exists")
    def test_launch_campaign_supports_realtime_x5_mode(self, ensure_dataset_exists) -> None:
        request = CampaignLaunchRequest(dataset_path="data/source-1000.csv", mode="realtime-x5")

        campaign = self.service.launch_campaign(request)

        ensure_dataset_exists.assert_called_once_with("data/source-1000.csv")
        self.assertEqual(campaign.mode, "realtime-x5")
        self.assertEqual(campaign.status, "configuring")

    @patch.object(DatasetService, "ensure_dataset_exists")
    def test_launch_campaign_supports_realtime_x20_mode(self, ensure_dataset_exists) -> None:
        request = CampaignLaunchRequest(dataset_path="data/source-1000.csv", mode="realtime-x20")

        campaign = self.service.launch_campaign(request)

        ensure_dataset_exists.assert_called_once_with("data/source-1000.csv")
        self.assertEqual(campaign.mode, "realtime-x20")
        self.assertEqual(campaign.status, "configuring")

    @patch.object(DatasetService, "ensure_dataset_exists")
    def test_launch_campaign_persists_analysis_threshold_parameters(self, ensure_dataset_exists) -> None:
        request = CampaignLaunchRequest(
            dataset_path="data/source-1000.csv",
            mode="batch",
            iforest_anomaly_k_sigma=2.5,
            iforest_anomaly_warmup_window=80,
            kitnet_estimator="p99",
            kitnet_threshold_window=900,
            kitnet_threshold_percentile=98.5,
            kitnet_anomaly_k_sigma=4.0,
            kitnet_anomaly_warmup_window=120,
        )

        campaign = self.service.launch_campaign(request)

        ensure_dataset_exists.assert_called_once_with("data/source-1000.csv")
        config = self._assert_config_written()
        self.assertEqual(config["dataset_path"], "data/source-1000.csv")
        self.assertEqual(config["mode"], "batch")
        self.assertEqual(config["selected_models"], ["hstree", "iforest", "kitnet"])
        self.assertEqual(config["models"]["iforest"]["anomaly_k_sigma"], 2.5)
        self.assertEqual(config["models"]["iforest"]["anomaly_warmup_window"], 80)
        self.assertEqual(config["models"]["kitnet"]["estimator"], "p99")
        self.assertEqual(config["models"]["kitnet"]["threshold_window"], 900)
        self.assertEqual(config["models"]["kitnet"]["threshold_percentile"], 98.5)
        self.assertEqual(config["models"]["kitnet"]["anomaly_k_sigma"], 4.0)
        self.assertEqual(config["models"]["kitnet"]["anomaly_warmup_window"], 120)
        self.assertEqual(campaign.iforest_anomaly_k_sigma, 2.5)
        self.assertEqual(campaign.iforest_anomaly_warmup_window, 80)
        self.assertEqual(campaign.kitnet_estimator, "p99")
        self.assertEqual(campaign.kitnet_threshold_window, 900)
        self.assertEqual(campaign.kitnet_threshold_percentile, 98.5)
        self.assertEqual(campaign.kitnet_anomaly_k_sigma, 4.0)
        self.assertEqual(campaign.kitnet_anomaly_warmup_window, 120)

    @patch.object(DatasetService, "ensure_dataset_exists")
    def test_launch_campaign_persists_deepant_parameters(self, ensure_dataset_exists) -> None:
        request = CampaignLaunchRequest(
            dataset_path="data/source-1000.csv",
            mode="batch",
            selected_models=["hstree", "iforest", "kitnet", "deepant"],
            deepant_estimator="welford",
            deepant_threshold_window=750,
            deepant_anomaly_k_sigma=2.0,
            deepant_anomaly_warmup_window=200,
            deepant_lookback_size=50,
            deepant_initial_window_size=256,
            deepant_epochs_initial=20,
        )

        campaign = self.service.launch_campaign(request)

        ensure_dataset_exists.assert_called_once_with("data/source-1000.csv")
        config = self._assert_config_written()
        self.assertEqual(config["selected_models"], ["hstree", "iforest", "kitnet", "deepant"])
        self.assertEqual(config["models"]["deepant"]["estimator"], "welford")
        self.assertEqual(config["models"]["deepant"]["threshold_window"], 750)
        self.assertEqual(config["models"]["deepant"]["anomaly_k_sigma"], 2.0)
        self.assertEqual(config["models"]["deepant"]["anomaly_warmup_window"], 200)
        self.assertEqual(config["models"]["deepant"]["lookback_size"], 50)
        self.assertEqual(config["models"]["deepant"]["initial_window_size"], 256)
        self.assertEqual(config["models"]["deepant"]["epochs_initial"], 20)
        self.assertEqual(campaign.deepant_estimator, "welford")
        self.assertEqual(campaign.deepant_threshold_window, 750)
        self.assertEqual(campaign.deepant_anomaly_k_sigma, 2.0)
        self.assertEqual(campaign.deepant_anomaly_warmup_window, 200)
        self.assertEqual(campaign.deepant_lookback_size, 50)
        self.assertEqual(campaign.deepant_initial_window_size, 256)
        self.assertEqual(campaign.deepant_epochs_initial, 20)
        self.assertIn("deepant", campaign.selected_models)

    @patch.object(DatasetService, "ensure_dataset_exists")
    def test_launch_campaign_persists_mdragpoint_parameters(self, ensure_dataset_exists) -> None:
        request = CampaignLaunchRequest(
            dataset_path="data/source-1000.csv",
            mode="batch",
            selected_models=["hstree", "mdragpoint"],
            mdragpoint_verdict_mode="external",
            mdragpoint_estimator="welford",
            mdragpoint_threshold_window=750,
            mdragpoint_anomaly_k_sigma=2.0,
            mdragpoint_anomaly_warmup_window=150,
            mdragpoint_dynamic_threshold=True,
            mdragpoint_lof_init=True,
            mdragpoint_n_init=250,
            mdragpoint_max_nb_clusters=20,
            mdragpoint_max_cluster_size=15,
            mdragpoint_lof_neighbors=8,
        )

        campaign = self.service.launch_campaign(request)

        ensure_dataset_exists.assert_called_once_with("data/source-1000.csv")
        config = self._assert_config_written()
        self.assertEqual(config["selected_models"], ["hstree", "mdragpoint"])
        self.assertEqual(config["models"]["mdragpoint"]["verdict_mode"], "external")
        self.assertEqual(config["models"]["mdragpoint"]["estimator"], "welford")
        self.assertEqual(config["models"]["mdragpoint"]["threshold_window"], 750)
        self.assertEqual(config["models"]["mdragpoint"]["anomaly_k_sigma"], 2.0)
        self.assertEqual(config["models"]["mdragpoint"]["anomaly_warmup_window"], 150)
        self.assertEqual(config["models"]["mdragpoint"]["dynamic_threshold"], True)
        self.assertEqual(config["models"]["mdragpoint"]["lof_init"], True)
        self.assertEqual(config["models"]["mdragpoint"]["n_init"], 250)
        self.assertEqual(config["models"]["mdragpoint"]["max_nb_clusters"], 20)
        self.assertEqual(config["models"]["mdragpoint"]["max_cluster_size"], 15)
        self.assertEqual(config["models"]["mdragpoint"]["lof_neighbors"], 8)
        self.assertEqual(campaign.mdragpoint_verdict_mode, "external")
        self.assertEqual(campaign.mdragpoint_estimator, "welford")
        self.assertEqual(campaign.mdragpoint_threshold_window, 750)
        self.assertEqual(campaign.mdragpoint_anomaly_k_sigma, 2.0)
        self.assertTrue(campaign.mdragpoint_dynamic_threshold)
        self.assertTrue(campaign.mdragpoint_lof_init)
        self.assertIn("mdragpoint", campaign.selected_models)

    @patch.object(DatasetService, "ensure_dataset_exists")
    def test_launch_campaign_passes_explainability_nsamples(self, ensure_dataset_exists) -> None:
        request = CampaignLaunchRequest(
            dataset_path="data/source-1000.csv",
            mode="batch",
            selected_models=["hstree", "explainability", "explainability-v2"],
            explainability_nsamples=1000,
            explainability_v2_nsamples=2000,
            explainability_model="hstree",
            explainability_mode="reference",
            explainability_method="kernel",
            explainability_v2_model="hstree",
            explainability_v2_mode="reference",
            explainability_v2_method="kernel",
        )

        campaign = self.service.launch_campaign(request)

        ensure_dataset_exists.assert_called_once_with("data/source-1000.csv")
        config = self._assert_config_written()
        self.assertEqual(config["selected_models"], ["hstree", "explainability", "explainability-v2"])
        self.assertEqual(config["explainability"]["nsamples"], 1000)
        self.assertEqual(config["explainability"]["v2_nsamples"], 2000)
        self.assertEqual(campaign.explainability_nsamples, 1000)
        self.assertEqual(campaign.explainability_v2_nsamples, 2000)

    @patch.object(DatasetService, "ensure_dataset_exists")
    def test_launch_campaign_uses_dataset_preset_when_filters_are_empty(self, ensure_dataset_exists) -> None:
        request = CampaignLaunchRequest(dataset_path="data/source.csv", mode="realtime")

        campaign = self.service.launch_campaign(request)

        ensure_dataset_exists.assert_called_once_with("data/source.csv")
        self.assertEqual(campaign.selected_groups, [
            "1A", "2B", "3A", "4B", "IMPBK UV", "NETBK", "Selek", "Sortie CTA", "TTHBK",
        ])
        self.assertEqual(campaign.excluded_fields, DEFAULT_EXCLUDED_FIELDS)

    # ------------------------------------------------------------------
    # start_production
    # ------------------------------------------------------------------

    @patch.object(DatasetService, "ensure_dataset_exists")
    def test_start_production_updates_prepared_campaign(self, ensure_dataset_exists) -> None:
        preset = self.dataset_service.get_dataset_preset("data/source-1000.csv")
        self.state.set(CampaignView(
            dataset_path="data/source-1000.csv", mode="batch",
            selected_groups=[], excluded_fields=[], status="configuring",
            created_at="2026-04-07T00:00:00+00:00", started_at=None,
            updated_at="2026-04-07T00:00:00+00:00",
        ))

        request = CampaignLaunchRequest(dataset_path="data/source-1000.csv", mode="batch")
        campaign = self.service.start_production(request)

        ensure_dataset_exists.assert_called_once_with("data/source-1000.csv")
        self.mock_orchestrator.start_production.assert_called_once_with(self._config_path())
        config = self._assert_config_written()
        self.assertEqual(config["dataset_path"], "data/source-1000.csv")
        self.assertEqual(config["mode"], "batch")
        self.assertEqual(config["selected_groups"], preset.selected_groups if preset is not None else [])
        self.assertEqual(config["excluded_fields"], preset.excluded_fields if preset is not None else [])
        self.assertEqual(config["selected_models"], ["hstree", "iforest", "kitnet"])
        self.assertEqual(campaign.status, "running")
        self.assertIsNotNone(campaign.started_at)
        self.assertEqual(self.state.get_platform_status(), "processing_data")

    @patch.object(DatasetService, "ensure_dataset_exists")
    def test_start_production_supports_realtime_mode(self, ensure_dataset_exists) -> None:
        preset = self.dataset_service.get_dataset_preset("data/source.csv")
        self.state.set(CampaignView(
            dataset_path="data/source.csv", mode="realtime",
            selected_groups=["1A"], excluded_fields=["timestamp"], status="configuring",
            created_at="2026-04-07T00:00:00+00:00", started_at=None,
            updated_at="2026-04-07T00:00:00+00:00",
        ))

        request = CampaignLaunchRequest(dataset_path="data/source.csv", mode="realtime")
        campaign = self.service.start_production(request)

        ensure_dataset_exists.assert_called_once_with("data/source.csv")
        config = self._assert_config_written()
        self.assertEqual(config["dataset_path"], "data/source.csv")
        self.assertEqual(config["mode"], "realtime")
        self.assertEqual(config["selected_groups"], preset.selected_groups if preset is not None else [])
        self.assertEqual(config["excluded_fields"], preset.excluded_fields if preset is not None else [])
        self.assertEqual(campaign.mode, "realtime")

    @patch.object(DatasetService, "ensure_dataset_exists")
    def test_start_production_supports_realtime_x5_mode(self, ensure_dataset_exists) -> None:
        preset = self.dataset_service.get_dataset_preset("data/source.csv")
        self.state.set(CampaignView(
            dataset_path="data/source.csv", mode="realtime-x5",
            selected_groups=["1A"], excluded_fields=["timestamp"], status="configuring",
            created_at="2026-04-07T00:00:00+00:00", started_at=None,
            updated_at="2026-04-07T00:00:00+00:00",
        ))

        request = CampaignLaunchRequest(dataset_path="data/source.csv", mode="realtime-x5")
        campaign = self.service.start_production(request)

        ensure_dataset_exists.assert_called_once_with("data/source.csv")
        config = self._assert_config_written()
        self.assertEqual(config["mode"], "realtime-x5")
        self.assertEqual(config["selected_groups"], preset.selected_groups if preset is not None else [])
        self.assertEqual(campaign.mode, "realtime-x5")

    @patch.object(DatasetService, "ensure_dataset_exists")
    def test_start_production_supports_realtime_x20_mode(self, ensure_dataset_exists) -> None:
        preset = self.dataset_service.get_dataset_preset("data/source.csv")
        self.state.set(CampaignView(
            dataset_path="data/source.csv", mode="realtime-x20",
            selected_groups=["1A"], excluded_fields=["timestamp"], status="configuring",
            created_at="2026-04-07T00:00:00+00:00", started_at=None,
            updated_at="2026-04-07T00:00:00+00:00",
        ))

        request = CampaignLaunchRequest(dataset_path="data/source.csv", mode="realtime-x20")
        campaign = self.service.start_production(request)

        ensure_dataset_exists.assert_called_once_with("data/source.csv")
        config = self._assert_config_written()
        self.assertEqual(config["mode"], "realtime-x20")
        self.assertEqual(campaign.mode, "realtime-x20")

    @patch.object(DatasetService, "ensure_dataset_exists")
    def test_start_production_supports_batch_mode(self, ensure_dataset_exists) -> None:
        preset = self.dataset_service.get_dataset_preset("data/source.csv")
        self.state.set(CampaignView(
            dataset_path="data/source.csv", mode="batch",
            selected_groups=["1A"], excluded_fields=["timestamp"], status="configuring",
            created_at="2026-04-07T00:00:00+00:00", started_at=None,
            updated_at="2026-04-07T00:00:00+00:00",
        ))

        request = CampaignLaunchRequest(dataset_path="data/source.csv", mode="batch")
        campaign = self.service.start_production(request)

        ensure_dataset_exists.assert_called_once_with("data/source.csv")
        config = self._assert_config_written()
        self.assertEqual(config["mode"], "batch")
        self.assertEqual(campaign.mode, "batch")

    @patch.object(DatasetService, "ensure_dataset_exists")
    def test_start_production_passes_analysis_threshold_parameters(self, ensure_dataset_exists) -> None:
        preset = self.dataset_service.get_dataset_preset("data/source-1000.csv")
        self.state.set(CampaignView(
            dataset_path="data/source-1000.csv", mode="batch",
            selected_groups=[], excluded_fields=[], status="configuring",
            created_at="2026-04-07T00:00:00+00:00", started_at=None,
            updated_at="2026-04-07T00:00:00+00:00",
        ))

        request = CampaignLaunchRequest(
            dataset_path="data/source-1000.csv", mode="batch",
            iforest_anomaly_k_sigma=2.5, iforest_anomaly_warmup_window=80,
            kitnet_estimator="p99", kitnet_threshold_window=900,
            kitnet_threshold_percentile=98.5, kitnet_anomaly_k_sigma=4.0,
            kitnet_anomaly_warmup_window=120,
        )
        campaign = self.service.start_production(request)

        ensure_dataset_exists.assert_called_once_with("data/source-1000.csv")
        config = self._assert_config_written()
        self.assertEqual(config["dataset_path"], "data/source-1000.csv")
        self.assertEqual(config["mode"], "batch")
        self.assertEqual(config["selected_groups"], preset.selected_groups if preset is not None else [])
        self.assertEqual(config["excluded_fields"], preset.excluded_fields if preset is not None else [])
        self.assertEqual(config["models"]["iforest"]["anomaly_k_sigma"], 2.5)
        self.assertEqual(config["models"]["iforest"]["anomaly_warmup_window"], 80)
        self.assertEqual(config["models"]["kitnet"]["estimator"], "p99")
        self.assertEqual(config["models"]["kitnet"]["threshold_window"], 900)
        self.assertEqual(config["models"]["kitnet"]["threshold_percentile"], 98.5)
        self.assertEqual(config["models"]["kitnet"]["anomaly_k_sigma"], 4.0)
        self.assertEqual(config["models"]["kitnet"]["anomaly_warmup_window"], 120)
        self.assertEqual(campaign.iforest_anomaly_k_sigma, 2.5)
        self.assertEqual(campaign.iforest_anomaly_warmup_window, 80)
        self.assertEqual(campaign.kitnet_estimator, "p99")
        self.assertEqual(campaign.kitnet_threshold_window, 900)
        self.assertEqual(campaign.kitnet_threshold_percentile, 98.5)
        self.assertEqual(campaign.kitnet_anomaly_k_sigma, 4.0)
        self.assertEqual(campaign.kitnet_anomaly_warmup_window, 120)

    # ------------------------------------------------------------------
    # reset / stop / stop_and_reset
    # ------------------------------------------------------------------

    def test_reset_campaign_keeps_campaign_and_changes_status(self) -> None:
        self.state.set(CampaignView(
            dataset_path="data/source-1000.csv", mode="batch",
            selected_groups=[], excluded_fields=[], status="running",
            created_at="2026-04-07T00:00:00+00:00", started_at="2026-04-07T00:00:00+00:00",
            updated_at="2026-04-07T00:00:00+00:00",
        ))

        result = self.service.reset_campaign()

        self.mock_orchestrator.reset_campaign.assert_called_once()
        self.assertEqual(result.action, "reset_campaign")
        self.assertEqual(result.details, "topics recreated and influxdb buckets cleared")
        self.assertEqual(self.state.get().status, "reseted")
        self.assertEqual(self.state.get_platform_status(), "waiting_for_launch")

    def test_stop_and_reset_for_new_campaign_updates_platform_state(self) -> None:
        self.state.set(CampaignView(
            dataset_path="data/source-1000.csv", mode="batch",
            selected_groups=[], excluded_fields=[], status="running",
            created_at="2026-04-07T00:00:00+00:00", started_at="2026-04-07T00:00:00+00:00",
            updated_at="2026-04-07T00:00:00+00:00",
        ))

        result = self.service.stop_and_reset_for_new_campaign()

        self.mock_orchestrator.stop_campaign.assert_called_once()
        self.mock_orchestrator.reset_campaign.assert_called_once()
        self.assertEqual(result.action, "stop_and_reset_for_new_campaign")
        self.assertIsNone(self.state.get())
        self.assertEqual(self.state.get_platform_status(), "waiting_for_launch")

    # ------------------------------------------------------------------
    # platform status
    # ------------------------------------------------------------------

    @patch.object(DockerService, "get_services_status")
    def test_platform_status_detects_running_services(self, get_services_status) -> None:
        get_services_status.return_value = [
            ServiceStatusView(name="kafka1", container_name="kafka1", state="running", health="healthy", status="healthy")
        ]

        status = self.service.get_platform_status()

        self.assertEqual(status.platform_status, "waiting_for_launch")
        self.assertEqual(status.campaign_status, "idle")

    @patch.object(DockerService, "get_services_status")
    def test_platform_status_reports_current_phase(self, get_services_status) -> None:
        get_services_status.return_value = []
        self.state.set_platform_status("processing_data")

        status = self.service.get_platform_status()

        self.assertEqual(status.platform_status, "processing_data")


    # ------------------------------------------------------------------
    # explainability-v2 stop/reset guards (OC-11)
    # ------------------------------------------------------------------

    def _set_v2_state(self, state: str | None) -> None:
        patcher = patch.object(
            ProgressService,
            "get_explainability_v2_progress",
            return_value=ExplainabilityV2ProgressView(
                selected=state is not None,
                state=state,
                reason=(
                    "v2 consumer lag: 17"
                    if state == "processing"
                    else "v2 worker service stopped"
                ),
            ),
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def _set_running_batch_campaign_with_v2(self, v2_state: str) -> None:
        self.state.set(
            CampaignView(
                dataset_path="data/source-1000.csv",
                mode="batch",
                selected_groups=[],
                excluded_fields=[],
                selected_models=["hstree", "explainability-v2"],
                status="running",
                created_at="2026-04-07T00:00:00+00:00",
                started_at="2026-04-07T00:00:00+00:00",
                updated_at="2026-04-07T00:00:00+00:00",
            )
        )
        self.state.set_platform_status("processing_data")
        self._set_v2_state(v2_state)

    def test_stop_campaign_blocked_while_v2_processing(self) -> None:
        self._set_running_batch_campaign_with_v2("processing")

        with self.assertRaises(HTTPException) as ctx:
            self.service.stop_campaign()

        self.assertEqual(ctx.exception.status_code, 409)
        self.assertEqual(ctx.exception.detail["state"], "processing")
        self.assertEqual(ctx.exception.detail["reason"], "v2 consumer lag: 17")
        self.assertIn("force=true", ctx.exception.detail["message"])
        self.mock_orchestrator.stop_campaign.assert_not_called()
        # The platform state was not moved before the guard refused.
        self.assertEqual(self.state.get_platform_status(), "processing_data")

    def test_reset_campaign_blocked_while_v2_processing(self) -> None:
        self._set_running_batch_campaign_with_v2("processing")

        with self.assertRaises(HTTPException) as ctx:
            self.service.reset_campaign()

        self.assertEqual(ctx.exception.status_code, 409)
        self.assertEqual(ctx.exception.detail["state"], "processing")
        self.mock_orchestrator.reset_campaign.assert_not_called()

    def test_stop_and_reset_blocked_while_v2_processing(self) -> None:
        self._set_running_batch_campaign_with_v2("processing")

        with self.assertRaises(HTTPException) as ctx:
            self.service.stop_and_reset_for_new_campaign()

        self.assertEqual(ctx.exception.status_code, 409)
        self.mock_orchestrator.stop_campaign.assert_not_called()
        self.mock_orchestrator.reset_campaign.assert_not_called()

    def test_reset_campaign_blocked_after_v2_failed(self) -> None:
        self._set_running_batch_campaign_with_v2("failed")

        with self.assertRaises(HTTPException) as ctx:
            self.service.reset_campaign()

        self.assertEqual(ctx.exception.status_code, 409)
        self.assertEqual(ctx.exception.detail["state"], "failed")
        self.mock_orchestrator.reset_campaign.assert_not_called()

    def test_stop_campaign_allowed_after_v2_complete(self) -> None:
        self._set_running_batch_campaign_with_v2("complete")

        result = self.service.stop_campaign()

        self.mock_orchestrator.stop_campaign.assert_called_once()
        self.assertEqual(result.status, "ok")
        self.assertFalse(result.forced)
        self.assertFalse(result.campaign_incomplete)

    def test_reset_campaign_allowed_after_v2_complete(self) -> None:
        self._set_running_batch_campaign_with_v2("complete")

        result = self.service.reset_campaign()

        self.mock_orchestrator.reset_campaign.assert_called_once()
        self.assertEqual(result.status, "ok")
        self.assertFalse(result.forced)

    def test_force_stop_calls_orchestrator_and_marks_campaign_incomplete(self) -> None:
        self._set_running_batch_campaign_with_v2("processing")

        result = self.service.stop_campaign(force=True)

        self.mock_orchestrator.stop_campaign.assert_called_once()
        self.assertTrue(result.forced)
        self.assertTrue(result.campaign_incomplete)
        self.assertEqual(result.status, "ok")

    def test_force_reset_calls_orchestrator_and_marks_campaign_incomplete(self) -> None:
        self._set_running_batch_campaign_with_v2("failed")

        result = self.service.reset_campaign(force=True)

        self.mock_orchestrator.reset_campaign.assert_called_once()
        self.assertTrue(result.forced)
        self.assertTrue(result.campaign_incomplete)

    def test_force_stop_and_reset_calls_orchestrator_and_marks_campaign_incomplete(self) -> None:
        self._set_running_batch_campaign_with_v2("processing")

        result = self.service.stop_and_reset_for_new_campaign(force=True)

        self.mock_orchestrator.stop_campaign.assert_called_once()
        self.mock_orchestrator.reset_campaign.assert_called_once()
        self.assertTrue(result.forced)
        self.assertTrue(result.campaign_incomplete)
        self.assertIsNone(self.state.get())

    def test_guard_not_triggered_without_v2_selection(self) -> None:
        # Campaign without v2: stop and reset proceed even with a null state.
        self.state.set(
            CampaignView(
                dataset_path="data/source-1000.csv",
                mode="batch",
                selected_groups=[],
                excluded_fields=[],
                selected_models=["hstree"],
                status="running",
                created_at="2026-04-07T00:00:00+00:00",
                started_at="2026-04-07T00:00:00+00:00",
                updated_at="2026-04-07T00:00:00+00:00",
            )
        )
        self._set_v2_state(None)

        result = self.service.stop_campaign()

        self.mock_orchestrator.stop_campaign.assert_called_once()
        self.assertEqual(result.status, "ok")

    # ------------------------------------------------------------------
    # combined V1 + V2 stop/reset guards (V1-04c)
    # ------------------------------------------------------------------

    def _set_v1_state(self, state: str | None) -> None:
        patcher = patch.object(
            ProgressService,
            "get_explainability_v1_progress",
            return_value=ExplainabilityV1ProgressView(
                selected=state is not None,
                state=state,
                reason=(
                    "v1 consumer lag: 5"
                    if state == "processing"
                    else "v1 worker service stopped"
                ),
            ),
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def _set_running_batch_campaign_with_v1(self, v1_state: str) -> None:
        self.state.set(
            CampaignView(
                dataset_path="data/source-1000.csv",
                mode="batch",
                selected_groups=[],
                excluded_fields=[],
                selected_models=["hstree", "explainability"],
                status="running",
                created_at="2026-04-07T00:00:00+00:00",
                started_at="2026-04-07T00:00:00+00:00",
                updated_at="2026-04-07T00:00:00+00:00",
            )
        )
        self.state.set_platform_status("processing_data")
        self._set_v1_state(v1_state)
        self._set_v2_state(None)

    def test_stop_campaign_blocked_while_v1_processing(self) -> None:
        self._set_running_batch_campaign_with_v1("processing")

        with self.assertRaises(HTTPException) as ctx:
            self.service.stop_campaign()

        self.assertEqual(ctx.exception.status_code, 409)
        self.assertEqual(ctx.exception.detail["state"], "processing")
        self.assertEqual(ctx.exception.detail["reason"], "v1 consumer lag: 5")
        self.assertIn("force=true", ctx.exception.detail["message"])
        self.assertIn("explainability is processing", ctx.exception.detail["message"])
        self.mock_orchestrator.stop_campaign.assert_not_called()

    def test_reset_campaign_blocked_after_v1_failed(self) -> None:
        self._set_running_batch_campaign_with_v1("failed")

        with self.assertRaises(HTTPException) as ctx:
            self.service.reset_campaign()

        self.assertEqual(ctx.exception.status_code, 409)
        self.assertEqual(ctx.exception.detail["state"], "failed")
        self.mock_orchestrator.reset_campaign.assert_not_called()

    def test_stop_and_reset_blocked_while_v1_processing(self) -> None:
        self._set_running_batch_campaign_with_v1("processing")

        with self.assertRaises(HTTPException) as ctx:
            self.service.stop_and_reset_for_new_campaign()

        self.assertEqual(ctx.exception.status_code, 409)
        self.mock_orchestrator.stop_campaign.assert_not_called()
        self.mock_orchestrator.reset_campaign.assert_not_called()

    def test_v1_complete_allows_stop_and_reset(self) -> None:
        self._set_running_batch_campaign_with_v1("complete")

        result = self.service.stop_and_reset_for_new_campaign()

        self.mock_orchestrator.stop_campaign.assert_called_once()
        self.mock_orchestrator.reset_campaign.assert_called_once()
        self.assertEqual(result.status, "ok")

    def test_v1_processing_and_v2_processing_block_stop(self) -> None:
        # The decision combines V1 and V2: the first blocking view wins.
        self._set_running_batch_campaign_with_v1("processing")
        self._set_v2_state("processing")

        with self.assertRaises(HTTPException) as ctx:
            self.service.stop_campaign()

        self.assertEqual(ctx.exception.status_code, 409)
        # V1 is evaluated first: the conflict names V1.
        self.assertIn("explainability is processing", ctx.exception.detail["message"])

    def test_v1_complete_with_v2_processing_still_blocks_stop(self) -> None:
        self._set_running_batch_campaign_with_v1("complete")
        self._set_v2_state("processing")

        with self.assertRaises(HTTPException) as ctx:
            self.service.stop_campaign()

        self.assertEqual(ctx.exception.status_code, 409)
        self.assertIn("explainability-v2 is processing", ctx.exception.detail["message"])

    def test_v1_failed_and_v2_complete_block_reset(self) -> None:
        self._set_running_batch_campaign_with_v1("failed")
        self._set_v2_state("complete")

        with self.assertRaises(HTTPException) as ctx:
            self.service.reset_campaign()

        self.assertEqual(ctx.exception.status_code, 409)
        self.assertEqual(ctx.exception.detail["state"], "failed")

    def test_v1_complete_and_v2_complete_allow_stop(self) -> None:
        self._set_running_batch_campaign_with_v1("complete")
        self._set_v2_state("complete")

        result = self.service.stop_campaign()

        self.mock_orchestrator.stop_campaign.assert_called_once()
        self.assertEqual(result.status, "ok")

    def test_force_bypasses_the_combined_v1_v2_guard(self) -> None:
        self._set_running_batch_campaign_with_v1("processing")
        self._set_v2_state("processing")

        result = self.service.stop_campaign(force=True)

        self.mock_orchestrator.stop_campaign.assert_called_once()
        self.assertTrue(result.forced)
        self.assertTrue(result.campaign_incomplete)

    def test_guard_not_triggered_without_v1_selection(self) -> None:
        # Campaign without V1: stop proceeds even with a null V1 state.
        self.state.set(
            CampaignView(
                dataset_path="data/source-1000.csv",
                mode="batch",
                selected_groups=[],
                excluded_fields=[],
                selected_models=["hstree"],
                status="running",
                created_at="2026-04-07T00:00:00+00:00",
                started_at="2026-04-07T00:00:00+00:00",
                updated_at="2026-04-07T00:00:00+00:00",
            )
        )
        self._set_v1_state(None)
        self._set_v2_state(None)

        result = self.service.stop_campaign()

        self.mock_orchestrator.stop_campaign.assert_called_once()
        self.assertEqual(result.status, "ok")
