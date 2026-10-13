"""Unit tests for the AskMyDasma API."""

from __future__ import annotations

import unittest
from unittest.mock import patch

try:
    import fastapi  # noqa: F401

    FASTAPI_AVAILABLE = True
except ImportError:
    fastapi = None  # type: ignore[assignment]
    FASTAPI_AVAILABLE = False

if FASTAPI_AVAILABLE:
    from fastapi.testclient import TestClient

    from src.dasma_kafka.web.app import create_app
    from src.dasma_kafka.web.models import (
        ActionResultView,
        CampaignProgressView,
        DatasetsView,
        ExplainabilityV1ProgressView,
        ExplainabilityV2ProgressView,
        ServiceStatusView,
    )
    from src.dasma_kafka.web.services.dataset_service import DatasetService
else:
    TestClient = None  # type: ignore[assignment, misc]
    create_app = None  # type: ignore[assignment]
    DatasetsView = None  # type: ignore[assignment, misc]
    ServiceStatusView = None  # type: ignore[assignment, misc]
    DatasetService = None  # type: ignore[assignment, misc]
    ActionResultView = None  # type: ignore[assignment, misc]
    CampaignProgressView = None  # type: ignore[assignment, misc]
    ExplainabilityV1ProgressView = None  # type: ignore[assignment, misc]
    ExplainabilityV2ProgressView = None  # type: ignore[assignment, misc]


@unittest.skipIf(not FASTAPI_AVAILABLE, reason="fastapi is not installed")
class WebApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(create_app(restore_campaign=False))

    def test_health_endpoint(self) -> None:
        response = self.client.get("/health")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})

    def test_launch_rejects_mismatched_hstree_windows_with_actionable_message(self) -> None:
        response = self.client.post(
            "/api/campaigns/launch",
            json={
                "dataset_path": "data/source-1000.csv",
                "mode": "batch",
                "selected_models": ["hstree"],
                "hstree_initial_window_size": 128,
                "hstree_window_size": 2048,
            },
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.json()["detail"],
            "hstree: la fenêtre d'amorçage (128) doit être égale à la fenêtre du modèle "
            "(2048) : fenêtre de référence du papier",
        )

    def test_start_production_returns_validation_error_as_bad_request(self) -> None:
        message = "hstree: la fenêtre d'amorçage doit être égale à la fenêtre du modèle"
        with patch(
            "src.dasma_kafka.web.services.campaign_service.CampaignService.start_production",
            side_effect=ValueError(message),
        ):
            response = self.client.post(
                "/api/campaigns/start-production",
                json={
                    "dataset_path": "data/source-1000.csv",
                    "mode": "batch",
                    "selected_models": ["hstree"],
                    "hstree_initial_window_size": 128,
                    "hstree_window_size": 2048,
                },
            )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"], message)

    def test_user_frontend_page_is_served(self) -> None:
        response = self.client.get("/askmydasma/")

        self.assertEqual(response.status_code, 200)
        self.assertIn("Start analysis", response.text)
        self.assertIn("Stop and reset for new campaign", response.text)
        self.assertIn("Open administration", response.text)
        self.assertIn('data-grafana-path="/d/source-data-hstree-influxdb/features-hstree?', response.text)
        self.assertIn('data-grafana-path="/d/source-data-iforest-influxdb/features-iforest?', response.text)
        self.assertIn('data-grafana-path="/d/source-data-kitnet-influxdb/features-kitnet?', response.text)
        self.assertIn('data-grafana-path="/d/source-data-deepant-influxdb/features-deepant?', response.text)
        self.assertIn('data-grafana-path="/d/source-data-mdragpoint-influxdb/features-mdragpoint?', response.text)
        self.assertIn('id="model-deepant"', response.text)
        self.assertIn('id="model-mdragpoint"', response.text)
        self.assertIn('id="model-explainability-v2"', response.text)
        self.assertIn('id="mdragpoint-params"', response.text)
        self.assertIn('id="mdragpoint-verdict-mode"', response.text)
        self.assertIn("Native mDragStream internal threshold", response.text)
        self.assertIn("External P99/Welford threshold", response.text)
        self.assertIn('id="mdragpoint-external-params"', response.text)
        self.assertIn('id="mdragpoint-dynamic-threshold"', response.text)
        self.assertIn('id="mdragpoint-lof-init"', response.text)
        self.assertIn('id="mdragpoint-estimator"', response.text)
        self.assertIn('id="mdragpoint-window"', response.text)
        self.assertIn('id="mdragpoint-percentile"', response.text)
        self.assertIn('id="mdragpoint-k-sigma"', response.text)
        self.assertIn('id="mdragpoint-warmup-window"', response.text)
        self.assertIn('id="mdragpoint-n-init"', response.text)
        self.assertIn('id="mdragpoint-max-nb-clusters"', response.text)
        self.assertIn('id="mdragpoint-max-cluster-size"', response.text)
        self.assertIn('id="mdragpoint-lof-neighbors"', response.text)
        self.assertIn('target="_blank"', response.text)
        # Repeatability controls (step 7): five model checkboxes, no
        # explainability repeatability control.
        for model in ("hstree", "iforest", "kitnet", "deepant", "mdragpoint"):
            self.assertIn(f'id="repeatability-{model}"', response.text)
            self.assertIn(f'id="repeatability-{model}-wrapper"', response.text)
            self.assertIn(f'for="repeatability-{model}"', response.text)
            self.assertIn(f'name="{model}_repeatability"', response.text)
        self.assertIn("Runs two deterministic instances of the model and compares their outputs.", response.text)
        self.assertNotIn('id="repeatability-explainability"', response.text)
        self.assertNotIn('id="repeatability-explainability-v2"', response.text)
        # Reproducibility controls (P2-1): five model checkboxes, a global role
        # selector, a campaign-id field, and no explainability control.
        for model in ("hstree", "iforest", "kitnet", "deepant", "mdragpoint"):
            self.assertIn(f'id="reproducibility-{model}"', response.text)
            self.assertIn(f'name="{model}_reproducibility"', response.text)
        self.assertIn(">Reproducibility<", response.text)
        self.assertIn('id="reproducibility-source-role"', response.text)
        self.assertIn('id="reproducibility-campaign-id"', response.text)
        self.assertIn('<option value="source-1">source-1</option>', response.text)
        self.assertIn('<option value="source-2">source-2</option>', response.text)
        self.assertNotIn('id="reproducibility-explainability"', response.text)
        self.assertNotIn('id="reproducibility-explainability-v2"', response.text)
        # Validation mode (global group): English labels, mutually exclusive
        # presentation checkboxes without a submitted name, and every
        # validation control hidden on first display.
        self.assertIn("<legend>Validation mode</legend>", response.text)
        self.assertIn('<label for="validation-mode-repeatability">Repeatability</label>', response.text)
        self.assertIn('<label for="validation-mode-reproducibility">Reproducibility</label>', response.text)
        for checkbox_id in ("validation-mode-repeatability", "validation-mode-reproducibility"):
            tag = response.text.split(f'id="{checkbox_id}"', 1)[0].rsplit("<input", 1)[1]
            self.assertNotIn("name=", tag)
        for model in ("hstree", "iforest", "kitnet", "deepant", "mdragpoint"):
            for family in ("repeatability", "reproducibility"):
                wrapper = response.text.split(f'id="{family}-{model}-wrapper"', 1)[0].rsplit("<div", 1)[1]
                self.assertIn("is-hidden", wrapper, f"{family}-{model}")
                self.assertIn("hidden", wrapper, f"{family}-{model}")
        settings = response.text.split('id="reproducibility-settings"', 1)[0].rsplit("<div", 1)[1]
        self.assertIn("is-hidden", settings)
        self.assertIn("hidden", settings)
        fieldset_start = response.text.index('<fieldset class="validation-mode"')
        fieldset_end = response.text.index("</fieldset>", fieldset_start)
        settings_start = response.text.index('id="reproducibility-settings"')
        self.assertLess(fieldset_start, settings_start)
        self.assertLess(settings_start, fieldset_end)
        self.assertIn(">Source role:</label>", response.text)
        self.assertIn(">Campaign ID:</label>", response.text)
        self.assertNotIn("Répétabilité", response.text)
        self.assertNotIn("Reproductibilité", response.text)

    def test_admin_frontend_page_is_served(self) -> None:
        response = self.client.get("/askmydasma/admin/")

        self.assertEqual(response.status_code, 200)
        self.assertIn("Stop", response.text)
        self.assertIn("Services", response.text)

    def test_campaign_progress_endpoint_without_campaign(self) -> None:
        response = self.client.get("/api/campaign-progress")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "dataset_path": None,
                "total_records": 0,
                "items": [],
                "explainability_v2": {
                    "selected": False,
                    "state": None,
                    "reason": None,
                    "upstream_records": None,
                    "published_records": None,
                    "consumer_lag": None,
                },
                "explainability_v1": {
                    "selected": False,
                    "state": None,
                    "reason": None,
                    "upstream_records": None,
                    "published_records": None,
                    "consumer_lag": None,
                },
            },
        )

    @patch("src.dasma_kafka.web.services.campaign_service.CampaignService.get_campaign_progress")
    def test_campaign_progress_endpoint_serializes_v2_state(self, get_progress) -> None:
        get_progress.return_value = CampaignProgressView(
            dataset_path="data/source-1000.csv",
            total_records=100,
            items=[],
            explainability_v2=ExplainabilityV2ProgressView(
                selected=True,
                state="processing",
                reason="v2 consumer lag: 17",
                upstream_records=100,
                published_records=83,
                consumer_lag=17,
            ),
        )

        response = self.client.get("/api/campaign-progress")

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(
            body["explainability_v2"],
            {
                "selected": True,
                "state": "processing",
                "reason": "v2 consumer lag: 17",
                "upstream_records": 100,
                "published_records": 83,
                "consumer_lag": 17,
            },
        )

    @patch("src.dasma_kafka.web.services.campaign_service.CampaignService.get_campaign_progress")
    def test_campaign_progress_endpoint_serializes_v1_state(self, get_progress) -> None:
        get_progress.return_value = CampaignProgressView(
            dataset_path="data/source-1000.csv",
            total_records=100,
            items=[],
            explainability_v1=ExplainabilityV1ProgressView(
                selected=True,
                state="processing",
                reason="v1 consumer lag: 5",
                upstream_records=100,
                published_records=83,
                consumer_lag=5,
            ),
        )

        response = self.client.get("/api/campaign-progress")

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(
            body["explainability_v1"],
            {
                "selected": True,
                "state": "processing",
                "reason": "v1 consumer lag: 5",
                "upstream_records": 100,
                "published_records": 83,
                "consumer_lag": 5,
            },
        )

    def test_campaign_progress_endpoint_without_campaign_has_empty_v1_view(self) -> None:
        response = self.client.get("/api/campaign-progress")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json()["explainability_v1"],
            {
                "selected": False,
                "state": None,
                "reason": None,
                "upstream_records": None,
                "published_records": None,
                "consumer_lag": None,
            },
        )

    @patch("src.dasma_kafka.web.services.progress_service.ProgressService.get_explainability_v1_progress")
    def test_stop_endpoint_refused_while_v1_processing(self, get_v1) -> None:
        get_v1.return_value = ExplainabilityV1ProgressView(
            selected=True, state="processing", reason="v1 consumer lag: 5"
        )

        response = self.client.post("/api/campaigns/stop")

        self.assertEqual(response.status_code, 409)
        detail = response.json()["detail"]
        self.assertEqual(detail["state"], "processing")
        self.assertEqual(detail["reason"], "v1 consumer lag: 5")
        self.assertIn("force=true", detail["message"])

    @patch("src.dasma_kafka.web.services.progress_service.ProgressService.get_explainability_v1_progress")
    def test_reset_endpoint_blocked_after_v1_failed(self, get_v1) -> None:
        get_v1.return_value = ExplainabilityV1ProgressView(
            selected=True, state="failed", reason="v1 worker service stopped"
        )

        response = self.client.post("/api/campaigns/reset")

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["detail"]["state"], "failed")

    @patch("src.dasma_kafka.web.services.progress_service.ProgressService.get_explainability_v2_progress")
    def test_stop_endpoint_refused_while_v2_processing(self, get_v2) -> None:
        get_v2.return_value = ExplainabilityV2ProgressView(
            selected=True, state="processing", reason="v2 consumer lag: 17"
        )

        response = self.client.post("/api/campaigns/stop")

        self.assertEqual(response.status_code, 409)
        detail = response.json()["detail"]
        self.assertEqual(detail["state"], "processing")
        self.assertEqual(detail["reason"], "v2 consumer lag: 17")
        self.assertIn("force=true", detail["message"])

    @patch("src.dasma_kafka.web.services.progress_service.ProgressService.get_explainability_v2_progress")
    def test_reset_endpoint_blocked_after_v2_failed(self, get_v2) -> None:
        get_v2.return_value = ExplainabilityV2ProgressView(
            selected=True, state="failed", reason="v2 worker service stopped"
        )

        response = self.client.post("/api/campaigns/reset")

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["detail"]["state"], "failed")

    @patch("src.dasma_kafka.web.services.campaign_service.CampaignService.stop_campaign")
    def test_stop_endpoint_passes_force_parameter(self, stop_campaign) -> None:
        stop_campaign.return_value = ActionResultView(status="ok", action="stop_campaign")

        response = self.client.post("/api/campaigns/stop?force=true")

        self.assertEqual(response.status_code, 200)
        stop_campaign.assert_called_once_with(force=True)

    @patch("src.dasma_kafka.web.services.campaign_service.CampaignService.reset_campaign")
    def test_reset_endpoint_passes_force_parameter(self, reset_campaign) -> None:
        reset_campaign.return_value = ActionResultView(status="ok", action="reset_campaign")

        response = self.client.post("/api/campaigns/reset?force=true")

        self.assertEqual(response.status_code, 200)
        reset_campaign.assert_called_once_with(force=True)

    @patch("src.dasma_kafka.web.services.campaign_service.CampaignService.stop_and_reset_for_new_campaign")
    def test_stop_and_reset_endpoint_passes_force_parameter(self, stop_and_reset) -> None:
        stop_and_reset.return_value = ActionResultView(
            status="ok", action="stop_and_reset_for_new_campaign"
        )

        response = self.client.post("/api/campaigns/stop-and-reset?force=true")

        self.assertEqual(response.status_code, 200)
        stop_and_reset.assert_called_once_with(force=True)

    @patch("src.dasma_kafka.web.services.campaign_service.CampaignService.stop_and_reset_for_new_campaign")
    def test_stop_and_reset_endpoint(self, stop_and_reset_for_new_campaign) -> None:
        stop_and_reset_for_new_campaign.return_value = {
            "status": "ok",
            "action": "stop_and_reset_for_new_campaign",
            "details": "topics recreated",
        }

        response = self.client.post("/api/campaigns/stop-and-reset")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["action"], "stop_and_reset_for_new_campaign")

    @patch("src.dasma_kafka.web.services.campaign_service.CampaignService.start_production")
    def test_start_production_endpoint(self, start_production) -> None:
        start_production.return_value = {
            "dataset_path": "data/source-1000.csv",
            "mode": "batch",
            "selected_groups": [],
            "excluded_fields": [],
            "status": "running",
            "created_at": "2026-04-07T00:00:00+00:00",
            "started_at": "2026-04-07T00:00:00+00:00",
            "updated_at": "2026-04-07T00:00:00+00:00",
        }

        response = self.client.post(
            "/api/campaigns/start-production",
            json={
                "dataset_path": "data/source-1000.csv",
                "mode": "batch",
                "selected_groups": [],
                "excluded_fields": [],
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "running")

    @patch("src.dasma_kafka.web.services.docker_service.DockerService.get_services_status")
    def test_platform_status_endpoint_reports_waiting_phase(self, get_services_status) -> None:
        get_services_status.return_value = [
            ServiceStatusView(name="grafana", container_name="grafana", state="running", health="healthy", status="healthy")
        ]
        response = self.client.get("/api/status")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["platform_status"], "waiting_for_launch")

    @patch("src.dasma_kafka.web.services.dataset_service.DatasetService.get_dataset_time_range")
    def test_dataset_time_range_endpoint(self, get_dataset_time_range) -> None:
        get_dataset_time_range.return_value = {
            "dataset_path": "data/source-1000.csv",
            "row_count": 1000,
            "mode": "source",
            "time_column": "timestamp",
            "start": "2024-01-01 10:00:00",
            "end": "2024-01-01 10:10:00",
            "duration_seconds": 600,
            "status": "detected",
        }

        response = self.client.get("/api/dataset-time-range?dataset_path=data/source-1000.csv")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "detected")

    @patch("src.dasma_kafka.web.services.dataset_service.DatasetService.get_dataset_preview")
    def test_dataset_preview_endpoint(self, get_dataset_preview) -> None:
        get_dataset_preview.return_value = {
            "dataset_path": "data/source-1000.csv",
            "columns": ["timestamp", "Group", "A", "B"],
            "sample_rows": [
                {
                    "timestamp": "2024-01-01 10:00:00",
                    "Group": "Sortie CTA",
                    "A": "1.2",
                    "B": "2.3",
                },
                {
                    "timestamp": "2024-01-01 10:00:01",
                    "Group": "Sortie CTA",
                    "A": "1.4",
                    "B": "2.5",
                },
            ],
            "max_rows": 10,
            "status": "ok",
        }

        response = self.client.get("/api/dataset-preview?dataset_path=data/source-1000.csv")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "ok")
        self.assertEqual(response.json()["columns"][0], "timestamp")

    @patch.object(DatasetService, "list_datasets")
    def test_datasets_endpoint(self, list_datasets) -> None:
        from src.dasma_kafka.web.dataset_presets import DATASET_PRESETS, DEFAULT_EXCLUDED_FIELDS
        dataset_paths = [
            "data/source.csv", "data/source-5000.csv", "data/source-1000.csv",
            "data/source-1000-clean.csv", "data/source-10000.csv",
            "data/component_2022-fixed.csv", "data/131_OPPORTUNITY-1000.csv",
        ]
        list_datasets.return_value = DatasetsView(
            datasets=dataset_paths,
            presets={p: DATASET_PRESETS[p] for p in dataset_paths if p in DATASET_PRESETS},
        )

        response = self.client.get("/api/datasets")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "datasets": [
                    "data/source.csv",
                    "data/source-5000.csv",
                    "data/source-1000.csv",
                    "data/source-1000-clean.csv",
                    "data/source-10000.csv",
                    "data/component_2022-fixed.csv",
                    "data/131_OPPORTUNITY-1000.csv",
                ],
                "presets": {
                    "data/source.csv": {
                        "label": "Preset source",
                        "selected_groups": [
                            "1A",
                            "2B",
                            "3A",
                            "4B",
                            "IMPBK UV",
                            "NETBK",
                            "Selek",
                            "Sortie CTA",
                            "TTHBK",
                        ],
                        "excluded_fields": DEFAULT_EXCLUDED_FIELDS,
                        "source": "docker-compose",
                    },
                    "data/source-5000.csv": {
                        "label": "Preset source-5000",
                        "selected_groups": [
                            "1A",
                            "2B",
                            "3A",
                            "4B",
                            "IMPBK UV",
                            "NETBK",
                            "Selek",
                            "Sortie CTA",
                            "TTHBK",
                        ],
                        "excluded_fields": DEFAULT_EXCLUDED_FIELDS,
                        "source": "docker-compose",
                    },
                    "data/source-1000.csv": {
                        "label": "Preset source-1000",
                        "selected_groups": [
                            "1A",
                            "2B",
                            "3A",
                            "4B",
                            "IMPBK UV",
                            "NETBK",
                            "Selek",
                            "Sortie CTA",
                            "TTHBK",
                        ],
                        "excluded_fields": DEFAULT_EXCLUDED_FIELDS,
                        "source": "docker-compose",
                    },
                    "data/source-1000-clean.csv": {
                        "label": "Preset source-1000-clean",
                        "selected_groups": [
                            "1A",
                            "2B",
                            "3A",
                            "4B",
                            "IMPBK UV",
                            "NETBK",
                            "Selek",
                            "Sortie CTA",
                            "TTHBK",
                        ],
                        "excluded_fields": DEFAULT_EXCLUDED_FIELDS,
                        "source": "docker-compose",
                    },
                    "data/source-10000.csv": {
                        "label": "Preset source-10000",
                        "selected_groups": [
                            "1A",
                            "2B",
                            "3A",
                            "4B",
                            "IMPBK UV",
                            "NETBK",
                            "Selek",
                            "Sortie CTA",
                            "TTHBK",
                        ],
                        "excluded_fields": DEFAULT_EXCLUDED_FIELDS,
                        "source": "docker-compose",
                    },
                    "data/component_2022-fixed.csv": {
                        "label": "Historical 2022 preset",
                        "selected_groups": [],
                        "excluded_fields": [
                            "timestamp",
                            "Group",
                            "group",
                            "name",
                            "time",
                        ],
                        "source": "compose/legacy/docker-compose-components-2022.yml",
                    },
                    "data/131_OPPORTUNITY-1000.csv": {
                        "label": "OPPORTUNITY preset",
                        "selected_groups": [],
                        "excluded_fields": [
                            "timestamp",
                            "Group",
                        ],
                        "source": "compose/legacy/docker-compose-131-OPPORTUNITY.yml",
                    },
                },
            },
        )


if __name__ == "__main__":
    unittest.main()
