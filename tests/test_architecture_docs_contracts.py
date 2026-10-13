from pathlib import Path
from unittest import TestCase


ROOT = Path(__file__).resolve().parents[1]


class ArchitectureDocsContractsTest(TestCase):
    def _read(self, relative_path: str) -> str:
        return (ROOT / relative_path).read_text(encoding="utf-8")

    def test_schema_and_bootstrap_include_active_models_and_event_time_consensus(self) -> None:
        for relative_path in (
            "docs/architecture/schema.md",
            "docs/architecture/bootstrap.md",
        ):
            doc = self._read(relative_path)
            self.assertIn("deepant-analysis", doc)
            self.assertIn("deepant-analysis-viz", doc)
            self.assertIn("mdragstream-point-analysis", doc)
            self.assertIn("mdragstream-point-analysis-viz", doc)
            self.assertIn("joins active model verdicts per `(Group, source_event_id)`", doc)

        self.assertIn("creates 26 topics", self._read("docs/architecture/schema.md"))

    def test_data_storage_documents_model_buckets_and_five_model_consensus(self) -> None:
        doc = self._read("docs/architecture/data-storage.md")
        consensus_doc = self._read("runtime/consensus/consensus_analysis.md")

        self.assertIn("deepant-analysis", doc)
        self.assertIn("deepant_anomaly", doc)
        self.assertIn("mdragstream-point-analysis", doc)
        self.assertIn("mdragpoint_anomaly", doc)
        self.assertIn("mdragpoint_internal_r", doc)
        self.assertIn(
            "intentionally do not display `mdragpoint_threshold` or",
            doc,
        )
        self.assertIn("event-time join per (Group, source_event_id)", doc)
        self.assertIn("source_event_id` tag", doc)
        self.assertIn("five-model consensus", doc)
        self.assertNotIn("three-model consensus", doc)
        self.assertNotIn("four-model consensus", doc)

        self.assertIn("Its range is `0..active_models`", consensus_doc)
        self.assertIn("maximum is `5`", consensus_doc)
        self.assertIn("mdragstream-point-analysis-viz", consensus_doc)
        self.assertIn("SELECTED_MODELS", consensus_doc)
        self.assertNotIn("Solution retenue", consensus_doc)
        self.assertNotIn("rôle résiduel", consensus_doc)

    def test_observability_docs_keep_active_models(self) -> None:
        observability = self._read("docs/observability/observability-grafana-migration.md")

        self.assertIn("deepant-analysis-viz", observability)
        self.assertIn("source-data_deepant_influxdb.json", observability)
        self.assertIn("Source Data and DeepAnT", observability)
        self.assertIn("mdragstream-point-analysis-viz", observability)
        self.assertIn("source-data_mdragpoint_influxdb.json", observability)
        self.assertIn("Source Data and mDragStream Point-based", observability)

    def test_askmydasma_campaign_runtime_contract_documents_json_source_of_truth(self) -> None:
        doc = self._read("docs/architecture/askmydasma-campaign-runtime.md")
        docs_index = self._read("docs/README.md")

        for fragment in (
            ".tmp/current-campaign.json",
            "scripts/web/campaign_config.py",
            "launch_campaign.sh .tmp/current-campaign.json",
            "start_production.sh .tmp/current-campaign.json",
            "relaunch_campaign.sh .tmp/current-campaign.json",
            "selected_models",
            "Production starts only the selected model",
            "DASMA_COMPOSE_SERVICES",
            "DASMA_ENABLED=0",
            "does not pay the PyTorch",
            "GET /api/campaign-progress",
            "Backend Restore",
            "processing_data",
        ):
            self.assertIn(fragment, doc)

        self.assertIn("architecture/askmydasma-campaign-runtime.md", docs_index)

    def test_askmydasma_validation_workflow_documents_default_and_optional_models(self) -> None:
        doc = self._read("docs/architecture/askmydasma-validation-workflow.md")
        docs_index = self._read("docs/README.md")

        for fragment in (
            "ASKMYDASMA_PYTHON=.venv/bin/python bash start.sh",
            "scripts/web/start_production.sh .tmp/current-campaign.json",
            "scripts/web/stop_campaign.sh",
            "scripts/web/reset_campaign.sh",
            "DASMA_COMPOSE_SERVICES",
            "deepant-analysis",
            "mdragstream-point-analysis",
            "explainability-v2",
            "reset-topics",
            "reset-schema-subjects",
        ):
            self.assertIn(fragment, doc)

        self.assertIn("architecture/askmydasma-validation-workflow.md", docs_index)

