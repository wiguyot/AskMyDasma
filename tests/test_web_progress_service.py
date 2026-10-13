"""Tests for AskMyDasma progress calculation."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.dasma_kafka.web.models import CampaignView, ServiceStatusView
from src.dasma_kafka.web.orchestration.compose_runner import ComposeError, ComposeRunner
from src.dasma_kafka.web.services.docker_service import DockerService
from src.dasma_kafka.web.services.progress_service import (
    EXPLAINABILITY_V1_CONSUMER_GROUP,
    EXPLAINABILITY_V1_UPSTREAM_TOPIC,
    EXPLAINABILITY_V2_CONSUMER_GROUP,
    EXPLAINABILITY_V2_UPSTREAM_TOPIC,
    PROGRESS_TOPICS,
    ProgressService,
)
from src.dasma_kafka.web.settings import WebSettings


def _running_v2_service() -> list[ServiceStatusView]:
    return [
        ServiceStatusView(
            name="explainability-v2",
            container_name="explainability-v2",
            state="running",
            health="healthy",
            status="healthy",
        )
    ]


class ProgressServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        project_root = Path(self.temp_dir.name)
        data_dir = project_root / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        (data_dir / "source.csv").write_text("a,b\n1,2\n3,4\n5,6\n", encoding="utf-8")

        self.settings = WebSettings(
            project_root=project_root,
            scripts_dir=project_root / "scripts",
            web_scripts_dir=project_root / "scripts" / "web",
            data_dir=data_dir,
            tmp_dir=project_root / ".tmp",
            host_project_root=project_root,
            frontend_dir=project_root / "frontend" / "askmydasma",
        )
        self.docker_service = MagicMock(spec=DockerService)
        self.docker_service.get_services_status.return_value = _running_v2_service()
        self.service = ProgressService(self.settings, docker_service=self.docker_service)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    @patch.object(ProgressService, "_get_topic_offsets")
    def test_get_campaign_progress_uses_offsets_against_dataset_rows(self, get_topic_offsets) -> None:
        get_topic_offsets.return_value = {
            "source-data": 2,
            "hstree-analysis": 2,
            "iforest-analysis": 1,
            "kitnet-analysis": None,
            "deepant-analysis": 0,
            "mdragstream-point-analysis": 0,
            "explainability": -1,
            "explainability-v2": -1,
        }
        campaign = CampaignView(
            dataset_path="data/source.csv",
            mode="batch",
            selected_groups=[],
            excluded_fields=[],
            status="running",
            created_at="2026-04-08T00:00:00+00:00",
            started_at="2026-04-08T00:00:00+00:00",
            updated_at="2026-04-08T00:00:00+00:00",
        )

        progress = self.service.get_campaign_progress(campaign)

        self.assertEqual(progress.dataset_path, "data/source.csv")
        self.assertEqual(progress.total_records, 3)
        self.assertEqual(
            [item.topic for item in progress.items],
            ["hstree-analysis", "iforest-analysis", "kitnet-analysis", "source-data"],
        )
        self.assertEqual([item.produced_records for item in progress.items], [3, 2, 0, 3])
        self.assertEqual([item.progress_percent for item in progress.items], [100.0, 66.7, 0.0, 100.0])
        get_topic_offsets.assert_called_once_with(
            ["hstree-analysis", "iforest-analysis", "kitnet-analysis", "source-data"]
        )

    @patch.object(ProgressService, "_get_topic_offsets")
    def test_get_campaign_progress_includes_only_selected_optional_models(self, get_topic_offsets) -> None:
        get_topic_offsets.return_value = {
            "source-data": 2,
            "hstree-analysis": 2,
            "deepant-analysis": 0,
            "explainability-v2": -1,
        }
        campaign = CampaignView(
            dataset_path="data/source.csv",
            mode="batch",
            selected_groups=[],
            excluded_fields=[],
            selected_models=["hstree", "deepant", "explainability-v2"],
            status="running",
            created_at="2026-04-08T00:00:00+00:00",
            started_at="2026-04-08T00:00:00+00:00",
            updated_at="2026-04-08T00:00:00+00:00",
        )

        progress = self.service.get_campaign_progress(campaign)

        self.assertEqual(
            [item.topic for item in progress.items],
            ["hstree-analysis", "deepant-analysis", "explainability-v2", "source-data"],
        )
        self.assertEqual([item.produced_records for item in progress.items], [3, 1, 0, 3])
        get_topic_offsets.assert_called_once_with(
            ["hstree-analysis", "deepant-analysis", "explainability-v2", "source-data"]
        )

    @patch.object(ProgressService, "_get_topic_offsets")
    def test_get_campaign_progress_ignores_unknown_and_duplicate_models(self, get_topic_offsets) -> None:
        get_topic_offsets.return_value = {
            "source-data": 2,
            "hstree-analysis": 2,
        }
        campaign = CampaignView(
            dataset_path="data/source.csv",
            mode="batch",
            selected_groups=[],
            excluded_fields=[],
            selected_models=["hstree", "unknown", "hstree"],
            status="running",
            created_at="2026-04-08T00:00:00+00:00",
            started_at="2026-04-08T00:00:00+00:00",
            updated_at="2026-04-08T00:00:00+00:00",
        )

        progress = self.service.get_campaign_progress(campaign)

        self.assertEqual(
            [item.topic for item in progress.items],
            ["hstree-analysis", "source-data"],
        )
        get_topic_offsets.assert_called_once_with(["hstree-analysis", "source-data"])

    def test_progress_topics_include_deepant_analysis(self) -> None:
        self.assertIn(("DeepAnT Analysis", "deepant-analysis"), PROGRESS_TOPICS)

    def test_progress_topics_include_mdragpoint_analysis(self) -> None:
        self.assertIn(
            ("mDragStream Point-based Analysis", "mdragstream-point-analysis"),
            PROGRESS_TOPICS,
        )

    @patch.object(ProgressService, "_get_topic_offsets")
    def test_get_campaign_progress_includes_explainability_v2_view(self, get_topic_offsets) -> None:
        get_topic_offsets.return_value = {
            "source-data": 2,
            "hstree-analysis": 2,
            "explainability-v2": 2,
        }
        campaign = CampaignView(
            dataset_path="data/source.csv",
            mode="batch",
            selected_groups=[],
            excluded_fields=[],
            selected_models=["hstree", "explainability-v2"],
            status="running",
            created_at="2026-04-08T00:00:00+00:00",
            started_at="2026-04-08T00:00:00+00:00",
            updated_at="2026-04-08T00:00:00+00:00",
        )

        with patch.object(ProgressService, "_get_consumer_group_lag", return_value=0):
            progress = self.service.get_campaign_progress(campaign)

        self.assertIsNotNone(progress.explainability_v2)
        self.assertTrue(progress.explainability_v2.selected)
        self.assertEqual(progress.explainability_v2.state, "complete")
        self.assertEqual(progress.explainability_v2.upstream_records, 3)
        self.assertEqual(progress.explainability_v2.published_records, 3)
        self.assertEqual(progress.explainability_v2.consumer_lag, 0)



class ExplainabilityV2ProgressTest(unittest.TestCase):
    """OC-11A: cautious v2 state derived from upstream, lag and Docker state."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        project_root = Path(self.temp_dir.name)
        data_dir = project_root / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        (data_dir / "source.csv").write_text("a,b\n1,2\n3,4\n5,6\n", encoding="utf-8")

        self.settings = WebSettings(
            project_root=project_root,
            scripts_dir=project_root / "scripts",
            web_scripts_dir=project_root / "scripts" / "web",
            data_dir=data_dir,
            tmp_dir=project_root / ".tmp",
            host_project_root=project_root,
            frontend_dir=project_root / "frontend" / "askmydasma",
        )
        self.docker_service = MagicMock(spec=DockerService)
        self.docker_service.get_services_status.return_value = _running_v2_service()
        self.service = ProgressService(self.settings, docker_service=self.docker_service)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _campaign(
        self,
        mode: str = "batch",
        status: str = "running",
        models: list[str] | None = None,
    ) -> CampaignView:
        return CampaignView(
            dataset_path="data/source.csv",
            mode=mode,
            selected_groups=[],
            excluded_fields=[],
            selected_models=models if models is not None else ["hstree", "explainability-v2"],
            status=status,
            created_at="2026-04-08T00:00:00+00:00",
            started_at="2026-04-08T00:00:00+00:00",
            updated_at="2026-04-08T00:00:00+00:00",
        )

    @staticmethod
    def _observe(offsets: dict[str, int | None], lag: int | None):
        return (
            patch.object(ProgressService, "_get_topic_offsets", return_value=offsets),
            patch.object(ProgressService, "_get_consumer_group_lag", return_value=lag),
        )

    def test_v2_not_selected_returns_null_state(self) -> None:
        offset_patch, lag_patch = self._observe({}, 0)

        with offset_patch, lag_patch:
            view = self.service.get_explainability_v2_progress(self._campaign(models=["hstree"]))

        self.assertFalse(view.selected)
        self.assertIsNone(view.state)
        self.assertIsNone(view.reason)
        self.assertIsNone(view.upstream_records)
        self.assertIsNone(view.published_records)
        self.assertIsNone(view.consumer_lag)

    def test_v2_absent_campaign_returns_null_state(self) -> None:
        offset_patch, lag_patch = self._observe({}, 0)

        with offset_patch, lag_patch:
            view = self.service.get_explainability_v2_progress(None)

        self.assertFalse(view.selected)
        self.assertIsNone(view.state)

    def test_batch_with_incomplete_upstream_is_processing(self) -> None:
        offset_patch, lag_patch = self._observe(
            {"source-data": 1, "hstree-analysis": 1, "explainability-v2": 1}, 0
        )

        with offset_patch, lag_patch:
            view = self.service.get_explainability_v2_progress(self._campaign())

        self.assertEqual(view.state, "processing")
        self.assertEqual(view.reason, "waiting for HSTree upstream")
        self.assertEqual(view.upstream_records, 2)

    def test_batch_with_positive_lag_is_processing(self) -> None:
        offset_patch, lag_patch = self._observe(
            {"source-data": 2, "hstree-analysis": 2, "explainability-v2": 2}, 17
        )

        with offset_patch, lag_patch:
            view = self.service.get_explainability_v2_progress(self._campaign())

        self.assertEqual(view.state, "processing")
        self.assertEqual(view.reason, "v2 consumer lag: 17")
        self.assertEqual(view.consumer_lag, 17)

    def test_batch_with_unknown_lag_is_never_complete(self) -> None:
        offset_patch, lag_patch = self._observe(
            {"source-data": 2, "hstree-analysis": 2, "explainability-v2": 2}, None
        )

        with offset_patch, lag_patch:
            view = self.service.get_explainability_v2_progress(self._campaign())

        self.assertEqual(view.state, "processing")
        self.assertEqual(view.reason, "unable to confirm v2 consumer lag")

    def test_batch_with_insufficient_v2_outputs_is_processing(self) -> None:
        offset_patch, lag_patch = self._observe(
            {"source-data": 2, "hstree-analysis": 2, "explainability-v2": 1}, 0
        )

        with offset_patch, lag_patch:
            view = self.service.get_explainability_v2_progress(self._campaign())

        self.assertEqual(view.state, "processing")
        self.assertEqual(view.reason, "v2 published results: 2/3")

    def test_batch_with_all_criteria_met_is_complete(self) -> None:
        offset_patch, lag_patch = self._observe(
            {"source-data": 2, "hstree-analysis": 2, "explainability-v2": 2}, 0
        )

        with offset_patch, lag_patch:
            view = self.service.get_explainability_v2_progress(self._campaign())

        self.assertEqual(view.state, "complete")
        self.assertIn("confirmed", view.reason)
        self.assertEqual(view.upstream_records, 3)
        self.assertEqual(view.published_records, 3)
        self.assertEqual(view.consumer_lag, 0)

    def test_dead_v2_service_is_failed_even_with_full_outputs(self) -> None:
        self.docker_service.get_services_status.return_value = [
            ServiceStatusView(
                name="explainability-v2",
                container_name="explainability-v2",
                state="exited",
                health="not_configured",
                status="stopped",
            )
        ]
        offset_patch, lag_patch = self._observe(
            {"source-data": 2, "hstree-analysis": 2, "explainability-v2": 2}, 0
        )

        with offset_patch, lag_patch:
            view = self.service.get_explainability_v2_progress(self._campaign())

        self.assertEqual(view.state, "failed")
        self.assertEqual(view.reason, "v2 worker service stopped")

    def test_absent_v2_service_is_failed_while_campaign_running(self) -> None:
        self.docker_service.get_services_status.return_value = []
        offset_patch, lag_patch = self._observe(
            {"source-data": 2, "hstree-analysis": 2, "explainability-v2": 2}, 0
        )

        with offset_patch, lag_patch:
            view = self.service.get_explainability_v2_progress(self._campaign())

        self.assertEqual(view.state, "failed")

    def test_docker_observation_failure_is_processing_never_complete(self) -> None:
        self.docker_service.get_services_status.side_effect = RuntimeError("docker unreachable")
        offset_patch, lag_patch = self._observe(
            {"source-data": 2, "hstree-analysis": 2, "explainability-v2": 2}, 0
        )

        with offset_patch, lag_patch:
            view = self.service.get_explainability_v2_progress(self._campaign())

        self.assertEqual(view.state, "processing")

    def test_realtime_with_zero_lag_stays_processing(self) -> None:
        offset_patch, lag_patch = self._observe(
            {"source-data": 2, "hstree-analysis": 2, "explainability-v2": 2}, 0
        )

        with offset_patch, lag_patch:
            view = self.service.get_explainability_v2_progress(self._campaign(mode="realtime"))

        self.assertEqual(view.state, "processing")
        self.assertEqual(view.reason, "realtime campaign active: v2 keeps processing")


class ExplainabilityV1ProgressTest(unittest.TestCase):
    """V1-04c: cautious v1 state derived from upstream, lag and Docker state.

    The V1 machine mirrors the V2 one with the V1 stream identities
    (service `explainability`, group `explainability-consumer`, output
    topic `explainability`). Completeness is operational and at least once.
    """

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        project_root = Path(self.temp_dir.name)
        data_dir = project_root / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        (data_dir / "source.csv").write_text("a,b\n1,2\n3,4\n5,6\n", encoding="utf-8")

        self.settings = WebSettings(
            project_root=project_root,
            scripts_dir=project_root / "scripts",
            web_scripts_dir=project_root / "scripts" / "web",
            data_dir=data_dir,
            tmp_dir=project_root / ".tmp",
            host_project_root=project_root,
            frontend_dir=project_root / "frontend" / "askmydasma",
        )
        self.docker_service = MagicMock(spec=DockerService)
        self.docker_service.get_services_status.return_value = [
            ServiceStatusView(
                name="explainability",
                container_name="explainability",
                state="running",
                health="healthy",
                status="healthy",
            )
        ]
        self.service = ProgressService(self.settings, docker_service=self.docker_service)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _campaign(
        self,
        mode: str = "batch",
        status: str = "running",
        models: list[str] | None = None,
    ) -> CampaignView:
        return CampaignView(
            dataset_path="data/source.csv",
            mode=mode,
            selected_groups=[],
            excluded_fields=[],
            selected_models=models if models is not None else ["hstree", "explainability"],
            status=status,
            created_at="2026-04-08T00:00:00+00:00",
            started_at="2026-04-08T00:00:00+00:00",
            updated_at="2026-04-08T00:00:00+00:00",
        )

    @staticmethod
    def _observe(offsets: dict[str, int | None], lag: int | None):
        return (
            patch.object(ProgressService, "_get_topic_offsets", return_value=offsets),
            patch.object(ProgressService, "_get_consumer_group_lag", return_value=lag),
        )

    def test_v1_not_selected_returns_null_state(self) -> None:
        offset_patch, lag_patch = self._observe({}, 0)

        with offset_patch, lag_patch:
            view = self.service.get_explainability_v1_progress(self._campaign(models=["hstree"]))

        self.assertFalse(view.selected)
        self.assertIsNone(view.state)
        self.assertIsNone(view.reason)
        self.assertIsNone(view.upstream_records)
        self.assertIsNone(view.published_records)
        self.assertIsNone(view.consumer_lag)

    def test_v1_absent_campaign_returns_null_state(self) -> None:
        offset_patch, lag_patch = self._observe({}, 0)

        with offset_patch, lag_patch:
            view = self.service.get_explainability_v1_progress(None)

        self.assertFalse(view.selected)
        self.assertIsNone(view.state)

    def test_batch_with_incomplete_upstream_is_processing(self) -> None:
        offset_patch, lag_patch = self._observe(
            {"source-data": 1, "hstree-analysis": 1, "explainability": 1}, 0
        )

        with offset_patch, lag_patch:
            view = self.service.get_explainability_v1_progress(self._campaign())

        self.assertEqual(view.state, "processing")
        self.assertEqual(view.reason, "waiting for HSTree upstream")
        self.assertEqual(view.upstream_records, 2)

    def test_batch_with_positive_lag_is_processing(self) -> None:
        offset_patch, lag_patch = self._observe(
            {"source-data": 2, "hstree-analysis": 2, "explainability": 2}, 17
        )

        with offset_patch, lag_patch:
            view = self.service.get_explainability_v1_progress(self._campaign())

        self.assertEqual(view.state, "processing")
        self.assertEqual(view.reason, "v1 consumer lag: 17")
        self.assertEqual(view.consumer_lag, 17)

    def test_batch_with_unknown_lag_is_never_complete(self) -> None:
        offset_patch, lag_patch = self._observe(
            {"source-data": 2, "hstree-analysis": 2, "explainability": 2}, None
        )

        with offset_patch, lag_patch:
            view = self.service.get_explainability_v1_progress(self._campaign())

        self.assertEqual(view.state, "processing")
        self.assertEqual(view.reason, "unable to confirm v1 consumer lag")

    def test_batch_with_insufficient_v1_outputs_is_processing(self) -> None:
        offset_patch, lag_patch = self._observe(
            {"source-data": 2, "hstree-analysis": 2, "explainability": 1}, 0
        )

        with offset_patch, lag_patch:
            view = self.service.get_explainability_v1_progress(self._campaign())

        self.assertEqual(view.state, "processing")
        self.assertEqual(view.reason, "v1 published results: 2/3")

    def test_batch_with_all_criteria_met_is_complete(self) -> None:
        offset_patch, lag_patch = self._observe(
            {"source-data": 2, "hstree-analysis": 2, "explainability": 2}, 0
        )

        with offset_patch, lag_patch:
            view = self.service.get_explainability_v1_progress(self._campaign())

        self.assertEqual(view.state, "complete")
        self.assertIn("confirmed", view.reason)
        self.assertEqual(view.upstream_records, 3)
        self.assertEqual(view.published_records, 3)
        self.assertEqual(view.consumer_lag, 0)

    def test_dead_v1_service_is_failed_even_with_full_outputs(self) -> None:
        self.docker_service.get_services_status.return_value = [
            ServiceStatusView(
                name="explainability",
                container_name="explainability",
                state="exited",
                health="not_configured",
                status="stopped",
            )
        ]
        offset_patch, lag_patch = self._observe(
            {"source-data": 2, "hstree-analysis": 2, "explainability": 2}, 0
        )

        with offset_patch, lag_patch:
            view = self.service.get_explainability_v1_progress(self._campaign())

        self.assertEqual(view.state, "failed")
        self.assertEqual(view.reason, "v1 worker service stopped")

    def test_absent_v1_service_is_failed_while_campaign_running(self) -> None:
        self.docker_service.get_services_status.return_value = []
        offset_patch, lag_patch = self._observe(
            {"source-data": 2, "hstree-analysis": 2, "explainability": 2}, 0
        )

        with offset_patch, lag_patch:
            view = self.service.get_explainability_v1_progress(self._campaign())

        self.assertEqual(view.state, "failed")

    def test_docker_observation_failure_is_processing_never_complete(self) -> None:
        self.docker_service.get_services_status.side_effect = RuntimeError("docker unreachable")
        offset_patch, lag_patch = self._observe(
            {"source-data": 2, "hstree-analysis": 2, "explainability": 2}, 0
        )

        with offset_patch, lag_patch:
            view = self.service.get_explainability_v1_progress(self._campaign())

        self.assertEqual(view.state, "processing")
        self.assertEqual(view.reason, "unable to confirm v1 worker service state")

    def test_realtime_with_zero_lag_stays_processing(self) -> None:
        offset_patch, lag_patch = self._observe(
            {"source-data": 2, "hstree-analysis": 2, "explainability": 2}, 0
        )

        with offset_patch, lag_patch:
            view = self.service.get_explainability_v1_progress(self._campaign(mode="realtime"))

        self.assertEqual(view.state, "processing")
        self.assertEqual(view.reason, "realtime campaign active: v1 keeps processing")

    def test_campaign_progress_includes_the_v1_view(self) -> None:
        with patch.object(
            ProgressService,
            "_get_topic_offsets",
            return_value={"source-data": 2, "hstree-analysis": 2, "explainability": 2},
        ), patch.object(ProgressService, "_get_consumer_group_lag", return_value=0):
            progress = self.service.get_campaign_progress(self._campaign())

        self.assertIsNotNone(progress.explainability_v1)
        self.assertTrue(progress.explainability_v1.selected)
        self.assertEqual(progress.explainability_v1.state, "complete")

    def test_campaign_progress_without_campaign_keeps_an_empty_v1_view(self) -> None:
        progress = self.service.get_campaign_progress(None)
        self.assertFalse(progress.explainability_v1.selected)
        self.assertIsNone(progress.explainability_v1.state)


class ConsumerGroupLagParserTest(unittest.TestCase):
    """The kafka-consumer-groups.sh parser tolerates ambiguous output."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        project_root = Path(self.temp_dir.name)
        self.settings = WebSettings(
            project_root=project_root,
            scripts_dir=project_root / "scripts",
            web_scripts_dir=project_root / "scripts" / "web",
            data_dir=project_root / "data",
            tmp_dir=project_root / ".tmp",
            host_project_root=project_root,
            frontend_dir=project_root / "frontend" / "askmydasma",
        )
        self.service = ProgressService(self.settings, docker_service=MagicMock(spec=DockerService))

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    @staticmethod
    def _describe_output(rows: list[str]) -> str:
        header = (
            "GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG "
            "CONSUMER-ID HOST CLIENT-ID"
        )
        return "\n".join([header, *rows]) + "\n"

    def test_sums_partition_lags(self) -> None:
        output = self._describe_output(
            [
                "explainability-v2-consumer hstree-analysis 0 10 15 5 consumer-1 host-1 client-1",
                "explainability-v2-consumer hstree-analysis 1 20 22 2 consumer-2 host-2 client-2",
            ]
        )
        with patch.object(ComposeRunner, "exec") as exec_mock:
            exec_mock.return_value = MagicMock(stdout=output)
            lag = self.service._get_consumer_group_lag(
                EXPLAINABILITY_V2_CONSUMER_GROUP, EXPLAINABILITY_V2_UPSTREAM_TOPIC
            )
        self.assertEqual(lag, 7)

    def test_tolerates_rows_without_active_member_offset(self) -> None:
        output = self._describe_output(["explainability-v2-consumer hstree-analysis 0 - 15 - - - -"])
        with patch.object(ComposeRunner, "exec") as exec_mock:
            exec_mock.return_value = MagicMock(stdout=output)
            lag = self.service._get_consumer_group_lag(
                EXPLAINABILITY_V2_CONSUMER_GROUP, EXPLAINABILITY_V2_UPSTREAM_TOPIC
            )
        # Missing offset information is ambiguous: never a zero lag.
        self.assertIsNone(lag)

    def test_returns_none_without_header(self) -> None:
        with patch.object(ComposeRunner, "exec") as exec_mock:
            exec_mock.return_value = MagicMock(stdout="Warning: Consumer group 'x' does not exist.\n")
            lag = self.service._get_consumer_group_lag(
                EXPLAINABILITY_V2_CONSUMER_GROUP, EXPLAINABILITY_V2_UPSTREAM_TOPIC
            )
        self.assertIsNone(lag)

    def test_returns_none_when_command_fails(self) -> None:
        with patch.object(
            ComposeRunner, "exec", side_effect=ComposeError(["cmd"], 1, "boom")
        ):
            lag = self.service._get_consumer_group_lag(
                EXPLAINABILITY_V2_CONSUMER_GROUP, EXPLAINABILITY_V2_UPSTREAM_TOPIC
            )
        self.assertIsNone(lag)

    def test_topic_with_no_records_returns_none(self) -> None:
        # Header present, no data rows: the topic does not appear explicitly,
        # so the lag is unknown (never a fabricated zero).
        output = self._describe_output([])
        with patch.object(ComposeRunner, "exec") as exec_mock:
            exec_mock.return_value = MagicMock(stdout=output)
            lag = self.service._get_consumer_group_lag(
                EXPLAINABILITY_V2_CONSUMER_GROUP, EXPLAINABILITY_V2_UPSTREAM_TOPIC
            )
        self.assertIsNone(lag)


class ConsumerGroupLagKafka39FormatTest(unittest.TestCase):
    """OC-12R phase A: the real Kafka 3.9 output has no --topic flag.

    `kafka-consumer-groups.sh --describe --group G --topic T` is refused by
    Kafka 3.9 ("Option [topic] can't be used with option [describe]"), so the
    reader must describe the whole group and filter rows on the TOPIC column.
    """

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        project_root = Path(self.temp_dir.name)
        self.settings = WebSettings(
            project_root=project_root,
            scripts_dir=project_root / "scripts",
            web_scripts_dir=project_root / "scripts" / "web",
            data_dir=project_root / "data",
            tmp_dir=project_root / ".tmp",
            host_project_root=project_root,
            frontend_dir=project_root / "frontend" / "askmydasma",
        )
        self.service = ProgressService(self.settings, docker_service=MagicMock(spec=DockerService))

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _exec_output(self, stdout: str):
        return patch.object(ComposeRunner, "exec", return_value=MagicMock(stdout=stdout))

    def test_command_does_not_use_topic_flag(self) -> None:
        captured = {}

        def fake_exec(service, *cmd, **kwargs):
            captured["cmd"] = cmd
            return MagicMock(stdout="")

        with patch.object(ComposeRunner, "exec", side_effect=fake_exec):
            self.service._get_consumer_group_lag("explainability-v2-consumer", "hstree-analysis")

        script = captured["cmd"][-1]
        self.assertIn("--describe", script)
        self.assertIn("--group", script)
        self.assertNotIn("--topic", script)

    def test_filters_rows_on_the_topic_column(self) -> None:
        # Real Kafka 3.9 describe layout (9 columns): other topics must be
        # ignored, partitions of the requested topic summed.
        stdout = (
            "GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID\n"
            "g other-topic 0 500 900 400 c h cl\n"
            "g hstree-analysis 0 10 15 5 c h cl\n"
            "g hstree-analysis 1 20 22 2 c h cl\n"
        )
        with self._exec_output(stdout):
            lag = self.service._get_consumer_group_lag("g", "hstree-analysis")
        self.assertEqual(lag, 7)

    def test_missing_topic_rows_returns_none_never_zero(self) -> None:
        stdout = (
            "GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID\n"
            "g other-topic 0 500 900 400 c h cl\n"
        )
        with self._exec_output(stdout):
            lag = self.service._get_consumer_group_lag("g", "hstree-analysis")
        # The requested topic is absent: the lag is unknown, never zero.
        self.assertIsNone(lag)

    def test_dash_lag_returns_none(self) -> None:
        stdout = (
            "GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID\n"
            "g hstree-analysis 0 - 15 - - - -\n"
        )
        with self._exec_output(stdout):
            lag = self.service._get_consumer_group_lag("g", "hstree-analysis")
        self.assertIsNone(lag)

    def test_zero_lag_requires_an_explicit_numeric_zero(self) -> None:
        stdout = (
            "GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID\n"
            "g hstree-analysis 0 110 110 0 c h cl\n"
        )
        with self._exec_output(stdout):
            lag = self.service._get_consumer_group_lag("g", "hstree-analysis")
        self.assertEqual(lag, 0)

    def test_header_with_lag_but_without_topic_returns_none(self) -> None:
        # OC-12R.1: an ambiguous header (LAG present, TOPIC missing) must be
        # rejected instead of indexing a wrong column.
        stdout = (
            "GROUP PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID\n"
            "g 0 110 110 0 c h cl\n"
        )
        with self._exec_output(stdout):
            lag = self.service._get_consumer_group_lag("g", "hstree-analysis")
        self.assertIsNone(lag)
