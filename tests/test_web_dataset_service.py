"""Tests for AskMyDasma dataset time summaries."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.dasma_kafka.web.services.dataset_service import DatasetService
from src.dasma_kafka.web.settings import WebSettings


class DatasetTimeRangeServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        project_root = Path(self.temp_dir.name)
        data_dir = project_root / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        (data_dir / "dated.csv").write_text(
            "timestamp,value\n2024-01-01 10:00:00,1\n2024-01-01 10:00:05,2\n2024-01-01 10:00:10,3\n",
            encoding="utf-8",
        )
        (data_dir / "plain.csv").write_text(
            "value,group\n1,A\n2,B\n3,C\n",
            encoding="utf-8",
        )
        (data_dir / "header_only.csv").write_text(
            "timestamp,value,group\n",
            encoding="utf-8",
        )
        self.settings = WebSettings(
            project_root=project_root,
            scripts_dir=project_root / "scripts",
            web_scripts_dir=project_root / "scripts" / "web",
            data_dir=data_dir,
            tmp_dir=project_root / ".tmp",
            host_project_root=project_root,
            frontend_dir=project_root / "frontend" / "askmydasma",
        )
        self.service = DatasetService(self.settings)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_detects_time_range_from_timestamp_column(self) -> None:
        self.service.list_datasets = lambda: type("obj", (), {"datasets": ["data/dated.csv"]})()

        result = self.service.get_dataset_time_range("data/dated.csv")

        self.assertEqual(result.status, "detected")
        self.assertEqual(result.row_count, 3)
        self.assertEqual(result.time_column, "timestamp")
        self.assertEqual(result.start, "2024-01-01 10:00:00")
        self.assertEqual(result.end, "2024-01-01 10:00:10")
        self.assertEqual(result.duration_seconds, 10)

    def test_returns_not_detected_without_known_time_column(self) -> None:
        self.service.list_datasets = lambda: type("obj", (), {"datasets": ["data/plain.csv"]})()

        result = self.service.get_dataset_time_range("data/plain.csv")

        self.assertEqual(result.status, "not_detected")
        self.assertEqual(result.row_count, 3)
        self.assertIsNone(result.time_column)

    def test_dataset_preview_returns_first_row_and_columns(self) -> None:
        self.service.list_datasets = lambda: type("obj", (), {"datasets": ["data/plain.csv"]})()

        result = self.service.get_dataset_preview("data/plain.csv")

        self.assertEqual(result.status, "ok")
        self.assertEqual(result.columns, ["value", "group"])
        self.assertEqual(result.max_rows, 10)
        self.assertEqual(len(result.sample_rows), 3)
        self.assertEqual(result.sample_rows[0], {"value": "1", "group": "A"})
        self.assertEqual(result.sample_rows[2], {"value": "3", "group": "C"})

    def test_dataset_preview_handles_header_only_csv(self) -> None:
        self.service.list_datasets = lambda: type("obj", (), {"datasets": ["data/header_only.csv"]})()

        result = self.service.get_dataset_preview("data/header_only.csv")

        self.assertEqual(result.status, "empty")
        self.assertEqual(result.columns, ["timestamp", "value", "group"])
        self.assertEqual(result.max_rows, 10)
        self.assertEqual(result.sample_rows, [])
