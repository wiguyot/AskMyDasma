"""Service for reading available datasets."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from ..dataset_presets import DATASET_PRESETS
from ..models import DatasetPreviewView, DatasetTimeRangeView, DatasetsView
from ..settings import WebSettings


TIME_COLUMN_CANDIDATES = [
    "timestamp",
    "Timestamp",
    "time",
    "Time",
    "datetime",
    "Datetime",
    "date",
    "Date",
    "LabviewTime",
]
DATASET_PREVIEW_MAX_ROWS = 10


class DatasetService:
    """Encapsulate access to the dataset list."""

    def __init__(self, settings: WebSettings):
        self.settings = settings

    def list_datasets(self) -> DatasetsView:
        data_dir = self.settings.data_dir
        if not data_dir.is_dir():
            datasets: list[str] = []
        else:
            datasets = sorted(
                str(p.relative_to(self.settings.project_root))
                for p in data_dir.iterdir()
                if p.suffix == ".csv" and p.is_file()
            )
        presets = {
            dataset_path: DATASET_PRESETS[dataset_path]
            for dataset_path in datasets
            if dataset_path in DATASET_PRESETS
        }
        return DatasetsView(datasets=datasets, presets=presets)

    def get_dataset_preset(self, dataset_path: str):
        return DATASET_PRESETS.get(dataset_path)

    def ensure_dataset_exists(self, dataset_path: str) -> None:
        if dataset_path not in self.list_datasets().datasets:
            raise FileNotFoundError(dataset_path)

    def _resolve_dataset_path(self, dataset_path: str) -> Path:
        path = Path(dataset_path)
        if path.is_absolute():
            return path
        return self.settings.project_root / dataset_path

    def get_dataset_time_range(self, dataset_path: str) -> DatasetTimeRangeView:
        self.ensure_dataset_exists(dataset_path)
        csv_path = self._resolve_dataset_path(dataset_path)
        dataframe = pd.read_csv(csv_path, low_memory=False)
        row_count = int(len(dataframe.index))

        time_column = next((name for name in TIME_COLUMN_CANDIDATES if name in dataframe.columns), None)
        if time_column is None:
            return DatasetTimeRangeView(dataset_path=dataset_path, row_count=row_count, status="not_detected")

        parsed = pd.to_datetime(dataframe[time_column], errors="coerce")
        valid = parsed.dropna()
        if valid.empty:
            return DatasetTimeRangeView(
                dataset_path=dataset_path,
                row_count=row_count,
                time_column=time_column,
                status="unrecognized_format",
            )

        start = valid.iloc[0].to_pydatetime()
        end = valid.iloc[-1].to_pydatetime()
        duration_seconds = int((end - start).total_seconds())

        return DatasetTimeRangeView(
            dataset_path=dataset_path,
            row_count=row_count,
            time_column=time_column,
            start=start.isoformat(sep=" ", timespec="seconds"),
            end=end.isoformat(sep=" ", timespec="seconds"),
            duration_seconds=duration_seconds,
            status="detected",
        )

    def get_dataset_preview(self, dataset_path: str) -> DatasetPreviewView:
        self.ensure_dataset_exists(dataset_path)
        csv_path = self._resolve_dataset_path(dataset_path)

        header_dataframe = pd.read_csv(csv_path, nrows=0, low_memory=False)
        columns = [str(column) for column in header_dataframe.columns]
        if not columns:
            return DatasetPreviewView(
                dataset_path=dataset_path,
                max_rows=DATASET_PREVIEW_MAX_ROWS,
                status="empty",
            )

        sample_dataframe = pd.read_csv(csv_path, nrows=DATASET_PREVIEW_MAX_ROWS, low_memory=False)
        if sample_dataframe.empty:
            return DatasetPreviewView(
                dataset_path=dataset_path,
                columns=columns,
                max_rows=DATASET_PREVIEW_MAX_ROWS,
                status="empty",
            )

        sample_rows: list[dict[str, str | None]] = []
        for _, row in sample_dataframe.iterrows():
            row_dict = row.to_dict()
            sample_rows.append({column: self._to_preview_value(row_dict.get(column)) for column in columns})

        return DatasetPreviewView(
            dataset_path=dataset_path,
            columns=columns,
            sample_rows=sample_rows,
            max_rows=DATASET_PREVIEW_MAX_ROWS,
            status="ok",
        )

    @staticmethod
    def _to_preview_value(value: Any) -> str | None:
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except Exception:
            pass
        return str(value)
