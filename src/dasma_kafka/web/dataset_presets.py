"""Dataset presets explicitly documented in the Compose variants."""

from __future__ import annotations

from .models import DatasetPresetView


DEFAULT_EXCLUDED_FIELDS = [
    "source_event_id",
    "timestamp",
    "Group",
    "group",
    "Cycle",
    "LabviewTime",
    "time",
    "StepNumber",
    "State (0=OK 1=Warning 2=Error)",
    "BackgroundSubtracted(0/1)",
    "ActionNumber",
]

SOURCE_SELECTED_GROUPS = [
    "1A",
    "2B",
    "3A",
    "4B",
    "IMPBK UV",
    "NETBK",
    "Selek",
    "Sortie CTA",
    "TTHBK",
]

SOURCE_DATASETS = [
    "data/source.csv",
    "data/source-5000.csv",
    "data/source-1000.csv",
    "data/source-1000-clean.csv",
    "data/source-10000.csv",
]


DATASET_PRESETS: dict[str, DatasetPresetView] = {
    "data/component_2022-fixed.csv": DatasetPresetView(
        label="Historical 2022 preset",
        selected_groups=[],
        excluded_fields=[
            "timestamp",
            "Group",
            "group",
            "name",
            "time",
        ],
        source="compose/legacy/docker-compose-components-2022.yml",
    ),
    "data/131_OPPORTUNITY-1000.csv": DatasetPresetView(
        label="OPPORTUNITY preset",
        selected_groups=[],
        excluded_fields=[
            "timestamp",
            "Group",
        ],
        source="compose/legacy/docker-compose-131-OPPORTUNITY.yml",
    ),
}

for dataset_path in SOURCE_DATASETS:
    suffix = dataset_path.removeprefix("data/").removesuffix(".csv")
    DATASET_PRESETS[dataset_path] = DatasetPresetView(
        label=f"Preset {suffix}",
        selected_groups=SOURCE_SELECTED_GROUPS,
        excluded_fields=DEFAULT_EXCLUDED_FIELDS,
        source="docker-compose",
    )
