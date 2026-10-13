"""Reusable helpers for init-topic."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dasma_kafka.config.env import get_env_int

if TYPE_CHECKING:
    from confluent_kafka.admin import AdminClient


@dataclass(frozen=True)
class TopicSpec:
    """Topic declaration loaded from environment."""

    name: str
    partitions: int
    rf: int


def configure_logging(level: str) -> None:
    """Configure init-topic logging."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def ensure_topic(admin: Any, name: str, partitions: int, rf: int) -> None:
    """Create a topic if it does not already exist."""
    from confluent_kafka.admin import NewTopic

    metadata = admin.list_topics(timeout=10)
    if name in metadata.topics:
        logging.info("[init-topics] topic %s already exists", name)
        return

    futures = admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=rf)])
    future = futures.get(name)
    try:
        future.result(30)
        logging.info("[init-topics] created topic %s (partitions=%d, rf=%d)", name, partitions, rf)
    except Exception as exc:
        logging.warning("[init-topics] topic create returned for %s: %s", name, exc)


def load_topics_from_env() -> list[TopicSpec]:
    """Load all topic specs from the environment variables used by docker-compose."""

    def spec(name_key: str, part_key: str, rf_key: str, default_name: str) -> TopicSpec | None:
        import os

        name = os.environ.get(name_key, default_name)
        if not name:
            return None
        partitions = get_env_int(part_key, 1)
        rf = get_env_int(rf_key, 1)
        return TopicSpec(name=name, partitions=partitions, rf=rf)

    specs: list[TopicSpec | None] = [
        spec("TOPIC_NAME1", "TOPIC_PARTITIONS1", "TOPIC_RF1", "source-data"),
        spec("TOPIC_NAME9", "TOPIC_PARTITIONS9", "TOPIC_RF9", "source-data-viz"),
        spec("TOPIC_NAME2", "TOPIC_PARTITIONS2", "TOPIC_RF2", "hstree-analysis"),
        spec("TOPIC_NAME4", "TOPIC_PARTITIONS4", "TOPIC_RF4", "hstree-analysis-viz"),
        spec("TOPIC_NAME7", "TOPIC_PARTITIONS7", "TOPIC_RF7", "iforest-analysis"),
        spec("TOPIC_NAME8", "TOPIC_PARTITIONS8", "TOPIC_RF8", "iforest-analysis-viz"),
        spec("TOPIC_NAME10", "TOPIC_PARTITIONS10", "TOPIC_RF10", "kitnet-analysis"),
        spec("TOPIC_NAME11", "TOPIC_PARTITIONS11", "TOPIC_RF11", "kitnet-analysis-viz"),
        spec("TOPIC_NAME12", "TOPIC_PARTITIONS12", "TOPIC_RF12", "consensus-analysis"),
        spec("TOPIC_NAME3", "TOPIC_PARTITIONS3", "TOPIC_RF3", "explainability"),
        spec("TOPIC_NAME6", "TOPIC_PARTITIONS6", "TOPIC_RF6", "explainability-viz"),
        spec("TOPIC_NAME5", "TOPIC_PARTITIONS5", "TOPIC_RF5", "topic-metrics"),
        spec("TOPIC_NAME13", "TOPIC_PARTITIONS13", "TOPIC_RF13", "deepant-analysis"),
        spec("TOPIC_NAME14", "TOPIC_PARTITIONS14", "TOPIC_RF14", "deepant-analysis-viz"),
        spec("TOPIC_NAME15", "TOPIC_PARTITIONS15", "TOPIC_RF15", "mdragstream-point-analysis"),
        spec("TOPIC_NAME16", "TOPIC_PARTITIONS16", "TOPIC_RF16", "mdragstream-point-analysis-viz"),
        spec("TOPIC_NAME17", "TOPIC_PARTITIONS17", "TOPIC_RF17", "explainability-v2"),
        spec("TOPIC_NAME18", "TOPIC_PARTITIONS18", "TOPIC_RF18", "explainability-v2-viz"),
        spec("TOPIC_NAME19", "TOPIC_PARTITIONS19", "TOPIC_RF19", "repeatability-v1"),
        spec("TOPIC_NAME20", "TOPIC_PARTITIONS20", "TOPIC_RF20", "repeatability-v1-viz"),
        spec("TOPIC_NAME21", "TOPIC_PARTITIONS21", "TOPIC_RF21", "repeatability-v2"),
        spec("TOPIC_NAME22", "TOPIC_PARTITIONS22", "TOPIC_RF22", "repeatability-v2-viz"),
        spec("TOPIC_NAME23", "TOPIC_PARTITIONS23", "TOPIC_RF23", "repeatability-diff"),
        spec("TOPIC_NAME24", "TOPIC_PARTITIONS24", "TOPIC_RF24", "reproducibility-diff"),
        spec("TOPIC_NAME25", "TOPIC_PARTITIONS25", "TOPIC_RF25", "reproducibility-summary"),
        spec("TOPIC_NAME26", "TOPIC_PARTITIONS26", "TOPIC_RF26", "explainability-shap-diff"),
    ]

    by_name: dict[str, TopicSpec] = {}
    for topic_spec in specs:
        if topic_spec is None:
            continue
        if topic_spec.name not in by_name:
            by_name[topic_spec.name] = topic_spec

    return list(by_name.values())
