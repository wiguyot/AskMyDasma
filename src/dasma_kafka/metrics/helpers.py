"""Reusable helpers for build-metrics."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from confluent_kafka import Consumer, Producer


def parse_topics(env_value: str) -> list[str]:
    """Parse the topic list from METRICS_TOPICS."""
    if not env_value:
        return []
    return [topic.strip() for topic in env_value.split(",") if topic.strip()]


def create_consumer(bootstrap_servers: str, group_id: str = "metrics-builder") -> Consumer:
    """Create the consumer used to query watermarks."""
    from confluent_kafka import Consumer

    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }
    return Consumer(conf)


def create_producer(bootstrap_servers: str) -> Producer:
    """Create the JSON producer used to publish metrics snapshots."""
    from confluent_kafka import Producer

    return Producer({"bootstrap.servers": bootstrap_servers})


def get_last_offset_for_topic(consumer: Consumer, topic: str, timeout: float = 5.0) -> int | None:
    """Return the last offset for partition 0 of a topic."""
    from confluent_kafka import TopicPartition, KafkaException

    try:
        metadata = consumer.list_topics(timeout=timeout)
    except KafkaException as exc:
        logging.warning("Unable to fetch Kafka metadata: %s", exc)
        return None
    except Exception as exc:
        logging.warning("Unexpected error while calling list_topics(): %s", exc)
        return None

    topic_md = metadata.topics.get(topic)
    if topic_md is None:
        logging.debug("Topic '%s' not found in metadata; it may not have been created yet.", topic)
        return None
    if topic_md.error is not None:
        logging.warning("Topic '%s' has a metadata error: %s", topic, topic_md.error)
        return None
    if 0 not in topic_md.partitions:
        logging.warning("Topic '%s' has no partition 0 (partitions=%s).", topic, list(topic_md.partitions.keys()))
        return None

    topic_partition = TopicPartition(topic, 0)
    try:
        _, high = consumer.get_watermark_offsets(topic_partition, timeout=timeout)
    except KafkaException as exc:
        logging.warning("Unable to fetch watermarks for %s[0]: %s", topic, exc)
        return None
    except Exception as exc:
        logging.warning("Unexpected error while calling get_watermark_offsets(%s[0]): %s", topic, exc)
        return None

    if high <= 0:
        return -1
    return high - 1


def build_metrics_snapshot(consumer: Consumer, topics: list[str]) -> dict[str, Any]:
    """Build a metrics snapshot for the configured topics."""
    snapshot: dict[str, Any] = {}
    snapshot["timestamp"] = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

    for topic in topics:
        last = get_last_offset_for_topic(consumer, topic)
        if last is None:
            logging.debug("Topic '%s' ignored because it is missing or in error.", topic)
            continue
        snapshot[topic] = last

    return snapshot


def format_snapshot_summary(snapshot: dict[str, Any], max_items: int = 12) -> str:
    """Format a compact human-readable summary of a snapshot."""
    items = [(key, value) for key, value in snapshot.items() if key != "timestamp"]
    items.sort(key=lambda item: item[0])
    shown = items[:max_items]
    parts = [f"{key}={value}" for key, value in shown]
    if len(items) > max_items:
        parts.append(f"...(+{len(items) - max_items})")
    return ", ".join(parts)


def snapshot_to_json(snapshot: dict[str, Any]) -> str:
    """Serialize a snapshot to JSON."""
    return json.dumps(snapshot, ensure_ascii=False)
