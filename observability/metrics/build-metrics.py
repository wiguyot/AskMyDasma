#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build-metrics.py

This script periodically builds metrics for a list of Kafka topics and publishes
them to a dedicated `topic-metrics` topic.

Behavior:

- Every TEMPORISATION seconds (environment variable, in seconds):
  - Reads the list of topics to monitor from METRICS_TOPICS
    (ex: "source-data,hstree-analysis,iforest-analysis").
  - For each topic, if it exists and has at least one partition:
      - Fetches the high watermark of partition 0 (last offset + 1).
      - Derives the last offset (high - 1, or -1 if there are no messages).
  - Builds a JSON document shaped like:
      {
        "timestamp": "2025-11-24T17:00:00Z",
        "source-data": 1234,
        "hstree-analysis": 5678,
        "hstree-analysis-viz": 5677
      }
  - Publishes this JSON to the `topic-metrics` topic, or METRICS_TOPIC_NAME.

Limitations / interpretation:

- This script measures **only partition 0** for each topic ([topic][0] high watermark).
  If a topic has multiple partitions, the published metric is **not** the global topic sum.
- The published value is an **offset** (high-1): it is **not** an exact message counter.
  With retention/deletion/compaction, the offset remains a progress indicator, not a volume.
- If a topic does not exist yet or has a metadata error, the field is **absent** from JSON, not set to 0.
- `MIN_BROKERS` is only used to wait for a reachable cluster with at least N visible brokers.
  It does not validate replication factors or partition counts.

Environment variables:

- BOOTSTRAP_SERVERS : broker list (ex: "kafka1:9092,kafka2:9092,kafka3:9092"). Default in this script: "kafka1:9092".
- METRICS_TOPICS    : comma-separated list of topics to monitor.
                      ex: "source-data,hstree-analysis,iforest-analysis"
- METRICS_TOPIC_NAME: topic where metrics are published (default: "topic-metrics")
- TEMPORISATION     : period in seconds between two measurements (default: "10")
- LOG_LEVEL         : "DEBUG" | "INFO" | "WARNING" | "ERROR" | "CRITICAL" (default: "INFO")
"""

import sys
import time
import logging
from typing import Any
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from dasma_kafka.config.env import get_env_float, get_env_int, get_env_str
from dasma_kafka.kafka.admin import make_admin, wait_for_cluster
from dasma_kafka.metrics.helpers import (
    build_metrics_snapshot,
    create_consumer,
    create_producer,
    format_snapshot_summary,
    parse_topics,
    snapshot_to_json,
)


def main():
    bootstrap = get_env_str("BOOTSTRAP_SERVERS", "kafka1:9092")
    topics_env = get_env_str("METRICS_TOPICS", "")
    metrics_topic = get_env_str("METRICS_TOPIC_NAME", "topic-metrics")
    min_brokers = get_env_int("MIN_BROKERS", 1)
    period_s = get_env_float("TEMPORISATION", 10.0)
    log_level_str = get_env_str("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    topics = parse_topics(topics_env)
    if not topics:
        logging.error(
            "No topic to monitor. "
            "Set the METRICS_TOPICS environment variable, ex: 'source-data,hstree-analysis,iforest-analysis'."
        )
        sys.exit(1)

    logging.info(
        "Starting build-metrics with bootstrap_servers=%s, topics=%s, metrics_topic=%s, period=%.1fs",
        bootstrap,
        ",".join(topics),
        metrics_topic,
        period_s,
    )


    # Wait until the Kafka cluster is ready before creating the consumer and producer.
    admin = make_admin(bootstrap)
    wait_for_cluster(admin, target_brokers=min_brokers, sleep_seconds=3.0)

    consumer = create_consumer(bootstrap)
    producer = create_producer(bootstrap)

    last_delivery: dict[str, Any] = {"ok": False, "err": None}

    def delivery_report(err, msg):
        if err is not None:
            last_delivery["ok"] = False
            last_delivery["err"] = err
        else:
            last_delivery["ok"] = True
            last_delivery["err"] = None

    try:
        while True:
            snapshot = build_metrics_snapshot(consumer, topics)

            # If there is no field except timestamp, no topic could be measured.
            if len(snapshot) > 1:
                try:
                    payload = snapshot_to_json(snapshot)
                    last_delivery["ok"] = False
                    last_delivery["err"] = None

                    producer.produce(topic=metrics_topic, key=None, value=payload, callback=delivery_report)

                    # Poll until we get a delivery report (or timeout) so INFO reflects real delivery.
                    deadline = time.time() + 2.0
                    while time.time() < deadline and last_delivery["ok"] is False and last_delivery["err"] is None:
                        producer.poll(0.1)

                    if last_delivery["err"] is not None:
                        logging.warning("Metrics publication failed in '%s': %s", metrics_topic, last_delivery["err"])
                    elif last_delivery["ok"]:
                        summary = format_snapshot_summary(snapshot)
                        logging.info("Metrics published in '%s' (%d topic(s)): %s", metrics_topic, len(snapshot) - 1, summary)
                    else:
                        # No callback received in time; not necessarily a failure, but we can't confirm.
                        logging.info("Metrics produced in '%s' and waiting for ACK (%d topic(s)).", metrics_topic, len(snapshot) - 1)
                    logging.debug("Published in '%s': %s", metrics_topic, payload)
                except BufferError:
                    # Full buffer: drain it a bit and retry on the next iteration.
                    logging.warning("Producer buffer full, metric not published in this iteration.")
                    producer.poll(0.5)
                except Exception as e:
                    logging.warning("Error while publishing the metric in '%s': %s", metrics_topic, e)
            else:
                logging.debug("No metric to publish because no topic was measured.")

            time.sleep(period_s)

    except KeyboardInterrupt:
        logging.info("Interrupted by user, stopping build-metrics...")
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        try:
            producer.flush(5.0)
        except Exception:
            pass


if __name__ == "__main__":
    main()
