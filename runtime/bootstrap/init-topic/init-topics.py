"""
init-topics.py

Single entry point to initialize all Kafka topics used by dasma-kafka.

What it does:
- waits until the Kafka cluster is operational, with at least N brokers visible
- creates all configured topics, analysis, viz, and metrics, if they are missing

Dependency: `pip install confluent-kafka`

Examples:
  # Inside the Docker network, using service names:
  BOOTSTRAP_SERVERS="kafka1:9092" python init-topics.py

  # On the host, with ports published by docker-compose:
  BOOTSTRAP_SERVERS="localhost:9092" python init-topics.py

Environment variables, `docker-compose init-topic`:
  - `BOOTSTRAP_SERVERS` (default: `localhost:9092`)
  - `MIN_BROKERS` (default: `1`)
  - `LOG_LEVEL` (default: `INFO`)

Topics, name / partitions / RF, are read from these environment variables:
  - `TOPIC_NAME1` / `TOPIC_PARTITIONS1` / `TOPIC_RF1`                    (`source-data`)
  - `TOPIC_NAME9` / `TOPIC_PARTITIONS9` / `TOPIC_RF9`                    (`source-data-viz`)
  - `TOPIC_NAME2` / `TOPIC_PARTITIONS2` / `TOPIC_RF2`                    (`hstree-analysis`)
  - `TOPIC_NAME4` / `TOPIC_PARTITIONS4` / `TOPIC_RF4`                    (`hstree-analysis-viz`)
  - `TOPIC_NAME7` / `TOPIC_PARTITIONS7` / `TOPIC_RF7`                    (`iforest-analysis`)
  - `TOPIC_NAME8` / `TOPIC_PARTITIONS8` / `TOPIC_RF8`                    (`iforest-analysis-viz`)
  - `TOPIC_NAME10` / `TOPIC_PARTITIONS10` / `TOPIC_RF10`                 (`kitnet-analysis`)
  - `TOPIC_NAME11` / `TOPIC_PARTITIONS11` / `TOPIC_RF11`                 (`kitnet-analysis-viz`)
  - `TOPIC_NAME12` / `TOPIC_PARTITIONS12` / `TOPIC_RF12`                 (`consensus-analysis`)
  - `TOPIC_NAME3` / `TOPIC_PARTITIONS3` / `TOPIC_RF3`                    (`explainability`)
  - `TOPIC_NAME6` / `TOPIC_PARTITIONS6` / `TOPIC_RF6`                    (`explainability-viz`)
  - `TOPIC_NAME5` / `TOPIC_PARTITIONS5` / `TOPIC_RF5`                    (`topic-metrics`)
  - `TOPIC_NAME13` / `TOPIC_PARTITIONS13` / `TOPIC_RF13`                 (`deepant-analysis`)
  - `TOPIC_NAME14` / `TOPIC_PARTITIONS14` / `TOPIC_RF14`                 (`deepant-analysis-viz`)

Notes:
- Topic creation is idempotent.
- Readiness waits for `max(MIN_BROKERS, max RF of all topics)`.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# This entry point lives under runtime/bootstrap/init-topic/; parents[3] is the
# repository root on the host and /app in the kafka-tools container.
ROOT_DIR = Path(__file__).resolve().parents[3]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from dasma_kafka.config.env import get_env_float, get_env_int, get_env_str
from dasma_kafka.init_topic.helpers import configure_logging, ensure_topic, load_topics_from_env
from dasma_kafka.kafka.admin import make_admin, preflight_hostname_resolution, wait_for_cluster


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Wait for Kafka and create all dasma-kafka topics (source-data / analysis / viz / explainability / metrics)."
    )
    parser.add_argument(
        "--bootstrap-servers",
        "-b",
        default=get_env_str("BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Comma-separated list of bootstrap servers, for example host1:9092,host2:9092",
    )
    parser.add_argument(
        "--min-brokers",
        type=int,
        default=get_env_int("MIN_BROKERS", 1),
        help="Minimum number of visible brokers before considering the cluster ready (default: 1).",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=get_env_float("SLEEP_SECONDS", 3.0),
        help="Delay between attempts in seconds (default: 3).",
    )
    parser.add_argument(
        "--log-level",
        default=get_env_str("LOG_LEVEL", "INFO"),
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Log level (default: INFO).",
    )

    args = parser.parse_args()
    configure_logging(args.log_level)

    preflight_hostname_resolution(args.bootstrap_servers)

    topics = load_topics_from_env()
    if not topics:
        logging.error("No topics configured in the environment. Nothing to do.")
        return

    max_rf = max(t.rf for t in topics)
    if max_rf > args.min_brokers:
        logging.warning(
            "max RF=%d > min_brokers=%d: waiting for at least %d broker(s) before creating topics.",
            max_rf,
            args.min_brokers,
            max_rf,
        )

    target_brokers = max(args.min_brokers, max_rf)

    admin = make_admin(args.bootstrap_servers)
    wait_for_cluster(admin, target_brokers, args.sleep)

    # Create in a stable order for readability
    for t in sorted(topics, key=lambda x: x.name):
        ensure_topic(admin, t.name, t.partitions, t.rf)

    logging.info("Done.")


if __name__ == "__main__":
    main()
