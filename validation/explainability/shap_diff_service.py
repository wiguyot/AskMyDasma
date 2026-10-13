#!/usr/bin/env python3
"""Kafka adapter for the precomputed V1/V2 SHAP comparison stream.

The comparison logic lives in :mod:`shap_diff` and remains independently
testable. This adapter is intentionally disabled by default in Compose.
"""

from __future__ import annotations

import json
import os
import signal
import sys
import time
from typing import Any, Dict

from confluent_kafka import Consumer, KafkaError, Producer

from validation.explainability.shap_diff import (
    ComparisonBucket,
    ShapDiffError,
    build_emission_records,
    process_arrival,
    strict_identity,
)


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


def _consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": _env("BOOTSTRAP_SERVERS", "kafka1:9092"),
        "group.id": _env("SHAP_DIFF_GROUP_ID", "explainability-shap-diff-consumer"),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })


def _emit(producer: Producer, topic: str, row: Dict[str, Any]) -> None:
    producer.produce(topic=topic, key=row["Group"], value=json.dumps(row, ensure_ascii=False))
    producer.poll(0)


def main() -> None:
    if _env("DASMA_ENABLED", "0") == "0":
        print("[explainability-shap-diff] disabled", flush=True)
        signal.pause()
        return
    topic_v1 = _env("SHAP_DIFF_V1_TOPIC", "explainability-viz")
    topic_v2 = _env("SHAP_DIFF_V2_TOPIC", "explainability-v2-viz")
    destination = _env("SHAP_DIFF_DEST_TOPIC", "explainability-shap-diff")
    consumer = _consumer()
    producer = Producer({
        "bootstrap.servers": _env("BOOTSTRAP_SERVERS", "kafka1:9092"),
        "enable.idempotence": True,
        "acks": "all",
    })
    consumer.subscribe([topic_v1, topic_v2])
    pending: Dict[tuple[str, str], ComparisonBucket] = {}
    pending_messages: Dict[tuple[str, str], Dict[str, list[Any]]] = {}
    running = True

    def stop(_signal, _frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    try:
        while running:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(str(message.error()))
            try:
                record = json.loads(message.value())
                strict_identity(record)
                side = "v1" if message.topic() == topic_v1 else "v2"
                bucket, reason = process_arrival(pending, side, record)
                key = (bucket.group, bucket.source_event_id)
                if reason is None:
                    # A complete bucket can receive an identical at-least-once
                    # redelivery after its original inputs were committed. No
                    # new comparison is needed, but this duplicate offset must
                    # still be acknowledged so it cannot poison the consumer.
                    if bucket.v1 is not None and bucket.v2 is not None and bucket.last_signature is not None:
                        consumer.commit(message=message, asynchronous=False)
                    else:
                        pending_messages.setdefault(key, {}).setdefault(side, []).append(message)
                    continue
                pending_messages.setdefault(key, {}).setdefault(side, []).append(message)
                if bucket.v1 is not None and bucket.v2 is not None:
                    for row in build_emission_records(bucket.v1, bucket.v2):
                        _emit(producer, destination, row.as_record())
                    if producer.flush(30) != 0:
                        raise RuntimeError("SHAP comparison delivery did not drain")
                    for source_messages in pending_messages.pop(key).values():
                        for source_message in source_messages:
                            consumer.commit(message=source_message, asynchronous=False)
            except (ValueError, ShapDiffError, json.JSONDecodeError) as error:
                print(f"[explainability-shap-diff] rejected message: {error}", file=sys.stderr, flush=True)
                consumer.commit(message=message, asynchronous=False)
    finally:
        producer.flush(10)
        consumer.close()


if __name__ == "__main__":
    main()
