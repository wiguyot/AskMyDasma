#!/usr/bin/env python3
"""
Kafka Producer - dynamic Avro schema generation
==============================================

Summary
-------
This producer reads a CSV file, infers an Avro schema from its columns and
pandas dtypes, and publishes each row as an Avro record to a Kafka topic.
Optionally, it also publishes a JSON copy to a separate `*-viz` topic for Telegraf and Influx ingestion. JSON keys use the sanitized Avro field names, not necessarily the original CSV headers.
The schema is registered, or evolved, automatically in Schema Registry.

Key properties:
- Idempotent, non-transactional producer. Idempotence reduces duplicates on retries, but there is no end-to-end Exactly-Once without transactions and consumer-side guarantees.
- Supports arbitrary CSV columns: all columns are exported to Kafka.
- Basic type inference: integers -> long, floats -> double, booleans -> boolean, everything else -> string, including timestamps by default.

Limitations:
- No `transactional.id`, so there is no end-to-end Exactly-Once, EOS, semantics.
- Schema evolution depends on Schema Registry compatibility settings. Auto-registering does not guarantee that incompatible changes will be accepted.
- Missing values or nulls: the generated schema uses non-nullable primitive types, such as `double` and `long`. If the CSV contains `NaN` or `None`, serialization may fail unless the schema uses unions, for example `["null", "double"]`. Unions are not implemented here.
"""

import sys
import os
import json
import time
from pathlib import Path
from typing import Dict, Any
from datetime import datetime, timedelta

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from dasma_kafka.config.env import get_env_bool, get_env_float, get_env_str
from dasma_kafka.producer.helpers import (
    apply_group_filter,
    apply_dataframe_exclusions,
    DEFAULT_DATA_SOURCE_FILE,
    DEFAULT_TOPIC,
    DEFAULT_TOPIC_VIZ,
    build_avro_schema_from_dataframe,
    cast_value_to_avro,
    ensure_source_event_id_column,
    ensure_timestamp_column,
    infer_avro_type,
    make_avro_producer,
    make_json_producer,
    parse_excluded_fields,
    parse_selected_groups,
    read_csv_auto,
    resolve_data_source_path,
)


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------

def main() -> None:
    """Entry point for the CSV→Kafka producer.

    Steps:
      1. Read the CSV file into a DataFrame.
      2. Infer an Avro schema from the DataFrame columns and dtypes.
      3. Build a SerializingProducer using this schema.
      4. Iterate over each row and send it as an Avro record to Kafka.
    """
    # The data source can be overridden at runtime, for example from docker-compose, using:
    #   DATA_SOURCE_FILE: "data/131_OPPORTUNITY-1000.csv"
    data_source = get_env_str("DATA_SOURCE_FILE", DEFAULT_DATA_SOURCE_FILE)

    # Optional producer mode
    transformation = get_env_str("DASMA_PRODUCER_TRANSFORMATION", "", strip=True).lower()
    selected_groups = parse_selected_groups(get_env_str("DASMA_SELECTED_GROUPS", "", strip=True))
    excluded_fields = parse_excluded_fields(get_env_str("DASMA_EXCLUDED_FIELDS", "", strip=True))
    wait_for_start = get_env_bool("DASMA_PRODUCER_WAIT_FOR_START", False)
    start_signal_file = Path(
        get_env_str("DASMA_PRODUCER_START_SIGNAL_FILE", "/tmp/dasma-producer-start.signal", strip=True)
    )
    if transformation == "realtime":
        print("DASMA PRODUCER in realtime mode")
        print("warmup: the first 960 records are sent as fast as possible")
        print("then time_delay=1 second")
    elif transformation == "realtime-x5":
        print("DASMA PRODUCER in realtime-x5 mode")
        print("warmup: the first 960 records are sent as fast as possible")
        print("then time_delay=0.2 second, 5 messages per second")
    elif transformation == "realtime-x20":
        print("DASMA PRODUCER in realtime-x20 mode")
        print("warmup: the first 960 records are sent as fast as possible")
        print("then time_delay=0.05 second, 20 messages per second")
    elif transformation == "realtime-x100":
        print("DASMA PRODUCER in realtime-x100 mode")
        print("warmup: the first 960 records are sent as fast as possible")
        print("then time_delay=0.01 second, 100 messages per second")
    elif transformation == "":
        print("DASMA PRODUCER in normal mode")
    else:
        # Any other value keeps normal behavior, but we make it explicit
        print(f"DASMA PRODUCER in normal mode (DASMA_PRODUCER_TRANSFORMATION={transformation})")

    if wait_for_start:
        print(f"Waiting for an explicit start signal at {start_signal_file}")
        while not start_signal_file.exists():
            time.sleep(0.5)
        print("Explicit start signal received, beginning production")

    # Path resolution rules:
    # - If an absolute path is provided, use it as-is.
    # - If a relative path is provided, resolve it from the current working directory first
    #   (common in Docker when /app is the WORKDIR), then from the repository root
    #   (parent of this script directory), and finally from this script directory.
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent
    csv_path, candidates = resolve_data_source_path(data_source, repo_root, script_dir)

    try:
        print(f"Reading file {csv_path}")
        df = read_csv_auto(str(csv_path))
    except Exception as exc:  # pragma: no cover - startup failure
        if not csv_path.exists() and not os.path.isabs(data_source):
            sys.exit(
                "Error reading '{path}': {err}\n"
                "DATA_SOURCE_FILE='{src}' was resolved to '{path}'.\n"
                "Tried (in order):\n- {c0}\n- {c1}\n- {c2}".format(
                    path=csv_path,
                    err=exc,
                    src=data_source,
                    c0=candidates[0],
                    c1=candidates[1],
                    c2=candidates[2],
                )
            )
        sys.exit(f"Error reading '{csv_path}': {exc}")

    if df.columns.empty:
        sys.exit("No columns found in the CSV.")

    try:
        original_rows = len(df)
        df, group_column, filtered_rows = apply_group_filter(df, selected_groups)
    except ValueError as exc:
        sys.exit(str(exc))
    if selected_groups and group_column:
        print(
            "[filters] Applied selected group filter on column "
            f"'{group_column}': {filtered_rows}/{original_rows} rows kept "
            f"({', '.join(selected_groups)})"
        )

    # Timestamp handling
    # ------------------
    # If DATA_TIMESTAMP_NAME is provided, and not empty, it is the name of the column
    # containing event timestamps and it must exist in the CSV.
    # If it is not provided, we generate a synthetic timestamp column, named `timestamp`,
    # starting at now and strictly increasing by 1 microsecond per row.

    env_ts_name = get_env_str("DATA_TIMESTAMP_NAME", "", strip=True)
    try:
        df = ensure_timestamp_column(df, env_ts_name)
    except ValueError as exc:
        sys.exit(str(exc))
    df = ensure_source_event_id_column(df)

    protected_fields = {env_ts_name} if env_ts_name else set()
    df, dropped_columns = apply_dataframe_exclusions(df, excluded_fields, protected_fields=protected_fields)
    if dropped_columns:
        print(
            "[filters] Dropped excluded source columns before Kafka publication: "
            + ", ".join(dropped_columns)
        )
    if excluded_fields:
        preserved_fields = [
            field
            for field in excluded_fields
            if field in {"source_event_id", "timestamp", "Timestamp", "time", "Time", "Group", "group"} or field == env_ts_name
        ]
        if preserved_fields:
            print(
                "[filters] Preserved metadata columns despite exclusion request: "
                + ", ".join(preserved_fields)
            )

    # Infer the Avro schema from the full DataFrame. All columns will be exported.
    # Also obtain a mapping from original column names to sanitized Avro field names.
    avro_schema, name_mapping = build_avro_schema_from_dataframe(df)

    # Timestamp field name, Avro-sanitized, used for realtime restamping
    ts_col_name = env_ts_name if env_ts_name else "timestamp"
    ts_field_name = name_mapping.get(ts_col_name)
    ts_realtime_format = "%Y-%m-%d %H:%M:%S"

    # Topic name is configurable via the environment, default `source-data`.
    topic = get_env_str("TOPIC_NAME", DEFAULT_TOPIC)

    # Optional JSON copy topic, for Telegraf and Influx. Set `PRODUCE_JSON_COPY=false` to disable.
    # Note: the JSON payload uses the sanitized Avro field names, see `name_mapping`, not the original CSV headers.
    viz_topic = get_env_str("TOPIC_NAME_VIZ", DEFAULT_TOPIC_VIZ)
    produce_json_copy = get_env_bool("PRODUCE_JSON_COPY", True)

    avro_producer = make_avro_producer(avro_schema)
    json_producer = make_json_producer() if produce_json_copy else None

    # Pre-compute the Avro type for each column to avoid recomputing dtypes
    # for every row.
    avro_types_by_col: Dict[str, str] = {
        col: infer_avro_type(df[col]) for col in df.columns
    }

    # Optional pacing between messages, controlled by `TIME_DELAY` in seconds.
    # If `TIME_DELAY` is `0` or invalid, no delay is applied.
    time_delay = get_env_float("TIME_DELAY", 0.0)

    # In realtime mode, enforce an explicit post-warmup rhythm, overriding `TIME_DELAY`,
    # but only after a burst warmup: the first 960 messages are sent without any delay.
    realtime_warmup_n = 960
    if transformation == "realtime":
        time_delay = 1.0
    elif transformation == "realtime-x5":
        time_delay = 0.2
    elif transformation == "realtime-x20":
        time_delay = 0.05
    elif transformation == "realtime-x100":
        time_delay = 0.01

    # Iterate row by row: print to stdout and send to Kafka.
    # Use `name=None` so that `itertuples` returns plain tuples instead of
    # namedtuples. This avoids issues with column names that are not valid
    # Python identifiers, for example containing spaces, punctuation, or `+`.
    for idx, row in enumerate(df.itertuples(index=False, name=None), start=1):
        record: Dict[str, Any] = {}
        # Zip together column names and row values so we never rely on
        # attribute access for columns. Values are stored under the sanitized
        # Avro field names defined in `name_mapping`.
        for col, raw_value in zip(df.columns, row):
            avro_type = avro_types_by_col[col]
            field_name = name_mapping[col]
            record[field_name] = cast_value_to_avro(raw_value, avro_type)

        # In realtime mode, replace the `timestamp` feature, or `DATA_TIMESTAMP_NAME`.
        # - Warmup burst, `idx <= realtime_warmup_n`: we shift the series back in time to simulate a stream catching up.
        #   Example with `realtime_warmup_n=960`:
        #     idx=1   -> now - 959s
        #     idx=2   -> now - 958s
        #     ...
        #     idx=960 -> now - 0s
        # - After warmup: timestamp = now.
        # Expected format: `YYYY-MM-DD HH:MM:SS`, for example `2020-12-15 11:14:32`.
        if transformation in {"realtime", "realtime-x5", "realtime-x20", "realtime-x100"} and ts_field_name:
            if idx <= realtime_warmup_n:
                back_seconds = (realtime_warmup_n - idx)
                record[ts_field_name] = (datetime.now() - timedelta(seconds=back_seconds)).strftime(ts_realtime_format)
            else:
                record[ts_field_name] = datetime.now().strftime(ts_realtime_format)

        # Human-readable JSON to stdout, useful for debugging, plus payload for the JSON topic.
        json_payload = json.dumps(record, ensure_ascii=False)
        print(json_payload)

        # 1. Publish Avro to the main topic.
        while True:
            try:
                avro_producer.produce(topic=topic, key=None, value=record)
                break
            except BufferError:
                # If the local buffer is full, give Kafka some time to drain delivery callbacks.
                avro_producer.poll(0.1)

        # 2. Optionally publish a JSON copy to the viz topic.
        if json_producer is not None:
            while True:
                try:
                    json_producer.produce(topic=viz_topic, key=None, value=json_payload)
                    break
                except BufferError:
                    json_producer.poll(0.1)

        # Poll to serve delivery callbacks and progress internal state.
        avro_producer.poll(0)
        if json_producer is not None:
            json_producer.poll(0)

        # Optional pacing to control throughput.
        # - Normal mode: `TIME_DELAY` controls pacing.
        # - Realtime mode: the first `realtime_warmup_n` messages are sent as fast as possible,
        #   then we switch to the configured realtime cadence.
        effective_delay = time_delay
        if transformation in {"realtime", "realtime-x5", "realtime-x20", "realtime-x100"} and idx <= realtime_warmup_n:
            effective_delay = 0.0

        if effective_delay > 0:
            now_hms = datetime.now().strftime("%H:%M:%S")
            print(f"[{now_hms}] waiting before next message: {effective_delay}s")
            time.sleep(effective_delay)

    # Ensure all outstanding messages are delivered before exiting.
    avro_producer.flush(10)
    if json_producer is not None:
        json_producer.flush(10)

    print("--------------")
    print("Finished writing & sending to Kafka")
    print("--------------")


if __name__ == "__main__":
    main()
