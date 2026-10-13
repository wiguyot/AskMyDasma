# Avro + JSON Kafka producer (`producer.py`)

This module reads a CSV file and publishes each row to Kafka in **Avro** format (through Schema Registry), with an optional **JSON copy** to a `*-viz` topic for Telegraf/Influx ingestion.

> Important: the JSON copy uses **sanitized Avro field names** (Avro-compatible), not necessarily the original CSV headers.

## Role in the project

- Quickly inject `source-data` data from a CSV file into Kafka.
- Provide a stable Avro format for analysis services (`hstree-analysis`, `iforest-analysis`, `explainability`).
- Optionally provide a JSON stream for dashboards through Telegraf/Influx (`*-viz`).

## Inputs / Outputs

### Input

- A CSV file (default: `source.csv`).
- The Kafka cluster (through `BOOTSTRAP_SERVERS`).
- Schema Registry (through `SCHEMA_REGISTRY_URL`).

### Outputs

- Avro topic (default: `source-data`).
- Optional visualization JSON topic (default: `source-data-viz`).

## How it works

1. Load the CSV file with `pandas`.
2. Keep only the groups listed in `DASMA_SELECTED_GROUPS`, when the list is not empty.
3. Check or build a `timestamp` column (string format).
4. Check or build a `source_event_id` column, used as the exact join key by the consensus service.
5. Remove from the DataFrame the columns listed in `DASMA_EXCLUDED_FIELDS`, except protected metadata required by the pipeline (`source_event_id`, `timestamp`, `Group`, etc.).
6. Normalize column names into valid Avro names, with the mapping kept in memory.
7. Infer an Avro schema from pandas types, then automatically register it in Schema Registry.
8. Produce each row in Avro on the main topic.
9. If enabled, produce a JSON copy on the `*-viz` topic.

## Environment variables

### Kafka connection

- `BOOTSTRAP_SERVERS`: for example `kafka1:9092,kafka2:9092,kafka3:9092`.
  - Default in this script: `kafka1:9092`.

### Schema Registry

- `SCHEMA_REGISTRY_URL`: for example `http://schema-registry:8081`.
- `SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO`: for example `user:password` if auth is enabled.

### Topics

- `TOPIC_NAME`: destination Avro topic (default: `source-data`).
- `TOPIC_NAME_VIZ`: destination JSON topic (default: `source-data-viz`).
- `PRODUCE_JSON_COPY`: `true|false` (default: `true`).

### CSV file

- `DATA_SOURCE_FILE`: CSV file path (default in code: `data/source.csv`).
- `DASMA_SELECTED_GROUPS`: optional CSV list of `Group`/`group` values to keep before Kafka publication.
  - Empty means all groups.
  - If set, the CSV must contain a `Group` or `group` column and the selection must match at least one row.
- `DASMA_EXCLUDED_FIELDS`: CSV list of columns to remove from the source flow before Kafka publication.
  - Technical fields required by the pipeline (`source_event_id`, `timestamp`, `Group`, `group`, etc.) are deliberately preserved even if they appear in this list.
  - In the main Compose file, this variable is passed through unchanged to the `producer` service.

### Source event key

- `source_event_id` is generated as `event-000000000001`, `event-000000000002`, ... after group filtering when the CSV does not already provide it.
- If the CSV already contains `source_event_id`, the producer preserves it and casts it to string.
- This key is included in both `source-data` and `source-data-viz`. It lets downstream models and the consensus service distinguish several source rows that share the same display `timestamp` second under `realtime-x5/x20/x100`.

## Kafka properties

The Avro producer and optional JSON producer are configured in **idempotent** mode:

- `enable.idempotence=true`
- `acks=all`
- `max.in.flight.requests.per.connection=1`

This **reduces duplicates** in case of retries, but there is **no** end-to-end Exactly-Once guarantee without transactions and matching guarantees on the consumer side.

## Limitations / watch points

- **Missing values (`NaN`/`None`)**: the generated schema uses non-nullable primitive types (`long`, `double`, ...). If the CSV contains missing values, Avro serialization may fail because `['null', ...]` unions are not implemented.
- **Schema evolution**: auto-registration works, but acceptance of schema changes depends on the compatibility rules configured in Schema Registry.
- **Column names**: CSV names are sanitized, with invalid characters replaced. The JSON copy reuses those sanitized names.
- **Excluded columns**: columns removed through `DASMA_EXCLUDED_FIELDS` are no longer published in `source-data` or `source-data-viz`, except for protected metadata.
- **Selected groups**: group filtering is applied before schema inference, Avro publication and the JSON visualization copy, so `source-data` and `source-data-viz` share the same filtered rows.
- **Analyzers and observability**: because exclusion is applied directly in the producer, Grafana InfluxDB dashboards should no longer see a genuinely excluded feature reappear in fresh pipeline data.

## Execution examples

### In Docker Compose

```bash
BOOTSTRAP_SERVERS="kafka1:9092,kafka2:9092,kafka3:9092" \
SCHEMA_REGISTRY_URL="http://schema-registry:8081" \
TOPIC_NAME="source-data" \
DATA_SOURCE_FILE="data/source.csv" \
python producer.py
```

### Disable the JSON copy

```bash
PRODUCE_JSON_COPY=false python producer.py
```

---

Documented file: `producer/producer.py`.
