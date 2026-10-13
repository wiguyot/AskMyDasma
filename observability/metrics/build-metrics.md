# `build-metrics` service

## Role

The **`build-metrics`** service periodically publishes simple metrics about Kafka activity in the **dasma-kafka** project.

For a given list of topics, it measures the **latest known offset** (high watermark) and publishes that information as a **JSON** message to a metrics topic.

This service is intentionally *lightweight*: it does not consume the functional flow (`source-data` / analysis / explainability), it only queries Kafka.

## What is it used for?

- Provide an indicator answering “is the topic progressing?” while production is running
- Feed dashboards, through InfluxDB/Grafana via Telegraf, with a progression metric
- Quickly diagnose a production stop or stagnation

## Inputs / Outputs

### Input

- Connection to the Kafka cluster through `BOOTSTRAP_SERVERS`
- The list of topics to measure through `METRICS_TOPICS`

The script **does not read** messages. It queries Kafka metadata and the watermark.

### Output

- Publication of a **JSON message** on the metrics topic, `topic-metrics` by default, configurable through `METRICS_TOPIC_NAME`

Example JSON payload, simplified schema:

```json
{
  "timestamp": "2026-02-18T13:00:00Z",
  "source-data": 123456,
  "hstree-analysis": 123400,
  "iforest-analysis": 120001,
  "explainability": 119900
}
```

- Each topic field contains the value **high watermark - 1**, that is the latest offset.
- If a topic is empty, the value may be `-1`.

## How it works

The service loops as follows:

1. wait until Kafka is reachable, with at least `MIN_BROKERS` visible brokers
2. for each requested topic, fetch the high watermark of **partition 0**
3. build a timestamped JSON payload
4. publish that JSON to `METRICS_TOPIC_NAME`
5. sleep for `TEMPORISATION` seconds, then repeat

## Environment variables

### Kafka connection

- `BOOTSTRAP_SERVERS`: Kafka brokers, for example `kafka1:9092,kafka2:9092,kafka3:9092`
  - Default in this script: `kafka1:9092`
- `MIN_BROKERS`: minimum number of visible brokers before starting, default `1`

### Measured topics / output topic

- `METRICS_TOPICS`: comma-separated list
  - Example: `source-data,hstree-analysis,iforest-analysis,explainability,topic-metrics`
- `METRICS_TOPIC_NAME`: Kafka topic where the JSON payload is published, default `topic-metrics`

### Frequency

- `TEMPORISATION`: delay between two measurements in seconds, default `5`

## Limitations / interpretation

These limitations are **important** if you want to interpret the metric correctly.

- The service measures **only partition 0** of each topic, that is the high watermark of `[topic][0]`
  - If a topic has multiple partitions, the published metric is **not** the global sum of the topic
- The published value is an **offset** (`high - 1`), not an exact message counter
  - With retention, deletion, or compaction, the offset remains a progression indicator, not a volume
- If a topic does not exist yet or metadata retrieval fails, the field is **absent** from the JSON, not set to `0`
- `MIN_BROKERS` is used only to wait for a reachable cluster with at least `N` visible brokers. It is not an RF or partition validation mechanism

## Usage advice

- On a multi-broker cluster, configure `BOOTSTRAP_SERVERS` with **multiple** brokers to reduce single points of failure
- If you have multi-partition topics and want a global metric, the script must evolve to sum offsets across all partitions

---

Documented file: `observability/metrics/build-metrics.py`.
