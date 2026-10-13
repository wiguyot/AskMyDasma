# Architecture Schema

This document describes the architecture actually deployed by [`docker-compose.yml`](../../docker-compose.yml).

## Diagram

```text
                                     +--------------------------------------+
                                     |            Docker Network            |
                                     |             `kafka-net`              |
                                     +-----------------+--------------------+
                                                       |
             +------------------+   +------------------+   +------------------+
             |      kafka1      |   |      kafka2      |   |      kafka3      |
             |  Broker (KRaft)  |   |  Broker (KRaft)  |   |  Broker (KRaft)  |
             |  host: 9092      |   |    internal      |   |    internal      |
             +---------+--------+   +---------+--------+   +---------+--------+
                       \                |                    /
                        \               |                   /
                         +--------------v------------------+
                         |        schema-registry          |
                         |          host: 8081             |
                         +--------------+------------------+
                                        |
                                        | topics created by `init-topic`
                                        v
                          +-------------------------------+
                          |           init-topic          |
                          | creates 26 topics (P=1, RF=3)|
                          +---------------+---------------+
                                          |
                                          v
                    +---------------------+----------------------+
                    |                                            |
          +---------v----------+                      +----------v-----------+
          |      producer      |                      |     build-metrics    |
          | CSV -> source-data |                      |   offset snapshots   |
          | + source-data-viz  |                      |   -> topic-metrics   |
          +---------+----------+                      +----------+-----------+
                    |                                            |
                    v                                            |
             +-------------+                                     |
             | source-data |                                     |
             +------+------+                                     |
                    |                                            |
     +--------------+--------------+--------------+--------------+              |
     |              |              |              |              |              |
     v              v              v              v              v              v
+-------------+ +-------------+ +-------------+ +-------------+ +-----------------------+ +---------------+
|hstree-analysis| |iforest-analysis| |kitnet-analysis| |deepant-analysis| |mdragstream-point-analysis| | topic-metrics |
+------+------+ +------+------+ +------+------+ +------+------+ +-----------+-----------+ +---------------+
       |               |                |               |                  |
       v               v                v               v                  v
 hstree-analysis   iforest-analysis   kitnet-analysis   deepant-analysis   mdragstream-point-analysis
 + viz             + viz              + viz             + viz              + viz
       |               |                |               |                  |
       +---------------+----------------+---------------+------------------+
                       |
                       v
              +------------------+
              | consensus-analysis|
              | event-time join   |
              | anomaly_count     |
              +------------------+

 source-data or selected detector output
       |
       v
 +------------------+
 | explainability V1/V2 |
 | +viz             |
 +---------+--------+
           |
           v
  explainability
  + explainability-viz

All JSON topics `*-viz`, `consensus-analysis`, and `topic-metrics`
                     |
                     v
               +-----------+
               | telegraf  |
               +-----+-----+
                     |
                     v
               +-----------+
               | influxdb  |
               +-----+-----+
                     |
                     v
               +-----------+
               | grafana   |
               +-----------+

Diagnostic UIs:
- Kafka UI: host 8080
```

## Key Details

- **Network**: all services run on `kafka-net`.
- **Kafka**: 3 KRaft brokers `kafka1`, `kafka2`, `kafka3`.
- **Host exposure**:
  - Kafka: `9092` through `kafka1`
  - Schema Registry: `8081`
  - Kafka UI: `8080`
  - InfluxDB: `8086`
  - Grafana: `3000`
- **Auto-create topics**: disabled.

## Topics Managed by `init-topic`

[`runtime/bootstrap/init-topic/init-topics.py`](../../runtime/bootstrap/init-topic/init-topics.py) creates the following topics:

- `source-data`
- `source-data-viz`
- `hstree-analysis`
- `hstree-analysis-viz`
- `iforest-analysis`
- `iforest-analysis-viz`
- `kitnet-analysis`
- `kitnet-analysis-viz`
- `deepant-analysis`
- `deepant-analysis-viz`
- `mdragstream-point-analysis`
- `mdragstream-point-analysis-viz`
- `consensus-analysis`
- `explainability`
- `explainability-viz`
- `topic-metrics`
- `explainability-v2`
- `explainability-v2-viz`
- `repeatability-v1`
- `repeatability-v1-viz`
- `repeatability-v2`
- `repeatability-v2-viz`
- `repeatability-diff`
- `reproducibility-diff`
- `reproducibility-summary`
- `explainability-shap-diff`

In the main Compose file, they are declared with:

- `partitions = 1`
- `replication factor = 3`

## Kafka Flows

- `producer` publishes:
  - `source-data` in Avro
  - `source-data-viz` in JSON
- `hstree-analysis` consumes `source-data` and publishes:
  - `hstree-analysis` in Avro
  - `hstree-analysis-viz` in JSON
- `iforest-analysis` consumes `source-data` and publishes:
  - `iforest-analysis` in Avro
  - `iforest-analysis-viz` in JSON
- `kitnet-analysis` consumes `source-data` and publishes:
  - `kitnet-analysis` in Avro
  - `kitnet-analysis-viz` in JSON
- `deepant-analysis` consumes `source-data` and publishes:
  - `deepant-analysis` in Avro
  - `deepant-analysis-viz` in JSON
- `mdragstream-point-analysis` consumes `source-data` and publishes:
  - `mdragstream-point-analysis` in Avro
  - `mdragstream-point-analysis-viz` in JSON
- `consensus-analysis` consumes the selected model JSON outputs, joins active model verdicts per `(Group, source_event_id)`, keeps `timestamp` for display, and publishes:
  - `consensus-analysis` in JSON
- `explainability` consumes `source-data` in `simple` mode or the selected
  detector output in `reference` mode, and publishes:
  - `explainability` in Avro
  - `explainability-viz` in JSON
- `explainability-v2` uses the same source-selection rules and publishes
  `explainability-v2` and `explainability-v2-viz`.
- `explainability-shap-diff` compares V1/V2 visualization records when both are enabled.
- `build-metrics` publishes `topic-metrics` in JSON

## Observability

- [`observability/telegraf/telegraf.conf`](../../observability/telegraf/telegraf.conf) consumes:
  - `topic-metrics`
  - `source-data-viz`
  - `hstree-analysis-viz`
  - `iforest-analysis-viz`
  - `kitnet-analysis-viz`
  - `deepant-analysis-viz`
  - `mdragstream-point-analysis-viz`
  - `explainability-viz`
  - `consensus-analysis`
- `telegraf` writes to `influxdb`.
- `grafana` is part of the current architecture, automatically provisioned and connected to the repository data.

## InfluxDB One-Shot Services

- `influxdb-init-buckets` creates or updates the required buckets.
- `influxdb-apply-template` applies [`observability/influxdb/dasma-template.yml`](../../observability/influxdb/dasma-template.yml).

## Persistent Volumes

- `kafka1-data`
- `kafka2-data`
- `kafka3-data`
- `grafana-data`
- `pip-cache`
- `influxdb-data`

## Reference

The reference entry point remains [`docker-compose.yml`](../../docker-compose.yml). The other Compose files in the repository should not be interpreted as describing this architecture without prior verification.
