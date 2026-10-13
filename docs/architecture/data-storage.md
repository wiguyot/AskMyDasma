# Data Storage

This document describes where runtime data is stored in the current Docker
Compose architecture. For service-to-service flows and schemas, see
[`schema.md`](schema.md).

## Runtime Flow

```text
CSV
  -> producer
  -> Kafka:
       source-data       (Avro, source schema)
       source-data-viz   (JSON for observability)

source-data
  -> hstree-analysis  (Avro) + hstree-analysis-viz  (JSON)
  -> iforest-analysis (Avro) + iforest-analysis-viz (JSON)
  -> kitnet-analysis  (Avro) + kitnet-analysis-viz  (JSON)
  -> deepant-analysis (Avro) + deepant-analysis-viz (JSON)
  -> mdragstream-point-analysis (Avro)
     + mdragstream-point-analysis-viz (JSON)

source-data or selected detector output
  -> explainability (Avro) + explainability-viz (JSON)
  -> explainability-v2 (Avro) + explainability-v2-viz (JSON)

explainability-viz + explainability-v2-viz
  -> explainability-shap-diff (JSON)

model *-viz topics
  -> consensus-analysis (JSON)
     event-time join per (Group, source_event_id):
       anomaly_count, <model>_anomaly (bool), <model>_vote (1/0),
       active_models, reported_models, carried_models,
       complete, emit_reason

build-metrics
  -> topic-metrics (JSON offsets for source, model, viz, consensus topics)

JSON observability topics
  -> Telegraf
  -> InfluxDB buckets:
       source-data
       hstree-analysis
       iforest-analysis
       kitnet-analysis
       deepant-analysis
       mdragstream-point-analysis
       explainability
       explainability-v2
       explainability-shap-diff
       repeatability-v1, repeatability-v2, repeatability-diff
       reproducibility-diff, reproducibility-summary
       consensus-analysis
       topic-metrics
  -> Grafana dashboards

Persistent Docker volumes:
  kafka1-data, kafka2-data, kafka3-data, influxdb-data, grafana-data, pip-cache
```

## Storage Roles

- **Kafka** stores the raw and enriched streams. Long retention allows replay
  from the beginning with `auto.offset.reset=earliest`.
- **InfluxDB** is the queryable time-series store used by Grafana. Each analysis
  service writes to its own bucket through a JSON `*-viz` topic.
- **Grafana** uses InfluxDB for the analysis and validation dashboards. The
  historical Kafka metrics dashboard reads `topic-metrics` through a separate
  Kafka datasource; it does not consume Avro detector topics.
- **Docker volumes** persist Kafka logs, InfluxDB data, Grafana state, and the
  optional legacy pip-cache volume across container restarts. Runtime images
  install Python dependencies at build time.

The InfluxDB tag/field distinction matters: tags are indexed and efficient for
Flux filters, while fields are not indexed.

## Current Buckets

| Bucket | Source topic | Main fields and tags |
|---|---|---|
| `source-data` | `source-data-viz` | Raw source fields, `Group` tag |
| `hstree-analysis` | `hstree-analysis-viz` | `hstree_score`, `hstree_threshold`, `hstree_anomaly`, `hstree_status` tag |
| `iforest-analysis` | `iforest-analysis-viz` | `iforest_score`, `iforest_threshold`, `iforest_anomaly`, `iforest_status` tag |
| `kitnet-analysis` | `kitnet-analysis-viz` | `kitnet_score`, `kitnet_threshold`, `kitnet_anomaly`, `kitnet_status` tag |
| `deepant-analysis` | `deepant-analysis-viz` | `deepant_score`, `deepant_threshold`, `deepant_anomaly`, `deepant_status` tag |
| `mdragstream-point-analysis` | `mdragstream-point-analysis-viz` | `mdragpoint_score`, `mdragpoint_anomaly`, `mdragpoint_threshold`, `mdragpoint_internal_r`, `mdragpoint_status` tag |
| `explainability` | `explainability-viz` | V1 SHAP/base values, status and explained-model identity |
| `explainability-v2` | `explainability-v2-viz` | V2 SHAP/base values, status and explained-model identity |
| `explainability-shap-diff` | `explainability-shap-diff` | Precomputed V1/V2 feature-contribution differences |
| `repeatability-v1`, `repeatability-v2` | corresponding `-viz` topics | Paired detector outputs |
| `repeatability-diff` | `repeatability-diff` | Per-event equality and differences |
| `reproducibility-diff` | `reproducibility-diff` | Published cross-machine comparison rows |
| `reproducibility-summary` | `reproducibility-summary` | Published comparison summaries |
| `consensus-analysis` | `consensus-analysis` | `source_event_id` tag, `anomaly_count`, per-model `*_vote` (1/0), `active_models`, `reported_models`, `carried_models`, `complete` (per-model `*_anomaly` booleans are emitted but not persisted) |
| `topic-metrics` | `topic-metrics` | Topic offset and production status metrics |

The mDragStream Point dashboards display the native point score and anomaly
markers by default. They intentionally do not display `mdragpoint_threshold` or
`mdragpoint_internal_r` by default to avoid confusing a displayed distance threshold with the full native
candidate/cluster decision. In native mode, `mdragpoint_threshold` mirrors
`mdragpoint_internal_r`; in external mode, it represents the external estimator.

## Consensus Storage

The Python event-time consensus service publishes to `consensus-analysis`;
Telegraf writes those JSON records to the bucket with the same name. The service consumes the selected model `*-analysis-viz` topics and
joins their verdicts per `(Group, source_event_id)`: it accumulates the votes of
the active models for the same source event and emits one consensus record when
the event is **complete** (every active model has voted) or when its
**timeout/watermark** (`CONSENSUS_TIMEOUT_SEC`) expires for late models. The
record keeps `timestamp` as the Grafana display time and carries:

- `source_event_id` (Influx tag, prevents overwrites when several events share one second)
- `anomaly_count`
- `hstree_anomaly` … `mdragpoint_anomaly` (boolean, for JSON consumers)
- `hstree_vote` … `mdragpoint_vote` (numeric twin: `1` anomaly / `0` normal or not reported)
- `active_models`
- `reported_models`
- `carried_models`
- `complete`
- `emit_reason`

Telegraf's JSON parser persists numbers but **drops booleans**, so the
`*_anomaly` booleans never reach InfluxDB. The numeric `*_vote` fields are the
InfluxDB-persistable twins: Grafana decomposes the consensus per model from the
votes. Missing model votes are encoded as `0`, while the matching
`*_anomaly` boolean remains `null` for JSON consumers. This avoids stale numeric
votes when progressive consensus records overwrite earlier InfluxDB fields.

`anomaly_count` ranges from `0` to `active_models`. With all current model
pipelines enabled, this is a five-model consensus. Dashboards should preserve
that active-model semantics even when they compute derived views directly from
the five model buckets.

## Avro Versus JSON Topics

Avro topics without the `-viz` suffix are internal runtime streams. They are
used by Python consumers and by downstream services that need schema-registry
records.

JSON `*-viz` topics are observability streams. Telegraf consumes them and writes
them to InfluxDB. The consensus, metrics and validation JSON topics are also ingested. Grafana
queries their InfluxDB measurements; the Kafka metrics demonstration is the
separate direct-topic exception noted above.
