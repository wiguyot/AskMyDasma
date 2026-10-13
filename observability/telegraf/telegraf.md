# Telegraf: Kafka to InfluxDB

Telegraf consumes JSON observability streams, converts them into InfluxDB 2.x
points and writes the buckets used by Grafana. Avro detector streams remain
internal Kafka inputs for downstream computation.

## Configuration

The [main Compose file](../../docker-compose.yml) uses image `telegraf:1.30`
on `kafka-net`, with the [configuration](telegraf.conf) mounted read-only at
`/etc/telegraf/telegraf.conf`. It supplies `INFLUX_TOKEN` for organization `dasma`
and starts Telegraf after its configured infrastructure dependencies.

## Topic and measurement mapping

| Kafka JSON topic | InfluxDB measurement | Bucket |
|---|---|---|
| `topic-metrics` | `dasma_topic_metrics` | `topic-metrics` |
| `source-data-viz` | `source-data` | `source-data` |
| `hstree-analysis-viz` | `hstree-analysis` | `hstree-analysis` |
| `iforest-analysis-viz` | `iforest-analysis` | `iforest-analysis` |
| `kitnet-analysis-viz` | `kitnet-analysis` | `kitnet-analysis` |
| `deepant-analysis-viz` | `deepant-analysis` | `deepant-analysis` |
| `mdragstream-point-analysis-viz` | `mdragstream-point-analysis` | `mdragstream-point-analysis` |
| `explainability-viz` | `explainability` | `explainability` |
| `explainability-v2-viz` | `explainability-v2` | `explainability-v2` |
| `consensus-analysis` | `consensus-analysis` | `consensus-analysis` |
| `repeatability-v1-viz` | `repeatability-v1` | `repeatability-v1` |
| `repeatability-v2-viz` | `repeatability-v2` | `repeatability-v2` |
| `repeatability-diff` | `repeatability-diff` | `repeatability-diff` |
| `reproducibility-diff` | `reproducibility-diff` | `reproducibility-diff` |
| `reproducibility-summary` | `reproducibility-summary` | `reproducibility-summary` |
| `explainability-shap-diff` | `explainability-shap-diff` | `explainability-shap-diff` |

The V1/V2 Explainability inputs use `json_v2` to ingest status, model and
boolean fields with explicit types. Most other inputs use `json`. The latter
drops boolean values; consensus therefore also publishes numeric `*_vote` fields.

## Event identity and nested explanations

Consensus and explanation points include `Group` and `source_event_id` tags
to distinguish separate observations that share a display timestamp.
Explainability also tags `explained_model`. Nested `shap_values` maps carry
these same identity/model fields so Telegraf can obtain the tags at that path;
they are metadata, not numeric feature contributions. Older points may lack
tags added in later versions.

Inputs use configured consumer groups and `offset = "oldest"` for new groups.
The output configuration routes each measurement using `namepass`. The
InfluxDB URL is `http://influxdb:8086`, with the token read from `INFLUX_TOKEN`.
An optional file output writes to stdout for local diagnosis.

## Troubleshooting

- No consumption: check topic existence, messages, `init-topic` completion and
  Telegraf consumer-group progress.
- No stored points: check InfluxDB health, token, organization, bucket creation
  and Telegraf logs.
- Parse errors: inspect messages for valid JSON and confirm the configured
  parser and field types. Avro payloads cannot be read by these JSON inputs.

See the [storage guide](../../docs/architecture/data-storage.md) and
[Grafana guide](../../docs/observability/observability-grafana-migration.md).
