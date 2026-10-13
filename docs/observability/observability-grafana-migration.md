# Grafana Observability Guide

Analysis and validation dashboards query InfluxDB through the provisioned
[InfluxDB datasource](../../observability/grafana/provisioning/datasources/influxdb.yml).
Telegraf ingests the Kafka JSON streams and writes their measurements and
buckets; see the [telemetry mapping](../../observability/telegraf/telegraf.md).

## Data flow

```text
Kafka JSON visualization and validation topics
  -> Telegraf
  -> InfluxDB buckets
  -> Grafana dashboards
```

The historical Kafka-backed Records dashboard uses the separate
[Kafka datasource](../../observability/grafana/provisioning/datasources/kafka.yml)
to inspect `topic-metrics`. Use the InfluxDB dashboards to interpret detector
and explanation results.

## Provisioned dashboards

- [DeepAnT](../../observability/grafana/dashboards/By%20model/source-data_deepant_influxdb.json)
- [HSTree](../../observability/grafana/dashboards/By%20model/source-data_hstree_influxdb.json)
- [IForest](../../observability/grafana/dashboards/By%20model/source-data_iforest_influxdb.json)
- [KitNET](../../observability/grafana/dashboards/By%20model/source-data_kitnet_influxdb.json)
- [mDragStream Point-based](../../observability/grafana/dashboards/By%20model/source-data_mdragpoint_influxdb.json)
- [Explainability v1 vs v2](../../observability/grafana/dashboards/Explainability/explainability_compare_influxdb.json)
- [Explainability V1 — iForest](../../observability/grafana/dashboards/Explainability/explainability_iforest_influxdb.json)
- [Explainability V1 — HSTree](../../observability/grafana/dashboards/Explainability/explainability_influxdb.json)
- [Explainability V1 — KitNET](../../observability/grafana/dashboards/Explainability/explainability_kitnet_influxdb.json)
- [Explainability V2 — iForest](../../observability/grafana/dashboards/Explainability/explainability_v2_iforest_influxdb.json)
- [Explainability V2 — HSTree](../../observability/grafana/dashboards/Explainability/explainability_v2_influxdb.json)
- [Explainability V2 — KitNET](../../observability/grafana/dashboards/Explainability/explainability_v2_kitnet_influxdb.json)
- [Anomaly Analysis Overview](../../observability/grafana/dashboards/anomaly_analysis_overview_influxdb.json)
- [topics metrics](../../observability/grafana/dashboards/hstree_metrics.json)
- [Repeatability](../../observability/grafana/dashboards/repeatability_influxdb.json)
- [Reproducibility](../../observability/grafana/dashboards/reproducibility_influxdb.json)

The [dashboard provider](../../observability/grafana/provisioning/dashboards/dashboards.yml)
loads repository JSON files. Open Grafana at `http://localhost:3000`.
The [InfluxDB template](../../observability/influxdb/dasma-template.yml) retains
historical dashboard definitions; it is distinct from Grafana's provisioning.
No full panel-by-panel equivalence is asserted between the two systems.

## Choosing filters and reading results

Model dashboards let you select Group and Feature. Feature choices come from
fields in stored source-data points, avoiding an old feature list when the
current pipeline has changed. Model panels show scores and anomaly markers;
inspect status fields when distinguishing warmup from real scores.

The Source Data and DeepAnT dashboard reads `deepant-analysis-viz` through
Telegraf. Source Data and mDragStream Point-based uses
`mdragstream-point-analysis-viz`. In native mDragStream mode, the verdict comes
from candidate/cluster logic; the default dashboard hides its internal
threshold curve.

Choose the Explainability dashboard matching V1/V2 and the selected model.
TreeExplainer for iForest explains mean path length, while KernelSHAP explains
the local anomaly score. A displayed V1/V2 difference is meaningful only when
models, methods and effective states are comparable.

Repeatability compares paired runs locally. Reproducibility displays a CSV
comparison explicitly published after the offline cross-machine check.
These dashboards show configuration-specific evidence, not causal explanations
or a general guarantee of detection quality.

## Checking a dashboard change

Verify its datasource, bucket, measurement, variable filters and field types
against a short campaign. Check source identity coverage and statuses before
interpreting plotted values. Static JSON checks do not replace runtime or
visual inspection.
