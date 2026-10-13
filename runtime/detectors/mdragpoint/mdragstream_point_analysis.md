# mDragStream Point-based DASMA-Kafka Integration

## Scope

This document describes the DASMA-Kafka integration of the **Point-based**
variant of mDragStream.

The Subsequence-based variant is not part of this phase. It is deferred because
it has different temporal semantics: one score is produced for a window rather
than for every incoming Kafka message.

## Runtime Components

The integration is split across these files:

- `runtime/detectors/mdragpoint/models/mdragpoint.py`
  - Point-based model wrapper.
  - Exposes `MDragPointGlobalModel.partial_fit_score(x)`.
  - Exposes the native verdict through `last_is_anomaly` and the native internal
    threshold through `get_internal_threshold()`.
- `runtime/detectors/mdragpoint/mdragstream_point_analysis.py`
  - Kafka service.
  - Consumes Avro records from `source-data`.
  - Produces Avro records to `mdragstream-point-analysis`.
  - Produces JSON visualization records to `mdragstream-point-analysis-viz`.
- `src/dasma_kafka/mdragpoint_analysis/helpers.py`
  - Pure helper functions for group resolution, feature inference, Avro fields,
    visualization records, and schema-safe enrichment.

## Kafka Flow

```text
source-data
  -> mdragstream-point-analysis
  -> mdragstream-point-analysis-viz
  -> Telegraf
  -> InfluxDB bucket mdragstream-point-analysis
  -> Grafana dashboards
```

The Avro output keeps the source fields and adds:

```text
mdragpoint_score
mdragpoint_status
mdragpoint_anomaly
mdragpoint_threshold
mdragpoint_internal_r
```

The JSON visualization topic is the stream read by Telegraf.

## Model Contract

For each group, the service keeps one `MDragPointGlobalModel` instance:

```text
models_by_group[group_key]
```

The feature vector is inferred from numeric fields in the input record after
excluding operational or label-like fields such as:

```text
timestamp, Group, group, Cycle, LabviewTime, time
```

If the numeric feature set changes during a run, the service logs a warning and
resets all group models. This avoids silently scoring incompatible vectors.

## Warmup

The Point-based model buffers `MDRAGPOINT_N_INIT` points before initializing its
clusters and internal threshold.

Direct-service default (AskMyDasma campaigns use 1,000):

```text
MDRAGPOINT_N_INIT=500
```

This warmup is per group. With many groups, a dashboard can remain sparse for a
while even though the service is consuming messages.

During warmup:

```text
mdragpoint_status = "warming_up"
mdragpoint_anomaly = null
mdragpoint_threshold = null
```

## Native Verdict Mode

The default verdict mode is:

```text
MDRAGPOINT_VERDICT_MODE=internal
```

In this mode, the service uses mDragStream's native Point decision:

```text
status, anomaly, threshold = classify_mdragpoint_score_internal(...)
```

The published fields mean:

- `mdragpoint_score`: distance from the new point to the closest known cluster.
- `mdragpoint_internal_r`: native internal threshold `r`.
- `mdragpoint_threshold`: mirrors `mdragpoint_internal_r`.
- `mdragpoint_anomaly`: native `last_is_anomaly` from the model.
- `mdragpoint_status`: `warming_up`, `normal`, or `anomaly`.

This is the default because the Point algorithm uses `r` not only as a display
threshold, but also as part of its internal candidate/cluster mechanics.

## External Verdict Mode

The service also supports:

```text
MDRAGPOINT_VERDICT_MODE=external
```

This mode classifies `mdragpoint_score` with an external estimator:

```text
MDRAGPOINT_THRESHOLD_MODE=percentile
MDRAGPOINT_THRESHOLD_MODE=welford
```

The UI alias `p99` is normalized to `percentile`.

External mode is useful for comparison with HSTree, IForest, KitNET, and
DeepAnT, but it is not the default because it can over-trigger on DASMA data: it
does not account for the native candidate rehabilitation and cluster creation
rules.

## Direct-service Parameters

| Parameter | Default | Meaning |
|---|---:|---|
| `MDRAGPOINT_VERDICT_MODE` | `internal` | `internal` uses the native mDragStream verdict; `external` uses P99/Welford on `mdragpoint_score`. |
| `MDRAGPOINT_DYNAMIC_THRESHOLD` | `1` | Enables native dynamic updates of `r` (on by default, matching the UI). |
| `MDRAGPOINT_LOF_INIT` | `0` | Applies LOF filtering to the initial threshold distances. |
| `MDRAGPOINT_N_INIT` | `500` | Number of initial points buffered per group. |
| `MDRAGPOINT_MAX_NB_CLUSTERS` | `30` | Maximum number of clusters retained. |
| `MDRAGPOINT_MAX_CLUSTER_SIZE` | `30` | Maximum number of points per cluster. |
| `MDRAGPOINT_METRIC` | `mahalanobis` | Distance metric: `mahalanobis`, `euclidean`, `manhattan`, or `minkowski`. |
| `MDRAGPOINT_THRESHOLD_METHOD` | `std` | Native internal threshold initialization/update method. |
| `MDRAGPOINT_THRESHOLD_T` | `2.0` | Multiplier for native `mean + t * std`. |
| `MDRAGPOINT_LOF_NEIGHBORS` | `10` | LOF neighbors when `MDRAGPOINT_LOF_INIT=1`. |

These are direct-service fallbacks. Campaign defaults differ for initialization,
LOF, cluster count and neighbors; see the [campaign parameter guide](../../../docs/models/hyper-parameters.md).

External verdict parameters:

| Parameter | Default | Meaning |
|---|---:|---|
| `MDRAGPOINT_THRESHOLD_MODE` | `percentile` | External threshold mode. UI accepts `p99`. |
| `MDRAGPOINT_THRESHOLD_PERCENTILE` | `99.0` | Percentile used by the sliding estimator. |
| `MDRAGPOINT_THRESHOLD_WINDOW` | `500` | Sliding window size for the percentile estimator. |
| `MDRAGPOINT_ANOMALY_K_SIGMA` | `3.0` | K value for Welford `mean + k * std`. |
| `MDRAGPOINT_ANOMALY_MIN_SAMPLES` | `100` | Minimum samples before external verdicts become active. |

## AskMyDasma UI

The UI exposes:

- a model checkbox: `mDragStream Point-based`;
- a verdict mode selector:
  - `Native mDragStream internal threshold`;
  - `External P99/Welford threshold`;
- external threshold controls shown only when external mode is selected;
- core Point parameters in a collapsible expert block.

The default UI mode is native/internal.

## Grafana Semantics

The default Grafana panels intentionally show:

- `mdragpoint_score` as the main curve;
- red markers derived from `mdragpoint_status == "anomaly"`;
- no `mdragpoint_threshold` or `mdragpoint_internal_r` curve by default.

This avoids implying that an external P99/Welford estimator is the model's main
decision mechanism. The internal threshold fields are still written to InfluxDB
for inspection and debugging.

## Consensus Semantics

The consensus service reads `mdragstream-point-analysis-viz` and maps it to the
model key:

```text
mdragpoint
```

When `mdragpoint` is selected, the consensus output includes:

```text
mdragpoint_anomaly
```

Only active selected models count toward `active_models` and
`reported_models`.

## Known Scientific Limits

This integration is intentionally faithful to the current Point baseline rather
than already corrected.

Known Point items retained for now:

- **B2**: `threshold_distance` keeps one distance per outer-loop row instead of
  all pairwise distances. The current wrapper preserves this historical behavior
  until the correction phase.
- **B3**: the historical ablation material mislabeled Case 4. This affects
  historical result labels, not the online service directly.
- **B7**: when LOF initialization is enabled, LOF filters threshold distances
  but the Mahalanobis covariance is still computed on the full init buffer.

Operational caveats:

- Warmup is long compared with HSTree.
- Mahalanobis distance can be sensitive to collinear or low-variance features.
- `score_partial` is read-only, while `partial_fit_score` updates model state.
- The current integration needs local DASMA validation before the output can be
  treated as scientifically validated.

## Validation Status

Implemented and tested surfaces:

- model unit tests;
- helper and Avro schema tests;
- Docker/script activation tests;
- UI parameter serialization tests;
- consensus tests;
- Grafana dashboard contract tests.

End-to-end validation path:

```text
Kafka -> mdragstream-point-analysis -> Telegraf -> InfluxDB -> Grafana -> consensus
```

This end-to-end validation should be run on a short campaign after any change to
the model parameters, scripts, Telegraf, or Grafana dashboards.
