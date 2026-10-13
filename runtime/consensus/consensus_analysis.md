# Consensus Analysis

## Purpose

The consensus service answers one question for every source event: **how
many active anomaly models vote anomaly for the same source data point in a
given `Group`?**

It is a *vote of the models for one event*, not an event counter. For a
`source_event_id` `E` and group `G`, each active model contributes one verdict
(`normal` / `anomaly`), and the consensus is the number of those verdicts that
are `anomaly`.

The output field is `anomaly_count`. Its range is `0..active_models`, not a
fixed `0..3`. With all current model pipelines enabled, the maximum is `5`:
HSTree, Isolation Forest, KitNET, DeepAnT, and mDragStream Point-based.

## Runtime Model

The service consumes the JSON visualization topics produced by the selected
models:

| Model | Input topic | Anomaly field |
|---|---|---|
| `hstree` | `hstree-analysis-viz` | `hstree_anomaly` |
| `iforest` | `iforest-analysis-viz` | `iforest_anomaly` |
| `kitnet` | `kitnet-analysis-viz` | `kitnet_anomaly` |
| `deepant` | `deepant-analysis-viz` | `deepant_anomaly` |
| `mdragpoint` | `mdragstream-point-analysis-viz` | `mdragpoint_anomaly` |

### Event-time join

The models do not produce their scores at the same rate — their compute cost
differs (Isolation Forest refits, KitNET autoencoder training, DeepAnT
inference). Their verdicts for the same source event therefore arrive
asynchronously. The service joins them by `(Group, source_event_id)`:

```text
pending[Group][source_event_id] = {
  timestamp: display_timestamp,
  verdicts: {model: verdict},
  first_seen: wall_clock
}
```

Each incoming `*-viz` record adds its model's verdict to the matching bucket.
A bucket is published when:

- **completion** — every active model has voted for that `(Group, source_event_id)`;
  the consensus is emitted immediately with `emit_reason = "complete"`;
- **timeout / watermark** — the bucket has been pending for more than
  `CONSENSUS_TIMEOUT_SEC` wall-clock seconds without completing; a partial
  consensus is emitted with `emit_reason = "timeout"` and the late models
  handled by the configured late-model policy.

### Progressive completion

After a timeout publish the bucket is **kept**, not dropped. As the slow models
finally report, an updated consensus for the same `(Group, source_event_id)` is
re-published (`emit_reason = "progress"` while still partial, then `"complete"`).
Because Telegraf/InfluxDB key a point by measurement + tags + timestamp, the
consensus bucket stores `source_event_id` as a tag. Re-publishes for the same
event overwrite the previous version of that event, while distinct events that
share the same display second remain distinct.

To make this safe, verdicts are **sticky/monotone** within a bucket: a model's
verdict can only strengthen along `None` (warming up) `< False < True`. This
guarantees `anomaly_count` and `reported_models` never decrease over a bucket's
life, so a re-publish can never lower a consensus and erase an anomaly. Unchanged
re-publishes are suppressed.

A bucket stops accepting verdicts and is evicted after `CONSENSUS_RETENTION_SEC`
(it is then sealed as a short tombstone so verdicts arriving even later are
dropped rather than recreating a partial that would overwrite the good point).

This replaces the former *latest-state* design, which kept only the last known
verdict per `(Group, model)` and could mix a KitNET verdict for `E` with an older
HSTree verdict for another event. The event-time join guarantees that the
verdicts combined into one consensus point describe the **same** source event.

### Source event key and display timestamp

The realtime producer restamps records at **second** granularity
(`%Y-%m-%d %H:%M:%S`). Under the accelerated modes (`realtime-x5/x20/x100`)
several distinct source points of the same group can share one second. The
producer therefore adds `source_event_id` before schema generation and every
model propagates it into its `*-analysis-viz` topic. The consensus uses that key
for exact synchronization and keeps `timestamp` only as the Grafana display
time.

For backwards compatibility, messages without `source_event_id` fall back to
the old `(Group, timestamp)` bucket. That fallback is exact only at **≤ 1 source
point per group-second** and otherwise degrades to a sticky "any anomaly seen in
that second" merge.

### Late-model policy

`CONSENSUS_LATE_POLICY` controls what happens to a model that has not voted when
a bucket times out:

| Policy | Behaviour |
|---|---|
| `missing` (default) | Late models stay `null`: not counted in `anomaly_count`, not in `reported_models`. The strict, honest interpretation — the count reflects only the verdicts that actually arrived for the instant. |
| `carry_forward` (experimental) | Late models are filled from their last known verdict for the group. Carried anomalies do increment `anomaly_count` and are reported through `carried_models`. A fresh verdict for the instant always wins over a carried one. **Caveat:** `last_known` is global per `(Group, model)` with no source-time guard, so with out-of-order delivery a future verdict can be carried onto an older instant. Use only when per-partition ordering is trusted. |

## Output Semantics

`anomaly_count` is the number of active models whose verdict for the source event is
`true` (including carried verdicts under `carry_forward`).

`reported_models` is the number of active models that produced a **fresh**
verdict for that exact `(Group, source_event_id)`. `complete` is `true` when
`reported_models == active_models`. `carried_models` counts the models filled
from history under `carry_forward` (always `0` under `missing`). `emit_reason`
is one of `complete`, `timeout`, `progress`, or `shutdown`.

Missing model states are represented as `null` in the per-model output fields.

### Per-model votes (`*_vote`)

Each model is published twice: a boolean `*_anomaly` (for JSON consumers) and a
numeric `*_vote` — `1` anomaly, `0` normal, `0` not reported. **Telegraf's JSON
parser persists numbers but drops booleans**, so only the `*_vote` fields reach
InfluxDB; they are what Grafana reads to decompose the consensus per model. The
active models' votes sum to `anomaly_count`. A missing vote is encoded as `0` so
progressive re-publishes overwrite any previous numeric value in InfluxDB
instead of leaving a stale model vote behind.

Example output:

```json
{
  "timestamp": "2026-01-01 12:34:56",
  "source_event_id": "event-000000123456",
  "Group": "Sortie CTA",
  "anomaly_count": 2,
  "hstree_anomaly": true,  "hstree_vote": 1,
  "iforest_anomaly": true,  "iforest_vote": 1,
  "kitnet_anomaly": false,  "kitnet_vote": 0,
  "deepant_anomaly": null,  "deepant_vote": 0,
  "mdragpoint_anomaly": null,  "mdragpoint_vote": 0,
  "active_models": 3,
  "reported_models": 3,
  "carried_models": 0,
  "complete": true,
  "emit_reason": "complete"
}
```

The record `timestamp` is the source display timestamp, so each consensus point
lands at the correct position on the Grafana time axis. `source_event_id` is the
exact join/storage key.

## Selected Models

`SELECTED_MODELS` controls the active model set for a campaign. The service
subscribes only to topics for models present in that comma-separated list.

Examples:

```text
SELECTED_MODELS=hstree,iforest,kitnet
SELECTED_MODELS=hstree,iforest,kitnet,deepant,mdragpoint
SELECTED_MODELS=deepant,mdragpoint
```

When no valid model is selected, the service idles instead of emitting
misleading consensus records.

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `DASMA_ENABLED` | `1` | `0` disables the service and keeps the container idling |
| `BOOTSTRAP_SERVERS` | `kafka1:9092,kafka2:9092,kafka3:9092` | Kafka broker list |
| `CONSENSUS_GROUP_ID` | `consensus-analysis-consumer` | Kafka consumer group id |
| `KAFKA_DEST_TOPIC` | `consensus-analysis` | Output topic |
| `SELECTED_MODELS` | `hstree,iforest,kitnet` | Comma-separated active models |
| `CONSENSUS_TIMEOUT_SEC` | `30` | Wall-clock watermark before a partial bucket is published |
| `CONSENSUS_RETENTION_SEC` | `300` | Seconds a bucket accepts late verdicts before it is sealed/evicted |
| `CONSENSUS_LATE_POLICY` | `missing` | `missing` or `carry_forward` (experimental) |
| `AUTO_OFFSET_RESET` | `earliest` | Consumer offset reset policy |
| `LOG_EVERY` | `100` | Progress log cadence |
| `ANALYSE_IDLE_EXIT_SEC` | `0` | Exit after N idle seconds; `0` disables idle exit |

## InfluxDB And Grafana

Telegraf consumes `consensus-analysis` and writes it to the
`consensus-analysis` InfluxDB bucket. `Group` and `source_event_id` are tags in
that bucket, so several source events can coexist at the same second without
overwriting each other.

The `Consensus Strength (max models/event)` panel reads `anomaly_count` from
that bucket directly and applies `aggregateWindow(..., fn: max)` over the
display window so short anomaly peaks are not hidden by a more recent normal
point. It must keep the per-`Group` active-model semantics rather than
re-deriving a different count from the five model buckets:

- HSTree from `hstree-analysis`
- Isolation Forest from `iforest-analysis`
- KitNET from `kitnet-analysis`
- DeepAnT from `deepant-analysis`
- mDragStream Point-based from `mdragstream-point-analysis`

The `Consensus by Model (1/model/window)` panel decomposes that count from the
per-model `*_vote` series (`hstree_vote`, `iforest_vote`, `kitnet_vote`,
`deepant_vote`, `mdragpoint_vote`). The query groups the `source_event_id` tag
away and uses `max` per display window, so each model contributes at most one
unit in a window. The view stays model-oriented and capped by the active model
count instead of becoming an event counter.

## Related Files

| File | Role |
|---|---|
| [`consensus_analysis.py`](consensus_analysis.py) | Runtime service and event-time join implementation |
| [`../tests/test_consensus_analysis.py`](../../tests/test_consensus_analysis.py) | Unit tests for bucket join, finalisation, watermark, and late-model policy |
| [`observability/telegraf/telegraf.conf`](../../observability/telegraf/telegraf.conf) | Kafka-to-InfluxDB mapping for JSON observability topics |
| [`observability/grafana/dashboards/anomaly_analysis_overview_influxdb.json`](../../observability/grafana/dashboards/anomaly_analysis_overview_influxdb.json) | Main anomaly overview dashboard |
