# mDragStream Point-based for Beginners

## The Idea in One Sentence

mDragStream Point-based watches each incoming point and asks: **is this point
close enough to what the stream already considers normal?**

If the point is too far from the known normal clusters, the model marks it as
suspicious.

## What Is a Point?

In DASMA-Kafka, one Kafka message contains many numeric measurements: chemical
signals, counters, temperatures, or other sensor values.

mDragStream Point-based turns these numeric fields into one vector:

```text
point = [feature_1, feature_2, feature_3, ...]
```

The model then compares this point with previously seen points from the same
group.

## How the Model Learns Normal Behavior

1. It first waits for an initial buffer of points.
2. This warmup is controlled by `MDRAGPOINT_N_INIT`.
3. Once the buffer is full, it builds an initial baseline.
4. The baseline is represented by clusters of normal points.
5. The model also computes an internal distance threshold called `r`.

Before the warmup is complete, the status is `warming_up`.

## How a New Point Is Scored

For each new point, the model computes the distance to the closest known cluster.

- Small distance: the point looks like known normal behavior.
- Large distance: the point is far from known normal behavior.

This distance is published as:

```text
mdragpoint_score
```

## Native Verdict

mDragStream Point-based has its own internal anomaly decision.

By default, DASMA-Kafka uses this native decision:

```text
MDRAGPOINT_VERDICT_MODE=internal
```

In this mode:

- `mdragpoint_anomaly` comes from the model's native Point logic.
- `mdragpoint_threshold` mirrors the internal threshold `mdragpoint_internal_r`.
- Grafana shows the score and red markers derived from `mdragpoint_status == "anomaly"`.

This default follows the native decision mechanism because the
Point algorithm uses its internal threshold to manage candidates, clusters, and
the transition from suspicious point to normal cluster.

## External Verdict Mode

DASMA-Kafka also keeps an experimental external threshold mode:

```text
MDRAGPOINT_VERDICT_MODE=external
```

In this mode, `mdragpoint_score` is classified with the same style of external
threshold used by other models:

- P99 sliding window
- Welford mean + k-sigma

This mode is useful for comparison, but it is not the default because it ignores
part of the native mDragStream candidate/cluster logic.

## Why Warmup Can Look Like No Data

mDragStream Point-based has a longer warmup than HSTree.

With the direct-service default:

```text
MDRAGPOINT_N_INIT=500
```

the model needs 500 points **per group** before its first meaningful verdict.
AskMyDasma campaigns instead default to `n_init=1000` and enable LOF
initialization; saved campaigns can override these values.

If there are 9 groups, the operator may need thousands of source messages before
all groups leave `warming_up`.

## What You See in Grafana

The default Grafana panels show:

- the yellow `mdragpoint_score` curve;
- red markers for native anomalies;
- no green internal-threshold curve by default.

The internal threshold still exists in the data as `mdragpoint_internal_r`, but
it is hidden from the default dashboard to avoid confusing it with an external
P99/Welford estimator.

## Important Limits

- The current phase integrates only **Point-based** mDragStream.
- The Subsequence variant is intentionally deferred.
- The first implementation keeps known historical Point behavior for scientific
  traceability before applying later corrections.
- A local validation campaign is still needed before treating the results as
  scientifically validated for DASMA data.

**Keywords:** streaming, point, cluster, distance, internal threshold, anomaly.
