# Multi-group iForest analysis

## Objective

Consume **Avro** records from the Kafka topic `source-data`, compute an **Isolation Forest** anomaly score and verdict, and republish:

- an **enriched Avro version** in `iforest-analysis`
- an **enriched JSON version for visualization** in `iforest-analysis-viz`, or the topic defined by `KAFKA_DEST_VIZ_TOPIC`

The processing is **multi-group**: one Isolation Forest model is maintained per group key resolved through [`src/dasma_kafka/common/grouping.py`](../../../src/dasma_kafka/common/grouping.py), using `Group` then `group`, otherwise `__NO_GROUP__`.

## General principle

For each consumed message:

1. Determine the group through `resolve_group_key(record)`, using `Group` then `group`, otherwise `__NO_GROUP__`.
2. Create an Isolation Forest model on the fly if this is the first time that group appears.
3. Build a numeric feature vector from the record fields, excluding metadata fields.
4. If a fitted forest exists, score the event with that forest **before** learning from the event. During the first
   `IFOREST_INITIAL_WINDOW_SIZE` events, no score exists; the output carries the `0.0` warmup sentinel.
5. Add the event to the group's sliding window and refit when due. The first real score is for event
   `IFOREST_INITIAL_WINDOW_SIZE + 1`.
6. Classify the score with a per-group online threshold once enough real scores have been observed.
7. Enrich the Avro record with the iForest analysis fields and publish it to `iforest-analysis`.
8. Produce a JSON record on `iforest-analysis-viz` for Grafana.

## Models by group

The service maintains a dictionary of models, one per group, for example:

```text
models_by_group = {
  "Sortie CTA": <IForestGlobalModel>,
  "Air Neuf": <IForestGlobalModel>,
  "Extraction": <IForestGlobalModel>,
  ...
}
```

Each model has its own state, warmup, reference window, forest, and so on, and is **not shared** across groups.

## Windows, warmup and reference

The model uses two window concepts:

- `IFOREST_INITIAL_WINDOW_SIZE`: number of points needed to fit the first forest. These points are not scored; the
  first real score is produced for the next event. The default is 512, or `2ψ`.
- `IFOREST_WINDOW_SIZE`: size of the sliding reference window, recent data, used to periodically retrain the forest.

The invariant is `initial_window_size ≥ max_samples (ψ)` and `window_size ≥ initial_window_size`. ψ remains one
fixed integer across every forest generation; the default is 256. Each tree draws ψ samples without replacement.
The number of trees defaults to 100, and the forest is refitted every 128 scored events. This streaming protocol
(sliding window plus periodic refit) is a local adaptation; each fitted forest itself is scikit-learn's Isolation
Forest based on Liu et al. (2008).

## Anomaly trigger

After the model exits warmup, the default `native` estimator applies the scoring forest's own threshold,
with Auto contamination by default (see [Native estimator](#native-estimator)). External estimators remain
selectable: `p99` over the recent `IFOREST_THRESHOLD_WINDOW` scores, cumulative `welford`, and
`welford-windowed`. In either Welford mode the trigger compares each real score to:

```text
mean + IFOREST_ANOMALY_K_SIGMA * std
```

For external estimators, the verdict remains in `warming_up` until `IFOREST_ANOMALY_MIN_SAMPLES` real scores
have fed the estimator for the group. Once active:

- scores below or equal to the threshold are emitted as `normal`, with `iforest_anomaly=false`
- scores above the threshold are emitted as `anomaly`, with `iforest_anomaly=true`
- anomalous scores are not fed back into external estimators, to limit threshold contamination

## Added iForest fields

Both the Avro output topic and the JSON viz topic carry:

- `iforest_score`: continuous iForest anomaly score
- `iforest_status`: `warming_up`, `normal`, or `anomaly`
- `iforest_anomaly`: boolean verdict, `null` during warmup
- `iforest_threshold`: current per-group threshold, `null` during warmup

## Features and exclusions

- **Features** are inferred only once, from the first message. They are the **numeric** fields, `int` or `float`, that are **not excluded**.
- The list of excluded fields is configurable through `IFOREST_EXCLUDE_FIELDS`.
- Default value: `source_event_id,timestamp,Group,group,Cycle,LabviewTime,time`.

> Note: the service infers the feature list once and then keeps using it as-is.

## Input / output topics

- Source, Avro: `KAFKA_SOURCE_TOPIC`, default `source-data`
- Destination, Avro: `KAFKA_DEST_TOPIC`, default `iforest-analysis`
- Visualization destination, JSON: `KAFKA_DEST_VIZ_TOPIC`, default `iforest-analysis-viz`

The JSON `*-viz` output contains the enriched record; it does not add `iforest_score_log10*`, moving-average fields,
or a `WINDOWS_SIZE` option.

## Kafka behavior, read / write

- `enable.auto.commit=false`: manual commit
- The message is committed **only after** successful production, Avro plus JSON, which gives **at-least-once**
- The original Kafka timestamp is preserved when present

## Important environment variables

### Kafka / Schema Registry

- `BOOTSTRAP_SERVERS`: broker list used by the consumer and producers, with whitespace trimmed. Default: `kafka1:9092,kafka2:9092,kafka3:9092`
- `SCHEMA_REGISTRY_URL`, plus Basic Auth options: Avro deserialization
- `ANALYSE_IFOREST_GROUP_ID`: consumer group id
- `KAFKA_SOURCE_TOPIC`, `KAFKA_DEST_TOPIC`, `KAFKA_DEST_VIZ_TOPIC`

### Topic layout control, optional

- `TOPIC_PARTITIONS` / `TOPIC_RF`: if present, the service performs a **non-blocking sanity check** at startup on the topic layout and only logs a warning on mismatch

### Seed and runtime behavior

- `ANALYSE_SEED`: global seed for NumPy and `random`, and `random_state` on the scikit-learn side
- `ANALYSE_SCHEMA_WAIT_SEC`: wait time for the source Avro schema, `0` means infinite wait
- `ANALYSE_SCHEMA_POLL_INTERVAL_SEC`: polling interval during the wait
- `ANALYSE_IDLE_EXIT_SEC`: exit after too much inactivity, `0` means disabled

### iForest parameters

- `IFOREST_EXCLUDE_FIELDS`
- `IFOREST_INITIAL_WINDOW_SIZE`: default `512`, must be ≥ ψ
- `IFOREST_WINDOW_SIZE`: default `2048`, must be ≥ initial window
- `IFOREST_REFIT_EVERY`: default `128`
- `IFOREST_N_ESTIMATORS`: default `100`
- `IFOREST_MAX_SAMPLES`: fixed integer ψ ≥ 2, default `256`; `auto` and fractions are rejected
- `IFOREST_THRESHOLD_MODE`: default `native`; `p99`, `welford` and `welford-windowed` remain available
- `IFOREST_CONTAMINATION`: default `auto`; fractions from `0.01` to `0.50` in steps of `0.01` are accepted.
  The UI presents Auto or 1–50%. This parameter controls the native verdict, not the anomaly score.
- `IFOREST_THRESHOLD_WINDOW`: P99 lookback, default `500`
- `IFOREST_WELFORD_WINDOW`: Welford lookback, default `2048`
- `IFOREST_ANOMALY_K_SIGMA`: threshold sensitivity, default `3.0`
- `IFOREST_ANOMALY_MIN_SAMPLES`: number of real scores per group before activating an external estimator's
  verdict, default `100` in a campaign. It does not apply to Native.

### Native estimator

After model warmup, Native compares the event's score with `-offset_` from the exact forest that scored it.
An anomaly requires `score > threshold`; equality is normal. With Auto, the threshold is `0.5` on our positive
score scale. A numeric contamination calibrates the cutoff on each training window, targeting that proportion
of outliers in training; it does not guarantee the same proportion on future events. Scores are not probabilities.

The event is scored before being learned. If it triggers a refit, its published threshold still belongs to the
previous scoring forest. Before a real score exists, status is `warming_up`, with null threshold and verdict.
Native needs no additional threshold calibration period. The existing `iforest_threshold` field feeds Grafana.
Analysis and Explainability v1/v2 receive identical contamination settings.

## Notes

- `iforest_score` is the paper-oriented anomaly score `s(x, ψ) = 2^(-E(h(x))/c(ψ))`, in `]0, 1]`; higher means
  more anomalous and `0.5` is the paper's reference point. `0.0` is only a warmup/error sentinel, never a real score;
  interpret it together with `iforest_status`, especially `warming_up`.
- A group with at most 512 records never receives a real score with the default initial window. On the known
  `source-5000` group distribution, groups 2B, 1A, 4B and 3A remain unscored. No score does not mean normal.
- The service's downstream consumers should use `iforest_status` or `iforest_anomaly` to identify valid verdicts,
  not infer normality from the numeric `0.0` sentinel.
- **Implementation details (scikit-learn 1.8.0):** training and scoring inputs are converted to float32; split
  equality follows `x ≤ threshold` to the left; at a node, a feature whose finite range is at most `1e-7` may be
  treated as constant and skipped; path correction uses exact `c(2)=1`, `c(n≤1)=0` and the library approximation
  for larger sizes. These library-level differences from the paper are accepted
  and documented in [`paper-conformity.md`](../../../docs/models/iforest/paper-conformity.md).
- **Consumer check:** the consensus service derives its vote from `iforest_anomaly`; the iForest Grafana dashboard
  uses `iforest_status == "anomaly"` to mark anomaly points. Neither treats the numeric `0.0` warmup sentinel as a
  normal verdict.
- State persistence is not provided: restarting the container resets the models, and warmup must happen again.
