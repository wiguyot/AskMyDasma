# Multi-group HSTree analysis

This detector offers **HSTree** with **Paper (Tan et al., 2011)** (the default)
and **PySAD** variants, selectable in the UI's Core parameters
or through `HSTREE_VARIANT`. The PySAD variant differs from the published algorithm. See
[`HSTree paper deviations`](../../../docs/models/hstree/paper-deviations.md) for the differences
and the decisions made for this project.

## Objective

Consume **Avro** records from the Kafka topic `source-data`, compute an **HSTree** anomaly score and an explicit anomaly verdict, then republish:

- an enriched Avro version in `hstree-analysis`
- an enriched JSON version for visualization in `hstree-analysis-viz`, or the topic defined by `KAFKA_DEST_VIZ_TOPIC`

The processing is multi-group: one HSTree model and one threshold estimator are maintained per group key resolved through `src/dasma_kafka/common/grouping.py`, using `Group` then `group`, otherwise `__NO_GROUP__`.

## Processing Flow

For each consumed message:

1. Determine the group through `resolve_group_key(record)`.
2. Create an HSTree model for the group if this is the first time the group appears.
3. Build a numeric feature vector from the record fields, excluding metadata fields.
4. In normal scoring, compute `hstree_score` against the current reference
   (`score_partial`), then count the event (`fit_partial`), including on reference
   rollover (paper order). During warmup it is published as the numeric filler
   `0.0`; this is not a score and must not be interpreted on its own. Use
   `hstree_status == "warming_up"` to identify it. The event that completes the
   256-event reference window is also marked `warming_up` with `hstree_score = 0.0`;
   the first scored event is the next one.
5. Classify the score with a per-group online threshold once enough real scores have been observed.
6. Enrich the Avro record with the HSTree analysis fields and publish it to `hstree-analysis`.
7. Produce a JSON record on `hstree-analysis-viz` with derived fields for Grafana.

## Models By Group

The service maintains a dictionary of models, one per group:

```text
models_by_group = {
  "Sortie CTA": <HSTreeGlobalModel>,
  "Air Neuf": <HSTreeGlobalModel>,
  "Extraction": <HSTreeGlobalModel>,
  ...
}
```

Each model has its own state, warmup, window and tree structure. Models do not share data across groups.

The default ensemble has 25 trees, matching the paper's default; `HSTREE_NUM_TREES` remains configurable.
The reference-window defaults are `HSTREE_INITIAL_WINDOW_SIZE=256` and
`HSTREE_WINDOW_SIZE=256`. The project requires these values to be equal because
this is the reference-window configuration selected for the PySAD variant.

## HSTree Variant

`HSTREE_VARIANT` selects the algorithm while keeping the same service, topics,
and output fields. It defaults to `paper`; this variant uses a
separate work range per tree and scores only the first terminal node reached
(maximum depth or reference mass at most `0.1 × ψ`). Both implementations score
against the current reference and then count the event. An unsupported value
is rejected at startup. The output field `hstree_variant` identifies the
selected implementation on both Avro and visualization JSON records. The
`pysad` variant uses common initial min/max bounds and sums weighted reference
masses along the full path.

With the `paper` variant, a real post-warmup score can be exactly `0.0`: the
point may reach an empty terminal node in every tree. Use `hstree_status` to
distinguish this valid score from the numeric `0.0` filler used during warmup.
The derived `hstree_score_log10` is also `0.0` for a zero score, so that field
alone does not distinguish the two cases.

## Anomaly Trigger

After the HSTree model exits its internal warmup, the service classifies real
scores with a per-group threshold. The default campaign estimator is `p99` (a
sliding percentile threshold). The `p99` campaign alias is normalized by the
HSTree service to its internal `percentile` mode:

```text
P99 over the last HSTREE_THRESHOLD_WINDOW normal scores
```

The percentile mode is controlled by:

- `HSTREE_THRESHOLD_MODE=percentile`, or campaign alias `p99`
- `HSTREE_THRESHOLD_PERCENTILE=99.0`
- `HSTREE_THRESHOLD_WINDOW=500`

The two Welford modes remain available as explicit choices:

```text
welford           cumulative mean/std threshold
welford-windowed  sliding-window mean/std threshold over HSTREE_WELFORD_WINDOW scores
```

In both Welford modes, the threshold is:

```text
mean + HSTREE_ANOMALY_K_SIGMA * std
```

The verdict remains in `warming_up` until `HSTREE_ANOMALY_MIN_SAMPLES` real scores have fed the estimator for the group. Once active:

- scores below or equal to the threshold are emitted as `normal`, with `hstree_anomaly=false`
- scores above the threshold are emitted as `anomaly`, with `hstree_anomaly=true`
- anomalous scores are not fed back into the estimator, to limit threshold contamination

## Added HSTree Fields

Both the Avro output topic and the JSON viz topic carry:

- `hstree_score`: continuous HSTree score
- `hstree_status`: `warming_up`, `normal`, or `anomaly`
- `hstree_anomaly`: boolean verdict, `null` during warmup
- `hstree_threshold`: current per-group threshold, `null` during warmup

## Features And Exclusions

- Features are inferred only once, from the first message. They are numeric fields, `int` or `float`, that are not excluded.
- The list of excluded fields is configurable through `HSTREE_EXCLUDE_FIELDS`.
- Default value: `source_event_id,timestamp,Group,group,Cycle,LabviewTime,time`.

The service infers the feature list once and then keeps using it as-is.

## Input / Output Topics

- Source, Avro: `KAFKA_SOURCE_TOPIC`, default `source-data`
- Destination, Avro: `KAFKA_DEST_TOPIC`, default `hstree-analysis`
- Visualization destination, JSON: `KAFKA_DEST_VIZ_TOPIC`, default `hstree-analysis-viz`

## Visualization Fields

The JSON produced on `*-viz` includes the enriched record plus derived fields:

- `hstree_score_log10`
- `hstree_score_log10_diff`

If `WINDOWS_SIZE > 0`, a moving average and its derived fields are added:

- `hstree_score_ma_<N>`
- `hstree_score_ma_<N>_log10`
- `hstree_score_ma_<N>_log10_diff`

The state of derived fields, differences and moving averages is kept per group to avoid mixing groups together.

Warmup filler values also enter these visualisation calculations. In particular,
`hstree_score_log10` maps a zero filler to `0.0`, and
`hstree_score_log10_diff` shows a transition when real scores begin. If the
moving-average window is enabled, warmup zeros enter its per-group history and
can dilute the average until they roll out of the window. Treat these derived
series as visualisations of the published numeric field, not as a replacement
for checking `hstree_status`.

## Kafka Behavior

- `enable.auto.commit=false`: manual commit
- The message is committed only after successful production, Avro plus JSON, which gives at-least-once processing
- The original Kafka timestamp is preserved when present

## Important Environment Variables

- `BOOTSTRAP_SERVERS`: broker list, with whitespace trimmed for robustness
- `SCHEMA_REGISTRY_URL`, plus Basic Auth options: Avro deserialization
- `ANALYSE_HSTREE_GROUP_ID`: consumer group id
- `TOPIC_PARTITIONS` / `TOPIC_RF`: if present, the service performs a non-blocking sanity check on topic layout and only logs a warning on mismatch
- `ANALYSE_SEED`: global seed, NumPy plus random
- `WINDOWS_SIZE`: window size for visualization metrics, default `0`
- `HSTREE_INITIAL_WINDOW_SIZE`, `HSTREE_WINDOW_SIZE`, `HSTREE_NUM_TREES` (default `25`), `HSTREE_MAX_DEPTH`
- `HSTREE_VARIANT`: `paper` (default) or `pysad`
- `HSTREE_THRESHOLD_MODE`: threshold engine, default `percentile`; accepts campaign alias `p99`, plus `welford` and `welford-windowed`
- `HSTREE_THRESHOLD_PERCENTILE`: percentile used in percentile mode, default `99.0`
- `HSTREE_THRESHOLD_WINDOW`: sliding window size used in percentile mode, default `500`
- `HSTREE_WELFORD_WINDOW`: sliding window size used in `welford-windowed` mode, default `2048`
- `HSTREE_ANOMALY_K_SIGMA`: Welford threshold sensitivity, default `3.0`
- `HSTREE_ANOMALY_MIN_SAMPLES`: number of real scores per group before activating the verdict, default `100`

AskMyDasma writes the campaign JSON first, then exports runtime environment variables from that JSON. For HSTree it exposes the estimator (`p99`, `welford`, or `welford-windowed`), the percentile window and percentile value, the Welford window, the Welford sigma/warmup parameters, and the model structure parameters.

## Explainability Impact

The new HSTree trigger fields are output fields. They must not become SHAP input features:

- `hstree_status` is categorical
- `hstree_anomaly` is a verdict
- `hstree_threshold` is derived from the score distribution

They are therefore excluded from the Explainability feature set.

The Explainability mirror does not publish a local HSTree score during upstream
warmup: the explanation result has `explanation_status="warmup"` and
`local_hstree_score` is unavailable. This differs from the detector's numeric
`hstree_score=0.0` filler.

## Reference-window limit for small groups

After the boundary-event correction, the first real score for a group is on
its ψ + 1 event. A group with ψ events or fewer never receives a score: all its
detector outputs remain `warming_up` with `hstree_score=0.0`, and the
Explainability mirror leaves them in `warmup`. With ψ = 256, the historical
`source-5000` dataset has four such groups: `2B` (215 events), `1A` (75), `4B`
(53), and `3A` (50). Which groups are scored depends on the dataset and ψ.
In dashboards and consensus output, an unscored group means HSTree has not
evaluated it; it does not mean the group is normal.

The consensus consumer uses the detector's `hstree_anomaly` verdict, not the
numeric `hstree_score`, so it does not interpret warmup filler zeros as votes.
The HSTree score dashboard plots the numeric `hstree_score` series as published,
including warmup zeros; its anomaly markers are selected by
`hstree_status == "anomaly"`. The Explainability score-fidelity table requires
both upstream and local scores, so it excludes warmup rows. Be aware that a
warmup zero can still appear as a point in the raw detector score graph.

## Note About `ANALYSE_GROUP`

If `ANALYSE_GROUP` is defined, it is ignored: multi-group mode processes all groups.
