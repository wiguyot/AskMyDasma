# Multi-group KitNet analysis

## Objective

Consume **Avro** records from the Kafka topic `source-data`, compute a **KitNet** anomaly score and verdict, and republish:

- an **enriched Avro version** in `kitnet-analysis`
- an **enriched JSON version for visualization** in `kitnet-analysis-viz`, or the topic defined by `KAFKA_DEST_VIZ_TOPIC`, empty means disabled

The processing is **multi-group**: one KitNet model is maintained per group key resolved through [`src/dasma_kafka/common/grouping.py`](../../../src/dasma_kafka/common/grouping.py), using `Group` then `group`, otherwise `__NO_GROUP__`.

## View of the KitNET algorithm, detector side

KitNET is an **unsupervised** and **online**, streaming, anomaly detection method. In the **Kitsune** paper, KitNET corresponds only to the “Feature Mapper + Autoencoders” part. This service does not implement AfterImage, the feature extraction layer.

1. a **Feature Mapper**, `FM`, that splits the feature space into **subsets**, clusters of correlated features
2. an **ensemble of autoencoders**, one autoencoder per subset, producing local reconstruction errors
3. an **aggregating autoencoder** that takes local errors as input and produces a global error

The intuition is:

- an autoencoder learns to reconstruct “normal” behavior
- an anomaly causes an **increase in reconstruction error**
- splitting into subsets reduces complexity and avoids a single large autoencoder on 80+ features

### Anomaly score

The main score, typical for KitNet, is a **reconstruction error**, often an RMSE:

- `local_rmse`: local error per subset autoencoder
- `kitnet_score`: global error from the aggregator

The higher the score, the more abnormal the observation is considered.

## Learning phases, grace periods

KitNet is classically deployed in **three phases**:

1. **FM grace period**, Feature Mapper learning
   - Observe a data stream and estimate correlations or dependencies between features
   - Then build a feature partition into subsets, bounded by `MAX_AUTOENC_SIZE`

2. **AD grace period**, autoencoder learning
   - Local autoencoders and the aggregator learn online from observations assumed to be mostly normal

3. **Detection**
   - The trained autoencoder weights remain fixed while incoming points are scored
   - A score is published for every message

The service can publish numeric scores during training, but the verdict remains
`warming_up`. Inspect `kitnet_status` rather than interpreting a training score
as a normal or anomalous verdict.

## Models by group

The service maintains a dictionary of models, one per group:

```text
models_by_group = {
  "Sortie CTA": <KitNetModel>,
  "Air Neuf": <KitNetModel>,
  "Extraction": <KitNetModel>,
  ...
}
```

Each model contains:

- Feature Mapper state, estimated correlations and final mapping
- the weights or state of local autoencoders
- the state of the aggregating autoencoder
- its counters, number of observations seen and current phase

## Features and exclusions

The source Avro message, `source-data`, contains numeric columns plus metadata fields.

The script must:

- build a vector `x` using only **numeric features**
- explicitly exclude certain fields through `KITNET_EXCLUDE_FIELDS`, comma-separated

Typical fields to exclude, depending on the dataset:

- `timestamp`
- `Group`, `group`
- `name`
- `time`

## Input / output topics

- Source: `KAFKA_SOURCE_TOPIC`, expected default `source-data`
- Avro destination: `KAFKA_DEST_TOPIC`, expected default `kitnet-analysis`
- JSON destination: `KAFKA_DEST_VIZ_TOPIC`, expected default `kitnet-analysis-viz`, empty means disabled

As with HSTree and iForest, the Avro topic is intended for downstream processing and archiving, while the JSON topic is used for Telegraf, InfluxDB, and Grafana.

## Fields added on output

### Avro side, `kitnet-analysis`

The enriched record contains:

- `kitnet_score`: global anomaly score, `float`
- `kitnet_status`: `warming_up`, `normal`, or `anomaly`
- `kitnet_anomaly`: boolean verdict, `null` during warmup
- `kitnet_threshold`: current per-group threshold, `null` during warmup

### JSON side, `kitnet-analysis-viz`

The JSON mirrors the enriched Avro record, same fields, and is mainly intended for Telegraf, InfluxDB, and Grafana.

- `kitnet_score` by `Group`
- `kitnet_threshold` by `Group`
- `kitnet_anomaly` as an anomaly marker
- `kitnet_status` as a low-cardinality tag

## Anomaly trigger

During the Feature Mapper and Anomaly Detector grace periods, the verdict remains `warming_up`. After these grace periods, the service keeps an online Welford estimator per group. The default campaign estimator is `welford-windowed` (Welford over the last `KITNET_WELFORD_WINDOW` scores); cumulative `welford` and `p99` (sliding percentile) remain selectable. In every Welford mode it compares each real score to:

```text
mean + KITNET_ANOMALY_K_SIGMA * std
```

The verdict remains in `warming_up` until `KITNET_ANOMALY_MIN_SAMPLES` real scores have fed the estimator for the group. Once active:

- scores below or equal to the threshold are emitted as `normal`, with `kitnet_anomaly=false`
- scores above the threshold are emitted as `anomaly`, with `kitnet_anomaly=true`
- anomalous scores are not fed back into the estimator, to limit threshold contamination

## Kafka behavior, read / write

The Python script follows the same delivery contract as `hstree_analysis.py` and `iforest_analysis.py`:

- Avro consumer, through `confluent-kafka`, on `KAFKA_SOURCE_TOPIC`
- Avro producer on `KAFKA_DEST_TOPIC`
- optional JSON producer on `KAFKA_DEST_VIZ_TOPIC`, empty means disabled
- preserve the **Kafka timestamp** when present, meaning the consumed message timestamp is reused on output
- commit offsets **after** successful production, Avro and JSON if enabled

## Important environment variables

### Kafka / Schema Registry

- `BOOTSTRAP_SERVERS`
- `SCHEMA_REGISTRY_URL`, plus Basic Auth options
- `ANALYSE_KITNET_GROUP_ID`: consumer group id, for example `analyse-kitnet`
- `KAFKA_SOURCE_TOPIC`, `KAFKA_DEST_TOPIC`, `KAFKA_DEST_VIZ_TOPIC`

### Topic layout control, optional

- `TOPIC_PARTITIONS` / `TOPIC_RF`: non-blocking sanity check at startup, warning on mismatch

### Seed and runtime behavior

- `ANALYSE_SEED`: global seed, NumPy plus `random`, useful for reproducibility
- `ANALYSE_SCHEMA_WAIT_SEC`: wait for the source Avro schema, `0` means infinite wait
- `ANALYSE_SCHEMA_POLL_INTERVAL_SEC`: polling interval
- `ANALYSE_IDLE_EXIT_SEC`: exit after too much inactivity, `0` means disabled

### KitNet parameters

The exact names can be adapted in code, but the intent is:

- `KITNET_EXCLUDE_FIELDS`
- `KITNET_MAX_SIZE_AE`: maximum size of one subset, one local autoencoder, in KitNET-py, default `10`
- `KITNET_GRACE_FEATURE_MAPPING`: FM grace period, `int`, or `None` to disable, default `None`
- `KITNET_GRACE_ANOMALY_DETECTOR`: AD grace period, default `50000`
- `KITNET_HIDDEN_RATIO`: hidden layer size ratio, for example `0.75`
- `KITNET_LEARNING_RATE`: learning step, for example `0.1`
- `KITNET_ANOMALY_K_SIGMA`: threshold sensitivity, default `3.0`
- `KITNET_ANOMALY_MIN_SAMPLES`: number of real scores per group before activating the verdict, default `50`

Compatible aliases, same values:

- `KITNET_FM_GRACE_PERIOD`: alias of `KITNET_GRACE_FEATURE_MAPPING`
- `KITNET_AD_GRACE_PERIOD`: alias of `KITNET_GRACE_ANOMALY_DETECTOR`

## Performance and GPU requirements

In your context, around 80 features and a Kafka stream, KitNet is generally:

- **CPU-friendly**, because of small autoencoders and simple updates
- suitable for a Python container without a GPU

A GPU is only useful if:

- autoencoders are implemented through a heavy deep learning framework such as Torch or TensorFlow
- and or throughput is extremely high

For an implementation faithful to KitNet, based on lightweight autoencoders, CPU is sufficient.

## Notes

- KitNet assumes that grace periods contain mostly normal behavior. If the stream starts in a strongly abnormal regime, learning may be biased.
- As with HSTree and iForest, state is not persistent. Restarting reinitializes models by group unless explicit persistence is added.

---

This document describes the behavior of the project script `kitnet_analysis_pysad_cpu.py`.

## Kafka-oriented flow mapping

```text
                    +---------------------------+
                    |   Schema Registry (SR)    |
                    |  subject: <topic>-value   |
                    +-------------+-------------+
                                  |
                                  | fetch latest Avro schema
                                  v
+------------------+      poll      +-------------------------+
| Kafka source      |  ----------->  | Consumer (manual commit)|
| topic: source-data |                | group.id = ...          |
+--------+---------+                +-----------+-------------+
         |                                        |
         | Avro record (dict)                     | build feature vector x
         |                                        v
         |                              +-----------------------+
         |                              | KitNET per group      |
         |                              |  model[group]         |
         |                              |  fit+score online     |
         |                              +----------+------------+
         |                                         |
         | enriched record { ..., kitnet_score }   |
         |                                         v
         |                              +-----------------------+
         |                              | Producer (Avro)       |
         |                              | dest-topic: analyse.. |
         |                              | idempotent + acks=all |
         |                              +----------+------------+
         |                                         |
         |                                (optional JSON copy)
         |                                         v
         |                              +-----------------------+
         |                              | Producer (JSON)       |
         |                              | viz-topic: ...-viz  |
         |                              +----------+------------+
         |                                         |
         |                     wait delivery reports (acks)
         |                                         v
         +---------------------------------> commit source offset
                                         (only if Avro + JSON OK)
```
