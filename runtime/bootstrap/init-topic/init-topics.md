# init-topics.py — Kafka topic initialization

This script is the single entry point used to create **all Kafka topics** required by the **dasma-kafka** project.

It is designed to be launched, most of the time, through the Docker Compose service `init-topic`, and then **stops** once the topics have been created.

## Goal

- Wait until the Kafka cluster is operational (with a minimum number of visible brokers).
- Create the configured topics (data, analysis, consensus, explainability, metrics, and the JSON visualization variants) **if** they do not already exist.

The script is **idempotent**: if a topic already exists, it is not recreated.

## Dependency

The script uses the `confluent-kafka` Admin API:

```bash
python -m pip install confluent-kafka
```

## How it works

1. **DNS pre-check / host resolution**
   - The script parses `BOOTSTRAP_SERVERS` and tries to resolve each hostname.
   - If one or more hosts cannot be resolved, it logs an error and gives a *hint* (for example `kafka-1` → `kafka1`).

2. **Loading topics from environment variables**
   - Topics are defined through `TOPIC_NAME*`, `TOPIC_PARTITIONS*`, `TOPIC_RF*` triplets.
   - The script de-duplicates by name. If a name appears multiple times, the first occurrence is kept.

3. **Waiting for cluster availability**
   - The script waits until the number of visible brokers is **≥ target_brokers**.
   - `target_brokers = max(MIN_BROKERS, max_rf)`
     - `MIN_BROKERS` is an explicit minimum threshold.
     - `max_rf` is the highest replication factor among the configured topics.
   - This avoids trying to create a topic with `RF=3` when only one broker is visible.

4. **Topic creation**
   - For each topic, in stable name-sorted order, it checks whether it already exists.
   - If not, it calls `create_topics()` with `num_partitions` and `replication_factor`.
   - If a topic is created concurrently elsewhere, the race error is logged as a `warning` and then ignored.

## CLI parameters

The script also accepts arguments, which are useful outside Docker or for debugging:

- `--bootstrap-servers` / `-b`: bootstrap server list (default: `BOOTSTRAP_SERVERS` or `localhost:9092`)
- `--min-brokers`: minimum number of visible brokers (default: `MIN_BROKERS` or `1`)
- `--sleep`: delay between retries (default: `SLEEP_SECONDS` or `3`)
- `--log-level`: `DEBUG|INFO|WARNING|ERROR|CRITICAL` (default: `LOG_LEVEL` or `INFO`)

Example:

```bash
python runtime/bootstrap/init-topic/init-topics.py \
  --bootstrap-servers "localhost:9092" \
  --min-brokers 3 \
  --sleep 2 \
  --log-level INFO
```

## Environment variables

### Connection / behavior

- `BOOTSTRAP_SERVERS` (default: `localhost:9092`)
- `MIN_BROKERS` (default: `1`)
- `SLEEP_SECONDS` (default: `3`)
- `LOG_LEVEL` (default: `INFO`)

### Topic definition

Each topic is defined by a triplet:

- `TOPIC_NAME<N>`: topic name
- `TOPIC_PARTITIONS<N>`: number of partitions
- `TOPIC_RF<N>`: replication factor

The expected mapping in this project is:

| Index | Default name | Variables | Role |
|---:|---|---|---|
| 1 | `source-data` | `TOPIC_NAME1`, `TOPIC_PARTITIONS1`, `TOPIC_RF1` | Raw component data |
| 9 | `source-data-viz` | `TOPIC_NAME9`, `TOPIC_PARTITIONS9`, `TOPIC_RF9` | (Optional) `source-data` visualization stream |
| 2 | `hstree-analysis` | `TOPIC_NAME2`, `TOPIC_PARTITIONS2`, `TOPIC_RF2` | HSTree analysis results |
| 4 | `hstree-analysis-viz` | `TOPIC_NAME4`, `TOPIC_PARTITIONS4`, `TOPIC_RF4` | (Optional) HSTree visualization stream |
| 7 | `iforest-analysis` | `TOPIC_NAME7`, `TOPIC_PARTITIONS7`, `TOPIC_RF7` | iForest analysis results |
| 8 | `iforest-analysis-viz` | `TOPIC_NAME8`, `TOPIC_PARTITIONS8`, `TOPIC_RF8` | (Optional) iForest visualization stream |
| 10 | `kitnet-analysis` | `TOPIC_NAME10`, `TOPIC_PARTITIONS10`, `TOPIC_RF10` | KitNET analysis results |
| 11 | `kitnet-analysis-viz` | `TOPIC_NAME11`, `TOPIC_PARTITIONS11`, `TOPIC_RF11` | (Optional) KitNET visualization stream |
| 12 | `consensus-analysis` | `TOPIC_NAME12`, `TOPIC_PARTITIONS12`, `TOPIC_RF12` | Consensus anomaly count stream |
| 3 | `explainability` | `TOPIC_NAME3`, `TOPIC_PARTITIONS3`, `TOPIC_RF3` | Explainability results |
| 6 | `explainability-viz` | `TOPIC_NAME6`, `TOPIC_PARTITIONS6`, `TOPIC_RF6` | (Optional) explainability visualization stream |
| 5 | `topic-metrics` | `TOPIC_NAME5`, `TOPIC_PARTITIONS5`, `TOPIC_RF5` | Internal metrics |
| 13 | `deepant-analysis` | `TOPIC_NAME13`, `TOPIC_PARTITIONS13`, `TOPIC_RF13` | Analysis, explanation or validation stream |
| 14 | `deepant-analysis-viz` | `TOPIC_NAME14`, `TOPIC_PARTITIONS14`, `TOPIC_RF14` | JSON visualization stream |
| 15 | `mdragstream-point-analysis` | `TOPIC_NAME15`, `TOPIC_PARTITIONS15`, `TOPIC_RF15` | Analysis, explanation or validation stream |
| 16 | `mdragstream-point-analysis-viz` | `TOPIC_NAME16`, `TOPIC_PARTITIONS16`, `TOPIC_RF16` | JSON visualization stream |
| 17 | `explainability-v2` | `TOPIC_NAME17`, `TOPIC_PARTITIONS17`, `TOPIC_RF17` | Analysis, explanation or validation stream |
| 18 | `explainability-v2-viz` | `TOPIC_NAME18`, `TOPIC_PARTITIONS18`, `TOPIC_RF18` | JSON visualization stream |
| 19 | `repeatability-v1` | `TOPIC_NAME19`, `TOPIC_PARTITIONS19`, `TOPIC_RF19` | Analysis, explanation or validation stream |
| 20 | `repeatability-v1-viz` | `TOPIC_NAME20`, `TOPIC_PARTITIONS20`, `TOPIC_RF20` | JSON visualization stream |
| 21 | `repeatability-v2` | `TOPIC_NAME21`, `TOPIC_PARTITIONS21`, `TOPIC_RF21` | Analysis, explanation or validation stream |
| 22 | `repeatability-v2-viz` | `TOPIC_NAME22`, `TOPIC_PARTITIONS22`, `TOPIC_RF22` | JSON visualization stream |
| 23 | `repeatability-diff` | `TOPIC_NAME23`, `TOPIC_PARTITIONS23`, `TOPIC_RF23` | Analysis, explanation or validation stream |
| 24 | `reproducibility-diff` | `TOPIC_NAME24`, `TOPIC_PARTITIONS24`, `TOPIC_RF24` | Analysis, explanation or validation stream |
| 25 | `reproducibility-summary` | `TOPIC_NAME25`, `TOPIC_PARTITIONS25`, `TOPIC_RF25` | Analysis, explanation or validation stream |
| 26 | `explainability-shap-diff` | `TOPIC_NAME26`, `TOPIC_PARTITIONS26`, `TOPIC_RF26` | Analysis, explanation or validation stream |

> Note: the **default** names apply only when the `TOPIC_NAME<N>` variable is not defined.

## Execution examples

### Inside the Docker Compose network

From a container on the `kafka-net` network, use service names:

```bash
BOOTSTRAP_SERVERS="kafka1:9092,kafka2:9092,kafka3:9092" python runtime/bootstrap/init-topic/init-topics.py
```

### On the host

If port `9092` is published to the host:

```bash
BOOTSTRAP_SERVERS="localhost:9092" python runtime/bootstrap/init-topic/init-topics.py
```

## Troubleshooting

### I do not see brokers / “Kafka not ready yet” loop

- Check that `BOOTSTRAP_SERVERS` points to an address that is actually reachable from **the container** or from **the host**, depending on where the script is executed.
- If you run it on the host, use `localhost:9092` or the published port, not `kafka1:9092`.

### Unresolvable hostname (`kafka-1`, `kafka-2`, …)

The script explicitly logs unresolved hosts and often suggests the expected form in this project (`kafka1`, `kafka2`, `kafka3`).

### RF > number of brokers

If `TOPIC_RF<N>=3` but only one broker is visible, the script automatically waits for **at least 3 brokers** (`target_brokers = max(MIN_BROKERS, max_rf)`).

---

*Documented file: `runtime/bootstrap/init-topic/init-topics.py`.*
