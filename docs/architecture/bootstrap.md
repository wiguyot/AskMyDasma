# DASMA-KAFKA Bootstrap

This document is a quick orientation note on the current state of the repository.

The reference entry point is [`docker-compose.yml`](../../docker-compose.yml).

## Overview

The repository implements a Dockerized Kafka pipeline to:

- inject a CSV file into Kafka
- compute several streaming anomaly scores
- produce visualization JSON streams
- explain HSTree, iForest or KitNET with independently configured V1/V2 services
- send visualization data to InfluxDB through Telegraf
- display the data in Grafana

## Current Stack

- 3-broker KRaft Kafka cluster: `kafka1`, `kafka2`, `kafka3`
- `schema-registry`
- `kafka-ui`
- `init-topic`
- `producer`
- `hstree-analysis`
- `iforest-analysis`
- `kitnet-analysis`
- `deepant-analysis`
- `mdragstream-point-analysis`
- `consensus-analysis`
- `explainability`
- `explainability-v2`
- `explainability-shap-diff`
- `build-metrics`
- `influxdb`
- `influxdb-init-buckets`
- `influxdb-apply-template`
- `telegraf`
- `grafana`

## Logical Pipeline

- `producer` reads a CSV file and publishes:
  - `source-data` in Avro
  - `source-data-viz` in JSON
- `hstree-analysis` consumes `source-data` and publishes:
  - `hstree-analysis`
  - `hstree-analysis-viz`
- `iforest-analysis` consumes `source-data` and publishes:
  - `iforest-analysis`
  - `iforest-analysis-viz`
- `kitnet-analysis` consumes `source-data` and publishes:
  - `kitnet-analysis`
  - `kitnet-analysis-viz`
- `deepant-analysis` consumes `source-data` and publishes:
  - `deepant-analysis`
  - `deepant-analysis-viz`
- `mdragstream-point-analysis` consumes `source-data` and publishes:
  - `mdragstream-point-analysis`
  - `mdragstream-point-analysis-viz`
- `consensus-analysis` consumes the selected model JSON outputs, joins active model verdicts per `(Group, source_event_id)`, keeps `timestamp` for display, and publishes:
  - `consensus-analysis`
- `explainability` consumes `source-data` in `simple` mode or the selected
  detector output in `reference` mode, and publishes:
  - `explainability`
  - `explainability-viz`
- `explainability-v2` uses the same source-selection rules and publishes
  `explainability-v2` and `explainability-v2-viz`.
- `explainability-shap-diff` compares V1/V2 visualization records when both are enabled.
- `build-metrics` publishes `topic-metrics`
- `telegraf` consumes the JSON topics `*-viz`, `consensus-analysis`, and `topic-metrics`, then writes to `influxdb`
- `grafana` serves the visualization interface

## Managed Topics

The service [`runtime/bootstrap/init-topic/init-topics.py`](../../runtime/bootstrap/init-topic/init-topics.py) creates:

- `source-data`
- `source-data-viz`
- `hstree-analysis`
- `hstree-analysis-viz`
- `iforest-analysis`
- `iforest-analysis-viz`
- `kitnet-analysis`
- `kitnet-analysis-viz`
- `deepant-analysis`
- `deepant-analysis-viz`
- `mdragstream-point-analysis`
- `mdragstream-point-analysis-viz`
- `consensus-analysis`
- `explainability`
- `explainability-viz`
- `topic-metrics`
- `explainability-v2`
- `explainability-v2-viz`
- `repeatability-v1`
- `repeatability-v1-viz`
- `repeatability-v2`
- `repeatability-v2-viz`
- `repeatability-diff`
- `reproducibility-diff`
- `reproducibility-summary`
- `explainability-shap-diff`

## Useful Commands

Recommended startup:

```bash
bash start.sh
```

This command:

- starts the shared infrastructure exposed by `docker-compose.yml`
- launches the AskMyDasma web backend on `http://127.0.0.1:8000/askmydasma/`
- prepares an initial campaign unless `ASKMYDASMA_AUTO_PREPARE_ON_START=0`

Recommended shutdown and cleanup:

```bash
bash stop.sh
```

`stop.sh` stops the AskMyDasma backend, stops the Docker stack, and then removes project volumes. The historical `pip-cache` volume may remain for compatibility, but Compose-managed Python services no longer install dependencies at runtime.

`start.sh` prepares missing images before launching Compose. Public images are
pulled from their registries. Project images declared with a Compose `build:`
block are built locally instead of being pulled, which allows a startup from an
empty Docker image cache.

After Dockerfile or service dependency changes, an explicit rebuild remains the
recommended way to refresh all local runtime images:

```bash
docker compose build
docker compose config --images
```

Then start normally with `bash start.sh`, or use
`docker compose up --pull never -d` for direct Compose runs. Compose reuses the
local image tags on following starts unless the images are rebuilt.

Manual startup of the Docker stack only:

```bash
docker compose up -d
```

In that case, the AskMyDasma backend is not started automatically.

## Exposed Interfaces

- Kafka bootstrap host: `localhost:9092`
- Schema Registry: `http://localhost:8081`
- Kafka UI: `http://localhost:8080`
- InfluxDB: `http://localhost:8086`
- Grafana: `http://localhost:3000`

## Notes

- Minimal Python packaging and an initial test base are already in place.
- For a more detailed architecture description, see [`schema.md`](../../docs/architecture/schema.md).
