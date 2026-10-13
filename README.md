# AskMyDasma

## What This Project Does

AskMyDasma is the public name of this project and its web interface.
The repository and Python package keep the technical name `dasma-kafka`, also
written as DASMA-Kafka in earlier documentation.

This repository adapts the DASMA project to a modern information-flow architecture: CSV data is streamed through Kafka, anomaly-detection and explainability algorithms are containerized as independent services, and observability is handled through Telegraf, InfluxDB, and Grafana.

To launch the local platform with the included sample:

```bash
bash start.sh
```

Then open the user workspace:

- AskMyDasma: `http://127.0.0.1:8000/askmydasma/`

From there, choose the dataset, select the analysis models, prepare the campaign, and start production.

The operational reference for the repository is [`docker-compose.yml`](docker-compose.yml).

For the curated user and technical documentation, start with the
[public documentation index](docs/public/README.md). Dated [validation reports](experiments/README.md)
describe the datasets and configurations used for individual studies.

See [acknowledgments and algorithm references](docs/public/ACKNOWLEDGMENTS.md)
for project credits and the scientific sources for the detectors.
If you use the software in research, see [`CITATION.cff`](CITATION.cff) for
the software citation. Please cite AskMyDasma and its authors in publications
that use it. The project is released under the [MIT License](LICENSE).

To report a problem, suggest a feature, or contribute a change, see
[`CONTRIBUTING.md`](CONTRIBUTING.md). GitHub issue forms are provided for bug
reports and feature requests when this repository is hosted on GitHub.

## Prerequisites

All analysis services and, by default, the AskMyDasma web backend run inside Docker
containers. The launch/image-preparation scripts still call host `python3` for
small JSON utilities: keep it available even in container mode. Python ≥ 3.12 and
the project dependencies are required for local development, tests, offline Python
tools or the optional `host` backend mode, not for hosting the default container
backend. Docker images are the runtime reference.

The following ports must be free on the host:

| Port | Service |
|------|---------|
| 8000 | AskMyDasma web backend |
| 3000 | Grafana |
| 8086 | InfluxDB |
| 9092 | Kafka |
| 8081 | Schema Registry |
| 8080 | Kafka UI |

### Mac — Apple Silicon (M1 / M2 / M3 / M4)

- [Docker Desktop for Mac (Apple Silicon)](https://www.docker.com/products/docker-desktop/) — includes Docker Engine and Docker Compose
- Host `python3` for launch helpers; Python ≥ 3.12 for development/host mode — via [python.org](https://www.python.org/downloads/) or `brew install python@3.12`
- `bash` and `curl`; install Apple Command Line Tools if `git` is unavailable

### Linux (x86_64)

- Docker Engine ≥ 24 — via the [official install script](https://docs.docker.com/engine/install/) for your distribution
- Docker Compose plugin (`docker compose`) — included in recent Docker Engine packages
- Host `python3` for launch helpers; Python ≥ 3.12 for development/host mode — via your distribution's package manager (`apt`, `dnf`, …) or [python.org](https://www.python.org/downloads/)
- `git`, `bash`, `curl` — install via your package manager if not already present

### Windows

Docker on Windows requires WSL 2 (Windows Subsystem for Linux). The launch scripts (`start.sh` and `stop.sh`) are Bash scripts and must be run inside WSL 2.

- [Docker Desktop for Windows](https://www.docker.com/products/docker-desktop/) with the **WSL 2 backend** enabled
- [WSL 2](https://learn.microsoft.com/windows/wsl/install) with a Linux distribution (Ubuntu 22.04 or later recommended)
- Inside WSL 2: `python3`, `git`, `bash`, `curl`; Python ≥ 3.12 for development/host mode — install via `apt`

All commands (`bash start.sh`, `bash stop.sh`, etc.) must be run from a WSL 2 terminal, not from PowerShell or Command Prompt.

## Repository Organization

- application code:
  - [`producer/`](producer)
  - [`runtime/detectors/hstree/`](runtime/detectors/hstree)
  - [`runtime/detectors/iforest/`](runtime/detectors/iforest)
  - [`runtime/detectors/kitnet/`](runtime/detectors/kitnet)
  - [`runtime/detectors/deepant/`](runtime/detectors/deepant)
  - [`runtime/detectors/mdragpoint/`](runtime/detectors/mdragpoint)
  - [`runtime/consensus/`](runtime/consensus)
  - [`runtime/explainability/regular/`](runtime/explainability/regular)
  - [`runtime/explainability/v2/`](runtime/explainability/v2)
  - [`docs/models/explainability/`](docs/models/explainability) (documentation and references)
  - [`experiments/explainability/`](experiments/explainability) (offline tools and reports)
  - [`src/dasma_kafka/`](src/dasma_kafka)
- structured documentation:
  - [`docs/`](docs)
  - [`docs/architecture/`](docs/architecture)
  - [`docs/observability/`](docs/observability)
- scripts:
  - main runner: [`scripts/run_integration_smoke.py`](scripts/run_integration_smoke.py)
  - shell wrappers: [`scripts/integration/`](scripts/integration)
  - web control: [`scripts/web/`](scripts/web)
- AskMyDasma web frontend:
  - [`frontend/askmydasma/`](frontend/askmydasma)
  - [`frontend/askmydasma/admin/`](frontend/askmydasma/admin)
## Repository Layout Conventions

What remains at the repository root:

- [`docker-compose.yml`](docker-compose.yml)
  Main entry point to launch the application.
- [`README.md`](README.md)
  Documentation entry point for the repository.
- [`pyproject.toml`](pyproject.toml)
  Standard Python project configuration.
- [`start.sh`](start.sh)
  Starts the shared infrastructure and the AskMyDasma web backend.
- [`stop.sh`](stop.sh)
  Stops the local environment and cleans project volumes. The historical
  `pip-cache` volume may remain for compatibility, but Compose-managed Python
  services no longer install dependencies at runtime.
- The curated documentation entry point is [`docs/public/README.md`](docs/public/README.md).

What should no longer clutter the root:

- architecture, observability and model documentation (use `docs/`;
  the curated user documentation starts at [`docs/public/`](docs/public/))
- shell wrappers for tests
- historical Docker Compose variants

Where to look for what:

- documentation: [`docs/`](docs)
- scripts: [`scripts/`](scripts)
- shared Python code: [`src/dasma_kafka/`](src/dasma_kafka)
- tests: [`tests/`](tests)

## Reference Architecture

The main Compose file deploys:

- a 3-broker KRaft Kafka cluster: `kafka1`, `kafka2`, `kafka3`
- `schema-registry`
- `kafka-ui`
- `producer`
- `hstree-analysis`
- `iforest-analysis`
- `kitnet-analysis`
- `deepant-analysis`
- `mdragstream-point-analysis`
- `repeatability-v1`, `repeatability-v2`, and `repeatability-diff` (started only
  for a repeatability campaign)
- `consensus-analysis`
- `explainability`
- `explainability-v2`
- `explainability-shap-diff` (automatically selected when both Explainability streams are active)
- `reproducibility-export` (only for a reproducibility campaign)
- `reproducibility-publish` (manual publication of a completed CSV comparison)
- `askmydasma-web` (default backend, behind the `askmydasma-container` profile)
- `build-metrics`
- `influxdb`
- `influxdb-init-buckets`
- `influxdb-apply-template`
- `telegraf`
- `grafana`

## Data Flow

```text
CSV
  -> producer
  -> source-data (Avro)
     -> hstree-analysis -> hstree-analysis + hstree-analysis-viz
     -> iforest-analysis -> iforest-analysis + iforest-analysis-viz
     -> kitnet-analysis -> kitnet-analysis + kitnet-analysis-viz
     -> deepant-analysis -> deepant-analysis + deepant-analysis-viz
     -> mdragstream-point-analysis -> mdragstream-point-analysis + mdragstream-point-analysis-viz

selected model *-viz topics
  -> consensus-analysis

source-data (independent) OR selected detector *-analysis (against detector)
  -> explainability V1 -> explainability + explainability-viz
  -> explainability V2 -> explainability-v2 + explainability-v2-viz

explainability-viz + explainability-v2-viz (when both services are active)
  -> explainability-shap-diff -> explainability-shap-diff (precomputed JSON differences)

build-metrics
  -> topic-metrics

topics *-viz + explainability-shap-diff + consensus-analysis + topic-metrics
  -> telegraf
  -> influxdb
  -> grafana

repeatability campaign for one selected model
  -> repeatability-v1 + repeatability-v2 (same image, parameters, and seed)
  -> repeatability-v1-viz + repeatability-v2-viz
  -> repeatability-diff (event-time comparison by Group + source_event_id)
  -> telegraf -> repeatability InfluxDB buckets -> Grafana

reproducibility campaign for one selected model (normal campaign + CSV evidence)
  -> reproducibility-export (writes <model>-source-<1|2>.csv + metadata under
     data/reproducibility/<campaign-id>/)
  -> python validation/reproducibility/compare_csv.py (offline merge join, report + summary)
  -> docker compose run --rm --no-deps reproducibility-publish \
       --report /app/data/reproducibility/<campaign-id>/<model>-comparison.csv
  -> reproducibility-diff + reproducibility-summary topics
  -> telegraf -> reproducibility InfluxDB buckets -> Grafana (Reproducibility)
```

## Topics Created by `init-topic`

The service [`runtime/bootstrap/init-topic/init-topics.py`](runtime/bootstrap/init-topic/init-topics.py) idempotently creates the following topics:

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
- `explainability-v2`
- `explainability-v2-viz`
- `topic-metrics`
- `repeatability-v1`
- `repeatability-v1-viz`
- `repeatability-v2`
- `repeatability-v2-viz`
- `repeatability-diff`
- `reproducibility-diff`
- `reproducibility-summary`
- `explainability-shap-diff`

By default in the main Compose file:

- partitions: `1`
- replication factor: `3`

## Application Services

- [`producer/producer.py`](producer/producer.py)
  Reads a CSV file, infers an Avro schema dynamically, publishes to `source-data`, and sends JSON to `source-data-viz`.
- [`runtime/detectors/hstree/hstree_analysis.py`](runtime/detectors/hstree/hstree_analysis.py)
  Consumes `source-data`, computes `hstree_score`, and republishes in Avro and JSON.
  Both `pysad` and `paper` variants are implemented. `pysad` delegates the tree
  implementation to PySAD; `paper` uses the repository's paper-oriented implementation.
  See the [implementation differences and limits](docs/models/hstree/).
- [`runtime/detectors/iforest/iforest_analysis.py`](runtime/detectors/iforest/iforest_analysis.py)
  Consumes `source-data`, computes `iforest_score`, and republishes in Avro and JSON.
- [`runtime/detectors/kitnet/kitnet_analysis_pysad_cpu.py`](runtime/detectors/kitnet/kitnet_analysis_pysad_cpu.py)
  Consumes `source-data`, computes `kitnet_score`, and republishes in Avro and JSON.
- [`runtime/detectors/deepant/deepant_analysis.py`](runtime/detectors/deepant/deepant_analysis.py)
  Consumes `source-data`, runs the DeepAnT CNN predictor, computes `deepant_score`, `deepant_status`, `deepant_anomaly`, and `deepant_threshold`, and republishes in Avro and JSON. By default it runs **parallel-by-group** (`DEEPANT_PARALLEL_BY_GROUP=1`) over a bounded worker pool: groups are spread across at most `DEEPANT_MAX_GROUP_WORKERS` (default `2`) processes by stable hash, so a worker may own several groups and any number of groups is supported, while preserving publish-before-commit; set `DEEPANT_PARALLEL_BY_GROUP=0` for the sequential mode. See the [DeepAnT service guide](services/deepant/README.md) and [scientific limits](runtime/detectors/deepant/scientific-validity.md).
- [`runtime/detectors/mdragpoint/mdragstream_point_analysis.py`](runtime/detectors/mdragpoint/mdragstream_point_analysis.py)
  Consumes `source-data`, runs the mDragStream Point-based model, computes `mdragpoint_score`, `mdragpoint_status`, `mdragpoint_anomaly`, `mdragpoint_threshold`, and `mdragpoint_internal_r`, and republishes in Avro and JSON. By default, the verdict uses mDragStream's native Point-based anomaly flag; the external P99/Welford threshold mode remains available through `MDRAGPOINT_VERDICT_MODE=external`.
- [`runtime/consensus/consensus_analysis.py`](runtime/consensus/consensus_analysis.py)
  Consumes selected model JSON outputs and joins active model verdicts per `(Group, source_event_id)`: it accumulates the models' votes for the same source event, keeps `timestamp` for Grafana display, and publishes `anomaly_count` to `consensus-analysis` once the event is complete or its timeout/watermark expires (late models handled by `CONSENSUS_LATE_POLICY`).
- [`runtime/explainability/regular/explainability-multi-thread-by-group.py`](runtime/explainability/regular/explainability-multi-thread-by-group.py)
  Explainability V1: consumes the configured source, reconstructs the selected
  local model per group, computes SHAP, and republishes in Avro and JSON.
- [`runtime/explainability/v2/explainability_v2.py`](runtime/explainability/v2/explainability_v2.py)
  Explainability V2: the algorithmic reference, with independently configured
  model/mode/method and separate topics. V1 is its twin only under comparable configurations.
- [`validation/explainability/shap_diff_service.py`](validation/explainability/shap_diff_service.py)
  Joins the two JSON streams by event identity and publishes per-feature differences,
  avoiding the former expensive SHAP joins in Grafana.
- [`observability/metrics/build-metrics.py`](observability/metrics/build-metrics.py)
  Periodically measures offsets for tracked topics and publishes a JSON snapshot to `topic-metrics`.

## mDragStream Point-based Documentation

- beginner-friendly read: [`runtime/detectors/mdragpoint/mdragpoint-for-beginners.md`](runtime/detectors/mdragpoint/mdragpoint-for-beginners.md)
- technical read: [`runtime/detectors/mdragpoint/mdragstream_point_analysis.md`](runtime/detectors/mdragpoint/mdragstream_point_analysis.md)
- active Kafka service: [`runtime/detectors/mdragpoint/mdragstream_point_analysis.py`](runtime/detectors/mdragpoint/mdragstream_point_analysis.py)
- model wrapper: [`runtime/detectors/mdragpoint/models/mdragpoint.py`](runtime/detectors/mdragpoint/models/mdragpoint.py)

## Explainability Documentation

**Explainability V1 and V2 are implemented and independently configurable.** V2
is the algorithmic reference; V1 (historically "regular") offers the same model/mode
capabilities. Their services, topics, consumer groups and dashboards remain separate.
See the [Explainability guide](docs/public/explainability.md) for current
capabilities, operating modes, and evidence limits.

Each Explainability service is selected independently in AskMyDasma. When V1 or
V2 is enabled, its model, mode and method are mandatory. HSTree and IForest support
KernelSHAP in **`independent`** mode (internal value `simple`: consume `source-data`
and explain the local score) or **`against detector`** mode (internal value
`reference`: consume the matching detector topic and verify score fidelity before SHAP).
KitNET is available with KernelSHAP in both Explainability model selectors; the UI
warns that it has a very high compute cost. IForest also supports TreeExplainer
with the interventional feature-perturbation method. It explains mean path length
rather than the anomaly-score scale, using the forest that produced each event's
score across refits.
The [iForest implementation guide](docs/models/iforest/paper-conformity.md) describes the streaming adaptation; its
campaign evidence describes the validated runtime. A direct Docker launch without these
variables fails explicitly; a disabled service remains idle. The V1/V2
comparison dashboard remains available, but its differences are not a proof
when the two services use different models or methods.

- beginner-friendly read: [`docs/models/explainability/regular/explainability-for-beginners.md`](docs/models/explainability/regular/explainability-for-beginners.md)
- technical read: [`docs/models/explainability/regular/explainability.md`](docs/models/explainability/regular/explainability.md)
- scientific interpretation limits: [`docs/models/explainability/regular/explainability-limits.md`](docs/models/explainability/regular/explainability-limits.md)
- KitNET explainability scope and campaign interpretation: [`docs/models/explainability/kitnet-explainability-scope.md`](docs/models/explainability/kitnet-explainability-scope.md)
- historical compatibility: [`runtime/explainability/regular/explainability.py`](runtime/explainability/regular/explainability.py)
  Old V1 entry point kept as a wrapper. The active V1 entry point is [`runtime/explainability/regular/explainability-multi-thread-by-group.py`](runtime/explainability/regular/explainability-multi-thread-by-group.py); V2 is the algorithmic reference.
- out-of-pipeline utilities:
  - [`experiments/explainability/regular/benchmark_shap.py`](experiments/explainability/regular/benchmark_shap.py)
  - [`experiments/explainability/regular/analyze_logs.py`](experiments/explainability/regular/analyze_logs.py)
  - [`experiments/explainability/v2/shap_stability_runner.py`](experiments/explainability/v2/shap_stability_runner.py)
    Historical offline study; its SHAP call requires adaptation to the current API. It is not used by the UI or production services.
  - [Dated validation evidence](experiments/README.md)
- Python modules:
  - [`src/dasma_kafka/explainability/config.py`](src/dasma_kafka/explainability/config.py)
  - [`src/dasma_kafka/explainability/features.py`](src/dasma_kafka/explainability/features.py)
  - [`src/dasma_kafka/explainability/model.py`](src/dasma_kafka/explainability/model.py)
  - [`src/dasma_kafka/explainability/shap_engine.py`](src/dasma_kafka/explainability/shap_engine.py)
  - [`src/dasma_kafka/explainability/runtime.py`](src/dasma_kafka/explainability/runtime.py)

## Observability

- [`observability/telegraf/telegraf.conf`](observability/telegraf/telegraf.conf) consumes:
  - `topic-metrics`
  - `source-data-viz`
  - `hstree-analysis-viz`
  - `iforest-analysis-viz`
  - `kitnet-analysis-viz`
  - `deepant-analysis-viz`
  - `mdragstream-point-analysis-viz`
  - `explainability-viz`
  - `explainability-v2-viz`
  - `explainability-shap-diff`
  - `consensus-analysis`
  - `repeatability-v1-viz`
  - `repeatability-v2-viz`
  - `repeatability-diff`
  - `reproducibility-diff`
  - `reproducibility-summary`
- InfluxDB stores measurements in several buckets.
- Grafana is provisioned through:
  - [`observability/grafana/provisioning/datasources/kafka.yml`](observability/grafana/provisioning/datasources/kafka.yml)
  - [`observability/grafana/provisioning/datasources/influxdb.yml`](observability/grafana/provisioning/datasources/influxdb.yml)
  - [`observability/grafana/provisioning/dashboards/dashboards.yml`](observability/grafana/provisioning/dashboards/dashboards.yml)
- Available Grafana dashboards:
  - [`observability/grafana/dashboards/hstree_metrics.json`](observability/grafana/dashboards/hstree_metrics.json)
  - [`observability/grafana/dashboards/By model/source-data_hstree_influxdb.json`](observability/grafana/dashboards/By%20model/source-data_hstree_influxdb.json)
  - [`observability/grafana/dashboards/By model/source-data_iforest_influxdb.json`](observability/grafana/dashboards/By%20model/source-data_iforest_influxdb.json)
  - [`observability/grafana/dashboards/By model/source-data_kitnet_influxdb.json`](observability/grafana/dashboards/By%20model/source-data_kitnet_influxdb.json)
  - [`observability/grafana/dashboards/By model/source-data_deepant_influxdb.json`](observability/grafana/dashboards/By%20model/source-data_deepant_influxdb.json)
  - [`observability/grafana/dashboards/By model/source-data_mdragpoint_influxdb.json`](observability/grafana/dashboards/By%20model/source-data_mdragpoint_influxdb.json)
  - [`observability/grafana/dashboards/anomaly_analysis_overview_influxdb.json`](observability/grafana/dashboards/anomaly_analysis_overview_influxdb.json)
  - [`observability/grafana/dashboards/Explainability/explainability_influxdb.json`](observability/grafana/dashboards/Explainability/explainability_influxdb.json)
  - [`observability/grafana/dashboards/Explainability/explainability_v2_influxdb.json`](observability/grafana/dashboards/Explainability/explainability_v2_influxdb.json)
  - [`observability/grafana/dashboards/Explainability/explainability_compare_influxdb.json`](observability/grafana/dashboards/Explainability/explainability_compare_influxdb.json)
  - [`observability/grafana/dashboards/Explainability/explainability_iforest_influxdb.json`](observability/grafana/dashboards/Explainability/explainability_iforest_influxdb.json)
  - [`observability/grafana/dashboards/Explainability/explainability_v2_iforest_influxdb.json`](observability/grafana/dashboards/Explainability/explainability_v2_iforest_influxdb.json)
  - [`observability/grafana/dashboards/Explainability/explainability_kitnet_influxdb.json`](observability/grafana/dashboards/Explainability/explainability_kitnet_influxdb.json)
  - [`observability/grafana/dashboards/Explainability/explainability_v2_kitnet_influxdb.json`](observability/grafana/dashboards/Explainability/explainability_v2_kitnet_influxdb.json)
  - [`observability/grafana/dashboards/repeatability_influxdb.json`](observability/grafana/dashboards/repeatability_influxdb.json)
  - [`observability/grafana/dashboards/reproducibility_influxdb.json`](observability/grafana/dashboards/reproducibility_influxdb.json)
- The mDragStream Point-based panels show the yellow `mdragpoint_score` line and red native-anomaly markers derived from `mdragpoint_status == "anomaly"`. They intentionally do not display `mdragpoint_threshold` or `mdragpoint_internal_r` by default.

## Repeatability (local phase 1)

AskMyDasma can run a local repeatability test for exactly one anomaly model:
HSTree, IForest, KitNET, DeepAnT, or mDragStream Point-based. Select the model,
then enable its **Repeatability** checkbox. The campaign launches two isolated
instances with the same image, parameters, seed, pinned native-thread settings,
and input order.

Every standard parameter exposed for the selected model is part of this shared
campaign configuration: model windows, threshold/estimator and warmup settings,
and the model-specific controls (for example HSTree trees, IForest refits,
KitNET learning settings, DeepAnT training settings, or mDragStream cluster and
LOF settings). AskMyDasma exports the selected values to both instances before
they start. The configuration is therefore fixed for a run: changing a form
field after production has started requires preparing or relaunching the
campaign; it does not reconfigure running containers.

The normal service of the tested model is replaced by `repeatability-v1`,
`repeatability-v2`, and `repeatability-diff`. Its normal kill switch and
`consensus-analysis` are disabled for the test so no normal-model consumer
interferes with the dedicated topics. The test is dedicated to that single
model: no other anomaly model, Explainability, or Explainability v2 can stay
selected or run during a repeatability campaign. A configuration that keeps
another model selected — for example one waiting for the deliberately absent
`hstree-analysis` topic — is rejected before launch, even if the request
bypasses the interface. Unchecking **Repeatability** is the action that
restores ordinary multi-model selection; it does not re-check the models that
were deselected.

The `Repeatability` Grafana dashboard uses `Group` and source-component filters:

1. **Source components by group** gives the raw input context.
2. **Global anomaly score per event — v1 vs v2** plots the two event-aligned
   scores from `repeatability-diff`; identical runs naturally overlap. Warmup
   events are retained and marked rather than filtered out.
3. **Global score difference per event** shows `score_diff` and `abs_diff`.
   The verdict, summary statistics, and latest event table complete the view.

For complete events, `identical = 1` and `abs_diff = 0` are the expected result.
This establishes local repeatability. The CSV-based cross-machine protocol
has also been exercised on a 4,999-event HSTree pilot. Broader qualification
and its Grafana publication check remain outside that measured scope.

## Reproducibility

### Cross-machine reproducibility (phase 2)

AskMyDasma can also produce a cross-machine reproducibility proof for exactly
one anomaly model. Select the model, enable its **Reproducibility**
checkbox, choose the role (`source-1` or `source-2`) and enter a portable
`campaign-id`. The campaign stays a normal campaign — the model's regular
service, `consensus-analysis`, `build-metrics` and `telegraf` keep running —
plus one additional consumer, `reproducibility-export`, which writes the
evidence pair under `data/reproducibility/<campaign-id>/`:

- `<model>-source-<1|2>.csv` (CSV format v1)
- `<model>-source-<1|2>.metadata.json`

A campaign reset never deletes `data/reproducibility/`. After both roles have
been exported (locally or on two machines), compare them offline — no Kafka,
no Grafana:

```sh
python validation/reproducibility/compare_csv.py \
  --source-1 data/reproducibility/<campaign-id>/<model>-source-1.csv \
  --source-2 data/reproducibility/<campaign-id>/<model>-source-2.csv \
  --report data/reproducibility/<campaign-id>/<model>-comparison.csv \
  --abs-tolerance 0.0 --relative-tolerance 0.0
```

The comparison report and its `<...>.summary.json` are the evidence. To
publish a completed comparison to the observability stack (manual, one-shot,
never automatic):

```sh
docker compose run --rm --no-deps reproducibility-publish \
  --report /app/data/reproducibility/<campaign-id>/<model>-comparison.csv
```

This streams one flat JSON event per report row to `reproducibility-diff` and a
single batch summary to `reproducibility-summary` (idempotent producer,
deterministic keys). Telegraf stores them in the two dedicated InfluxDB
buckets, and the `Reproducibility` dashboard
([`observability/grafana/dashboards/reproducibility_influxdb.json`](observability/grafana/dashboards/reproducibility_influxdb.json))
shows exact / within-tolerance / divergent / missing counters, per-event score
differences, the execution-time offset diagnostic (never a model divergence)
and the first divergence/missing markers.

`within_tolerance` is a second, explicitly labelled indicator for small
cross-machine floating-point drift; it is never presented as exact. The local
CSV report and summary remain the authoritative evidence — the Kafka topics,
InfluxDB buckets and dashboard are a reconstructible view, strictly separated
from every `repeatability-*` resource.

The repository improves reproducibility in two ways:

- the main images now use explicit tags, for example:
  - `python:3.12.13-slim-trixie`
  - `grafana/grafana:12.0.0`
  - the other main Kafka / Influx / Telegraf components are also tagged
- Python dependencies are pinned
- the first prebuilt runtime image lives in
  [`services/anomaly-light`](services/anomaly-light) and is used by
  `hstree-analysis` and `kitnet-analysis`
- the lightweight Kafka utility image lives in
  [`services/kafka-tools`](services/kafka-tools) and is used by `init-topic`,
  `build-metrics`, and `consensus-analysis`
- the scikit-learn anomaly image lives in
  [`services/anomaly-sklearn`](services/anomaly-sklearn) and is used by
  `iforest-analysis` (scikit-learn) and `mdragstream-point-analysis` (PyOD)
- the SHAP runtime image lives in
  [`services/explainability`](services/explainability) and is used by
  `explainability` and `explainability-v2`
- the PyTorch runtime image lives in [`services/deepant`](services/deepant) and
  is used only by `deepant-analysis`

This reproducibility level remains intermediate:

- containers remain independent
- the Python services managed by `docker-compose.yml` no longer install
  packages at container startup
- DeepAnT remains intentionally isolated, so `torch` is not pulled into any
  other runtime image

### Runtime Image Workflow

`start.sh` prepares the runtime images needed by the selected Compose services.
For images pulled from public registries, it runs `docker pull` when the image is
missing locally. For project images declared with a Compose `build:` block, such
as `dasma-kafka-kafka-tools:0.1.0`, it runs `docker compose build <service>`
instead of trying to pull a private/local tag. This makes a clean Docker setup
work after images and volumes have been purged. Every build image is prepared on
the host here, because the AskMyDasma orchestrator runs inside a container that
cannot build images (no Buildx, and the build context path is unavailable inside
the container); the orchestrator only ever starts pre-built images.

After Dockerfile or Python dependency changes, rebuilding explicitly is still the
recommended way to refresh all local project images:

```bash
docker compose build
```

Then start the application normally:

```bash
bash start.sh
```

For direct Compose runs, keep the same separation between build and runtime:

```bash
docker compose up --pull never -d
```

`docker compose up --build ...` remains acceptable for one-off local checks, but
the operational workflow is to let `start.sh` prepare missing images, or build
explicitly first when dependencies changed. On the next startup, Compose reuses
the local image tags unless a Dockerfile, requirement file, or explicit build
command changes them.

## Startup

Prerequisites:

- Docker
- Docker Compose

Recommended command:

```bash
bash start.sh
```

What `start.sh` launches:

- the Kafka / Influx / Grafana infrastructure
- the observation interfaces
- the AskMyDasma web backend (containerized by default — see [Backend Modes](#backend-modes))

What `start.sh` does not launch:

- `producer`
- `hstree-analysis`
- `iforest-analysis`
- `kitnet-analysis`
- `deepant-analysis`
- `consensus-analysis`
- `explainability`
- `explainability-v2`
- `explainability-shap-diff`
- `mdragstream-point-analysis`
- `build-metrics`
- `telegraf`

These compute components are only launched when production starts, after the source file and parameters are prepared from the web application.

Recommended usage order:

1. run `bash start.sh`
2. open the AskMyDasma web application
3. choose the dataset and campaign parameters
   The default mode proposed by the interface and startup preparation is `realtime-x100`.
4. launch the campaign from the interface

Recommended shutdown:

```bash
bash stop.sh
```

`stop.sh` stops the AskMyDasma web backend (host process or container), runs `docker compose down --remove-orphans`, and removes project volumes. The historical `pip-cache` volume may remain for compatibility, but the Compose-managed Python services no longer depend on runtime package installation.

### Backend Modes

The AskMyDasma backend can run two ways. The mode is selected with the
`ASKMYDASMA_BACKEND_MODE` environment variable read by `start.sh`.

- **`container` (default)** — the backend runs as the `askmydasma-web` Docker
  service. It is built from [`services/askmydasma/Dockerfile`](services/askmydasma/Dockerfile)
  (Python + Docker CLI + Compose plugin) and is started behind the
  `askmydasma-container` Compose profile. This is the target operational mode and
  works identically on Linux, macOS and Windows.
- **`host`** — the backend runs as a local uvicorn process on the host (the
  historical mode). Useful for fast iteration on the backend itself.

```bash
bash start.sh                           # container mode (default)
ASKMYDASMA_BACKEND_MODE=host bash start.sh   # host mode
```

The backend orchestrates the platform by calling `docker compose` (start, stop,
reset, relaunch campaigns). In container mode it does so through the mounted
Docker socket.

> **⚠️ Docker socket security.** In container mode the `askmydasma-web` service
> mounts `/var/run/docker.sock`. This grants the container full control over the
> Docker host — it is equivalent to root access on the machine. This is
> acceptable for **local development only** and must not be enabled on a shared
> or exposed host.

To run AskMyDasma in a container without `start.sh`:

```bash
docker compose --profile askmydasma-container up askmydasma-web
```

Manual startup of the full Docker stack:

```bash
docker compose up -d
```

This mode is no longer the recommended way to use the web application because it also starts the compute components before the dataset is selected.

## Validation

Unit tests:

```bash
bash scripts/tests/run-unit-tests.sh
```

Prebuilt image check after packaging changes:

```bash
docker compose build
docker compose config --images
```

Integration runner:

```bash
python3 scripts/run_integration_smoke.py hstree
python3 scripts/run_integration_smoke.py metrics
python3 scripts/run_integration_smoke.py iforest
python3 scripts/run_integration_smoke.py explainability
```

Full validation of available scenarios:

```bash
python3 scripts/run_integration_smoke.py all
```

The following wrappers remain available as aliases:

- [`scripts/integration/run-integration-smoke.sh`](scripts/integration/run-integration-smoke.sh)
- [`scripts/integration/run-integration-smoke-metrics.sh`](scripts/integration/run-integration-smoke-metrics.sh)
- [`scripts/integration/run-integration-smoke-iforest.sh`](scripts/integration/run-integration-smoke-iforest.sh)
- [`scripts/integration/run-integration-smoke-explainability.sh`](scripts/integration/run-integration-smoke-explainability.sh)

## AskMyDasma

Main URL to use:

- user web application: `http://127.0.0.1:8000/askmydasma/`
- administration web application: `http://127.0.0.1:8000/askmydasma/admin/`

Useful URLs:

- web backend health: `http://127.0.0.1:8000/health`
- Grafana: `http://localhost:3000`
- Kafka UI: `http://localhost:8080`
- InfluxDB: `http://localhost:8086`
- Schema Registry: `http://localhost:8081`

In practice:

- to control the application, use AskMyDasma first
- Grafana is used to visualize metrics and data
- Kafka UI is used for Kafka inspection (topics, consumer groups, schemas)
- InfluxDB and Schema Registry are mainly technical components

The interface recognizes the following historical dataset presets when their
CSV files are supplied:

- family `source*.csv`
  same excluded fields as the reference configuration
- `data/component_2022-fixed.csv`
  historical 2022 preset
- `data/131_OPPORTUNITY-1000.csv`
  OPPORTUNITY preset

Other datasets remain selectable, but without specific prefilled values.

Group handling in AskMyDasma:

- the interface no longer exposes manual group selection
- groups are automatically detected by the analysis and explainability services
- the API field `selected_groups` is kept for compatibility, but is not used to drive computations

Excluded field handling:

- AskMyDasma passes `excluded_fields` through to `docker compose` via `DASMA_EXCLUDED_FIELDS`
- `producer` applies this exclusion before Kafka publication, so excluded columns no longer appear in `source-data` or `source-data-viz`
- metadata required by the pipeline is still preserved even if it appears in the exclusion list, especially `timestamp`, `Timestamp`, `time`, `Time`, `Group`, and `group`
- the analysis and explainability services also receive the same exclusion list to avoid rebuilding these columns in their `*-viz` JSON outputs

Campaign reset and relaunch:

- `scripts/web/launch_campaign.sh` performs a `stop` and then a `reset` before a new launch so the system starts from a clean state
- `scripts/web/reset_campaign.sh` recreates Kafka topics, purges the InfluxDB buckets used for observability, and removes the Schema Registry subjects associated with business topics

Grafana variables for InfluxDB dashboards:

- the dashboards `Features - HSTree`, `Features - IForest`, `Features - KitNET`, `Features - DeepAnT`, `Features - mDragStream Point-based`, and `Explainability` now populate their `Feature` variable from fields actually present in InfluxDB points
- they no longer rely on `schema.fieldKeys(...)`, to avoid an old feature still remembered in the bucket reappearing in the interface after it is no longer published

## Unified Group Resolution

Group key resolution is centralized in [`src/dasma_kafka/common/grouping.py`](src/dasma_kafka/common/grouping.py).

Resolution order:

1. `record["Group"]`
2. `record["group"]`
3. fallback `__NO_GROUP__`

This mechanism is used by:

- `hstree-analysis`
- `iforest-analysis`
- `kitnet-analysis`
- `deepant-analysis`
- `mdragstream-point-analysis`
- `explainability`

## Unified Timestamp Resolution

Timestamp marker resolution is centralized in [`src/dasma_kafka/common/timestamps.py`](src/dasma_kafka/common/timestamps.py).

Default resolution order:

1. `timestamp`
2. `Timestamp`
3. `time`
4. `Time`
5. `LabviewTime`
6. `datetime`
7. `Datetime`
8. `date`
9. `Date`

Current usage:

- `explainability` resolves the time value through this function before publishing
- the analyzers' `*-viz` JSON topics populate `timestamp` through this resolution if the `timestamp` key is not already present

## AskMyDasma Backend v1

The AskMyDasma web control backend is implemented under [`src/dasma_kafka/web/`](src/dasma_kafka/web) with supporting scripts in [`scripts/web/`](scripts/web).

The recommended launch is `bash start.sh`, with the container backend by default
(see [Backend Modes](#backend-modes)). For optional host-mode development only:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[web]'
```

Launch the host backend directly:

```bash
bash scripts/web/run_backend.sh
```

The container backend is declared in the main Compose file under the
`askmydasma-container` profile. A bare `docker compose up -d` does not enable that
profile; prefer [`start.sh`](start.sh), which starts the infrastructure and backend
without starting production. [`stop.sh`](stop.sh) stops the backend and stack, then cleans
project volumes. Host mode is an optional alternative, not a prerequisite.

Exposed v1 API:

- `GET /health`
- `GET /api/status`
- `GET /api/services`
- `GET /api/datasets`
- `GET /api/dataset-time-range`
- `GET /api/dataset-preview`
- `GET /api/grafana-url`
- `GET /api/current-campaign`
- `GET /api/campaign-progress`
- `POST /api/campaigns/launch`
- `POST /api/campaigns/start-production`
- `POST /api/campaigns/stop`
- `POST /api/campaigns/reset`
- `POST /api/campaigns/stop-and-reset`
- `POST /api/campaigns/relaunch`

Frontend served by the backend:

- static files: [`frontend/askmydasma/`](frontend/askmydasma)
- administration static files: [`frontend/askmydasma/admin/`](frontend/askmydasma/admin)
- user URL served by the backend: `http://127.0.0.1:8000/askmydasma/`
- administration URL served by the backend: `http://127.0.0.1:8000/askmydasma/admin/`

This interface lets the user:

- choose a dataset
- choose `batch`, `realtime`, `realtime-x5`, `realtime-x20`, or `realtime-x100` (current UI default).
  Realtime modes send a burst of the first 960 messages, then 1, 5, 20 or 100 msg/s respectively.
  This producer burst is distinct from each model's per-group learning warmup.
- select HSTree, IForest, KitNET, DeepAnT, mDragStream Point-based, and optionally Explainability V1/V2
- enable **Repeatability** for at most one selected anomaly model; the interface
  disables all incompatible models and Explainability selections automatically
- configure the HSTree threshold window, the IForest / KitNET / DeepAnT threshold parameters, the DeepAnT CNN/refit parameters (including **Max workers**, the bounded worker-pool size for the parallel-by-group execution, default `2`), and the mDragStream Point-based verdict mode (native internal threshold by default, external P99/Welford optional) plus warmup / cluster / LOF parameters
- choose, per model, the threshold estimator: `p99` (sliding percentile), `welford`
  (cumulative `mean + k_sigma × std`), or `welford-windowed` (sliding-window Welford).
  iForest also offers `native`, which uses the fitted forest's own threshold,
  controlled by `contamination` (default `auto`). Current defaults are `p99`
  for HSTree, `native` for iForest, and `welford-windowed` for KitNET and
  DeepAnT; mDragStream Point defaults to its native verdict. The separately editable Welford window
  defaults to 2048 for HSTree / IForest / DeepAnT, 1000 for KitNET and 900 for mDragStream.
  See [`docs/architecture/askmydasma-campaign-runtime.md`](docs/architecture/askmydasma-campaign-runtime.md#threshold-estimator-modes).
- prepare a campaign, start production, stop, reset, and relaunch a campaign
- inspect the current state
- visualize the list of services
- open Grafana

## Datasets

The repository includes these CSV files under [`data`](data):

- [`eau-1500.csv`](data/eau-1500.csv): a smaller sample for short campaigns.
- [`eau.csv`](data/eau.csv): the full included dataset.
- [`source.csv`](data/source.csv): an identical copy of `eau.csv`, used by the default startup.

`bash start.sh` prepares `data/source.csv` by default. To use the smaller sample,
run `ASKMYDASMA_START_DATASET_PATH=data/eau-1500.csv bash start.sh`.

The dataset actually injected depends on `DATA_SOURCE_FILE` in the `producer` service of the selected Compose configuration.

## Repository State

- The reference Compose file is [`docker-compose.yml`](docker-compose.yml).
- Python packages, unit/contract tests and integration campaigns are established:
  [`pyproject.toml`](pyproject.toml), [`tests/`](tests), [`scripts/run_integration_smoke.py`](scripts/run_integration_smoke.py).
  Test totals and campaign results must be read with their dated revision, not as a permanent repository status.
- A curated documentation path is available in [`docs/public/README.md`](docs/public/README.md).
