# AskMyDasma Runtime Validation Workflow

This workflow validates the local AskMyDasma runtime after changes to campaign
configuration, Docker Compose orchestration, or model selection.

## Baseline Startup

Start infrastructure and the default container backend. Host `python3` is
needed for launch helpers; a host `.venv` is needed only for local development,
tests or the optional host backend. `ASKMYDASMA_PYTHON` selects that host Python
when host mode is used:

```bash
ASKMYDASMA_PYTHON=.venv/bin/python bash start.sh
```

Useful checks:

```bash
curl -fsS http://127.0.0.1:8000/health
curl -fsS http://127.0.0.1:8000/api/status
curl -fsS http://127.0.0.1:8000/api/current-campaign
curl -fsS http://127.0.0.1:8000/api/campaign-progress
docker compose ps --all
```

Expected default model selection:

```text
hstree,iforest,kitnet
```

Expected default runtime services:

```text
hstree-analysis
iforest-analysis
kitnet-analysis
consensus-analysis
build-metrics
telegraf
producer
```

Optional services such as `deepant-analysis`, `mdragstream-point-analysis`,
`explainability`, and `explainability-v2` must not be recreated for the default
campaign.

## Prebuilt Image Validation

After changes to Dockerfiles, service requirement files, or Compose image
mapping, rebuild the runtime images before starting the stack:

```bash
docker compose build
docker compose config --images
```

Then run startup or a short smoke scenario. The integration runner expects the
runtime images to be locally available and starts Compose with `--pull never`,
so a successful smoke test confirms that runtime startup no longer depends on
package installation inside running containers.

## JSON Contract Checks

Inspect the current campaign JSON:

```bash
python3 scripts/web/campaign_config.py export-env .tmp/current-campaign.json
python3 scripts/web/campaign_config.py payload .tmp/current-campaign.json --action validate
python3 scripts/web/campaign_config.py all-runtime-services
python3 scripts/web/campaign_config.py reset-topics
python3 scripts/web/campaign_config.py reset-schema-subjects
```

For the default campaign, `DASMA_COMPOSE_SERVICES` should contain only the three
default analysis services plus `consensus-analysis`, `build-metrics`, and
`telegraf`.

## Production Restart

Stop current campaign containers:

```bash
bash scripts/web/stop_campaign.sh
```

Restart production from the current JSON:

```bash
bash scripts/web/start_production.sh .tmp/current-campaign.json
```

Then check:

```bash
docker compose ps --all --format '{{.Name}} {{.State}}'
curl -fsS http://127.0.0.1:8000/api/campaign-progress
```

The progress response should show only active model topics plus `source-data`.

## Optional Model Scenarios

Create temporary JSON files under `.tmp/` and start production with each file.
Run `bash scripts/web/stop_campaign.sh` between scenarios.

### DeepAnT

`selected_models`:

```json
["hstree", "deepant"]
```

Expected runtime services include:

```text
hstree-analysis
deepant-analysis
consensus-analysis
build-metrics
telegraf
producer
```

Expected checks:

```bash
python3 scripts/web/campaign_config.py export-env .tmp/deepant-campaign.json | rg 'DASMA_COMPOSE_SERVICES|DASMA_DEEPANT_ENABLED'
docker compose ps --all --format '{{.Name}} {{.State}}'
```

### mDragStream Point

`selected_models`:

```json
["mdragpoint"]
```

Expected runtime services include:

```text
mdragstream-point-analysis
consensus-analysis
build-metrics
telegraf
producer
```

Expected checks:

```bash
python3 scripts/web/campaign_config.py export-env .tmp/mdragpoint-campaign.json | rg 'DASMA_COMPOSE_SERVICES|DASMA_MDRAGPOINT_ENABLED'
docker compose ps --all --format '{{.Name}} {{.State}}'
```

### Explainability v2

Explainability V2 requires explicit model, mode and method selections. In
`reference` mode, the selected detector must also be enabled; `simple` mode
can run independently from the detector.

Valid `selected_models`:

```json
["hstree", "explainability-v2"]
```

Include these settings in the complete campaign JSON:

```json
{
  "explainability": {
    "v2_model": "hstree",
    "v2_mode": "reference",
    "v2_method": "kernel",
    "v2_nsamples": 100
  }
}
```

Expected runtime services include:

```text
hstree-analysis
explainability-v2
consensus-analysis
build-metrics
telegraf
producer
```

Expected checks:

```bash
python3 scripts/web/campaign_config.py export-env .tmp/explainability-v2-campaign.json | rg 'DASMA_COMPOSE_SERVICES|DASMA_EXPLAINABILITY_V2_ENABLED'
docker compose ps --all --format '{{.Name}} {{.State}}'
```

## Reset

Use reset when Kafka topics, Schema Registry subjects, or InfluxDB buckets must
be recreated:

```bash
bash scripts/web/reset_campaign.sh
```

`reset_campaign.sh` reads its topic and Schema Registry subject inventories from
`scripts/web/campaign_config.py`, so model-topic inventory changes are kept in one
place.
