# Scripts

This directory contains repository utility scripts.

Organization:

- main integration runner: [`run_integration_smoke.py`](../scripts/run_integration_smoke.py)
- wrappers shell: [`integration/`](../scripts/integration)
- unit tests: [`tests/`](../scripts/tests)
- AskMyDasma web control scripts: [`web/`](../scripts/web)
- recommended root launchers: [`../start.sh`](../start.sh), [`../stop.sh`](../stop.sh)

Placement rule:

- one-off launch scripts and shell wrappers go under `scripts/`
- historical scripts must not be moved back to the repository root

Scripts web v1:

- [`web/list_datasets.sh`](../scripts/web/list_datasets.sh)
- [`web/get_services_status.sh`](../scripts/web/get_services_status.sh)
- [`web/reset_campaign.sh`](../scripts/web/reset_campaign.sh)
- [`web/launch_campaign.sh`](../scripts/web/launch_campaign.sh)
- [`web/start_production.sh`](../scripts/web/start_production.sh)
- [`web/stop_campaign.sh`](../scripts/web/stop_campaign.sh)
- [`web/get_topic_offsets.sh`](../scripts/web/get_topic_offsets.sh)
- [`web/relaunch_campaign.sh`](../scripts/web/relaunch_campaign.sh)
- [`web/run_backend.sh`](../scripts/web/run_backend.sh)

Recommended usage:

The default startup uses `data/source.csv`, an identical copy of `data/eau.csv`.
Set `ASKMYDASMA_START_DATASET_PATH=data/eau-1500.csv` for a shorter sample.

- `bash start.sh` starts the infrastructure, the web application, and prepares the initial campaign in a ready state
- the web application at `http://127.0.0.1:8000/askmydasma/` can then select the dataset, choose models, prepare a campaign, and start production
- the application provides presets for the `source*.csv` family
- the application also provides two historical presets:
  - `data/component_2022-fixed.csv`
  - `data/131_OPPORTUNITY-1000.csv`
- `bash stop.sh` stops the web backend, stops the Docker stack, and cleans project volumes; the historical `pip-cache` volume may remain for compatibility, but Compose-managed Python services no longer install dependencies at runtime

`start.sh` can be customized with:

- `ASKMYDASMA_AUTO_PREPARE_ON_START=0` to disable the automatic campaign preparation
- `ASKMYDASMA_START_DATASET_PATH` to override the dataset used for the initial prepared campaign
- `ASKMYDASMA_START_MODE` to override the initial execution mode
