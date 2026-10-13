# AskMyDasma Campaign Runtime Contract

AskMyDasma uses one persisted campaign JSON as the runtime source of truth:

```text
.tmp/current-campaign.json
```

The FastAPI backend writes this file when a campaign is prepared or production is
started. The web shell scripts no longer receive a long positional argument
list; they receive only the path to this JSON file.

## Script Contract

The runtime scripts accept one optional argument: the campaign JSON path.

```bash
bash scripts/web/launch_campaign.sh .tmp/current-campaign.json
bash scripts/web/start_production.sh .tmp/current-campaign.json
bash scripts/web/relaunch_campaign.sh .tmp/current-campaign.json
```

If the argument is omitted, the scripts default to `.tmp/current-campaign.json`.

`scripts/web/campaign_config.py` is the single translator between the JSON
contract and Docker Compose environment variables:

```bash
python3 scripts/web/campaign_config.py export-env .tmp/current-campaign.json
python3 scripts/web/campaign_config.py payload .tmp/current-campaign.json
```

`export-env` emits shell `export` statements used by the shell scripts before
calling Docker Compose. `payload` emits the normalized campaign payload returned
by the scripts to the FastAPI backend.

## JSON Shape

The JSON is structured by campaign metadata, model parameters, and explainability
parameters:

```json
{
  "dataset_path": "data/eau-1500.csv",
  "mode": "realtime-x100",
  "selected_groups": [],
  "excluded_fields": [],
  "selected_models": [
    "hstree",
    "iforest",
    "kitnet"
  ],
  "models": {
    "hstree": {
      "estimator": "p99",
      "threshold_window": 500,
      "welford_window": 2048,
      "anomaly_k_sigma": 3.0,
      "anomaly_warmup_window": 100,
      "initial_window_size": 256,
      "window_size": 256,
      "num_trees": 25,
      "variant": "paper",
      "max_depth": 15,
      "repeatability": false,
      "reproducibility": false
    },
    "iforest": {
      "estimator": "native",
      "threshold_window": 500,
      "welford_window": 2048,
      "anomaly_k_sigma": 3.0,
      "anomaly_warmup_window": 100,
      "initial_window_size": 512,
      "window_size": 2048,
      "n_estimators": 100,
      "max_samples": "256",
      "contamination": "auto",
      "refit_every": 128,
      "repeatability": false,
      "reproducibility": false
    },
    "kitnet": {
      "estimator": "welford-windowed",
      "threshold_window": 1000,
      "welford_window": 1000,
      "threshold_percentile": 99.0,
      "anomaly_k_sigma": 3.0,
      "anomaly_warmup_window": 100,
      "max_size_ae": 10,
      "grace_feature_mapping": 1000,
      "grace_anomaly_detector": 1000,
      "learning_rate": 0.1,
      "hidden_ratio": 0.75,
      "clip": 10.0,
      "repeatability": false,
      "reproducibility": false
    },
    "deepant": {
      "estimator": "welford-windowed",
      "threshold_window": 500,
      "welford_window": 2048,
      "threshold_percentile": 99.0,
      "anomaly_k_sigma": 3.0,
      "anomaly_warmup_window": 100,
      "lookback_size": 100,
      "initial_window_size": 512,
      "refit_every": 128,
      "window_size": 2048,
      "filters": 32,
      "kernel_size": 2,
      "dropout": 0.25,
      "lr": 0.001,
      "epochs_initial": 10,
      "epochs_refit": 5,
      "batch_size": 32,
      "max_group_workers": 2,
      "repeatability": false,
      "reproducibility": false
    },
    "mdragpoint": {
      "verdict_mode": "internal",
      "estimator": "p99",
      "threshold_window": 500,
      "welford_window": 900,
      "threshold_percentile": 99.0,
      "anomaly_k_sigma": 3.0,
      "anomaly_warmup_window": 100,
      "dynamic_threshold": true,
      "lof_init": true,
      "n_init": 1000,
      "max_nb_clusters": 50,
      "max_cluster_size": 30,
      "lof_neighbors": 20,
      "repeatability": false,
      "reproducibility": false
    }
  },
  "explainability": {
    "nsamples": 100,
    "v2_nsamples": 100
  }
}
```

Known top-level fields and nested model fields are validated. Unknown fields
fail early in `campaign_config.py` so configuration drift is visible.

### Explainability model modes

The structured `explainability` object also accepts `model`, `mode`, `method`
and their `v2_*` counterparts. Each selected service must provide all three;
there is deliberately no default mode. HSTree, IForest, and KitNET support
KernelSHAP. IForest also supports TreeExplainer with interventional feature
perturbation; HSTree and KitNET do not. The UI warns that KitNET with KernelSHAP
has a very high compute cost. The UI labels `simple` as `Independent` and
`reference` as `Check against detector`: the former reads `source-data`, while
the latter reads the selected detector's topic and checks the local score
against the detector output before computing SHAP. Starting an enabled service
directly without its required variables fails before Kafka subscription; when
AskMyDasma disables the service, those variables may remain empty. V1/V2
comparison should not be interpreted as a parity proof if the two selections
differ.

Each anomaly-model object (`hstree`, `iforest`, `kitnet`, `deepant`, and
`mdragpoint`) accepts the optional boolean `repeatability`, defaulting to
`false`. The API also exposes this value as the corresponding flat field (for
example `hstree_repeatability`) so it survives the UI → API → persisted JSON →
runtime cycle.

## Threshold Estimator Modes

HSTree, iForest, KitNET and DeepAnT derive their verdicts by thresholding the
continuous score. mDragStream Point normally uses its native candidate/cluster
logic; external thresholding is optional. The `estimator` field selects how that threshold is computed. All models
offer the three external modes below; iForest also offers `native`:

| `estimator` | Threshold rule | Baseline horizon |
|-------------|----------------|------------------|
| `p99` | sliding-window Pth percentile of recent scores | last `threshold_window` scores |
| `welford` | `mean + k_sigma × std`, cumulative (never forgets) | all scores since campaign start |
| `welford-windowed` | `mean + k_sigma × std`, computed over a sliding window | last `welford_window` scores |
| `native` (iForest only) | the fitted forest's own threshold, controlled by `contamination` | the training window of the forest that scored the event |

The default campaign estimators are `p99` for HSTree, `native` for iForest,
and `welford-windowed` for KitNET and DeepAnT. mDragStream Point uses its
internal verdict by default; its external estimators apply only when
`verdict_mode` is `external`.

For iForest, `contamination` defaults to `auto`, which uses the standard
Isolation Forest decision rule. A percentage from 1% to 50%, in steps of 1%,
sets the proportion used to calibrate the native threshold on each training
window. It does not guarantee that proportion of anomalies in future data.
Native verdicts are available after model warmup, without a separate estimator
calibration period. This setting has no effect on the external estimators.

`welford` and `welford-windowed` share the same `anomaly_k_sigma` and
`anomaly_warmup_window` parameters; the windowed variant adds only
`welford_window`, the size of its sliding baseline. Unlike plain `welford`, the
windowed variant forgets old scores, so the threshold adapts to concept drift on
long campaigns.

`campaign_config.py` maps these fields to per-model environment variables, for
example `IFOREST_THRESHOLD_MODE`, `IFOREST_ANOMALY_K_SIGMA`, and
`IFOREST_WELFORD_WINDOW`.

> **Two-sided env var contract.** Exporting a variable from `campaign_config.py`
> is only half the wiring: `docker-compose.yml` must also *reference* each
> `${VAR}` in the `environment:` block of the service that consumes it. An export
> without a matching compose reference is silently ignored — the container falls
> back to its in-code default and the UI choice has no effect. Any new campaign
> parameter must therefore be added on **both** sides. Contract tests check the export and Compose wiring; an end-to-end campaign
> additionally verifies the effective container behavior. See the [runtime validation workflow](askmydasma-validation-workflow.md).

### `welford_window` Default per Model

The campaign defaults below define the window of recent scores used by
`welford-windowed`. This window can be edited independently of the model's
own window and does not always have the same size:

| Model | Default `welford_window` | Origin |
|-------|--------------------------|--------|
| `hstree` | `2048` | Separate threshold window; the HSTree reference window defaults to `256` |
| `iforest` | `2048` | IForest sliding reference window (`window_size`) |
| `deepant` | `2048` | DeepAnT rolling window (`window_size`) |
| `kitnet` | `1000` | Separate threshold window; KitNET has no sliding data window |
| `mdragpoint` | `900` | cluster memory: `max_nb_clusters × max_cluster_size` |

AskMyDasma exports these campaign values explicitly. A manual Compose launch
without a `KITNET_WELFORD_WINDOW` override uses `500` for KitNET; the other
Compose defaults match the table. When a detector runs directly without that
environment variable, HSTree uses `2048`, iForest and DeepAnT use their model
window, KitNET uses its `threshold_window`, and mDragStream uses its cluster
memory size. These fallbacks can differ from the campaign values.

For `mdragpoint`, the external estimator (including `welford-windowed`) applies
only in `external` verdict mode; in the default `internal` verdict mode the
native mDragStream threshold prevails.

## Model Selection

`selected_models` controls which services process records. The current known
model keys are:

```text
hstree
iforest
kitnet
deepant
mdragpoint
explainability
explainability-v2
```

`campaign_config.py` converts this list into Docker kill-switch variables such as
`DASMA_HSTREE_ENABLED`, `DASMA_DEEPANT_ENABLED`, and
`DASMA_EXPLAINABILITY_V2_ENABLED`.

`campaign_config.py` also derives the Docker Compose service list. It exports
that list as `DASMA_COMPOSE_SERVICES`. Production starts only the selected model
services plus the always-on runtime services:
`consensus-analysis`, `build-metrics`, and `telegraf`. The `producer` service is
created separately so the backend can wait for readiness before sending the start
signal.

The kill-switch variables remain exported as a safety guard. If a non-selected
service is started manually, it receives `DASMA_ENABLED=0` and idles without
consuming Kafka. The DeepAnT entry point checks this flag before heavy imports, so a disabled
DeepAnT service does not pay the PyTorch import cost. PyTorch is installed when
the image is built.

`GET /api/campaign-progress` filters progress rows from the same
`selected_models` list. It always includes `source-data`, and it only includes
optional model topics when those models are active.

## Repeatability Campaigns

A repeatability campaign is an exclusive local test for one selected anomaly
model. Set `repeatability: true` in exactly one of `hstree`, `iforest`, `kitnet`,
`deepant`, or `mdragpoint`; AskMyDasma exposes the same choice through one
**Repeatability** checkbox per model.

`campaign_config.py` rejects a configuration when more than one flag is true,
when the flagged model is not selected, or when any other model — including
`explainability` or `explainability-v2` — is still selected: a repeatability
campaign runs exactly the flagged model and nothing else. The same validation
runs in the AskMyDasma service before the campaign JSON is persisted, so an API
request or a stale `current-campaign.json` that bypasses the interface is
rejected before launch instead of silently launching unselected services.

For the selected model M, `DASMA_COMPOSE_SERVICES` replaces M's normal Compose
service with this ordered trio:

```text
repeatability-v1
repeatability-v2
repeatability-diff
```

The two instances receive the same image, script, model parameters, deterministic
seed, and native-thread pinning. They publish Avro results to
`repeatability-v1` / `repeatability-v2` and JSON visualizations to their
respective `-viz` topics. `repeatability-diff` joins those JSON records strictly
on `(Group, source_event_id)`, compares all configured result fields, and emits
the event-level verdict to `repeatability-diff`.

All standard parameters exposed by AskMyDasma for the tested model are included
in that shared environment, not only the repeatability-specific variables. This
includes estimator, threshold, warmup and window values as well as model-specific
controls such as HSTree tree settings, IForest refit settings, KitNET learning
settings, DeepAnT architecture/training settings, and mDragStream cluster/LOF
settings. `build_env()` exports each selected campaign value before Compose
creates either instance, so the parameter set is immutable for that run. Editing
the form after production starts has no effect on running containers; prepare or
relaunch the campaign to test a new parameter set.

The runtime forces both `DASMA_<M>_ENABLED=0` and
`DASMA_CONSENSUS_ENABLED=0`: the normal model and consensus must not consume or
wait for a topic deliberately replaced by the pair. No normal analysis service
runs during the test — the flagged model is the only selected one, so no other
`*-analysis` service, Explainability, or Explainability v2 is launched.
Unchecking the **Repeatability** checkbox in the interface restores ordinary
multi-model selection (it does not re-check previously deselected models).
Reset removes the five repeatability
topics and the two Avro schema subjects; Telegraf writes the three JSON flows to
separate `repeatability-v1`, `repeatability-v2`, and `repeatability-diff`
InfluxDB buckets.

The `Repeatability` Grafana dashboard displays raw source components, the two
event-aligned global scores, score differences, warmup markers, verdicts, and
summary statistics. A complete event with `identical = 1` and `abs_diff = 0`
confirms local repeatability. It does not demonstrate reproducibility between
different machines. Cross-machine reproducibility uses a separate CSV export
and comparison workflow, described below.

## Backend Restore

On FastAPI startup, the backend reads `.tmp/current-campaign.json` and restores
the in-memory `CampaignView` if no campaign is already present.

Restore behavior:

- missing JSON: no campaign is restored
- invalid JSON: startup continues and no campaign is restored
- valid JSON without a running producer: campaign status becomes `configuring`,
  platform status becomes `ready_for_production`
- valid JSON with a running `producer` service: campaign status becomes
  `running`, platform status becomes `processing_data`

This keeps `/api/current-campaign`, `/api/status`, `/api/campaign-progress`,
debug views, relaunch, and logs aligned on the same JSON source after a backend
restart.

## Cross-machine reproducibility

Set `reproducibility: true` on exactly one selected anomaly model, with no other
models selected, and keep its `repeatability` flag false. Provide the portable
run identity and role in the top-level object:

```json
{
  "reproducibility": {
    "source_role": "source-1",
    "campaign_id": "example-water-run"
  }
}
```

This fragment supplements a complete campaign JSON. The second machine uses
`source-2` with the same campaign ID and configuration. The normal detector,
consensus and telemetry services run alongside `reproducibility-export`. The
exporter writes CSV and metadata under `data/reproducibility/<campaign-id>/`.
Compare the two exports offline and optionally publish the resulting comparison
through `reproducibility-publish`. See the [project guide](../../README.md#reproducibility)
for commands. Matching results support that particular machine/configuration
comparison, rather than a universal cross-machine guarantee.
