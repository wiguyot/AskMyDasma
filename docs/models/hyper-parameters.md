# Model Parameters

This guide lists the defaults for newly prepared AskMyDasma campaigns, defined
in [campaign_config.py](../../scripts/web/campaign_config.py). Saved campaigns
keep their own settings. Direct service launches can have different environment
defaults; use the campaign runtime contract for the supported UI workflow.

## Choosing parameters

Exclude metadata and labels from model features. Keep enough observations in
each group for model warmup and, where needed, threshold and background warmup.
A larger window adapts more slowly; a smaller window can be less stable.
More trees, longer training or a larger SHAP budget can increase compute cost.

Native thresholds belong to their detector. P99 and Welford are external score
estimators. P99 does not guarantee a 1% anomaly rate. Evaluate parameter changes
on appropriate labeled or controlled examples before drawing detection-quality
conclusions.

## HSTree

| Campaign parameter | Default |
|---|---|
| `hstree_estimator` | `p99` |
| `hstree_threshold_window` | `500` |
| `hstree_welford_window` | `2048` |
| `hstree_anomaly_k_sigma` | `3.0` |
| `hstree_anomaly_warmup_window` | `100` |
| `hstree_initial_window_size` | `256` |
| `hstree_window_size` | `256` |
| `hstree_num_trees` | `25` |
| `hstree_variant` | `paper` |
| `hstree_max_depth` | `15` |
| `hstree_repeatability` | `false` |
| `hstree_reproducibility` | `false` |

## Isolation Forest

| Campaign parameter | Default |
|---|---|
| `iforest_estimator` | `native` |
| `iforest_threshold_window` | `500` |
| `iforest_welford_window` | `2048` |
| `iforest_anomaly_k_sigma` | `3.0` |
| `iforest_anomaly_warmup_window` | `100` |
| `iforest_initial_window_size` | `512` |
| `iforest_window_size` | `2048` |
| `iforest_n_estimators` | `100` |
| `iforest_max_samples` | `256` |
| `iforest_contamination` | `auto` |
| `iforest_refit_every` | `128` |
| `iforest_repeatability` | `false` |
| `iforest_reproducibility` | `false` |

## KitNET

| Campaign parameter | Default |
|---|---|
| `kitnet_estimator` | `welford-windowed` |
| `kitnet_threshold_window` | `1000` |
| `kitnet_welford_window` | `1000` |
| `kitnet_threshold_percentile` | `99.0` |
| `kitnet_anomaly_k_sigma` | `3.0` |
| `kitnet_anomaly_warmup_window` | `100` |
| `kitnet_max_size_ae` | `10` |
| `kitnet_grace_feature_mapping` | `1000` |
| `kitnet_grace_anomaly_detector` | `1000` |
| `kitnet_learning_rate` | `0.1` |
| `kitnet_hidden_ratio` | `0.75` |
| `kitnet_clip` | `10.0` |
| `kitnet_repeatability` | `false` |
| `kitnet_reproducibility` | `false` |

## DeepAnT

| Campaign parameter | Default |
|---|---|
| `deepant_estimator` | `welford-windowed` |
| `deepant_threshold_window` | `500` |
| `deepant_welford_window` | `2048` |
| `deepant_threshold_percentile` | `99.0` |
| `deepant_anomaly_k_sigma` | `3.0` |
| `deepant_anomaly_warmup_window` | `100` |
| `deepant_lookback_size` | `100` |
| `deepant_initial_window_size` | `512` |
| `deepant_refit_every` | `128` |
| `deepant_window_size` | `2048` |
| `deepant_filters` | `32` |
| `deepant_kernel_size` | `2` |
| `deepant_dropout` | `0.25` |
| `deepant_lr` | `0.001` |
| `deepant_epochs_initial` | `10` |
| `deepant_epochs_refit` | `5` |
| `deepant_batch_size` | `32` |
| `deepant_max_group_workers` | `2` |
| `deepant_repeatability` | `false` |
| `deepant_reproducibility` | `false` |

## mDragStream Point

| Campaign parameter | Default |
|---|---|
| `mdragpoint_verdict_mode` | `internal` |
| `mdragpoint_estimator` | `p99` |
| `mdragpoint_threshold_window` | `500` |
| `mdragpoint_welford_window` | `900` |
| `mdragpoint_threshold_percentile` | `99.0` |
| `mdragpoint_anomaly_k_sigma` | `3.0` |
| `mdragpoint_anomaly_warmup_window` | `100` |
| `mdragpoint_dynamic_threshold` | `true` |
| `mdragpoint_lof_init` | `true` |
| `mdragpoint_n_init` | `1000` |
| `mdragpoint_max_nb_clusters` | `50` |
| `mdragpoint_max_cluster_size` | `30` |
| `mdragpoint_lof_neighbors` | `20` |
| `mdragpoint_repeatability` | `false` |
| `mdragpoint_reproducibility` | `false` |

## Constraints and interpretation

- HSTree initial and reference windows must be equal. Paper is the default
  variant; PySAD remains selectable.
- Isolation Forest requires `initial_window_size >= max_samples` and
  `window_size >= initial_window_size`. Its native contamination parameter
  changes the threshold, not the continuous anomaly score.
- KitNET must complete feature-mapping and detector-training grace periods.
- DeepAnT requires an initial training window at least as large as its lookback.
- mDragStream initialization, covariance estimation and threshold setup depend
  on the chosen distance and initial observations. Native and external verdicts
  have different semantics.

See the [campaign contract](../architecture/askmydasma-campaign-runtime.md),
[model overview](models-and-explainability.md) and
[Explainability limits](explainability/regular/explainability-limits.md).
