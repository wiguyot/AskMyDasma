# KitNET Explanation Scope

This note describes what KitNET explanations mean in the V1 and V2 runtimes.
The supported explanation method is KernelSHAP.

## Explained score

In `reference` mode, the service maintains a local mirror of the detector's
KitNET model. Matching feature vectors, event order and model settings are
necessary for fidelity. The explained output is `local_score`; after training
grace periods, validation must compare it exactly with the upstream
`kitnet_score` for each `(Group, source_event_id)`.

In `simple` mode, no upstream KitNET score is required. The explanation applies
to the model learned locally from source events. Comparing simple and reference
modes requires the same observations, configuration and effective model state.

SHAP decomposes the local model's score. It does not directly explain
`kitnet_anomaly` or its threshold. Matching local and upstream scores supports
faithful interpretation, but does not establish detection quality or causal
correctness.

## Training grace periods and readiness

KitNET first learns its feature mapping (`grace_feature_mapping`), then trains
its autoencoders (`grace_anomaly_detector`). Until both periods finish, the
detector can publish a numeric score, but Explainability emits `warmup` and
neither checks nor explains that score. Validation should still check event
identity and reference metadata during warmup.

After training, `KitNET.process()` uses the trained model and
`KitNET.execute()` scores without updating learned weights. This allows
KernelSHAP to evaluate masked inputs against a fixed scoring model. The mirror's
`score_batch` can increment PySAD's diagnostic `n_executed` counter; tests verify
that subsequent predictions remain unchanged.

Readiness depends on the number of observations in each group, not elapsed
wall-clock time. Campaign defaults use 1,000 observations for each training
period; direct service defaults and explicitly configured validation profiles
can differ. Use exported statuses to determine readiness.

## Invalid inputs

A feature containing `NaN`, `+inf` or `-inf` is invalid for explanation. The
mirror emits `explanation_status = "failed"` and
`explanation_error_code = "invalid_features"` before SHAP or numerical fidelity
checks. The invalid row does not enter the SHAP background. This applies during
training and afterward, even if the detector published a numeric fallback.

The detector has separate behavior: a `NaN` row produces a `0.0` fallback and
increments its `seen` counter. Tests also characterize PySAD's behavior for
infinities, which can produce warnings or non-finite scores. Report explanation
statuses and identity coverage separately; a detector status alone does not
show that a local explanation was computed.

## Validation scope

Reference-mode validation requires exact identity coverage, expected reference
fields and statuses, matching scores after training, no unexpected
`model_mismatch`, and `computed` explanations once the background is ready.
Simple-mode comparisons must use matching identities and settings.

Unit and static contracts cover these rules. They do not replace a Kafka
campaign, exported-event checks or verification in InfluxDB and Grafana. This
note does not claim that a full KitNET campaign has been validated on the
current revision.
