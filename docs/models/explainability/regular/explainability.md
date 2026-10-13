# Explainability Technical Overview

AskMyDasma provides two separately configurable services, `explainability`
(V1) and `explainability-v2` (V2). Each has its own output topics, consumer group
and dashboards. Both support HSTree, iForest and KitNET with KernelSHAP;
iForest also supports interventional TreeExplainer.

See the [user guide](../../../public/explainability.md) for method selection
and interpretation, and the [campaign contract](../../../architecture/askmydasma-campaign-runtime.md)
for configuration.

## Processing

1. Consume source events in `simple` mode or the selected detector output in
   `reference` mode.
2. Resolve the group using `Group`, then `group`, otherwise `__NO_GROUP__`.
3. Process each group's events sequentially and maintain its own model state.
4. Wait for model and explanation-background readiness.
5. In reference mode, verify the local score against the detector score.
6. Explain the appropriate model output and publish Avro and visualization JSON.

Groups can be processed in parallel. The local model is reconstructed because
the detector's in-memory model is not shared between services. Event ordering,
feature ordering and configuration must agree for reference score fidelity.

V1 publishes to `explainability` and `explainability-viz`; V2 publishes to
`explainability-v2` and `explainability-v2-viz`. Status fields distinguish computed
explanations from warmup, mismatches and unavailable results.

## Code

- [V1 entry point](../../../../runtime/explainability/regular/explainability-multi-thread-by-group.py)
- [V1 modules](../../../../src/dasma_kafka/explainability/)
- [V2 modules](../../../../src/dasma_kafka/explainability_v2/)
- [Scientific limits](explainability-limits.md)
- [KitNET scope](../kitnet-explainability-scope.md)
