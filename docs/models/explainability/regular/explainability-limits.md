# Scientific Limits of Explainability

See the [Explainability guide](../../../public/explainability.md) for supported
models, methods and operating modes.

## What is explained

KernelSHAP attributes the selected local model's anomaly score to its input
features. Interventional TreeExplainer for iForest attributes mean path length,
which has a different scale. Feature contributions describe the explained
function; they do not establish physical causality or ground-truth anomalies.

In `simple` mode, the service learns its own model from source events. In
`reference` mode, it checks its local score against the detector output before
computing the explanation. Matching scores on a witness campaign does not
prove state identity for every other dataset or partition layout.

## Sampling and reference data

KernelSHAP's sample budget controls approximation cost and can affect feature
ranks and contribution signs. A budget of 100 is an operational setting, not
proof of scientific stability. Increasing the budget does not by itself
validate the explanation.

The background defines the reference for masked features. A short rolling
background can include transitions or anomalies and may represent some groups
poorly. Feature correlations can make masked combinations unrealistic, so
attributions require domain interpretation.

## Cost and reproducibility

Compute cost depends on sample count, background size, feature count and model
cost. KitNET with KernelSHAP can be especially expensive. Fixed seeds and event
ordering support repeatability, but library versions, model generations and
partition ordering must also be controlled.

V1 and V2 can select different models, methods and modes. Their outputs are
comparable only when these selections and effective states match.

## Validation scope

Structural completeness, additive consistency, score fidelity and repeatability
are distinct checks. Passing them does not establish causal correctness or
business relevance. Dated [validation reports](../../../../experiments/README.md)
apply only to their stated versions and configurations. A full scientific
assessment also requires background sensitivity, sampling stability, domain
review and suitable labeled or controlled examples.

## References

- Lundberg, S. M. and Lee, S.-I. *A Unified Approach to Interpreting Model
  Predictions*, NeurIPS 2017.
  [Original paper](https://papers.neurips.cc/paper/7062-a-unified-approach-to-interpreting-model-predictions).
- [KernelExplainer documentation](https://shap.readthedocs.io/en/stable/generated/shap.KernelExplainer.html).
