# Understanding Explainability

An anomaly detector assigns a score to each observation. Explainability shows
how input features contribute to the model output selected for explanation.
It helps inspect an unusual point and form hypotheses for further investigation.

## Reading contributions

For an additive explanation:

```text
explained output ≈ reference value + sum of feature contributions
```

A positive contribution raises the explained output; a negative contribution
lowers it. A large absolute contribution means a strong influence on that
output. Its meaning depends on the model and method: iForest TreeExplainer
explains mean path length, while KernelSHAP explains the anomaly score.

A contribution is not proof that a feature caused an anomaly. The anomaly
verdict also depends on the detector's threshold and status.

## Groups and readiness

Each group has its own local model and background observations. Explanations
become available only after the model and background are ready. Missing
explanations during warmup should not be interpreted as zero influence.

## Choosing a configuration

V1 and V2 each have a model, method and mode selection. Independent (`simple`)
mode explains a model learned from source events. Check-against-detector
(`reference`) mode verifies a local score against the detector before explaining.

See the [Explainability guide](../../../public/explainability.md) for supported
combinations, and the [scientific limits](explainability-limits.md) for interpreting
sampling, reference backgrounds and reproducibility.
