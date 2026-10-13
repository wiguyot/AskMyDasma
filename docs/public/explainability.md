# Explainability in AskMyDasma

AskMyDasma offers two separately configurable Explainability services, V1 and
V2. Each can explain a selected detector independently of the other. The
services publish to separate Kafka topics and have separate dashboards.

## Supported combinations

| Model being explained | KernelSHAP | TreeExplainer |
|---|---:|---:|
| HSTree | Yes | No |
| iForest | Yes | Yes, interventional feature perturbation |
| KitNET | Yes; very high compute cost | No |

TreeExplainer for iForest explains mean path length, which is a different scale
from the anomaly score. Its output should not be read as an attribution of the
anomaly-score value itself. TreeExplainer uses the forest state that produced
the event's score, including across refits.

## Operating modes

- **Independent** (`simple` in the runtime contract): the Explainability service
  reads `source-data`, builds its own local model, and explains that model's
  score.
- **Check against detector** (`reference` in the runtime contract): the service
  reads the selected detector's output, verifies that its local score matches
  the detector score, then computes the explanation.

Each enabled service requires its own model, mode, and method selection. V1 and
V2 can use different selections; a displayed V1/V2 difference is not evidence
of parity when their models, methods, or effective states differ.

## How to interpret the results

An explanation describes how the selected method attributes a model output to
input features. It does not establish causality or show that a feature caused
an anomaly. KernelSHAP is sampled and its cost depends on the model, background,
and sample count. The UI warns that KitNET with KernelSHAP has a very high
compute cost. TreeExplainer is available only for iForest in the interventional
configuration described above.

## Evidence and limits

The repository also contains dated research and campaign reports. Each report
applies only to its stated dataset, code version, settings, and test setup. Raw input datasets and evidence exports from historical studies are not
bundled; each report states its reproduction requirements.

For the complete campaign configuration contract, see the [AskMyDasma runtime
contract](../architecture/askmydasma-campaign-runtime.md). For the detailed processing flow, see the [technical overview](../models/explainability/regular/explainability.md).
