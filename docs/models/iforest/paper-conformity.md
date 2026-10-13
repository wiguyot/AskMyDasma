# Isolation Forest — Implementation and Scientific Limits

The detector uses scikit-learn's Isolation Forest, based on Liu, F. T., Ting,
K. M. and Zhou, Z.-H., *Isolation Forest*, ICDM 2008, pp. 413–422.
[Original paper](https://cs.nju.edu.cn/zhouzh/zhouzh.files/publication/icdm08b.pdf).

## Streaming adaptation

The paper describes a batch algorithm. AskMyDasma maintains one forest per
group and periodically fits a new forest on a sliding reference window.

| Parameter | Default |
|---|---:|
| Subsample size (`max_samples`, ψ) | 256 |
| Trees | 100 |
| Initial training window | 512 |
| Sliding window | 2,048 |
| Refit interval | 128 |

The initial window must contain at least ψ points; the sliding window must
be at least as large as the initial window. ψ is a fixed integer across
forest generations. The model scores an event with the existing forest
before adding it to the training window or refitting.

The first real score occurs after the initial window. A `0.0` warmup sentinel
is not an anomaly score or a normal verdict. Small groups may never exit
warmup. Producer metadata, including `State` and `BackgroundSubtracted`
and their sanitized names, must be excluded from the model features.

## Scores and thresholds

The integration negates scikit-learn's `score_samples` output, so higher
scores indicate more unusual observations. This preserves the paper's score
polarity without claiming numerical identity with every implementation.

The default `native` estimator uses the scoring forest's threshold.
`IFOREST_CONTAMINATION` accepts `auto` or 1–50% in steps of 1%; it affects this
threshold rather than the continuous anomaly score. P99, Welford and windowed
Welford remain selectable. An existing campaign keeps its selected estimator.

## Differences inherited from scikit-learn

- Inputs are converted to float32 for training and scoring.
- A split routes `x <= split_value` to the left branch.
- Near-constant attributes are skipped using a scale-sensitive tolerance.
- Small-leaf path-length normalization uses exact special cases.

These details can affect tree construction and scores. A dataset-level check
showing no float32 cardinality loss does not rule out effects within nodes.
Periodic refits can change score distributions even with fixed ψ and seeds.

## Explainability

KernelSHAP explains the local anomaly score. Interventional TreeExplainer
explains mean path length, a different output scale. The explanation must use
the forest that produced the event's score, including when a refit follows.

## Evidence and limitations

- [Paper-conformance study and Docker witness](../../../experiments/iforest/iforest-paper-conformance-2026-10-01.md)
- [TreeSHAP campaign evidence](../../../experiments/explainability/phase-c-iforest-treeshap-2026-10-01.md)

The dated study used P99 before the native estimator became the default.
Its counts describe that configuration. Unlabeled anomaly counts do not
measure precision, recall, false positives or detection quality; P99 does not
guarantee a 1% anomaly rate.

See the [runtime contract](../../../runtime/detectors/iforest/iforest_analysis.md)
for output fields and configuration.
