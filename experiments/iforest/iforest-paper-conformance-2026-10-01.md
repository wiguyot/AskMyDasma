# iForest paper-conformance study — 2026-10-01

> Historical report. The original input CSV files, raw exports and generated
> evidence named below are not included in this public repository. Paths describe
> the original runs or output destinations, not bundled files. Reproduction
> requires the original data and matching code/dependency versions. Revision and
> artifact hashes identify the recorded run; they do not guarantee availability.

## Scope and method

This is an offline replay only: no Docker service, Kafka topic, or live campaign was started. The script replays each
group in `source_event_id` order with the detector's feature-vector helper. It compares four deliberately separated
configurations:

1. Historical feature filter (81 numeric fields), historical settings, append/refit before score.
2. Current feature filter (79 fields), but otherwise the same historical settings and operation order. The
   difference from (1) isolates the two excluded Avro metadata fields.
3. Current 79-field filter, ψ=256, 100 trees, initial window 512, R=128, and score-before-learning.
4. Same as (3), except initial window 256, to show the cost and effect of choosing 512.

The historical streaming model is frozen in the script; it is not imported from Git history at runtime. All
configurations use the same data ordering, seed 42, and threshold rules: P99 over 500 scores, cumulative Welford,
and windowed Welford over 2,048 scores, each warmed by 100 real scores. The paper's `s > 0.5` and `s ≥ 0.6` marks are
reported as counts only; they are not used as additional verdict estimators.

Command:

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:. OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 \
  .venv/bin/python experiments/iforest/iforest-paper-conformance-study.py \
  --source data/source-5000.csv --source data/source-10000.csv \
  --output data/iforest-evidence/2026-10-01/offline-study.json
```

Environment used: Python 3.12.15, NumPy 2.4.6, scikit-learn 1.8.0. This is the local venv, not a Docker runtime.
The T0 inventory found the two relevant images use scikit-learn 1.8.0 and NumPy 2.4.4. T8's rebuilt image witness
used those versions. Dataset and JSON SHA-256 values, ordered feature lists, per-group/per-generation scores,
verdict counts, and warning counts are in the non-versioned JSON evidence:

- `source-5000.csv`: `8b0b0720389f8ac16f81e54f25de8abcb079bc92170b95b38640ddc7843fcf9f`
- `source-10000.csv`: `c44d5696283a530f6b10d2a32f35dd27bd4e04ef8ba6b4ff3863d1d25639644c`
- `offline-study.json`: `ea86a14edd2a1e7db829dc6e8207b61da489c39b8b0ad391d0700bee59673cf2`

## Feature-selection and float32 findings

| Dataset | Rows | Historical fields | Current fields | Float32 cardinality losses | Current fields with range ≤ 1e-7 |
|---|---:|---:|---:|---:|---:|
| `source-5000.csv` | 4,999 | 81 | 79 | 0 | 0 |
| `source-10000.csv` | 10,000 | 81 | 79 | 0 | 0 |

The old filter retained the two producer-sanitized metadata fields; the mandatory current filter removes them. With the
historical model settings held fixed, this changed the P99 anomaly count from 310 to 299 on `source-5000`, and from
1,266 to 1,151 on `source-10000`. Welford counts changed from 211 to 201 and from 435 to 387 respectively. These
are configuration effects, not evidence that one feature set is more scientifically correct; no labels are present.

No numeric feature in either dataset lost distinct values during float32 conversion, and none of the 79 current
features had a total range at or below `1e-7` on these files. This is only a dataset-level cardinality/range check,
not proof that float32 or scikit-learn's split tolerance never changes a tree.

The separate K3 probe confirms the scale-sensitive guard: the values `0, 1e-9, …, 7e-9` remain eight distinct
float32 values, but scikit-learn fits a one-node tree because the range is within its absolute near-constant
tolerance. Multiplying the same values by `1e9` yields a 13-node tree. Thus distinct-value counts alone do not
detect this K3 behavior; scale matters.

## Scores and verdict counts

The table reports `anomalies / classified` for each threshold method. Scores that are in model warmup or threshold
warmup are excluded from the denominator. Per-group event and generation counts are in the JSON artifact.

| Dataset | Configuration | Model scores | P99 | Welford | Windowed Welford |
|---|---|---:|---:|---:|---:|
| source-5000 | Historical filter + settings | 4,059 | 310 / 3,471 (8.9%) | 211 / 3,471 (6.1%) | 211 / 3,471 (6.1%) |
| source-5000 | Feature filter only | 4,059 | 299 / 3,471 (8.6%) | 201 / 3,471 (5.8%) | 201 / 3,471 (5.8%) |
| source-5000 | Paper defaults, initial 512 | 2,046 | 302 / 1,546 (19.5%) | 290 / 1,546 (18.8%) | 290 / 1,546 (18.8%) |
| source-5000 | Paper defaults, initial 256 | 3,326 | 336 / 2,826 (11.9%) | 246 / 2,826 (8.7%) | 246 / 2,826 (8.7%) |
| source-10000 | Historical filter + settings | 9,060 | 1,266 / 8,472 (14.9%) | 435 / 8,472 (5.1%) | 435 / 8,472 (5.1%) |
| source-10000 | Feature filter only | 9,060 | 1,151 / 8,472 (13.6%) | 387 / 8,472 (4.6%) | 387 / 8,472 (4.6%) |
| source-10000 | Paper defaults, initial 512 | 7,047 | 1,782 / 6,547 (27.2%) | 909 / 6,547 (13.9%) | 909 / 6,547 (13.9%) |
| source-10000 | Paper defaults, initial 256 | 8,327 | 1,628 / 7,827 (20.8%) | 643 / 7,827 (8.2%) | 643 / 7,827 (8.2%) |

Welford and windowed Welford happen to produce identical counts here: no group has enough post-threshold-warmup
scores to evict values from the 2,048-score Welford window. P99 anomaly rates are not guaranteed to be 1%: the
threshold updates only from scores classified normal, and these datasets have no labels or stationarity guarantee.
The larger rates under the corrected settings need interpretation in T8 with the real campaign and dashboards; they
do not by themselves establish increased false positives or better detection.

## ψ, generations, and sliding-window check

The historical configuration starts with effective ψ values 128, 256, 384, then 512 as the available training
window grows. The corrected configuration uses ψ=256 in every fitted generation, with maximum tree depth 8. This
removes the changing ψ/normalization/height mechanism. It does **not** make observed score means constant: changes
in group data and training-window composition remain visible between generations.

For `Sortie CTA`, with current features:

| Dataset/configuration | First generation mean s | Last generation mean s | Effective ψ across generations |
|---|---:|---:|---|
| source-5000, historical settings | 0.4759 | 0.4727 | 128 → 256 → 384 → 512 |
| source-5000, paper defaults / initial 512 | 0.4268 | 0.4819 | 256 throughout |
| source-5000, paper defaults / initial 256 | 0.4474 | 0.4819 | 256 throughout |
| source-10000, historical settings | 0.4759 | 0.4186 | 128 → 256 → 384 → 512 |
| source-10000, paper defaults / initial 512 | 0.4268 | 0.4232 | 256 throughout |
| source-10000, paper defaults / initial 256 | 0.4474 | 0.4232 | 256 throughout |

The corrected scores still vary substantially by generation for some groups (for example the source-5000 Selek
last-generation mean is about 0.599). The experiment therefore supports the narrower conclusion that ψ and tree
height no longer vary; it does not prove that score distributions are stationary or that score scale is stable under
real drift.

`source-10000.csv` has 2,293 `Sortie CTA` events, exceeding W=2,048. Under the new configuration its maximum
observed model buffer is exactly 2,048, confirming the sliding window is actually exercised. `source-5000.csv`
never exceeds 1,102 events in a group, so its window does not roll over.

With initial window 512, groups 1A (75), 2B (215), 3A (50), and 4B (53) never receive a real model score in either
file. With initial window 256, 2B still does not reach a real score (215 events). This is expected and must not be
read as “normal”; the published numeric sentinel is not a detector verdict.

## Conclusion and limits

- The 81→79 metadata exclusion has a measurable, modest effect on score/verdict counts; it is now isolated from
  changes to model parameters and operation order.
- Fixed ψ removes the structural per-generation subsample-size and tree-height change. It does not remove
  generation-to-generation score variation caused by the data/window, and the 512 warmup makes 2B unscored.
- Initial 256 yields more scored events, but allows the first forest to train on exactly ψ points; it does not
  subsample that first training window. Initial 512 creates the first forest from twice ψ and already subsamples.
- The anomaly counts are unlabeled observations. They cannot validate scientific detection quality. T8 confirmed
  the P99 mechanics and Docker versions, but no ground-truth labels are available to assess detection quality.

## T8 — Docker witness (2026-10-01)

### Runtime and configuration

The three images were rebuilt from the recorded source revision and exercised against a fresh, single-partition Kafka
campaign on `data/source-5000.csv` (4,999 events, nine groups). Runtime versions reported by the images:
Python 3.12.13, NumPy 2.4.4, scikit-learn 1.8.0, pandas 3.0.2, PySAD 0.3.4 and SHAP 0.51.0. The detector and
both explainability mirrors used seed 42, initial window 512, rolling window 2,048, ψ=256, 100 estimators,
refit every 128 events, P99 over the last 500 normal scores, and 100-score threshold warmup. Explainability used
the `reference` iForest model and KernelSHAP with 100 samples/background.

The campaign source was branch commit `90173d8` plus the T8 working-tree addition of an INFO log for the
Explainability iForest mirror parameters (diagnostic only; no scoring behavior change). Rebuilt image IDs:
`dasma-kafka-anomaly-sklearn:0.1.0` — `sha256:4cbc2bc0abf55ab321f48cbac2571df9ce2bc830f1d83a1c89b7e025a6b0c99a`;
`dasma-kafka-explainability:0.1.0` (used by V1 and V2) —
`sha256:6b9d7328bf4bd38f333c517fac522ad06832fb210fa33688c6fad122588e8fb0`.

The three consumer groups reached offset 4,999 with zero lag. There were 4,999 unique `(Group, source_event_id)`
identities in each output. The detector produced 2,046 real model scores and 2,953 model-warmup records. In each
of the five groups large enough to score, the first real score is event 513. Its detector verdict still reports
`warming_up` until event 613 because the P99 estimator needs 100 real scores of its own. Explainability V1 and V2
each emitted 2,953 `warmup` and 2,046 `computed`, with no `model_mismatch` or `failed`; all computed records use
`model_version=local-iforest-v2`.

The detector and both mirrors used the same 79 ordered features (fingerprint
`9b32ba35841f9b71a8e9d87bb934918aea8f6cbf644ec9986f53af8a5866117e`). Across the 4,092 computed explanations
(2,046 per version), the local explained score exactly matched the detector score; SHAP additivity over the 79
numeric feature contributions had maximum absolute error 0. V1 and V2 also matched each other exactly. The
scikit-learn `max_samples` warning was absent.

### Threshold and repeatability results

Among 1,546 classified detector outputs, 302 were anomalies (19.534%). Every verdict matched the strict rule
`score > threshold`, with all observed thresholds in `[0.45491577319469284, 0.5185633954937486]`, hence inside
]0, 1]. This rate is an unlabeled observation, not a false-positive rate or a measure of scientific quality.

The separate repeatability campaign emitted 4,999 complete pairs: zero divergences and maximum score difference
0. Every pair had the same verdict and status (`warmup` 3,453, `normal` 1,244, `anomaly` 302).

The HSTree-only non-regression check started `hstree-analysis` without an iForest service or iForest output field;
it processed through offset 3,642 before the exact container was stopped. This was a startup/isolation check, not
a full HSTree campaign. The invalid initial-window startup check emitted the R3 message and exited 1.

### Reproducibility artifacts

The raw topic exports are intentionally local and ignored by Git, under
`data/iforest-evidence/2026-10-01/docker-campaign/`:

| Export | SHA-256 |
|---|---|
| `iforest-analysis-viz.jsonl` | `846889fdd9231757102fe75d920459ed3dae6dbd8917748190bc82af1d887fb8` |
| `explainability-v1-viz.jsonl` | `2be3f4806e6c02f5c172c9635aedc0959a2df211d6d5787e1c3bd3a09376b2fd` |
| `explainability-v2-viz.jsonl` | `9aa5129055ec969bd35b36585621894c0523eb047bbbbacf9fbc74d4746c1a80` |
| `repeatability-diff.jsonl` | `a7c6bbf253c957aefbc127e98aa13900aa7e48c307275e8a39aaff428a9cd894` |

The report and code are versioned; the high-volume raw exports remain local. No detection-quality conclusion is
made without labels. Visual dashboard review was not a T8 acceptance criterion and was not performed. Final
verification: `1814 passed, 11 warnings, 129 subtests`; `docker compose config -q` and `git diff --check` passed.
