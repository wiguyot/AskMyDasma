# iForest TreeSHAP Qualification — 2026-10-01

> Historical report. The original input CSV files, raw exports and generated
> evidence named below are not included in this public repository. Paths describe
> the original runs or output destinations, not bundled files. Reproduction
> requires the original data and matching code/dependency versions. Revision and
> artifact hashes identify the recorded run; they do not guarantee availability.

## Scope and prerequisites

Qualification targeted interventional TreeSHAP with the same background as the
comparison method, explaining mean path length rather than the exponential
anomaly score. The model manager retained `last_scoring_model` and
`last_scoring_generation`: if forest A scored an event before a refit installed
B, SHAP still used A.

Docker Engine 29.8.1 was available; no containers were active at the start.
The historical stability runner still used the old
`compute_shap_values(model=...)` signature. The qualification benchmark was
independent of it; general repair of that runner was outside the study.
The `stability.py` metrics module could be used independently.

| Environment | Python | SHAP | sklearn | NumPy |
|---|---|---|---|---|
| venv | 3.12.15 | 0.51.0 | 1.8.0 | 2.4.6 |
| Explainability image | 3.12.13 | 0.51.0 | 1.8.0 | 2.4.4 |

Initial reference image: `dasma-kafka-explainability:0.1.0`, ID
`sha256:6b9d7328bf4bd38f333c517fac522ad06832fb210fa33688c6fad122588e8fb0`.
Base revision: `7c21f9a`. Computation-library versions were held fixed.

## Conversion and fidelity

Shared modules implemented configurable float64/float32 ordinal encoding,
corrected leaf conversion, converted predictions and an independent
`tree.apply` calculation. `Independent(max_samples=len(background))` preserved
the full background. An initial smoke test used three trees, three variables,
32 rows and seed 42; predictions matched at rtol 1e-12 and explanations were
finite and additive.

Fifteen fidelity tests passed locally and in Docker, covering three seeds,
feature subsets, full configuration, exponential scores at atol 1e-12, real
CSV observations, near-constant features, adversarial root thresholds,
correction functions and guards. Native conversion diverged at adversarial
thresholds; ordinal encoding retained rtol/atol 1e-12 fidelity. Native
predictions agreed outside those cases.

Local command:

```bash
PYTHONPATH=src:. .venv/bin/python -m pytest -q tests/test_iforest_treeshap_conversion.py
```

Docker used the reference image, repository mounted `/audit:ro`,
`PYTHONPATH=/tmp/test-tools:/audit/src:/audit`, and
`python3 -m pytest -q -p no:cacheprovider tests/test_iforest_treeshap_conversion.py`.
Pytest 8.4.2 was installed only under `/tmp/test-tools` in the temporary container;
computation libraries and the reference image were unchanged. Three
matplotlib/SHAP warnings occurred, with no failures.

## Exactness and precision

Twelve exactness tests passed locally and in Docker with identical numerical
results. Nine small configurations (3/5/7 variables, 1/3/5 trees, backgrounds
5/10/20, seeds 7/42/103) were compared with direct enumeration, ExactExplainer
and exhaustive KernelSHAP without regularization. Maximum contribution error
against the oracle was 1.0925e-7; maximum additivity error was 1.2681e-7.
Twenty synthetic full-configuration events (79 variables, 100 trees, ψ=256)
had maximum additivity error 6.7727e-8.

Tolerances fixed before benchmarking were contribution `atol=2e-7`, `rtol=1e-6`
with maximum reference-contribution scale, and additivity `atol=1e-6`,
`rtol=1e-7` against independent mean path length. Base values were checked at
1e-12. Two executions were bitwise identical. A 150-row background, constant
inputs and invalid inputs were covered. These tolerances cover observed error,
without promising exact float64 arithmetic.

The corresponding test file is `tests/test_iforest_treeshap_exactness.py`;
`-s` displays the maxima in the same local/Docker environments.

## Short benchmark

The independent `iforest_tree_vs_kernel.py` runner had short/full profiles and
three passing selection tests. It replayed the actual manager: A scored the
event, a possible refit installed B, the background included the event, and A
was explained. Twenty events covered five groups, first-refit boundaries and
maximum scores, using 79 variables and 2,046 eligible events from 4,999 rows.
Offsets were synthetic, rather than offsets recovered from a past campaign.

| Environment | Tree median | Conversion amortized over the sample | Kernel score median | Maximum additivity error |
|---|---:|---:|---:|---:|
| local | 5.331 ms | 14.300 ms | 23.425 ms | 1.3811e-7 |
| Docker | 5.051 ms | 13.735 ms | 24.947 ms | 1.3811e-7 |

Fidelity, additivity and boundary checks passed. Even amortizing all conversion
cost over only those 20 events, median total cost was lower than the comparison
KernelSHAP profile. This stage did not activate the runtime integration.
Original outputs were under `data/treeshap-evidence/2026-10-01/{short,short-docker}/`.
Run with `--profile short --output <directory>`, `PYTHONPATH=src:.` and
BLAS/OMP threads pinned to 1. Docker source provenance was passed using
`AUDIT_GIT_REVISION` / `AUDIT_WORKTREE`; the runner also recorded its own SHA-256
and thread settings.

## Full benchmark

```bash
PYTHONPATH=src:. OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 \
  .venv/bin/python experiments/explainability/treeshap/iforest_tree_vs_kernel.py \
  --profile full --output data/treeshap-evidence/2026-10-01/full
```

Clean revision `b192a2d`; 300 events, five groups and 18 scoring generations.
Selection covered groups/deciles, extreme scores and refit boundaries;
identities were recorded in `summary.json`.

| Method | Mesures | Median | p95 | Mean RMSE versus Tree-int |
|---|---:|---:|---:|---:|
| Tree interventional | 300 | 5.405 ms | 7.413 ms | 0 |
| Tree path-dependent (different attribution game) | 300 | 1.559 ms | 1.646 ms | 0.01914 |
| Kernel score, 100, default LARS | 300 | 23.314 ms | 24.671 ms | not comparable |
| Kernel E[h], 100, l1_reg=0 | 900 | 47.915 ms | 50.206 ms | 2.27278 |
| Kernel E[h], 1000, l1_reg=0 | 900 | 336.223 ms | 350.211 ms | 0.003742 |
| Kernel E[h], 2000, l1_reg=0 | 900 | 655.987 ms | 682.593 ms | 0.002281 |

Conservative conversion amortization over only 300 selected events was
1.132 ms/event. Total median Tree cost was about 6.537 ms, compared with
23.314 ms for KernelSHAP on the anomaly score (about 3.6 times faster here).
Maximum Tree additivity error was 1.6593e-7, within the fixed tolerance;
fidelity was checked at 1e-12 for every selected point.

Kernel score calculations produced LARS warnings, completed with finite outputs
and used unchanged settings. The poor 100-sample unregularized Kernel E[h]
estimate is a different configuration from production KernelSHAP. Aggregate
convergence improved at higher budgets: mean Spearman was
0.0851 / 0.9913 / 0.9959, without requiring per-event monotonic improvement.

Kernel score and Tree E[h] explain distinct attribution games. Their reversed
ranks were compared: mean Spearman 0.3635 and mean Jaccard@10 0.2740. These
values are not numerical errors in TreeSHAP.

Original output directory: `data/treeshap-evidence/2026-10-01/full/`.
Recorded hashes:

- Input: `8b0b0720389f8ac16f81e54f25de8abcb079bc92170b95b38640ddc7843fcf9f`.
- Events: `d220c2c7a1183abee40222fc5e9e7a87adb4fcdfa6c6cd2449eec493652cd6a1`.
- Summary: `c3c2824a0ebf3a4364f29a2bf021ec3c2874b37bc2f6576d79611432e206d1fe`.

Per-event encoding, construction and computation times were recorded in the
Tree-int JSONL rows.

## Qualification outcome and limits

| Criterion | Result |
|---|---|
| I-a fidelity | I2 Docker/local tests and 300 events at rtol/atol 1e-12: passed |
| I-b exactness | Three oracles, nine configurations, I3 tolerances: passed |
| Additivity / state | Scoring snapshots, backgrounds including x, refit boundaries: passed |
| I-c diagnostic | Aggregate convergence, distinct attribution games, no definition mismatch: passed |
| I-d cost | Tree faster including conversion; cost criterion passed |

Qualification passed, supporting the subsequent integration assessed in the
[full campaign report](../phase-c-iforest-treeshap-2026-10-01.md).
TreeSHAP explains E[h]; its contributions are not additive on the exponential
score or binary verdict. Exact calculation does not validate the scientific
choice of a rolling background containing the event/anomalies or masked inputs
off the data manifold. Full-profile local timings are not Kafka throughput
measurements; short Docker and numerical tests are separate evidence.
