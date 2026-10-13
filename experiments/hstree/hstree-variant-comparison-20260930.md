# HSTree Offline Variant Comparison — 2026-09-30

> Historical report. The original input CSV files, raw exports and generated
> evidence named below are not included in this public repository. Paths describe
> the original runs or output destinations, not bundled files. Reproduction
> requires the original data and matching code/dependency versions. Revision and
> artifact hashes identify the recorded run; they do not guarantee availability.

## Summary

The offline replay compared `pysad` and `paper` on `source-5000.csv` and
`source-10000.csv`, using 81 features, ψ=256, 25 trees, depth 15 and P99 with
window 500 and warmup 100. Each variant was replayed with seeds 42 and 43 to
separate algorithm differences from tree randomness. Scores and verdicts were
aligned by `source_event_id` within each group.

No ground-truth labels were used. Counts and rates describe these datasets,
orders and settings; they do not establish AUC or superiority of either variant.

## Method and reproducibility

- Script: `experiments/hstree/hstree-variant-comparison.py`, using the runtime
  model implementation available at the time of the replay.
- Base revision: `c66b7ed082da857690b5684bae4c686893cab1e8`, plus uncommitted
  Phase 3 implementation changes. The base hash alone does not fully identify
  the tested source tree.
- Local environment: Python 3.12.14, NumPy 2.4.6, PySAD 0.4.0.
- Groups initialized under `isolated_rng(seed)` in CSV row order. The first ψ
  events per group were unscored; groups with at most ψ events never left warmup.
- Estimators were recalculated separately for each variant: P99 window 500,
  warmup 100; windowed Welford window 2,048, `k=3`.
- Timings measure `HSTreeGlobalModel.partial_fit_score` per event, including
  warmup and delayed model creation. They do not measure KernelSHAP. Initialization
  can inflate the mean, making the median and P95 more informative.
- These measurements used the local environment, rather than the PySAD 0.3.4
  Docker campaign image. Subsequent runtime checks are documented separately.

Input hashes:

| Dataset | Rows | Features | SHA-256 |
|---|---:|---:|---|
| `data/source-5000.csv` | 4,999 | 81 | `8b0b0720389f8ac16f81e54f25de8abcb079bc92170b95b38640ddc7843fcf9f` |
| `data/source-10000.csv` | 10,000 | 81 | `c44d5696283a530f6b10d2a32f35dd27bd4e04ef8ba6b4ff3863d1d25639644c` |

Original per-event outputs and their hashes were written under
`data/hstree-evidence/20260930-variants/`; they are not bundled.

## Overall results

| Dataset / variant (seed 42) | Scored events | P99 anomalies | P99 rate | Welford anomalies | Exactly zero Paper scores | Model median / event | Model P95 / event |
|---|---:|---:|---:|---:|---:|---:|---:|
| source-5000 — `pysad` | 3,326 | 206 | 6.19 % | 0 | — | 0.102 ms | 0.131 ms |
| source-5000 — `paper` | 3,326 | 189 | 5.68 % | 0 | 0 | 0.071 ms | 0.088 ms |
| source-10000 — `pysad` | 8,327 | 2,142 | 25.73 % | 0 | — | 0.107 ms | 0.139 ms |
| source-10000 — `paper` | 8,327 | 1,509 | 18.12 % | 0 | 0 | 0.073 ms | 0.091 ms |

Groups `1A`, `2B`, `3A` and `4B` were below ψ in both datasets. Both variants
therefore scored the same 3,326 or 8,327 events. No real Paper score was exactly
zero on these inputs; this does not rule out valid zero scores on other inputs.

## Algorithm differences and seed variability

Spearman uses all shared scores; Jaccard compares P99 anomaly identity sets.

| Dataset | Comparison | Shared scores | Spearman | Anomalies left / right | Jaccard P99 |
|---|---|---:|---:|---:|---:|
| source-5000 | `pysad` seed 42 vs 43 (seed variability) | 3,326 | 0.969 | 206 / 223 | 0.758 |
| source-5000 | `paper` seed 42 vs 43 (seed variability) | 3,326 | 0.913 | 189 / 198 | 0.506 |
| source-5000 | `pysad` vs `paper`, seed 42 | 3,326 | 0.908 | 206 / 189 | 0.574 |
| source-10000 | `pysad` seed 42 vs 43 (seed variability) | 8,327 | 0.955 | 2,142 / 2,632 | 0.630 |
| source-10000 | `paper` seed 42 vs 43 (seed variability) | 8,327 | 0.911 | 1,509 / 1,701 | 0.400 |
| source-10000 | `pysad` vs `paper`, seed 42 | 8,327 | 0.913 | 2,142 / 1,509 | 0.386 |

The score and verdict differences are real, but their magnitude is comparable
to seed variability, especially for Paper. Paper's median model time was about
30% lower in this local replay. This is neither a SHAP benchmark nor a runtime
throughput guarantee. P99 rates on `source-10000` remained high with both
variants. Windowed Welford returned no anomalies in the reported runs. This
descriptive comparison alone did not justify changing the default variant.

### P99 counts by group

| Dataset | Group | Scored | `pysad` anomalies | `paper` anomalies |
|---|---|---:|---:|---:|
| source-5000 | Sortie CTA | 846 | 51 | 45 |
| source-5000 | TTHBK | 658 | 1 | 2 |
| source-5000 | Selek | 636 | 64 | 88 |
| source-5000 | IMPBK UV | 598 | 46 | 23 |
| source-5000 | NETBK | 588 | 44 | 31 |
| source-10000 | Sortie CTA | 2,037 | 882 | 596 |
| source-10000 | TTHBK | 1,650 | 173 | 105 |
| source-10000 | Selek | 1,575 | 284 | 497 |
| source-10000 | IMPBK UV | 1,540 | 466 | 161 |
| source-10000 | NETBK | 1,525 | 337 | 150 |

## Replay commands

The commands below require the original input files and matching implementation:

```bash
PYTHONPATH=src:. .venv/bin/python experiments/hstree/hstree-variant-comparison.py \
  --source data/source-5000.csv \
  --output data/hstree-evidence/20260930-variants/source-5000.json
PYTHONPATH=src:. .venv/bin/python experiments/hstree/hstree-variant-comparison.py \
  --source data/source-10000.csv \
  --output data/hstree-evidence/20260930-variants/source-10000.json
```
Counts, scores, correlations and anomaly sets are deterministic for a fixed
code version, dependency set, dataset and seed. Durations vary and must not be
compared byte for byte. The script outputs per-group metrics and event series.
Kafka campaigns, V1/V2 fidelity, repeatability and KernelSHAP timings are
separate checks; see the [runtime report](hstree-runtime-validation-20260930.md)
and [runtime variant comparison](hstree-runtime-variant-comparison-20260930.md).
