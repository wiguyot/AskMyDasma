# HSTree and Explainability Runtime Validation — 2026-09-30

> Historical report. The original input CSV files, raw exports and generated
> evidence named below are not included in this public repository. Paths describe
> the original runs or output destinations, not bundled files. Reproduction
> requires the original data and matching code/dependency versions. Revision and
> artifact hashes identify the recorded run; they do not guarantee availability.

## Summary

Three Docker campaigns on `data/source-5000.csv` used the V1/V2 parity runner:
PySAD with 25 trees, PySAD with 50 trees, and Paper with 25 trees. Every detector,
V1 and V2 export contained all 4,999 expected identities. For all 3,326 computed
identities, both local scores exactly matched the detector score. V1/V2
comparisons returned `parity_ok` with no numerical divergence.

HSTree-only repeatability campaigns launched through AskMyDasma for PySAD and
Paper showed `identical=1` and `abs_diff=0.0` for all complete observed results.
The topics also contained older campaign results, so global offsets and counters
could not isolate the identity count for each repeatability run. This supports
the observed equality with a weaker counting record than the parity campaigns.

## Environment and provenance

- Date: 2026-09-30.
- Source revision: `8d49af1da0931a2a42a0fe160cb19bcafc488e22`.
- Dataset: `data/source-5000.csv`, 4,999 data rows; SHA-256
  `8b0b0720389f8ac16f81e54f25de8abcb079bc92170b95b38640ddc7843fcf9f`.
- Docker Engine client/server: 29.8.1 / 29.8.1. Kafka 3.9.2, Grafana 12.0.0,
  InfluxDB 2.7 and Karapace Schema Registry tagged `latest` were used.
- Shared Compose stack, with single-partition topics. Campaign services were
  stopped after checks; infrastructure remained running. No topics were reset
  or deleted during these checks.
- Original unversioned output directories: `/tmp/hstree-phase3-pysad-25`,
  `/tmp/hstree-phase3-pysad-50`, `/tmp/hstree-phase3-paper-25`.
- Run IDs: `v1v2-20260930-175748-614ea785` (PySAD/25),
  `v1v2-20260930-181008-e36e9240` (PySAD/50),
  `v1v2-20260930-183550-f72b021c` (Paper/25).

## Parity campaign results

| Variant / trees | Campaign duration | Compared identities | Warmup | Computed | Strictly equal numeric values | Verdict |
|---|---:|---:|---:|---:|---:|---|
| PySAD / 25 | 690.81 s | 4,999 | 1,673 | 3,326 | 598,740 | `parity_ok` |
| PySAD / 50 | 1,383.96 s | 4,999 | 1,673 | 3,326 | 598,740 | `parity_ok` |
| Paper / 25 | 377.37 s | 4,999 | 1,673 | 3,326 | 598,740 | `parity_ok` |

`three-export-coverage.json` reported `all_equal: true`: no missing or extra
identities in any export. The comparator found no divergence, invalid numeric
value or status mismatch, and counted 598,740 `strict_equal` numeric values.
The local score exactly equaled `hstree_score` in both services for all 3,326
computed records in each configuration. Paper outputs carried
`hstree_model_version=local-hstree-paper-v1`.

## SHAP computation time

These values summarize exported `shap_computation_time_ms`:

| Variant / trees | V1 median | V2 median | V1 P95 | V2 P95 | Observed maximum V1 / V2 |
|---|---:|---:|---:|---:|---:|
| PySAD / 25 | 657.42 ms | 660.67 ms | 695.50 ms | 704.50 ms | 768.24 / 785.40 ms |
| PySAD / 50 | 1,438.32 ms | 1,411.31 ms | 1,538.91 ms | 1,596.48 ms | 1,748.92 / 1,837.03 ms |
| Paper / 25 | 295.49 ms | 298.33 ms | 355.80 ms | 356.73 ms | 400.44 / 405.09 ms |

Doubling PySAD's tree count increased median SHAP time in this configuration.
Paper with 25 trees was faster than PySAD with 25 trees on this machine and
campaign. These observations do not establish a general speedup or scientific
equivalence between variants.

## Repeatability and interface checks

Both repeatability runs had equal complete comparisons in `repeatability-diff`,
with no observed complete mismatch. Persistent topics mixed these results with
older outputs. A strict event-count audit requires a supported non-destructive
run delimiter, such as distinct output topics or run IDs.

After stopping computation services and reloading AskMyDasma, the form restored
the `source-5000` dataset, HSTree selection, Repeatability checkbox, 25 trees
and Paper variant. The Paper campaign configuration and exported model version
also confirmed the selected variant.

## Scope and limitations

- These runs establish detector/local fidelity and V1/V2 parity for the three
  named configurations, rather than a complete statistical variant comparison.
- A later [runtime variant comparison](hstree-runtime-variant-comparison-20260930.md)
  used eight isolated runs with fresh topics, both seeds 42/43 and both datasets.
- Repeatability observations were positive, with counting limited by topic reuse.
- No destructive cleanup was performed as part of this historical validation.
