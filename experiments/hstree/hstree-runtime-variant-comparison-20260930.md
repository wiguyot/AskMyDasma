# HSTree Runtime Variant Comparison — 2026-09-30

> Historical report. The original input CSV files, raw exports and generated
> evidence named below are not included in this public repository. Paths describe
> the original runs or output destinations, not bundled files. Reproduction
> requires the original data and matching code/dependency versions. Revision and
> artifact hashes identify the recorded run; they do not guarantee availability.

## Summary

Eight dedicated Kafka campaigns compared `pysad` and `paper` with seeds 42 and
43 on `source-5000.csv` and `source-10000.csv`. Each used a fresh output topic.
JSON exports were saved under `data/hstree-evidence/20260930-b8/` before metrics
were calculated. Joining on `(Group, source_event_id)` gave exact source-identity
coverage, without duplicates or missing events, for all eight campaigns.

Scored counts, P99 anomaly counts, Spearman correlations and Jaccard overlap
agreed with the offline replay. PySAD with seed 43 produced one Welford anomaly
on each dataset, compared with zero offline. Docker used PySAD 0.3.4 and the
local replay used 0.4.0. This environment difference is a plausible explanation,
not an isolated cause. No ground-truth labels were used, so the comparison does
not establish which variant detects anomalies better.

## Protocol and provenance

- Mounted source revision: `699d568f276a76a528a28d1e6bfccb245f55f623`.
- Producer image: `dasma-kafka-producer:0.1.0`, ID
  `sha256:123268561ad44d347e8131136a9618faa9f10e02a3ee32d82340c9882c418f8f`.
- Detector image: `dasma-kafka-anomaly-light:0.1.0`, ID
  `sha256:c7d6c9463d27d82c59c718247140cf68d28bd101e30635b2b29513def30d5833`.
- Runtime: Python 3.12.13, NumPy 2.4.4, PySAD 0.3.4.
- Common settings: ψ/reference window 256, 25 trees, maximum depth 15. Exported
  scores were replayed through P99 (window 500, warmup 100) and windowed Welford
  (window 2,048, warmup 100, `k=3`).
- Original run metadata path:
  `data/hstree-evidence/20260930-b8/<dataset>/<variant>-seed<seed>/run.json`.
  Export hashes were recorded in `comparison.json` in the same parent directory.
- Input SHA-256: `source-5000.csv`:
  `8b0b0720389f8ac16f81e54f25de8abcb079bc92170b95b38640ddc7843fcf9f`;
  `source-10000.csv`:
  `c44d5696283a530f6b10d2a32f35dd27bd4e04ef8ba6b4ff3863d1d25639644c`.
- Each run produced 4,999 or 10,000 valid JSON events. Validation checked unique
  identities, equality with the source set and the `hstree_variant` marker.
  All run IDs and topics were distinct. Temporary campaign containers were
  removed; the topics were left available at the end of the original study.

The service used `HSTREE_ANOMALY_MIN_SAMPLES=2`. For comparability with offline
results, the metrics below recalculate both estimators in event order with
warmup 100; they do not use the service's threshold statuses. Model warmup
remained 256 observations per group.

## Results by campaign

P99 rates use valid model scores as the denominator, matching the offline report.

| Dataset | Variant | Seed | Events | Valid scores | P99 anomalies / rate | Welford anomalies | Exactly zero scores |
|---|---|---:|---:|---:|---:|---:|---:|
| source-5000 | pysad | 42 | 4,999 | 3,326 | 206 / 6.19 % | 0 | 0 |
| source-5000 | pysad | 43 | 4,999 | 3,326 | 223 / 6.70 % | 1 | 0 |
| source-5000 | paper | 42 | 4,999 | 3,326 | 189 / 5.68 % | 0 | 0 |
| source-5000 | paper | 43 | 4,999 | 3,326 | 198 / 5.95 % | 0 | 0 |
| source-10000 | pysad | 42 | 10,000 | 8,327 | 2,142 / 25.73 % | 0 | 0 |
| source-10000 | pysad | 43 | 10,000 | 8,327 | 2,632 / 31.61 % | 1 | 0 |
| source-10000 | paper | 42 | 10,000 | 8,327 | 1,509 / 18.12 % | 0 | 0 |
| source-10000 | paper | 43 | 10,000 | 8,327 | 1,701 / 20.43 % | 0 | 0 |

## Comparisons on shared identities

| Dataset | Comparison | Scored pairs | Spearman | P99 anomalies left / right | Jaccard P99 | Welford anomalies left / right |
|---|---|---:|---:|---:|---:|---:|
| source-5000 | pysad 42 vs 43 (seed variability) | 3,326 | 0.969 | 206 / 223 | 0.758 | 0 / 1 |
| source-5000 | paper 42 vs 43 (seed variability) | 3,326 | 0.913 | 189 / 198 | 0.506 | 0 / 0 |
| source-5000 | pysad vs paper, seed 42 | 3,326 | 0.908 | 206 / 189 | 0.574 | 0 / 0 |
| source-10000 | pysad 42 vs 43 (seed variability) | 8,327 | 0.955 | 2,142 / 2,632 | 0.630 | 0 / 1 |
| source-10000 | paper 42 vs 43 (seed variability) | 8,327 | 0.911 | 1,509 / 1,701 | 0.400 | 0 / 0 |
| source-10000 | pysad vs paper, seed 42 | 8,327 | 0.913 | 2,142 / 1,509 | 0.386 | 0 / 0 |

When both Welford anomaly sets are empty, Jaccard is undefined (`null`); the
anomaly counts are reported directly.

## Conclusion and limitations

Scores and P99 decisions differ between variants, and seeds introduce measurable
variability. Correlations and overlap are nearly identical to the offline
results. The high P99 rates on `source-10000` require descriptive interpretation;
this study alone did not change the default variant. The isolated Welford
anomaly is an environment/version discrepancy with no established cause.

Recalculating raw metrics requires the original exports and analysis artifacts,
which are not bundled. This report retains the measurements and input hashes.
