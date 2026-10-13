# HSTree P99 Anomaly-Rate Study — 2026-09-30

> Historical report. The original input CSV files, raw exports and generated
> evidence named below are not included in this public repository. Paths describe
> the original runs or output destinations, not bundled files. Reproduction
> requires the original data and matching code/dependency versions. Revision and
> artifact hashes identify the recorded run; they do not guarantee availability.

## Summary

The pinned replay found 202 anomalies among 2,826 classified events with windows
256/256 on `source-5000`. Of those, 177/202 (87.6%) occurred at positions 32–127
after a reference rollover. On `source-10000`, anomalies remained frequent
across several windows and concentrated in some hours of December 17, 2020.
These results support both a reference-renewal effect and time-dependent score
changes, without identifying a unique cause for each anomaly.

The offline replay and campaign C7a used Python 3.12.13, NumPy 2.4.4 and PySAD
0.3.4 in the detector image. Scores, thresholds and verdicts matched exactly
for all 4,999 identities. Both produced 202 anomalies, matching the earlier C4
replay. The original C7 run had reported 235 anomalies, but its exports were
unavailable. C7a could reproduce 202 but could not identify the first divergence
from C7. This discrepancy remains unresolved in the historical evidence.

## Method

The [study script](hstree-p99-anomaly-study.py) sanitizes columns like the Avro
producer, assigns sequential `source_event_id` values and processes each group
independently in source order. It uses the runtime feature helpers. The tested
exclusions retained **81 features**: sanitized `State...` and
`BackgroundSubtracted...` metadata were still included. These are historical
settings, rather than a statement about current feature exclusions.

Each model used 50 trees, depth 15 and seed 42. P99 used the last 500 normal
scores with 100-score warmup. Windowed Welford used `k=3`, window 2,048 and
warmup 100. Initial/model windows were 128/2,048, 256/256 and 512/512 on
`source-5000`, and 256/256, 512/512 and 1,024/1,024 on `source-10000`.

Original commands:

```bash
docker run --rm -v "$PWD:/app:ro" -v "$PWD/data/hstree-evidence/20260930-c7a-09e30744:/evidence:rw" \
  -w /app --entrypoint /bin/sh dasma-kafka-anomaly-light:0.1.0 -lc \
  'PYTHONPATH=src:. python experiments/hstree/hstree-p99-anomaly-study.py \
    --source data/source-5000.csv --output /evidence/offline-source-5000-pinned.json'
docker run --rm -v "$PWD:/app:ro" -v "$PWD/data/hstree-evidence/20260930-c7a-09e30744:/evidence:rw" \
  -w /app --entrypoint /bin/sh dasma-kafka-anomaly-light:0.1.0 -lc \
  'PYTHONPATH=src:. python experiments/hstree/hstree-p99-anomaly-study.py \
    --source data/source-10000.csv --output /evidence/offline-source-10000-pinned.json'
PYTHONPATH=src:. .venv/bin/python experiments/hstree/compare-hstree-c7a.py \
  --campaign data/hstree-evidence/20260930-c7a-09e30744/hstree-c7a-viz.jsonl \
  --replay data/hstree-evidence/20260930-c7a-09e30744/offline-source-5000-pinned.json \
  --config 256/256 --output data/hstree-evidence/20260930-c7a-09e30744/c7a-vs-offline.json
PYTHONPATH=src:. .venv/bin/python experiments/hstree/compare-hstree-c7a.py \
  --campaign data/hstree-evidence/20260930-c7a-09e30744/hstree-c7a-viz.jsonl \
  --output data/hstree-evidence/20260930-c7a-09e30744/c7a-p99-replay.json
```
The original artifact directory was
`data/hstree-evidence/20260930-c7a-09e30744/`. C7a and offline outputs shared all
4,999 identities, with no duplicates, missing events or differences in score,
status, anomaly flag or threshold. Raw exports and generated files are not
included in the public repository.

## Environment and input hashes

| Dataset | Rows | SHA-256 |
|---|---:|---|
| `data/source-5000.csv` | 4,999 | `8b0b0720389f8ac16f81e54f25de8abcb079bc92170b95b38640ddc7843fcf9f` |
| `data/source-10000.csv` | 10,000 | `c44d5696283a530f6b10d2a32f35dd27bd4e04ef8ba6b4ff3863d1d25639644c` |

Detector image: `dasma-kafka-anomaly-light:0.1.0`, ID
`sha256:c7d6c9463d27d82c59c718247140cf68d28bd101e30635b2b29513def30d5833`.
Export and replay-output hashes were recorded in the original artifact manifest.

## Overall results

Rates below use events classified after the estimator's 100-score warmup,
rather than all real HSTree scores.

| Dataset | Initial/model windows | HSTree scores | P99-classified events | P99 anomalies | Rate | Windowed Welford anomalies |
|---|---:|---:|---:|---:|---:|---:|
| source-5000 | 128/2,048 | 4,053 | 3,466 | 128 | 3.69 % | 0 |
| source-5000 | 256/256 | 3,326 | 2,826 | 202 | 7.15 % | 0 |
| source-5000 | 512/512 | 2,046 | 1,546 | 268 | 17.34 % | 28 |
| source-10000 | 256/256 | 8,327 | 7,827 | 2,359 | 30.14 % | 0 |
| source-10000 | 512/512 | 7,047 | 6,547 | 3,231 | 49.35 % | 38 |
| source-10000 | 1,024/1,024 | 4,487 | 3,987 | 311 | 7.80 % | 21 |

Groups with at most ψ events never received a real score. At ψ=256 these were
`1A` (75 rows), `2B` (215), `3A` (50) and `4B` (53). At ψ=512, `2B` also remained
unscored. At ψ=1,024 only larger groups were scored.

For 256/256 on `source-5000`, anomaly/classified counts by group were IMPBK UV
46/498, NETBK 44/488, Selek 62/536, Sortie CTA 49/746 and TTHBK 1/558. On
`source-10000` they were IMPBK UV 528/1,440, NETBK 411/1,425, Selek 246/1,475,
Sortie CTA 999/1,937 and TTHBK 175/1,550.

## Reference windows and score scale

For 256/256 on `source-5000`, reference windows numbered within each group gave:

| Window | Classified scores | Anomalies | P10 | Median | P90 |
|---:|---:|---:|---:|---:|---:|
| 0 | 775 | 23 | −177,867,646 | −19,398,314 | −6,753,036 |
| 1 | 1,280 | 0 | −236,052,494 | −47,840,462 | −24,814,503 |
| 2 | 692 | 142 | −279,076,967 | −80,127,958 | −631,262 |
| 3 | 79 | 37 | −124,722,776 | −28,292,166 | −6,788,204 |

Window 2 had 142 anomalies among 692 classified events; window 1 had none among
1,280. Median scores changed substantially between windows, showing that
reference renewal affects score scale. Positions 32–127 after rollover contained
177/202 anomalies (87.6%). For 512/512 the same range contained 37/268 (13.8%),
so post-rollover concentration alone does not explain the observed rates.

On `source-10000`, 256/256 and 512/512 remained highly anomalous across several
consecutive windows. At 256/256, 903/2,359 anomalies (38.3%) were at positions
32–127. At 1,024/1,024 the rate fell to 7.80%; rates therefore did not vary
monotonically with ψ. Classified/anomaly counts for windows 0–7 at 256/256 were
775/23, 1,280/0, 1,280/578, 1,280/307, 1,280/497, 1,270/457, 416/262 and 246/235.

## Alignment with event time

For 256/256 on `source-5000`, high hourly rates on December 17, 2020 included
73/103 (70.9%) at 11:00, 57/102 (55.9%) at 10:00 and 49/67 (73.1%) at 12:00.
On `source-10000`, high rates continued at 17:00 (78/102, 76.5%), 13:00
(76/103, 73.8%) and 11:00 (73/103, 70.9%). This supports a time-dependent regime
change as a possible contributor.

Exact cross-group coincidence at minute resolution was limited: 10/202 (5.0%)
anomalies on `source-5000` and 126/2,359 (5.3%) on `source-10000` occurred in
minutes with at least two anomalous groups. This alone is insufficient to
attribute the anomalies to a shared global change.

## Conclusion and possible follow-up

The observations support a mixed mechanism:

- Model/threshold effects: score medians change with reference renewal, and
  256/256 anomalies on `source-5000` cluster after rollover. Updating P99 only
  from normal scores can leave the threshold behind a changing score scale.
- Data effects: `source-10000` anomalies persist across windows and concentrate
  at specific times, rather than always near a rollover.

These two CSV files do not justify exclusive causal attribution. Further work
would require ground truth or replaying the same timestamps with differently
aligned references and inspecting the campaign exports.

If the window effect is confirmed in a deployed configuration, options include
ψ=512 with equal windows, aligning P99's horizon with rollovers, recalibrating
after rollover, or retaining and documenting the existing tradeoff. This study
changed no defaults.
