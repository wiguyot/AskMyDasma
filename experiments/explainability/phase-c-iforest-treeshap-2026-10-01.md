# iForest TreeSHAP Campaign and Regression Evidence — 2026-10-01

> Historical report. The original input CSV files, raw exports and generated
> evidence named below are not included in this public repository. Paths describe
> the original runs or output destinations, not bundled files. Reproduction
> requires the original data and matching code/dependency versions. Revision and
> artifact hashes identify the recorded run; they do not guarantee availability.

## Summary

Interventional TreeSHAP conversion was integrated in V1 and V2 and passed
coverage, detector fidelity, additivity, refit and parity checks on
`source-5000.csv`. HSTree + KernelSHAP regression checks and an iForest
KernelSHAP reference were also run with quantified exports.

Each complete campaign used 4,999 data rows, nine groups, one partition,
background size 100 and initial iForest warmup 512 per group. iForest settings
were `n_estimators=100`, `max_samples=256`, `window_size=2048` and
`refit_every=128`. Input SHA-256:
`8b0b0720389f8ac16f81e54f25de8abcb079bc92170b95b38640ddc7843fcf9f`.

## Campaigns

In this historical report, `against detector` means runtime `reference` mode;
`independent` means runtime `simple` mode.

| Campaign | Mode and method | Coverage / statuses | Fidelity / parity | SHAP median / p95 V1; V2 |
|---|---|---|---|---|
| V-A1 — HSTree regression check | `against detector`, KernelSHAP | 4,999 ; 1,673 `warmup`, 3,326 `computed` | 3,326/3,326 local scores exactly match the detector in each service ; V1/V2 `parity_ok`, 618,696 strictly equal numeric values | 635.405 / 664.13 ms ; 629.57 / 658.57 ms |
| V-B1 — iForest KernelSHAP reference | `against detector`, KernelSHAP | 4,999 ; 2,953 `warmup`, 2,046 `computed` | reference/detector/local scores match ; `parity_ok`, 383,952 strictly equal values | 27.725 / 29.86 ms ; 27.85 / 29.9425 ms |
| V-C1 — iForest TreeSHAP | `against detector`, TreeSHAP interventional | 4,999 ; 2,953 `warmup`, 2,046 `computed` | reference/detector/local scores match ; 13 refit boundaries checked ; maximum additivity error 2.0631e-7 ; mean-path-length → score relation, maximum error 2.22e-16 ; `parity_ok`, 383,952 strictly equal values | 9.9 / 11.6175 ms ; 9.93 / 11.6975 ms |
| V-C2 — iForest TreeSHAP | `independent`, TreeSHAP interventional | 4,999 ; 2,953 `warmup`, 2,046 `computed` | 13 refit boundaries checked ; same additivity and path/score bounds ; `parity_ok`, 383,952 strictly equal values | 10.095 / 11.95 ms ; 10.04 / 11.7175 ms |

All four audits counted zero `failed` or `model_mismatch` events. For V-C1 and
V-C2, both parity categories covered all 4,999 identities with no divergence,
invalid numeric values or merely tolerated equality. Compared contributions
and numeric fields were strictly identical between V1 and V2.

The observed reference-mode iForest TreeSHAP median was about 2.8 times lower
than KernelSHAP on this campaign. This is a configuration-specific operational
cost comparison. TreeSHAP explains mean path length and KernelSHAP explains
the anomaly score, so their attributions are not interchangeable.

## Environment and provenance

- Campaign source revision: `d0a4d71ed2631421ed632de333f4915ebe15eb4e`
  (after I7, before the I8 report commit).
- Explainability image: `dasma-kafka-explainability:0.1.0`, ID
  `sha256:6b9d7328bf4bd38f333c517fac522ad06832fb210fa33688c6fad122588e8fb0`.
- Image versions: Python 3.12.13, SHAP 0.51.0, scikit-learn 1.8.0, NumPy 2.4.4.
  Local audit environment: Python 3.12.15, SHAP 0.51.0, scikit-learn 1.8.0,
  NumPy 2.4.6.
- Infrastructure: Docker Desktop / Engine 29.8.1, Compose 5.5.1,
  Kafka `apache/kafka:3.9.2`.
- Each campaign used an isolated Compose project, volumes and networks.
  Existing standard containers remained stopped and intact. Dedicated services
  were stopped after the campaigns.
- [Campaign runner](../../scripts/integration/run-iforest-treeshap-campaign.sh),
  [Compose overlay](../../docker/compose/experiments/docker-compose.iforest-treeshap.yml)
  and [audit tool](treeshap/audit_iforest_treeshap_campaign.py).
  JSON comparison reports used the `(Group, source_event_id)` parity comparator.

Original exports, manifests and reports were generated under
`data/treeshap-evidence/2026-10-01/` (about 200 MB at the time). They are not
included. The following SHA-256 hashes identify the recorded exports:

| Campaign | V1 JSONL | V2 JSONL | Detector JSONL |
|---|---|---|---|
| V-A1 | `6ccea65d153ce17a5a8672a6d718e175d92a6f5439b61da1948aa1886ab2a0db` | `bd3429c98f66b01940a6dae51b353000d4e6a1b5647e8af731b7e5a53f283822` | `50cc90b7880d0d3ab772c88900489a5829733ab01de3f2f711695fc195e82f50` |
| V-B1 | `7030d323114ce7d313a3a572d9c6acdfdfd42443fa0535fd069566899b460b87` | `c0198f31cd4629e560e2f08c3fd51eefd17ef4afbc2e04910b055438fd4b74e1` | `2f7ef242d4886f632cc1d2247660fd3708cdd2c148ef4b1c1679d6b4559dc149` |
| V-C1 | `5bbb2c541dc1adcef21f7edee7331bae7ca2a157173d1883b24320a958af89b2` | `cd8f11adad7e8358124a80139ba172d28077d8ea8edeb7f17446e7ac895752d1` | `2f7ef242d4886f632cc1d2247660fd3708cdd2c148ef4b1c1679d6b4559dc149` |
| V-C2 | `d7f4c77b644bd47fa4055f737511f557784274feea7268b213507932f02c4e87` | `0f92d461357e182ff63e313702b5b00d6b19d46966d71830a728f9daa25caf2a` | n/a (`independent`) |

## Limitations

- UI/Grafana visual checks are separate; this report assesses runtime contracts
  and JSON outputs, not panel readability.
- One complete input dataset, one partition and one reference image were used.
- Timings depend on the machine, profile and model; they must not be generalized
  to other detectors or volumes.
- TreeSHAP is additive on `iforest_mean_path_length`, not `iforest_score`.
  Applying the score's exponential transform to individual contributions does
  not turn them into additive anomaly-score contributions.
