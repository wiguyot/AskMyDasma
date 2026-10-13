# Validation Evidence and Offline Tools

These dated reports describe specific datasets, dependency versions and
configurations. They are historical measurements, not guarantees for later
versions or evidence of detection quality on unlabeled data. Generated outputs
under `data/` are local artifacts; the reports state their availability and
reproduction limits.

## HSTree

- [Runtime fidelity and parity](hstree/hstree-runtime-validation-20260930.md)
- [Runtime variant comparison](hstree/hstree-runtime-variant-comparison-20260930.md)
- [Offline variant comparison](hstree/hstree-variant-comparison-20260930.md)
- [P99 anomaly-rate study](hstree/hstree-p99-anomaly-rate-20260930.md)

## Isolation Forest and TreeSHAP

- [Paper-conformance study](iforest/iforest-paper-conformance-2026-10-01.md)
- [TreeSHAP campaign evidence](explainability/phase-c-iforest-treeshap-2026-10-01.md)
- [TreeSHAP qualification](explainability/treeshap/iforest-treeshap-qualification-2026-10-01.md)

Offline scripts are organized under `hstree/`, `iforest/` and `explainability/`.
Inspect each script's command-line options and dependency requirements before
running a study. Some campaign tools require a local Docker/Kafka stack.

## Explainability tool directories

- `regular/`: offline timing and log-analysis tools for V1.
- `treeshap/`: iForest method comparison and campaign audits.
- `v1_v2/`: parity comparison, model-state diagnostics and Grafana query checks.
- `v2/`: structural validation, seed stability and capacity tools.

These tools support regression tests and reproducible studies. Campaign runners
and their Docker Compose overlays are kept under `scripts/integration/` and
`docker/compose/experiments/`; generated run outputs are not bundled.

## Known limitation of the historical stability study

`explainability/v2/shap_stability_runner.py` calls SHAP through an outdated
interface. `capacity_benchmark.py` imports its helper and shares that limitation.
These offline studies require adaptation before execution. They are not called
by the UI or production services.
