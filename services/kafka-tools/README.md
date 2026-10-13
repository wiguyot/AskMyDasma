# kafka-tools image

Runtime image for small Kafka utility services that only need the Confluent
Kafka Python client plus repository code mounted by Docker Compose.

Current consumers:

- `init-topic`
- `build-metrics`
- `consensus-analysis`
- `repeatability-diff`
- `explainability-shap-diff`
- `reproducibility-export`
- `reproducibility-publish`

This profile keeps topic initialization, metrics publication, and consensus
packaging separate from the heavier anomaly and explainability images.
