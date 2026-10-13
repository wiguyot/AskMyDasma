# explainability image

Runtime image for SHAP-based explainability services.

Current consumers:

- `explainability`
- `explainability-v2`

This image keeps SHAP and its scientific dependency stack out of the anomaly
runtime images. The application code is still mounted from the repository by
Docker Compose.
