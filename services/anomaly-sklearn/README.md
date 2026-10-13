# anomaly-sklearn image

Runtime image for anomaly services that need the common Kafka/Avro stack plus
scikit-learn and PyOD-related dependencies.

Current consumers:

- `iforest-analysis`
- `mdragstream-point-analysis`

This image remains separate from `services/anomaly-light` because its consumers use the pinned scikit-learn dependency profile.
Isolation Forest is implemented with `sklearn.ensemble.IsolationForest`;
PySAD/PyOD dependencies in the image do not change that implementation.
