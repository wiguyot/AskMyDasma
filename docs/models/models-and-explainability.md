# Models and Explainability: A Short Guide

This page gives a short overview of the models in DASMA-Kafka. It focuses on
what each model does in this project.

## HSTree — Half-Space Trees

- HSTree detects unusual points in a data stream.
- It splits the feature space into regions. Points in less crowded regions can
  receive higher anomaly scores.
- The service keeps one model for each `Group` and publishes a score, status,
  anomaly decision, and threshold.
- HSTree is designed for fast, continuous monitoring.

## iForest — Isolation Forest

- iForest uses random trees to measure how quickly a point can be separated
  from other points.
- A point that is isolated with fewer steps is treated as more unusual.
- The service trains one model for each `Group` on a moving window of data.
- It is a useful tree-based model for comparison with other detectors.

## KitNET

- KitNET is an online autoencoder for multivariate data streams.
- It first learns which features are related, then measures how well it can
  rebuild each new point.
- A larger reconstruction error gives a higher anomaly score.
- KitNET can also use KernelSHAP in both Explainability services. This can take
  a very long time to compute.

## DeepAnT

- DeepAnT predicts the next value in a time series.
- A large difference between its prediction and the real value gives a higher
  anomaly score.
- It runs separately for each group and uses CPU workers with a fixed limit.
- It can help find changes in a time pattern.

## mDragStream Point-Based

- mDragStream Point-Based builds and updates clusters as data arrives.
- It checks how far each new point is from the clusters it has learned.
- The model has its own anomaly decision. An external P99 or Welford threshold
  can also be used.
- The service publishes the score, status, decision, threshold, and internal
  distance value.

## Anomaly Consensus

- Consensus is a vote between the active models, not a separate detector.
- For each `(Group, source_event_id)`, it counts how many models report an anomaly.
- The source timestamp is retained for display, rather than used as a unique event key.
- It publishes the count and each model's vote for Grafana.

## Explainability

- KernelSHAP is available for HSTree, iForest, and KitNET. KitNET can be very
  slow with this method.
- TreeExplainer is available for iForest.
- TreeExplainer explains the forest's mean path length, not the anomaly score
  itself.
- A feature contribution describes how a feature affects the output being
  explained. It does not prove that the feature caused an anomaly.
