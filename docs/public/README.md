# AskMyDasma Documentation

This page collects the main user and technical guides for AskMyDasma. It
explains how to start the local platform, how its main parts work, and what the
anomaly-detection and Explainability services do.

The repository and Python package use the technical name `dasma-kafka`.
Earlier documentation may refer to the project as DASMA-Kafka.

## Start here

- [Project overview, prerequisites, and quick start](../../README.md)
- [Architecture overview](../architecture/bootstrap.md)
- [System diagram](../architecture/schema.md)
- [AskMyDasma campaign runtime contract](../architecture/askmydasma-campaign-runtime.md)
- [Runtime validation workflow](../architecture/askmydasma-validation-workflow.md)

## Models

- [HSTree beginner guide](../../runtime/detectors/hstree/HSTree-for-beginners.md)
- [iForest beginner guide](../../runtime/detectors/iforest/IForest-for-beginners.md)
- [KitNET beginner guide](../../runtime/detectors/kitnet/kitnet-for-beginners.md)
- [DeepAnT beginner guide](../../runtime/detectors/deepant/deepant-for-beginners.md)
- [mDragStream Point beginner guide](../../runtime/detectors/mdragpoint/mdragpoint-for-beginners.md)
- [Model and Explainability summary](../models/models-and-explainability.md)
- [Explainability guide and evidence limits](explainability.md)
- [Acknowledgments and algorithm references](ACKNOWLEDGMENTS.md)

## Validation evidence

This report describes an offline study and a separate Docker witness on a
named dataset and code revision. Its results apply to those configurations;
they do not promise performance on other data or later versions.

- [iForest paper-conformance study](../../experiments/iforest/iforest-paper-conformance-2026-10-01.md)

See the [validation evidence index](../../experiments/README.md) for the other
retained reports. The [technical documentation
index](../README.md) also covers storage, observability and model limitations.
