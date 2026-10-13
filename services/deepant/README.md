# deepant image

Runtime image for the DeepAnT anomaly service.

Current consumer:

- `deepant-analysis`

This image intentionally isolates PyTorch CPU-only from every other runtime
profile. Campaign selection still controls whether the `deepant-analysis`
container is started, and the Python service keeps its `DASMA_ENABLED=0`
kill-switch before heavy imports as a defensive guard.

For campaign parameters, see the [campaign contract](../../docs/architecture/askmydasma-campaign-runtime.md)
and [model parameter guide](../../docs/models/hyper-parameters.md). See the
[beginner guide](../../runtime/detectors/deepant/deepant-for-beginners.md) and
[scientific limits](../../runtime/detectors/deepant/scientific-validity.md)
for interpretation.
