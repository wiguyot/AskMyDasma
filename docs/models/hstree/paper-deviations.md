# HSTree — Implementation Differences and Limits

AskMyDasma provides two HSTree variants: `paper` and `pysad`. Paper is the
default for new campaigns; existing campaigns keep their saved selection.
Both are displayed as HSTree in the interface.

## Algorithm reference

Tan, S. C., Ting, K. M. and Liu, T. F. *Fast Anomaly Detection for Streaming
Data*. IJCAI 2011, pp. 1511–1516.
[Original paper](https://www.ijcai.org/Proceedings/11/Papers/254.pdf).

## Variant differences

| Behavior | Paper variant | PySAD variant |
|---|---|---|
| Working space | Independently randomized expanded bounds per tree | Shared minimum/maximum bounds from the initial observations |
| Tree score | Reference mass at the terminal node, weighted by depth | Sum along the visited path |
| Early stopping | Maximum depth or reference mass at most `0.1 × window_size` | PySAD path-scoring rule |

The Paper variant uses a local `PaperHalfSpaceTrees` subclass and reuses PySAD's
mass updates. For each feature, its expanded bounds use a uniform random `s`
in `[0, 1]`, `m = max(s, 1-s)`, and initial width `w`:

```text
lower = min + (s - 2m) × w
upper = min + (s + 2m) × w
```

A constant feature uses width 1. Both variants publish a negated mass score:
a higher value indicates a more unusual observation.

## Streaming integration

- Each group has its own model and threshold state.
- Initial and reference windows must have equal sizes; the default is 256.
- The default forest has 25 trees.
- After initialization, the model scores each event before counting it.
- The event completing initialization remains in warmup; the next event gets
  the first real score.
- Warmup publishes a `0.0` sentinel. A real score can also equal zero, so
  consumers must inspect `hstree_status`.
- A group that never exceeds its initial window never gets a real score.

P99 is the default operational threshold; Welford and windowed Welford are
also available. These verdict estimators are additions to the published
algorithm. P99 updates from scores classified normal and does not guarantee
an anomaly rate of 1%. Welford may produce an ineffective positive threshold
for negative scores on some datasets.

## Explainability and evidence

The detector and Explainability mirrors must use matching parameters, feature
ordering, random state and event ordering for score fidelity. A reconstructed
local model is not a shared detector instance.

- [Runtime validation](../../../experiments/hstree/hstree-runtime-validation-20260930.md)
- [Runtime variant comparison](../../../experiments/hstree/hstree-runtime-variant-comparison-20260930.md)
- [P99 anomaly-rate study](../../../experiments/hstree/hstree-p99-anomaly-rate-20260930.md)

These dated, configuration-specific observations do not establish detection
quality or superiority of a variant without ground-truth labels. Dependency
versions and partition layout can also affect reproducibility.

See the [detector guide](../../../runtime/detectors/hstree/hstree_analysis.md)
for configuration and output fields.
