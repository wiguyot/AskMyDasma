# Source Data and HSTree Dashboard

The [Grafana dashboard](../../observability/grafana/dashboards/By%20model/source-data_hstree_influxdb.json)
plots source features and HSTree output together. Its UID is stable for links
from AskMyDasma; the file is loaded by Grafana provisioning.

## Filters and panels

- **Group** (`group`): selects the logical model group.
- **Feature** (`feature`): selects a source-data measurement field.
- **Features** panel: shows the input time series for the selected filters.
- **HSTree Analysis** panel: shows HSTree score and associated detector output.

The datasource is InfluxDB. Source observations come from `source-data-viz`;
HSTree observations come from `hstree-analysis-viz`. Telegraf stores them in
the corresponding `source-data` and `hstree-analysis` measurements/buckets.

## Interpretation

HSTree supports Paper and PySAD variants. A higher negated mass score is more
unusual, but scores from different variants are not interchangeable. Warmup
can publish `hstree_score=0.0`; a real Paper score can also be zero. Use
`hstree_status` to distinguish these cases and to identify anomaly markers.
Derived logarithmic or moving-average series visualize published numeric values
and may include warmup values.

The graph's timestamp provides display context. For exact event comparisons,
use `(Group, source_event_id)` rather than assuming a unique timestamp.
An unscored group means the detector has not evaluated it, not that it is normal.

See the [HSTree runtime guide](../../runtime/detectors/hstree/hstree_analysis.md),
[storage guide](../architecture/data-storage.md) and
[Grafana guide](observability-grafana-migration.md).
