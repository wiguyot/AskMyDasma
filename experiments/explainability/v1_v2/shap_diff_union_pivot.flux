fieldSel = if "${feature}" == "" then "ALL" else "${feature}"

// v2 stores per-feature SHAP under bare <feature> field names (telegraf
// json_v2 nested map). For feature=ALL, every non-SHAP scalar field is
// excluded; the list mirrors the telegraf json_v2 included_keys.
scalarFields = ["explanation_error_code", "explanation_profile", "explanation_status", "explainability_seed", "feature_fingerprint", "hstree_anomaly", "hstree_model_fingerprint", "hstree_model_version", "hstree_score", "hstree_status", "hstree_threshold", "local_hstree_score", "shap_background_size", "shap_base_value", "shap_computation_time_ms", "shap_nsamples"]

fieldName = fieldSel

v1 = from(bucket: "explainability")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "explainability")
  |> filter(fn: (r) => if "${group}" == "" or "${group}" == "ALL" then true else r.Group == "${group}")
  |> filter(fn: (r) => if fieldSel == "ALL" then not contains(value: r._field, set: scalarFields) else r._field == fieldName)
  |> map(fn: (r) => ({ r with _field: "shap_values_" + r._field }))
  |> keep(columns: ["_time", "_field", "Group", "source_event_id", "_value"])
  |> set(key: "side", value: "v1")

v2 = from(bucket: "explainability-v2")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "explainability-v2")
  |> filter(fn: (r) => if "${group}" == "" or "${group}" == "ALL" then true else r.Group == "${group}")
  |> filter(fn: (r) => if fieldSel == "ALL" then not contains(value: r._field, set: scalarFields) else r._field == fieldSel)
  |> filter(fn: (r) => exists r._value)
  |> map(fn: (r) => ({ r with _field: "shap_values_" + r._field }))
  |> keep(columns: ["_time", "_field", "Group", "source_event_id", "_value"])
  |> set(key: "side", value: "v2")

// Validate each identity/feature/side BEFORE pivot: never let pivot choose
// the last value of a duplicate. Do not group by timestamp here: two
// timestamps for the same identity on one side are also ambiguous.
checked = union(tables: [v1, v2])
  |> group(columns: ["_field", "Group", "source_event_id", "side"])
  |> reduce(
    identity: {row_count: 0, _value: 0.0, _time: time(v: 0)},
    fn: (r, accumulator) => ({
      row_count: accumulator.row_count + 1,
      _value: r._value,
      _time: r._time
    })
  )
  |> map(fn: (r) => if r.row_count == 1 then r else
    die(msg: "SHAP comparison: duplicate " + r.side + " Group=" + r.Group + " source_event_id=" + r.source_event_id + " feature=" + r._field))

checked
  |> group(columns: ["_field", "Group"])
  |> pivot(rowKey: ["source_event_id", "_time"], columnKey: ["side"], valueColumn: "_value")
  // Identity is mandatory; timestamp additionally verifies source-time parity.
  // Never turn a missing counterpart (including a different timestamp) into zero.
  |> map(fn: (r) => if exists r.v1 and exists r.v2 then r else
    die(msg: "SHAP comparison: missing counterpart or timestamp mismatch Group=" + r.Group + " source_event_id=" + r.source_event_id + " feature=" + r._field + " time=" + string(v: r._time)))
  |> map(fn: (r) => ({_time: r._time, _field: "diff " + r._field + " group=\"" + r.Group + "\"", Group: r.Group, source_event_id: r.source_event_id, _value: r.v2 - r.v1}))
  |> group(columns: ["Group", "_field"])
  |> sort(columns: ["_time", "source_event_id"])
  |> yield(name: "diff")
