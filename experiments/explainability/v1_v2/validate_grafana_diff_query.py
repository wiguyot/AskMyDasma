"""Opt-in, read-only Grafana/Flux checks using synthetic array.from inputs.

No database writes or Docker commands. Execute explicitly against a running
Grafana with the influxdb-dasma datasource. Tests run the actual candidate Flux
pipeline, including duplicate and missing-counterpart guards.
"""

from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path
import urllib.error
import urllib.request


def comparison_rows(response: dict) -> Counter:
    """Decode by field name: Grafana field order can vary between frames."""
    rows = Counter()
    for result in response.get("results", {}).values():
        if result.get("error"):
            raise ValueError(result["error"])
        for frame in result.get("frames", []):
            fields = frame["schema"]["fields"]
            columns = {field["name"]: values for field, values in
                       zip(fields, frame["data"]["values"])}
            if not columns:
                continue
            labels = next(f["labels"] for f in fields if f["name"] == "_value")
            rows.update((labels["Group"], labels["_field"], timestamp, identity, value)
                        for timestamp, identity, value in zip(
                            columns["_time"], columns["source_event_id"], columns["_value"]))
    return rows


def execute(grafana_url: str, query: str) -> dict:
    payload = {"from": "2026-09-25T12:00:00Z", "to": "2026-09-25T12:05:00Z",
               "queries": [{"refId": "A", "query": query,
                            "datasource": {"type": "influxdb", "uid": "influxdb-dasma"},
                            "intervalMs": 1000, "maxDataPoints": 2000}]}
    request = urllib.request.Request(grafana_url.rstrip("/") + "/api/ds/query",
                                     data=json.dumps(payload).encode(),
                                     headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=40) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        if error.code != 400:
            raise
        return json.loads(error.read())


def fixture_stream(side: str, rows: list[tuple]) -> str:
    records = []
    for group, identity, feature, timestamp, value in rows:
        records.append('{Group: %s, source_event_id: %s, _field: %s, _time: time(v: %s), _value: %s, side: %s}' % (
            json.dumps(group), json.dumps(identity), json.dumps(feature),
            json.dumps(timestamp), repr(float(value)), json.dumps(side)))
    return side + " = array.from(rows: [" + ", ".join(records) + "])\n"


def validate(query: str, grafana_url: str) -> list[dict]:
    pipeline = query[query.index("// Validate each identity/"):]
    timestamp = "2026-09-25T12:00:00Z"
    # Same timestamp, distinct IDs; same ID, different groups and features.
    v1 = [("G1", "row-1", "shap_values_A", timestamp, 1.0),
          ("G1", "row-2", "shap_values_A", timestamp, 2.0),
          ("G2", "row-1", "shap_values_A", timestamp, 3.0),
          ("G1", "row-1", "shap_values_B", timestamp, 4.0)]
    v2 = [r[:-1] + (value,) for r, value in zip(v1, [1.0, 5.0, 2.0, 4.5])]
    expected = Counter((r[0], 'diff ' + r[2] + ' group="' + r[0] + '"',
                        1790337600000, r[1], b[-1] - r[-1]) for r, b in zip(v1, v2))
    cases = [
        ("nominal", v1, v2, None),
        ("reversed", list(reversed(v1)), list(reversed(v2)), None),
        ("missing_v1", v1[1:], v2, "missing counterpart or timestamp mismatch"),
        ("missing_v2", v1, v2[1:], "missing counterpart or timestamp mismatch"),
        ("duplicate_identical_v1", v1 + [v1[0]], v2, "duplicate v1"),
        ("duplicate_conflicting_v2", v1, v2 + [v2[0][:-1] + (9.0,)], "duplicate v2"),
        ("duplicate_other_timestamp", v1 + [v1[0][:3] + ("2026-09-25T12:00:01Z", 1.0)], v2, "duplicate v1"),
        ("timestamp_mismatch", [v1[0][:3] + ("2026-09-25T12:00:01Z", 1.0)] + v1[1:], v2,
         "missing counterpart or timestamp mismatch"),
    ]
    results = []
    for name, left, right, expected_error in cases:
        flux = 'import "array"\n' + fixture_stream("v1", left) + fixture_stream("v2", right) + pipeline
        response = execute(grafana_url, flux)
        errors = [result["error"] for result in response.get("results", {}).values() if result.get("error")]
        if expected_error:
            passed = any(expected_error in error for error in errors)
        else:
            passed = not errors and comparison_rows(response) == expected
        results.append({"case": name, "passed": passed, "errors": errors})
        print(name + ": " + ("PASS" if passed else "FAIL"), flush=True)
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--query-file", required=True, type=Path)
    parser.add_argument("--grafana-url", default="http://localhost:3000")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    results = validate(args.query_file.read_text(), args.grafana_url)
    args.output.write_text(json.dumps(results, indent=2, sort_keys=True) + "\n")
    return 0 if all(result["passed"] for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
