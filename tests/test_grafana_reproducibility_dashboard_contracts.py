"""Contracts for the reproducibility Grafana dashboard (P2-7).

Static inspection of observability/grafana/dashboards/reproducibility_influxdb.json: metadata,
valid JSON, panel/variable structure, buckets, measurements, filters and the
strict separation from every repeatability-* resource.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from unittest import TestCase

ROOT = Path(__file__).resolve().parents[1]
DASHBOARD = ROOT / "observability" / "grafana" / "dashboards" / "reproducibility_influxdb.json"
DIFF_BUCKET = "reproducibility-diff"
SUMMARY_BUCKET = "reproducibility-summary"


class DashboardMetadataTest(TestCase):
    """Valid JSON, UID, title, tags and default period."""

    def setUp(self) -> None:
        self.dashboard = json.loads(DASHBOARD.read_text(encoding="utf-8"))

    def test_uid_title_tags_and_period(self) -> None:
        self.assertEqual(self.dashboard["uid"], "reproducibility-influxdb")
        self.assertEqual(self.dashboard["title"], "Reproducibility")
        self.assertEqual(
            self.dashboard["tags"], ["reproducibility", "influxdb", "observability"]
        )
        self.assertEqual(self.dashboard["time"]["from"], "now-30m")
        self.assertEqual(self.dashboard["time"]["to"], "now")

    def test_valid_json_with_schema_version(self) -> None:
        self.assertIsInstance(self.dashboard["panels"], list)
        self.assertGreaterEqual(self.dashboard["schemaVersion"], 30)


class VariableContractTest(TestCase):
    """Three single-choice variables with a defaulted ALL line."""

    def setUp(self) -> None:
        self.dashboard = json.loads(DASHBOARD.read_text(encoding="utf-8"))
        self.variables = {v["name"]: v for v in self.dashboard["templating"]["list"]}

    def test_three_single_choice_variables(self) -> None:
        self.assertEqual(
            set(self.variables), {"campaign_id", "model", "group"}
        )
        for name, variable in self.variables.items():
            self.assertEqual(variable["type"], "query", name)
            self.assertFalse(variable["multi"], name)
            self.assertFalse(variable["includeAll"], name)
            self.assertEqual(variable["current"]["text"], "ALL", name)
            self.assertEqual(variable["current"]["value"], "ALL", name)

    def test_variables_use_schema_tag_values_with_all_line(self) -> None:
        expected = {
            "campaign_id": "campaign_id",
            "model": "model",
            "group": "Group",
        }
        for name, tag in expected.items():
            query = self.variables[name]["query"]
            self.assertIn("schema.tagValues(", query, name)
            self.assertIn(f'tag: "{tag}"', query, name)
            self.assertIn('array.from(rows: [{_value: "ALL"}])', query, name)
            self.assertIn(f'bucket: "{DIFF_BUCKET}"', query, name)
            self.assertIn(f'r._measurement == "{DIFF_BUCKET}"', query, name)

    def test_no_field_keys_variables(self) -> None:
        for variable in self.variables.values():
            self.assertNotIn("schema.fieldKeys", variable["query"])


class PanelContractTest(TestCase):
    """The six minimum panels with the right buckets and filters."""

    def setUp(self) -> None:
        self.dashboard = json.loads(DASHBOARD.read_text(encoding="utf-8"))
        self._expected_titles = [
            "Comparison summary",
            "Scores source-1 vs source-2",
            "Score differences per event (abs_diff / relative_diff)",
            "Timestamp offset (execution diagnostic",
            "Divergences and missing events",
            "First diagnostic",
        ]

    def _panel(self, title_prefix: str) -> dict:
        matches = [
            panel for panel in self.dashboard["panels"]
            if panel["title"].startswith(title_prefix)
        ]
        self.assertEqual(len(matches), 1, title_prefix)
        return matches[0]

    def _queries_of(self, title_prefix: str) -> list[str]:
        return [target["query"] for target in self._panel(title_prefix)["targets"]]

    def _assert_filters(self, query: str, summary: bool = False) -> None:
        self.assertIn(
            'if "${campaign_id}" == "ALL" then true else r.campaign_id == "${campaign_id}"',
            query,
        )
        self.assertIn(
            'if "${model}" == "ALL" then true else r.model == "${model}"', query
        )
        if not summary:
            self.assertIn(
                'if "${group}" == "ALL" then true else r.Group == "${group}"', query
            )

    def test_six_expected_panels(self) -> None:
        for title in self._expected_titles:
            self._panel(title)  # must exist exactly once

    def test_comparison_summary_panel(self) -> None:
        queries = self._queries_of("Comparison summary")
        self.assertEqual(len(queries), 5)
        fields = set()
        for query in queries:
            self.assertIn(f'from(bucket: "{SUMMARY_BUCKET}")', query)
            self.assertIn(f'r._measurement == "{SUMMARY_BUCKET}"', query)
            self.assertIn("|> last()", query)
            self._assert_filters(query, summary=True)
            match = re.search(r'_field == "([a-z_0-9]+)"', query)
            self.assertIsNotNone(match)
            fields.add(match.group(1))
        self.assertEqual(
            fields,
            {"exact", "within_tolerance", "divergent", "missing_source_1", "missing_source_2"},
        )

    def test_scores_panel(self) -> None:
        queries = self._queries_of("Scores source-1 vs source-2")
        self.assertEqual(len(queries), 2)
        for query in queries:
            self.assertIn(f'from(bucket: "{DIFF_BUCKET}")', query)
            self._assert_filters(query)
        fields = {re.search(r'_field == "([a-z_0-9]+)"', q).group(1) for q in queries}
        self.assertEqual(fields, {"score_source_1", "score_source_2"})

    def test_score_differences_panel(self) -> None:
        queries = self._queries_of("Score differences per event (abs_diff / relative_diff)")
        fields = {re.search(r'_field == "([a-z_0-9]+)"', q).group(1) for q in queries}
        self.assertEqual(fields, {"score_abs_diff", "score_relative_diff"})
        for query in queries:
            self._assert_filters(query)

    def test_timestamp_offset_panel_is_explicitly_diagnostic(self) -> None:
        title = "Timestamp offset (execution diagnostic"
        panel = self._panel(title)
        queries = self._queries_of(title)
        self.assertEqual(
            {re.search(r'_field == "([a-z_0-9]+)"', q).group(1) for q in queries},
            {"timestamp_offset_seconds"},
        )
        description = panel.get("description", "")
        lowered = (title + " " + description).lower()
        self.assertIn("diagnostic", lowered)
        self.assertIn("never", lowered)
        for query in queries:
            self._assert_filters(query)

    def test_divergences_and_missing_panel(self) -> None:
        title = "Divergences and missing events"
        panel = self._panel(title)
        self.assertEqual(panel["type"], "table")
        queries = self._queries_of(title)
        # A single Flux query with schema.pivot gathers status, differing
        # fields, maxima and scores of one event into one table row.
        self.assertEqual(len(queries), 1)
        query = queries[0]
        # pivot() is the native Flux function (same as the repeatability
        # dashboard); no schema. prefix and no schema import.
        self.assertIn("pivot(rowKey:", query)
        self.assertNotIn("schema.pivot(", query)
        self.assertNotIn('import "influxdata/influxdb/schema"', query)
        self.assertIn('columnKey: ["_field"]', query)
        self.assertNotIn("join(", query)
        self.assertIn('r.comparison_status == "divergent"', query)
        self.assertIn('r.comparison_status == "missing_source_1"', query)
        self.assertIn('r.comparison_status == "missing_source_2"', query)
        self.assertIn('"differing_fields"', query)
        self.assertIn('"max_abs_diff"', query)
        self.assertIn('"max_relative_diff"', query)
        self.assertIn('"score_source_1"', query)
        self.assertIn('"score_source_2"', query)
        self.assertIn('from(bucket: "reproducibility-diff")', query)
        self._assert_filters(query)

    def test_first_diagnostic_panel(self) -> None:
        queries = self._queries_of("First diagnostic")
        joined = "\n".join(queries)
        for field in (
            "first_divergence_group",
            "first_divergence_source_event_id",
            "first_divergence_fields",
            "first_missing_source_1_group",
            "first_missing_source_1_source_event_id",
            "first_missing_source_2_group",
            "first_missing_source_2_source_event_id",
        ):
            self.assertIn(f'"{field}"', joined, field)
        for query in queries:
            self.assertIn(f'from(bucket: "{SUMMARY_BUCKET}")', query)
            self._assert_filters(query, summary=True)


class SeparationContractTest(TestCase):
    """Strict separation from repeatability-* and no stray references."""

    def setUp(self) -> None:
        self.dashboard = json.loads(DASHBOARD.read_text(encoding="utf-8"))

    def test_no_join_and_no_repeatability_in_dashboard(self) -> None:
        text = DASHBOARD.read_text(encoding="utf-8")
        self.assertNotIn("join(", text)
        self.assertNotIn("repeatability", text)

    def test_no_source_buckets(self) -> None:
        text = DASHBOARD.read_text(encoding="utf-8")
        for forbidden in ("topic-metrics", "source-data", "hstree-analysis",
                          "iforest-analysis", "kitnet-analysis", "deepant-analysis",
                          "mdragstream-point-analysis", "explainability", "consensus-analysis"):
            self.assertNotIn(f'from(bucket: "{forbidden}")', text)

    def test_no_reproducibility_reference_in_other_dashboards(self) -> None:
        for path in (ROOT / "observability" / "grafana" / "dashboards").glob("*.json"):
            if path.name == DASHBOARD.name:
                continue
            text = path.read_text(encoding="utf-8")
            self.assertNotIn("reproducibility", text, path.name)
