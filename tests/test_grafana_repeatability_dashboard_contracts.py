"""Static contracts for the repeatability Grafana dashboard (step 8).

No real Grafana or InfluxDB: the tests inspect the dashboard JSON structure
and the Flux strings of each target — never the raw file as a whole.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from unittest import TestCase

ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_PATH = ROOT / "observability" / "grafana" / "dashboards" / "repeatability_influxdb.json"


class GrafanaRepeatabilityDashboardContractsTest(TestCase):
    def setUp(self) -> None:
        self.dashboard = json.loads(DASHBOARD_PATH.read_text(encoding="utf-8"))
        self.panels = self.dashboard["panels"]

    def _panel(self, title: str) -> dict:
        for panel in self.panels:
            if panel.get("title") == title:
                return panel
        raise AssertionError(f"panel not found: {title}")

    def _queries(self, panel: dict) -> list[str]:
        return [target["query"] for target in panel.get("targets", [])]

    # ------------------------------------------------------------------
    # 1. metadata, datasource, tags, implicit provisioning
    # ------------------------------------------------------------------

    def test_dashboard_metadata(self) -> None:
        self.assertEqual(self.dashboard["uid"], "repeatability-influxdb")
        self.assertEqual(self.dashboard["title"], "Repeatability")
        self.assertIn("repeatability", self.dashboard["tags"])
        self.assertIn("influxdb", self.dashboard["tags"])
        self.assertIn("observability", self.dashboard["tags"])
        self.assertEqual(self.dashboard["time"]["from"], "now-30m")
        self.assertEqual(self.dashboard["refresh"], "5m")
        for panel in self.panels:
            self.assertEqual(panel["datasource"]["type"], "influxdb")
            self.assertEqual(panel["datasource"]["uid"], "influxdb-dasma")
        # Implicit provisioning: the file lives under observability/grafana/dashboards/.
        self.assertTrue(DASHBOARD_PATH.exists())
        self.assertEqual(DASHBOARD_PATH.parent.name, "dashboards")

    # ------------------------------------------------------------------
    # 2. variables group / component
    # ------------------------------------------------------------------

    def test_variables_with_all_choice_for_group_and_source_component(self) -> None:
        variables = {variable["name"]: variable for variable in self.dashboard["templating"]["list"]}
        self.assertEqual(set(variables), {"group", "component"})

        group = variables["group"]
        self.assertEqual(group["label"], "Group")
        self.assertEqual(group["current"]["value"], "ALL")
        self.assertIn('bucket: "repeatability-v1"', group["query"])
        self.assertIn('tag: "Group"', group["query"])
        self.assertIn("schema.tagValues", group["query"])

        component = variables["component"]
        self.assertEqual(component["current"]["value"], "ALL")
        self.assertIn('from(bucket: "source-data")', component["query"])
        self.assertIn('distinct(column: "_field")', component["query"])
        self.assertIn('not contains(value: r._value, set: ban)', component["query"])
        # schema.fieldKeys is explicitly forbidden.
        self.assertNotIn("schema.fieldKeys", json.dumps(self.dashboard))

    # ------------------------------------------------------------------
    # 3. score pair reads the event-time diff join, with warmup markers
    # ------------------------------------------------------------------

    def test_global_score_pair_uses_event_time_diff_join(self) -> None:
        overlay = self._panel("Global anomaly score per event — v1 vs v2")
        self.assertEqual(overlay["type"], "timeseries")
        queries = self._queries(overlay)
        self.assertEqual(len(queries), 3)

        first, second, warmup = queries
        for query in queries:
            self.assertIn('from(bucket: "repeatability-diff")', query)
            self.assertIn('r._measurement == "repeatability-diff"', query)
            self.assertIn('"${group}" == "ALL"', query)
            self.assertNotIn("join(", query)

        self.assertIn('r._field == "score_a"', first)
        self.assertIn('r._field == "score_b"', second)
        self.assertIn('_field: "score v1"', first)
        self.assertIn('_field: "score v2"', second)
        for query in queries:
            self.assertIn('|> keep(columns: ["_time", "_value", "Group", "_field"])', query)
            self.assertIn('|> group(columns: ["Group", "_field"])', query)
        self.assertIn('r._field == "warming_up_a" or r._field == "warming_up_b"', warmup)
        self.assertIn('r._value == 1', warmup)
        self.assertIn('warming_up_.*', json.dumps(overlay))

    # ------------------------------------------------------------------
    # 4. diff / verdict / synthesis read repeatability-diff only
    # ------------------------------------------------------------------

    def _diff_panel_queries(self) -> list[str]:
        titles = (
            "Global anomaly score per event — v1 vs v2",
            "Global score difference per event (score_diff / abs_diff)",
            "Repeatability verdict (identical)",
            "Divergences (identical == 0)",
            "Incomplete events (complete_int == 0)",
            "% identical events",
            "Latest comparison events (emit_reason)",
        )
        queries = []
        for title in titles:
            for query in self._queries(self._panel(title)):
                self.assertIn('from(bucket: "repeatability-diff")', query, title)
                self.assertIn('r._measurement == "repeatability-diff"', query, title)
                self.assertIn('"${group}" == "ALL"', query, title)
                queries.append(query)
        return queries

    def test_diff_panels_use_expected_fields_without_boolean_or_join(self) -> None:
        joined = "\n".join(self._diff_panel_queries())
        for field in ("score_diff", "abs_diff", "identical", "complete_int"):
            self.assertIn(f'"{field}"', joined)
        # `complete` (boolean, dropped by Telegraf) must never be a Flux field.
        for query in self._diff_panel_queries():
            self.assertIsNone(re.search(r'r\._field == "complete"', query))
        self.assertNotIn("join(", joined)

    def test_diff_panel_title_and_fields(self) -> None:
        panel = self._panel("Global score difference per event (score_diff / abs_diff)")
        self.assertIn("difference", panel["title"].lower())
        query = self._queries(panel)[0]
        self.assertIn('r._field == "score_diff" or r._field == "abs_diff"', query)
        self.assertIn('|> keep(columns: ["_time", "_value", "Group", "_field"])', query)
        self.assertIn('|> group(columns: ["Group", "_field"])', query)

    def test_verdict_panel_uses_identical_only(self) -> None:
        query = self._queries(self._panel("Repeatability verdict (identical)"))[0]
        self.assertIn('r._field == "identical"', query)
        self.assertNotIn("score_diff", query)
        self.assertIsNone(re.search(r'r\._field == "complete"', query))

    # ------------------------------------------------------------------
    # 5. synthesis indicators, emit_reason, source panel
    # ------------------------------------------------------------------

    def test_synthesis_has_four_indicators(self) -> None:
        max_query = self._queries(self._panel("Max abs diff"))[0]
        self.assertIn('r._field == "abs_diff"', max_query)
        self.assertIn("|> max()", max_query)

        divergence_query = self._queries(self._panel("Divergences (identical == 0)"))[0]
        self.assertIn('r._field == "identical"', divergence_query)
        self.assertIn("r._value == 0", divergence_query)
        self.assertIn("|> count()", divergence_query)

        incomplete_query = self._queries(self._panel("Incomplete events (complete_int == 0)"))[0]
        self.assertIn('r._field == "complete_int"', incomplete_query)
        self.assertIn("r._value == 0", incomplete_query)
        self.assertIn("|> count()", incomplete_query)

        percent_query = self._queries(self._panel("% identical events"))[0]
        self.assertIn('r._field == "identical"', percent_query)
        self.assertIn("exists r._value", percent_query)
        self.assertIn("|> mean()", percent_query)
        self.assertIn("r._value * 100.0", percent_query)

    def test_emit_reason_table_and_source_panel(self) -> None:
        table_query = self._queries(self._panel("Latest comparison events (emit_reason)"))[0]
        for expected in ("emit_reason", "identical", "complete_int", "source_event_id", "Group"):
            self.assertIn(expected, table_query)
        self.assertIn('|> pivot(rowKey: ["_time", "Group", "source_event_id"]', table_query)
        self.assertIn("|> limit(", table_query)

        source_query = self._queries(self._panel("Source components by group"))[0]
        self.assertIn('from(bucket: "source-data")', source_query)
        self.assertIn('r._measurement == "source-data"', source_query)
        self.assertIn('["timestamp", "LabviewTime", "LabViewTime", "StepNumber", "Cycle"]', source_query)
        self.assertIn('"${group}" == "ALL"', source_query)
        self.assertIn('componentSel = if "${component}"', source_query)
        for model_field in ("hstree_score", "iforest_score", "mdragpoint_internal_r"):
            self.assertNotIn(model_field, source_query)

    # ------------------------------------------------------------------
    # 6. no existing dashboard modified
    # ------------------------------------------------------------------

    def test_other_dashboards_do_not_reference_repeatability(self) -> None:
        for path in DASHBOARD_PATH.parent.glob("*.json"):
            if path.name == DASHBOARD_PATH.name:
                continue
            content = path.read_text(encoding="utf-8")
            self.assertNotIn("repeatability-influxdb", content, path.name)
            self.assertNotIn("repeatability-v1", content, path.name)
