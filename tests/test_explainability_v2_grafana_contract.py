"""Grafana contract for the isolated explainability-v2 dashboard.

The v2 dashboard must expose explanation_status results, the normal/anomaly
split among published v2 results, SHAP computation time, and the event
identity Group + source_event_id — without ever reading the regular
explainability bucket or claiming exhaustive upstream coverage.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from unittest import TestCase


ROOT = Path(__file__).resolve().parents[1]
V2_DASHBOARD = ROOT / "observability" / "grafana" / "dashboards" / "Explainability" / "explainability_v2_influxdb.json"
REGULAR_DASHBOARD = ROOT / "observability" / "grafana" / "dashboards" / "Explainability" / "explainability_influxdb.json"
COMPARE_DASHBOARD = ROOT / "observability" / "grafana" / "dashboards" / "Explainability" / "explainability_compare_influxdb.json"

V2_BUCKET = "explainability-v2"
V2_MEASUREMENT = "explainability-v2"

STATUS_PANEL_ID = 4
VERDICT_PANEL_ID = 5
SHAP_TIME_PANEL_ID = 6

NEW_PANEL_TITLES = {
    "How many Explainability V2 HSTree results were produced per selected group?",
    "HSTree score fidelity by group",
    "HSTree SHAP computation time V2 (ms)",
    "How many Explainability V2 iForest results were produced per selected group?",
    "iForest score fidelity by group",
    "iForest SHAP computation time V2 (ms)",
}


def load_dashboard(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def panel_queries(panel: dict) -> list[str]:
    return [
        target["query"]
        for target in panel.get("targets", [])
        if "query" in target
    ]


def buckets_used(query: str) -> set[str]:
    return set(re.findall(r'from\(bucket:\s*"([^"]+)"\)', query))


def measurements_used(query: str) -> set[str]:
    return set(re.findall(r'r\._measurement\s*==\s*"([^"]+)"', query))


class ExplainabilityV2GrafanaIdentityTest(TestCase):
    def setUp(self) -> None:
        self.data = load_dashboard(V2_DASHBOARD)
        self.panels = {panel["id"]: panel for panel in self.data["panels"]}

    def test_dashboard_json_valid_with_v2_title_and_uid(self) -> None:
        self.assertEqual(self.data["title"], "Explainability V2 — HSTree")
        self.assertEqual(self.data["uid"], "explainability-v2-influxdb")

    def test_new_queries_read_only_v2_bucket_and_measurement(self) -> None:
        for panel_id in (STATUS_PANEL_ID, VERDICT_PANEL_ID, SHAP_TIME_PANEL_ID):
            for query in panel_queries(self.panels[panel_id]):
                used = buckets_used(query)
                self.assertEqual(used, {V2_BUCKET}, panel_id)
                self.assertEqual(measurements_used(query), {V2_MEASUREMENT}, panel_id)
                self.assertNotIn("source-data", query, panel_id)

    def test_every_explainability_query_includes_hstree_and_legacy_points(self) -> None:
        for panel in self.data["panels"]:
            for query in panel_queries(panel):
                occurrences = query.count('r._measurement == "explainability-v2"')
                self.assertEqual(
                    query.count('not exists r.explained_model or r.explained_model == "hstree"'),
                    occurrences,
                    panel["id"],
                )

    def test_status_panel_counts_published_statuses(self) -> None:
        panel = self.panels[STATUS_PANEL_ID]
        self.assertEqual(
            panel["title"],
            "How many Explainability V2 HSTree results were produced per selected group?",
        )
        query = "\n".join(panel_queries(panel))
        self.assertIn('r._field == "explanation_status"', query)
        self.assertIn('${group}', query)
        self.assertIn('group(columns: ["status"])', query)
        # No status is filtered out or rewritten to zero.
        self.assertNotIn('== "computed") then', query)
        # model_mismatch is a published status and must remain visible in this
        # all-status panel; only the verdict panel's computed count excludes it.
        self.assertNotIn('r._value == "computed"', query)
        self.assertNotIn("fill(column: \"_value\", value: 0)", query)

    def test_verdict_panel_pivots_event_identity(self) -> None:
        panel = self.panels[VERDICT_PANEL_ID]
        self.assertEqual(panel["title"], "HSTree score fidelity by group")
        query = "\n".join(panel_queries(panel))
        self.assertIn("hstree_score", query)
        self.assertIn("local_hstree_score", query)
        self.assertIn(
            'pivot(rowKey: ["_time", "Group", "source_event_id"]', query
        )
        self.assertIn("reduce(", query)
        self.assertIn("identical_scores", query)
        self.assertIn("different_scores", query)
        # schema.pivot and full joins are fragile Flux forms: the aggregation
        # must go through the built-in pivot() and a single reduce().
        self.assertNotIn("schema.pivot", query)
        self.assertNotIn("join(", query)
        self.assertNotIn('method: "full"', query)
        # schema.pivot must not sneak back into any other panel either.
        for other_id, other in self.panels.items():
            for other_query in panel_queries(other):
                self.assertNotIn("schema.pivot", other_query, other_id)

    def test_shap_time_panel_keeps_event_identity(self) -> None:
        panel = self.panels[SHAP_TIME_PANEL_ID]
        self.assertEqual(panel["title"], "HSTree SHAP computation time V2 (ms)")
        query = "\n".join(panel_queries(panel))
        self.assertIn('r._field == "shap_computation_time_ms"', query)
        self.assertIn('${group}', query)
        # Non-computed events are never converted to a zero duration.
        self.assertNotIn("fill(column", query)
        self.assertIn("aggregateWindow", query)
        display_name = panel["fieldConfig"]["defaults"].get("displayName", "")
        self.assertIn("source_event_id", display_name)

    def test_event_panels_keep_group_and_event_identity(self) -> None:
        for panel_id in (1, 2):
            panel = self.panels[panel_id]
            display_name = panel["fieldConfig"]["defaults"].get("displayName", "")
            queries = "\n".join(panel_queries(panel))
            self.assertIn("source_event_id", display_name + queries, panel_id)
            self.assertIn("${group}", queries, panel_id)
        # Score panel 1 is intentionally collapsed to one curve per field and
        # group so Grafana can render it; the feature panel remains event-level.
        score_query = "\n".join(panel_queries(self.panels[1]))
        self.assertIn('group(columns: ["_field", "Group"])', score_query)
        self.assertIn(
            'aggregateWindow(every: 1s, fn: last, createEmpty: false, timeSrc: "_time")',
            score_query,
        )
        for query in panel_queries(self.panels[2]) + panel_queries(self.panels[6]):
            self.assertNotIn("|> group()", query)

    def test_regular_and_compare_dashboards_unchanged(self) -> None:
        regular = load_dashboard(REGULAR_DASHBOARD)
        self.assertEqual(regular["title"], "Explainability V1 — HSTree")
        self.assertEqual(regular["uid"], "explainability-influxdb")
        for panel in regular["panels"]:
            for query in panel_queries(panel):
                self.assertNotIn(V2_BUCKET, buckets_used(query) | measurements_used(query))

        compare = load_dashboard(COMPARE_DASHBOARD)
        self.assertEqual(compare["title"], "Explainability v1 vs v2")
        self.assertEqual(compare["uid"], "explainability-compare-influxdb")
        compare_titles = {panel["title"] for panel in compare["panels"]}
        self.assertFalse(NEW_PANEL_TITLES & compare_titles)

    def test_panel_ids_unique_and_grid_positions_do_not_overlap(self) -> None:
        panel_ids = [panel["id"] for panel in self.data["panels"]]
        self.assertEqual(len(panel_ids), len(set(panel_ids)))

        rectangles = []
        for panel in self.data["panels"]:
            grid = panel["gridPos"]
            rectangles.append(
                (
                    panel["id"],
                    grid["x"],
                    grid["y"],
                    grid["x"] + grid["w"],
                    grid["y"] + grid["h"],
                )
            )
        for index, (id_a, ax0, ay0, ax1, ay1) in enumerate(rectangles):
            for id_b, bx0, by0, bx1, by1 in rectangles[index + 1 :]:
                overlaps = ax0 < bx1 and bx0 < ax1 and ay0 < by1 and by0 < ay1
                self.assertFalse(overlaps, f"panels {id_a} and {id_b} overlap")


class ExplainabilityV2GrafanaTypeCompatibilityTest(TestCase):
    """OC-12R: panels must stay compatible with the real field types.

    With the json_v2 Telegraf parser, `explanation_status` is stored as a
    string field and `hstree_anomaly` as a bool field. The verdict panel
    unions the two fields through a pivot: the bool stream must be cast to
    string before the union, exactly as the queries already do. The nested
    `shap_values` map is stored as bare `<feature>` fields (empirical json_v2
    1.30.3 behavior), so the SHAP contribution panel no longer filters on a
    `shap_values_` prefix.
    """

    def setUp(self) -> None:
        self.data = load_dashboard(V2_DASHBOARD)
        self.panels = {panel["id"]: panel for panel in self.data["panels"]}

    def test_status_panel_uses_the_string_field_directly(self) -> None:
        query = "\n".join(panel_queries(self.panels[STATUS_PANEL_ID]))
        self.assertIn('r._field == "explanation_status"', query)
        self.assertNotIn("string(v:", query)  # no cast needed for a string field

    def test_fidelity_panel_compares_upstream_and_local_scores(self) -> None:
        query = "\n".join(panel_queries(self.panels[VERDICT_PANEL_ID]))
        self.assertIn('r._field == "hstree_score"', query)
        self.assertIn('r._field == "local_hstree_score"', query)
        self.assertIn('pivot(rowKey: ["_time", "Group", "source_event_id"]', query)
        self.assertIn("identical_scores", query)
        self.assertIn("different_scores", query)

    def test_shap_panel_matches_bare_feature_fields(self) -> None:
        query = "\n".join(panel_queries(self.panels[2]))
        self.assertNotIn("shap_values_", query)
        self.assertIn('r._field == fieldName', query)
        self.assertIn('${feature}', query)
        # No rename transformation: the field name IS the feature name.
        self.assertNotIn("renameByRegex", json.dumps(self.panels[2]))


class ExplainabilityV2GroupKeyCollapseTest(TestCase):
    """Panels aggregating by a label column must collapse the group key.

    Grafana's influxdb-flux parser turns group-key columns into frame
    LABELS, not fields: without a final ``group()`` the status barchart has
    no string x field ("Bar chart requires a string or time field") and the
    verdict table loses its verdict column. Both behaviors were validated
    through the Grafana /api/ds/query API.
    """

    def setUp(self) -> None:
        self.data = load_dashboard(V2_DASHBOARD)
        self.panels = {panel["id"]: panel for panel in self.data["panels"]}

    def test_status_bar_chart_collapses_the_group_key(self) -> None:
        query = "\n".join(panel_queries(self.panels[4]))
        self.assertIn('group(columns: ["status", "Group"])', query)
        self.assertIn("|> count()", query)
        # The group key must be collapsed AFTER the count: the frame must
        # keep status as a regular string field for the barchart x field.
        self.assertLess(
            query.index('group(columns: ["status", "Group"])'),
            query.index("|> count()"),
        )
        self.assertLess(
            query.index("|> count()"),
            query.index("|> group()"),
        )
        # warmup precedes computed in time: the descending status sort must
        # come after the group-key collapse so the barchart keeps that order.
        self.assertIn('sort(columns: ["status"], desc: true)', query)
        self.assertLess(
            query.index("|> group()"),
            query.index('sort(columns: ["status"], desc: true)'),
        )

    def test_verdict_table_collapses_the_group_key(self) -> None:
        query = "\n".join(panel_queries(self.panels[5]))
        self.assertLess(
            query.index("|> reduce("),
            query.index("|> group()"),
        )
        # The group and aggregate score columns must remain selectable.
        self.assertIn('keep(columns: ["Group", "compared_scores", "identical_scores", "different_scores", "identical_rate"])', query)
