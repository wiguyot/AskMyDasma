"""Pure tests for the OC-14 capacity report and fixture coherence.

No Docker, Kafka or InfluxDB: only the pure capacity_report functions and
the fixture/manifest files.
"""

import copy
import json
import random
import unittest
from pathlib import Path

from experiments.explainability.v2.capacity_report import (
    DECISION_WITHOUT_SLO,
    ReportError,
    aggregate_budgets,
    analyze_lag_samples,
    apply_slo,
    assumed_input_consequences,
    delivery_throughput,
    percentile,
    render_markdown,
    summarize_shap_times,
    theoretical_capacity_per_group,
    validate_campaign,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = PROJECT_ROOT / "tests" / "fixtures" / "explainability_v2" / "oc14_capacity_manifest.json"
CSV_PATH = PROJECT_ROOT / "tests" / "fixtures" / "explainability_v2" / "oc14_capacity_source.csv"


def make_manifest():
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def make_campaign(budget, valid=True):
    """A valid synthetic campaign for one budget."""
    campaign = {
        "budget": budget,
        "run_id": f"oc14-20260918-140000-0000{budget:04d}",
        "totals": {"expected": 124, "hstree": 124, "v2": 124, "v2_json": 124},
        "status_distribution": {"warmup": 99, "computed": 25, "failed": 0},
        "shap": {"eligible": 25, "computed": 25, "failed": 0},
        "duplicates": {"source_event_id": []},
        "missing_keys_count": 0,
        "producer": {
            "started_at": 0.0,
            "exit_at": 6.5,
            "duration_seconds": 6.5,
        },
        "completion_time": 40.0,
        "total_duration_seconds": 40.0,
        "lag_analysis": {
            "max_lag": 12,
            "time_to_max_lag_seconds": 8.0,
            "lag_area_event_seconds": 220.5,
            "final_lag": 0,
            "drain_seconds": 33.5,
        },
        "delivery_throughput_events_per_second": 25 / 33.5,
        "shap_time_stats": {
            "count": 25, "min": 10.0, "p50": 20.0, "p95": 40.0,
            "max": 45.0, "mean": 22.0,
        },
    }
    if not valid:
        campaign["status_distribution"]["failed"] = 2
        campaign["status_distribution"]["computed"] = 23
        campaign["totals"]["v2_json"] = 122
    return campaign


def make_samples():
    return [
        {"t": 1.0, "lag": 4},
        {"t": 2.0, "lag": 8},
        {"t": 3.0, "lag": 8},
        {"t": 4.0, "lag": 0},
    ]


class NumericHelperTest(unittest.TestCase):
    def test_percentile_and_mean_are_deterministic(self) -> None:
        values = [30.0, 10.0, 20.0, 40.0]
        stats = summarize_shap_times(values)
        shuffled = values[:]
        random.Random(5).shuffle(shuffled)
        self.assertEqual(stats, summarize_shap_times(shuffled))
        self.assertEqual(stats["min"], 10.0)
        self.assertEqual(stats["max"], 40.0)
        self.assertEqual(stats["mean"], 25.0)
        self.assertEqual(stats["p50"], 25.0)
        self.assertEqual(percentile([10.0, 20.0], 0.95), 19.5)

    def test_theoretical_capacity(self) -> None:
        self.assertAlmostEqual(theoretical_capacity_per_group(0.5), 2.0)
        self.assertIsNone(theoretical_capacity_per_group(None))
        self.assertIsNone(theoretical_capacity_per_group(0.0))

    def test_delivery_throughput(self) -> None:
        self.assertAlmostEqual(delivery_throughput(25, 10.0), 2.5)
        self.assertIsNone(delivery_throughput(25, None))
        self.assertIsNone(delivery_throughput(25, 0.0))


class LagAnalysisTest(unittest.TestCase):
    def test_max_lag_area_and_drain(self) -> None:
        analysis = analyze_lag_samples(
            make_samples(), producer_exit_time=6.5, completion_time=40.0
        )
        self.assertEqual(analysis["max_lag"], 8)
        self.assertEqual(analysis["time_to_max_lag_seconds"], 2.0)
        # Trapezoid over (1,4),(2,8),(3,8),(4,0): 6 + 8 + 4 = 18
        self.assertEqual(analysis["lag_area_event_seconds"], 18.0)
        self.assertEqual(analysis["final_lag"], 0)
        self.assertEqual(analysis["drain_seconds"], 33.5)

    def test_samples_out_of_order_give_the_same_result(self) -> None:
        samples = make_samples()
        shuffled = samples[:]
        random.Random(9).shuffle(shuffled)
        first = analyze_lag_samples(samples, 6.5, 40.0)
        second = analyze_lag_samples(shuffled, 6.5, 40.0)
        self.assertEqual(first, second)

    def test_none_lag_samples_are_tolerated(self) -> None:
        samples = [{"t": 0.5, "lag": None}, {"t": 1.5, "lag": 6}, {"t": 2.5, "lag": 0}]
        analysis = analyze_lag_samples(samples, 3.0, 5.0)
        self.assertEqual(analysis["max_lag"], 6)
        self.assertEqual(analysis["final_lag"], 0)

    def test_nonzero_final_lag_is_reported(self) -> None:
        samples = [{"t": 1.0, "lag": 5}, {"t": 2.0, "lag": 3}]
        analysis = analyze_lag_samples(samples, 1.5, None)
        self.assertEqual(analysis["final_lag"], 3)
        self.assertIsNone(analysis["drain_seconds"])


class CampaignValidityTest(unittest.TestCase):
    def test_valid_campaign_has_no_reasons(self) -> None:
        manifest = make_manifest()
        reasons = validate_campaign(make_campaign(100), manifest)
        self.assertEqual(reasons, [])

    def test_failed_status_invalidates(self) -> None:
        manifest = make_manifest()
        campaign = make_campaign(100, valid=False)
        reasons = validate_campaign(campaign, manifest)
        self.assertTrue(any("failed_is_2" in reason for reason in reasons))

    def test_model_mismatch_status_invalidates(self) -> None:
        manifest = make_manifest()
        campaign = make_campaign(100)
        campaign["status_distribution"]["model_mismatch"] = 1
        campaign["status_distribution"]["computed"] = 24
        reasons = validate_campaign(campaign, manifest)
        self.assertTrue(any("model_mismatch_is_1" in reason for reason in reasons))

    def test_missing_event_invalidates(self) -> None:
        manifest = make_manifest()
        campaign = make_campaign(100)
        campaign["totals"]["v2_json"] = 123
        campaign["missing_keys_count"] = 1
        reasons = validate_campaign(campaign, manifest)
        self.assertTrue(any("missing_events_1" in reason for reason in reasons))

    def test_unexpected_status_invalidates(self) -> None:
        manifest = make_manifest()
        campaign = make_campaign(100)
        campaign["status_distribution"]["surprise"] = 1
        reasons = validate_campaign(campaign, manifest)
        self.assertTrue(any("unexpected_statuses" in reason for reason in reasons))

    def test_nonzero_final_lag_invalidates(self) -> None:
        manifest = make_manifest()
        campaign = make_campaign(100)
        campaign["lag_analysis"]["final_lag"] = 7
        reasons = validate_campaign(campaign, manifest)
        self.assertTrue(any("final_lag_is_7" in reason for reason in reasons))

    def test_duplicate_invalidates(self) -> None:
        manifest = make_manifest()
        campaign = make_campaign(100)
        campaign["duplicates"]["source_event_id"] = ["oc14-evt-101"]
        reasons = validate_campaign(campaign, manifest)
        self.assertTrue(any("duplicate_keys" in reason for reason in reasons))


class AggregationTest(unittest.TestCase):
    def test_four_budgets_aggregate(self) -> None:
        manifest = make_manifest()
        campaigns = [make_campaign(budget) for budget in (100, 1000, 1500, 2000)]
        aggregate = aggregate_budgets(manifest, campaigns)
        self.assertEqual([row["budget"] for row in aggregate["budgets"]], [100, 1000, 1500, 2000])
        self.assertTrue(all(row["valid"] for row in aggregate["budgets"]))
        self.assertEqual(aggregate["manifest"]["fixture_version"], manifest["fixture_version"])

    def test_missing_budget_campaign_is_rejected(self) -> None:
        manifest = make_manifest()
        campaigns = [make_campaign(budget) for budget in (100, 1000, 1500)]
        with self.assertRaises(ReportError):
            aggregate_budgets(manifest, campaigns)

    def test_aggregate_json_is_identical_when_campaign_order_is_reversed(self) -> None:
        manifest = make_manifest()
        campaigns = [make_campaign(budget) for budget in (100, 1000, 1500, 2000)]
        first = aggregate_budgets(manifest, campaigns)
        reversed_campaigns = list(reversed(copy.deepcopy(campaigns)))
        second = aggregate_budgets(manifest, reversed_campaigns)
        self.assertEqual(json.dumps(first, sort_keys=True), json.dumps(second, sort_keys=True))

    def test_offline_benchmark_is_joined_by_budget(self) -> None:
        manifest = make_manifest()
        campaigns = [make_campaign(budget) for budget in (100, 1000, 1500, 2000)]
        offline = {
            "budgets": [
                {"budget": 100, "mean_ms": 1711.4, "p95_ms": 2186.7, "capacity_per_group": 0.584}
            ]
        }
        aggregate = aggregate_budgets(manifest, campaigns, offline)
        row100 = aggregate["budgets"][0]
        self.assertEqual(row100["offline_mean_ms"], 1711.4)
        self.assertIsNone(aggregate["budgets"][1].get("offline_mean_ms"))


class SloTest(unittest.TestCase):
    def make_aggregate(self):
        manifest = make_manifest()
        campaigns = [make_campaign(budget) for budget in (100, 1000, 1500, 2000)]
        return aggregate_budgets(manifest, campaigns)

    def test_compliant_and_non_compliant_budgets(self) -> None:
        aggregate = self.make_aggregate()
        # Target below every observed throughput and a generous drain: all pass.
        slo = apply_slo(aggregate, target_rate=0.1, max_drain_seconds=600.0)
        self.assertTrue(all(item["compliant"] for item in slo["budgets"]))
        self.assertEqual(slo["max_compatible_budget"], 2000)
        self.assertIn("non requise", slo["decision"])

    def test_impossible_target_marks_everything_non_compliant(self) -> None:
        aggregate = self.make_aggregate()
        slo = apply_slo(aggregate, target_rate=1000.0, max_drain_seconds=1.0)
        self.assertFalse(any(item["compliant"] for item in slo["budgets"]))
        self.assertIsNone(slo["max_compatible_budget"])
        self.assertIn("optimisation requise", slo["decision"])

    def test_invalid_budget_is_never_compliant(self) -> None:
        aggregate = self.make_aggregate()
        aggregate["budgets"][2]["valid"] = False
        slo = apply_slo(aggregate, target_rate=0.1, max_drain_seconds=600.0)
        budget_1500 = [item for item in slo["budgets"] if item["budget"] == 1500][0]
        self.assertFalse(budget_1500["compliant"])

    def test_absent_slo_means_no_decision(self) -> None:
        aggregate = self.make_aggregate()
        markdown = render_markdown(aggregate, slo=None, consequences=None)
        self.assertIn(DECISION_WITHOUT_SLO, markdown)
        for option in (
            "conserver le différé actuel",
            "background/modèle figé avec cache",
            "snapshots",
            "HSTree",
        ):
            self.assertIn(option, markdown)

    def test_slo_margins_are_computed(self) -> None:
        aggregate = self.make_aggregate()
        slo = apply_slo(aggregate, target_rate=0.5, max_drain_seconds=60.0)
        first = slo["budgets"][0]
        self.assertAlmostEqual(
            first["throughput_margin_events_per_second"],
            round(aggregate["budgets"][0]["delivered_throughput_events_per_second"] - 0.5, 6),
        )


class ConsequenceTest(unittest.TestCase):
    def test_assumed_rates_are_pure_arithmetic(self) -> None:
        manifest = make_manifest()
        campaigns = [make_campaign(budget) for budget in (100, 1000, 1500, 2000)]
        aggregate = aggregate_budgets(manifest, campaigns)
        consequences = assumed_input_consequences(aggregate, [0.2, 1.0])
        self.assertEqual(
            [entry["assumed_rate_events_per_second_per_group"] for entry in consequences],
            [0.2, 1.0],
        )
        # budget 100 mean 22 ms -> capacity ~45 ev/s: 1 ev/s is sustainable.
        sustainable = consequences[1]["budgets"][0]["sustainable"]
        self.assertTrue(sustainable)

    def test_invalid_rate_is_rejected(self) -> None:
        with self.assertRaises(ReportError):
            assumed_input_consequences({"budgets": []}, [0.0])


class MarkdownAndDeterminismTest(unittest.TestCase):
    def test_markdown_table_and_sections(self) -> None:
        manifest = make_manifest()
        campaigns = [make_campaign(budget) for budget in (100, 1000, 1500, 2000)]
        aggregate = aggregate_budgets(manifest, campaigns)
        markdown = render_markdown(aggregate)
        self.assertIn("| nsamples | SHAP moyen/p95 (ms)", markdown)
        self.assertIn("| 100 |", markdown)
        self.assertIn("Coût arithmétique hors ligne", markdown)
        self.assertIn(DECISION_WITHOUT_SLO, markdown)

    def test_render_markdown_is_deterministic(self) -> None:
        manifest = make_manifest()
        campaigns = [make_campaign(budget) for budget in (100, 1000, 1500, 2000)]
        aggregate = aggregate_budgets(manifest, campaigns)
        self.assertEqual(render_markdown(aggregate), render_markdown(aggregate))


class FixtureCoherenceTest(unittest.TestCase):
    """OC-14 A: fixture/manifest coherence."""

    @classmethod
    def setUpClass(cls) -> None:
        import hashlib

        cls.manifest = make_manifest()
        lines = CSV_PATH.read_text(encoding="utf-8").strip().splitlines()
        cls.header = lines[0].split(",")
        cls.rows = [line.split(",") for line in lines[1:]]
        cls.sha = cls.manifest["source_csv_sha256"]

    def test_csv_sha256_matches_the_manifest(self) -> None:
        import hashlib

        self.assertEqual(
            hashlib.sha256(CSV_PATH.read_bytes()).hexdigest(), self.sha
        )

    def test_total_rows_match_the_manifest(self) -> None:
        self.assertEqual(len(self.rows), self.manifest["expected_total_events"])

    def test_feature_count_matches_the_manifest(self) -> None:
        features = [name for name in self.manifest_header() if name.startswith("feat_")]
        self.assertEqual(len(features), self.manifest["feature_count"])
        self.assertEqual(len(features), 81)

    def manifest_header(self):
        lines = CSV_PATH.read_text(encoding="utf-8").splitlines()
        return lines[0].split(",")

    def test_unique_timestamps_and_ids(self) -> None:
        header = self.manifest_header()
        ts_index = header.index("timestamp")
        id_index = header.index("source_event_id")
        timestamps = [row[ts_index] for row in self.rows]
        ids = [row[id_index] for row in self.rows]
        self.assertEqual(len(set(timestamps)), len(timestamps))
        self.assertEqual(len(set(ids)), len(ids))

    def test_single_group(self) -> None:
        header = self.manifest_header()
        group_index = header.index("Group")
        groups = {row[group_index] for row in self.rows}
        self.assertEqual(groups, {self.manifest["group"]})
        self.assertEqual(self.manifest["group_count"], 1)

    def test_budgets_and_params_declared(self) -> None:
        self.assertEqual(self.manifest["expected_budgets"], [100, 1000, 1500, 2000])
        env = self.manifest["campaign_env"]
        self.assertEqual(env["HSTREE_INITIAL_WINDOW_SIZE"], "100")
        self.assertEqual(env["SHAP_BACKGROUND_SIZE"], "100")
        self.assertEqual(env["HSTREE_WINDOW_SIZE"], "2048")
        self.assertEqual(env["HSTREE_NUM_TREES"], "50")
        self.assertEqual(env["HSTREE_MAX_DEPTH"], "15")
        self.assertEqual(env["EXPLAINABILITY_SEED"], env["ANALYSE_SEED"])
        self.assertEqual(self.manifest["producer_emission"]["time_delay_seconds"], 0.05)

    def test_warmup_and_shap_expectations(self) -> None:
        self.assertEqual(self.manifest["expected_warmup_events"], 99)
        self.assertEqual(self.manifest["expected_shap_events"], 25)
        self.assertGreaterEqual(self.manifest["expected_shap_events"], 20)

    def test_values_are_valid_floats(self) -> None:
        header = self.manifest_header()
        feature_indexes = [
            index for index, name in enumerate(header) if name.startswith("feat_")
        ]
        for row in self.rows[:5] + self.rows[-5:]:
            for index in feature_indexes:
                float(row[index])  # must not raise


if __name__ == "__main__":
    unittest.main()
