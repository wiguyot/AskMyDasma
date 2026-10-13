"""OC-13 contract tests: fixture coherence, compose isolation, runner safety.

Pure tests: no Docker, Kafka or InfluxDB. The runner module is imported for
its pure functions only; its `__main__` guard is verified separately.
"""

import datetime as dt
import importlib.util
import json
import re
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = PROJECT_ROOT / "tests" / "fixtures" / "explainability_v2"
OC13_CSV = FIXTURE_DIR / "oc13_source.csv"
OC13_MANIFEST = FIXTURE_DIR / "oc13_manifest.json"
OC13_COMPOSE = PROJECT_ROOT / "docker/compose/experiments/docker-compose.oc13.yml"
RUNNER = PROJECT_ROOT / "scripts" / "integration" / "run-explainability-regular-v2-comparison.py"
RUNNER_SH = PROJECT_ROOT / "scripts" / "integration" / "run-explainability-regular-v2-comparison.sh"
COMPARATOR = PROJECT_ROOT / "experiments" / "explainability" / "v2" / "compare_regular_v2.py"


def load_csv_rows() -> list:
    lines = OC13_CSV.read_text(encoding="utf-8").strip().splitlines()
    header = lines[0].split(",")
    rows = []
    for line in lines[1:]:
        cells = line.split(",")
        rows.append(dict(zip(header, cells)))
    return rows


def parse_compose_service_env(compose_text: str, service: str) -> dict:
    """Extract the environment mapping of one service from the OC-13 compose
    file (dedicated mini-parser: no yaml dependency in the test suite)."""
    return _service_block(compose_text, service)["env"]


def _service_block(compose_text: str, service: str) -> dict:
    """Return {'text': block, 'env': env dict} for one 2-space-indented service.

    A service block ends at the next line indented by exactly two spaces
    followed by a letter (the next top-level service or document end)."""
    after = compose_text.split(f"\n  {service}:", 1)[1]
    block = re.split(r"\n  (?=[a-zA-Z])", after, maxsplit=1)[0]
    env = {}
    if "environment:" in block:
        env_block = block.split("environment:", 1)[1]
        for line in env_block.splitlines():
            match = re.match(r'^\s{6}([A-Z0-9_]+):\s*"?(.*?)"?\s*$', line)
            if match:
                env[match.group(1)] = match.group(2)
    return {"text": block, "env": env}


class OC13FixtureCoherenceTest(unittest.TestCase):
    """OC-13 A: fixture and manifest coherence."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.rows = load_csv_rows()
        cls.manifest = json.loads(OC13_MANIFEST.read_text(encoding="utf-8"))

    def test_csv_total_matches_manifest(self) -> None:
        self.assertEqual(len(self.rows), self.manifest["expected_total_events"])

    def test_source_ids_are_unique(self) -> None:
        ids = [row["source_event_id"] for row in self.rows]
        self.assertEqual(len(set(ids)), len(ids))

    def test_group_timestamp_couples_are_unique(self) -> None:
        keys = [f"{row['Group']}|{row['timestamp']}" for row in self.rows]
        self.assertEqual(len(set(keys)), len(keys))

    def test_manifest_keys_match_the_csv_exactly(self) -> None:
        keys = [f"{row['Group']}|{row['timestamp']}" for row in self.rows]
        self.assertEqual(self.manifest["expected_keys"], keys)

    def test_warmup_keys_match_the_first_events(self) -> None:
        keys = self.manifest["expected_keys"]
        self.assertEqual(self.manifest["expected_warmup_count"], 99)
        self.assertEqual(self.manifest["warmup_keys"], keys[:99])

    def test_manifest_declares_strict_common_parameters(self) -> None:
        env = self.manifest["campaign_env"]
        self.assertEqual(env["HSTREE_INITIAL_WINDOW_SIZE"], "100")
        self.assertEqual(env["HSTREE_WINDOW_SIZE"], "2048")
        self.assertEqual(env["HSTREE_NUM_TREES"], "50")
        self.assertEqual(env["HSTREE_MAX_DEPTH"], "15")
        self.assertEqual(env["SHAP_BACKGROUND_SIZE"], "100")
        self.assertEqual(env["SHAP_NSAMPLES"], "100")
        # Identical HSTree and explainability seeds.
        self.assertEqual(env["ANALYSE_SEED"], env["EXPLAINABILITY_SEED"])
        for thread_var in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
            self.assertEqual(env[thread_var], "1")

    def test_manifest_declares_explicit_numeric_tolerances(self) -> None:
        tolerances = self.manifest["numeric_tolerances"]
        self.assertEqual(tolerances["absolute_tolerance"], 1e-12)
        self.assertEqual(tolerances["relative_tolerance"], 1e-12)

    def test_fixture_has_normals_and_outliers(self) -> None:
        values = {(float(row["feat_a"]), float(row["feat_b"])) for row in self.rows}
        self.assertIn((10.0, 20.0), values)  # normals
        outliers = {value for value in values if value != (10.0, 20.0)}
        self.assertTrue(outliers, "fixture must contain outliers")

    def test_single_group_without_simultaneous_pair(self) -> None:
        groups = {row["Group"] for row in self.rows}
        self.assertEqual(groups, {self.manifest["group"]})
        timestamps = [row["timestamp"] for row in self.rows]
        self.assertEqual(len(set(timestamps)), len(timestamps))


class OC13ComposeContractTest(unittest.TestCase):
    """OC-13 C: isolated compose with only dedicated services."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.text = OC13_COMPOSE.read_text(encoding="utf-8")

    def test_only_oc13_services_are_defined(self) -> None:
        self.assertIn("  oc13-producer:", self.text)
        self.assertIn("  oc13-hstree:", self.text)
        self.assertIn("  oc13-regular:", self.text)
        self.assertIn("  oc13-v2:", self.text)
        self.assertNotIn("  oc13-telegraf", self.text)
        self.assertNotIn("  oc12-", self.text)

    def test_no_observability_stack_in_the_comparison(self) -> None:
        self.assertNotIn("telegraf:", self.text)
        self.assertNotIn("grafana:", self.text)
        self.assertNotIn("influxdb:", self.text)

    def test_every_container_and_topic_carries_the_run_id(self) -> None:
        for service in ("oc13-producer", "oc13-hstree", "oc13-regular", "oc13-v2"):
            block = _service_block(self.text, service)["text"]
            self.assertIn(f"container_name: {service}-" + "${OC13_RUN_ID}", block)
        self.assertEqual(self.text.count("${OC13_RUN_ID}") >= 12, True)
        for topic_template in (
            "oc13-source-${OC13_RUN_ID}",
            "oc13-hstree-${OC13_RUN_ID}",
            "oc13-hstree-viz-${OC13_RUN_ID}",
            "oc13-regular-${OC13_RUN_ID}",
            "oc13-regular-viz-${OC13_RUN_ID}",
            "oc13-v2-${OC13_RUN_ID}",
            "oc13-v2-viz-${OC13_RUN_ID}",
        ):
            self.assertIn(topic_template, self.text)

    def test_both_explainability_services_consume_the_same_dedicated_hstree_topic(self) -> None:
        for service in ("oc13-regular", "oc13-v2"):
            block = _service_block(self.text, service)["text"]
            self.assertIn('KAFKA_SOURCE_TOPIC: "oc13-hstree-${OC13_RUN_ID}"', block)

    def test_regular_and_v2_have_identical_strict_parameters(self) -> None:
        regular = parse_compose_service_env(self.text, "oc13-regular")
        v2 = parse_compose_service_env(self.text, "oc13-v2")
        shared = [
            "HSTREE_INITIAL_WINDOW_SIZE",
            "HSTREE_WINDOW_SIZE",
            "HSTREE_NUM_TREES",
            "HSTREE_MAX_DEPTH",
            "HSTREE_EXCLUDE_FIELDS",
            "SHAP_BACKGROUND_SIZE",
            "SHAP_NSAMPLES",
            "SHAP_ENABLED",
            "EXPLAINABILITY_SEED",
            "OMP_NUM_THREADS",
            "OPENBLAS_NUM_THREADS",
            "MKL_NUM_THREADS",
            "NUMEXPR_NUM_THREADS",
        ]
        for key in shared:
            self.assertIn(key, regular, f"missing {key} in oc13-regular")
            self.assertIn(key, v2, f"missing {key} in oc13-v2")
            self.assertEqual(regular[key], v2[key], f"{key} differs between regular and v2")
        self.assertEqual(regular["SHAP_NSAMPLES"], "100")
        self.assertEqual(v2["SHAP_NSAMPLES"], "100")

    def test_dedicated_consumer_groups(self) -> None:
        regular = parse_compose_service_env(self.text, "oc13-regular")
        v2 = parse_compose_service_env(self.text, "oc13-v2")
        hstree = parse_compose_service_env(self.text, "oc13-hstree")
        self.assertEqual(regular["EXPLAINABILITY_GROUP_ID"], "oc13-regular-consumer-${OC13_RUN_ID}")
        self.assertEqual(v2["EXPLAINABILITY_GROUP_ID"], "oc13-v2-consumer-${OC13_RUN_ID}")
        self.assertEqual(hstree["ANALYSE_HSTREE_GROUP_ID"], "oc13-hstree-consumer-${OC13_RUN_ID}")

    def test_standard_services_are_not_redefined_or_stopped(self) -> None:
        for forbidden in ("  producer:", "  hstree-analysis:", "  explainability:", "  explainability-v2:"):
            self.assertNotIn(forbidden, self.text)
        self.assertNotIn("down", self.text)
        self.assertNotIn("--rm", self.text)

    def test_manifest_parameters_match_the_compose_values(self) -> None:
        manifest = json.loads(OC13_MANIFEST.read_text(encoding="utf-8"))
        for service in ("oc13-regular", "oc13-v2"):
            env = parse_compose_service_env(self.text, service)
            for key, value in manifest["campaign_env"].items():
                if key in ("ANALYSE_SEED",):
                    continue  # hstree-service seed, checked below
                self.assertEqual(env.get(key), value, f"{service}:{key}")
        hstree_env = parse_compose_service_env(self.text, "oc13-hstree")
        self.assertEqual(hstree_env["ANALYSE_SEED"], manifest["campaign_env"]["ANALYSE_SEED"])


class OC13RunnerContractTest(unittest.TestCase):
    """OC-13 D: OC-12R.1 safety guarantees in the runner source."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.runner = RUNNER.read_text(encoding="utf-8")

    def test_finally_block_with_started_tracking(self) -> None:
        main_body = self.runner.split("def main(")[1].split("if __name__")[0]
        self.assertIn("oc13_started = False", main_body)
        self.assertIn("oc13_started = True", main_body)
        self.assertIn("finally:", main_body)

    def test_cleanup_targets_only_oc13_services(self) -> None:
        helper = self.runner.split("def stop_oc13_services")[1].split("def ")[0]
        self.assertIn('compose("stop", *OC13_SERVICES', helper)
        self.assertIn('compose("rm", "-f", *OC13_SERVICES', helper)
        self.assertNotIn("STANDARD_COMPUTE_SERVICES", helper)

    def test_failure_flow_logs_diagnostics_before_cleanup(self) -> None:
        finally_block = self.runner.split("def main(")[1].split("if __name__")[0].split("finally:", 1)[1]
        self.assertLess(
            finally_block.index("dump_service_logs"),
            finally_block.index("stop_oc13_services"),
        )

    def test_keep_up_neutralizes_cleanup(self) -> None:
        finally_block = self.runner.split("def main(")[1].split("if __name__")[0].split("finally:", 1)[1]
        self.assertIn("oc13_started and not args.keep_up", finally_block)

    def test_runner_refuses_running_standard_services(self) -> None:
        self.assertIn("assert_standard_services_stopped", self.runner)
        self.assertIn('"explainability"', self.runner)
        self.assertIn('"explainability-v2"', self.runner)
        self.assertIn("the OC-13 runner never stops shared services", self.runner)

    def test_global_timeout_is_bounded(self) -> None:
        self.assertIn("--global-timeout", self.runner)
        self.assertIn("global timeout of", self.runner)

    def test_waits_in_the_required_order(self) -> None:
        main_body = self.runner.split("def main(")[1].split("if __name__")[0]
        positions = [
            main_body.index('log("waiting for the oc13 producer to finish")'),
            main_body.index('log(f"waiting for {expected} events in {topic}")'),
            main_body.index('log(f"waiting for zero lags'),
            main_body.index('log("exporting the regular and v2 JSON topics")'),
            main_body.index('log("running the OC-13 pure comparator on the raw exports")'),
        ]
        self.assertEqual(positions, sorted(positions))

    def test_waits_cover_all_three_viz_topics_and_both_lags(self) -> None:
        main_body = self.runner.split("def main(")[1].split("if __name__")[0]
        self.assertIn('("hstree", hstree_viz_topic)', main_body)
        self.assertIn('("regular", regular_viz_topic)', main_body)
        self.assertIn('("v2", v2_viz_topic)', main_body)
        self.assertIn("regular_lag == 0 and v2_lag == 0", main_body)

    def test_comparator_is_invoked_with_all_inputs(self) -> None:
        run_block = self.runner.split("def run_comparator")[1].split("def ")[0]
        self.assertIn('"--manifest", str(MANIFEST)', run_block)
        self.assertIn('"--regular-export"', run_block)
        self.assertIn('"--v2-export"', run_block)
        self.assertIn('"--runner-metrics"', run_block)
        self.assertIn('"--output", str(artifacts / "oc13-comparison-report.json")', run_block)

    def test_comparator_failure_fails_the_campaign(self) -> None:
        run_block = self.runner.split("def run_comparator")[1].split("def ")[0]
        self.assertIn("returncode != 0", run_block)
        self.assertIn('not report.get("passed")', run_block)

    def test_minimum_artifacts_are_written(self) -> None:
        self.assertIn("oc13-regular-viz.jsonl", self.runner)
        self.assertIn("oc13-v2-viz.jsonl", self.runner)
        self.assertIn("lag-metrics.json", self.runner)
        self.assertIn("runner-metrics.json", self.runner)
        self.assertIn("campaign-env.json", self.runner)
        self.assertIn("services-failure-logs.txt", self.runner)
        self.assertIn("oc13-comparison-report.json", self.runner)

    def test_lag_helper_requires_topic_and_lag_columns(self) -> None:
        helper = self.runner.split("def consumer_group_lag")[1].split("def ")[0]
        self.assertIn('"LAG" in upper_tokens and "TOPIC" in upper_tokens', helper)

    def test_no_topic_group_or_data_deletion(self) -> None:
        self.assertNotIn("--delete", self.runner)
        create_block = self.runner.split("def create_oc13_topics")[1].split("def ")[0]
        self.assertIn('"--create", "--if-not-exists"', create_block)


class OC13RunnerPureFunctionTest(unittest.TestCase):
    """Pure tests over the runner module: no Docker, Kafka or InfluxDB."""

    @classmethod
    def setUpClass(cls) -> None:
        spec = importlib.util.spec_from_file_location("oc13_runner", RUNNER)
        cls.runner_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.runner_module)

    def test_generate_run_id_is_unique_for_the_same_instant(self) -> None:
        moment = dt.datetime(2026, 9, 18, 12, 0, 0)
        first = self.runner_module.generate_run_id(now=moment)
        second = self.runner_module.generate_run_id(now=moment)
        self.assertNotEqual(first, second)

    def test_generate_run_id_format_is_safe(self) -> None:
        run_id = self.runner_module.generate_run_id(now=dt.datetime(2026, 9, 18, 12, 0, 0))
        self.assertRegex(run_id, r"^oc13-\d{8}-\d{6}-[0-9a-f]{8}$")

    def test_oc13_topic_template_substitution(self) -> None:
        self.runner_module._RUN_ID = "oc13-20260918-120000-abcdef01"
        try:
            self.assertEqual(
                self.runner_module.oc13_topic("oc13-source-{run_id}"),
                "oc13-source-oc13-20260918-120000-abcdef01",
            )
        finally:
            self.runner_module._RUN_ID = ""

    def test_main_guard_prevents_execution_on_import(self) -> None:
        self.assertIn('if __name__ == "__main__":', RUNNER.read_text(encoding="utf-8"))
        self.assertNotIn("main()", RUNNER.read_text(encoding="utf-8").split('if __name__ == "__main__":')[0][-200:])


class OC13ComparatorWiringTest(unittest.TestCase):
    """OC-13 B: the comparator is the single decision point."""

    def test_comparator_module_exists_with_pure_entry_points(self) -> None:
        text = COMPARATOR.read_text(encoding="utf-8")
        for symbol in ("def compare_campaign", "def index_export", "def build_report", "def main"):
            self.assertIn(symbol, text)
        self.assertIn('if __name__ == "__main__":', text)

    def test_comparator_rejects_the_oc12_campaign_name(self) -> None:
        text = COMPARATOR.read_text(encoding="utf-8")
        self.assertIn('CAMPAIGN_NAME = "oc13-regular-v2-comparison"', text)

    def test_wrapper_is_a_thin_launcher(self) -> None:
        wrapper = RUNNER_SH.read_text(encoding="utf-8")
        self.assertIn("run-explainability-regular-v2-comparison.py", wrapper)
        self.assertIn('PYTHONPATH=src:.', wrapper)


if __name__ == "__main__":
    unittest.main()
