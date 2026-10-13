"""V1-06 contract tests: collision fixture, compose isolation, runner wiring.

Pure tests: no Docker, Kafka or InfluxDB. The runner module is imported for
its pure functions and subprocess wiring only (the parity comparator it
invokes is itself pure); its `__main__` guard is verified separately.

The V1-06 comparison path is STRICTLY SEPARATE from the frozen OC-13
comparison: OC-13 keeps its historical (Group, timestamp) witness; V1-06
compares on the exclusive (Group, source_event_id) identity through the
V1-05c parity CLI.
"""

import csv
import importlib.util
import json
import re
import tempfile
import unittest
from pathlib import Path

from experiments.explainability.v1_v2.parity_comparator import (
    COMPARISON_KEY,
    ParityComparatorError,
    VERDICT_PARITY_OK,
    build_parity_report,
    index_export,
    load_manifest,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = PROJECT_ROOT / "tests" / "fixtures" / "explainability_v1_v2"
Q1_THREE_EXPORTS = FIXTURE_DIR / "q1_three_exports"
V1V2_MANIFEST_TEMPLATE = FIXTURE_DIR / "v1v2_parity_manifest.json"
SOURCE_CSV = PROJECT_ROOT / "tests" / "fixtures" / "explainability_v2" / "oc12_source.csv"
V1V2_COMPOSE = PROJECT_ROOT / "docker/compose/experiments/docker-compose.v1v2.yml"
RUNNER = PROJECT_ROOT / "scripts" / "integration" / "run-explainability-v1-v2-parity.py"
RUNNER_SH = PROJECT_ROOT / "scripts" / "integration" / "run-explainability-v1-v2-parity.sh"
OC13_RUNNER = PROJECT_ROOT / "scripts" / "integration" / "run-explainability-regular-v2-comparison.py"
OC13_COMPARATOR = PROJECT_ROOT / "experiments" / "explainability" / "v2" / "compare_regular_v2.py"
PARITY_MODULE = "experiments.explainability.v1_v2.parity_comparator"


def load_csv_rows() -> list:
    with SOURCE_CSV.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def parse_compose_service_env(compose_text: str, service: str) -> dict:
    """Extract the environment mapping of one service from the V1-06 compose
    file (dedicated mini-parser: no yaml dependency in the test suite)."""
    return _service_block(compose_text, service)["env"]


def _service_block(compose_text: str, service: str) -> dict:
    """Return {'text': block, 'env': env dict} for one 2-space-indented service."""
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


class V1V2FixtureScenarioTest(unittest.TestCase):
    """V1-06 A: the collision fixture matches the V1-05c manifest template."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.rows = load_csv_rows()
        cls.template = json.loads(V1V2_MANIFEST_TEMPLATE.read_text(encoding="utf-8"))

    def test_template_passes_the_v1_05c_validation(self) -> None:
        self.assertEqual(load_manifest(json.loads(json.dumps(self.template))), self.template)

    def test_comparison_key_is_exclusively_group_and_source_event_id(self) -> None:
        self.assertEqual(self.template["comparison_key"], COMPARISON_KEY)

    def test_expected_keys_match_the_csv_identities_exactly(self) -> None:
        keys = [f"{row['Group']}|{row['source_event_id']}" for row in self.rows]
        self.assertEqual(sorted(self.template["expected_keys"]), sorted(keys))
        self.assertEqual(len(self.template["expected_keys"]), len(keys))

    def test_source_event_ids_are_unique(self) -> None:
        ids = [row["source_event_id"] for row in self.rows]
        self.assertEqual(len(set(ids)), len(ids))

    def test_scenario_carries_a_timestamp_collision_with_distinct_ids(self) -> None:
        # At least two events of the same group share one timestamp while
        # keeping distinct source_event_id values: the (Group, timestamp) key
        # would merge them, the exclusive identity must not.
        collisions = {}
        for row in self.rows:
            collisions.setdefault((row["Group"], row["timestamp"]), []).append(
                row["source_event_id"]
            )
        pairs = {key: ids for key, ids in collisions.items() if len(ids) > 1}
        self.assertTrue(pairs, "the V1-06 fixture must contain a timestamp collision")
        self.assertTrue(all(len(set(ids)) == len(ids) for ids in pairs.values()))

    def test_tolerances_are_declared_and_non_negative(self) -> None:
        tolerances = self.template["numeric_tolerances"]
        self.assertEqual(tolerances["absolute_tolerance"], 1e-12)
        self.assertEqual(tolerances["relative_tolerance"], 1e-12)


class V1V2CollisionPairComparableTest(unittest.TestCase):
    """V1-06 B: same timestamp, distinct source_event_id stays comparable."""

    @classmethod
    def setUpClass(cls) -> None:
        rows = load_csv_rows()
        collisions = {}
        for row in rows:
            collisions.setdefault((row["Group"], row["timestamp"]), []).append(row)
        cls.collision_rows = next(
            pair for pair in collisions.values() if len(pair) > 1
        )

    @staticmethod
    def _export_record(row, **overrides):
        record = {
            "timestamp": row["timestamp"],
            "Group": row["Group"],
            "source_event_id": row["source_event_id"],
            "hstree_score": 1.25,
            "features": {"feat_a": float(row["feat_a"]), "feat_b": float(row["feat_b"])},
            "shap_values": {"feat_a": 0.1, "feat_b": -0.2},
            "shap_base_value": 0.05,
            "shap_computation_time_ms": 1.0,
            "local_hstree_score": 0.5,
            "hstree_status": "anomaly",
            "hstree_anomaly": True,
            "hstree_threshold": 0.7,
            "explanation_status": "computed",
            "explanation_error_code": None,
            "explanation_profile": "exhaustive",
            "shap_nsamples": 100,
            "shap_background_size": 100,
            "explainability_seed": 42,
            "hstree_model_version": "local-hstree-v3",
            "hstree_model_fingerprint": "0" * 64,
            "feature_fingerprint": "1" * 64,
        }
        record.update(overrides)
        return record

    def _report(self, v1_records, v2_records):
        manifest = load_manifest(
            {
                "campaign": "v1v2-collision",
                "comparison_key": COMPARISON_KEY,
                "exports": {"v1": "v1.jsonl", "v2": "v2.jsonl"},
                "expected_keys": [
                    f"{row['Group']}|{row['source_event_id']}" for row in self.collision_rows
                ],
                "numeric_tolerances": {"absolute_tolerance": 1e-12, "relative_tolerance": 1e-12},
            }
        )
        return build_parity_report(
            manifest,
            index_export(v1_records, "v1"),
            index_export(v2_records, "v2"),
        )

    def test_collision_pair_produces_two_distinct_comparable_identities(self) -> None:
        v1_records = [self._export_record(row) for row in self.collision_rows]
        v2_records = [self._export_record(row) for row in self.collision_rows]
        report = self._report(v1_records, v2_records)
        summary = report["summary"]
        self.assertEqual(summary["verdict"], VERDICT_PARITY_OK)
        self.assertEqual(summary["coverage"]["compared_identities"], 2)
        self.assertEqual(summary["coverage"]["only_v1"], 0)
        self.assertEqual(summary["coverage"]["only_v2"], 0)

    def test_collision_pair_members_are_compared_independently(self) -> None:
        # Diverge only the SECOND member of the pair: the first must stay
        # equal instead of being merged into one timestamp key.
        first, second = self.collision_rows
        v1_records = [self._export_record(row) for row in self.collision_rows]
        v2_records = [
            self._export_record(first),
            self._export_record(second, hstree_score=9.9),
        ]
        report = self._report(v1_records, v2_records)
        categories = report["summary"]["status"]["categories"]
        self.assertEqual(categories["both_computed_equal"], 1)
        self.assertEqual(categories["both_computed_divergent"], 1)
        self.assertEqual(report["summary"]["verdict"], "parity_ko")

    def test_timestamp_only_difference_is_never_a_new_identity(self) -> None:
        # Two records with the same source_event_id but different timestamps
        # are the SAME identity: the timestamp is never a comparison key.
        row = self.collision_rows[0]
        v1_records = [self._export_record(row)]
        v2_records = [self._export_record(row, timestamp="2099-01-01 00:00:00")]
        report = self._report(v1_records, v2_records)
        comparison = report["comparison"]["identities"]
        self.assertIn(f"{row['Group']}|{row['source_event_id']}", comparison)
        self.assertEqual(report["summary"]["coverage"]["compared_identities"], 1)


class V1V2ComposeContractTest(unittest.TestCase):
    """V1-06 C: isolated compose with only dedicated v1v2-* services."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.text = V1V2_COMPOSE.read_text(encoding="utf-8")

    def test_only_v1v2_services_are_defined(self) -> None:
        self.assertIn("  v1v2-producer:", self.text)
        self.assertIn("  v1v2-hstree:", self.text)
        self.assertIn("  v1v2-regular:", self.text)
        self.assertIn("  v1v2-v2:", self.text)
        self.assertNotIn("  oc13-", self.text)
        self.assertNotIn("  oc12-", self.text)

    def test_no_observability_stack_in_the_comparison(self) -> None:
        self.assertNotIn("telegraf:", self.text)
        self.assertNotIn("grafana:", self.text)
        self.assertNotIn("influxdb:", self.text)

    def test_every_container_and_topic_carries_the_run_id(self) -> None:
        for service in ("v1v2-producer", "v1v2-hstree", "v1v2-regular", "v1v2-v2"):
            block = _service_block(self.text, service)["text"]
            self.assertIn(f"container_name: {service}-" + "${V1V2_RUN_ID}", block)
        for topic_template in (
            "v1v2-source-${V1V2_RUN_ID}",
            "v1v2-hstree-${V1V2_RUN_ID}",
            "v1v2-hstree-viz-${V1V2_RUN_ID}",
            "v1v2-regular-${V1V2_RUN_ID}",
            "v1v2-regular-viz-${V1V2_RUN_ID}",
            "v1v2-v2-${V1V2_RUN_ID}",
            "v1v2-v2-viz-${V1V2_RUN_ID}",
        ):
            self.assertIn(topic_template, self.text)

    def test_source_fixture_is_the_collision_campaign(self) -> None:
        producer = parse_compose_service_env(self.text, "v1v2-producer")
        self.assertEqual(
            producer["DATA_SOURCE_FILE"],
            "${V1V2_SOURCE_CSV:-tests/fixtures/explainability_v2/oc12_source.csv}",
        )

    def test_both_explainability_services_consume_the_same_dedicated_hstree_topic(self) -> None:
        for service in ("v1v2-regular", "v1v2-v2"):
            block = _service_block(self.text, service)["text"]
            self.assertIn('KAFKA_SOURCE_TOPIC: "v1v2-hstree-${V1V2_RUN_ID}"', block)

    def test_regular_and_v2_have_identical_strict_parameters(self) -> None:
        regular = parse_compose_service_env(self.text, "v1v2-regular")
        v2 = parse_compose_service_env(self.text, "v1v2-v2")
        shared = [
            "HSTREE_INITIAL_WINDOW_SIZE",
            "HSTREE_WINDOW_SIZE",
            "HSTREE_NUM_TREES",
            "HSTREE_VARIANT",
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
            self.assertIn(key, regular, f"missing {key} in v1v2-regular")
            self.assertIn(key, v2, f"missing {key} in v1v2-v2")
            self.assertEqual(regular[key], v2[key], f"{key} differs between regular and v2")
        self.assertEqual(regular["HSTREE_NUM_TREES"], "${V1V2_HSTREE_NUM_TREES:-25}")
        self.assertEqual(regular["HSTREE_VARIANT"], "${V1V2_HSTREE_VARIANT:-paper}")
        self.assertEqual(regular["HSTREE_INITIAL_WINDOW_SIZE"], "256")
        self.assertEqual(regular["HSTREE_WINDOW_SIZE"], "256")
        self.assertEqual(regular["SHAP_NSAMPLES"], "100")
        self.assertEqual(v2["SHAP_NSAMPLES"], "100")

    def test_dedicated_consumer_groups(self) -> None:
        regular = parse_compose_service_env(self.text, "v1v2-regular")
        v2 = parse_compose_service_env(self.text, "v1v2-v2")
        hstree = parse_compose_service_env(self.text, "v1v2-hstree")
        self.assertEqual(regular["EXPLAINABILITY_GROUP_ID"], "v1v2-regular-consumer-${V1V2_RUN_ID}")
        self.assertEqual(v2["EXPLAINABILITY_GROUP_ID"], "v1v2-v2-consumer-${V1V2_RUN_ID}")
        self.assertEqual(hstree["ANALYSE_HSTREE_GROUP_ID"], "v1v2-hstree-consumer-${V1V2_RUN_ID}")

    def test_regular_is_the_v1_worker_and_v2_the_v2_worker(self) -> None:
        regular = _service_block(self.text, "v1v2-regular")["text"]
        v2 = _service_block(self.text, "v1v2-v2")["text"]
        self.assertIn(
            'entrypoint: ["python", "/app/runtime/explainability/regular/explainability-multi-thread-by-group.py"]',
            regular,
        )
        self.assertIn(
            'entrypoint: ["python", "/app/runtime/explainability/v2/explainability_v2.py"]',
            v2,
        )

    def test_standard_services_are_not_redefined_or_stopped(self) -> None:
        for forbidden in ("  producer:", "  hstree-analysis:", "  explainability:", "  explainability-v2:"):
            self.assertNotIn(forbidden, self.text)
        self.assertNotIn("down", self.text)
        self.assertNotIn("--rm", self.text)


class V1V2RunnerContractTest(unittest.TestCase):
    """V1-06 D: OC-12R.1 safety guarantees and V1-05c wiring in the runner."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.runner = RUNNER.read_text(encoding="utf-8")

    def _main_body(self) -> str:
        return self.runner.split("def main(")[1].split("if __name__")[0]

    def test_finally_block_with_started_tracking(self) -> None:
        main_body = self._main_body()
        self.assertIn("v1v2_started = False", main_body)
        self.assertIn("v1v2_started = True", main_body)
        self.assertIn("finally:", main_body)

    def test_cleanup_targets_only_v1v2_services(self) -> None:
        helper = self.runner.split("def stop_v1v2_services")[1].split("def ")[0]
        self.assertIn('compose("stop", *V1V2_SERVICES', helper)
        self.assertIn('compose("rm", "-f", *V1V2_SERVICES', helper)
        self.assertNotIn("STANDARD_COMPUTE_SERVICES", helper)

    def test_failure_flow_logs_diagnostics_before_cleanup(self) -> None:
        finally_block = self._main_body().split("finally:", 1)[1]
        self.assertLess(
            finally_block.index("dump_service_logs"),
            finally_block.index("stop_v1v2_services"),
        )

    def test_keep_up_neutralizes_cleanup(self) -> None:
        finally_block = self._main_body().split("finally:", 1)[1]
        self.assertIn("v1v2_started and not args.keep_up", finally_block)

    def test_runner_refuses_running_standard_services(self) -> None:
        self.assertIn("assert_standard_services_stopped", self.runner)
        self.assertIn('"explainability"', self.runner)
        self.assertIn('"explainability-v2"', self.runner)
        self.assertIn("the V1-06 runner never stops shared services", self.runner)

    def test_global_timeout_is_bounded(self) -> None:
        self.assertIn("--global-timeout", self.runner)
        self.assertIn("def remaining(cap: float = 2400.0)", self.runner)
        self.assertIn("global timeout of", self.runner)

    def test_waits_in_the_required_order(self) -> None:
        main_body = self._main_body()
        positions = [
            main_body.index('log("waiting for the v1v2 producer to finish")'),
            main_body.index('log(f"waiting for {expected} events in {topic}")'),
            main_body.index('log(f"waiting for zero lags'),
            main_body.index('log("exporting the HSTree, regular (V1) and v2 JSON topics")'),
            main_body.index('log("validating common identity coverage across upstream, V1 and V2")'),
            main_body.index('log("building the V1-05c parity manifest from the run exports")'),
            main_body.index('log("running the V1-05c parity comparator on the raw exports")'),
        ]
        self.assertEqual(positions, sorted(positions))

    def test_waits_cover_all_three_viz_topics_and_both_lags(self) -> None:
        main_body = self._main_body()
        self.assertIn('("hstree", hstree_viz_topic)', main_body)
        self.assertIn('("regular", regular_viz_topic)', main_body)
        self.assertIn('("v2", v2_viz_topic)', main_body)
        self.assertIn("regular_lag == 0 and v2_lag == 0", main_body)

    def test_three_exports_are_exported_and_coverage_is_written(self) -> None:
        self.assertIn('artifacts / "v1v2-hstree-viz.jsonl"', self.runner)
        self.assertIn("validate_three_export_coverage(", self.runner)
        self.assertIn('artifacts / "three-export-coverage.json"', self.runner)

    def test_scenario_check_runs_before_docker(self) -> None:
        main_body = self._main_body()
        self.assertIn("assert_fixture_scenario(", main_body)
        self.assertLess(
            main_body.index("assert_fixture_scenario("),
            main_body.index('compose("up", "-d", *INFRA_SERVICES'),
        )

    def test_parity_cli_is_invoked_with_manifest_and_output(self) -> None:
        run_block = self.runner.split("def run_parity_comparator")[1].split("def ")[0]
        self.assertIn(f'PARITY_MODULE = "{PARITY_MODULE}"', self.runner)
        self.assertIn('sys.executable, "-m", PARITY_MODULE', run_block)
        self.assertIn('"--manifest", str(artifacts / "parity-manifest.json")', run_block)
        self.assertIn('"--output", str(artifacts / "parity-report.json")', run_block)

    def test_verdict_exit_codes_are_propagated(self) -> None:
        run_block = self.runner.split("def run_parity_comparator")[1].split("def ")[0]
        self.assertIn("result.returncode not in VERDICT_EXIT_CODES", run_block)
        self.assertIn("return result.returncode", run_block)
        self.assertIn("VERDICT_EXIT_CODES = (0, 1, 2)", self.runner)
        self.assertIn("CAMPAIGN_FAILURE_EXIT = 3", self.runner)
        main_body = self._main_body()
        self.assertIn("return verdict_code", main_body)
        self.assertIn("return CAMPAIGN_FAILURE_EXIT", main_body)

    def test_cli_summary_is_printed_as_the_proof(self) -> None:
        run_block = self.runner.split("def run_parity_comparator")[1].split("def ")[0]
        self.assertIn("print(result.stdout", run_block)

    def test_comparator_input_errors_are_explicit(self) -> None:
        run_block = self.runner.split("def run_parity_comparator")[1].split("def ")[0]
        self.assertIn("3=manifest, 4=file, 5=JSONL, 6=usage", run_block)

    def test_manifest_is_rewritten_and_validated_before_the_cli(self) -> None:
        builder = self.runner.split("def build_parity_manifest")[1].split("def ")[0]
        self.assertIn('manifest["exports"] = {"v1": str(regular_export), "v2": str(v2_export)}', builder)
        self.assertIn("load_manifest(", builder)
        self.assertIn(f"from {PARITY_MODULE} import load_manifest", builder)

    def test_runner_never_uses_the_oc13_comparator(self) -> None:
        self.assertNotIn("compare_regular_v2", self.runner)
        self.assertNotIn("oc13_manifest", self.runner)

    def test_minimum_artifacts_are_written(self) -> None:
        for artifact in (
            "v1v2-regular-viz.jsonl",
            "v1v2-v2-viz.jsonl",
            "v1v2-hstree-viz.jsonl",
            "three-export-coverage.json",
            "parity-manifest.json",
            "parity-report.json",
            "lag-metrics.json",
            "runner-metrics.json",
            "campaign-env.json",
            "services-failure-logs.txt",
        ):
            self.assertIn(artifact, self.runner)

    def test_lag_helper_requires_topic_and_lag_columns(self) -> None:
        helper = self.runner.split("def consumer_group_lag")[1].split("def ")[0]
        self.assertIn('"LAG" in upper_tokens and "TOPIC" in upper_tokens', helper)

    def test_no_topic_group_or_data_deletion(self) -> None:
        self.assertNotIn("--delete", self.runner)
        create_block = self.runner.split("def create_v1v2_topics")[1].split("def ")[0]
        self.assertIn('"--create", "--if-not-exists"', create_block)

    def test_main_guard_prevents_execution_on_import(self) -> None:
        self.assertIn('if __name__ == "__main__":', self.runner)


class V1V2RunnerPureFunctionTest(unittest.TestCase):
    """Pure tests over the runner module: no Docker, Kafka or InfluxDB.

    The parity CLI invoked by run_parity_comparator is itself pure (no
    Docker, Kafka or network), so the subprocess wiring is exercised against
    temporary exports only.
    """

    @classmethod
    def setUpClass(cls) -> None:
        spec = importlib.util.spec_from_file_location("v1v2_parity_runner", RUNNER)
        cls.runner_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.runner_module)
        # Keep volume checks independent of optional demonstration datasets.
        fixture_dir = tempfile.TemporaryDirectory(dir=FIXTURE_DIR)
        cls.addClassCleanup(fixture_dir.cleanup)
        cls.volume_source_csv = Path(fixture_dir.name) / "synthetic-volume.csv"
        with cls.volume_source_csv.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["timestamp", "Group", "feat_a"])
            writer.writeheader()
            for index in range(4999):
                writer.writerow({
                    "timestamp": index,
                    "Group": f"Volume-{index % 3}",
                    "feat_a": index % 17,
                })

    def test_generate_run_id_is_unique_for_the_same_instant(self) -> None:
        import datetime as dt

        moment = dt.datetime(2026, 9, 22, 12, 0, 0)
        first = self.runner_module.generate_run_id(now=moment)
        second = self.runner_module.generate_run_id(now=moment)
        self.assertNotEqual(first, second)

    def test_generate_run_id_format_is_safe(self) -> None:
        import datetime as dt

        run_id = self.runner_module.generate_run_id(now=dt.datetime(2026, 9, 22, 12, 0, 0))
        self.assertRegex(run_id, r"^v1v2-\d{8}-\d{6}-[0-9a-f]{8}$")

    def test_three_export_coverage_uses_the_q1_identity_contract(self) -> None:
        manifest = json.loads((Q1_THREE_EXPORTS / "manifest.json").read_text(encoding="utf-8"))
        coverage = self.runner_module.validate_three_export_coverage(
            Q1_THREE_EXPORTS / "upstream.jsonl",
            Q1_THREE_EXPORTS / "v1.jsonl",
            Q1_THREE_EXPORTS / "v2.jsonl",
            manifest["expected_keys"],
        )
        self.assertTrue(coverage["all_equal"])
        self.assertEqual(coverage["expected_count"], 10)

    def test_three_export_coverage_rejects_missing_identity(self) -> None:
        manifest = json.loads((Q1_THREE_EXPORTS / "manifest.json").read_text(encoding="utf-8"))
        with tempfile.TemporaryDirectory() as tmp:
            v2_path = Path(tmp) / "v2.jsonl"
            v2_path.write_text(
                "\n".join((Q1_THREE_EXPORTS / "v2.jsonl").read_text(encoding="utf-8").splitlines()[:-1])
                + "\n",
                encoding="utf-8",
            )
            with self.assertRaises(self.runner_module.RunnerError) as context:
                self.runner_module.validate_three_export_coverage(
                    Q1_THREE_EXPORTS / "upstream.jsonl",
                    Q1_THREE_EXPORTS / "v1.jsonl",
                    v2_path,
                    manifest["expected_keys"],
                )
            self.assertIn("coverage mismatch", str(context.exception))

    def test_verdict_exit_codes_are_distinct(self) -> None:
        self.assertEqual(self.runner_module.VERDICT_EXIT_CODES, (0, 1, 2))
        self.assertEqual(self.runner_module.CAMPAIGN_FAILURE_EXIT, 3)

    def test_fixture_scenario_accepts_the_shipped_campaign(self) -> None:
        template = json.loads(V1V2_MANIFEST_TEMPLATE.read_text(encoding="utf-8"))
        self.runner_module.assert_fixture_scenario(SOURCE_CSV, template)

    def test_volume_source_derives_the_producer_generated_event_ids(self) -> None:
        keys = self.runner_module.expected_keys_for_source(self.volume_source_csv)
        self.assertEqual(len(keys), 4999)
        self.assertEqual(keys[0], "Volume-0|event-000000000001")
        self.assertTrue(keys[-1].endswith("|event-000000004999"))
        self.assertEqual(len(set(keys)), len(keys))

    def test_volume_source_manifest_is_dataset_specific(self) -> None:
        template = json.loads(V1V2_MANIFEST_TEMPLATE.read_text(encoding="utf-8"))
        manifest = self.runner_module.manifest_for_source(self.volume_source_csv, template)
        self.assertEqual(len(manifest["expected_keys"]), 4999)
        self.assertEqual(
            manifest["source_csv"],
            str(self.volume_source_csv.relative_to(PROJECT_ROOT)),
        )
        self.runner_module.assert_fixture_scenario(
            self.volume_source_csv, manifest, require_timestamp_collision=False
        )

    def test_collision_check_can_be_disabled_for_volume_dataset(self) -> None:
        template = json.loads(V1V2_MANIFEST_TEMPLATE.read_text(encoding="utf-8"))
        manifest = self.runner_module.manifest_for_source(self.volume_source_csv, template)
        with self.assertRaises(self.runner_module.RunnerError):
            self.runner_module.assert_fixture_scenario(self.volume_source_csv, manifest)

    def test_fixture_scenario_rejects_a_missing_expected_key(self) -> None:
        template = json.loads(V1V2_MANIFEST_TEMPLATE.read_text(encoding="utf-8"))
        template["expected_keys"] = template["expected_keys"][:-1]
        with self.assertRaises(self.runner_module.RunnerError):
            self.runner_module.assert_fixture_scenario(SOURCE_CSV, template)

    def test_fixture_scenario_rejects_a_fixture_without_collision(self) -> None:
        template = json.loads(V1V2_MANIFEST_TEMPLATE.read_text(encoding="utf-8"))
        with tempfile.TemporaryDirectory() as tmp:
            collision_free = Path(tmp) / "no-collision.csv"
            with collision_free.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["timestamp", "Group", "source_event_id", "feat_a", "feat_b"],
                )
                writer.writeheader()
                for index in range(3):
                    writer.writerow(
                        {
                            "timestamp": f"2026-09-18 12:00:0{index}",
                            "Group": "G",
                            "source_event_id": f"evt-{index}",
                            "feat_a": "10.0",
                            "feat_b": "20.0",
                        }
                    )
            template["expected_keys"] = [f"G|evt-{index}" for index in range(3)]
            with self.assertRaises(self.runner_module.RunnerError) as ctx:
                self.runner_module.assert_fixture_scenario(collision_free, template)
            self.assertIn("no (Group, timestamp) collision", str(ctx.exception))

    def test_build_parity_manifest_rewrites_exports_and_stays_valid(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifacts = Path(tmp)
            v1_export = artifacts / "v1v2-regular-viz.jsonl"
            v2_export = artifacts / "v1v2-v2-viz.jsonl"
            v1_export.write_text("", encoding="utf-8")
            v2_export.write_text("", encoding="utf-8")
            target = self.runner_module.build_parity_manifest(artifacts, v1_export, v2_export)
            self.assertEqual(target, artifacts / "parity-manifest.json")
            manifest = json.loads(target.read_text(encoding="utf-8"))
            self.assertEqual(manifest["exports"]["v1"], str(v1_export))
            self.assertEqual(manifest["exports"]["v2"], str(v2_export))
            self.assertEqual(manifest["comparison_key"], COMPARISON_KEY)
            load_manifest(manifest)

    def test_parity_comparator_propagates_the_verdict_code(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifacts = Path(tmp)
            v1_export = artifacts / "v1.jsonl"
            v2_export = artifacts / "v2.jsonl"
            records = [
                {
                    "timestamp": "2026-09-22 12:00:00",
                    "Group": "G",
                    "source_event_id": "row-1",
                    "explanation_status": "warmup",
                    "hstree_score": 1.25,
                    "hstree_status": "anomaly",
                    "hstree_anomaly": True,
                    "hstree_threshold": 0.7,
                    "shap_values": None,
                    "shap_base_value": None,
                    "local_hstree_score": None,
                }
            ]
            v1_export.write_text(
                "".join(json.dumps(record) + "\n" for record in records), encoding="utf-8"
            )
            v2_export.write_text(
                "".join(json.dumps(record) + "\n" for record in records), encoding="utf-8"
            )
            (artifacts / "parity-manifest.json").write_text(
                json.dumps(
                    {
                        "campaign": "wiring",
                        "comparison_key": COMPARISON_KEY,
                        "exports": {"v1": str(v1_export), "v2": str(v2_export)},
                        "expected_keys": ["G|row-1"],
                        "numeric_tolerances": {
                            "absolute_tolerance": 1e-12,
                            "relative_tolerance": 1e-12,
                        },
                    }
                ),
                encoding="utf-8",
            )
            code = self.runner_module.run_parity_comparator(artifacts, timeout=60.0)
            self.assertEqual(code, 0)
            self.assertTrue((artifacts / "parity-report.json").is_file())

    def test_parity_comparator_maps_input_errors_to_campaign_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifacts = Path(tmp)
            v1_export = artifacts / "v1.jsonl"
            v2_export = artifacts / "v2.jsonl"
            v1_export.write_text('{"Group": "G"}\nnot-json\n', encoding="utf-8")
            v2_export.write_text("", encoding="utf-8")
            (artifacts / "parity-manifest.json").write_text(
                json.dumps(
                    {
                        "campaign": "wiring",
                        "comparison_key": COMPARISON_KEY,
                        "exports": {"v1": str(v1_export), "v2": str(v2_export)},
                        "expected_keys": ["G|row-1"],
                        "numeric_tolerances": {
                            "absolute_tolerance": 1e-12,
                            "relative_tolerance": 1e-12,
                        },
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaises(self.runner_module.RunnerError) as ctx:
                self.runner_module.run_parity_comparator(artifacts, timeout=60.0)
            # JSONL error (5) is a campaign failure, never a verdict.
            self.assertIn("exit code 5", str(ctx.exception))

    def test_incomplete_inputs_verdict_is_propagated(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifacts = Path(tmp)
            v1_export = artifacts / "v1.jsonl"
            v2_export = artifacts / "v2.jsonl"
            record = {
                "timestamp": "2026-09-22 12:00:00",
                "Group": "G",
                "source_event_id": "row-1",
                "explanation_status": "warmup",
                "hstree_score": 1.25,
                "hstree_status": "anomaly",
                "hstree_anomaly": True,
                "hstree_threshold": 0.7,
                "shap_values": None,
                "shap_base_value": None,
                "local_hstree_score": None,
            }
            v1_export.write_text(json.dumps(record) + "\n", encoding="utf-8")
            v2_export.write_text("", encoding="utf-8")
            (artifacts / "parity-manifest.json").write_text(
                json.dumps(
                    {
                        "campaign": "wiring",
                        "comparison_key": COMPARISON_KEY,
                        "exports": {"v1": str(v1_export), "v2": str(v2_export)},
                        "expected_keys": ["G|row-1"],
                        "numeric_tolerances": {
                            "absolute_tolerance": 1e-12,
                            "relative_tolerance": 1e-12,
                        },
                    }
                ),
                encoding="utf-8",
            )
            code = self.runner_module.run_parity_comparator(artifacts, timeout=60.0)
            self.assertEqual(code, 2)

    def test_wrapper_is_a_thin_launcher(self) -> None:
        wrapper = RUNNER_SH.read_text(encoding="utf-8")
        self.assertIn("run-explainability-v1-v2-parity.py", wrapper)
        self.assertIn('PYTHONPATH=src:.', wrapper)


class OC13SeparationTest(unittest.TestCase):
    """V1-06 E: the OC-13 path stays frozen; only the Docker pattern is reused."""

    def test_oc13_runner_still_targets_the_oc13_comparator(self) -> None:
        runner = OC13_RUNNER.read_text(encoding="utf-8")
        self.assertIn("compare_regular_v2.py", runner)
        self.assertIn("oc13_manifest.json", runner)
        self.assertIn("oc13-comparison-report.json", runner)

    def test_oc13_comparator_keeps_the_timestamp_key(self) -> None:
        comparator = OC13_COMPARATOR.read_text(encoding="utf-8")
        self.assertIn('CAMPAIGN_NAME = "oc13-regular-v2-comparison"', comparator)
        self.assertIn('return f"{group}|{timestamp}"', comparator)

    def test_v1v2_runner_is_a_separate_file(self) -> None:
        self.assertNotEqual(RUNNER, OC13_RUNNER)
        runner = RUNNER.read_text(encoding="utf-8")
        oc13_runner = OC13_RUNNER.read_text(encoding="utf-8")
        self.assertIn("V1-06", runner)
        self.assertNotIn("V1-06", oc13_runner)


class ParityManifestErrorContractTest(unittest.TestCase):
    """The runner surfaces the V1-05c error kinds instead of swallowing them."""

    def test_parity_errors_are_value_errors_from_the_pure_module(self) -> None:
        self.assertTrue(issubclass(ParityComparatorError, ValueError))


if __name__ == "__main__":
    unittest.main()
