"""OC-14 contract tests for the capacity runner, compose and offline benchmark.

Pure tests: no Docker, Kafka or InfluxDB. The runner module is imported for
its pure functions only; no Docker call can happen at pytest import time.
"""

import datetime as dt
import importlib.util
import json
import re
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OC14_COMPOSE = PROJECT_ROOT / "docker/compose/experiments/docker-compose.oc14.yml"
RUNNER = PROJECT_ROOT / "scripts" / "integration" / "run-explainability-v2-capacity.py"
RUNNER_SH = PROJECT_ROOT / "scripts" / "integration" / "run-explainability-v2-capacity.sh"
BENCHMARK = PROJECT_ROOT / "experiments" / "explainability" / "v2" / "capacity_benchmark.py"
REPORT = PROJECT_ROOT / "experiments" / "explainability" / "v2" / "capacity_report.py"
STABILITY_RUNNER = PROJECT_ROOT / "experiments" / "explainability" / "v2" / "shap_stability_runner.py"
MANIFEST = PROJECT_ROOT / "tests" / "fixtures" / "explainability_v2" / "oc14_capacity_manifest.json"


class OC14RunnerBudgetContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.runner = RUNNER.read_text(encoding="utf-8")

    def test_default_budgets_are_exactly_100_1000_1500_2000(self) -> None:
        self.assertIn("DEFAULT_BUDGETS = [100, 1000, 1500, 2000]", self.runner)

    def test_manifest_declares_the_same_budgets(self) -> None:
        manifest = json_load(MANIFEST)
        self.assertEqual(manifest["expected_budgets"], [100, 1000, 1500, 2000])

    def test_background_100_everywhere(self) -> None:
        compose = OC14_COMPOSE.read_text(encoding="utf-8")
        self.assertIn('SHAP_BACKGROUND_SIZE: "100"', compose)
        manifest = json_load(MANIFEST)
        self.assertEqual(manifest["campaign_env"]["SHAP_BACKGROUND_SIZE"], "100")
        benchmark = BENCHMARK.read_text(encoding="utf-8")
        self.assertIn('"background": 100', json.dumps(manifest["offline_benchmark"]))
        self.assertIn('shap_background_size=int(env["SHAP_BACKGROUND_SIZE"])', benchmark)


class OC14ComposeContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.text = OC14_COMPOSE.read_text(encoding="utf-8")

    def test_only_oc14_services_are_defined(self) -> None:
        self.assertIn("  oc14-producer:", self.text)
        self.assertIn("  oc14-hstree:", self.text)
        self.assertIn("  oc14-v2:", self.text)
        self.assertNotIn("  oc12-", self.text)
        self.assertNotIn("  oc13-", self.text)
        self.assertNotIn("  oc14-telegraf", self.text)
        self.assertNotIn("telegraf:", self.text)
        self.assertNotIn("grafana:", self.text)
        self.assertNotIn("influxdb:", self.text)

    def test_every_topic_and_container_carries_run_id_and_budget(self) -> None:
        for template in (
            "oc14-ns${OC14_NSAMPLES}-source-${OC14_RUN_ID}",
            "oc14-ns${OC14_NSAMPLES}-hstree-${OC14_RUN_ID}",
            "oc14-ns${OC14_NSAMPLES}-hstree-viz-${OC14_RUN_ID}",
            "oc14-ns${OC14_NSAMPLES}-v2-${OC14_RUN_ID}",
            "oc14-ns${OC14_NSAMPLES}-v2-viz-${OC14_RUN_ID}",
        ):
            self.assertIn(template, self.text)
        self.assertIn("container_name: oc14-producer-${OC14_RUN_ID}", self.text)
        self.assertIn("container_name: oc14-hstree-${OC14_RUN_ID}", self.text)
        self.assertIn("container_name: oc14-v2-${OC14_RUN_ID}", self.text)

    def test_v2_budget_comes_from_the_environment(self) -> None:
        self.assertIn('SHAP_NSAMPLES: "${OC14_NSAMPLES}"', self.text)

    def test_dedicated_consumer_groups(self) -> None:
        self.assertIn("oc14-ns${OC14_NSAMPLES}-hstree-consumer-${OC14_RUN_ID}", self.text)
        self.assertIn("oc14-ns${OC14_NSAMPLES}-v2-consumer-${OC14_RUN_ID}", self.text)

    def test_no_destructive_primitive(self) -> None:
        self.assertNotIn("--delete", self.text)
        self.assertNotIn("down", self.text)
        self.assertNotIn("  producer:", self.text)
        self.assertNotIn("  explainability:", self.text)
        self.assertNotIn("  explainability-v2:", self.text)


class OC14RunnerSafetyContractTest(unittest.TestCase):
    """OC-12R.1/OC-13 guarantees in the OC-14 runner source."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.runner = RUNNER.read_text(encoding="utf-8")

    def test_refuses_standard_services_including_telegraf(self) -> None:
        block = self.runner.split("STANDARD_COMPUTE_SERVICES = [")[1].split("]")[0]
        for service in ("producer", "hstree-analysis", "explainability-v2", "explainability", "telegraf"):
            self.assertIn(f'"{service}"', block)

    def test_finally_block_with_started_tracking_and_budget_envs(self) -> None:
        main_body = self.runner.split("def main(")[1].split("if __name__")[0]
        self.assertIn("oc14_started = False", main_body)
        self.assertIn("oc14_started = True", main_body)
        self.assertIn("budget_envs_started", main_body)
        self.assertIn("finally:", main_body)

    def test_failure_flow_logs_diagnostics_before_cleanup(self) -> None:
        main_body = self.runner.split("def main(")[1].split("if __name__")[0]
        main_finally = main_body.split("finally:")[-1]
        self.assertLess(
            main_finally.index("dump_service_logs"),
            main_finally.index("stop_oc14_services"),
        )

    def test_keep_up_neutralizes_cleanup(self) -> None:
        self.assertIn("--keep-up", self.runner)
        self.assertIn("if not args.keep_up:", self.runner)

    def test_only_oc14_topics_are_created(self) -> None:
        create_block = self.runner.split("def create_oc14_topics")[1].split("def ")[0]
        self.assertIn('"--create", "--if-not-exists"', create_block)
        self.assertNotIn("--delete", self.runner)
        self.assertNotIn("kafka-acls", self.runner)
        # No deletion of topics/groups/subjects anywhere.
        self.assertNotIn("kafka-configs.sh", self.runner)
        self.assertNotIn("/api/v2/delete", self.runner)

    def test_global_timeout_is_bounded(self) -> None:
        self.assertIn("--global-timeout", self.runner)
        self.assertIn("global timeout of", self.runner)

    def test_lag_sampler_interval_is_one_second(self) -> None:
        sampler = self.runner.split("class LagSampler")[1].split("def analyze_campaign_export")[0]
        self.assertIn("interval: float = 1.0", sampler)

    def test_lag_parser_requires_topic_and_lag_columns(self) -> None:
        helper = self.runner.split("def consumer_group_lag")[1].split("def ")[0]
        self.assertIn('"LAG" in upper_tokens and "TOPIC" in upper_tokens', helper)

    def test_mandatory_artifacts_and_metrics(self) -> None:
        for artifact in (
            "lag-samples.json",
            "campaign.json",
            "offline-benchmark.json",
            "oc14-capacity-report.json",
            "oc14-capacity-report.md",
            "campaign-env.json",
            "host-info.json",
            "runner-metrics.json",
            "services-failure-logs.txt",
        ):
            self.assertIn(artifact, self.runner)

    def test_mandatory_computed_metrics(self) -> None:
        for metric in (
            "status_distribution",
            "duration_seconds",
            "total_duration_seconds",
            "drain_seconds",
            "max_lag",
            "lag_area_event_seconds",
            "delivery_throughput_events_per_second",
            "shap_time_stats",
            "final_lag",
        ):
            self.assertIn(metric, self.runner)

    def test_invalid_budget_measurement_fails_the_campaign(self) -> None:
        self.assertIn("no performance conclusion", self.runner)
        self.assertIn('not campaign["valid"]', self.runner)

    def test_report_uses_the_pure_module_and_versioned_output(self) -> None:
        self.assertIn("oc14-capacity-", self.runner)
        self.assertIn('REPORTS_DIR / f"oc14-capacity-{campaign_run_id}.md"', self.runner)

    def test_no_docker_execution_at_module_level(self) -> None:
        # All compose/run_command calls live inside functions or methods.
        module_level = self.runner.split("class RunnerError")[0] + self.runner.split("def main(")[0].split("def generate_run_id")[0]
        self.assertNotIn("subprocess.run(", module_level)
        self.assertIn('if __name__ == "__main__":', self.runner)


class OC14RunnerPureFunctionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        spec = importlib.util.spec_from_file_location("oc14_runner", RUNNER)
        cls.runner_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.runner_module)

    def test_generate_run_id_is_unique_for_the_same_instant(self) -> None:
        moment = dt_datetime(2026, 9, 18, 14, 0, 0)
        first = self.runner_module.generate_run_id(now=moment)
        second = self.runner_module.generate_run_id(now=moment)
        self.assertNotEqual(first, second)
        self.assertRegex(first, r"^oc14-\d{8}-\d{6}-[0-9a-f]{8}$")

    def test_topic_templates_carry_budget_and_run_id(self) -> None:
        topic = self.runner_module.OC14_TOPICS[0].format(nsamples=1000, run_id="oc14-20260918-140000-abcdef01")
        self.assertEqual(topic, "oc14-ns1000-source-oc14-20260918-140000-abcdef01")


class OC14OfflineBenchmarkContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.benchmark = BENCHMARK.read_text(encoding="utf-8")

    def test_uses_the_stability_runner_building_blocks_without_modifying_it(self) -> None:
        self.assertIn("from shap_stability_runner import build_model, load_group_rows, shap_vector", self.benchmark)
        # The existing stability runner keeps its public interface intact.
        stability = STABILITY_RUNNER.read_text(encoding="utf-8")
        self.assertIn('p.add_argument("--background", type=int, default=40)', stability_runner_text())
        self.assertIn("def main()", stability_runner_text())

    def test_budgets_default_and_min_calls(self) -> None:
        self.assertIn("DEFAULT_BUDGETS = [100, 1000, 1500, 2000]", self.benchmark)
        self.assertIn("DEFAULT_CALLS_PER_BUDGET = 3", self.benchmark)
        self.assertIn("calls_per_budget < 3", self.benchmark)

    def test_blas_threads_pinned_before_imports(self) -> None:
        pinning_position = self.benchmark.index('os.environ.setdefault(_thread_var, "1")')
        imports_position = self.benchmark.index("from dasma_kafka")
        self.assertLess(pinning_position, imports_position)

    def test_capacity_formula_is_reported(self) -> None:
        self.assertIn('"capacity_per_group": (1.0 / mean_seconds)', self.benchmark)

    def test_benchmark_does_not_conclude_on_kafka(self) -> None:
        self.assertIn("does NOT conclude on Kafka delivery", self.benchmark)


class OC14WrapperAndReportModuleTest(unittest.TestCase):
    def test_wrapper_is_a_thin_launcher(self) -> None:
        wrapper = RUNNER_SH.read_text(encoding="utf-8")
        self.assertIn("run-explainability-v2-capacity.py", wrapper)
        self.assertIn("PYTHONPATH=src:.", wrapper)

    def test_report_module_has_slo_options(self) -> None:
        report = REPORT.read_text(encoding="utf-8")
        self.assertIn("--target-events-per-second-per-group", report)
        self.assertIn("--max-drain-seconds", report)
        self.assertIn("--assumed-input-rates", report)
        self.assertIn("Décision automatique impossible", report)


def json_load(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def stability_runner_text() -> str:
    return STABILITY_RUNNER.read_text(encoding="utf-8")


def dt_datetime(*args):
    import datetime as dt

    return dt.datetime(*args)


if __name__ == "__main__":
    unittest.main()
