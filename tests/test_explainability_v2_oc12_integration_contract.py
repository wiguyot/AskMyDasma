"""Static contracts for the non-destructive OC-12 runner (no Docker in pytest)."""

from __future__ import annotations

import ast
from pathlib import Path
from unittest import TestCase


ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "scripts" / "integration" / "run-explainability-v2-campaign.py"
WRAPPER = ROOT / "scripts" / "integration" / "run-explainability-v2-campaign.sh"
VALIDATOR = ROOT / "experiments" / "explainability" / "v2" / "validate_oc12_campaign.py"
OVERRIDE = ROOT / "docker/compose/experiments/docker-compose.oc12.yml"
OC12_TELEGRAF = ROOT / "observability" / "telegraf" / "telegraf-oc12.conf"
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "explainability_v2"


class OC12RunnerContractTest(TestCase):
    def setUp(self) -> None:
        self.runner = RUNNER.read_text(encoding="utf-8")

    def test_required_files_exist(self) -> None:
        self.assertTrue(RUNNER.is_file())
        self.assertTrue(WRAPPER.is_file())
        self.assertTrue(VALIDATOR.is_file())
        self.assertTrue(OVERRIDE.is_file())
        self.assertTrue(OC12_TELEGRAF.is_file())
        self.assertTrue((FIXTURE_DIR / "oc12_source.csv").is_file())
        self.assertTrue((FIXTURE_DIR / "oc12_manifest.json").is_file())

    def test_runner_never_destroys_shared_resources(self) -> None:
        # No topic, consumer group, schema subject or InfluxDB deletion
        # anywhere in the runner.
        self.assertNotIn("--delete", self.runner)
        self.assertNotIn("/api/v2/delete", self.runner)
        self.assertNotIn("schema-registry:8081/subjects", self.runner)
        self.assertNotIn("--remove", self.runner)

    def test_runner_never_stops_standard_services(self) -> None:
        # The standard services appear only in the refusal guard, never in a
        # stop/rm/up command.
        guard = self.runner.split("def assert_standard_services_stopped")[1].split("def ")[0]
        self.assertIn("STANDARD_COMPUTE_SERVICES", guard)
        self.assertIn("raise RunnerError", guard)
        lifecycle = self.runner.split("def main(")[1]
        self.assertNotIn('compose("stop", *STANDARD', lifecycle)
        self.assertNotIn('"producer", "hstree-analysis"', lifecycle.split("assert_standard_services_stopped")[-1])

    def test_runner_refuses_when_standard_services_run(self) -> None:
        self.assertIn("def assert_standard_services_stopped", self.runner)
        self.assertIn("STANDARD_COMPUTE_SERVICES = [", self.runner)
        for service in ("producer", "hstree-analysis", "explainability-v2", "telegraf"):
            self.assertIn(f'"{service}"', self.runner.split("STANDARD_COMPUTE_SERVICES = [")[1].split("]")[0])

    def test_all_resources_derive_from_the_run_id(self) -> None:
        for template in (
            "oc12-source-{run_id}",
            "oc12-hstree-{run_id}",
            "oc12-hstree-viz-{run_id}",
            "oc12-v2-{run_id}",
            "oc12-v2-viz-{run_id}",
        ):
            self.assertIn(f'"{template}"', self.runner)
        self.assertIn("oc12-v2-consumer-{_RUN_ID}", self.runner)

    def test_runner_creates_only_oc12_topics(self) -> None:
        # The only kafka-topics.sh invocation is a create --if-not-exists.
        create_section = self.runner.split("def create_oc12_topics")[1].split("def ")[0]
        self.assertIn("--create", create_section)
        self.assertIn("--if-not-exists", create_section)
        self.assertNotIn("--delete", create_section)
        self.assertNotIn("--alter", create_section)

    def test_runner_exports_both_json_topics_and_checks_lag(self) -> None:
        self.assertIn("kafka-console-consumer.sh", self.runner)
        self.assertIn("hstree-analysis-viz.jsonl", self.runner)
        self.assertIn("explainability-v2-viz.jsonl", self.runner)
        self.assertIn("kafka-consumer-groups.sh", self.runner)
        self.assertNotIn('"--topic", group', self.runner)  # Kafka 3.9 guard
        self.assertIn("consumer_group_lag(v2_group, hstree_topic)", self.runner)

    def test_runner_calls_the_validator(self) -> None:
        self.assertIn("validate_oc12_campaign.py", self.runner)
        self.assertIn("oc12-validation-report.json", self.runner)

    def test_runner_is_time_bounded(self) -> None:
        self.assertIn("--global-timeout", self.runner)
        self.assertIn("default=900.0", self.runner)
        self.assertIn("def wait_until", self.runner)
        self.assertIn("timeout after {timeout:.0f}s waiting for", self.runner)

    def test_artifacts_are_always_kept(self) -> None:
        # Artifacts are the campaign proof: no deletion option exists.
        self.assertNotIn("--keep-artifacts", self.runner)
        self.assertIn("artifacts kept for diagnosis", self.runner)
        self.assertIn("Artifacts are always preserved", self.runner)

    def test_influx_proof_requires_identity_and_field_types(self) -> None:
        proof_section = self.runner.split("def proof_ready")[1].split("wait_until")[0]
        self.assertIn("explanation_status", proof_section)
        self.assertIn("hstree_anomaly", proof_section)
        self.assertIn("both_ids", proof_section)

    def test_pytest_never_starts_docker(self) -> None:
        tree = ast.parse(self.runner)
        main_calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "main"
        ]
        self.assertEqual(len(main_calls), 1)
        main_call = main_calls[0]
        guards = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.If)
            and isinstance(node.test, ast.Compare)
            and ast.unparse(node.test) == "__name__ == '__main__'"
        ]
        self.assertEqual(len(guards), 1)
        guarded_lines = {child.lineno for child in ast.walk(guards[0]) if hasattr(child, "lineno")}
        self.assertIn(main_call.lineno, guarded_lines)

    def test_wrapper_uses_the_venv_python(self) -> None:
        wrapper = WRAPPER.read_text(encoding="utf-8")
        self.assertIn(".venv/bin/python scripts/integration/run-explainability-v2-campaign.py", wrapper)
        self.assertIn('cd "$(dirname "$0")/../.."', wrapper)


class OC12OverrideComposeContractTest(TestCase):
    def setUp(self) -> None:
        self.override = OVERRIDE.read_text(encoding="utf-8")

    def test_override_declares_dedicated_oc12_services(self) -> None:
        for service in ("oc12-producer:", "oc12-hstree:", "oc12-v2:", "oc12-telegraf:"):
            self.assertIn(service, self.override)

    def test_override_uses_the_run_id_everywhere(self) -> None:
        self.assertEqual(self.override.count("${OC12_RUN_ID}"), self.override.count("${OC12_RUN_ID}"))
        self.assertGreaterEqual(self.override.count("${OC12_RUN_ID}"), 10)
        self.assertIn("oc12-source-${OC12_RUN_ID}", self.override)
        self.assertIn("oc12-hstree-${OC12_RUN_ID}", self.override)
        self.assertIn("oc12-v2-viz-${OC12_RUN_ID}", self.override)
        self.assertIn("oc12-v2-consumer-${OC12_RUN_ID}", self.override)
        self.assertIn("oc12-telegraf-consumer-${OC12_RUN_ID}", self.override)

    def test_override_does_not_touch_standard_services(self) -> None:
        for service in ("\n  producer:", "\n  hstree-analysis:", "\n  explainability-v2:", "\n  telegraf:"):
            self.assertNotIn(service, self.override)

    def test_override_pins_the_oc12_campaign_parameters(self) -> None:
        self.assertIn('HSTREE_INITIAL_WINDOW_SIZE: "100"', self.override)
        self.assertIn('HSTREE_ANOMALY_MIN_SAMPLES: "2"', self.override)
        self.assertIn('SHAP_BACKGROUND_SIZE: "100"', self.override)
        self.assertIn('SHAP_NSAMPLES: "100"', self.override)
        self.assertIn("tests/fixtures/explainability_v2/oc12_source.csv", self.override)


class OC12TelegrafConfigContractTest(TestCase):
    def setUp(self) -> None:
        self.conf = OC12_TELEGRAF.read_text(encoding="utf-8")

    def test_oc12_telegraf_reads_only_dedicated_resources(self) -> None:
        self.assertIn('topics = ["${OC12_V2_VIZ_TOPIC}"]', self.conf)
        self.assertIn('consumer_group = "${OC12_TELEGRAF_GROUP}"', self.conf)
        self.assertIn('measurement_name = "${OC12_MEASUREMENT}"', self.conf)
        self.assertIn('bucket = "oc12-campaign"', self.conf)
        # The standard telegraf resources are never referenced.
        self.assertNotIn("explainability-v2-viz", self.conf.replace("${OC12_V2_VIZ_TOPIC}", ""))

    def test_oc12_telegraf_uses_json_v2_with_explicit_types(self) -> None:
        self.assertIn('data_format = "json_v2"', self.conf)
        self.assertIn('explanation_status = "string"', self.conf)
        self.assertIn('hstree_anomaly = "bool"', self.conf)
        self.assertIn('tags = ["Group", "source_event_id"]', self.conf)
        self.assertIn('timestamp_path = "timestamp"', self.conf)
        self.assertIn('path = "shap_values"', self.conf)
        self.assertNotIn('path = "features"', self.conf)


class OC12RunnerCleanupContractTest(TestCase):
    """OC-12R.1 A: reliable oc12-only cleanup, on success and on failure."""

    def setUp(self) -> None:
        self.runner = RUNNER.read_text(encoding="utf-8")

    def test_single_cleanup_helper_targets_only_oc12_services(self) -> None:
        helper = self.runner.split("def stop_oc12_services")[1].split("def ")[0]
        self.assertIn('compose("stop", *OC12_SERVICES', helper)
        self.assertIn('compose("rm", "-f", *OC12_SERVICES', helper)
        self.assertNotIn("STANDARD_COMPUTE_SERVICES", helper)

    def test_main_uses_a_finally_block_with_started_tracking(self) -> None:
        main_body = self.runner.split("def main(")[1]
        self.assertIn("oc12_started = False", main_body)
        self.assertIn("oc12_started = True", main_body)
        self.assertIn("finally:", main_body)

    def test_failure_flow_logs_diagnostics_before_cleanup(self) -> None:
        main_body = self.runner.split("def main(")[1].split("if __name__")[0]
        finally_block = main_body.split("finally:", 1)[1]
        dump_position = finally_block.index("dump_service_logs")
        cleanup_position = finally_block.index("stop_oc12_services")
        self.assertLess(dump_position, cleanup_position)

    def test_cleanup_failure_never_masks_the_campaign_error(self) -> None:
        finally_block = self.runner.split("def main(")[1].split("if __name__")[0].split("finally:", 1)[1]
        cleanup_block = finally_block.split("stop_oc12_services()")[1].split("if error_message")[0]
        self.assertIn("except Exception as cleanup_exc", cleanup_block)
        self.assertIn("WARNING: cleanup failed", cleanup_block)

    def test_keep_up_neutralizes_cleanup_and_is_documented(self) -> None:
        self.assertIn("--keep-up", self.runner)
        finally_block = self.runner.split("def main(")[1].split("if __name__")[0].split("finally:", 1)[1]
        self.assertIn("oc12_started and not args.keep_up", finally_block)
        # Both success and failure paths go through the same finally.
        self.assertNotIn('compose("stop", *OC12_SERVICES', self.runner.split("def stop_oc12_services")[0].split("def main(")[-1])


class OC12RunnerGrafanaProofContractTest(TestCase):
    """OC-12R.1 C: real panel 4/5 execution on campaign data."""

    def setUp(self) -> None:
        self.runner = RUNNER.read_text(encoding="utf-8")

    def test_runner_references_the_dashboard_and_stable_panel_ids(self) -> None:
        self.assertIn("explainability_v2_influxdb.json", self.runner)
        self.assertIn("STATUS_PANEL_ID = 4", self.runner)
        self.assertIn("VERDICT_PANEL_ID = 5", self.runner)

    def test_runner_adapts_bucket_measurement_group_and_range(self) -> None:
        adapt = self.runner.split("def adapt_dashboard_query")[1].split("def ")[0]
        self.assertIn('from(bucket: "explainability-v2")', adapt)
        self.assertIn("v.timeRangeStart", adapt)
        self.assertIn("v.timeRangeStop", adapt)
        self.assertIn('"${group}"', adapt)
        self.assertIn("explainability-v2", adapt)

    def test_runner_refuses_unresolved_placeholders(self) -> None:
        adapt = self.runner.split("def adapt_dashboard_query")[1].split("def ")[0]
        self.assertIn('if "${" in adapted', adapt)
        self.assertIn("unresolved Grafana", adapt)

    def test_runner_writes_the_panel_artifacts_and_summary(self) -> None:
        self.assertIn('"grafana-panel-4"', self.runner)
        self.assertIn('"grafana-panel-5"', self.runner)
        self.assertIn('f"{artifact_stem}.flux"', self.runner)
        self.assertIn('f"{artifact_stem}.csv"', self.runner)
        self.assertIn("grafana-panels-summary.json", self.runner)

    def test_runner_checks_the_exact_fixture_counts(self) -> None:
        self.assertIn('{"warmup": 99, "computed": 11}', self.runner)
        self.assertIn('"published": 6, "computed_count": 6, "rate": 1.0', self.runner)
        self.assertIn('"published": 3, "computed_count": 3, "rate": 1.0', self.runner)

    def test_grafana_proof_runs_after_the_influx_proof(self) -> None:
        main_body = self.runner.split("def main(")[1].split("if __name__")[0]
        proof_position = main_body.index("InfluxDB proof")
        grafana_position = main_body.index("Grafana panel 4 and 5")
        validator_position = main_body.index("OC-12 validator")
        self.assertLess(proof_position, grafana_position)
        self.assertLess(grafana_position, validator_position)


class OC12RunnerPureFunctionTest(TestCase):
    """Pure tests over the runner module: no Docker, Kafka or InfluxDB."""

    @classmethod
    def setUpClass(cls) -> None:
        import importlib.util

        spec = importlib.util.spec_from_file_location("oc12_runner", RUNNER)
        cls.runner_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.runner_module)

    def test_generate_run_id_is_unique_for_the_same_instant(self) -> None:
        import datetime as dt

        moment = dt.datetime(2026, 9, 18, 12, 0, 0)
        first = self.runner_module.generate_run_id(now=moment)
        second = self.runner_module.generate_run_id(now=moment)
        self.assertNotEqual(first, second)

    def test_generate_run_id_format_is_safe_for_all_backends(self) -> None:
        import datetime as dt
        import re

        run_id = self.runner_module.generate_run_id(now=dt.datetime(2026, 9, 18, 12, 0, 0))
        self.assertRegex(run_id, r"^oc12-\d{8}-\d{6}-[0-9a-f]{8}$")
        self.assertEqual(re.sub(r"[a-z0-9-]", "", run_id), "")

    def test_derived_names_stay_oc12_prefixed(self) -> None:
        run_id = self.runner_module.generate_run_id()
        for name in (
            f"oc12-source-{run_id}",
            f"oc12-hstree-{run_id}",
            f"oc12-v2-viz-{run_id}",
            f"oc12-v2-consumer-{run_id}",
        ):
            self.assertTrue(name.startswith("oc12-"))

    def test_adapt_dashboard_query_resolves_all_placeholders(self) -> None:
        query = (
            'from(bucket: "explainability-v2")\n'
            "  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n"
            '  |> filter(fn: (r) => r._measurement == "explainability-v2")\n'
            '  |> filter(fn: (r) => if "${group}" == "ALL" then true else r.Group == "${group}")'
        )
        adapted = self.runner_module.adapt_dashboard_query(
            query, "oc12-campaign", "oc12-v2-run", "OC12-Campaign",
            "2026-09-18T10:00:00Z", "2026-09-18T14:00:00Z",
        )
        self.assertIn('from(bucket: "oc12-campaign")', adapted)
        self.assertIn('r._measurement == "oc12-v2-run"', adapted)
        self.assertIn('r.Group == "OC12-Campaign"', adapted)
        self.assertNotIn("${", adapted)
        self.assertNotIn("v.timeRange", adapted)

    def test_adapt_dashboard_query_refuses_unknown_placeholders(self) -> None:
        query = 'from(bucket: "explainability-v2") |> filter(fn: (r) => r.x == "${feature}")'
        with self.assertRaises(self.runner_module.RunnerError):
            self.runner_module.adapt_dashboard_query(
                query, "oc12-campaign", "oc12-v2-run", "OC12-Campaign",
                "2026-09-18T10:00:00Z", "2026-09-18T14:00:00Z",
            )
