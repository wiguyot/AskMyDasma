"""OC-15 contract tests: fixture/manifest coherence, compose and runner
safety contracts. Pure tests: no Docker, Kafka or InfluxDB."""

import datetime as dt
import hashlib
import importlib.util
import json
import re
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = PROJECT_ROOT / "tests" / "fixtures" / "explainability_v2"
OC15_CSV = FIXTURE_DIR / "oc15_validation_source.csv"
OC15_MANIFEST = FIXTURE_DIR / "oc15_validation_manifest.json"
OC15_COMPOSE = PROJECT_ROOT / "docker/compose/experiments/docker-compose.oc15.yml"
RUNNER = PROJECT_ROOT / "scripts" / "integration" / "run-explainability-v2-minimal-validation.py"
RUNNER_SH = PROJECT_ROOT / "scripts" / "integration" / "run-explainability-v2-minimal-validation.sh"
VALIDATOR = PROJECT_ROOT / "experiments" / "explainability" / "v2" / "validate_minimal_explainability.py"
SEED_STABILITY = PROJECT_ROOT / "experiments" / "explainability" / "v2" / "measure_shap_seed_stability.py"


def json_load(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


class OC15FixtureCoherenceTest(unittest.TestCase):
    """OC-15 A: fixture/manifest coherence."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.manifest = json_load(OC15_MANIFEST)
        lines = OC15_CSV.read_text(encoding="utf-8").strip().splitlines()
        cls.header = lines[0].split(",")
        cls.rows = [dict(zip(cls.header, line.split(","))) for line in lines[1:]]

    def test_csv_sha256_matches_the_manifest(self) -> None:
        self.assertEqual(
            hashlib.sha256(OC15_CSV.read_bytes()).hexdigest(),
            self.manifest["source_csv_sha256"],
        )

    def test_total_rows_match_the_manifest(self) -> None:
        self.assertEqual(len(self.rows), self.manifest["expected_total_events"])
        self.assertEqual(self.manifest["expected_total_events"], 220)

    def test_expected_keys_match_the_csv(self) -> None:
        keys = sorted(f"{row['Group']}|{row['source_event_id']}" for row in self.rows)
        self.assertEqual(sorted(self.manifest["expected_keys"]), keys)

    def test_source_ids_unique_and_timestamps_unique_per_group(self) -> None:
        ids = [row["source_event_id"] for row in self.rows]
        self.assertEqual(len(set(ids)), len(ids))
        for group in self.manifest["groups"]:
            timestamps = [row["timestamp"] for row in self.rows if row["Group"] == group]
            self.assertEqual(len(set(timestamps)), len(timestamps), group)

    def test_exactly_one_cross_group_timestamp_collision(self) -> None:
        pair = self.manifest["cross_group_timestamp_pair"]
        collisions = {}
        for row in self.rows:
            collisions.setdefault(row["timestamp"], set()).add(row["Group"])
        shared = {ts: groups for ts, groups in collisions.items() if len(groups) > 1}
        self.assertEqual(list(shared), [pair["timestamp"]])
        self.assertEqual(shared[pair["timestamp"]], set(self.manifest["groups"]))

    def test_two_independent_groups(self) -> None:
        groups = {row["Group"] for row in self.rows}
        self.assertEqual(groups, set(self.manifest["groups"]))
        self.assertEqual(self.manifest["group_count"], 2)

    def test_warmup_and_shap_counts(self) -> None:
        self.assertEqual(self.manifest["expected_warmup_events"], 198)
        self.assertEqual(self.manifest["expected_shap_events"], 22)
        self.assertEqual(len(self.manifest["warmup_keys"]), 198)
        self.assertEqual(len(self.manifest["shap_eligible_keys"]), 22)

    def test_expected_cases_are_declared_and_disjoint(self) -> None:
        verdicts = self.manifest["expected_verdicts"]
        anomaly = set(verdicts["anomaly"])
        normal = set(verdicts["normal"])
        threshold = set(verdicts["threshold_warmup"])
        self.assertEqual(len(anomaly), 5)   # alpha: 3, beta: 2
        self.assertEqual(len(normal), 13)   # alpha: 6, beta: 7 (109 is normal)
        self.assertEqual(len(threshold), 4)  # events 100-101 of both groups
        self.assertFalse(anomaly & normal)
        self.assertFalse(anomaly & threshold)
        self.assertFalse(normal & threshold)
        eligible = set(self.manifest["shap_eligible_keys"])
        self.assertEqual(anomaly | normal | threshold, eligible)

    def test_normals_and_known_outliers_present_in_csv(self) -> None:
        alpha = [row for row in self.rows if row["Group"] == "OC15-Alpha"]
        beta = [row for row in self.rows if row["Group"] == "OC15-Beta"]
        alpha_base = {(float(row["feat_a"]), float(row["feat_b"])) for row in alpha}
        beta_base = {(float(row["feat_a"]), float(row["feat_b"])) for row in beta}
        # Different baselines: the groups are statistically independent.
        self.assertIn((10.0, 20.0), alpha_base)
        self.assertIn((30.0, 5.0), beta_base)
        self.assertNotIn((10.0, 20.0), beta_base)
        self.assertTrue({value for value in alpha_base if value != (10.0, 20.0)})
        self.assertTrue({value for value in beta_base if value != (30.0, 5.0)})

    def test_manifest_declares_params_and_seeds(self) -> None:
        env = self.manifest["campaign_env"]
        self.assertEqual(env["HSTREE_INITIAL_WINDOW_SIZE"], "100")
        self.assertEqual(env["SHAP_BACKGROUND_SIZE"], "100")
        self.assertEqual(env["SHAP_NSAMPLES"], "100")  # production value
        self.assertEqual(env["EXPLAINABILITY_SEED"] if "EXPLAINABILITY_SEED" in env else "42", "42")
        self.assertEqual(self.manifest["reference_seed"], 42)
        self.assertEqual(self.manifest["secondary_seeds"], [43, 44])
        self.assertEqual(self.manifest["feature_count"], 2)


class OC15ComposeContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.text = OC15_COMPOSE.read_text(encoding="utf-8")

    def test_only_oc15_services(self) -> None:
        for service in ("oc15-producer", "oc15-hstree", "oc15-v2"):
            self.assertIn(f"  {service}:", self.text)
        self.assertNotIn("  oc12-", self.text)
        self.assertNotIn("  oc13-", self.text)
        self.assertNotIn("  oc14-", self.text)
        self.assertNotIn("telegraf:", self.text)
        self.assertNotIn("grafana:", self.text)
        self.assertNotIn("influxdb:", self.text)

    def test_resources_carry_seed_and_run_id(self) -> None:
        for template in (
            "oc15-s${OC15_SEED}-source-${OC15_RUN_ID}",
            "oc15-s${OC15_SEED}-hstree-${OC15_RUN_ID}",
            "oc15-s${OC15_SEED}-hstree-viz-${OC15_RUN_ID}",
            "oc15-s${OC15_SEED}-v2-${OC15_RUN_ID}",
            "oc15-s${OC15_SEED}-v2-viz-${OC15_RUN_ID}",
        ):
            self.assertIn(template, self.text)
        self.assertIn("container_name: oc15-producer-${OC15_RUN_ID}", self.text)
        self.assertIn("container_name: oc15-hstree-${OC15_RUN_ID}", self.text)
        self.assertIn("container_name: oc15-v2-${OC15_RUN_ID}", self.text)
        self.assertIn("oc15-s${OC15_SEED}-v2-consumer-${OC15_RUN_ID}", self.text)
        self.assertIn("oc15-s${OC15_SEED}-hstree-consumer-${OC15_RUN_ID}", self.text)

    def test_production_shap_nsamples_untouched_and_seed_is_the_variable(self) -> None:
        self.assertIn('SHAP_NSAMPLES: "100"', self.text)
        self.assertIn('EXPLAINABILITY_SEED: "${OC15_SEED}"', self.text)

    def test_no_destructive_primitive_and_no_standard_service(self) -> None:
        self.assertNotIn("--delete", self.text)
        self.assertNotIn("  producer:", self.text)
        self.assertNotIn("  explainability:", self.text)
        self.assertNotIn("  explainability-v2:", self.text)


class OC15RunnerContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.runner = RUNNER.read_text(encoding="utf-8")

    def test_runner_executes_the_seven_required_steps_in_order(self) -> None:
        main_body = self.runner.split("def main(")[1].split("if __name__")[0]
        positions = [
            main_body.index("reference_run_a = run_single_campaign("),
            main_body.index('log("validating each reference export individually (structural)")'),
            main_body.index('log("running the pure minimal validator on both reference exports")'),
            main_body.index("for seed in secondary_seeds:"),
            main_body.index('log("validating each secondary export individually (structural)")'),
            main_body.index('log("measuring the exploratory seed stability against the reference")'),
            main_body.index("render_minimal_validation_markdown("),
        ]
        self.assertEqual(positions, sorted(positions))

    def test_secondary_exports_are_validated_before_stability(self) -> None:
        main_body = self.runner.split("def main(")[1].split("if __name__")[0]
        self.assertIn("secondary_validations[f\"seed{seed}\"] = validate_export_or_fail(", main_body)
        self.assertLess(
            main_body.index("oc15-structural-secondary.json"),
            main_body.index('log("measuring the exploratory seed stability against the reference")'),
        )

    def test_invalid_secondary_export_fails_explicitly_before_stability(self) -> None:
        helper = self.runner.split("def validate_export_or_fail")[1].split("def render_minimal_validation_markdown")[0]
        self.assertIn("is structurally INVALID", helper)
        self.assertIn("raise RunnerError", helper)

    def test_validator_failure_fails_the_campaign(self) -> None:
        block = self.runner.split("log(\"running the pure minimal validator on both reference exports\")")[1][:2500]
        self.assertIn("returncode != 0", block)
        self.assertIn("minimal validation FAILED", block)

    def test_both_reference_runs_use_the_reference_seed(self) -> None:
        main_body = self.runner.split("def main(")[1].split("if __name__")[0]
        self.assertIn("reference_seed = int(manifest[\"reference_seed\"])", main_body)
        self.assertIn("reference_seed, manifest, artifacts, deadline, args.keep_up", main_body)

    def test_safety_guarantees(self) -> None:
        block = self.runner.split("STANDARD_COMPUTE_SERVICES = [")[1].split("]")[0]
        for service in ("producer", "hstree-analysis", "explainability-v2", "explainability", "telegraf"):
            self.assertIn(f'"{service}"', block)
        main_body = self.runner.split("def main(")[1].split("if __name__")[0]
        self.assertIn("assert_standard_services_stopped()", main_body)
        self.assertIn("--keep-up", self.runner)
        self.assertIn("--global-timeout", self.runner)
        self.assertIn("finally:", main_body)

    def test_failure_flow_logs_diagnostics_before_cleanup(self) -> None:
        main_body = self.runner.split("def main(")[1].split("if __name__")[0]
        main_finally = main_body.split("finally:")[-1]
        self.assertLess(
            main_finally.index("dump_service_logs"),
            main_finally.index("stop_oc15_services"),
        )

    def test_only_oc15_topics_created_and_nothing_deleted(self) -> None:
        create_block = self.runner.split("def create_oc15_topics")[1].split("def ")[0]
        self.assertIn('"--create", "--if-not-exists"', create_block)
        self.assertNotIn("kafka-configs.sh", self.runner)
        self.assertNotIn("/api/v2/delete", self.runner)
        self.assertNotIn(" --delete", self.runner)
        # The runner never runs `docker compose down`: only stop/rm of the
        # dedicated oc15-* services.
        self.assertIsNone(re.search(r'compose\(\s*"down"', self.runner))

    def test_cleanup_targets_only_oc15_services(self) -> None:
        helper = self.runner.split("def stop_oc15_services")[1].split("def ")[0]
        self.assertIn('compose("stop", *OC15_SERVICES', helper)
        self.assertIn('compose("rm", "-f", *OC15_SERVICES', helper)
        self.assertNotIn("STANDARD_COMPUTE_SERVICES", helper)

    def test_no_docker_execution_at_module_level(self) -> None:
        module_level = self.runner.split("class RunnerError")[0]
        self.assertNotIn("subprocess.run(", module_level)
        self.assertIn('if __name__ == "__main__":', self.runner)

    def test_mandatory_artifacts(self) -> None:
        for artifact in (
            "campaign-env.json",
            "oc15-v2-viz.jsonl",
            "oc15-minimal-validation.json",
            "oc15-seed-stability.json",
            "oc15-summary.json",
            "services-failure-logs.txt",
        ):
            self.assertIn(artifact, self.runner)
        self.assertIn('REPORTS_DIR / f"oc15-minimal-validation-{campaign_run_id}.md"', self.runner)

    def test_versioned_report_only_written_by_the_runner(self) -> None:
        # The pure validator never writes the versioned Markdown: only the
        # runner does, after a real successful campaign.
        self.assertNotIn(".md", VALIDATOR.read_text(encoding="utf-8").replace("Markdown", "").replace("markdown", ""))
        self.assertIn("versioned", self.runner)


class OC15RunnerPureFunctionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        spec = importlib.util.spec_from_file_location("oc15_runner", RUNNER)
        cls.runner_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.runner_module)

    def test_generate_run_id_is_unique_for_the_same_instant(self) -> None:
        moment = dt.datetime(2026, 9, 19, 9, 0, 0)
        first = self.runner_module.generate_run_id(now=moment)
        second = self.runner_module.generate_run_id(now=moment)
        self.assertNotEqual(first, second)
        self.assertRegex(first, r"^oc15-\d{8}-\d{6}-[0-9a-f]{8}$")

    def test_render_markdown_is_deterministic_and_complete(self) -> None:
        manifest = json_load(OC15_MANIFEST)
        valid_structure = {
            "valid": True,
            "error_count": 0,
            "duplicates": [],
            "coverage": {"observed_events": 220, "expected_total": 220},
        }
        validation = {
            "verdict": "minimal_validation_passed",
            "error_count": 0,
            "errors_by_key": {},
            "structural_validation": {"run_a": valid_structure, "run_b": valid_structure},
            "configuration": {
                "numeric_tolerances": {"absolute_tolerance": 1e-12, "relative_tolerance": 1e-12},
                "additivity_tolerance": {"absolute_tolerance": 1e-6, "relative_tolerance": 1e-9},
            },
            "status_counts": {"run_a": {"warmup": 198, "computed": 22}},
            "verdict_counts": {"run_a": {"threshold_warmup": 200, "anomaly": 5, "normal": 13}},
        }
        stability = {
            "disclaimer": "Ces mesures exploratoires ne constituent PAS un seuil d'acceptation.",
            "seeds": {
                "43": {
                    "aggregate": {
                        "computed_events_compared": 22,
                        "spearman_mean": 1.0,
                        "jaccard_at_1_mean": 1.0,
                        "jaccard_at_2_mean": 1.0,
                        "sign_flip_rate_top1_mean": 0.0,
                        "relative_mean_abs_diff_mean": 0.05,
                        "identity_status_verdict_all_conserved": True,
                    }
                }
            },
        }
        secondary_validations = {
            "seed43": {
                "valid": True,
                "error_count": 0,
                "duplicates": [],
                "coverage": {"observed_events": 220, "expected_total": 220},
            }
        }
        first = self.runner_module.render_minimal_validation_markdown(
            "oc15-20260919-090000-abcdef01", manifest, validation, stability,
            {"run_a": "seed42-x", "run_b": "seed42-y"}, secondary_validations,
        )
        second = self.runner_module.render_minimal_validation_markdown(
            "oc15-20260919-090000-abcdef01", manifest, validation, stability,
            {"run_a": "seed42-x", "run_b": "seed42-y"}, secondary_validations,
        )
        self.assertEqual(first, second)
        self.assertIn("minimal_validation_passed", first)
        # The three OC-15R4 sections are clearly separated.
        self.assertIn("## 1. Validation sémantique et structurelle", first)
        self.assertIn("## 2. Reproductibilité même seed", first)
        self.assertIn("## 3. Stabilité inter-seed", first)
        self.assertIn("additivité SHAP", first)
        self.assertIn("ne sont PAS comparées", first)
        self.assertIn("seuil d'acceptation", first)
        self.assertIn("Aucune optimisation de KernelSHAP", first)

    def test_render_markdown_lists_errors_when_failed(self) -> None:
        manifest = json_load(OC15_MANIFEST)
        structure = {
            "valid": False,
            "error_count": 0,
            "duplicates": [],
            "coverage": {"observed_events": 220, "expected_total": 220},
        }
        validation = {
            "verdict": "minimal_validation_failed",
            "error_count": 2,
            "errors_by_key": {
                "OC15-Alpha|oc15-alpha-105": [
                    {"code": "shap_value_reproducibility_mismatch", "feature": "feat_a"}
                ]
            },
            "structural_validation": {"run_a": structure, "run_b": structure},
            "configuration": {"numeric_tolerances": {"absolute_tolerance": 1e-12, "relative_tolerance": 1e-12}, "additivity_tolerance": {"absolute_tolerance": 1e-6, "relative_tolerance": 1e-9}},
            "status_counts": {"run_a": {}},
            "verdict_counts": {"run_a": {}},
        }
        markdown = self.runner_module.render_minimal_validation_markdown(
            "oc15-20260919-090000-abcdef01", manifest, validation, None, {}, None
        )
        self.assertIn("minimal_validation_failed", markdown)
        self.assertIn("shap_value_reproducibility_mismatch", markdown)


class OC15WrapperAndModuleTest(unittest.TestCase):
    def test_wrapper_is_a_thin_launcher(self) -> None:
        wrapper = RUNNER_SH.read_text(encoding="utf-8")
        self.assertIn("run-explainability-v2-minimal-validation.py", wrapper)
        self.assertIn("PYTHONPATH=src:.", wrapper)

    def test_validator_and_stability_modules_are_pure_and_cli_ready(self) -> None:
        validator = VALIDATOR.read_text(encoding="utf-8")
        stability = SEED_STABILITY.read_text(encoding="utf-8")
        for text in (validator, stability):
            self.assertIn('if __name__ == "__main__":', text)
        self.assertIn("minimal_validation_passed", validator)
        self.assertIn("minimal_validation_failed", validator)
        self.assertIn("requiert", stability)

    def test_stability_module_reuses_the_existing_metrics(self) -> None:
        stability = SEED_STABILITY.read_text(encoding="utf-8")
        self.assertIn("from dasma_kafka.explainability_v2 import stability as S", stability)
        self.assertIn("S.spearman(", stability)
        self.assertIn("S.jaccard_at_k(", stability)
        self.assertIn("S.sign_flip_rate(", stability)
        self.assertIn("S.relative_mean_abs_diff(", stability)


if __name__ == "__main__":
    unittest.main()
