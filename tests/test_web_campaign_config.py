"""Tests for the web campaign JSON -> Docker env translator."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from runtime.detectors.hstree.hstree_analysis import normalize_hstree_threshold_mode
from scripts.web.campaign_config import (
    DEFAULTS,
    build_env,
    compose_services,
    load_campaign,
    load_campaign_dict,
)


class CampaignConfigTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _write_config(self, payload: dict) -> Path:
        path = Path(self.temp_dir.name) / "campaign.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def test_minimal_structured_json_generates_expected_exports(self) -> None:
        path = self._write_config(
            {
                "dataset_path": "data/source.csv",
                "mode": "realtime-x20",
                "selected_groups": ["1A", "NETBK"],
                "excluded_fields": ["timestamp", "Cycle"],
                "selected_models": ["hstree", "deepant"],
                "models": {
                    "deepant": {
                        "estimator": "welford",
                        "threshold_window": 750,
                        "batch_size": 16,
                        "max_group_workers": 12,
                    }
                },
            }
        )

        config = load_campaign(path)
        env = build_env(config, wait_for_start=True)

        self.assertEqual(env["DATA_SOURCE_FILE"], "data/source.csv")
        self.assertEqual(env["DASMA_PRODUCER_TRANSFORMATION"], "realtime-x20")
        self.assertEqual(env["DASMA_SELECTED_GROUPS"], "1A,NETBK")
        self.assertEqual(env["DASMA_EXCLUDED_FIELDS"], "timestamp,Cycle")
        self.assertEqual(env["DASMA_DEEPANT_ENABLED"], "1")
        self.assertEqual(
            env["DASMA_COMPOSE_SERVICES"],
            "hstree-analysis deepant-analysis consensus-analysis build-metrics telegraf",
        )
        self.assertEqual(env["DEEPANT_THRESHOLD_MODE"], "welford")
        self.assertEqual(env["DEEPANT_THRESHOLD_WINDOW"], "750")
        self.assertEqual(env["DEEPANT_BATCH_SIZE"], "16")
        self.assertEqual(env["DEEPANT_MAX_GROUP_WORKERS"], "12")
        self.assertEqual(env["DASMA_PRODUCER_WAIT_FOR_START"], "1")

    def test_hstree_reference_window_defaults_to_256(self) -> None:
        from src.dasma_kafka.web.models import CampaignLaunchRequest, CampaignView

        path = self._write_config({"dataset_path": "data/source.csv", "selected_models": ["hstree"]})
        config = load_campaign(path)
        env = build_env(config)

        self.assertEqual(DEFAULTS["hstree_initial_window_size"], 256)
        self.assertEqual(DEFAULTS["hstree_window_size"], 256)
        self.assertEqual(env["HSTREE_INITIAL_WINDOW_SIZE"], "256")
        self.assertEqual(env["HSTREE_WINDOW_SIZE"], "256")
        request = CampaignLaunchRequest(dataset_path="data/source.csv", mode="batch")
        view = CampaignView(dataset_path="data/source.csv", mode="batch", status="configuring")
        self.assertEqual((request.hstree_initial_window_size, request.hstree_window_size), (256, 256))
        self.assertEqual((view.hstree_initial_window_size, view.hstree_window_size), (256, 256))

    def test_hstree_window_defaults_match_frontend_fallbacks(self) -> None:
        root = Path(__file__).resolve().parents[1]
        html = (root / "frontend" / "askmydasma" / "index.html").read_text(encoding="utf-8")
        javascript = (root / "frontend" / "askmydasma" / "app-user.js").read_text(encoding="utf-8")

        self.assertIn('id="hstree-initial-window-size" name="hstree_initial_window_size" value="256"', html)
        self.assertIn('id="hstree-window-size" name="hstree_window_size" value="256"', html)
        self.assertIn('id="hstree-initial-window-size" name="hstree_initial_window_size" value="256" min="1" max="10000" step="1" readonly', html)
        self.assertIn("Initial window (matches Window size)", html)
        self.assertIn("function syncHstreeInitialWindowSize()", javascript)
        self.assertIn("syncHstreeInitialWindowSize();", javascript)
        self.assertIn('hstree_initial_window_size: parseIntParam(els.hstreeInitialWindowSize?.value || "256", 256, 1)', javascript)
        self.assertIn('hstree_window_size: parseIntParam(els.hstreeWindowSize?.value || "256", 256, 64)', javascript)
        self.assertIn("sliding K×σ baseline window (default: 2048 scores)", html)

    def test_iforest_paper_defaults_match_web_frontend_and_compose(self) -> None:
        from src.dasma_kafka.web.models import CampaignLaunchRequest, CampaignView

        root = Path(__file__).resolve().parents[1]
        html = (root / "frontend" / "askmydasma" / "index.html").read_text(encoding="utf-8")
        javascript = (root / "frontend" / "askmydasma" / "app-user.js").read_text(encoding="utf-8")
        compose = (root / "docker-compose.yml").read_text(encoding="utf-8")
        campaign = load_campaign(self._write_config({"dataset_path": "data/source.csv"}))
        env = build_env(campaign)

        self.assertEqual((DEFAULTS["iforest_initial_window_size"], DEFAULTS["iforest_n_estimators"],
                          DEFAULTS["iforest_max_samples"]), (512, 100, "256"))
        for model in (
            CampaignLaunchRequest(dataset_path="data/source.csv", mode="batch"),
            CampaignView(dataset_path="data/source.csv", mode="batch", status="configuring"),
        ):
            self.assertEqual((model.iforest_initial_window_size, model.iforest_n_estimators,
                              model.iforest_max_samples), (512, 100, "256"))
        self.assertEqual(env["IFOREST_INITIAL_WINDOW_SIZE"], "512")
        self.assertEqual(env["IFOREST_N_ESTIMATORS"], "100")
        self.assertEqual(env["IFOREST_MAX_SAMPLES"], "256")
        self.assertEqual(env["IFOREST_CONTAMINATION"], "auto")
        self.assertEqual(compose.count('IFOREST_INITIAL_WINDOW_SIZE: "${IFOREST_INITIAL_WINDOW_SIZE:-512}"'), 5)
        self.assertEqual(compose.count('IFOREST_N_ESTIMATORS: "${IFOREST_N_ESTIMATORS:-100}"'), 5)
        self.assertEqual(compose.count('IFOREST_MAX_SAMPLES: "${IFOREST_MAX_SAMPLES:-256}"'), 5)
        self.assertEqual(compose.count('IFOREST_CONTAMINATION: "${IFOREST_CONTAMINATION:-auto}"'), 5)
        self.assertIn('id="iforest-max-samples" name="iforest_max_samples" value="256" min="2"', html)
        self.assertIn('id="iforest-contamination"', html)
        self.assertIn('iforest_initial_window_size: parseIntParam(els.iforestInitialWindowSize?.value || "512", 512, 1)', javascript)
        self.assertIn("iforestContamination", javascript)

    def test_hstree_tree_count_defaults_match_frontend(self) -> None:
        from src.dasma_kafka.web.models import CampaignLaunchRequest, CampaignView

        root = Path(__file__).resolve().parents[1]
        html = (root / "frontend" / "askmydasma" / "index.html").read_text(encoding="utf-8")
        javascript = (root / "frontend" / "askmydasma" / "app-user.js").read_text(encoding="utf-8")
        compose = (root / "docker-compose.yml").read_text(encoding="utf-8")
        parity_compose = (root / "docker" / "compose" / "experiments" / "docker-compose.v1v2.yml").read_text(encoding="utf-8")
        config = load_campaign(self._write_config({"dataset_path": "data/source.csv"}))
        env = build_env(config)

        self.assertEqual(DEFAULTS["hstree_num_trees"], 25)
        self.assertEqual((CampaignLaunchRequest(dataset_path="data/source.csv", mode="batch").hstree_num_trees,
                          CampaignView(dataset_path="data/source.csv", mode="batch", status="configuring").hstree_num_trees),
                         (25, 25))
        self.assertEqual(env["HSTREE_NUM_TREES"], "25")
        self.assertIn('id="hstree-num-trees" name="hstree_num_trees" value="25"', html)
        self.assertIn("paper default 25", html)
        self.assertIn('els.hstreeNumTrees?.value || "25", 25', javascript)
        self.assertEqual(compose.count('HSTREE_NUM_TREES: "${HSTREE_NUM_TREES:-25}"'), 5)
        self.assertEqual(parity_compose.count('HSTREE_NUM_TREES: "${V1V2_HSTREE_NUM_TREES:-25}"'), 3)

    def test_hstree_variant_contract_defaults_and_rejects_unknown(self) -> None:
        from src.dasma_kafka.web.models import CampaignLaunchRequest, CampaignView

        defaults = load_campaign(self._write_config({"dataset_path": "data/source.csv"}))
        self.assertEqual(defaults["hstree_variant"], "paper")
        self.assertEqual(build_env(defaults)["HSTREE_VARIANT"], "paper")
        self.assertEqual(CampaignLaunchRequest(dataset_path="data/source.csv", mode="batch").hstree_variant,
                         "paper")
        self.assertEqual(CampaignView(dataset_path="data/source.csv", mode="batch", status="configuring").hstree_variant,
                         "paper")
        with self.assertRaisesRegex(
            ValueError,
            "hstree: variante inconnue 'invalid' \\(valeurs : pysad, paper\\)",
        ):
            load_campaign(self._write_config({
                "dataset_path": "data/source.csv", "hstree_variant": "invalid"
            }))

        root = Path(__file__).resolve().parents[1]
        html = (root / "frontend" / "askmydasma" / "index.html").read_text(encoding="utf-8")
        javascript = (root / "frontend" / "askmydasma" / "app-user.js").read_text(encoding="utf-8")
        self.assertIn('id="hstree-variant" name="hstree_variant"', html)
        self.assertIn('value="pysad">PySAD', html)
        self.assertIn('value="paper" selected>Paper (Tan et al., 2011)', html)
        self.assertIn('hstree_variant: els.hstreeVariant?.value || "paper"', javascript)
        self.assertIn("campaign.hstree_variant", javascript)
        compose = (root / "docker-compose.yml").read_text(encoding="utf-8")
        parity_compose = (root / "docker" / "compose" / "experiments" / "docker-compose.v1v2.yml").read_text(
            encoding="utf-8"
        )
        self.assertEqual(compose.count('HSTREE_VARIANT: "${HSTREE_VARIANT:-paper}"'), 5)
        self.assertEqual(parity_compose.count('HSTREE_VARIANT: "${V1V2_HSTREE_VARIANT:-paper}"'), 3)

    def test_p99_is_only_the_hstree_threshold_default(self) -> None:
        from src.dasma_kafka.web.models import CampaignLaunchRequest, CampaignView

        path = self._write_config({"dataset_path": "data/source.csv", "selected_models": ["hstree"]})
        config = load_campaign(path)
        env = build_env(config)
        self.assertEqual(DEFAULTS["hstree_estimator"], "p99")
        self.assertEqual(config["hstree_estimator"], "p99")
        self.assertEqual(env["HSTREE_THRESHOLD_MODE"], "p99")
        self.assertEqual(CampaignLaunchRequest(dataset_path="data/source.csv", mode="batch").hstree_estimator, "p99")
        self.assertEqual(
            CampaignView(dataset_path="data/source.csv", mode="batch", status="configuring").hstree_estimator,
            "p99",
        )
        self.assertEqual(DEFAULTS["iforest_estimator"], "native")
        for model in ("kitnet", "deepant"):
            self.assertEqual(DEFAULTS[f"{model}_estimator"], "welford-windowed")

        invalid_path = self._write_config(
            {"dataset_path": "data/source.csv", "selected_models": ["hstree"], "hstree_estimator": "invalid"}
        )
        self.assertEqual(load_campaign(invalid_path)["hstree_estimator"], "p99")
        explicit_path = self._write_config(
            {
                "dataset_path": "data/source.csv",
                "selected_models": ["hstree"],
                "hstree_estimator": "welford-windowed",
            }
        )
        self.assertEqual(load_campaign(explicit_path)["hstree_estimator"], "welford-windowed")

        root = Path(__file__).resolve().parents[1]
        html = (root / "frontend" / "askmydasma" / "index.html").read_text(encoding="utf-8")
        javascript = (root / "frontend" / "askmydasma" / "app-user.js").read_text(encoding="utf-8")
        self.assertIn('<option value="p99" selected>P99 sliding window</option>', html)
        self.assertRegex(
            html,
            r'<div\b(?=[^>]*\bid="hstree-welford-params")'
            r'(?=[^>]*\bclass="[^"]*\bis-hidden\b)[^>]*>',
        )
        self.assertIn('id="hstree-p99-params"', html)
        self.assertIn('hstree_estimator: els.hstreeEstimator?.value || "p99"', javascript)

    def test_iforest_native_is_default_and_external_modes_remain_supported(self) -> None:
        from src.dasma_kafka.web.models import CampaignLaunchRequest, CampaignView

        root = Path(__file__).resolve().parents[1]
        compose = (root / "docker-compose.yml").read_text(encoding="utf-8")
        html = (root / "frontend" / "askmydasma" / "index.html").read_text(encoding="utf-8")
        javascript = (root / "frontend" / "askmydasma" / "app-user.js").read_text(encoding="utf-8")
        defaults = load_campaign(self._write_config({"dataset_path": "data/source.csv"}))
        self.assertEqual(defaults["iforest_estimator"], "native")
        self.assertEqual(build_env(defaults)["IFOREST_THRESHOLD_MODE"], "native")
        self.assertEqual(CampaignLaunchRequest(dataset_path="data/source.csv", mode="batch").iforest_estimator, "native")
        self.assertEqual(CampaignView(dataset_path="data/source.csv", mode="batch", status="configuring").iforest_estimator, "native")
        self.assertEqual(compose.count('IFOREST_THRESHOLD_MODE: "${IFOREST_THRESHOLD_MODE:-native}"'), 3)
        self.assertIn('IFOREST_THRESHOLD_MODE", "native"', (root / "runtime/detectors/iforest/iforest_analysis.py").read_text(encoding="utf-8"))
        self.assertIn('<option value="native" selected>Native (Isolation Forest)</option>', html)
        self.assertIn('iforest_estimator: els.iforestEstimator?.value || "native"', javascript)

        for estimator in ("native", "p99", "welford", "welford-windowed"):
            explicit = load_campaign(self._write_config({
                "dataset_path": "data/source.csv", "iforest_estimator": estimator,
            }))
            self.assertEqual(explicit["iforest_estimator"], estimator)
            self.assertEqual(build_env(explicit)["IFOREST_THRESHOLD_MODE"], estimator)

    def test_iforest_campaign_contract_rejects_invalid_psi_windows_and_contamination(self) -> None:
        common = {"dataset_path": "data/source.csv", "selected_models": ["iforest"]}
        invalid_cases = (
            ({**common, "iforest_max_samples": "auto"},
             "iforest: max_samples doit être un entier ψ ≥ 2 (reçu : 'auto') : ψ doit rester constant d'une forêt à l'autre, comme dans le papier"),
            ({**common, "iforest_max_samples": "0.5"},
             "iforest: max_samples doit être un entier ψ ≥ 2 (reçu : '0.5') : ψ doit rester constant d'une forêt à l'autre, comme dans le papier"),
            ({**common, "iforest_initial_window_size": 128, "iforest_max_samples": "256"},
             "iforest: la fenêtre d'amorçage (128) doit être au moins égale à la taille de sous-échantillon ψ (256) : sinon scikit-learn réduit ψ et le sous-échantillonnage du papier disparaît"),
            ({**common, "iforest_initial_window_size": 512, "iforest_window_size": 256},
             "iforest: la fenêtre glissante (256) doit être au moins égale à la fenêtre d'amorçage (512)"),
            ({**common, "iforest_contamination": "0.51"},
             "iforest: contamination doit être 'auto' ou une fraction de 0.01 à 0.50 par pas de 0.01 (reçu : '0.51')"),
        )
        for payload, expected_message in invalid_cases:
            with self.subTest(payload=payload):
                with self.assertRaises(ValueError) as raised:
                    load_campaign_dict(payload)
                self.assertEqual(str(raised.exception), expected_message)

        valid = load_campaign_dict({
            **common,
            "iforest_initial_window_size": 512,
            "iforest_window_size": 2048,
            "iforest_max_samples": "256",
            "iforest_n_estimators": 100,
        })
        self.assertEqual(valid["iforest_max_samples"], 256)

    def test_iforest_constraints_do_not_apply_when_model_is_not_selected(self) -> None:
        config = load_campaign_dict({
            "dataset_path": "data/source.csv",
            "selected_models": ["kitnet"],
            "iforest_initial_window_size": 128,
            "iforest_max_samples": "auto",
            "iforest_contamination": "0.1",
        })
        self.assertEqual(config["iforest_initial_window_size"], 128)

    def test_explainability_iforest_selection_activates_iforest_constraints(self) -> None:
        payload = {
            "dataset_path": "data/source.csv",
            "selected_models": ["explainability"],
            "explainability_model": "iforest",
            "explainability_mode": "simple",
            "explainability_method": "kernel",
            "iforest_max_samples": "auto",
        }
        with self.assertRaisesRegex(ValueError, "max_samples doit être un entier"):
            load_campaign_dict(payload)

    def test_hstree_reference_window_must_match_when_hstree_is_used(self) -> None:
        base = {"dataset_path": "data/source.csv", "hstree_initial_window_size": 128, "hstree_window_size": 2048}
        invalid_cases = (
            {**base, "selected_models": ["hstree"]},
            {
                **base,
                "selected_models": ["explainability"],
                "explainability_model": "hstree",
                "explainability_mode": "simple",
                "explainability_method": "kernel",
            },
            {
                **base,
                "selected_models": ["explainability-v2"],
                "explainability_v2_model": "hstree",
                "explainability_v2_mode": "simple",
                "explainability_v2_method": "kernel",
            },
        )
        message = (
            r"hstree: la fenêtre d'amorçage \(128\) doit être égale à la fenêtre du modèle "
            r"\(2048\) : fenêtre de référence du papier"
        )
        for invalid in invalid_cases:
            with self.subTest(selected_models=invalid["selected_models"]):
                with self.assertRaisesRegex(ValueError, message):
                    load_campaign_dict(invalid)

    def test_unselected_hstree_window_settings_do_not_block_other_models(self) -> None:
        config = load_campaign_dict(
            {
                "dataset_path": "data/source.csv",
                "selected_models": ["iforest"],
                "hstree_initial_window_size": 128,
                "hstree_window_size": 2048,
            }
        )
        self.assertEqual(config["hstree_initial_window_size"], 128)
        self.assertEqual(config["hstree_window_size"], 2048)

    def test_explainability_v2_requires_explicit_selection(self) -> None:
        without_hstree = self._write_config(
            {
                "dataset_path": "data/source.csv",
                "selected_models": ["explainability-v2"],
                "explainability": {"v2_nsamples": 2000, "v2_model": "hstree", "v2_mode": "simple", "v2_method": "kernel"},
            }
        )
        env = build_env(load_campaign(without_hstree))
        self.assertEqual(env["DASMA_EXPLAINABILITY_V2_ENABLED"], "1")
        self.assertEqual(env["EXPLAINABILITY_V2_SHAP_NSAMPLES"], "2000")
        self.assertIn("explainability-v2", compose_services(load_campaign(without_hstree)))

        with_hstree = self._write_config(
            {
                "dataset_path": "data/source.csv",
                "selected_models": ["hstree", "explainability-v2"],
                "explainability": {"v2_model": "hstree", "v2_mode": "reference", "v2_method": "kernel"},
            }
        )
        with_hstree_config = load_campaign(with_hstree)
        self.assertEqual(build_env(with_hstree_config)["DASMA_EXPLAINABILITY_V2_ENABLED"], "1")
        self.assertIn("explainability-v2", compose_services(with_hstree_config))

        kitnet_simple = load_campaign(self._write_config({
            "dataset_path": "data/source.csv",
            "selected_models": ["explainability-v2"],
            "explainability": {"v2_model": "kitnet", "v2_mode": "simple", "v2_method": "kernel"},
        }))
        self.assertEqual(build_env(kitnet_simple)["EXPLAINABILITY_V2_MODEL"], "kitnet")
        self.assertIn("explainability-v2", compose_services(kitnet_simple))

        kitnet_reference = load_campaign(self._write_config({
            "dataset_path": "data/source.csv",
            "selected_models": ["kitnet", "explainability-v2"],
            "explainability": {"v2_model": "kitnet", "v2_mode": "reference", "v2_method": "kernel"},
        }))
        self.assertEqual(build_env(kitnet_reference)["EXPLAINABILITY_V2_SOURCE_TOPIC"], "kitnet-analysis")

        with self.assertRaisesRegex(ValueError, "que pour le modèle 'iforest'"):
            load_campaign(self._write_config({
                "dataset_path": "data/source.csv",
                "selected_models": ["explainability"],
                "explainability": {"model": "kitnet", "mode": "simple", "method": "tree"},
            }))

    def test_shap_diff_starts_only_when_v1_and_v2_are_selected(self) -> None:
        both = load_campaign(self._write_config({
            "dataset_path": "data/source.csv",
            "selected_models": ["hstree", "explainability", "explainability-v2"],
            "explainability": {"model": "hstree", "mode": "reference", "method": "kernel", "v2_model": "hstree", "v2_mode": "reference", "v2_method": "kernel"},
        }))
        services = compose_services(both)
        self.assertIn("explainability-shap-diff", services)
        self.assertEqual(build_env(both)["DASMA_SHAP_DIFF_ENABLED"], "1")

        v2_only = load_campaign(self._write_config({
            "dataset_path": "data/source.csv",
            "selected_models": ["hstree", "explainability-v2"],
            "explainability": {"v2_model": "hstree", "v2_mode": "reference", "v2_method": "kernel"},
        }))
        self.assertNotIn("explainability-shap-diff", compose_services(v2_only))
        self.assertEqual(build_env(v2_only)["DASMA_SHAP_DIFF_ENABLED"], "0")

    def test_build_env_emits_neutral_repeatability_values(self) -> None:
        """The repeatability compose interpolations must resolve deterministically.

        Until step 5 wires the per-model repeatability option, build_env emits
        neutral structural values matching the spec defaults so no new compose
        interpolation is silently ignored (bilateral compose/build_env contract).
        """
        path = self._write_config(
            {
                "dataset_path": "data/source.csv",
                "selected_models": ["hstree"],
            }
        )
        env = build_env(load_campaign(path))

        self.assertEqual(env["DASMA_REPEATABILITY_ENABLED"], "0")
        self.assertEqual(env["REPEATABILITY_IMAGE"], "dasma-kafka-anomaly-light:0.1.0")
        self.assertEqual(env["REPEATABILITY_SCRIPT"], "runtime/detectors/hstree/hstree_analysis.py")
        self.assertEqual(env["REPEATABILITY_MODEL_ID"], "hstree")
        self.assertEqual(env["REPEATABILITY_SCORE_FIELD"], "hstree_score")
        self.assertEqual(env["REPEATABILITY_STATUS_FIELD"], "hstree_status")
        self.assertEqual(env["REPEATABILITY_THRESHOLD_FIELD"], "hstree_threshold")
        self.assertEqual(env["REPEATABILITY_ANOMALY_FIELD"], "hstree_anomaly")
        self.assertEqual(env["REPEATABILITY_EXTRA_FIELDS"], "")
        self.assertEqual(env["REPEATABILITY_TIMEOUT_SEC"], "30")
        self.assertEqual(env["REPEATABILITY_RETENTION_SEC"], "300")
        self.assertEqual(env["ANALYSE_SEED"], "42")
        self.assertEqual(env["REPEATABILITY_VIZ_TOPIC_A"], "repeatability-v1-viz")
        self.assertEqual(env["REPEATABILITY_VIZ_TOPIC_B"], "repeatability-v2-viz")
        self.assertEqual(env["REPEATABILITY_DEST_TOPIC"], "repeatability-diff")
        self.assertEqual(env["REPEATABILITY_GROUP_ID"], "repeatability-diff-consumer")

    def test_repeatability_flags_default_false_in_defaults_and_models(self) -> None:
        from scripts.web.campaign_config import DEFAULTS
        from src.dasma_kafka.web.models import CampaignLaunchRequest, CampaignView

        for model in ("hstree", "iforest", "kitnet", "deepant", "mdragpoint"):
            key = f"{model}_repeatability"
            self.assertIn(key, DEFAULTS)
            self.assertIs(DEFAULTS[key], False)

        request = CampaignLaunchRequest(dataset_path="data/source.csv", mode="batch")
        for model in ("hstree", "iforest", "kitnet", "deepant", "mdragpoint"):
            self.assertIs(getattr(request, f"{model}_repeatability"), False)

        view = CampaignView(
            dataset_path="data/source.csv",
            mode="batch",
            status="configuring",
        )
        for model in ("hstree", "iforest", "kitnet", "deepant", "mdragpoint"):
            self.assertIs(getattr(view, f"{model}_repeatability"), False)

    def test_structured_cycle_preserves_repeatability_flag(self) -> None:
        """models.<m>.repeatability must survive flatten -> load_campaign -> env."""
        path = self._write_config(
            {
                "dataset_path": "data/source.csv",
                "mode": "batch",
                "selected_models": ["hstree"],
                "models": {
                    "hstree": {
                        "num_trees": 75,
                        "repeatability": True,
                    },
                },
            }
        )
        config = load_campaign(path)
        self.assertTrue(config["hstree_repeatability"])

        env = build_env(config)
        self.assertEqual(env["DASMA_REPEATABILITY_ENABLED"], "1")
        self.assertEqual(env["REPEATABILITY_MODEL_ID"], "hstree")

    def test_repeatability_exclusive_two_flags_rejected(self) -> None:
        path = self._write_config(
            {
                "dataset_path": "data/source.csv",
                "selected_models": ["hstree", "iforest"],
                "models": {
                    "hstree": {"repeatability": True},
                    "iforest": {"repeatability": True},
                },
            }
        )
        with self.assertRaises(ValueError):
            load_campaign(path)

    def test_repeatability_model_not_selected_rejected(self) -> None:
        path = self._write_config(
            {
                "dataset_path": "data/source.csv",
                "selected_models": ["hstree"],
                "models": {"deepant": {"repeatability": True}},
            }
        )
        with self.assertRaises(ValueError):
            load_campaign(path)

    def test_hstree_repeatability_with_explainability_rejected(self) -> None:
        for extra in ("explainability", "explainability-v2"):
            path = self._write_config(
                {
                    "dataset_path": "data/source.csv",
                    "selected_models": ["hstree", extra],
                    "models": {"hstree": {"repeatability": True}},
                }
            )
            with self.assertRaises(ValueError):
                load_campaign(path)

    def test_repeatability_with_duplicated_selected_model_rejected(self) -> None:
        """selected_models must be literally [M]: a duplicated entry is rejected."""
        path = self._write_config(
            {
                "dataset_path": "data/source.csv",
                "selected_models": ["hstree", "hstree"],
                "models": {"hstree": {"repeatability": True}},
            }
        )
        with self.assertRaises(ValueError) as ctx:
            load_campaign(path)
        self.assertIn("exclusive", str(ctx.exception))
        self.assertIn("duplicated", str(ctx.exception))

    def test_repeatability_selects_exactly_one_model(self) -> None:
        """For each M: [M] + flag is valid with only the trio and core services."""
        from scripts.web.campaign_config import build_env, load_campaign

        expected_images = {
            "hstree": "dasma-kafka-anomaly-light:0.1.0",
            "iforest": "dasma-kafka-anomaly-sklearn:0.1.0",
            "kitnet": "dasma-kafka-anomaly-light:0.1.0",
            "deepant": "dasma-kafka-deepant:0.1.0",
            "mdragpoint": "dasma-kafka-anomaly-sklearn:0.1.0",
        }
        for model in ("hstree", "iforest", "kitnet", "deepant", "mdragpoint"):
            path = self._write_config(
                {
                    "dataset_path": "data/source.csv",
                    "selected_models": [model],
                    "models": {model: {"repeatability": True}},
                }
            )
            env = build_env(load_campaign(path))

            self.assertEqual(
                env["DASMA_COMPOSE_SERVICES"],
                "repeatability-v1 repeatability-v2 repeatability-diff "
                "consensus-analysis build-metrics telegraf",
                model,
            )
            self.assertEqual(env["DASMA_REPEATABILITY_ENABLED"], "1")
            self.assertEqual(env["REPEATABILITY_IMAGE"], expected_images[model])
            self.assertEqual(env[f"DASMA_{model.upper()}_ENABLED"], "0")
            self.assertEqual(env["DASMA_CONSENSUS_ENABLED"], "0")
            for other in ("hstree", "iforest", "kitnet", "deepant", "mdragpoint"):
                if other != model:
                    self.assertEqual(env[f"DASMA_{other.upper()}_ENABLED"], "0", model)
            for forbidden in ("hstree-analysis", "iforest-analysis", "kitnet-analysis", "deepant-analysis", "mdragstream-point-analysis"):
                self.assertNotIn(forbidden, env["DASMA_COMPOSE_SERVICES"], model)

    def test_repeatability_with_extra_model_rejected_for_every_model(self) -> None:
        models = ("hstree", "iforest", "kitnet", "deepant", "mdragpoint")
        for model in models:
            others = [other for other in models if other != model]
            for other in others:
                path = self._write_config(
                    {
                        "dataset_path": "data/source.csv",
                        "selected_models": [model, other],
                        "models": {model: {"repeatability": True}},
                    }
                )
                with self.assertRaises(ValueError, msg=f"{model}+{other}") as ctx:
                    load_campaign(path)
                self.assertIn("exclusive", str(ctx.exception))

    def test_repeatability_with_explainability_rejected_for_every_model(self) -> None:
        for model in ("hstree", "iforest", "kitnet", "deepant", "mdragpoint"):
            for extra in ("explainability", "explainability-v2"):
                path = self._write_config(
                    {
                        "dataset_path": "data/source.csv",
                        "selected_models": [model, extra],
                        "models": {model: {"repeatability": True}},
                    }
                )
                with self.assertRaises(ValueError, msg=f"{model}+{extra}"):
                    load_campaign(path)

    def test_hstree_repeatability_env_and_services(self) -> None:
        """HSTree repeatability: exact mapping, trio only, kill switches at 0."""
        path = self._write_config(
            {
                "dataset_path": "data/source.csv",
                "mode": "batch",
                "selected_models": ["hstree"],
                "models": {"hstree": {"repeatability": True}},
            }
        )
        config = load_campaign(path)
        env = build_env(config)

        self.assertEqual(env["DASMA_REPEATABILITY_ENABLED"], "1")
        self.assertEqual(env["REPEATABILITY_IMAGE"], "dasma-kafka-anomaly-light:0.1.0")
        self.assertEqual(env["REPEATABILITY_SCRIPT"], "runtime/detectors/hstree/hstree_analysis.py")
        self.assertEqual(env["REPEATABILITY_MODEL_ID"], "hstree")
        self.assertEqual(env["REPEATABILITY_SCORE_FIELD"], "hstree_score")
        self.assertEqual(env["REPEATABILITY_STATUS_FIELD"], "hstree_status")
        self.assertEqual(env["REPEATABILITY_THRESHOLD_FIELD"], "hstree_threshold")
        self.assertEqual(env["REPEATABILITY_ANOMALY_FIELD"], "hstree_anomaly")
        self.assertEqual(env["REPEATABILITY_EXTRA_FIELDS"], "")
        self.assertEqual(env["DASMA_HSTREE_ENABLED"], "0")
        self.assertEqual(env["DASMA_IFOREST_ENABLED"], "0")
        self.assertEqual(env["DASMA_CONSENSUS_ENABLED"], "0")

        self.assertEqual(
            env["DASMA_COMPOSE_SERVICES"],
            "repeatability-v1 repeatability-v2 repeatability-diff "
            "consensus-analysis build-metrics telegraf",
        )

    def test_iforest_repeatability_uses_mapping(self) -> None:
        path = self._write_config(
            {
                "dataset_path": "data/source.csv",
                "selected_models": ["iforest"],
                "models": {"iforest": {"repeatability": True}},
            }
        )
        env = build_env(load_campaign(path))

        self.assertEqual(env["DASMA_REPEATABILITY_ENABLED"], "1")
        self.assertEqual(env["REPEATABILITY_IMAGE"], "dasma-kafka-anomaly-sklearn:0.1.0")
        self.assertEqual(
            env["REPEATABILITY_SCRIPT"],
            "runtime/detectors/iforest/iforest_analysis.py",
        )
        self.assertEqual(env["REPEATABILITY_MODEL_ID"], "iforest")
        self.assertEqual(env["REPEATABILITY_SCORE_FIELD"], "iforest_score")
        self.assertEqual(env["REPEATABILITY_STATUS_FIELD"], "iforest_status")
        self.assertEqual(env["REPEATABILITY_THRESHOLD_FIELD"], "iforest_threshold")
        self.assertEqual(env["REPEATABILITY_ANOMALY_FIELD"], "iforest_anomaly")
        self.assertEqual(env["REPEATABILITY_EXTRA_FIELDS"], "")
        self.assertEqual(env["DASMA_IFOREST_ENABLED"], "0")
        self.assertEqual(env["DASMA_CONSENSUS_ENABLED"], "0")
        self.assertEqual(
            env["DASMA_COMPOSE_SERVICES"],
            "repeatability-v1 repeatability-v2 repeatability-diff "
            "consensus-analysis build-metrics telegraf",
        )

    def test_kitnet_repeatability_uses_runtime_mapping(self) -> None:
        path = self._write_config(
            {
                "dataset_path": "data/source.csv",
                "selected_models": ["kitnet"],
                "models": {"kitnet": {"repeatability": True}},
            }
        )
        env = build_env(load_campaign(path))

        self.assertEqual(env["DASMA_REPEATABILITY_ENABLED"], "1")
        self.assertEqual(env["REPEATABILITY_IMAGE"], "dasma-kafka-anomaly-light:0.1.0")
        self.assertEqual(
            env["REPEATABILITY_SCRIPT"],
            "runtime/detectors/kitnet/kitnet_analysis_pysad_cpu.py",
        )
        self.assertEqual(env["REPEATABILITY_MODEL_ID"], "kitnet")
        self.assertEqual(env["REPEATABILITY_SCORE_FIELD"], "kitnet_score")
        self.assertEqual(env["REPEATABILITY_STATUS_FIELD"], "kitnet_status")
        self.assertEqual(env["REPEATABILITY_THRESHOLD_FIELD"], "kitnet_threshold")
        self.assertEqual(env["REPEATABILITY_ANOMALY_FIELD"], "kitnet_anomaly")
        self.assertEqual(env["REPEATABILITY_EXTRA_FIELDS"], "")
        self.assertEqual(env["DASMA_KITNET_ENABLED"], "0")
        self.assertEqual(env["DASMA_CONSENSUS_ENABLED"], "0")

    def test_deepant_repeatability_uses_runtime_mapping(self) -> None:
        path = self._write_config(
            {
                "dataset_path": "data/source.csv",
                "selected_models": ["deepant"],
                "models": {"deepant": {"repeatability": True}},
            }
        )
        env = build_env(load_campaign(path))

        self.assertEqual(env["DASMA_REPEATABILITY_ENABLED"], "1")
        self.assertEqual(env["REPEATABILITY_IMAGE"], "dasma-kafka-deepant:0.1.0")
        self.assertEqual(
            env["REPEATABILITY_SCRIPT"],
            "runtime/detectors/deepant/deepant_analysis.py",
        )
        self.assertEqual(env["REPEATABILITY_MODEL_ID"], "deepant")
        self.assertEqual(env["REPEATABILITY_SCORE_FIELD"], "deepant_score")
        self.assertEqual(env["REPEATABILITY_STATUS_FIELD"], "deepant_status")
        self.assertEqual(env["REPEATABILITY_THRESHOLD_FIELD"], "deepant_threshold")
        self.assertEqual(env["REPEATABILITY_ANOMALY_FIELD"], "deepant_anomaly")
        self.assertEqual(env["REPEATABILITY_EXTRA_FIELDS"], "")
        self.assertEqual(env["DASMA_DEEPANT_ENABLED"], "0")
        self.assertEqual(env["DASMA_CONSENSUS_ENABLED"], "0")

    def test_mdragpoint_repeatability_maps_internal_r_extra(self) -> None:
        path = self._write_config(
            {
                "dataset_path": "data/source.csv",
                "selected_models": ["mdragpoint"],
                "models": {"mdragpoint": {"repeatability": True}},
            }
        )
        env = build_env(load_campaign(path))

        self.assertEqual(env["DASMA_REPEATABILITY_ENABLED"], "1")
        self.assertEqual(env["REPEATABILITY_IMAGE"], "dasma-kafka-anomaly-sklearn:0.1.0")
        self.assertEqual(env["REPEATABILITY_SCRIPT"], "runtime/detectors/mdragpoint/mdragstream_point_analysis.py")
        self.assertEqual(env["REPEATABILITY_MODEL_ID"], "mdragpoint")
        self.assertEqual(env["REPEATABILITY_SCORE_FIELD"], "mdragpoint_score")
        self.assertEqual(env["REPEATABILITY_STATUS_FIELD"], "mdragpoint_status")
        self.assertEqual(env["REPEATABILITY_THRESHOLD_FIELD"], "mdragpoint_threshold")
        self.assertEqual(env["REPEATABILITY_ANOMALY_FIELD"], "mdragpoint_anomaly")
        self.assertEqual(env["REPEATABILITY_EXTRA_FIELDS"], "mdragpoint_internal_r")
        self.assertEqual(env["DASMA_MDRAGPOINT_ENABLED"], "0")
        self.assertEqual(env["DASMA_CONSENSUS_ENABLED"], "0")

    def test_mdragpoint_structured_config_sets_runtime_env(self) -> None:
        path = self._write_config(
            {
                "dataset_path": "data/source.csv",
                "selected_models": ["mdragpoint"],
                "models": {
                    "mdragpoint": {
                        "verdict_mode": "external",
                        "estimator": "welford",
                        "dynamic_threshold": True,
                        "lof_init": True,
                        "n_init": 250,
                    }
                },
            }
        )

        env = build_env(load_campaign(path))

        self.assertEqual(env["DASMA_MDRAGPOINT_ENABLED"], "1")
        self.assertEqual(env["MDRAGPOINT_VERDICT_MODE"], "external")
        self.assertEqual(env["MDRAGPOINT_THRESHOLD_MODE"], "welford")
        self.assertEqual(env["MDRAGPOINT_DYNAMIC_THRESHOLD"], "True")
        self.assertEqual(env["MDRAGPOINT_LOF_INIT"], "True")
        self.assertEqual(env["MDRAGPOINT_N_INIT"], "250")
        self.assertEqual(
            env["DASMA_COMPOSE_SERVICES"],
            "mdragstream-point-analysis consensus-analysis build-metrics telegraf",
        )

    def test_kitnet_structured_config_sets_threshold_runtime_env(self) -> None:
        path = self._write_config(
            {
                "dataset_path": "data/source.csv",
                "selected_models": ["kitnet"],
                "models": {
                    "kitnet": {
                        "estimator": "p99",
                        "threshold_window": 900,
                        "threshold_percentile": 98.5,
                        "anomaly_warmup_window": 120,
                    }
                },
            }
        )

        env = build_env(load_campaign(path))

        self.assertEqual(env["DASMA_KITNET_ENABLED"], "1")
        self.assertEqual(env["KITNET_THRESHOLD_MODE"], "p99")
        self.assertEqual(env["KITNET_THRESHOLD_WINDOW"], "900")
        self.assertEqual(env["KITNET_THRESHOLD_PERCENTILE"], "98.5")
        self.assertEqual(env["KITNET_ANOMALY_WARMUP_WINDOW"], "120")
        self.assertEqual(env["KITNET_ANOMALY_MIN_SAMPLES"], "120")

    def test_hstree_campaign_p99_selects_runtime_percentile_mode(self) -> None:
        path = self._write_config(
            {
                "dataset_path": "data/source.csv",
                "selected_models": ["hstree"],
                "models": {"hstree": {"estimator": "p99"}},
            }
        )

        env = build_env(load_campaign(path))

        self.assertEqual(env["HSTREE_THRESHOLD_MODE"], "p99")
        self.assertEqual(
            normalize_hstree_threshold_mode(env["HSTREE_THRESHOLD_MODE"]),
            "percentile",
        )

    def test_default_config_starts_only_default_model_services(self) -> None:
        path = self._write_config({"dataset_path": "data/source.csv"})

        self.assertEqual(
            compose_services(load_campaign(path)),
            [
                "hstree-analysis",
                "iforest-analysis",
                "kitnet-analysis",
                "consensus-analysis",
                "build-metrics",
                "telegraf",
            ],
        )

    def test_flat_legacy_json_remains_supported(self) -> None:
        path = self._write_config(
            {
                "dataset_path": "data/source.csv",
                "selected_models": ["hstree", "kitnet"],
                "kitnet_learning_rate": 0.05,
            }
        )

        env = build_env(load_campaign(path))

        self.assertEqual(env["DASMA_KITNET_ENABLED"], "1")
        self.assertEqual(env["KITNET_LEARNING_RATE"], "0.05")

    def test_legacy_mdragpoint_verdict_mode_alias_sets_estimator(self) -> None:
        path = self._write_config(
            {
                "dataset_path": "data/source.csv",
                "selected_models": ["mdragpoint"],
                "mdragpoint_verdict_mode": "welford",
            }
        )

        env = build_env(load_campaign(path))

        self.assertEqual(env["MDRAGPOINT_VERDICT_MODE"], "internal")
        self.assertEqual(env["MDRAGPOINT_THRESHOLD_MODE"], "welford")

    def test_unknown_top_level_field_is_rejected(self) -> None:
        path = self._write_config({"dataset_path": "data/source.csv", "unexpected": True})

        with self.assertRaisesRegex(ValueError, "unknown campaign config field"):
            load_campaign(path)

    def test_unknown_selected_model_is_rejected(self) -> None:
        path = self._write_config(
            {
                "dataset_path": "data/source.csv",
                "selected_models": ["hstree", "unknown"],
            }
        )

        with self.assertRaisesRegex(ValueError, "unknown selected model"):
            load_campaign(path)

    def test_unknown_structured_model_field_is_rejected(self) -> None:
        path = self._write_config(
            {
                "dataset_path": "data/source.csv",
                "models": {"hstree": {"unknown": 1}},
            }
        )

        with self.assertRaisesRegex(ValueError, "unknown campaign field"):
            load_campaign(path)

    def test_invalid_mode_is_rejected(self) -> None:
        path = self._write_config({"dataset_path": "data/source.csv", "mode": "fast"})

        with self.assertRaisesRegex(ValueError, "mode must be"):
            load_campaign(path)


if __name__ == "__main__":
    unittest.main()
