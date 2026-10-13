"""Contracts for the phase-2 reproducibility campaign configuration (P2-1).

Configuration only: the contract makes a tagged normal campaign expressible,
valid, persistable, restorable and transmittable to the runtime environment.
No CSV exporter, Compose service, topic, bucket or dashboard is involved, and
the normal model service of the flagged model is never replaced.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any
from unittest import TestCase

from scripts.web.campaign_config import build_env, load_campaign_dict
from src.dasma_kafka.web.campaign_config_contract import (
    STRUCTURED_TOP_LEVEL_FIELDS,
    campaign_request_to_config_payload,
    flatten_structured_campaign,
)
from src.dasma_kafka.web.models import CampaignLaunchRequest, CampaignView

MODELS = ("hstree", "iforest", "kitnet", "deepant", "mdragpoint")


def _structured_payload(model: str, **overrides: Any) -> dict[str, Any]:
    reproducibility: dict[str, Any] = {
        "source_role": overrides.pop("source_role", "source-1"),
        "campaign_id": overrides.pop("campaign_id", "campaign-42"),
    }
    payload: dict[str, Any] = {
        "dataset_path": "data/source.csv",
        "mode": "batch",
        "selected_models": [model],
        "models": {model: {"reproducibility": True}},
        "reproducibility": reproducibility,
    }
    payload.update(overrides)
    return payload


class ReproducibilityStructuredContractTest(TestCase):
    """Family 1: flat/structured round-trip and unknown-key rejection."""

    def test_structured_cycle_preserves_all_seven_values(self) -> None:
        payload = _structured_payload("iforest", source_role="source-2", campaign_id="essai-cta")
        flattened = flatten_structured_campaign(payload)
        config = load_campaign_dict(flattened)

        self.assertTrue(config["iforest_reproducibility"])
        self.assertEqual(config["reproducibility_source_role"], "source-2")
        self.assertEqual(config["reproducibility_campaign_id"], "essai-cta")

    def test_payload_emits_reproducibility_block(self) -> None:
        request = CampaignLaunchRequest(
            dataset_path="data/source.csv",
            mode="batch",
            selected_models=["hstree"],
            hstree_reproducibility=True,
            reproducibility_source_role="source-1",
            reproducibility_campaign_id="campaign-42",
        )
        payload = campaign_request_to_config_payload(
            request, selected_groups=[], excluded_fields=[],
        )
        self.assertEqual(
            payload["reproducibility"],
            {"source_role": "source-1", "campaign_id": "campaign-42"},
        )
        self.assertEqual(payload["models"]["hstree"]["reproducibility"], True)
        flattened = flatten_structured_campaign(payload)
        self.assertTrue(flattened["hstree_reproducibility"])
        self.assertEqual(flattened["reproducibility_source_role"], "source-1")
        self.assertEqual(flattened["reproducibility_campaign_id"], "campaign-42")

    def test_structured_top_level_fields_declare_reproducibility(self) -> None:
        self.assertIn("reproducibility", STRUCTURED_TOP_LEVEL_FIELDS)

    def test_unknown_keys_in_reproducibility_block_rejected(self) -> None:
        payload = _structured_payload("hstree")
        payload["reproducibility"]["machine"] = "lab-1"
        with self.assertRaises(ValueError) as ctx:
            flatten_structured_campaign(payload)
        self.assertIn("reproducibility", str(ctx.exception))

    def test_unknown_top_level_reproducibility_key_rejected(self) -> None:
        payload = _structured_payload("hstree")
        payload["reproducibility_role"] = "source-1"
        with self.assertRaises(ValueError):
            flatten_structured_campaign(payload)

    def test_flat_form_still_supported(self) -> None:
        config = load_campaign_dict({
            "dataset_path": "data/source.csv",
            "selected_models": ["kitnet"],
            "kitnet_reproducibility": True,
            "reproducibility_source_role": "source-2",
            "reproducibility_campaign_id": "campaign-42",
        })
        self.assertTrue(config["kitnet_reproducibility"])
        self.assertEqual(config["reproducibility_source_role"], "source-2")

    def test_pydantic_defaults(self) -> None:
        request = CampaignLaunchRequest(dataset_path="data/source.csv", mode="batch")
        for model in MODELS:
            self.assertIs(getattr(request, f"{model}_reproducibility"), False)
        self.assertEqual(request.reproducibility_source_role, "")
        self.assertEqual(request.reproducibility_campaign_id, "")

        view = CampaignView(dataset_path="data/source.csv", mode="batch", status="configuring")
        for model in MODELS:
            self.assertIs(getattr(view, f"{model}_reproducibility"), False)
        self.assertEqual(view.reproducibility_source_role, "")
        self.assertEqual(view.reproducibility_campaign_id, "")


class ReproducibilityValidCampaignTest(TestCase):
    """Family 2: exhaustive valid cases and exact runtime variables."""

    def test_each_model_with_source1_produces_exact_runtime_variables(self) -> None:
        for model in MODELS:
            config = load_campaign_dict(_structured_payload(model))
            env = build_env(config)

            self.assertEqual(env["DASMA_REPRODUCIBILITY_ENABLED"], "1", model)
            self.assertEqual(env["REPRODUCIBILITY_MODEL_ID"], model)
            self.assertEqual(env["REPRODUCIBILITY_SOURCE_ROLE"], "source-1")
            self.assertEqual(env["REPRODUCIBILITY_CAMPAIGN_ID"], "campaign-42")

            # The normal model stays selected and untouched: consensus active,
            # no repeatability pair, normal analysis service in the list.
            self.assertEqual(env["DASMA_CONSENSUS_ENABLED"], "1")
            self.assertEqual(env["DASMA_REPEATABILITY_ENABLED"], "0")
            self.assertEqual(env[f"DASMA_{model.upper()}_ENABLED"], "1")
            self.assertNotIn("repeatability-v1", env["DASMA_COMPOSE_SERVICES"])
            self.assertNotIn("repeatability-v2", env["DASMA_COMPOSE_SERVICES"])
            self.assertNotIn("repeatability-diff", env["DASMA_COMPOSE_SERVICES"])
            normal_service = {
                "hstree": "hstree-analysis",
                "iforest": "iforest-analysis",
                "kitnet": "kitnet-analysis",
                "deepant": "deepant-analysis",
                "mdragpoint": "mdragstream-point-analysis",
            }[model]
            self.assertIn(normal_service, env["DASMA_COMPOSE_SERVICES"])

    def test_source2_role_is_valid(self) -> None:
        config = load_campaign_dict(_structured_payload("hstree", source_role="source-2"))
        env = build_env(config)
        self.assertEqual(env["REPRODUCIBILITY_SOURCE_ROLE"], "source-2")


class OrdinaryCampaignUnchangedTest(TestCase):
    """Family 3: defaults keep ordinary campaigns intact and runtime neutral."""

    def test_neutral_runtime_variables_without_reproducibility(self) -> None:
        config = load_campaign_dict({
            "dataset_path": "data/source.csv",
            "selected_models": ["hstree", "iforest", "kitnet"],
        })
        env = build_env(config)
        self.assertEqual(env["DASMA_REPRODUCIBILITY_ENABLED"], "0")
        self.assertEqual(env["REPRODUCIBILITY_MODEL_ID"], "")
        self.assertEqual(env["REPRODUCIBILITY_SOURCE_ROLE"], "")
        self.assertEqual(env["REPRODUCIBILITY_CAMPAIGN_ID"], "")

    def test_stray_role_and_id_stay_neutral_without_flag(self) -> None:
        config = load_campaign_dict({
            "dataset_path": "data/source.csv",
            "selected_models": ["hstree"],
            "reproducibility_source_role": "source-1",
            "reproducibility_campaign_id": "campaign-42",
        })
        env = build_env(config)
        self.assertEqual(env["DASMA_REPRODUCIBILITY_ENABLED"], "0")
        self.assertEqual(env["REPRODUCIBILITY_SOURCE_ROLE"], "")
        self.assertEqual(env["REPRODUCIBILITY_CAMPAIGN_ID"], "")

    def test_multi_model_selection_still_allowed_without_flag(self) -> None:
        config = load_campaign_dict({
            "dataset_path": "data/source.csv",
            "selected_models": ["hstree", "iforest", "kitnet"],
        })
        env = build_env(config)
        self.assertIn("hstree-analysis", env["DASMA_COMPOSE_SERVICES"])
        self.assertIn("iforest-analysis", env["DASMA_COMPOSE_SERVICES"])


class ReproducibilityExclusivityTest(TestCase):
    """Family 4: cumulative validation rules, all rejections."""

    def _load(self, payload: dict[str, Any]) -> None:
        load_campaign_dict(payload)

    def test_two_reproducibility_flags_rejected(self) -> None:
        payload = _structured_payload("hstree")
        payload["selected_models"] = ["hstree"]
        payload["models"]["iforest"] = {"reproducibility": True}
        with self.assertRaises(ValueError) as ctx:
            self._load(payload)
        self.assertIn("reproducibility test is exclusive", str(ctx.exception))

    def test_flagged_model_not_selected_rejected(self) -> None:
        payload = _structured_payload("hstree")
        payload["selected_models"] = ["iforest"]
        with self.assertRaises(ValueError) as ctx:
            self._load(payload)
        self.assertIn("reproducibility test is exclusive", str(ctx.exception))

    def test_conflicting_model_rejected_for_every_model(self) -> None:
        for model in MODELS:
            others = [other for other in MODELS if other != model]
            for other in others:
                payload = _structured_payload(model)
                payload["selected_models"] = [model, other]
                with self.assertRaises(ValueError, msg=f"{model}+{other}") as ctx:
                    self._load(payload)
                self.assertIn("reproducibility test is exclusive", str(ctx.exception))

    def test_explainability_conflict_rejected(self) -> None:
        for extra in ("explainability", "explainability-v2"):
            payload = _structured_payload("hstree")
            payload["selected_models"] = ["hstree", extra]
            with self.assertRaises(ValueError) as ctx:
                self._load(payload)
            self.assertIn("reproducibility test is exclusive", str(ctx.exception))

    def test_duplicated_flagged_model_rejected(self) -> None:
        payload = _structured_payload("hstree")
        payload["selected_models"] = ["hstree", "hstree"]
        with self.assertRaises(ValueError) as ctx:
            self._load(payload)
        self.assertIn("reproducibility test is exclusive", str(ctx.exception))

    def test_missing_or_invalid_source_role_rejected(self) -> None:
        for role in ("", "source-3", "SOURCE-1", "source1"):
            payload = _structured_payload("hstree", source_role=role)
            with self.assertRaises(ValueError, msg=role) as ctx:
                self._load(payload)
            self.assertIn("source role", str(ctx.exception))

    def test_invalid_campaign_id_rejected(self) -> None:
        cases = {
            "empty": "",
            "spaces only": "   ",
            "too long": "a" * 129,
            "slash": "cam/paign",
            "backslash": "cam\\paign",
            "dotdot": "..",
            "leading dot": ".hidden",
            "spaces inside": "cam paign",
        }
        for label, campaign_id in cases.items():
            payload = _structured_payload("hstree", campaign_id=campaign_id)
            with self.assertRaises(ValueError, msg=label) as ctx:
                self._load(payload)
            self.assertIn("campaign-id", str(ctx.exception))

    def test_campaign_id_is_normalized_and_portable_ids_accepted(self) -> None:
        config = load_campaign_dict(_structured_payload("hstree", campaign_id="  campaign-42  "))
        self.assertEqual(config["reproducibility_campaign_id"], "campaign-42")
        for campaign_id in ("campaign-42", "A9", "x.y_z-w", "a" * 128):
            config = load_campaign_dict(_structured_payload("hstree", campaign_id=campaign_id))
            self.assertEqual(config["reproducibility_campaign_id"], campaign_id)

    def test_repeatability_and_reproducibility_mutually_exclusive(self) -> None:
        # Same model.
        payload = _structured_payload("hstree")
        payload["models"]["hstree"]["repeatability"] = True
        with self.assertRaises(ValueError) as ctx:
            self._load(payload)
        self.assertIn("repeatability", str(ctx.exception))

        # Different model.
        payload = _structured_payload("hstree")
        payload["models"]["iforest"] = {"repeatability": True}
        with self.assertRaises(ValueError) as ctx:
            self._load(payload)
        self.assertIn("repeatability", str(ctx.exception))


class ReproducibilityFrontendContractTest(TestCase):
    """Family 6 (JS part): fields read, published and restored."""

    def setUp(self) -> None:
        self.js = (Path(__file__).resolve().parents[1] / "frontend" / "askmydasma" / "app-user.js").read_text(encoding="utf-8")

    def test_payload_emits_the_seven_values(self) -> None:
        for model in MODELS:
            self.assertIn(
                f'{model}_reproducibility: validationMode === "reproducibility" && (els.reproducibility{model.capitalize()}?.checked ?? false),',
                self.js,
            )
        self.assertIn('reproducibility_source_role: validationMode === "reproducibility" ? (els.reproducibilitySourceRole?.value || "") : "",', self.js)
        self.assertIn('reproducibility_campaign_id: validationMode === "reproducibility" ? (els.reproducibilityCampaignId?.value.trim() || "") : "",', self.js)

    def test_sync_restores_seven_values_with_dedicated_dirty_flag(self) -> None:
        self.assertIn("let reproducibilityDirty = false;", self.js)
        self.assertIn("reproducibilityDirty = true;", self.js)
        sync_block = self.js.split("function syncFormFromCampaign", 1)[1].split("\n}", 1)[0]
        self.assertIn("!reproducibilityDirty", sync_block)
        for model in MODELS:
            self.assertIn(f'["{model}_reproducibility", els.reproducibility{model.capitalize()}],', sync_block)
        self.assertIn('["hstree_reproducibility", els.reproducibilityHstree],', sync_block)
        self.assertIn("els.reproducibilitySourceRole.value", sync_block)
        self.assertIn("els.reproducibilityCampaignId.value", sync_block)

    def test_change_listeners_mark_dirty(self) -> None:
        self.assertIn("reproducibilityDirty = true;", self.js)
        # The five model boxes mark the dedicated dirty flag via the shared
        # reproducibilityBoxes() helper loop.
        listener_block = self.js.split("for (const entry of reproducibilityBoxes()) {\n  if (entry.box) {\n    entry.box.addEventListener", 1)[1]
        self.assertIn("reproducibilityDirty = true;", listener_block.split("}\n}", 1)[0])
        self.assertIn("els.reproducibilitySourceRole.addEventListener", self.js)
        self.assertIn("els.reproducibilityCampaignId.addEventListener", self.js)

    def test_sync_deduces_reproducibility_mode_from_flags_role_and_id(self) -> None:
        sync_block = self.js.split("function syncFormFromCampaign", 1)[1].split("\n}", 1)[0]
        self.assertIn("!validationModeDirty", sync_block)
        self.assertIn("!reproducibilityDirty &&", sync_block)
        for model in MODELS:
            self.assertIn("Boolean(campaign[`${model}_reproducibility`])", sync_block)
        self.assertIn("Boolean(campaign.reproducibility_source_role)", sync_block)
        self.assertIn("Boolean(campaign.reproducibility_campaign_id)", sync_block)

    def test_mode_switch_resets_the_other_family_only(self) -> None:
        helper = self.js.split("function setValidationMode(", 1)[1].split("\n}", 1)[0]
        self.assertIn("resetReproducibilityFields();", helper)
        self.assertIn("resetRepeatabilityFlags();", helper)
        # The active mode's own flags are never cleared by setValidationMode:
        # resetReproducibilityFields only runs in the repeatability branch and
        # resetRepeatabilityFlags only in the reproducibility branch.
        self.assertLess(helper.index("if (mode ==="), helper.index("resetReproducibilityFields();"))
        self.assertLess(helper.index("resetReproducibilityFields();"), helper.index("} else if"))
        self.assertLess(helper.index("} else if"), helper.index("resetRepeatabilityFlags();"))


class ReproducibilityValidationModeHtmlTest(TestCase):
    """Family 6 (HTML part): contextual fields live under the global group."""

    def setUp(self) -> None:
        self.html = (Path(__file__).resolve().parents[1] / "frontend" / "askmydasma" / "index.html").read_text(encoding="utf-8")

    def test_settings_block_lives_under_the_validation_mode_group(self) -> None:
        fieldset_start = self.html.index('<fieldset class="validation-mode"')
        fieldset_end = self.html.index("</fieldset>", fieldset_start)
        settings_start = self.html.index('id="reproducibility-settings"')
        self.assertLess(fieldset_start, settings_start)
        self.assertLess(settings_start, fieldset_end)

    def test_contextual_fields_use_english_labels(self) -> None:
        self.assertIn(">Source role:</label>", self.html)
        self.assertIn(">Campaign ID:</label>", self.html)
        self.assertNotIn("Rôle de source", self.html)
        self.assertNotIn("Campaign-id", self.html)
        self.assertNotIn("Reproductibilité", self.html)

    def test_global_mode_checkboxes_are_not_submitted(self) -> None:
        for checkbox_id in ("validation-mode-repeatability", "validation-mode-reproducibility"):
            tag = self.html.split(f'id="{checkbox_id}"', 1)[0].rsplit("<input", 1)[1]
            self.assertNotIn("name=", tag)
