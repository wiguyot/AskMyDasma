"""Static frontend contracts for the repeatability selection (step 7).

No browser, no server and no Docker: these tests assert the concrete ids,
payload keys and UX rules in the served HTML and the two JavaScript files.
"""

from __future__ import annotations

from pathlib import Path
from unittest import TestCase

ROOT = Path(__file__).resolve().parents[1]
INDEX = ROOT / "frontend" / "askmydasma" / "index.html"
USER_JS = ROOT / "frontend" / "askmydasma" / "app-user.js"
ADMIN_JS = ROOT / "frontend" / "askmydasma" / "app.js"

MODELS = ("hstree", "iforest", "kitnet", "deepant", "mdragpoint")
CAMEL = {
    "hstree": "Hstree",
    "iforest": "Iforest",
    "kitnet": "Kitnet",
    "deepant": "Deepant",
    "mdragpoint": "Mdragpoint",
}
REPEATABILITY_HELP = "Runs two deterministic instances of the model and compares their outputs."


class RepeatabilityHtmlContractTest(TestCase):
    def setUp(self) -> None:
        self.html = INDEX.read_text(encoding="utf-8")

    def test_five_model_controls_with_exact_ids_and_labels(self) -> None:
        for model in MODELS:
            self.assertIn(f'id="repeatability-{model}"', self.html)
            self.assertIn(f'id="repeatability-{model}-wrapper"', self.html)
            self.assertIn(f'for="repeatability-{model}"', self.html)
            self.assertIn(f'name="{model}_repeatability"', self.html)
        self.assertEqual(self.html.count(">Repeatability<button"), 5)
        self.assertIn(REPEATABILITY_HELP, self.html)
        self.assertEqual(self.html.count('type="checkbox" id="repeatability-'), 5)

    def test_no_explainability_repeatability_control(self) -> None:
        self.assertNotIn('id="repeatability-explainability"', self.html)
        self.assertNotIn('id="repeatability-explainability-v2"', self.html)
        self.assertNotIn('name="explainability_repeatability"', self.html)
        self.assertNotIn('name="explainability_v2_repeatability"', self.html)

    def test_wrappers_are_stable_and_independent_from_params(self) -> None:
        for model in MODELS:
            wrapper = self.html.split(f'id="repeatability-{model}-wrapper"', 1)[1]
            wrapper = wrapper.split("</div>", 1)[0]
            # The wrapper contains only its own checkbox, not the model params.
            self.assertNotIn("model-params", wrapper)


class RepeatabilityUserJsContractTest(TestCase):
    def setUp(self) -> None:
        self.js = USER_JS.read_text(encoding="utf-8")

    def test_payload_emits_five_real_booleans(self) -> None:
        for model in MODELS:
            expected = (
                f'{model}_repeatability: validationMode === "repeatability" '
                f"&& (els.repeatability{CAMEL[model]}?.checked ?? false),"
            )
            self.assertIn(expected, self.js)
        self.assertNotIn('_repeatability: "true"', self.js)
        self.assertNotIn('_repeatability: "false"', self.js)
        self.assertNotIn("JSON.stringify(models", self.js)

    def test_sync_restores_five_flags_and_reapplies_state(self) -> None:
        for model in MODELS:
            self.assertIn(f'["{model}_repeatability", els.repeatability{CAMEL[model]}],', self.js)
        sync_block = self.js.split("function syncFormFromCampaign", 1)[1].split("\n}", 1)[0]
        self.assertIn("!repeatabilityDirty", sync_block)
        self.assertIn("updateInteractiveState()", sync_block)
        self.assertIn("let repeatabilityDirty = false;", self.js)
        self.assertIn("repeatabilityDirty = true;", self.js)

    def test_ux_rules_are_centralized_in_helpers(self) -> None:
        self.assertIn("function repeatabilityBoxes()", self.js)
        self.assertIn("function modelSelectorBoxes()", self.js)
        self.assertIn("function enforceSingleRepeatability(", self.js)
        self.assertIn("function enforceRepeatabilityExclusivity(", self.js)
        # updateInteractiveState applies the rules on every refresh.
        state_block = self.js.split("function updateInteractiveState()", 1)[1].split("\n}", 1)[0]
        self.assertIn("enforceRepeatabilityExclusivity(null)", state_block)
        self.assertIn(
            "entry.box.disabled = locked || activeReproducibility !== null || (!modelChecked && entry.model !== activeRepeatability)",
            state_block,
        )
        self.assertIn("entry.box.disabled = locked || activeRepeatability !== null || activeReproducibility !== null", state_block)
        # Listener-side recency: the user-ticked repeatability wins.
        self.assertIn("enforceRepeatabilityExclusivity(entry.model)", self.js)

    def test_exclusivity_helper_covers_every_selector(self) -> None:
        """One centralized helper spans the five model boxes, the two
        explainability boxes and the five repeatability checkboxes."""
        helpers = self.js.split("function repeatabilityBoxes()", 1)[1].split(
            "function updateInteractiveState()", 1
        )[0]
        for reference in (
            "els.modelHstree",
            "els.modelIforest",
            "els.modelKitnet",
            "els.modelDeepant",
            "els.modelMdragpoint",
            "els.modelExplainability",
            "els.modelExplainabilityV2",
            "els.repeatabilityHstree",
            "els.repeatabilityIforest",
            "els.repeatabilityKitnet",
            "els.repeatabilityDeepant",
            "els.repeatabilityMdragpoint",
        ):
            self.assertIn(reference, helpers)
        self.assertIn("enforceSingleRepeatability(sourceModel)", helpers)

    def test_active_repeatability_freezes_other_selectors(self) -> None:
        """An active repeatability unchecks every other selector and flags."""
        helper = self.js.split("function enforceRepeatabilityExclusivity(", 1)[1].split(
            "\n}", 1
        )[0]
        # Other model selectors and repeatability boxes are unchecked.
        self.assertIn("if (entry.box) entry.box.checked = false;", helper)
        self.assertIn("if (entry.modelBox) entry.modelBox.checked = false;", helper)
        # The flagged model stays checked...
        self.assertIn("if (entry.modelBox) entry.modelBox.checked = true;", helper)
        # ...and explainability services are deselected.
        self.assertIn("els.modelExplainability.checked = false;", helper)
        self.assertIn("els.modelExplainabilityV2.checked = false;", helper)

    def test_flagged_model_and_its_flag_stay_usable_to_exit(self) -> None:
        """The active repeatability checkbox stays enabled to leave the mode;
        the model selector and flag of M are only disabled while active."""
        state_block = self.js.split("function updateInteractiveState()", 1)[1].split("\n}", 1)[0]
        self.assertIn("entry.model !== activeRepeatability", state_block)
        helper = self.js.split("function enforceRepeatabilityExclusivity(", 1)[1].split("\n}", 1)[0]
        self.assertNotIn("entry.box.disabled", helper)
        self.assertNotIn("disabled = true", helper)

    def test_unchecking_repeatability_restores_ordinary_controls(self) -> None:
        """With no active repeatability, availability falls back to ordinary rules."""
        state_block = self.js.split("function updateInteractiveState()", 1)[1].split("\n}", 1)[0]
        # activeRepeatability === null: ordinary locked/model-checked rules.
        self.assertIn("locked || activeReproducibility !== null || (!modelChecked", state_block)
        self.assertIn("locked || activeRepeatability !== null", state_block)
        self.assertNotIn("modelHstree.checked = true", state_block)

    def test_hstree_repeatability_blocks_explainability_availability(self) -> None:
        self.assertIn(
            "const explainabilityAvailable = !locked && !els.repeatabilityHstree?.checked && activeReproducibility === null;",
            self.js,
        )

    def test_iforest_is_available_for_both_explainability_services(self) -> None:
        html = INDEX.read_text(encoding="utf-8")
        self.assertIn('<option value="iforest">IForest</option>', html)
        self.assertEqual(html.count('<option value="iforest">IForest</option>'), 2)
        self.assertNotIn('value="iforest" disabled', html)


class RepeatabilityAdminJsContractTest(TestCase):
    def setUp(self) -> None:
        self.js = ADMIN_JS.read_text(encoding="utf-8")

    def test_three_services_listed_once_in_order_with_labels(self) -> None:
        v1 = self.js.index('"repeatability-v1"')
        v2 = self.js.index('"repeatability-v2"')
        diff = self.js.index('"repeatability-diff"')
        self.assertLess(v1, v2)
        self.assertLess(v2, diff)
        for service in ("repeatability-v1", "repeatability-v2", "repeatability-diff"):
            self.assertEqual(self.js.count(f'"{service}"'), 2, service)
        self.assertIn('"repeatability-v1": "Repeatability v1"', self.js)
        self.assertIn('"repeatability-v2": "Repeatability v2"', self.js)
        self.assertIn('"repeatability-diff": "Repeatability diff"', self.js)


class ValidationModeHtmlContractTest(TestCase):
    """Global Validation mode group: fieldset, exclusivity controls, initial state."""

    def setUp(self) -> None:
        self.html = INDEX.read_text(encoding="utf-8")

    def test_global_group_is_a_fieldset_with_legend_and_two_checkboxes(self) -> None:
        self.assertIn('<fieldset class="validation-mode" id="validation-mode">', self.html)
        self.assertIn("<legend>Validation mode</legend>", self.html)
        self.assertIn('<input type="checkbox" id="validation-mode-repeatability">', self.html)
        self.assertIn('<input type="checkbox" id="validation-mode-reproducibility">', self.html)
        self.assertIn('<label for="validation-mode-repeatability">Repeatability</label>', self.html)
        self.assertIn('<label for="validation-mode-reproducibility">Reproducibility</label>', self.html)

    def test_global_checkboxes_are_not_submitted(self) -> None:
        for checkbox_id in ("validation-mode-repeatability", "validation-mode-reproducibility"):
            tag = self.html.split(f'id="{checkbox_id}"', 1)[0].rsplit("<input", 1)[1]
            self.assertNotIn("name=", tag)

    def test_validation_controls_are_hidden_on_first_display(self) -> None:
        for model in MODELS:
            for family in ("repeatability", "reproducibility"):
                wrapper = self.html.split(f'id="{family}-{model}-wrapper"', 1)[0].rsplit("<div", 1)[1]
                self.assertIn("is-hidden", wrapper, f"{family}-{model}")
                self.assertIn("hidden", wrapper, f"{family}-{model}")
        settings = self.html.split('id="reproducibility-settings"', 1)[0].rsplit("<div", 1)[1]
        self.assertIn("is-hidden", settings)
        self.assertIn("hidden", settings)

    def test_reproducibility_settings_live_under_the_validation_mode_group(self) -> None:
        fieldset_start = self.html.index('<fieldset class="validation-mode"')
        fieldset_end = self.html.index("</fieldset>", fieldset_start)
        settings_start = self.html.index('id="reproducibility-settings"')
        self.assertLess(fieldset_start, settings_start)
        self.assertLess(settings_start, fieldset_end)


class ValidationModeUserJsContractTest(TestCase):
    """Global Validation mode state machine in app-user.js."""

    def setUp(self) -> None:
        self.js = USER_JS.read_text(encoding="utf-8")

    def test_global_mode_helpers_exist(self) -> None:
        self.assertIn("function activeValidationMode()", self.js)
        self.assertIn("function applyValidationModeBoxes(", self.js)
        self.assertIn("function setValidationMode(", self.js)
        self.assertIn("function resetRepeatabilityFlags()", self.js)
        self.assertIn("function resetReproducibilityFields()", self.js)
        self.assertIn("let validationModeDirty = false;", self.js)

    def test_set_validation_mode_clears_only_the_opposite_mode(self) -> None:
        helper = self.js.split("function setValidationMode(", 1)[1].split("\n}", 1)[0]
        self.assertIn('if (mode === "repeatability") {', helper)
        self.assertIn("resetReproducibilityFields();", helper)
        self.assertIn('} else if (mode === "reproducibility") {', helper)
        self.assertIn("resetRepeatabilityFlags();", helper)

    def test_mode_listeners_mark_dirty_and_reset_on_uncheck(self) -> None:
        self.assertIn('setValidationMode("repeatability")', self.js)
        self.assertIn('setValidationMode("reproducibility")', self.js)
        self.assertIn("validationModeDirty = true;", self.js)
        repeatability_listener = self.js.split(
            "els.validationModeRepeatability.addEventListener", 1
        )[1].split("\n}", 1)[0]
        self.assertIn("resetRepeatabilityFlags();", repeatability_listener)
        reproducibility_listener = self.js.split(
            "els.validationModeReproducibility.addEventListener", 1
        )[1].split("\n}", 1)[0]
        self.assertIn("resetReproducibilityFields();", reproducibility_listener)

    def test_sync_deduces_the_global_mode_from_campaign_flags(self) -> None:
        sync_block = self.js.split("function syncFormFromCampaign", 1)[1].split("\n}", 1)[0]
        self.assertIn("!validationModeDirty", sync_block)
        self.assertIn("applyValidationModeBoxes(", sync_block)
        self.assertIn(
            'repeatabilityEvidence ? "repeatability" : reproducibilityEvidence ? "reproducibility" : null',
            sync_block,
        )
        # The deduction runs before the flag restoration blocks so a deduced
        # mode is never cleared by updateInteractiveState.
        self.assertLess(
            sync_block.index("applyValidationModeBoxes("),
            sync_block.index("repeatabilityBindings"),
        )

    def test_payload_gates_validation_fields_on_the_active_mode(self) -> None:
        for model in MODELS:
            self.assertIn(
                f'{model}_repeatability: validationMode === "repeatability" && (els.repeatability{CAMEL[model]}?.checked ?? false),',
                self.js,
            )
        self.assertIn(
            'reproducibility_source_role: validationMode === "reproducibility" ? (els.reproducibilitySourceRole?.value || "") : "",',
            self.js,
        )
        self.assertIn(
            'reproducibility_campaign_id: validationMode === "reproducibility" ? (els.reproducibilityCampaignId?.value.trim() || "") : "",',
            self.js,
        )

    def test_no_mode_active_clears_validation_flags(self) -> None:
        state_block = self.js.split("function updateInteractiveState()", 1)[1].split("\n}", 1)[0]
        self.assertIn("const validationMode = activeValidationMode();", state_block)
        self.assertIn("if (validationMode === null) {", state_block)
        self.assertIn("resetRepeatabilityFlags();", state_block)
        self.assertIn("resetReproducibilityFields();", state_block)

    def test_wrapper_visibility_follows_the_global_mode(self) -> None:
        state_block = self.js.split("function updateInteractiveState()", 1)[1].split("\n}", 1)[0]
        self.assertIn('const wrapperHidden = validationMode !== "repeatability" || !modelChecked;', state_block)
        self.assertIn('const wrapperHidden = validationMode !== "reproducibility" || !modelChecked;', state_block)
        self.assertIn("entry.wrapper.hidden = wrapperHidden;", state_block)
        self.assertIn('const settingsHidden = validationMode !== "reproducibility";', state_block)
        self.assertIn("els.reproducibilitySettings.hidden = settingsHidden;", state_block)
