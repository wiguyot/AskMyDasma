from html.parser import HTMLParser
from pathlib import Path
from unittest import TestCase


ROOT = Path(__file__).resolve().parents[1]
USER_INDEX = ROOT / "frontend" / "askmydasma" / "index.html"
USER_JS = ROOT / "frontend" / "askmydasma" / "app-user.js"


class SelectContractParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.selects: dict[str, list[dict[str, str | bool]]] = {}
        self._current_select: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        if tag == "select":
            self._current_select = attributes.get("id")
            if self._current_select:
                self.selects[self._current_select] = []
        elif tag == "option" and self._current_select:
            option = {
                "value": attributes.get("value", ""),
                "selected": "selected" in attributes,
                "disabled": "disabled" in attributes,
            }
            self.selects[self._current_select].append(option)

    def handle_endtag(self, tag: str) -> None:
        if tag == "select":
            self._current_select = None


class ExplainabilityModeFrontendContractsTest(TestCase):
    def setUp(self) -> None:
        self.html = USER_INDEX.read_text(encoding="utf-8")
        self.js = USER_JS.read_text(encoding="utf-8")
        parser = SelectContractParser()
        parser.feed(self.html)
        self.selects = parser.selects

    def test_mode_selects_have_an_empty_default_option(self) -> None:
        for select_id in ("explainability-mode", "explainability-v2-mode"):
            with self.subTest(select_id=select_id):
                options = self.selects[select_id]
                self.assertEqual(options[0]["value"], "")
                self.assertTrue(options[0]["selected"])
                self.assertFalse(any(option["selected"] for option in options[1:]))

    def test_method_option_values_match_the_server_contract(self) -> None:
        for select_id in ("explainability-method", "explainability-v2-method"):
            with self.subTest(select_id=select_id):
                values = {option["value"] for option in self.selects[select_id]}
                self.assertTrue(values)
                self.assertLessEqual(values, {"kernel", "tree"})

    def test_kitnet_is_visibly_not_applicable_for_both_explainability_services(self) -> None:
        for select_id in ("explainability-model", "explainability-v2-model"):
            with self.subTest(select_id=select_id):
                kitnet = next(option for option in self.selects[select_id] if option["value"] == "kitnet")
                self.assertTrue(kitnet["disabled"])
                self.assertIn('title="Not applicable: KitNET + KernelSHAP', self.html)
                self.assertIn("KitNET: N/A", self.html)

    def test_suspended_kitnet_cannot_be_restored_or_submitted_from_the_ui(self) -> None:
        self.assertIn('function selectedExplainabilityModel(input)', self.js)
        self.assertIn('["hstree", "iforest"].includes(input?.value) ? input.value : null', self.js)
        self.assertIn('function restoreExplainabilityModel(input, value)', self.js)
        self.assertIn('restoreExplainabilityModel(input, String(campaign[field]))', self.js)
        self.assertIn('explainability_model: selectedExplainabilityModel(els.explainabilityModel)', self.js)
        self.assertIn('explainability_v2_model: selectedExplainabilityModel(els.explainabilityV2Model)', self.js)

    def test_explainability_dashboard_links_follow_the_selected_model(self) -> None:
        self.assertIn('id="explainability-dashboard-link"', self.html)
        self.assertIn('id="explainability-v2-dashboard-link"', self.html)
        self.assertIn("function updateExplainabilityDashboardLinks()", self.js)
        for path in (
            "/d/explainability-influxdb/explainability?",
            "/d/explainability-iforest-influxdb/explainability-iforest?",
            "/d/explainability-kitnet-influxdb/explainability-kitnet?",
            "/d/explainability-v2-influxdb/explainability-v2?",
            "/d/explainability-v2-iforest-influxdb/explainability-v2-iforest?",
            "/d/explainability-v2-kitnet-influxdb/explainability-v2-kitnet?",
        ):
            self.assertIn(path, self.js)

    def test_iforest_and_kitnet_parameters_include_explainability_selections(self) -> None:
        self.assertIn('els.explainabilityModel?.value === "iforest"', self.js)
        self.assertIn('els.explainabilityV2Model?.value === "iforest"', self.js)
        self.assertIn('els.explainabilityModel?.value === "kitnet"', self.js)
        self.assertIn('els.explainabilityV2Model?.value === "kitnet"', self.js)

    def test_launch_is_blocked_with_a_message_when_a_selected_service_has_no_mode(self) -> None:
        self.assertIn('id="explainability-mode-error"', self.html)
        self.assertIn("function explainabilityServicesMissingMode()", self.js)
        self.assertIn(
            "els.launchButton.disabled = actionInFlight || running || missingExplainabilityModes.length > 0;",
            self.js,
        )
        self.assertIn("Choose a mode for each selected explainability service:", self.js)

    def test_restored_invalid_mode_is_left_unselected(self) -> None:
        self.assertIn(
            'inputs.value = ["simple", "reference"].includes(value) ? value : "";',
            self.js,
        )


if __name__ == "__main__":
    import unittest

    unittest.main()
