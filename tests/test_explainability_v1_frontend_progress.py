"""Frontend contracts for the explainability V1 progress card (V1-04c).

The V1 card mirrors the v2 card on both interfaces: state badge, consumer
lag, published records, readable reason, and the at-least-once reminder
("Complete is confirmed only after V1 delivery and commit (at least once)").
The card starts hidden and only appears when explainability_v1.selected is
true. The v2 card, its renderer and every existing button stay untouched.
"""

from __future__ import annotations

from pathlib import Path
from unittest import TestCase


ROOT = Path(__file__).resolve().parents[1]
ADMIN_INDEX = ROOT / "frontend" / "askmydasma" / "admin" / "index.html"
USER_INDEX = ROOT / "frontend" / "askmydasma" / "index.html"
APP_JS = ROOT / "frontend" / "askmydasma" / "app.js"
USER_JS = ROOT / "frontend" / "askmydasma" / "app-user.js"

V1_CARD_IDS = (
    "explainability-v1-progress",
    "explainability-v1-state",
    "explainability-v1-lag",
    "explainability-v1-records",
    "explainability-v1-reason",
)
V2_CARD_IDS = (
    "explainability-v2-progress",
    "explainability-v2-state",
    "explainability-v2-lag",
    "explainability-v2-records",
    "explainability-v2-reason",
    "explainability-v2-forced-warning",
)


class ExplainabilityV1FrontendProgressTest(TestCase):
    def setUp(self) -> None:
        self.admin_html = ADMIN_INDEX.read_text(encoding="utf-8")
        self.user_html = USER_INDEX.read_text(encoding="utf-8")
        self.app_js = APP_JS.read_text(encoding="utf-8")
        self.user_js = USER_JS.read_text(encoding="utf-8")

    # ------------------------------------------------------------------
    # user page (index.html + app-user.js)
    # ------------------------------------------------------------------

    def test_user_page_contains_the_v1_card(self) -> None:
        self.assertIn('src="./app-user.js', self.user_html)
        for element_id in V1_CARD_IDS:
            self.assertIn(f'id="{element_id}"', self.user_html)

    def test_user_page_v1_card_hidden_by_default(self) -> None:
        card_start = self.user_html.index('id="explainability-v1-progress"')
        tag_start = self.user_html.rindex("<div", 0, card_start)
        tag_end = self.user_html.index(">", card_start)
        opening_tag = self.user_html[tag_start : tag_end + 1]
        self.assertIn('id="explainability-v1-progress"', opening_tag)
        self.assertIn("is-hidden", opening_tag)

    def test_user_page_carries_the_at_least_once_mention(self) -> None:
        self.assertIn(
            "Complete is confirmed only after V1 delivery and commit (at least once).",
            self.user_html,
        )

    def test_user_page_reads_campaign_progress(self) -> None:
        self.assertIn('campaignProgress: "/api/campaign-progress"', self.user_js)
        self.assertIn("fetchJson(api.campaignProgress)", self.user_js)

    def test_user_page_renders_the_v1_view(self) -> None:
        self.assertIn("function renderExplainabilityV1Progress", self.user_js)
        self.assertIn("renderExplainabilityV1Progress(campaignProgress?.explainability_v1)", self.user_js)
        for element_id in V1_CARD_IDS:
            self.assertIn(f"#{element_id}", self.user_js)
        # Hidden when V1 is not selected.
        self.assertIn("if (!view || !view.selected)", self.user_js)
        # The three states reuse the shared badge classes.
        self.assertIn("v2-state-${state}", self.user_js)

    # ------------------------------------------------------------------
    # admin page (admin/index.html + app.js)
    # ------------------------------------------------------------------

    def test_admin_dom_contains_the_v1_card_with_stable_ids(self) -> None:
        for element_id in V1_CARD_IDS:
            self.assertIn(f'id="{element_id}"', self.admin_html)

    def test_admin_card_carries_the_at_least_once_mention(self) -> None:
        self.assertIn(
            "Complete is confirmed only after V1 delivery and commit (at least once).",
            self.admin_html,
        )

    def test_admin_render_covers_the_v1_states(self) -> None:
        self.assertIn("function renderExplainabilityV1Progress", self.app_js)
        self.assertIn("renderExplainabilityV1Progress(progress?.explainability_v1)", self.app_js)
        self.assertIn("#explainability-v1-progress", self.app_js)
        self.assertIn("#explainability-v1-lag", self.app_js)
        self.assertIn("#explainability-v1-records", self.app_js)
        self.assertIn("#explainability-v1-reason", self.app_js)

    def test_admin_render_calls_v1_before_the_empty_items_return(self) -> None:
        render_position = self.app_js.index("renderExplainabilityV1Progress(progress?.explainability_v1)")
        early_return_position = self.app_js.index("if (!items.length || totalRecords <= 0)")
        self.assertLess(render_position, early_return_position)

    # ------------------------------------------------------------------
    # non-regressions
    # ------------------------------------------------------------------

    def test_v2_card_is_untouched_on_both_pages(self) -> None:
        for element_id in V2_CARD_IDS:
            self.assertIn(f'id="{element_id}"', self.admin_html)
            self.assertIn(f'id="{element_id}"', self.user_html)
        self.assertIn("renderExplainabilityV2Progress", self.app_js)
        self.assertIn("renderExplainabilityV2Progress", self.user_js)
        self.assertIn(
            "Complete is confirmed only after v2 delivery and commit.",
            self.admin_html,
        )

    def test_existing_buttons_are_preserved(self) -> None:
        for button_id in (
            "stop-button",
            "reset-button",
            "relaunch-button",
            "refresh-button",
            "launch-button",
        ):
            self.assertIn(f'id="{button_id}"', self.admin_html)
        self.assertIn('id="stop-reset-button"', self.user_html)


if __name__ == "__main__":
    unittest.main()
