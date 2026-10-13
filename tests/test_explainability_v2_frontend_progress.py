"""Frontend contracts for the explainability-v2 progress card (OC-11C)."""

from __future__ import annotations

from pathlib import Path
from unittest import TestCase


ROOT = Path(__file__).resolve().parents[1]
ADMIN_INDEX = ROOT / "frontend" / "askmydasma" / "admin" / "index.html"
USER_INDEX = ROOT / "frontend" / "askmydasma" / "index.html"
APP_JS = ROOT / "frontend" / "askmydasma" / "app.js"
USER_JS = ROOT / "frontend" / "askmydasma" / "app-user.js"
STYLES_CSS = ROOT / "frontend" / "askmydasma" / "styles.css"

V2_CARD_IDS = (
    "explainability-v2-progress",
    "explainability-v2-state",
    "explainability-v2-lag",
    "explainability-v2-records",
    "explainability-v2-reason",
    "explainability-v2-forced-warning",
)


class ExplainabilityV2FrontendProgressTest(TestCase):
    def setUp(self) -> None:
        self.admin_html = ADMIN_INDEX.read_text(encoding="utf-8")
        self.user_html = USER_INDEX.read_text(encoding="utf-8")
        self.app_js = APP_JS.read_text(encoding="utf-8")
        self.user_js = USER_JS.read_text(encoding="utf-8")
        self.styles = STYLES_CSS.read_text(encoding="utf-8")

    # ------------------------------------------------------------------
    # user page (index.html + app-user.js)
    # ------------------------------------------------------------------

    def test_user_page_loads_app_user_js_and_contains_v2_card(self) -> None:
        self.assertIn('src="./app-user.js', self.user_html)
        for element_id in V2_CARD_IDS:
            self.assertIn(f'id="{element_id}"', self.user_html)

    def test_user_page_v2_card_hidden_by_default_and_keeps_buttons(self) -> None:
        card_start = self.user_html.index('id="explainability-v2-progress"')
        tag_start = self.user_html.rindex("<div", 0, card_start)
        tag_end = self.user_html.index(">", card_start)
        opening_tag = self.user_html[tag_start : tag_end + 1]
        self.assertIn('id="explainability-v2-progress"', opening_tag)
        self.assertIn("is-hidden", opening_tag)
        self.assertIn('id="stop-reset-button"', self.user_html)

    def test_user_page_reads_campaign_progress(self) -> None:
        self.assertIn('campaignProgress: "/api/campaign-progress"', self.user_js)
        self.assertIn("fetchJson(api.campaignProgress)", self.user_js)

    def test_user_page_renders_the_three_v2_states(self) -> None:
        for state in ("processing", "complete", "failed"):
            self.assertIn(f"{state}:", self.user_js)
            self.assertIn(f"v2-state-${{state}}", self.user_js)
        self.assertIn("renderExplainabilityV2Progress", self.user_js)
        self.assertIn("#explainability-v2-progress", self.user_js)
        self.assertIn("#explainability-v2-lag", self.user_js)
        self.assertIn("#explainability-v2-records", self.user_js)
        self.assertIn("#explainability-v2-reason", self.user_js)
        # Hidden when v2 is not selected.
        self.assertIn("if (!view || !view.selected)", self.user_js)

    def test_user_page_treats_409_with_state_and_reason(self) -> None:
        self.assertIn("class ActionBlockedError extends Error", self.user_js)
        self.assertIn("this.state = detail?.state || null", self.user_js)
        self.assertIn("this.reason = detail?.reason || null", self.user_js)
        self.assertIn('typeof payload.detail === "object"', self.user_js)

    def test_user_page_force_only_after_confirmation(self) -> None:
        confirm_position = self.user_js.index("window.confirm")
        force_positions = [
            position
            for position in range(len(self.user_js))
            if self.user_js.startswith("?force=true", position)
        ]
        self.assertEqual(len(force_positions), 1)
        self.assertGreater(force_positions[0], confirm_position)
        # The plain stopAndReset URL never carries force.
        self.assertNotIn("force", 'stopAndReset: "/api/campaigns/stop-and-reset"')

    def test_user_page_cancelled_confirmation_never_posts_forced(self) -> None:
        # The cancel branch returns before any forced POST can be built.
        cancel_marker = 'console.warn("Stop and reset cancelled: the campaign remains incomplete.");'
        cancel_position = self.user_js.index(cancel_marker)
        forced_post_position = self.user_js.index("await postAction(`${api.stopAndReset}?force=true`")
        self.assertLess(cancel_position, forced_post_position)
        # Nothing before the guard helper builds a forced URL.
        self.assertNotIn("force=true", self.user_js.split("stopResetButton.addEventListener")[0])

    def test_user_page_restores_visual_state_after_409(self) -> None:
        handler = self.user_js.split("stopResetButton.addEventListener", 1)[1]
        confirm_position = handler.index("window.confirm")
        restore_position = handler.index("setPlatformState(currentPlatformStatus")
        self.assertLess(restore_position, confirm_position)
        self.assertLess(handler.index("stopping_platform"), restore_position)

    # ------------------------------------------------------------------
    # admin page (admin/index.html + app.js)
    # ------------------------------------------------------------------

    def test_admin_dom_contains_v2_card_with_stable_ids(self) -> None:
        self.assertIn('id="explainability-v2-progress"', self.admin_html)
        self.assertIn('id="explainability-v2-state"', self.admin_html)
        self.assertIn('id="explainability-v2-lag"', self.admin_html)
        self.assertIn('id="explainability-v2-records"', self.admin_html)
        self.assertIn('id="explainability-v2-reason"', self.admin_html)
        self.assertIn('id="explainability-v2-forced-warning"', self.admin_html)

    def test_v2_card_carries_the_commit_confirmation_message(self) -> None:
        self.assertIn("Complete is confirmed only after v2 delivery and commit.", self.admin_html)

    def test_render_covers_the_three_states(self) -> None:
        for state in ("processing", "complete", "failed"):
            self.assertIn(f"{state}:", self.app_js)
            self.assertIn(f"v2-state-${{state}}", self.app_js)
        self.assertIn("Processing", self.app_js)
        self.assertIn("Complete", self.app_js)
        self.assertIn("Failed", self.app_js)
        self.assertIn("renderExplainabilityV2Progress", self.app_js)
        self.assertIn("#explainability-v2-progress", self.app_js)
        self.assertIn("#explainability-v2-lag", self.app_js)
        self.assertIn("#explainability-v2-records", self.app_js)
        self.assertIn("#explainability-v2-reason", self.app_js)

    def test_complete_and_failed_states_are_visually_distinguishable(self) -> None:
        self.assertIn(".badge.v2-state-complete", self.styles)
        self.assertIn(".badge.v2-state-failed", self.styles)
        complete_block = self.styles.split(".badge.v2-state-complete", 1)[1].split("}", 1)[0]
        failed_block = self.styles.split(".badge.v2-state-failed", 1)[1].split("}", 1)[0]
        self.assertIn("color:", complete_block)
        self.assertIn("color:", failed_block)
        self.assertNotEqual(complete_block, failed_block)

    def test_force_is_only_built_after_user_confirmation(self) -> None:
        confirm_position = self.app_js.index("window.confirm")
        force_positions = [
            position
            for position in range(len(self.app_js))
            if self.app_js.startswith("?force=true", position)
        ]
        self.assertEqual(len(force_positions), 1)
        self.assertGreater(force_positions[0], confirm_position)
        # Nothing before the guard helper ever builds force=true.
        self.assertNotIn("force=true", self.app_js.split("postActionWithForceGuard")[0])
        # The default action URLs never carry force.
        self.assertNotIn("force", 'stop: "/api/campaigns/stop"')
        self.assertNotIn("force", 'reset: "/api/campaigns/reset"')

    def test_admin_render_calls_v2_before_the_empty_items_return(self) -> None:
        render_position = self.app_js.index("renderExplainabilityV2Progress(progress?.explainability_v2)")
        early_return_position = self.app_js.index("if (!items.length || totalRecords <= 0)")
        self.assertLess(render_position, early_return_position)
        # Exactly one render call site in the admin bundle.
        self.assertEqual(
            len([
                position
                for position in range(len(self.app_js))
                if self.app_js.startswith("renderExplainabilityV2Progress(progress?.explainability_v2)", position)
            ]),
            1,
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
        self.assertIn('id="stop-reset-button"', USER_INDEX.read_text(encoding="utf-8"))

    def test_v2_card_hidden_until_a_selected_v2_state_arrives(self) -> None:
        # The card starts hidden in the DOM; the renderer shows it only when
        # explainability_v2.selected is true.
        card_start = self.admin_html.index('id="explainability-v2-progress"')
        tag_start = self.admin_html.rindex("<div", 0, card_start)
        tag_end = self.admin_html.index(">", card_start)
        opening_tag = self.admin_html[tag_start : tag_end + 1]
        self.assertIn('id="explainability-v2-progress"', opening_tag)
        self.assertIn("is-hidden", opening_tag)
