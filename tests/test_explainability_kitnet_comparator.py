"""KitNET upstream-versus-local comparison contract tests."""

from __future__ import annotations

import json
from pathlib import Path
from unittest import TestCase

from experiments.explainability.v1_v2.hstree_amont_vs_local import compare_pair


FIXTURES = Path(__file__).resolve().parent / "fixtures/explainability_kitnet"


def _jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def _compare(upstream: list[dict], local: list[dict]) -> dict:
    return compare_pair(
        upstream,
        local,
        upstream_name="kitnet_detector",
        local_name="kitnet_explainability",
        upstream_score_field="kitnet_score",
        local_score_field="local_score",
        upstream_status_field="kitnet_status",
        local_status_field="reference_status",
        local_explanation_status_field="explanation_status",
        local_error_code_field="explanation_error_code",
        upstream_anomaly_field="kitnet_anomaly",
        local_anomaly_field="reference_anomaly",
        upstream_threshold_field="kitnet_threshold",
        local_threshold_field="reference_threshold",
    )


class KitNETComparatorTest(TestCase):
    def test_fixture_covers_warmup_invalid_inputs_exact_coverage_and_score_fidelity(self) -> None:
        report = _compare(_jsonl(FIXTURES / "upstream.jsonl"), _jsonl(FIXTURES / "local.jsonl"))

        self.assertEqual(report["upstream_records"], 8)  # duplicate key indexed once
        self.assertEqual(report["local_records"], 8)
        self.assertEqual(report["compared_identities"], 6)
        self.assertEqual(report["categories"]["duplicate"], 1)
        self.assertEqual(report["categories"]["missing"], 2)
        self.assertEqual(report["categories"]["divergent"], 1)
        self.assertEqual(report["verdict"], "ko")
        self.assertEqual(report["only_upstream"], [["K", "upstream-only"]])
        self.assertEqual(report["only_local"], [["K", "local-only"]])
        self.assertEqual(report["score_exclusions"], {"warmup": 2, "invalid_features": 2})
        self.assertEqual(
            report["local_explanation_statuses"],
            {"computed": 2, "failed": 2, "warmup": 2},
        )
        self.assertEqual(report["local_explanation_error_codes"], {"invalid_features": 2})
        self.assertEqual(report["numeric_outcomes"]["divergent"], 1)
        self.assertEqual(report["numeric_outcomes"]["invalid_numeric"], 0)

        by_id = {item["key"][1]: item for item in report["identities"]}
        self.assertEqual(by_id["warm-zero"]["category"], "equal")
        self.assertEqual(by_id["warm-nonzero"]["category"], "equal")
        self.assertEqual(by_id["computed"]["category"], "equal")
        self.assertEqual(by_id["invalid-warmup"]["category"], "equal")
        self.assertEqual(by_id["invalid-ready"]["category"], "equal")
        score_detail = next(detail for detail in by_id["divergent"]["details"] if detail["path"] == "local_score")
        self.assertEqual(score_detail["outcome"], "divergent")
        self.assertEqual(by_id["duplicate"]["category"], "duplicate")

    def test_warmup_nonzero_upstream_score_is_not_compared_when_local_explainer_says_warmup(self) -> None:
        upstream = _jsonl(FIXTURES / "upstream.jsonl")[1]
        local = _jsonl(FIXTURES / "local.jsonl")[1]
        upstream["kitnet_score"] = 99.0
        report = _compare([upstream], [local])
        self.assertEqual(report["verdict"], "ok")
        self.assertEqual(report["numeric_outcomes"]["invalid_numeric"], 0)
        self.assertEqual(report["score_exclusions"]["warmup"], 1)

    def test_invalid_features_must_be_failed_and_are_excluded_during_and_after_warmup(self) -> None:
        upstream = _jsonl(FIXTURES / "upstream.jsonl")
        local = _jsonl(FIXTURES / "local.jsonl")
        invalid_upstream = [row for row in upstream if row["source_event_id"].startswith("invalid-")]
        invalid_local = [row for row in local if row["source_event_id"].startswith("invalid-")]
        local[4]["explanation_status"] = "computed"
        report = _compare(invalid_upstream, invalid_local)
        self.assertEqual(report["score_exclusions"]["invalid_features"], 2)
        self.assertEqual(report["categories"]["divergent"], 1)
        self.assertEqual(report["numeric_outcomes"]["invalid_numeric"], 0)
        self.assertEqual(report["verdict"], "ko")

    def test_default_hstree_and_iforest_contract_has_no_kitnet_only_counters(self) -> None:
        upstream = _jsonl(FIXTURES / "upstream.jsonl")[2]
        local = _jsonl(FIXTURES / "local.jsonl")[2]
        report = compare_pair(
            [
                {
                    **upstream,
                    "Group": "K",
                    "source_event_id": "base",
                    "hstree_score": 0.7,
                    "hstree_status": "normal",
                    "hstree_anomaly": False,
                    "hstree_threshold": 0.8,
                }
            ],
            [
                {
                    **local,
                    "Group": "K",
                    "source_event_id": "base",
                    "local_hstree_score": 0.7,
                    "hstree_status": "normal",
                    "hstree_anomaly": False,
                    "hstree_threshold": 0.8,
                }
            ],
        )
        self.assertEqual(report["verdict"], "ok")
        self.assertNotIn("score_exclusions", report)
