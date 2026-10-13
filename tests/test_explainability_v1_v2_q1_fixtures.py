import json
from pathlib import Path
from unittest import TestCase

from experiments.explainability.v1_v2.hstree_amont_contract import load_hstree_jsonl
from experiments.explainability.v1_v2.hstree_amont_vs_local import compare_three_exports


ROOT = Path("tests/fixtures/explainability_v1_v2/q1_three_exports")


def _load(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


class Q1ThreeExportFixtureTest(TestCase):
    def test_three_exports_have_the_same_expected_identity_set(self) -> None:
        manifest = json.loads((ROOT / "manifest.json").read_text(encoding="utf-8"))
        expected = sorted(tuple(key.split("|", 1)) for key in manifest["expected_keys"])
        upstream = load_hstree_jsonl(ROOT / "upstream.jsonl")
        self.assertEqual(list(upstream.identities), expected)
        for name in ("v1", "v2"):
            records = _load(ROOT / f"{name}.jsonl")
            actual = sorted((record["Group"], record["source_event_id"]) for record in records)
            self.assertEqual(actual, expected)

    def test_fixture_contains_interleaved_groups_and_timestamp_collisions(self) -> None:
        records = _load(ROOT / "upstream.jsonl")
        self.assertEqual([record["Group"] for record in records[:4]], ["G1", "G2", "G1", "G2"])
        same_timestamp = [record for record in records if record["timestamp"] == "2026-01-01 00:00:03"]
        self.assertEqual(len(same_timestamp), 4)
        self.assertEqual(len({(r["Group"], r["source_event_id"]) for r in same_timestamp}), 4)

    def test_v1_and_v2_equal_but_both_drift_from_upstream(self) -> None:
        report = compare_three_exports(
            _load(ROOT / "upstream.jsonl"),
            _load(ROOT / "v1.jsonl"),
            _load(ROOT / "v2.jsonl"),
        )
        comparisons = report["comparisons"]
        self.assertEqual(comparisons["v1_vs_v2"]["verdict"], "ok")
        self.assertEqual(comparisons["upstream_vs_v1"]["verdict"], "ko")
        self.assertEqual(comparisons["upstream_vs_v2"]["verdict"], "ko")
        self.assertEqual(comparisons["upstream_vs_v1"]["first_divergence"]["key"], ["G1", "event-000005"])

    def test_reordering_all_three_exports_keeps_report_identical(self) -> None:
        upstream = _load(ROOT / "upstream.jsonl")
        v1 = _load(ROOT / "v1.jsonl")
        v2 = _load(ROOT / "v2.jsonl")
        first = json.dumps(compare_three_exports(upstream, v1, v2), sort_keys=True, separators=(",", ":"))
        second = json.dumps(
            compare_three_exports(list(reversed(upstream)), list(reversed(v1)), list(reversed(v2))),
            sort_keys=True,
            separators=(",", ":"),
        )
        self.assertEqual(first, second)

