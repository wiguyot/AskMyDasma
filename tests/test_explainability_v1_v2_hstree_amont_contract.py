import json
import math
import tempfile
from pathlib import Path
from unittest import TestCase

from experiments.explainability.v1_v2.hstree_amont_contract import (
    HSTreeExportError,
    load_hstree_jsonl,
    validate_hstree_records,
)


FIXTURE = Path("tests/fixtures/explainability_v1_v2/hstree_amont_viz.jsonl")


def _record(**overrides):
    record = {
        "timestamp": "2026-01-01 00:00:00",
        "Group": "G1",
        "source_event_id": "event-1",
        "hstree_score": -1.0,
        "hstree_status": "normal",
        "hstree_anomaly": False,
        "hstree_threshold": -0.5,
    }
    record.update(overrides)
    return record


class HSTreeAmontContractTest(TestCase):
    def test_realistic_fixture_is_valid_and_sorted_by_identity(self) -> None:
        result = load_hstree_jsonl(FIXTURE)
        self.assertEqual(result.record_count, 4)
        self.assertEqual(
            result.identities,
            (
                ("G1", "event-000001"),
                ("G1", "event-000002"),
                ("G2", "event-000003"),
                ("G2", "event-000004"),
            ),
        )

    def test_summary_is_json_serializable_and_deterministic(self) -> None:
        result = load_hstree_jsonl(FIXTURE)
        self.assertEqual(json.loads(json.dumps(result.as_dict())), result.as_dict())

    def test_warmup_requires_null_verdict_and_threshold(self) -> None:
        validate_hstree_records([_record(hstree_status="warming_up", hstree_anomaly=None, hstree_threshold=None)])
        with self.assertRaisesRegex(HSTreeExportError, "warming_up"):
            validate_hstree_records([_record(hstree_status="warming_up", hstree_anomaly=False)])

    def test_normal_and_anomaly_require_consistent_verdicts(self) -> None:
        validate_hstree_records([_record()])
        validate_hstree_records([
            _record(hstree_status="anomaly", hstree_anomaly=True, hstree_threshold=-0.5)
        ])
        with self.assertRaisesRegex(HSTreeExportError, "requires hstree_anomaly=True"):
            validate_hstree_records([_record(hstree_status="anomaly", hstree_anomaly=False)])

    def test_identity_must_be_non_empty_string_and_is_not_trimmed(self) -> None:
        with self.assertRaisesRegex(HSTreeExportError, "Group must be"):
            validate_hstree_records([_record(Group="   ")])
        with self.assertRaisesRegex(HSTreeExportError, "source_event_id must be"):
            validate_hstree_records([_record(source_event_id=42)])
        result = validate_hstree_records([_record(Group=" G1 ", source_event_id=" event-1 ")])
        self.assertEqual(result.identities, ((" G1 ", " event-1 "),))

    def test_timestamp_and_required_fields_are_checked(self) -> None:
        with self.assertRaisesRegex(HSTreeExportError, "timestamp must be"):
            validate_hstree_records([_record(timestamp="")])
        incomplete = _record()
        del incomplete["hstree_threshold"]
        with self.assertRaisesRegex(HSTreeExportError, "missing fields"):
            validate_hstree_records([incomplete])

    def test_score_and_threshold_must_be_finite_numbers(self) -> None:
        for value in (True, "bad", math.nan, math.inf):
            with self.subTest(value=value):
                with self.assertRaisesRegex(HSTreeExportError, "hstree_score"):
                    validate_hstree_records([_record(hstree_score=value)])
        for value in (True, "bad", math.nan, math.inf):
            with self.subTest(value=value):
                with self.assertRaisesRegex(HSTreeExportError, "hstree_threshold"):
                    validate_hstree_records([_record(hstree_threshold=value)])

    def test_status_and_verdict_types_are_checked(self) -> None:
        with self.assertRaisesRegex(HSTreeExportError, "invalid hstree_status"):
            validate_hstree_records([_record(hstree_status="unknown")])
        with self.assertRaisesRegex(HSTreeExportError, "boolean hstree_anomaly"):
            validate_hstree_records([_record(hstree_anomaly="false")])

    def test_duplicate_identity_is_rejected(self) -> None:
        with self.assertRaisesRegex(HSTreeExportError, "duplicate identity"):
            validate_hstree_records([_record(), _record(hstree_score=-2.0)])

    def test_non_object_and_malformed_jsonl_are_rejected_with_line(self) -> None:
        with self.assertRaisesRegex(HSTreeExportError, r"fixture:1: record must"):
            validate_hstree_records(["not an object"], export_name="fixture")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bad.jsonl"
            path.write_text('{"Group": "G1"}\nnot-json\n', encoding="utf-8")
            with self.assertRaisesRegex(HSTreeExportError, r"bad.jsonl:2: invalid JSON"):
                load_hstree_jsonl(path)
