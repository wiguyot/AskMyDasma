"""Pure tests for the OC-12 campaign validator (no Docker, no Kafka)."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict, List

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "experiments" / "explainability" / "v2"))

import validate_oc12_campaign as validator


GROUP = "OC12-Test"
SHARED_TIMESTAMP = "2026-09-18 12:00:42"
WARMUP_IDS = [f"oc12-w{i:03d}" for i in range(1, 5)]
POST_IDS = [f"oc12-p{i:03d}" for i in range(1, 9)]
SIMULTANEOUS_IDS = ["oc12-p002", "oc12-p003"]  # post-warmup pair


def manifest() -> Dict[str, Any]:
    return {
        "campaign": "oc12-test",
        "expected_total_events": len(WARMUP_IDS) + len(POST_IDS),
        "warmup_ids": WARMUP_IDS,
        "post_warmup_ids": POST_IDS,
        "simultaneous_ids": SIMULTANEOUS_IDS,
        "simultaneous_group": GROUP,
        "simultaneous_timestamp": SHARED_TIMESTAMP,
    }


def hstree_rows() -> List[Dict[str, Any]]:
    rows = [
        {"Group": GROUP, "source_event_id": event_id, "hstree_anomaly": None, "hstree_status": "warming_up"}
        for event_id in WARMUP_IDS
    ]
    for index, event_id in enumerate(POST_IDS):
        rows.append(
            {
                "Group": GROUP,
                "source_event_id": event_id,
                "hstree_anomaly": index >= 6,  # last two are anomalies
                "hstree_status": "normal" if index < 6 else "anomaly",
            }
        )
    return rows


def v2_rows() -> List[Dict[str, Any]]:
    rows = [
        {
            "Group": GROUP,
            "source_event_id": event_id,
            "explanation_status": "warmup",
            "hstree_anomaly": None,
            "timestamp": f"2026-09-18 12:00:{index:02d}",
        }
        for index, event_id in enumerate(WARMUP_IDS, start=1)
    ]
    for index, event_id in enumerate(POST_IDS, start=5):
        rows.append(
            {
                "Group": GROUP,
                "source_event_id": event_id,
                "explanation_status": "computed",
                "hstree_anomaly": index > 10,
                # The simultaneous pair shares the timestamp.
                "timestamp": SHARED_TIMESTAMP if event_id in SIMULTANEOUS_IDS else f"2026-09-18 12:01:{index:02d}",
            }
        )
    return rows


def influx_proof() -> List[Dict[str, Any]]:
    return [
        {"timestamp": SHARED_TIMESTAMP, "Group": GROUP, "source_event_id": event_id}
        for event_id in SIMULTANEOUS_IDS
    ]


def write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> str:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
    return str(path)


def write_json(path: Path, payload: Any) -> str:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return str(path)


def run_validator(
    tmp: Path,
    manifest_payload: Dict[str, Any],
    hstree: List[Dict[str, Any]],
    v2: List[Dict[str, Any]],
    proof: List[Dict[str, Any]],
) -> Dict[str, Any]:
    manifest_path = write_json(tmp / "manifest.json", manifest_payload)
    hstree_path = write_jsonl(tmp / "hstree.jsonl", hstree)
    v2_path = write_jsonl(tmp / "v2.jsonl", v2)
    proof_path = write_json(tmp / "influx.json", {"points": proof})
    output_path = tmp / "report.json"
    exit_code = validator.main(
        [
            "--manifest", manifest_path,
            "--hstree-export", hstree_path,
            "--v2-export", v2_path,
            "--influx-proof", proof_path,
            "--output", str(output_path),
        ]
    )
    report = json.loads(output_path.read_text(encoding="utf-8"))
    report["_exit_code"] = exit_code
    return report


class OC12ValidatorTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_valid_campaign_passes(self) -> None:
        report = run_validator(self.tmp, manifest(), hstree_rows(), v2_rows(), influx_proof())

        self.assertEqual(report["_exit_code"], 0)
        self.assertTrue(report["passed"])
        self.assertEqual(report["errors"], [])
        counters = report["counters"]
        self.assertEqual(counters["hstree_events"], 12)
        self.assertEqual(counters["v2_events"], 12)
        self.assertEqual(counters["warmup_expected"], 4)
        self.assertEqual(counters["reliable_true"], 2)
        self.assertEqual(counters["reliable_false"], 6)
        self.assertEqual(report["verdict_matrix"]["normal"]["total"], 6)
        self.assertEqual(report["verdict_matrix"]["anomaly"]["total"], 2)
        self.assertEqual(report["verdict_matrix"]["anomaly"]["computed"], 2)
        self.assertEqual(len(report["simultaneous_identity"]["influx_points"]), 2)

    def test_unexpected_warmup_fails(self) -> None:
        rows = v2_rows()
        # A post-warmup event reports warmup instead of computed/failed.
        next(
            row for row in rows if row["source_event_id"] == POST_IDS[0]
        )["explanation_status"] = "warmup"
        report = run_validator(self.tmp, manifest(), hstree_rows(), rows, influx_proof())

        self.assertEqual(report["_exit_code"], 1)
        self.assertFalse(report["passed"])
        self.assertTrue(any("oc12-p001" in error for error in report["errors"]))

    def test_invalid_post_warmup_status_fails(self) -> None:
        rows = v2_rows()
        next(row for row in rows if row["source_event_id"] == POST_IDS[1])[
            "explanation_status"
        ] = "background_not_ready"
        report = run_validator(self.tmp, manifest(), hstree_rows(), rows, influx_proof())

        self.assertFalse(report["passed"])
        self.assertTrue(any("background_not_ready" in error for error in report["errors"]))

    def test_missing_v2_event_fails(self) -> None:
        rows = [row for row in v2_rows() if row["source_event_id"] != POST_IDS[2]]
        report = run_validator(self.tmp, manifest(), hstree_rows(), rows, influx_proof())

        self.assertFalse(report["passed"])
        self.assertTrue(any("oc12-p003" in error for error in report["errors"]))
        self.assertTrue(any("v2 export holds 11" in error for error in report["errors"]))

    def test_duplicate_v2_event_fails(self) -> None:
        rows = v2_rows()
        rows.append(dict(rows[0]))
        report = run_validator(self.tmp, manifest(), hstree_rows(), rows, influx_proof())

        self.assertFalse(report["passed"])
        self.assertTrue(any("duplicate v2 event" in error for error in report["errors"]))

    def test_missing_normal_or_anomaly_verdict_fails(self) -> None:
        # Only anomalies: no reliable normal verdict.
        rows = hstree_rows()
        for row in rows:
            if row["hstree_anomaly"] is False:
                row["hstree_anomaly"] = True
        report = run_validator(self.tmp, manifest(), rows, v2_rows(), influx_proof())

        self.assertFalse(report["passed"])
        self.assertTrue(any("no reliable hstree_anomaly=false" in error for error in report["errors"]))

    def test_identity_collision_without_source_event_id_fails(self) -> None:
        # The two simultaneous v2 events collapse onto the same
        # source_event_id: the collision is not protected.
        rows = v2_rows()
        for row in rows:
            if row["source_event_id"] == SIMULTANEOUS_IDS[1]:
                row["source_event_id"] = SIMULTANEOUS_IDS[0]
        report = run_validator(self.tmp, manifest(), hstree_rows(), rows, influx_proof())

        self.assertFalse(report["passed"])
        self.assertTrue(any("duplicate v2 event" in error for error in report["errors"]))

    def test_insufficient_influx_proof_fails(self) -> None:
        proof = influx_proof()[:1]  # one id only
        report = run_validator(self.tmp, manifest(), hstree_rows(), v2_rows(), proof)

        self.assertFalse(report["passed"])
        self.assertTrue(
            any("InfluxDB proof" in error and SIMULTANEOUS_IDS[1] in error for error in report["errors"])
        )
        self.assertTrue(any("at least two distinct points" in error for error in report["errors"]))

    def test_wrong_total_fails(self) -> None:
        rows = hstree_rows()[:-1]
        report = run_validator(self.tmp, manifest(), rows, v2_rows(), influx_proof())

        self.assertFalse(report["passed"])
        self.assertTrue(any("hstree export holds 11" in error for error in report["errors"]))

    def test_unexpected_v2_event_fails(self) -> None:
        rows = v2_rows()
        rows.append({"Group": GROUP, "source_event_id": "oc12-rogue", "explanation_status": "computed", "timestamp": "2026-09-18 12:02:00"})
        report = run_validator(self.tmp, manifest(), hstree_rows(), rows, influx_proof())

        self.assertFalse(report["passed"])
        self.assertTrue(any("unexpected v2 event" in error for error in report["errors"]))


class OC12FixtureCoherenceTest(unittest.TestCase):
    """The real OC-12 fixture must match its manifest."""

    def setUp(self) -> None:
        self.fixtures = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "explainability_v2"
        self.manifest = json.loads((self.fixtures / "oc12_manifest.json").read_text(encoding="utf-8"))

    def test_manifest_counts_match_csv(self) -> None:
        import csv

        with open(self.fixtures / "oc12_source.csv", newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))

        self.assertEqual(len(rows), self.manifest["expected_total_events"])
        event_ids = [row["source_event_id"] for row in rows]
        self.assertEqual(len(event_ids), len(set(event_ids)))
        self.assertEqual(
            sorted(event_ids),
            sorted(self.manifest["warmup_ids"] + self.manifest["post_warmup_ids"]),
        )

    def test_simultaneous_pair_shares_timestamp_and_group(self) -> None:
        import csv

        with open(self.fixtures / "oc12_source.csv", newline="", encoding="utf-8") as handle:
            rows = {row["source_event_id"]: row for row in csv.DictReader(handle)}

        id_a, id_b = self.manifest["simultaneous_ids"]
        self.assertEqual(rows[id_a]["timestamp"], rows[id_b]["timestamp"])
        self.assertEqual(rows[id_a]["timestamp"], self.manifest["simultaneous_timestamp"])
        self.assertEqual(rows[id_a]["Group"], rows[id_b]["Group"])
        self.assertNotEqual(id_a, id_b)

    def test_manifest_declares_both_verdicts_and_short_threshold(self) -> None:
        env = self.manifest["campaign_env"]
        self.assertEqual(env["DASMA_HSTREE_ENABLED"], "1")
        self.assertEqual(env["DASMA_EXPLAINABILITY_V2_ENABLED"], "1")
        self.assertEqual(env["HSTREE_ANOMALY_MIN_SAMPLES"], "2")
        self.assertEqual(env["HSTREE_INITIAL_WINDOW_SIZE"], "100")
        self.assertGreaterEqual(
            int(env["HSTREE_INITIAL_WINDOW_SIZE"]), 100
        )  # SHAP_BACKGROUND_SIZE is fixed at 100 in compose


class OC12ManifestContractTest(unittest.TestCase):
    """OC-12R phase A: the validator must enforce the full manifest contract."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_warmup_id_with_non_null_verdict_fails(self) -> None:
        rows = hstree_rows()
        next(row for row in rows if row["source_event_id"] == WARMUP_IDS[0])["hstree_anomaly"] = True
        report = run_validator(self.tmp, manifest(), rows, v2_rows(), influx_proof())

        self.assertFalse(report["passed"])
        self.assertTrue(any("warmup" in error and WARMUP_IDS[0] in error for error in report["errors"]))

    def test_post_warmup_id_with_null_verdict_fails(self) -> None:
        rows = hstree_rows()
        next(row for row in rows if row["source_event_id"] == POST_IDS[0])["hstree_anomaly"] = None
        report = run_validator(self.tmp, manifest(), rows, v2_rows(), influx_proof())

        self.assertFalse(report["passed"])
        self.assertTrue(any(POST_IDS[0] in error and ("null" in error or "boolean" in error) for error in report["errors"]))

    def test_overlapping_warmup_and_post_warmup_ids_fail(self) -> None:
        payload = manifest()
        payload["warmup_ids"] = WARMUP_IDS + [POST_IDS[0]]
        report = run_validator(self.tmp, payload, hstree_rows(), v2_rows(), influx_proof())

        self.assertFalse(report["passed"])
        self.assertTrue(any("disjoint" in error.lower() or "overlap" in error.lower() for error in report["errors"]))

    def test_duplicate_id_within_a_list_fails(self) -> None:
        payload = manifest()
        payload["post_warmup_ids"] = POST_IDS + [POST_IDS[0]]
        report = run_validator(self.tmp, payload, hstree_rows(), v2_rows(), influx_proof())

        self.assertFalse(report["passed"])
        self.assertTrue(any("unique" in error.lower() or "duplicate" in error.lower() for error in report["errors"]))

    def test_id_union_mismatching_total_fails(self) -> None:
        payload = manifest()
        payload["expected_total_events"] = len(WARMUP_IDS) + len(POST_IDS) + 1
        report = run_validator(self.tmp, payload, hstree_rows(), v2_rows(), influx_proof())

        self.assertFalse(report["passed"])
        self.assertTrue(any("expected_total" in error or "union" in error.lower() for error in report["errors"]))

    def test_simultaneous_ids_outside_post_warmup_fail(self) -> None:
        payload = manifest()
        payload["simultaneous_ids"] = [WARMUP_IDS[0], WARMUP_IDS[1]]
        report = run_validator(self.tmp, payload, hstree_rows(), v2_rows(), influx_proof())

        self.assertFalse(report["passed"])
        self.assertTrue(any("post-warmup" in error or "post_warmup" in error for error in report["errors"]))


if __name__ == "__main__":
    unittest.main()
