"""Reconciliation tool tests for explainability-v2 (OC-05).

The fixture covers: a normal computed point, an anomaly failed point, a normal
warmup point (non eligible), an anomaly missing from v2, and a duplicated v2
output on the normal computed point. The asserted contract:

- the only reconciliation key is (Group, source_event_id); the timestamp never
  participates in the identity;
- raw totals count every read row; duplicates are reported and never counted
  as a unique match;
- the v2 status vocabulary is enforced; warmup / background_not_ready /
  disabled are non eligible;
- invalid keys, statuses or verdicts fail with an explicit file:line error;
- .csv, .jsonl and .json inputs produce the same report; the report is fully
  sorted and deterministic.
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path

from dasma_kafka.explainability_v2.config import EXPLANATION_STATUSES

RECONCILE_PATH = (
    Path(__file__).resolve().parents[1]
    / "experiments"
    / "explainability"
    / "v2"
    / "reconcile_events.py"
)
_SPEC = importlib.util.spec_from_file_location("reconcile_events", RECONCILE_PATH)
reconcile = importlib.util.module_from_spec(_SPEC)
sys.modules["reconcile_events"] = reconcile
_SPEC.loader.exec_module(reconcile)

GROUP = "Group"
EVENT_ID = "source_event_id"
ANOMALY = "hstree_anomaly"
STATUS = "explanation_status"


def _fixture_rows() -> tuple:
    """Normal computed, anomaly failed, normal warmup, anomaly missing, v2 duplicate."""
    hstree_rows = [
        {GROUP: "G1", EVENT_ID: "row-1", ANOMALY: False, "timestamp": "2026-01-01 10:00:01"},
        {GROUP: "G1", EVENT_ID: "row-2", ANOMALY: True, "timestamp": "2026-01-01 10:00:02"},
        {GROUP: "G1", EVENT_ID: "row-3", ANOMALY: False, "timestamp": "2026-01-01 10:00:03"},
        {GROUP: "G1", EVENT_ID: "row-4", ANOMALY: True, "timestamp": "2026-01-01 10:00:04"},
    ]
    v2_rows = [
        {GROUP: "G1", EVENT_ID: "row-1", STATUS: "computed", "timestamp": "2026-01-01 10:00:21"},
        {GROUP: "G1", EVENT_ID: "row-1", STATUS: "computed", "timestamp": "2026-01-01 10:00:22"},
        {GROUP: "G1", EVENT_ID: "row-2", STATUS: "failed", "timestamp": "2026-01-01 10:00:22"},
        {GROUP: "G1", EVENT_ID: "row-3", STATUS: "warmup", "timestamp": "2026-01-01 10:00:23"},
    ]
    return hstree_rows, v2_rows


def _build_report(hstree_rows, v2_rows):
    hstree = reconcile.index_hstree_rows(
        [(i + 1, row) for i, row in enumerate(hstree_rows)], source_name="hstree"
    )
    v2 = reconcile.index_v2_rows(
        [(i + 1, row) for i, row in enumerate(v2_rows)], source_name="v2"
    )
    return reconcile.build_reconciliation_report(hstree, v2)


def _write_jsonl(path: Path, rows) -> None:
    path.write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8"
    )


def _write_json(path: Path, rows) -> None:
    path.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")


def _write_csv(path: Path, header: list, rows: list) -> None:
    lines = [",".join(header)]
    for row in rows:
        lines.append(",".join(str(row[column]) for column in header))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


class ReconciliationCoreTest(unittest.TestCase):
    def test_global_counts_key_and_duplicates(self) -> None:
        hstree_rows, v2_rows = _fixture_rows()
        report = _build_report(hstree_rows, v2_rows)

        self.assertEqual(report["hstree_total"], 4)
        self.assertEqual(report["v2_total"], 4)
        self.assertEqual(report["hstree_unique"], 4)
        self.assertEqual(report["v2_unique"], 3)
        self.assertEqual(report["matched_unique"], [["G1", "row-2"], ["G1", "row-3"]])
        self.assertEqual(report["missing_v2"], [["G1", "row-4"]])
        self.assertEqual(report["unexpected_v2"], [])
        self.assertEqual(report["duplicate_hstree_keys"], [])
        self.assertEqual(report["duplicate_v2_keys"], [["G1", "row-1"]])
        self.assertEqual(
            report["v2_status_distribution"],
            {"computed": 2, "failed": 1, "warmup": 1},
        )
        # A duplicated v2 output is never counted as a unique match.
        self.assertNotIn(["G1", "row-1"], report["matched_unique"])

    def test_verdict_split_statuses_and_missing(self) -> None:
        hstree_rows, v2_rows = _fixture_rows()
        report = _build_report(hstree_rows, v2_rows)

        # row-1 (normal) has a duplicated v2 output: its status is ambiguous,
        # it feeds ambiguous_v2, never computed/failed/non_eligible.
        self.assertEqual(
            report["verdict_matrix"]["normal"],
            {
                "total": 2,
                "computed": 0,
                "failed": 0,
                "model_mismatch": 0,
                "non_eligible": 1,
                "ambiguous_v2": 1,
                "missing_v2": 0,
            },
        )
        self.assertEqual(
            report["verdict_matrix"]["anomaly"],
            {
                "total": 2,
                "computed": 0,
                "failed": 1,
                "model_mismatch": 0,
                "non_eligible": 0,
                "ambiguous_v2": 0,
                "missing_v2": 1,
            },
        )
        # The fixture has no HSTree duplicate: the dedicated section is empty.
        self.assertEqual(
            report["verdict_matrix"]["ambiguous_hstree"],
            {"total": 0, "keys": []},
        )

    def test_warmup_is_non_eligible_not_computed(self) -> None:
        hstree_rows, v2_rows = _fixture_rows()
        report = _build_report(hstree_rows, v2_rows)

        self.assertEqual(report["verdict_matrix"]["normal"]["computed"], 0)
        self.assertEqual(report["verdict_matrix"]["normal"]["non_eligible"], 1)
        self.assertEqual(report["verdict_matrix"]["normal"]["failed"], 0)
        self.assertEqual(report["verdict_matrix"]["normal"]["ambiguous_v2"], 1)

    def test_model_mismatch_is_distinct_from_non_eligible(self) -> None:
        hstree_rows = [
            {GROUP: "G1", EVENT_ID: "row-1", ANOMALY: False},
            {GROUP: "G1", EVENT_ID: "row-2", ANOMALY: True},
        ]
        v2_rows = [
            {GROUP: "G1", EVENT_ID: "row-1", STATUS: "model_mismatch"},
            {GROUP: "G1", EVENT_ID: "row-2", STATUS: "warmup"},
        ]

        report = _build_report(hstree_rows, v2_rows)
        normal = report["verdict_matrix"]["normal"]
        anomaly = report["verdict_matrix"]["anomaly"]

        self.assertEqual(normal["model_mismatch"], 1)
        self.assertEqual(normal["non_eligible"], 0)
        self.assertEqual(normal["computed"], 0)
        self.assertEqual(anomaly["model_mismatch"], 0)
        self.assertEqual(anomaly["non_eligible"], 1)
        self.assertEqual(report["v2_status_distribution"], {
            "model_mismatch": 1,
            "warmup": 1,
        })

    def test_duplicated_hstree_with_conflicting_verdicts_is_ambiguous_and_order_independent(self) -> None:
        hstree_rows = [
            {GROUP: "G1", EVENT_ID: "row-1", ANOMALY: False},
            {GROUP: "G1", EVENT_ID: "row-1", ANOMALY: True},
            {GROUP: "G1", EVENT_ID: "row-2", ANOMALY: True},
        ]
        v2_rows = [
            {GROUP: "G1", EVENT_ID: "row-1", STATUS: "computed"},
            {GROUP: "G1", EVENT_ID: "row-2", STATUS: "computed"},
        ]
        report = _build_report(hstree_rows, v2_rows)
        report_reversed = _build_report(list(reversed(hstree_rows)), v2_rows)

        # Identical reports whatever the read order of conflicting verdicts.
        self.assertEqual(
            json.dumps(report, sort_keys=True),
            json.dumps(report_reversed, sort_keys=True),
        )
        # The ambiguous key is reported in the dedicated deterministic section,
        # never arbitrarily bucketed as normal or anomaly.
        self.assertEqual(
            report["verdict_matrix"]["ambiguous_hstree"],
            {"total": 1, "keys": [["G1", "row-1"]]},
        )
        self.assertEqual(
            report["verdict_matrix"]["normal"],
            {
                "total": 0,
                "computed": 0,
                "failed": 0,
                "model_mismatch": 0,
                "non_eligible": 0,
                "ambiguous_v2": 0,
                "missing_v2": 0,
            },
        )
        # The reliable key keeps feeding the anomaly bucket normally.
        self.assertEqual(
            report["verdict_matrix"]["anomaly"],
            {
                "total": 1,
                "computed": 1,
                "failed": 0,
                "model_mismatch": 0,
                "non_eligible": 0,
                "ambiguous_v2": 0,
                "missing_v2": 0,
            },
        )
        self.assertEqual(report["duplicate_hstree_keys"], [["G1", "row-1"]])
        self.assertNotIn(["G1", "row-1"], report["matched_unique"])

    def test_duplicated_hstree_with_identical_verdicts_is_also_ambiguous(self) -> None:
        # Even when the duplicates agree, the source verdict is ambiguous and
        # must not feed the matrix.
        hstree_rows = [
            {GROUP: "G1", EVENT_ID: "row-1", ANOMALY: False},
            {GROUP: "G1", EVENT_ID: "row-1", ANOMALY: False},
        ]
        report = _build_report(hstree_rows, [])

        self.assertEqual(
            report["verdict_matrix"]["ambiguous_hstree"],
            {"total": 1, "keys": [["G1", "row-1"]]},
        )
        # Not bucketed, and not counted as missing either.
        self.assertEqual(
            report["verdict_matrix"]["normal"],
            {
                "total": 0,
                "computed": 0,
                "failed": 0,
                "model_mismatch": 0,
                "non_eligible": 0,
                "ambiguous_v2": 0,
                "missing_v2": 0,
            },
        )
        self.assertEqual(report["missing_v2"], [])
        self.assertEqual(report["duplicate_hstree_keys"], [["G1", "row-1"]])
        self.assertNotIn(["G1", "row-1"], report["matched_unique"])

    def test_duplicated_v2_with_conflicting_statuses_is_ambiguous_and_order_independent(self) -> None:
        hstree_rows = [
            {GROUP: "G1", EVENT_ID: "row-1", ANOMALY: False},
            {GROUP: "G1", EVENT_ID: "row-2", ANOMALY: True},
        ]
        v2_rows = [
            {GROUP: "G1", EVENT_ID: "row-1", STATUS: "computed"},
            {GROUP: "G1", EVENT_ID: "row-1", STATUS: "failed"},
            {GROUP: "G1", EVENT_ID: "row-2", STATUS: "computed"},
        ]
        v2_rows_reversed = [
            {GROUP: "G1", EVENT_ID: "row-1", STATUS: "failed"},
            {GROUP: "G1", EVENT_ID: "row-1", STATUS: "computed"},
            {GROUP: "G1", EVENT_ID: "row-2", STATUS: "computed"},
        ]
        report = _build_report(hstree_rows, v2_rows)
        report_reversed = _build_report(hstree_rows, v2_rows_reversed)

        # Identical reports whatever the read order of conflicting duplicates.
        self.assertEqual(
            json.dumps(report, sort_keys=True),
            json.dumps(report_reversed, sort_keys=True),
        )
        normal = report["verdict_matrix"]["normal"]
        self.assertEqual(normal["ambiguous_v2"], 1)
        self.assertEqual(normal["computed"], 0)
        self.assertEqual(normal["failed"], 0)
        self.assertEqual(normal["non_eligible"], 0)
        # A single-status key keeps feeding the matrix normally.
        self.assertEqual(report["verdict_matrix"]["anomaly"]["computed"], 1)
        self.assertEqual(report["duplicate_v2_keys"], [["G1", "row-1"]])
        self.assertNotIn(["G1", "row-1"], report["matched_unique"])

    def test_duplicated_v2_with_identical_statuses_is_also_ambiguous(self) -> None:
        # Even when the duplicates agree, the key must not silently feed the
        # coverage counters.
        hstree_rows = [{GROUP: "G1", EVENT_ID: "row-1", ANOMALY: False}]
        v2_rows = [
            {GROUP: "G1", EVENT_ID: "row-1", STATUS: "computed"},
            {GROUP: "G1", EVENT_ID: "row-1", STATUS: "computed"},
        ]
        report = _build_report(hstree_rows, v2_rows)

        self.assertEqual(
            report["verdict_matrix"]["normal"],
            {
                "total": 1,
                "computed": 0,
                "failed": 0,
                "model_mismatch": 0,
                "non_eligible": 0,
                "ambiguous_v2": 1,
                "missing_v2": 0,
            },
        )

    def test_timestamp_never_participates_in_the_identity(self) -> None:
        hstree_rows, v2_rows = _fixture_rows()
        for row in hstree_rows:
            row["timestamp"] = "1999-12-31 23:59:59"
        for row in v2_rows:
            row["timestamp"] = "2099-01-01 00:00:00"

        report = _build_report(hstree_rows, v2_rows)
        self.assertEqual(report["matched_unique"], [["G1", "row-2"], ["G1", "row-3"]])
        self.assertEqual(report["missing_v2"], [["G1", "row-4"]])

    def test_report_is_fully_sorted_and_deterministic(self) -> None:
        hstree_rows, v2_rows = _fixture_rows()
        report_first = _build_report(hstree_rows, v2_rows)
        report_second = _build_report(list(reversed(hstree_rows)), list(reversed(v2_rows)))

        self.assertEqual(
            json.dumps(report_first, sort_keys=True),
            json.dumps(report_second, sort_keys=True),
        )
        for name in (
            "matched_unique",
            "missing_v2",
            "unexpected_v2",
            "duplicate_hstree_keys",
            "duplicate_v2_keys",
        ):
            keys = [tuple(key) for key in report_first[name]]
            self.assertEqual(keys, sorted(keys), name)

    def test_two_groups_are_not_mixed(self) -> None:
        hstree_rows = [
            {GROUP: "A", EVENT_ID: "row-1", ANOMALY: False},
            {GROUP: "B", EVENT_ID: "row-1", ANOMALY: True},
        ]
        v2_rows = [
            {GROUP: "A", EVENT_ID: "row-1", STATUS: "computed"},
            {GROUP: "B", EVENT_ID: "row-1", STATUS: "failed"},
        ]
        report = _build_report(hstree_rows, v2_rows)

        self.assertEqual(len(report["matched_unique"]), 2)
        self.assertEqual(report["verdict_matrix"]["normal"]["computed"], 1)
        self.assertEqual(report["verdict_matrix"]["anomaly"]["failed"], 1)

    def test_unexpected_v2_keys_are_reported(self) -> None:
        hstree_rows = [{GROUP: "G1", EVENT_ID: "row-1", ANOMALY: False}]
        v2_rows = [
            {GROUP: "G1", EVENT_ID: "row-1", STATUS: "computed"},
            {GROUP: "G1", EVENT_ID: "row-9", STATUS: "warmup"},
        ]
        report = _build_report(hstree_rows, v2_rows)

        self.assertEqual(report["unexpected_v2"], [["G1", "row-9"]])
        self.assertEqual(report["v2_status_distribution"], {"computed": 1, "warmup": 1})


class ReconciliationFormatTest(unittest.TestCase):
    def test_csv_jsonl_and_json_produce_the_same_report(self) -> None:
        hstree_rows, v2_rows = _fixture_rows()
        expected = _build_report(hstree_rows, v2_rows)

        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            # CSV booleans use the exact spellings 'true' / 'false'.
            _write_csv(
                directory / "hstree.csv",
                [GROUP, EVENT_ID, ANOMALY, "timestamp"],
                [{**row, ANOMALY: "true" if row[ANOMALY] else "false"} for row in hstree_rows],
            )
            _write_csv(
                directory / "v2.csv",
                [GROUP, EVENT_ID, STATUS, "timestamp"],
                [{**row, ANOMALY: "false"} for row in v2_rows],
            )
            _write_jsonl(directory / "hstree.jsonl", hstree_rows)
            _write_jsonl(directory / "v2.jsonl", v2_rows)
            _write_json(directory / "hstree.json", hstree_rows)
            _write_json(directory / "v2.json", v2_rows)

            for fmt in ("csv", "jsonl", "json"):
                with self.subTest(fmt=fmt):
                    hstree = reconcile.load_hstree_index(str(directory / f"hstree.{fmt}"))
                    v2 = reconcile.load_v2_index(str(directory / f"v2.{fmt}"))
                    report = reconcile.build_reconciliation_report(hstree, v2)
                    self.assertEqual(
                        json.dumps(report, sort_keys=True),
                        json.dumps(expected, sort_keys=True),
                        fmt,
                    )


class ReconciliationValidationTest(unittest.TestCase):
    def test_invalid_key_fails_with_file_and_line(self) -> None:
        for invalid in (
            {GROUP: "G1", EVENT_ID: "   "},
            {GROUP: "", EVENT_ID: "row-1"},
            {GROUP: 42, EVENT_ID: "row-1"},
            {GROUP: "G1"},
        ):
            with self.subTest(row=invalid):
                with self.assertRaises(reconcile.ReconciliationInputError) as ctx:
                    reconcile.index_hstree_rows(
                        [(7, invalid)], source_name="hstree.jsonl"
                    )
                self.assertIn("hstree.jsonl:7", str(ctx.exception))

    def test_invalid_status_fails_with_file_and_line(self) -> None:
        with self.assertRaises(reconcile.ReconciliationInputError) as ctx:
            reconcile.index_v2_rows(
                [(3, {GROUP: "G1", EVENT_ID: "row-1", STATUS: "bogus"})],
                source_name="v2.jsonl",
            )
        self.assertIn("v2.jsonl:3", str(ctx.exception))
        self.assertIn("bogus", str(ctx.exception))

    def test_hstree_verdict_must_be_an_explicit_boolean(self) -> None:
        for invalid in ("True", "yes", 1, None, ""):
            with self.subTest(anomaly=invalid):
                with self.assertRaises(reconcile.ReconciliationInputError):
                    reconcile.index_hstree_rows(
                        [(2, {GROUP: "G1", EVENT_ID: "row-1", ANOMALY: invalid})],
                        source_name="hstree.jsonl",
                    )

    def test_csv_boolean_spelling_is_strictly_lower_case(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "hstree.csv"
            path.write_text(
                "Group,source_event_id,hstree_anomaly\nG1,row-1,True\n",
                encoding="utf-8",
            )
            with self.assertRaises(reconcile.ReconciliationInputError) as ctx:
                reconcile.load_hstree_index(str(path))
            self.assertIn("hstree.csv:2", str(ctx.exception))

    def test_unsupported_extension_and_missing_file_fail_explicitly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "hstree.txt"
            path.write_text("hello", encoding="utf-8")
            with self.assertRaises(reconcile.ReconciliationInputError):
                reconcile.read_rows_with_lines(str(path))
            with self.assertRaises(reconcile.ReconciliationInputError):
                reconcile.read_rows_with_lines(str(Path(tmp) / "absent.jsonl"))


class ReconciliationCliTest(unittest.TestCase):
    def test_cli_exits_zero_with_missing_and_duplicates_and_writes_report(self) -> None:
        hstree_rows, v2_rows = _fixture_rows()
        with tempfile.TemporaryDirectory() as tmp:
            hstree_path = Path(tmp) / "hstree.jsonl"
            v2_path = Path(tmp) / "v2.jsonl"
            report_path = Path(tmp) / "report.json"
            _write_jsonl(hstree_path, hstree_rows)
            _write_jsonl(v2_path, v2_rows)

            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                code = reconcile.main(
                    [
                        "--hstree-input", str(hstree_path),
                        "--v2-output", str(v2_path),
                        "--report", str(report_path),
                    ]
                )

            self.assertEqual(code, 0)
            summary = stdout.getvalue()
            self.assertIn("missing_v2=1", summary)
            self.assertIn("duplicate_v2_keys=1", summary)
            self.assertIn("computed: normal=0 anomaly=0", summary)
            self.assertIn("failed: normal=0 anomaly=1", summary)
            self.assertIn("model_mismatch: normal=0 anomaly=0", summary)
            written = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(written["v2_total"], 4)
            self.assertEqual(written["verdict_matrix"]["anomaly"]["failed"], 1)

    def test_cli_invalid_input_returns_non_zero(self) -> None:
        hstree_rows, v2_rows = _fixture_rows()
        v2_rows = list(v2_rows) + [
            {GROUP: "G1", EVENT_ID: "row-9", STATUS: "not-a-status"}
        ]
        with tempfile.TemporaryDirectory() as tmp:
            hstree_path = Path(tmp) / "hstree.jsonl"
            v2_path = Path(tmp) / "v2.jsonl"
            _write_jsonl(hstree_path, hstree_rows)
            _write_jsonl(v2_path, v2_rows)

            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                code = reconcile.main(
                    ["--hstree-input", str(hstree_path), "--v2-output", str(v2_path)]
                )

            self.assertNotEqual(code, 0)
            self.assertIn("not-a-status", stderr.getvalue())
            self.assertIn("v2.jsonl:5", stderr.getvalue())

    def test_cli_missing_file_returns_non_zero(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                code = reconcile.main(
                    [
                        "--hstree-input", str(Path(tmp) / "absent.jsonl"),
                        "--v2-output", str(Path(tmp) / "absent.jsonl"),
                    ]
                )
            self.assertNotEqual(code, 0)
            self.assertIn("not found", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
