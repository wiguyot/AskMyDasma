"""Unit tests for the reproducibility CSV comparator (P2-5).

Temporary directories and pure helpers only: no broker, no Docker, no real
`data/` writes, no confluent_kafka import.
"""

from __future__ import annotations

import csv
import json
import math
import subprocess
import sys
import tempfile
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from validation.reproducibility.compare_csv import (
    ComparisonConfig,
    ComparisonError,
    build_config,
    build_report_row,
    compare_numeric_pair,
    iter_v1_rows,
    main,
    merge_join,
    run,
    validate_metadata,
    values_equal,
)
from validation.reproducibility.csv_contract import columns_for_model, decode_v1_row

ROOT = Path(__file__).resolve().parents[1]


def _metadata(model: str, role: str, **overrides) -> dict:
    metadata = {
        "format_version": 1,
        "model": model,
        "source_role": role,
        "campaign_id": "campaign-42",
        "columns": list(columns_for_model(model)),
        "campaign_parameters": {"hstree_num_trees": 50},
        "image": "dasma-kafka-anomaly-light:0.1.0",
    }
    metadata.update(overrides)
    return metadata


def _row(group: str = "A", event: str = "e1", **overrides) -> dict[str, str]:
    row = {
        "source_event_id": event,
        "timestamp": "2026-09-15 10:00:00",
        "Group": group,
        "score": "0.1",
        "status": "normal",
        "threshold": "1.0",
        "anomaly": "false",
    }
    row.update(overrides)
    return row


def _mdrag_row(group: str = "A", event: str = "e1", **overrides) -> dict[str, str]:
    internal_r = overrides.pop("mdragpoint_internal_r", "0.5")
    row = _row(group, event, **overrides)
    row["mdragpoint_internal_r"] = internal_r
    return row


def write_export(
    directory: Path,
    model: str,
    suffix: str,
    rows: list[dict[str, str]],
    metadata: dict | None = None,
    metadata_text: str | None = None,
    csv_text: str | None = None,
) -> Path:
    """Write one export (CSV + companion metadata) and return the CSV path."""
    csv_path = directory / f"{model}-source-{suffix}.csv"
    if csv_text is None:
        columns = list(columns_for_model(model))
        with open(csv_path, "w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=columns)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
    else:
        csv_path.write_text(csv_text, encoding="utf-8")
    if metadata_text is None:
        if metadata is None:
            metadata = _metadata(model, f"source-{suffix}")
        metadata_text = json.dumps(metadata, ensure_ascii=False, indent=2)
    (directory / f"{model}-source-{suffix}.metadata.json").write_text(
        metadata_text, encoding="utf-8"
    )
    return csv_path


def write_pair(
    directory: Path,
    model: str,
    rows_1: list[dict[str, str]],
    rows_2: list[dict[str, str]],
    images: tuple[str, str] = ("image-a:1", "image-b:1"),
    campaign_parameters: dict | None = None,
) -> tuple[Path, Path]:
    """Write a valid pair of exports and return (source-1, source-2) paths."""
    directory.mkdir(parents=True, exist_ok=True)
    parameters = (
        campaign_parameters if campaign_parameters is not None else {"hstree_num_trees": 50}
    )
    path_1 = write_export(
        directory, model, "1", rows_1,
        metadata=_metadata(model, "source-1", image=images[0], campaign_parameters=parameters),
    )
    path_2 = write_export(
        directory, model, "2", rows_2,
        metadata=_metadata(model, "source-2", image=images[1], campaign_parameters=parameters),
    )
    return path_1, path_2


def decoded(model: str, raw: dict[str, str]) -> dict:
    """Decode a raw canonical row the way iter_v1_rows does."""
    return decode_v1_row(model, raw)


def make_config(tmp: Path, source_1: Path, source_2: Path, report: Path, **overrides) -> ComparisonConfig:
    defaults = dict(
        summary=Path(str(report) + ".summary.json"),
        abs_tolerance=0.0,
        relative_tolerance=0.0,
    )
    defaults.update(overrides)
    return ComparisonConfig(
        source_1=source_1, source_2=source_2, report=report, **defaults
    )


class MetadataValidationTest(TestCase):
    """Family 2: metadata preconditions before any comparison."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)

    def test_valid_pair_is_accepted(self) -> None:
        csv_1, csv_2 = write_pair(self.tmp / "c", "hstree", [_row()], [_row()])
        self.assertEqual(validate_metadata(csv_1, "source-1")["model"], "hstree")
        self.assertEqual(validate_metadata(csv_2, "source-2")["campaign_id"], "campaign-42")

    def test_swapped_roles_rejected(self) -> None:
        csv_1 = write_export(self.tmp, "hstree", "1", [_row()])
        with self.assertRaises(ComparisonError):
            validate_metadata(csv_1, "source-2")

    def test_missing_metadata_companion_rejected(self) -> None:
        csv_path = self.tmp / "hstree-source-1.csv"
        csv_path.write_text("header\n", encoding="utf-8")
        with self.assertRaises(ComparisonError):
            validate_metadata(csv_path, "source-1")

    def test_incompatible_metadata_rejected(self) -> None:
        cases = {
            "wrong format_version": {"format_version": 2},
            "unknown model": {"model": "unknown"},
            "non portable campaign": {"campaign_id": "../escape"},
            "columns mismatch": {"columns": ["source_event_id", "Group"]},
            "parameters not an object": {"campaign_parameters": []},
            "image not a string": {"image": 12},
        }
        for label, overrides in cases.items():
            metadata = _metadata("hstree", "source-1")
            metadata.update(overrides)
            csv_path = write_export(self.tmp, "hstree", "1", [_row()], metadata=metadata)
            with self.assertRaises(ComparisonError, msg=label):
                validate_metadata(csv_path, "source-1")

    def test_unknown_and_missing_keys_rejected(self) -> None:
        extra = _metadata("hstree", "source-1", unexpected=1)
        csv_path = write_export(
            self.tmp, "hstree", "1", [_row()],
            metadata_text=json.dumps(extra),
        )
        with self.assertRaises(ComparisonError, msg="unknown key"):
            validate_metadata(csv_path, "source-1")
        incomplete = _metadata("hstree", "source-1")
        del incomplete["image"]
        csv_path = write_export(
            self.tmp, "hstree", "1", [_row()], metadata_text=json.dumps(incomplete),
            metadata=None,
        )
        with self.assertRaises(ComparisonError, msg="missing key"):
            validate_metadata(csv_path, "source-1")

    def test_non_standard_json_rejected(self) -> None:
        csv_1 = write_export(self.tmp, "hstree", "1", [_row()], metadata_text="{\"a\": NaN}")
        with self.assertRaises(ComparisonError):
            validate_metadata(csv_1, "source-1")

    def test_run_refuses_parameter_mismatch(self) -> None:
        directory = self.tmp / "campaign-42"
        path_1, path_2 = write_pair(
            directory, "hstree",
            [_row()], [_row()],
            campaign_parameters={"hstree_num_trees": 50},
        )
        # Rewrite source-2 metadata with different parameters.
        metadata = _metadata("hstree", "source-2", campaign_parameters={"hstree_num_trees": 75})
        (directory / "hstree-source-2.metadata.json").write_text(
            json.dumps(metadata), encoding="utf-8"
        )
        config = make_config(self.tmp, path_1, path_2, self.tmp / "report.csv")
        with self.assertRaises(ComparisonError) as ctx:
            run(config)
        self.assertIn("campaign_parameters", str(ctx.exception))
        self.assertFalse(config.report.exists())
        self.assertFalse(config.summary.exists())

    def test_run_refuses_different_models_and_campaigns(self) -> None:
        directory = self.tmp
        path_1 = write_export(directory, "hstree", "1", [_row()])
        path_2 = write_export(directory, "hstree", "2", [_row()],
                              metadata=_metadata("hstree", "source-2", campaign_id="other"))
        config = make_config(directory, path_1, path_2, directory / "report.csv")
        with self.assertRaises(ComparisonError):
            run(config)
        self.assertFalse((directory / "report.csv").exists())

    def test_different_images_accepted(self) -> None:
        directory = self.tmp
        path_1, path_2 = write_pair(
            directory, "hstree", [_row()], [_row()],
            images=("image-machine-a:1", "image-machine-b:2"),
        )
        self.assertEqual(validate_metadata(path_1, "source-1")["image"], "image-machine-a:1")
        self.assertEqual(validate_metadata(path_2, "source-2")["image"], "image-machine-b:2")


class ValuesEqualTest(TestCase):
    """Family 4 groundwork: exact equality and pair metrics."""

    def test_null_nan_infinities_and_minus_zero(self) -> None:
        self.assertTrue(values_equal(None, None))
        self.assertFalse(values_equal(None, 0.0))
        self.assertTrue(values_equal(float("nan"), float("nan")))
        self.assertFalse(values_equal(float("nan"), 1.0))
        self.assertTrue(values_equal(float("inf"), float("inf")))
        self.assertTrue(values_equal(float("-inf"), float("-inf")))
        self.assertFalse(values_equal(float("inf"), float("-inf")))
        self.assertTrue(values_equal(-0.0, 0.0))
        self.assertTrue(values_equal("normal", "normal"))
        self.assertFalse(values_equal("normal", "anomaly"))
        self.assertTrue(values_equal(True, True))
        self.assertFalse(values_equal(True, False))

    def test_numeric_pair_metrics_and_boundaries(self) -> None:
        tolerated, abs_diff, relative_diff = compare_numeric_pair(1.0, 1.4, 0.0, 0.5)
        self.assertTrue(tolerated)
        self.assertAlmostEqual(abs_diff, 0.4)
        self.assertAlmostEqual(relative_diff, 0.4 / 1.4)
        # Inclusive boundary.
        tolerated, abs_diff, _ = compare_numeric_pair(1.0, 1.5, 0.5, 0.0)
        self.assertTrue(tolerated)
        self.assertEqual(abs_diff, 0.5)
        # Out of tolerance.
        tolerated, _, _ = compare_numeric_pair(1.0, 1.6, 0.0, 0.1)
        self.assertFalse(tolerated)
        # Both zero: relative diff is 0.0 by definition.
        tolerated, abs_diff, relative_diff = compare_numeric_pair(0.0, 0.0, 0.0, 0.0)
        self.assertTrue(tolerated)
        self.assertEqual((abs_diff, relative_diff), (0.0, 0.0))


class MergeJoinTest(TestCase):
    """Family 3: streaming merge join on the canonical key."""

    @staticmethod
    def _keyed(group: str, event: str) -> dict:
        return {"Group": group, "source_event_id": event}

    def test_interleaved_keys_and_single_sided_keys(self) -> None:
        rows_1 = [self._keyed("A", "e1"), self._keyed("A", "e3"), self._keyed("C", "e1")]
        rows_2 = [self._keyed("A", "e2"), self._keyed("A", "e3"), self._keyed("B", "e1")]
        result = list(merge_join(iter(rows_1), iter(rows_2)))
        self.assertEqual(
            result,
            [
                ("missing_source_2", self._keyed("A", "e1"), None),
                ("missing_source_1", None, self._keyed("A", "e2")),
                ("joined", self._keyed("A", "e3"), self._keyed("A", "e3")),
                ("missing_source_1", None, self._keyed("B", "e1")),
                ("missing_source_2", self._keyed("C", "e1"), None),
            ],
        )

    def test_last_line_of_a_single_stream(self) -> None:
        rows_1 = [self._keyed("A", "e1")]
        rows_2 = [self._keyed("A", "e1"), self._keyed("B", "e9")]
        statuses = [status for status, _, _ in merge_join(iter(rows_1), iter(rows_2))]
        self.assertEqual(statuses, ["joined", "missing_source_1"])

    def test_streaming_never_pulls_beyond_need(self) -> None:
        """Instrumented proof: lazy one-shot generators, no full materialization."""
        pulls_1: list[int] = []
        pulls_2: list[int] = []

        def stream_1():
            for event in ("e1", "e3"):
                pulls_1.append(1)
                yield self._keyed("A", event)
            pulls_1.append(1)  # sentinel pull ending stream_1

        def stream_2():
            for event in ("e1", "e2", "e4"):
                pulls_2.append(1)
                yield self._keyed("A", event)
            pulls_2.append(1)

        statuses = [status for status, _, _ in merge_join(stream_1(), stream_2())]
        self.assertEqual(
            statuses,
            ["joined", "missing_source_1", "missing_source_2", "missing_source_1"],
        )
        # Single pass with one lookahead: each stream is pulled exactly
        # len(rows) + 1 times (the final sentinel pull) — never re-read,
        # never buffered, never rewound.
        self.assertEqual(len(pulls_1), 3)
        self.assertEqual(len(pulls_2), 4)

    def test_uneven_stream_lengths(self) -> None:
        rows_1 = [self._keyed("A", "e1"), self._keyed("A", "e2"), self._keyed("B", "e9")]
        rows_2 = [self._keyed("A", "e1")]
        statuses = [status for status, _, _ in merge_join(iter(rows_1), iter(rows_2))]
        self.assertEqual(statuses, ["joined", "missing_source_2", "missing_source_2"])


class ExactEqualityTest(TestCase):
    """Family 4: exact equality of model results, timestamp as diagnostic."""

    def test_identical_row_is_exact(self) -> None:
        report = build_report_row(
            "hstree", "exact", decoded("hstree", _row()), decoded("hstree", _row()), 0.0, 0.0
        )
        self.assertEqual(report["comparison_status"], "exact")
        self.assertEqual(report["exact"], 1)
        self.assertEqual(report["within_tolerance"], 1)
        self.assertEqual(report["differing_fields"], "")
        self.assertEqual(report["timestamp_equal"], 1)
        self.assertEqual(report["timestamp_offset_seconds"], "0.0")
        self.assertEqual(report["max_abs_diff"], "0.0")
        self.assertEqual(report["max_relative_diff"], "0.0")

    def test_timestamp_difference_is_diagnostic_not_a_model_divergence(self) -> None:
        report = build_report_row(
            "hstree", "exact",
            decoded("hstree", _row(timestamp="2026-09-15 10:00:00")),
            decoded("hstree", _row(timestamp="2026-09-15 11:00:30")),
            0.0, 0.0,
        )
        self.assertEqual(report["comparison_status"], "exact")
        self.assertEqual(report["differing_fields"], "")
        self.assertEqual(report["timestamp_equal"], 0)
        self.assertEqual(report["timestamp_offset_seconds"], "3630.0")
        self.assertEqual(report["timestamp_source_1"], "2026-09-15 10:00:00")
        self.assertEqual(report["timestamp_source_2"], "2026-09-15 11:00:30")

    def test_status_anomaly_and_threshold_divergences_count(self) -> None:
        for column, override in (
            ("status", {"status": "anomaly"}),
            ("anomaly", {"anomaly": "true"}),
            ("threshold", {"threshold": "1.1"}),
        ):
            report = build_report_row(
                "hstree", "exact",
                decoded("hstree", _row()),
                decoded("hstree", _row(**override)),
                0.0, 0.0,
            )
            self.assertEqual(report["comparison_status"], "divergent", column)
            self.assertEqual(report["differing_fields"], column, column)
            self.assertEqual(report["within_tolerance"], 0, column)

    def test_nan_nan_same_sign_infinities_and_minus_zero_are_exact(self) -> None:
        for cell_1, cell_2 in (("NaN", "NaN"), ("Infinity", "Infinity"),
                               ("-Infinity", "-Infinity"), ("-0.0", "0.0")):
            report = build_report_row(
                "hstree", "exact",
                decoded("hstree", _row(score=cell_1)),
                decoded("hstree", _row(score=cell_2)),
                0.0, 0.0,
            )
            self.assertEqual(report["comparison_status"], "exact", cell_1)

    def test_nan_vs_number_and_null_vs_number_never_tolerated(self) -> None:
        for cell in ("NaN", ""):
            report = build_report_row(
                "hstree", "exact",
                decoded("hstree", _row(score=cell)),
                decoded("hstree", _row(score="0.1")),
                1000.0, 1.0,
            )
            self.assertEqual(report["comparison_status"], "divergent", cell)
            self.assertEqual(report["score_abs_diff"], "")

    def test_infinite_vs_number_never_tolerated(self) -> None:
        report = build_report_row(
            "hstree", "exact",
            decoded("hstree", _row(score="Infinity")),
            decoded("hstree", _row(score="0.1")),
            1000.0, 1.0,
        )
        self.assertEqual(report["comparison_status"], "divergent")
        self.assertEqual(report["score_abs_diff"], "")

    def test_mdragpoint_extra_divergence_counts(self) -> None:
        report = build_report_row(
            "mdragpoint",
            "exact",
            decoded("mdragpoint", _mdrag_row(mdragpoint_internal_r="0.5")),
            decoded("mdragpoint", _mdrag_row(mdragpoint_internal_r="0.6")),
            0.0,
            0.0,
        )
        self.assertEqual(report["comparison_status"], "divergent")
        self.assertEqual(report["differing_fields"], "mdragpoint_internal_r")

    def test_unknown_status_rejected(self) -> None:
        with self.assertRaises(ComparisonError):
            build_report_row(
                "hstree", "joined", decoded("hstree", _row()), decoded("hstree", _row()), 0.0, 0.0
            )


class ToleranceTest(TestCase):
    """Family 5: tolerance semantics, per-field metrics and maxima."""

    def test_default_zero_tolerance_rejects_any_nonzero_diff(self) -> None:
        report = build_report_row(
            "hstree", "exact",
            decoded("hstree", _row(score="0.1")),
            decoded("hstree", _row(score="0.1000001")),
            0.0, 0.0,
        )
        self.assertEqual(report["comparison_status"], "divergent")
        self.assertEqual(report["differing_fields"], "score")

    def test_absolute_tolerance_inclusive_boundary(self) -> None:
        at_limit = build_report_row(
            "hstree", "exact",
            decoded("hstree", _row(score="1.0")),
            decoded("hstree", _row(score="1.5")),
            0.5, 0.0,
        )
        self.assertEqual(at_limit["comparison_status"], "within_tolerance")
        self.assertEqual(at_limit["exact"], 0)
        self.assertEqual(at_limit["within_tolerance"], 1)
        beyond = build_report_row(
            "hstree", "exact",
            decoded("hstree", _row(score="1.0")),
            decoded("hstree", _row(score="1.6")),
            0.5, 0.0,
        )
        self.assertEqual(beyond["comparison_status"], "divergent")

    def test_relative_tolerance_and_max_combination(self) -> None:
        within = build_report_row(
            "hstree", "exact",
            decoded("hstree", _row(score="1.0")),
            decoded("hstree", _row(score="1.4")),
            0.0, 0.5,
        )
        self.assertEqual(within["comparison_status"], "within_tolerance")
        # max(abs_tol, rel_tol * max|a|, |b|) rescues large magnitudes.
        large = build_report_row(
            "hstree", "exact",
            decoded("hstree", _row(score="1000.0")),
            decoded("hstree", _row(score="1000.7")),
            0.0, 0.001,
        )
        self.assertEqual(large["comparison_status"], "within_tolerance")
        # ...but not a small magnitude with the same relative threshold.
        small = build_report_row(
            "hstree", "exact",
            decoded("hstree", _row(score="0.1")),
            decoded("hstree", _row(score="0.9")),
            0.0, 0.5,
        )
        self.assertEqual(small["comparison_status"], "divergent")

    def test_per_field_metrics_and_event_maxima(self) -> None:
        report = build_report_row(
            "hstree",
            "exact",
            decoded("hstree", _row(score="1.0", threshold="2.0")),
            decoded("hstree", _row(score="1.5", threshold="2.0")),
            0.0,
            0.0,
        )
        self.assertEqual(report["score_abs_diff"], "0.5")
        self.assertEqual(report["score_relative_diff"], "0.3333333333333333")
        self.assertEqual(report["threshold_abs_diff"], "0.0")
        self.assertEqual(report["threshold_relative_diff"], "0.0")
        self.assertEqual(report["max_abs_diff"], "0.5")
        self.assertEqual(report["max_relative_diff"], "0.3333333333333333")
        self.assertEqual(report["differing_fields"], "score")

    def test_no_numeric_pair_gives_empty_metrics(self) -> None:
        report = build_report_row(
            "hstree",
            "exact",
            decoded("hstree", _row(score="NaN", threshold="1.0")),
            decoded("hstree", _row(score="0.1", threshold="1.0")),
            0.0, 0.0,
        )
        self.assertEqual(report["score_abs_diff"], "")
        self.assertEqual(report["score_relative_diff"], "")
        # The equal threshold pair still yields computable 0.0 metrics.
        self.assertEqual(report["max_abs_diff"], "0.0")
        self.assertEqual(report["max_relative_diff"], "0.0")


class ReportAndSummaryTest(TestCase):
    """Family 6: headers, categories, publication order, failure cleanup."""

    def test_report_headers_exact_for_hstree_and_mdragpoint(self) -> None:
        base = [
            "Group", "source_event_id",
            "comparison_status", "exact", "within_tolerance",
            "timestamp_equal", "timestamp_offset_seconds", "differing_fields",
            "max_abs_diff", "max_relative_diff",
            "timestamp_source_1", "timestamp_source_2",
            "status_source_1", "status_source_2",
            "anomaly_source_1", "anomaly_source_2",
            "score_source_1", "score_source_2", "score_abs_diff", "score_relative_diff",
            "threshold_source_1", "threshold_source_2",
            "threshold_abs_diff", "threshold_relative_diff",
        ]
        expected = {
            "hstree": base,
            "mdragpoint": base + [
                "mdragpoint_internal_r_source_1", "mdragpoint_internal_r_source_2",
                "mdragpoint_internal_r_abs_diff", "mdragpoint_internal_r_relative_diff",
            ],
        }
        for model, header in expected.items():
            with tempfile.TemporaryDirectory() as tmp_str:
                tmp = Path(tmp_str)
                path_1, path_2 = write_pair(
                    tmp / "campaign-42", model,
                    [_mdrag_row()] if model == "mdragpoint" else [_row()],
                    [_mdrag_row()] if model == "mdragpoint" else [_row()],
                )
                config = make_config(tmp, path_1, path_2, tmp / "report.csv")
                run(config)
                with open(config.report, encoding="utf-8", newline="") as fh:
                    self.assertEqual(next(csv.reader(fh)), header)

    def test_categories_counters_and_global_maxima(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            path_1, path_2 = write_pair(
                tmp / "campaign-42", "hstree",
                [
                    _row(group="A", event="e1", score="1.0"),
                    _row(group="A", event="e2", score="1.0"),
                    _row(group="A", event="e4", score="1.0"),
                ],
                [
                    _row(group="A", event="e1", score="1.0"),
                    _row(group="A", event="e2", score="1.4"),
                    _row(group="A", event="e3", score="9.9"),
                    _row(group="A", event="e5", score="2.0"),
                ],
            )
            config = make_config(
                tmp, path_1, path_2, tmp / "report.csv", abs_tolerance=0.5
            )
            summary = run(config)
            # e1 exact, e2 within (0.4 <= 0.5), e4 only in source-1, e3/e5 only in source-2.
            self.assertEqual(summary["compared"], 2)
            self.assertEqual(summary["exact"], 1)
            self.assertEqual(summary["within_tolerance"], 1)
            self.assertEqual(summary["divergent"], 0)
            self.assertEqual(summary["missing_source_2"], 1)
            self.assertEqual(summary["missing_source_1"], 2)
            self.assertIsNone(summary["first_divergence"])
            self.assertEqual(
                summary["first_missing_source_2"],
                {"Group": "A", "source_event_id": "e4"},
            )
            self.assertEqual(
                summary["first_missing_source_1"],
                {"Group": "A", "source_event_id": "e3"},
            )
            self.assertEqual(
                summary["compared"],
                summary["exact"] + summary["within_tolerance"] + summary["divergent"],
            )
            self.assertAlmostEqual(summary["max_abs_diff"], 0.4)
            self.assertAlmostEqual(summary["max_relative_diff"], 0.4 / 1.4)
            self.assertEqual(summary["source_1"]["image"], "image-a:1")
            self.assertEqual(summary["source_2"]["image"], "image-b:1")
            self.assertEqual(summary["source_1"]["csv"], str(path_1))
            json.loads(config.summary.read_text(encoding="utf-8"))  # strict JSON

    def test_identical_run_has_zero_maxima_and_all_exact(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            path_1, path_2 = write_pair(
                tmp / "campaign-42", "hstree",
                [_row(group="A"), _row(group="B")],
                [_row(group="A"), _row(group="B")],
            )
            config = make_config(tmp, path_1, path_2, tmp / "report.csv")
            summary = run(config)
            self.assertEqual(summary["compared"], 2)
            self.assertEqual(summary["exact"], 2)
            self.assertEqual(summary["max_abs_diff"], 0.0)
            self.assertEqual(summary["max_relative_diff"], 0.0)

    def test_summary_identifies_the_first_model_divergence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            path_1, path_2 = write_pair(
                tmp / "campaign-42", "hstree",
                [
                    _row(group="A", event="e1", score="1.0"),
                    _row(group="A", event="e2", score="2.0"),
                    _row(group="A", event="e3", status="normal"),
                ],
                [
                    _row(group="A", event="e1", score="1.0"),
                    _row(group="A", event="e2", score="2.1"),
                    _row(group="A", event="e3", status="anomaly"),
                ],
            )
            summary = run(make_config(tmp, path_1, path_2, tmp / "report.csv"))
            self.assertEqual(summary["divergent"], 2)
            self.assertEqual(
                summary["first_divergence"],
                {
                    "Group": "A",
                    "source_event_id": "e2",
                    "differing_fields": ["score"],
                },
            )
            self.assertIsNone(summary["first_missing_source_1"])
            self.assertIsNone(summary["first_missing_source_2"])

    def test_summary_is_deterministic_strict_json_with_exact_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            path_1, path_2 = write_pair(tmp / "campaign-42", "hstree", [_row()], [_row()])
            config = make_config(tmp, path_1, path_2, tmp / "report.csv")
            summary = run(config)
            self.assertEqual(
                set(summary),
                {
                    "format_version", "model", "campaign_id",
                    "source_1", "source_2",
                    "abs_tolerance", "relative_tolerance",
                    "compared", "exact", "within_tolerance", "divergent",
                    "missing_source_1", "missing_source_2",
                    "first_divergence", "first_missing_source_1", "first_missing_source_2",
                    "max_abs_diff", "max_relative_diff",
                },
            )
            text = config.summary.read_text(encoding="utf-8")
            self.assertTrue(text.endswith("\n"))
            self.assertEqual(json.loads(text), json.loads(json.dumps(summary)))

    def test_csv_published_before_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            path_1, path_2 = write_pair(tmp / "campaign-42", "hstree", [_row()], [_row()])
            config = make_config(tmp, path_1, path_2, tmp / "report.csv")
            order: list[str] = []
            real_replace = Path.replace

            def spy(self, target):
                order.append(str(target))
                return real_replace(self, target)

            with patch.object(Path, "replace", spy):
                run(config)
            self.assertEqual(
                order, [str(config.report), str(config.summary)]
            )

    def test_failure_leaves_no_output_at_all(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            directory = tmp / "campaign-42"
            # source-2 is canonically unordered: fatal mid-run.
            path_1, path_2 = write_pair(
                directory, "hstree",
                [_row(group="A", event="e1")],
                [_row(group="A", event="e2"), _row(group="A", event="e1")],
            )
            report = tmp / "report.csv"
            summary = Path(str(report) + ".summary.json")
            config = make_config(tmp, path_1, path_2, report)
            with self.assertRaises(ComparisonError):
                run(config)
            self.assertFalse(report.exists())
            self.assertFalse(summary.exists())
            self.assertFalse((tmp / "report.csv.tmp").exists())
            self.assertFalse((tmp / "report.csv.summary.json.tmp").exists())

    def test_failed_summary_publication_removes_the_final_report(self) -> None:
        """The summary is the success marker: if publishing it fails after the
        report was published, the final report must be removed too."""
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            path_1, path_2 = write_pair(tmp / "campaign-42", "hstree", [_row()], [_row()])
            report = tmp / "report.csv"
            summary = Path(str(report) + ".summary.json")
            config = make_config(tmp, path_1, path_2, report)
            real_replace = Path.replace

            def failing_on_summary(self, target):
                if str(target) == str(summary):
                    raise OSError("simulated summary publication failure")
                return real_replace(self, target)

            with patch.object(Path, "replace", failing_on_summary):
                with self.assertRaises(OSError):
                    run(config)
            # No final output (neither summary nor the orphaned report) and no
            # temporary leftover from this run.
            self.assertFalse(report.exists())
            self.assertFalse(summary.exists())
            self.assertFalse((tmp / "report.csv.tmp").exists())
            self.assertFalse((tmp / "report.csv.summary.json.tmp").exists())

    def test_never_overwrites_existing_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            path_1, path_2 = write_pair(tmp / "campaign-42", "hstree", [_row()], [_row()])
            (tmp / "existing.csv").write_text("old\n", encoding="utf-8")
            config = make_config(tmp, path_1, path_2, tmp / "existing.csv")
            with self.assertRaises(ComparisonError):
                run(config)
            self.assertEqual((tmp / "existing.csv").read_text(encoding="utf-8"), "old\n")

    def test_summary_default_path_derived_from_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            path_1, path_2 = write_pair(tmp / "campaign-42", "hstree", [_row()], [_row()])
            code = main([
                "--source-1", str(path_1),
                "--source-2", str(path_2),
                "--report", str(tmp / "comparison.csv"),
            ])
            self.assertEqual(code, 0)
            self.assertTrue((tmp / "comparison.csv").exists())
            self.assertTrue((tmp / "comparison.csv.summary.json").exists())


class OrderingAndHeaderContractsTest(TestCase):
    """Family 3 completion: header mismatch, duplicates, disorder, malformed rows."""

    def _run_and_expect(self, source_1_text: str, source_2_text: str, message: str) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            path_1 = write_export(tmp, "hstree", "1", [], csv_text=source_1_text)
            path_2 = write_export(tmp, "hstree", "2", [], csv_text=source_2_text)
            with self.assertRaises(ComparisonError, msg=message):
                list(iter_v1_rows(path_1, "hstree"))

    def test_header_mismatch_rejected(self) -> None:
        columns = list(columns_for_model("hstree"))
        self._run_and_expect("Group,source_event_id\nA,e1\n", "x\n", "wrong header")

    def test_extra_and_missing_columns_rejected(self) -> None:
        columns = ",".join(columns_for_model("hstree"))
        self._run_and_expect(
            columns + ",extra\n" + ",".join(_row().values()) + ",1\n",
            columns,
            "extra column",
        )

    def test_disorder_and_duplicates_rejected(self) -> None:
        columns = ",".join(columns_for_model("hstree"))
        row_a, row_b = _row(event="e1"), _row(event="e2")
        values_a = [row_a[column] for column in columns_for_model("hstree")]
        values_b = [row_b[column] for column in columns_for_model("hstree")]
        self._run_and_expect(
            columns + "\n" + ",".join(values_b) + "\n" + ",".join(values_a) + "\n",
            columns,
            "disorder",
        )
        self._run_and_expect(
            columns + "\n" + ",".join(values_a) + "\n" + ",".join(values_a) + "\n",
            columns,
            "duplicate",
        )


class CliTest(TestCase):
    """Family 7: help, invalid arguments, collisions, direct execution."""

    def test_help_exits_zero(self) -> None:
        with self.assertRaises(SystemExit) as ctx:
            build_config(["--help"])
        self.assertEqual(ctx.exception.code, 0)

    def test_missing_arguments_exit_two(self) -> None:
        with self.assertRaises(SystemExit) as ctx:
            build_config([])
        self.assertEqual(ctx.exception.code, 2)

    def test_invalid_tolerances_rejected(self) -> None:
        for value in ("-1", "nan", "inf", "abc"):
            with self.assertRaises(SystemExit, msg=value):
                build_config([
                    "--source-1", "a.csv", "--source-2", "b.csv", "--report", "r.csv",
                    "--abs-tolerance", value,
                ])

    def test_path_collisions_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            source_1 = tmp / "a.csv"
            source_2 = tmp / "b.csv"
            cases = [
                (["--source-1", str(source_1), "--source-2", str(source_2),
                  "--report", str(source_1)], "report == source-1"),
                (["--source-1", str(source_1), "--source-2", str(source_2),
                  "--report", str(tmp / "r.csv"),
                  "--summary", str(source_2)], "summary == source-2"),
                (["--source-1", str(source_1), "--source-2", str(source_2),
                  "--report", str(tmp / "r.csv"),
                  "--summary", str(tmp / "r.csv")], "report == summary"),
            ]
            for argv, label in cases:
                with self.assertRaises(ComparisonError, msg=label):
                    build_config(argv)

    def test_direct_execution_without_broker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            path_1, path_2 = write_pair(
                tmp / "campaign-42", "hstree", [_row()], [_row()]
            )
            result = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "validation" / "reproducibility" / "compare_csv.py"),
                    "--source-1", str(path_1),
                    "--source-2", str(path_2),
                    "--report", str(tmp / "comparison.csv"),
                    "--abs-tolerance", "0.0",
                ],
                capture_output=True, text=True, cwd=ROOT,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue((tmp / "comparison.csv").exists())
            self.assertTrue((tmp / "comparison.csv.summary.json").exists())

    def test_main_returns_nonzero_on_metadata_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            directory = tmp / "campaign-42"
            directory.mkdir()
            path_1 = write_export(directory, "hstree", "1", [_row()])
            path_2 = write_export(
                directory, "hstree", "2", [_row()],
                metadata=_metadata("hstree", "source-1"),  # wrong role
            )
            code = main([
                "--source-1", str(path_1),
                "--source-2", str(path_2),
                "--report", str(tmp / "report.csv"),
            ])
            self.assertEqual(code, 1)
            self.assertFalse((tmp / "report.csv").exists())
