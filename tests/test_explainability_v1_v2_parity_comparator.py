"""Pure tests for the V1/V2 parity comparator (V1-05a/b/c).

Covered contracts: the exclusive `(Group, source_event_id)` key (no
timestamp fallback), unusable identities, identical and conflicting
duplicates (order-independent, with shap_computation_time_ms excluded from
the canonical projection), deterministic index and report under reversed
read order, explicit input errors, purity of the module (no Docker, Kafka
or network import), and the V1-05c deterministic summary: verdict, named
per-category metrics and the CLI with distinct exit codes.
"""

from __future__ import annotations

import ast
import contextlib
import json
import tempfile
import unittest
from pathlib import Path

from experiments.explainability.v1_v2.parity_comparator import (
    BOTH_COMPUTED_DIVERGENT,
    BOTH_COMPUTED_EQUAL,
    BOTH_NON_COMPUTED_DIVERGENT,
    BOTH_NON_COMPUTED_EQUAL,
    CATEGORY_ORDER,
    COMPARISON_KEY,
    DIVERGENT,
    DUPLICATE_CONFLICTING,
    DUPLICATE_IDENTICAL,
    EXIT_INCOMPLETE_INPUTS,
    EXIT_INPUT_FILE_ERROR,
    EXIT_JSONL_ERROR,
    EXIT_MANIFEST_ERROR,
    EXIT_PARITY_KO,
    EXIT_PARITY_OK,
    EXIT_USAGE_ERROR,
    EXCLUDED_PARITY_FIELDS,
    FEATURE_SET_MISMATCH,
    INVALID_NUMERIC,
    METRIC_PREFIX,
    NUMERIC_OUTCOME_ORDER,
    ONE_SIDED_COMPUTED,
    PARITY_PROJECTION_FIELDS,
    SHAP_TRANSPORT_FIELDS,
    STRICT_EQUAL,
    TOLERATED_EQUAL,
    UNUSABLE_IDENTITY,
    VERDICT_INCOMPLETE_INPUTS,
    VERDICT_PARITY_KO,
    VERDICT_PARITY_OK,
    ParityComparatorError,
    ParityInputFileError,
    ParityJsonlError,
    ParityManifestError,
    ParityUsageError,
    build_parity_metrics,
    build_parity_report,
    build_parity_summary,
    canonical_projection,
    compare_exports,
    compare_identities,
    compute_parity_verdict,
    expected_keys_missing,
    format_cli_summary,
    identity_of,
    index_export,
    load_jsonl,
    load_jsonl_export,
    load_manifest,
    run_cli,
    write_report,
)

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures" / "explainability_v1_v2"
MODULE_PATH = ROOT / "experiments" / "explainability" / "v1_v2" / "parity_comparator.py"


def _record(group: str, sid, ts: str = "2026-09-21 12:00:00", **overrides):
    record = {
        "timestamp": ts,
        "Group": group,
        "source_event_id": sid,
        "hstree_score": 1.25,
        "features": {"A": 1.5, "B": 2.5},
        "shap_values": {"A": 0.1, "B": -0.2},
        "shap_base_value": 0.05,
        "shap_computation_time_ms": 1.0,
        "local_hstree_score": 0.5,
        "hstree_status": "anomaly",
        "hstree_anomaly": True,
        "hstree_threshold": 0.7,
        "explanation_status": "computed",
        "explanation_error_code": None,
        "explanation_profile": "exhaustive",
        "shap_nsamples": 100,
        "shap_background_size": 100,
        "explainability_seed": 42,
        "hstree_model_version": "local-hstree-v1",
        "hstree_model_fingerprint": "0" * 64,
        "feature_fingerprint": "1" * 64,
    }
    return record


class IdentityTest(unittest.TestCase):
    def test_timestamp_alone_is_insufficient(self) -> None:
        record = _record("g1", None, "2026-09-21 12:00:00")
        self.assertIsNone(identity_of(record))

    def test_missing_empty_or_non_string_identity_is_unusable(self) -> None:
        for record in (
            {},  # both missing
            {"Group": "g1"},  # source_event_id missing
            {"Group": "g1", "source_event_id": ""},  # empty id
            {"Group": "", "source_event_id": "row-1"},  # empty group
            {"Group": None, "source_event_id": "row-1"},  # null group
            {"Group": 42, "source_event_id": "row-1"},  # non-string group
            {"Group": "g1", "source_event_id": 42},  # non-string id
            {"Group": "   ", "source_event_id": "row-1"},  # whitespace group
            {"Group": "g1", "source_event_id": "   "},  # whitespace id
            "not-a-dict",  # not even a record
        ):
            self.assertIsNone(identity_of(record), record)

    def test_valid_identity_keeps_the_raw_published_values(self) -> None:
        # strip() only detects unusable values: the key is built from the raw
        # published strings, never from a normalized version.
        self.assertEqual(identity_of({"Group": "g1", "source_event_id": "row-1"}), "g1|row-1")

    def test_identity_differing_only_by_spaces_stays_two_distinct_identities(self) -> None:
        plain = _record("g1", "row-1", "2026-09-21 12:00:00")
        padded = {**_record("g1", "row-1", "2026-09-21 12:00:00"), "source_event_id": " row-1 "}
        first = identity_of(_record("g1", "row-1", "2026-09-21 12:00:00"))
        padded_identity = identity_of({**_record("g1", "row-1", "2026-09-21 12:00:00"), "source_event_id": " row-1 "})
        # "row-1" and " row-1 " are two distinct identities: no abusive merge.
        self.assertEqual(first, "g1|row-1")
        self.assertEqual(padded_identity, "g1| row-1 ")
        self.assertNotEqual(first, identity_of(_record("g1", " row-1 ", "2026-09-21 12:00:00")))

        index = index_export([plain, padded], "v1")
        # Both identities remain comparable (no unusable_identity, no merge).
        self.assertIn("g1|row-1", index.identities)
        self.assertIn("g1| row-1 ", index.identities)
        self.assertEqual(len(index.identities), 2)
        self.assertEqual(index.duplicate_identical, [])
        self.assertEqual(index.duplicate_conflicting, {})
        self.assertEqual(index.unusable_identity_count, 0)

    def test_same_timestamp_with_distinct_ids_are_distinct_keys(self) -> None:
        first = identity_of(_record("g1", "row-1", "2026-09-21 12:00:00"))
        second = identity_of(_record("g1", "row-2", "2026-09-21 12:00:00"))
        self.assertNotEqual(first, second)


class CanonicalProjectionTest(unittest.TestCase):
    def test_projection_excludes_operational_fields(self) -> None:
        first = _record("g1", "row-1")
        second = {**_record("g1", "row-1"), "shap_computation_time_ms": 42.0}
        self.assertEqual(canonical_projection(first), canonical_projection(second))
        self.assertNotIn("shap_computation_time_ms", canonical_projection(first))
        self.assertEqual(EXCLUDED_PARITY_FIELDS, ("shap_computation_time_ms",))

    def test_projection_is_deterministic(self) -> None:
        record = _record("g1", "row-1")
        self.assertEqual(
            canonical_projection(record), canonical_projection(dict(reversed(list(record.items()))))
        )


class DuplicateTest(unittest.TestCase):
    def test_identical_duplicates_collapse_onto_one_projection(self) -> None:
        records = [_record("g1", "row-1"), _record("g1", "row-1")]
        index = index_export(records, "v1")
        self.assertIn("g1|row-1", index.identities)
        self.assertEqual(index.duplicate_identical, ["g1|row-1"])
        self.assertEqual(index.duplicate_conflicting, {})
        self.assertEqual(index.record_count, 2)

    def test_conflicting_duplicates_leave_the_comparable_space(self) -> None:
        first = _record("g1", "row-1")
        second = {**_record("g1", "row-1"), "shap_base_value": 0.99}
        index = index_export([first, second], "v1")
        self.assertNotIn("g1|row-1", index.identities)
        self.assertEqual(
            index.duplicate_conflicting,
            {"g1|row-1": sorted([canonical_projection(first), canonical_projection(second)])},
        )

    def test_duplicates_differing_only_by_computation_time_are_identical(self) -> None:
        first = {**_record("g1", "row-1"), "shap_computation_time_ms": 1.0}
        second = {**_record("g1", "row-1"), "shap_computation_time_ms": 9.0}
        index = index_export([first, second], "v1")
        self.assertEqual(index.duplicate_identical, ["g1|row-1"])
        self.assertEqual(index.duplicate_conflicting, {})
        self.assertIn("g1|row-1", index.identities)

    def test_unusable_identity_excluded_from_the_comparison_space(self) -> None:
        records = [_record("g1", "row-1"), {"timestamp": "t", "Group": "g1"}]
        index = index_export(records, "v1")
        self.assertNotIn("g1|None", index.identities)
        self.assertEqual(index.unusable_identity_count, 1)
        self.assertEqual(
            index.unusable_identity_shapes,
            ['{"Group":"g1","source_event_id":null}'],
        )

    def test_non_string_identity_is_unusable(self) -> None:
        record = _record("g1", "row-1")
        record["source_event_id"] = 42
        index = index_export([record], "v1")
        self.assertEqual(index.unusable_identity_count, 1)
        self.assertEqual(index.identities, {})


class OrderIndependenceTest(unittest.TestCase):
    V1_RECORDS = [
        _record("g1", "row-2", "2026-09-21 12:00:01"),
        _record("g1", "row-1", "2026-09-21 12:00:00"),
        {**_record("g1", "row-3", "2026-09-21 12:00:02"), "shap_base_value": 0.05},
        {**_record("g1", "row-3", "2026-09-21 12:00:02"), "shap_base_value": 0.05},
    ]

    def test_index_is_identical_with_reversed_lines(self) -> None:
        forward = index_export(self.V1_RECORDS, "v1")
        reversed_index = index_export(list(reversed(self.V1_RECORDS)), "v1")
        self.assertEqual(forward.identities, reversed_index.identities)
        self.assertEqual(forward.duplicate_identical, reversed_index.duplicate_identical)
        self.assertEqual(forward.duplicate_conflicting, reversed_index.duplicate_conflicting)
        self.assertEqual(forward.unusable_identity_shapes, reversed_index.unusable_identity_shapes)

    def test_report_is_byte_identical_with_reversed_lines(self) -> None:
        import tempfile

        manifest = load_manifest(
            {
                "campaign": "order",
                "comparison_key": COMPARISON_KEY,
                "exports": {"v1": "v1.jsonl", "v2": "v2.jsonl"},
                "expected_keys": ["g1|row-1", "g1|row-2", "g1|row-3"],
                "numeric_tolerances": {"absolute_tolerance": 1e-12, "relative_tolerance": 1e-12},
            }
        )
        forward = build_parity_report(
            manifest,
            index_export(self.V1_RECORDS, "v1"),
            index_export([], "v2"),
        )
        backward = build_parity_report(
            manifest,
            index_export(list(reversed(self.V1_RECORDS)), "v1"),
            index_export([], "v2"),
        )
        self.assertEqual(json.dumps(forward, sort_keys=True), json.dumps(backward, sort_keys=True))

    def test_written_report_is_deterministic(self) -> None:
        import tempfile

        manifest = load_manifest(
            {
                "campaign": "order",
                "comparison_key": COMPARISON_KEY,
                "exports": {"v1": "v1.jsonl", "v2": "v2.jsonl"},
                "expected_keys": ["g1|row-1"],
                "numeric_tolerances": {"absolute_tolerance": 1e-12, "relative_tolerance": 1e-12},
            }
        )
        with tempfile.TemporaryDirectory() as tmp:
            first_path = Path(tmp) / "a.json"
            second_path = Path(tmp) / "b.json"
            write_report(
                build_parity_report(
                    manifest,
                    index_export(self.V1_RECORDS, "v1"),
                    index_export([], "v2"),
                ),
                first_path,
            )
            write_report(
                build_parity_report(
                    manifest,
                    index_export(list(reversed(self.V1_RECORDS)), "v1"),
                    index_export([], "v2"),
                ),
                second_path,
            )
            self.assertEqual(first_path.read_bytes(), second_path.read_bytes())


class InputErrorTest(unittest.TestCase):
    def test_valid_fixture_loads_without_error(self) -> None:
        self.assertEqual(len(load_jsonl(FIXTURES / "v1_export.jsonl")), 10)

    def test_malformed_line_is_located(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "broken.jsonl"
            path.write_text('{"Group": "g1"}\nnot-json\n', encoding="utf-8")
            with self.assertRaises(ParityComparatorError) as ctx:
                load_jsonl(path)
            self.assertIn("line 2", str(ctx.exception))

    def test_non_object_line_is_an_explicit_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "array.jsonl"
            path.write_text("[1, 2]\n", encoding="utf-8")
            with self.assertRaises(ParityComparatorError):
                load_jsonl_export(path, "v1")

    def test_manifest_requires_the_exclusive_key(self) -> None:
        manifest = {"campaign": "c", "comparison_key": "(Group, timestamp)", "exports": {"v1": "v1.jsonl", "v2": "v2.jsonl"}, "expected_keys": ["g|s"], "numeric_tolerances": {"absolute_tolerance": 0, "relative_tolerance": 0}}
        with self.assertRaises(ParityComparatorError):
            load_manifest(manifest)

    def test_manifest_rejects_bad_expected_keys(self) -> None:
        base = {"campaign": "c", "comparison_key": COMPARISON_KEY, "exports": {"v1": "v1.jsonl", "v2": "v2.jsonl"}, "numeric_tolerances": {"absolute_tolerance": 0, "relative_tolerance": 0}}
        for bad in (None, [], ["only-group"], ["g|s|extra"], ["g||s"], ["|row"], ["g|"], ["g|s", "g|s"]):
            with self.assertRaises(ParityComparatorError):
                load_manifest({**base, "expected_keys": bad})

    def test_manifest_rejects_non_positive_or_missing_tolerances(self) -> None:
        for tolerances in (
            None,
            {},
            {"absolute_tolerance": -1, "relative_tolerance": 1e-12},
            {"absolute_tolerance": 1e-12, "relative_tolerance": "tight"},
        ):
            manifest = {
                "campaign": "c",
                "comparison_key": COMPARISON_KEY,
                "exports": {"v1": "v1.jsonl", "v2": "v2.jsonl"},
                "expected_keys": ["g|s"],
                "numeric_tolerances": tolerances,
            }
            with self.assertRaises(ParityComparatorError):
                load_manifest(manifest)


class FixturesPipelineTest(unittest.TestCase):
    """The shipped fixtures flow through the whole V1-05a pipeline."""

    def setUp(self) -> None:
        manifest_path = FIXTURES / "parity_manifest.json"
        self.manifest = load_manifest(json.loads(manifest_path.read_text(encoding="utf-8")))
        self.v1_index = index_export(
            load_jsonl_export(FIXTURES / "v1_export.jsonl", "v1"), "v1"
        )
        self.v2_index = index_export(
            load_jsonl_export(FIXTURES / "v2_export.jsonl", "v2"), "v2"
        )
        self.report = build_parity_report(self.manifest, self.v1_index, self.v2_index)

    def test_v1_index_categories(self) -> None:
        v1 = self.report["v1"]
        # row-4a/row-4b: same timestamp, distinct source_event_id -> two keys.
        self.assertIn("PARITY-Campaign|row-4a", self.v1_index.identities)
        self.assertIn("PARITY-Campaign|row-4b", self.v1_index.identities)
        # dup-1 twice, differing only by computation time.
        self.assertEqual(self.v1_index.duplicate_identical, ["PARITY-Campaign|dup-1"])
        # dup-2 twice with conflicting shap_base_value.
        self.assertIn("PARITY-Campaign|dup-2", self.v1_index.duplicate_conflicting)
        # One unusable identity (missing source_event_id).
        self.assertEqual(self.v1_index.unusable_identity_count, 1)

    def test_v2_side_duplicates(self) -> None:
        self.assertEqual(self.v2_index.duplicate_identical, ["PARITY-Campaign|v2-dup"])
        self.assertIn("PARITY-Campaign|v2-conflict", self.v2_index.duplicate_conflicting)
        self.assertEqual(self.v2_index.unusable_identity_count, 0)

    def test_envelope_separates_identity_and_duplicates_for_both_exports(self) -> None:
        for side in ("v1", "v2"):
            section = self.report[side]
            self.assertIn("comparable_identities", section)
            self.assertIn(UNUSABLE_IDENTITY, section)
            self.assertIn(DUPLICATE_IDENTICAL, section)
            self.assertIn(DUPLICATE_CONFLICTING, section)
        self.assertEqual(self.report["comparison_key"], COMPARISON_KEY)
        self.assertEqual(self.report["report_version"], 1)

    def test_report_is_diffable_and_sorted(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "report.json"
            write_report(self.report, out)
            first = out.read_bytes()
            write_report(self.report, out)
            self.assertEqual(first, out.read_bytes())
            # Round-tripping through json.loads keeps the same content.
            self.assertEqual(json.loads(first), self.report)




def _pair(**v2_overrides):
    """A computed V1/V2 pair; V2 diverges only through `v2_overrides`."""
    v1 = _record("g1", "row-1")
    v2 = {**_record("g1", "row-1"), **v2_overrides}
    return v1, v2


_TOLERANCES = {"absolute_tolerance": 1e-12, "relative_tolerance": 1e-12}


def _manifest(tolerances=None):
    return load_manifest(
        {
            "campaign": "v1-05b",
            "comparison_key": COMPARISON_KEY,
            "exports": {
                "v1": str(FIXTURES / "v1_export.jsonl"),
                "v2": str(FIXTURES / "v2_export.jsonl"),
            },
            "expected_keys": ["g1|row-1"],
            "numeric_tolerances": dict(tolerances or _TOLERANCES),
        }
    )


def _compare_pair(v1, v2, tolerances=None):
    manifest = _manifest(tolerances)
    v1_index = index_export([v1], "v1")
    v2_index = index_export([v2], "v2")
    return compare_exports(manifest, v1_index, v2_index)


class CompareSemanticsTest(unittest.TestCase):
    """V1-05b: semantic comparison, tolerances and explicit divergences."""

    def test_strict_equality_is_both_computed_equal(self) -> None:
        v1, v2 = _pair()
        section = _compare_pair(v1, v2)
        comparison = section["identities"]["g1|row-1"]
        self.assertEqual(comparison["category"], BOTH_COMPUTED_EQUAL)
        self.assertEqual(comparison["divergences"], [])
        self.assertGreater(comparison["equalities"][STRICT_EQUAL], 0)
        self.assertEqual(comparison["equalities"][TOLERATED_EQUAL], 0)
        self.assertEqual(section["categories"], {BOTH_COMPUTED_EQUAL: 1})

    def test_iforest_tree_scale_and_value_are_part_of_parity_projection(self) -> None:
        v1, v2 = _pair()
        v1 = {
            **v1,
            "explanation_method": "tree",
            "explanation_output": "iforest_mean_path_length",
            "explained_value": 3.25,
            "local_score": 0.42,
        }
        v2 = dict(v1)
        equal = _compare_pair(v1, v2)["identities"]["g1|row-1"]
        self.assertEqual(equal["category"], BOTH_COMPUTED_EQUAL)

        wrong_scale = {**v2, "explanation_output": "iforest_score"}
        comparison = _compare_pair(v1, wrong_scale)["identities"]["g1|row-1"]
        self.assertEqual(comparison["category"], BOTH_COMPUTED_DIVERGENT)
        self.assertIn("explanation_output", {item["path"] for item in comparison["divergences"]})

        wrong_value = {**v2, "explained_value": 3.5}
        comparison = _compare_pair(v1, wrong_value)["identities"]["g1|row-1"]
        self.assertEqual(comparison["category"], BOTH_COMPUTED_DIVERGENT)
        self.assertIn("explained_value", {item["path"] for item in comparison["divergences"]})

    def test_tolerated_but_not_strict_equality_is_counted(self) -> None:
        v1, v2 = _pair(shap_values={"A": 0.1, "B": -0.2})
        v2 = {**v2, "shap_values": {"A": 0.1 + 5e-13, "B": -0.2}}
        section = _compare_pair(v1, v2)
        comparison = section["identities"]["g1|row-1"]
        self.assertEqual(comparison["category"], BOTH_COMPUTED_EQUAL)
        self.assertEqual(comparison["equalities"][TOLERATED_EQUAL], 1)
        self.assertEqual(section["numeric_outcomes"][TOLERATED_EQUAL], 1)

    def test_atol_and_rtol_exceeded_is_a_divergence_with_gaps(self) -> None:
        v1, v2 = _pair(shap_values={"A": 0.1, "B": -0.2})
        v2 = {**v2, "shap_values": {"A": 0.5, "B": -0.2}}
        section = _compare_pair(v1, v2)
        comparison = section["identities"]["g1|row-1"]
        self.assertEqual(comparison["category"], BOTH_COMPUTED_DIVERGENT)
        divergence = next(d for d in comparison["divergences"] if d["path"] == "shap_values.A")
        self.assertEqual(divergence["outcome"], DIVERGENT)
        self.assertAlmostEqual(divergence["absolute_gap"], 0.4)
        self.assertAlmostEqual(divergence["relative_gap"], 0.8)
        self.assertEqual(divergence["v1"], 0.1)
        self.assertEqual(divergence["v2"], 0.5)

    def test_null_and_zero_numeric_cases(self) -> None:
        # zero against zero: bit equality, strict.
        v1 = {**_record("g1", "row-1"), "shap_base_value": 0.0}
        v2 = {**_record("g1", "row-1"), "shap_base_value": 0.0}
        comparison = _compare_pair(v1, v2)["identities"]["g1|row-1"]
        self.assertEqual(comparison["category"], BOTH_COMPUTED_EQUAL)
        self.assertEqual(comparison["equalities"][STRICT_EQUAL] >= 1, True)

        # None against zero: divergence, never an equality.
        v1, v2 = _pair(shap_base_value=None)
        v2 = {**v2, "shap_base_value": 0.0}
        comparison = _compare_pair(v1, v2)["identities"]["g1|row-1"]
        self.assertEqual(comparison["category"], BOTH_COMPUTED_DIVERGENT)
        divergence = next(d for d in comparison["divergences"] if d["path"] == "shap_base_value")
        self.assertEqual(divergence["outcome"], DIVERGENT)

        # None against None: equality (strict).
        v1 = {**_record("g1", "row-1"), "shap_base_value": None}
        v2 = {**_record("g1", "row-1"), "shap_base_value": None}
        comparison = _compare_pair(v1, v2)["identities"]["g1|row-1"]
        self.assertEqual(comparison["category"], BOTH_COMPUTED_EQUAL)

    def test_nan_infinity_and_non_numeric_are_invalid_numeric(self) -> None:
        for bad in (float("nan"), float("inf"), "not-a-number"):
            v1, v2 = _pair(shap_values={"A": 0.1, "B": -0.2})
            v2 = {**v2, "shap_values": {"A": bad, "B": -0.2}}
            section = _compare_pair(v1, v2)
            comparison = section["identities"]["g1|row-1"]
            self.assertEqual(comparison["category"], BOTH_COMPUTED_DIVERGENT)
            divergence = next(d for d in comparison["divergences"] if d["path"] == "shap_values.A")
            self.assertEqual(divergence["outcome"], INVALID_NUMERIC)
            self.assertEqual(section["numeric_outcomes"][INVALID_NUMERIC], 1)

    def test_non_computed_hstree_score_divergence_without_shap_invention(self) -> None:
        v1 = {**_record("g1", "row-1"), "explanation_status": "warmup", "shap_values": None,
              "shap_base_value": None, "local_hstree_score": None, "hstree_score": 1.25}
        v2 = {**v1, "hstree_score": 9.9}
        section = _compare_pair(v1, v2)
        comparison = section["identities"]["g1|row-1"]
        self.assertEqual(comparison["category"], BOTH_NON_COMPUTED_DIVERGENT)
        divergence = next(d for d in comparison["divergences"] if d["path"] == "hstree_score")
        self.assertEqual(divergence["outcome"], DIVERGENT)
        # No SHAP comparison is invented for a non-computed status: a shap
        # base difference is invisible in this branch.
        v1 = {**v1, "shap_base_value": 0.1}
        v2 = {**v2, "shap_base_value": 0.2, "hstree_score": 1.25}
        comparison = _compare_pair(v1, v2)["identities"]["g1|row-1"]
        self.assertEqual(comparison["category"], BOTH_NON_COMPUTED_EQUAL)
        self.assertFalse(any(d["path"].startswith("shap") for d in comparison["divergences"]))

    def test_non_computed_equal_category(self) -> None:
        v1 = {**_record("g1", "row-1"), "explanation_status": "warmup", "shap_values": None,
              "shap_base_value": None, "local_hstree_score": None}
        v2 = dict(v1)
        section = _compare_pair(v1, v2)
        self.assertEqual(section["categories"], {BOTH_NON_COMPUTED_EQUAL: 1})

    def test_one_sided_computed_is_an_explicit_divergence(self) -> None:
        v1 = _record("g1", "row-1")
        v2 = {**_record("g1", "row-1"), "explanation_status": "warmup", "shap_values": None,
              "shap_base_value": None, "local_hstree_score": None}
        section = _compare_pair(v1, v2)
        comparison = section["identities"]["g1|row-1"]
        self.assertEqual(comparison["category"], ONE_SIDED_COMPUTED)
        self.assertEqual(
            comparison["explanation_status"], {"v1": "computed", "v2": "warmup"}
        )
        # The systematic status fact is reported as divergent.
        status_divergence = next(d for d in comparison["divergences"] if d["path"] == "explanation_status")
        self.assertEqual(status_divergence["outcome"], DIVERGENT)
        # No SHAP comparison is invented for the one-sided branch either.
        self.assertFalse(any(d["path"].startswith("shap_values") for d in comparison["divergences"]))

    def test_different_feature_sets_are_an_explicit_divergence(self) -> None:
        v1, v2 = _pair(features={"A": 1.5, "B": 2.5})
        v2 = {**v2, "features": {"A": 1.5, "C": 3.5}, "shap_values": {"A": 0.1, "C": -0.2}}
        section = _compare_pair(v1, v2)
        comparison = section["identities"]["g1|row-1"]
        self.assertEqual(comparison["category"], FEATURE_SET_MISMATCH)
        divergence = next(d for d in comparison["divergences"] if d["path"] == "features")
        self.assertEqual(divergence["outcome"], FEATURE_SET_MISMATCH)
        self.assertEqual(divergence["v1_features"], ["A", "B"])
        self.assertEqual(divergence["v2_features"], ["A", "C"])
        # No silent or partial value comparison.
        self.assertFalse(any(d["path"].startswith("shap_values.") for d in comparison["divergences"]))

    def test_shap_contribution_and_base_divergence_paths(self) -> None:
        v1, v2 = _pair(shap_values={"A": 0.1, "B": -0.2}, shap_base_value=0.05)
        v2 = {**v2, "shap_values": {"A": 0.1, "B": -0.9}, "shap_base_value": 0.5}
        comparison = _compare_pair(v1, v2)["identities"]["g1|row-1"]
        paths = {d["path"] for d in comparison["divergences"]}
        self.assertIn("shap_values.B", paths)
        self.assertIn("shap_base_value", paths)
        self.assertNotIn("shap_values.A", paths)

    def test_missing_shap_contribution_on_one_side_diverges(self) -> None:
        v1, v2 = _pair(shap_values={"A": 0.1, "B": -0.2})
        v2 = {**v2, "shap_values": {"A": 0.1}}
        comparison = _compare_pair(v1, v2)["identities"]["g1|row-1"]
        divergence = next(d for d in comparison["divergences"] if d["path"] == "shap_values.B")
        self.assertEqual(divergence["outcome"], DIVERGENT)
        self.assertIsNone(divergence["v2"])

    def test_comparison_is_deterministic_under_reversed_order(self) -> None:
        v1a = _record("g1", "row-1")
        v1b = _record("g1", "row-2")
        v2a = {**_record("g1", "row-1"), "hstree_score": 1.25}
        v2b = {**_record("g1", "row-2"), "shap_values": {"A": 0.1, "B": -0.2}}
        manifest = _manifest()

        forward_v1 = index_export([v1a, v1b], "v1")
        forward_v2 = index_export([v2a, v2b], "v2")
        forward = compare_exports(manifest, forward_v1, forward_v2)

        backward_v1 = index_export([v1b, v1a], "v1")
        backward_v2 = index_export([v2b, v2a], "v2")
        backward = compare_exports(manifest, backward_v1, backward_v2)

        self.assertEqual(json.dumps(forward, sort_keys=True), json.dumps(backward, sort_keys=True))

    def test_one_sided_coverage_stays_outside_the_comparison(self) -> None:
        v1 = _record("g1", "row-1")
        v2 = _record("g1", "row-2")
        section = _compare_pair(v1, v2)
        self.assertEqual(section["compared_identities"], 0)
        self.assertEqual(section["only_v1"], ["g1|row-1"])
        self.assertEqual(section["only_v2"], ["g1|row-2"])

    def test_envelope_carries_the_comparison_section(self) -> None:
        v1, v2 = _pair()
        manifest = _manifest()
        report = build_parity_report(manifest, index_export([v1], "v1"), index_export([v2], "v2"))
        self.assertIn("comparison", report)
        self.assertEqual(report["comparison"]["categories"], {BOTH_COMPUTED_EQUAL: 1})


class ShapTransportMetadataTest(unittest.TestCase):
    """V1-06 campaign fix: transport metadata inside shap_values.

    The real JSONL exports carry the text fields Group and source_event_id
    inside the shap_values map, next to the numeric contributions. They are
    excluded from the SHAP comparison: identity is owned by the canonical
    (Group, source_event_id) record key, never by the timestamp.
    """

    def _pair_with_transport(self, **v2_overrides):
        v1 = _record("g1", "row-1")
        v1["shap_values"] = {**v1["shap_values"], "Group": "g1", "source_event_id": "row-1"}
        v2 = {**_record("g1", "row-1"), **v2_overrides}
        v2["shap_values"] = {**v2["shap_values"], "Group": "g1", "source_event_id": "row-1"}
        return v1, v2

    def test_transport_field_constant(self) -> None:
        self.assertEqual(SHAP_TRANSPORT_FIELDS, ("Group", "source_event_id", "explained_model"))

    def test_identical_transport_metadata_is_not_a_divergence(self) -> None:
        v1, v2 = self._pair_with_transport()
        v1["shap_values"]["explained_model"] = "kitnet"
        v2["shap_values"]["explained_model"] = "kitnet"
        section = _compare_pair(v1, v2)
        comparison = section["identities"]["g1|row-1"]
        self.assertEqual(comparison["category"], BOTH_COMPUTED_EQUAL)
        paths = {d["path"] for d in comparison["divergences"]}
        self.assertNotIn("shap_values.Group", paths)
        self.assertNotIn("shap_values.source_event_id", paths)
        self.assertNotIn("shap_values.explained_model", paths)
        self.assertEqual(section["numeric_outcomes"][INVALID_NUMERIC], 0)

    def test_transport_metadata_difference_is_not_a_shap_divergence(self) -> None:
        # The metadata inside the map differs, the canonical identity stays
        # g1|row-1: no SHAP divergence may be invented from it.
        v1, v2 = self._pair_with_transport()
        v2["shap_values"] = {**v2["shap_values"], "Group": "Other-Group", "source_event_id": "other-id"}
        v1["shap_values"]["explained_model"] = "kitnet"
        v2["shap_values"]["explained_model"] = "iforest"
        section = _compare_pair(v1, v2)
        comparison = section["identities"]["g1|row-1"]
        self.assertEqual(comparison["category"], BOTH_COMPUTED_EQUAL)
        paths = {d["path"] for d in comparison["divergences"]}
        self.assertNotIn("shap_values.Group", paths)
        self.assertNotIn("shap_values.source_event_id", paths)
        self.assertNotIn("shap_values.explained_model", paths)

    def test_real_numeric_divergence_with_transport_metadata_present(self) -> None:
        v1, v2 = self._pair_with_transport()
        v2["shap_values"] = {**v2["shap_values"], "A": 0.5}
        section = _compare_pair(v1, v2)
        comparison = section["identities"]["g1|row-1"]
        self.assertEqual(comparison["category"], BOTH_COMPUTED_DIVERGENT)
        divergence = next(d for d in comparison["divergences"] if d["path"] == "shap_values.A")
        self.assertEqual(divergence["outcome"], DIVERGENT)
        self.assertAlmostEqual(divergence["absolute_gap"], 0.4)
        self.assertAlmostEqual(divergence["relative_gap"], 0.8)
        # Only the real contribution diverges: the transport metadata is not
        # reported and the numeric outcome counting stays exact.
        paths = {d["path"] for d in comparison["divergences"]}
        self.assertEqual(
            paths,
            {"shap_values.A"},
        )

    def test_invalid_numeric_contribution_with_transport_metadata_present(self) -> None:
        v1, v2 = self._pair_with_transport()
        v2["shap_values"] = {**v2["shap_values"], "A": float("nan")}
        section = _compare_pair(v1, v2)
        comparison = section["identities"]["g1|row-1"]
        divergence = next(d for d in comparison["divergences"] if d["path"] == "shap_values.A")
        self.assertEqual(divergence["outcome"], INVALID_NUMERIC)
        paths = {d["path"] for d in comparison["divergences"]}
        self.assertEqual(paths, {"shap_values.A"})

    def test_transport_metadata_stays_outside_the_feature_comparison(self) -> None:
        # The exclusion is scoped to the shap_values map: the numeric feature
        # comparison keeps its full behavior (same feature names, different
        # value -> divergence on features.A only).
        v1, v2 = self._pair_with_transport()
        v2 = {**v2, "features": {"A": 9.9, "B": 2.5}}
        section = _compare_pair(v1, v2)
        comparison = section["identities"]["g1|row-1"]
        self.assertEqual(comparison["category"], BOTH_COMPUTED_DIVERGENT)
        divergence = next(d for d in comparison["divergences"] if d["path"] == "features.A")
        self.assertEqual(divergence["outcome"], DIVERGENT)
        paths = {d["path"] for d in comparison["divergences"]}
        self.assertNotIn("shap_values.Group", paths)
        self.assertNotIn("shap_values.source_event_id", paths)

    def test_comparison_stays_deterministic_with_transport_metadata(self) -> None:
        v1a, v2a = self._pair_with_transport()
        v1b = _record("g1", "row-2")
        v1b["shap_values"] = {**v1b["shap_values"], "Group": "g1", "source_event_id": "row-2"}
        v2b = _record("g1", "row-2")
        v2b["shap_values"] = {**v2b["shap_values"], "Group": "g1", "source_event_id": "row-2"}
        manifest = _manifest()

        forward = build_parity_report(
            manifest,
            index_export([v1a, v1b], "v1"),
            index_export([v2a, v2b], "v2"),
        )
        backward = build_parity_report(
            manifest,
            index_export([v1b, v1a], "v1"),
            index_export([v2b, v2a], "v2"),
        )
        self.assertEqual(json.dumps(forward, sort_keys=True), json.dumps(backward, sort_keys=True))
        with tempfile.TemporaryDirectory() as tmp:
            first_path = Path(tmp) / "a.json"
            second_path = Path(tmp) / "b.json"
            write_report(forward, first_path)
            write_report(backward, second_path)
            self.assertEqual(first_path.read_bytes(), second_path.read_bytes())

    def test_shipped_fixtures_carry_transport_metadata_without_divergence(self) -> None:
        # Representative case added to the fixtures (V1-06 discovery): the
        # computed records of both exports carry Group and source_event_id
        # inside shap_values and must stay both_computed_equal.
        manifest = load_manifest(
            json.loads((FIXTURES / "parity_manifest.json").read_text(encoding="utf-8"))
        )
        v1_index = index_export(load_jsonl_export(FIXTURES / "v1_export.jsonl", "v1"), "v1")
        v2_index = index_export(load_jsonl_export(FIXTURES / "v2_export.jsonl", "v2"), "v2")
        section = compare_exports(manifest, v1_index, v2_index)
        self.assertEqual(section["numeric_outcomes"][INVALID_NUMERIC], 0)
        self.assertEqual(section["categories"].get(BOTH_COMPUTED_DIVERGENT, 0), 0)
        for key, comparison in section["identities"].items():
            if comparison["category"] == BOTH_COMPUTED_EQUAL:
                paths = {d["path"] for d in comparison["divergences"]}
                self.assertNotIn("shap_values.Group", paths, key)
                self.assertNotIn("shap_values.source_event_id", paths, key)
        # The shipped computed records really carry the transport metadata.
        computed = [r for r in load_jsonl(FIXTURES / "v1_export.jsonl") if r.get("explanation_status") == "computed"]
        self.assertTrue(computed)
        for record in computed:
            self.assertIn("Group", record["shap_values"])
            self.assertIn("source_event_id", record["shap_values"])


class PurityTest(unittest.TestCase):
    """The module must not import Docker, Kafka, network or subprocess."""

    FORBIDDEN_MODULES = {
        "confluent_kafka",
        "docker",
        "requests",
        "httpx",
        "urllib",
        "urllib3",
        "socket",
        "subprocess",
        "kafka",
        "aiokafka",
        "http",
        "ftplib",
        "smtplib",
        "telnetlib",
        "multiprocessing",
    }

    def _imports(self, tree) -> set:
        imports = set()
        for node in tree.body:
            if isinstance(node, ast.Import):
                imports.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.add(node.module.split(".")[0])
        return imports

    def test_no_docker_kafka_or_network_import(self) -> None:
        tree = ast.parse(MODULE_PATH.read_text(encoding="utf-8"))
        imported = self._imports(tree)
        self.assertFalse(imported & self.FORBIDDEN_MODULES, imported & self.FORBIDDEN_MODULES)

    def test_module_only_relies_on_the_standard_library(self) -> None:
        tree = ast.parse(MODULE_PATH.read_text(encoding="utf-8"))
        imported = self._imports(tree)
        self.assertLessEqual(
            imported,
            {"json", "math", "sys", "dataclasses", "pathlib", "typing", "__future__"},
        )


class ManifestExportsValidationTest(unittest.TestCase):
    """V1-05c: the manifest references the JSONL exports explicitly."""

    def _base(self, exports=None):
        manifest = {
            "campaign": "c",
            "comparison_key": COMPARISON_KEY,
            "expected_keys": ["g|s"],
            "numeric_tolerances": {"absolute_tolerance": 0, "relative_tolerance": 0},
        }
        if exports is not None:
            manifest["exports"] = exports
        return manifest

    def test_missing_exports_section_is_rejected(self) -> None:
        with self.assertRaises(ParityManifestError):
            load_manifest(self._base())

    def test_exports_must_be_an_object(self) -> None:
        with self.assertRaises(ParityManifestError):
            load_manifest(self._base(["v1.jsonl", "v2.jsonl"]))

    def test_each_export_must_be_a_non_empty_string(self) -> None:
        for exports in (
            {},
            {"v1": "v1.jsonl"},
            {"v2": "v2.jsonl"},
            {"v1": "", "v2": "v2.jsonl"},
            {"v1": "   ", "v2": "v2.jsonl"},
            {"v1": "v1.jsonl", "v2": ""},
            {"v1": "v1.jsonl", "v2": 42},
        ):
            with self.assertRaises(ParityManifestError):
                load_manifest(self._base(exports))

    def test_export_errors_are_manifest_errors(self) -> None:
        # Subclass contract: existing callers catching ParityComparatorError
        # keep working, while the CLI can map the manifest kind to its own
        # exit code.
        with self.assertRaises(ParityComparatorError):
            load_manifest(self._base())

    def test_shipped_fixture_manifest_passes_validation(self) -> None:
        manifest = load_manifest(
            json.loads((FIXTURES / "parity_manifest.json").read_text(encoding="utf-8"))
        )
        self.assertEqual(
            manifest["exports"],
            {
                "v1": "tests/fixtures/explainability_v1_v2/v1_export.jsonl",
                "v2": "tests/fixtures/explainability_v1_v2/v2_export.jsonl",
            },
        )

    def test_schema_documents_exports_tolerances_and_keys(self) -> None:
        schema = json.loads(
            (ROOT / "experiments" / "explainability" / "v1_v2" / "parity_manifest.schema.json").read_text(
                encoding="utf-8"
            )
        )
        for name in ("campaign", "comparison_key", "exports", "expected_keys", "numeric_tolerances"):
            self.assertIn(name, schema["required"])
        exports = schema["properties"]["exports"]
        self.assertEqual(exports["required"], ["v1", "v2"])
        self.assertEqual(exports["properties"]["v1"]["minLength"], 1)
        self.assertEqual(exports["properties"]["v2"]["minLength"], 1)
        tolerances = schema["properties"]["numeric_tolerances"]["properties"]
        self.assertEqual(tolerances["absolute_tolerance"]["minimum"], 0)
        self.assertEqual(tolerances["relative_tolerance"]["minimum"], 0)
        expected_keys = schema["properties"]["expected_keys"]
        self.assertTrue(expected_keys["uniqueItems"])
        self.assertGreaterEqual(expected_keys["minItems"], 1)


class VerdictTest(unittest.TestCase):
    """V1-05c: parity_ok / parity_ko / incomplete_inputs semantics."""

    _TOLERANCES = {"absolute_tolerance": 1e-12, "relative_tolerance": 1e-12}

    def _report(self, v1_records, v2_records, expected_keys):
        manifest = load_manifest(
            {
                "campaign": "verdict",
                "comparison_key": COMPARISON_KEY,
                "exports": {"v1": "v1.jsonl", "v2": "v2.jsonl"},
                "expected_keys": expected_keys,
                "numeric_tolerances": dict(self._TOLERANCES),
            }
        )
        return build_parity_report(
            manifest,
            index_export(v1_records, "v1"),
            index_export(v2_records, "v2"),
        )

    def test_clean_equal_inputs_are_parity_ok(self) -> None:
        records = [_record("g1", "row-1"), _record("g1", "row-2")]
        report = self._report(records, list(records), ["g1|row-1", "g1|row-2"])
        summary = report["summary"]
        self.assertEqual(summary["verdict"], VERDICT_PARITY_OK)
        self.assertEqual(summary["coverage"]["compared_identities"], 2)
        self.assertEqual(
            summary["status"]["categories"][BOTH_COMPUTED_EQUAL], 2
        )

    def test_single_numeric_divergence_is_parity_ko(self) -> None:
        v1 = [_record("g1", "row-1")]
        v2 = [{**_record("g1", "row-1"), "hstree_score": 9.9}]
        report = self._report(v1, v2, ["g1|row-1"])
        self.assertEqual(report["summary"]["verdict"], VERDICT_PARITY_KO)
        self.assertEqual(
            report["summary"]["status"]["categories"][BOTH_COMPUTED_DIVERGENT], 1
        )

    def test_feature_set_mismatch_is_parity_ko(self) -> None:
        v1 = [_record("g1", "row-1")]
        v2 = [
            {
                **_record("g1", "row-1"),
                "features": {"A": 1.5, "C": 3.5},
                "shap_values": {"A": 0.1, "C": -0.2},
            }
        ]
        report = self._report(v1, v2, ["g1|row-1"])
        self.assertEqual(report["summary"]["verdict"], VERDICT_PARITY_KO)
        self.assertEqual(
            report["summary"]["status"]["categories"][FEATURE_SET_MISMATCH], 1
        )

    def test_missing_expected_key_is_incomplete_inputs(self) -> None:
        records = [_record("g1", "row-1")]
        report = self._report(records, list(records), ["g1|row-1", "g1|row-2"])
        self.assertEqual(
            report["summary"]["verdict"], VERDICT_INCOMPLETE_INPUTS
        )
        self.assertEqual(
            report["summary"]["coverage"]["expected_keys_missing_v2"], 1
        )

    def test_unmatched_identity_is_incomplete_inputs(self) -> None:
        # g1|row-2 exists only in V1: a lag, so the run cannot conclude even
        # though every expected key is comparable.
        v1 = [_record("g1", "row-1"), _record("g1", "row-2")]
        v2 = [_record("g1", "row-1")]
        report = self._report(v1, v2, ["g1|row-1"])
        self.assertEqual(
            report["summary"]["verdict"], VERDICT_INCOMPLETE_INPUTS
        )
        self.assertEqual(report["summary"]["coverage"]["only_v1"], 1)

    def test_conflicting_duplicate_is_incomplete_inputs(self) -> None:
        v1 = [
            _record("g1", "row-1"),
            {**_record("g1", "row-1"), "shap_base_value": 0.99},
        ]
        v2 = [_record("g1", "row-1")]
        report = self._report(v1, v2, ["g1|row-1"])
        self.assertEqual(
            report["summary"]["verdict"], VERDICT_INCOMPLETE_INPUTS
        )
        self.assertEqual(report["summary"]["delivery"]["duplicate_conflicting_v1"], 1)

    def test_unusable_identity_is_incomplete_inputs(self) -> None:
        v1 = [_record("g1", "row-1"), {"timestamp": "t", "Group": "g1"}]
        v2 = [_record("g1", "row-1")]
        report = self._report(v1, v2, ["g1|row-1"])
        self.assertEqual(
            report["summary"]["verdict"], VERDICT_INCOMPLETE_INPUTS
        )
        self.assertEqual(report["summary"]["identity"]["unusable_identity_v1"], 1)

    def test_expected_keys_missing_function_is_sorted(self) -> None:
        manifest = load_manifest(
            {
                "campaign": "verdict",
                "comparison_key": COMPARISON_KEY,
                "exports": {"v1": "v1.jsonl", "v2": "v2.jsonl"},
                "expected_keys": ["g1|row-2", "g1|row-1"],
                "numeric_tolerances": dict(self._TOLERANCES),
            }
        )
        index = index_export([_record("g1", "row-1")], "v1")
        self.assertEqual(expected_keys_missing(manifest, index), ["g1|row-2"])


class SummaryMetricsTest(unittest.TestCase):
    """V1-05c: coherent counters and stable named metrics."""

    _TOLERANCES = {"absolute_tolerance": 1e-12, "relative_tolerance": 1e-12}

    def _report(self, v1_records, v2_records, expected_keys):
        manifest = load_manifest(
            {
                "campaign": "metrics",
                "comparison_key": COMPARISON_KEY,
                "exports": {"v1": "v1.jsonl", "v2": "v2.jsonl"},
                "expected_keys": expected_keys,
                "numeric_tolerances": dict(self._TOLERANCES),
            }
        )
        v1_index = index_export(v1_records, "v1")
        v2_index = index_export(v2_records, "v2")
        comparison = compare_exports(manifest, v1_index, v2_index)
        return (
            build_parity_summary(manifest, v1_index, v2_index, comparison),
            v1_index,
            v2_index,
        )

    def test_category_counts_sum_to_compared_identities(self) -> None:
        v1 = [_record("g1", "row-1"), _record("g1", "row-2")]
        v2 = [_record("g1", "row-1"), {**_record("g1", "row-2"), "hstree_score": 9.9}]
        summary, _, _ = self._report(v1, v2, ["g1|row-1", "g1|row-2"])
        categories = summary["status"]["categories"]
        self.assertEqual(
            sum(categories.values()),
            summary["coverage"]["compared_identities"],
        )
        self.assertEqual(
            set(categories), set(CATEGORY_ORDER)
        )
        self.assertEqual(categories[BOTH_COMPUTED_EQUAL], 1)
        self.assertEqual(categories[BOTH_COMPUTED_DIVERGENT], 1)

    def test_numeric_outcomes_cover_the_full_stable_set(self) -> None:
        v1 = [_record("g1", "row-1")]
        v2 = [_record("g1", "row-1")]
        summary, _, _ = self._report(v1, v2, ["g1|row-1"])
        self.assertEqual(
            set(summary["values"]["numeric_outcomes"]),
            set(NUMERIC_OUTCOME_ORDER),
        )

    def test_metrics_have_the_stable_named_set(self) -> None:
        v1 = [_record("g1", "row-1"), _record("g1", "row-2")]
        v2 = [_record("g1", "row-1"), {**_record("g1", "row-2"), "hstree_score": 9.9}]
        summary, v1_index, v2_index = self._report(
            v1, v2, ["g1|row-1", "g1|row-2"]
        )
        expected = {METRIC_PREFIX + "compared_identities"}
        expected |= {METRIC_PREFIX + "only_v1", METRIC_PREFIX + "only_v2"}
        expected |= {
            METRIC_PREFIX + "expected_keys_missing_v1",
            METRIC_PREFIX + "expected_keys_missing_v2",
        }
        expected |= {
            METRIC_PREFIX + "duplicate_identical_v1",
            METRIC_PREFIX + "duplicate_identical_v2",
            METRIC_PREFIX + "duplicate_conflicting_v1",
            METRIC_PREFIX + "duplicate_conflicting_v2",
            METRIC_PREFIX + "unusable_identity_v1",
            METRIC_PREFIX + "unusable_identity_v2",
        }
        expected |= {METRIC_PREFIX + "category_" + c for c in CATEGORY_ORDER}
        expected |= {METRIC_PREFIX + "numeric_" + o for o in NUMERIC_OUTCOME_ORDER}
        metrics = summary["metrics"]
        self.assertEqual(set(metrics), expected)
        # Every metric is an integer, zero included: a future Grafana series
        # can query any category regardless of the observed values.
        for value in metrics.values():
            self.assertIsInstance(value, int)
        self.assertEqual(metrics[METRIC_PREFIX + "compared_identities"], 2)
        self.assertEqual(
            metrics[METRIC_PREFIX + "category_" + BOTH_COMPUTED_DIVERGENT], 1
        )
        self.assertEqual(
            metrics[METRIC_PREFIX + "category_" + BOTH_COMPUTED_EQUAL], 1
        )
        self.assertEqual(metrics[METRIC_PREFIX + "only_v1"], 0)
        self.assertEqual(metrics[METRIC_PREFIX + "only_v2"], 0)

    def test_metrics_mirror_the_structural_counts(self) -> None:
        v1 = [
            _record("g1", "row-1"),
            _record("g1", "dup"),
            {**_record("g1", "dup"), "shap_computation_time_ms": 5.0},
            {"timestamp": "t", "Group": "g1"},
        ]
        v2 = [_record("g1", "row-1")]
        summary, _, _ = self._report(v1, v2, ["g1|row-1"])
        metrics = summary["metrics"]
        self.assertEqual(metrics[METRIC_PREFIX + "duplicate_identical_v1"], 1)
        self.assertEqual(metrics[METRIC_PREFIX + "unusable_identity_v1"], 1)
        self.assertEqual(metrics[METRIC_PREFIX + "only_v1"], 1)
        self.assertEqual(metrics[METRIC_PREFIX + "only_v2"], 0)
        self.assertEqual(
            metrics[METRIC_PREFIX + "expected_keys_missing_v2"], 0
        )
        self.assertEqual(
            summary["delivery"]["duplicate_identical_v1"],
            len(index_export(v1, "v1").duplicate_identical),
        )

    def test_summary_matches_dedicated_metric_builder(self) -> None:
        v1 = [_record("g1", "row-1")]
        v2 = [_record("g1", "row-1")]
        manifest = load_manifest(
            {
                "campaign": "metrics",
                "comparison_key": COMPARISON_KEY,
                "exports": {"v1": "v1.jsonl", "v2": "v2.jsonl"},
                "expected_keys": ["g1|row-1"],
                "numeric_tolerances": dict(self._TOLERANCES),
            }
        )
        v1_index = index_export(v1, "v1")
        v2_index = index_export(v2, "v2")
        comparison = compare_exports(manifest, v1_index, v2_index)
        self.assertEqual(
            build_parity_summary(manifest, v1_index, v2_index, comparison)["metrics"],
            build_parity_metrics(manifest, v1_index, v2_index, comparison),
        )


class CliContractTest(unittest.TestCase):
    """V1-05c: CLI verdicts, distinct exit codes and byte determinism."""

    _TOLERANCES = {"absolute_tolerance": 1e-12, "relative_tolerance": 1e-12}

    def _campaign(self, tmp, v1_records, v2_records, expected_keys):
        base = Path(tmp)
        v1_path = base / "v1.jsonl"
        v2_path = base / "v2.jsonl"
        v1_path.write_text(
            "".join(json.dumps(record) + "\n" for record in v1_records),
            encoding="utf-8",
        )
        v2_path.write_text(
            "".join(json.dumps(record) + "\n" for record in v2_records)
        )
        manifest_path = base / "manifest.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "campaign": "cli",
                    "comparison_key": COMPARISON_KEY,
                    "exports": {"v1": str(v1_path), "v2": str(v2_path)},
                    "expected_keys": expected_keys,
                    "numeric_tolerances": dict(self._TOLERANCES),
                }
            ),
            encoding="utf-8",
        )
        return manifest_path

    def _run(self, *arguments):
        import io

        stdout, stderr = io.StringIO(), io.StringIO()
        code = run_cli(list(arguments), stdout=stdout, stderr=stderr)
        return code, stdout.getvalue(), stderr.getvalue()

    def test_ok_inputs_exit_zero_with_summary_and_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manifest = self._campaign(
                tmp,
                [_record("g1", "row-1")],
                [_record("g1", "row-1")],
                ["g1|row-1"],
            )
            output = Path(tmp) / "report.json"
            code, out, err = self._run(
                "--manifest", str(manifest), "--output", str(output)
            )
            self.assertEqual(code, EXIT_PARITY_OK)
            self.assertEqual(err, "")
            self.assertIn("campaign: cli", out)
            self.assertIn("compared_identities: 1", out)
            self.assertIn(f"category {BOTH_COMPUTED_EQUAL}: 1", out)
            self.assertIn(f"verdict: {VERDICT_PARITY_OK}", out)
            report = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(report["summary"]["verdict"], VERDICT_PARITY_OK)

    def test_divergent_inputs_exit_one(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manifest = self._campaign(
                tmp,
                [_record("g1", "row-1")],
                [{**_record("g1", "row-1"), "hstree_score": 9.9}],
                ["g1|row-1"],
            )
            code, out, _ = self._run(
                "--manifest", str(manifest), "--output", str(Path(tmp) / "r.json")
            )
            self.assertEqual(code, EXIT_PARITY_KO)
            self.assertIn(f"verdict: {VERDICT_PARITY_KO}", out)

    def test_incomplete_inputs_exit_two(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manifest = self._campaign(
                tmp,
                [_record("g1", "row-1"), _record("g1", "row-2")],
                [_record("g1", "row-1")],
                ["g1|row-1"],
            )
            code, out, _ = self._run(
                "--manifest", str(manifest), "--output", str(Path(tmp) / "r.json")
            )
            self.assertEqual(code, EXIT_INCOMPLETE_INPUTS)
            self.assertIn("only_v1: 1", out)
            self.assertIn(f"verdict: {VERDICT_INCOMPLETE_INPUTS}", out)

    def test_report_is_byte_identical_across_two_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manifest = self._campaign(
                tmp,
                [_record("g1", "row-1"), _record("g1", "row-2")],
                [_record("g1", "row-1"), _record("g1", "row-2")],
                ["g1|row-1", "g1|row-2"],
            )
            first = Path(tmp) / "first.json"
            second = Path(tmp) / "second.json"
            self._run("--manifest", str(manifest), "--output", str(first))
            self._run("--manifest", str(manifest), "--output", str(second))
            self.assertEqual(first.read_bytes(), second.read_bytes())

    def test_missing_manifest_exits_four(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            code, _, err = self._run(
                "--manifest", str(Path(tmp) / "absent.json"),
                "--output", str(Path(tmp) / "r.json"),
            )
            self.assertEqual(code, EXIT_INPUT_FILE_ERROR)
            self.assertIn("manifest file not found", err)

    def test_invalid_manifest_json_exits_three(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manifest = Path(tmp) / "manifest.json"
            manifest.write_text("{not json}\n", encoding="utf-8")
            code, _, err = self._run(
                "--manifest", str(manifest), "--output", str(Path(tmp) / "r.json")
            )
            self.assertEqual(code, EXIT_MANIFEST_ERROR)
            self.assertIn("invalid JSON", err)

    def test_schema_violation_manifest_exits_three(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            v1_path = Path(tmp) / "v1.jsonl"
            v2_path = Path(tmp) / "v2.jsonl"
            v1_path.write_text("")
            v2_path.write_text("")
            manifest = Path(tmp) / "manifest.json"
            manifest.write_text(
                json.dumps(
                    {
                        "campaign": "c",
                        "comparison_key": COMPARISON_KEY,
                        "exports": {"v1": str(v1_path), "v2": str(v2_path)},
                        "expected_keys": ["g|s"],
                        "numeric_tolerances": {
                            "absolute_tolerance": -1,
                            "relative_tolerance": 0,
                        },
                    }
                ),
                encoding="utf-8",
            )
            code, _, err = self._run(
                "--manifest", str(manifest), "--output", str(Path(tmp) / "r.json")
            )
            self.assertEqual(code, EXIT_MANIFEST_ERROR)
            self.assertIn("must be >= 0", err)

    def test_missing_export_file_exits_four(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manifest = self._campaign(
                tmp, [_record("g1", "row-1")], [_record("g1", "row-1")], ["g1|row-1"]
            )
            loaded = json.loads(manifest.read_text(encoding="utf-8"))
            loaded["exports"]["v2"] = str(Path(tmp) / "absent.jsonl")
            manifest.write_text(json.dumps(loaded), encoding="utf-8")
            code, _, err = self._run(
                "--manifest", str(manifest), "--output", str(Path(tmp) / "r.json")
            )
            self.assertEqual(code, EXIT_INPUT_FILE_ERROR)
            self.assertIn("v2 export file not found", err)

    def test_malformed_jsonl_exits_five(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            v1_path = Path(tmp) / "v1.jsonl"
            v2_path = Path(tmp) / "v2.jsonl"
            v1_path.write_text('{"Group": "g1"}\nnot-json\n', encoding="utf-8")
            v2_path.write_text("", encoding="utf-8")
            manifest = Path(tmp) / "manifest.json"
            manifest.write_text(
                json.dumps(
                    {
                        "campaign": "c",
                        "comparison_key": COMPARISON_KEY,
                        "exports": {"v1": str(v1_path), "v2": str(v2_path)},
                        "expected_keys": ["g|s"],
                        "numeric_tolerances": dict(self._TOLERANCES),
                    }
                ),
                encoding="utf-8",
            )
            code, _, err = self._run(
                "--manifest", str(manifest), "--output", str(Path(tmp) / "r.json")
            )
            self.assertEqual(code, EXIT_JSONL_ERROR)
            self.assertIn("malformed JSON line 2", err)

    def test_usage_error_exits_six(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            code, _, err = self._run("--output", "r.json")
            self.assertEqual(code, EXIT_USAGE_ERROR)
            self.assertIn("missing required option --manifest", err)

    def test_help_exits_zero(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            code, out, _ = self._run("--help")
            self.assertEqual(code, EXIT_PARITY_OK)
            self.assertIn("usage:", out)

    def test_shipped_fixtures_run_through_the_cli(self) -> None:
        # The shipped fixture manifest resolves its exports from the repo
        # root, like source_csv; the run stays incomplete on purpose: the
        # V1-05a fixtures carry unmatched and conflicting identities.
        output_root = ROOT / ".tmp" / "v1-05c-cli-fixture-report.json"
        with contextlib.chdir(ROOT):
            code, out, _ = self._run(
                "--manifest", str(FIXTURES / "parity_manifest.json"),
                "--output", str(output_root),
            )
        self.assertEqual(code, EXIT_INCOMPLETE_INPUTS)
        self.assertIn(f"verdict: {VERDICT_INCOMPLETE_INPUTS}", out)
        report = json.loads(output_root.read_text(encoding="utf-8"))
        self.assertEqual(report["summary"]["verdict"], VERDICT_INCOMPLETE_INPUTS)
        output_root.unlink()


if __name__ == "__main__":
    unittest.main()
