"""Unit tests for the reproducibility CSV contract (P2-2).

Pure unit tests: no broker, no Docker, no temporary files. They lock the CSV
format v1 (columns, projection, deterministic cell encoding, canonical key,
metadata) shared by the P2-3 exporter and the P2-5 comparator.
"""

from __future__ import annotations

import json
import math
from unittest import TestCase

from validation.reproducibility.csv_contract import (
    COMMON_COLUMNS,
    FORMAT_VERSION,
    decode_v1_row,
    MODEL_COLUMNS,
    MODEL_FIELD_MAP,
    build_metadata,
    canonical_key,
    columns_for_model,
    normalize_viz_record,
    sort_canonical_rows,
)

MODELS = ("hstree", "iforest", "kitnet", "deepant", "mdragpoint")


def _viz_record(model: str, **overrides):
    base = {
        "source_event_id": "event-000000000001",
        "timestamp": "2026-09-15 10:00:00",
        "Group": "G1",
        f"{model}_score": 0.0123,
        f"{model}_status": "normal",
        f"{model}_threshold": 1.5,
        f"{model}_anomaly": False,
        "mdragpoint_internal_r": 0.75,
    }
    base.update(overrides)
    return base


class ColumnsContractTest(TestCase):
    """Family 1: constants and column layout."""

    def test_format_version_and_common_columns_exact(self) -> None:
        self.assertEqual(FORMAT_VERSION, 1)
        self.assertEqual(
            COMMON_COLUMNS,
            ("source_event_id", "timestamp", "Group", "score", "status", "threshold", "anomaly"),
        )
        self.assertIsInstance(FORMAT_VERSION, int)

    def test_model_columns(self) -> None:
        for model in ("hstree", "iforest", "kitnet", "deepant"):
            self.assertEqual(MODEL_COLUMNS[model], COMMON_COLUMNS)
        self.assertEqual(
            MODEL_COLUMNS["mdragpoint"],
            COMMON_COLUMNS + ("mdragpoint_internal_r",),
        )

    def test_columns_for_model_accepts_five_models(self) -> None:
        for model in MODELS:
            self.assertEqual(columns_for_model(model), MODEL_COLUMNS[model])

    def test_columns_for_model_rejects_explainability_and_unknown(self) -> None:
        for model in ("explainability", "explainability-v2", "mdragsubseq", ""):
            with self.assertRaises(ValueError, msg=model):
                columns_for_model(model)

    def test_mdragpoint_has_only_the_documented_extra(self) -> None:
        self.assertEqual(
            MODEL_COLUMNS["mdragpoint"],
            COMMON_COLUMNS + ("mdragpoint_internal_r",),
        )
        for model in ("hstree", "iforest", "kitnet", "deepant"):
            self.assertNotIn("mdragpoint_internal_r", MODEL_COLUMNS[model])


class ProjectionTest(TestCase):
    """Family 2: exhaustive projection for the five models."""

    def test_hstree_projection_exact(self) -> None:
        row = normalize_viz_record("hstree", _viz_record("hstree"))
        self.assertEqual(
            row,
            {
                "source_event_id": "event-000000000001",
                "timestamp": "2026-09-15 10:00:00",
                "Group": "G1",
                "score": "0.0123",
                "status": "normal",
                "threshold": "1.5",
                "anomaly": "false",
            },
        )
        self.assertEqual(list(row), list(MODEL_COLUMNS["hstree"]))

    def test_each_model_uses_its_prefixed_fields(self) -> None:
        for model in MODELS:
            record = _viz_record(model)
            record[f"{model}_score"] = 0.5
            row = normalize_viz_record(model, record)
            self.assertEqual(row["score"], "0.5", model)
            self.assertEqual(set(row), set(MODEL_COLUMNS[model]), model)

    def test_prefixed_and_derived_fields_not_exported(self) -> None:
        record = _viz_record(
            "hstree",
            hstree_score_ma=0.4,
            hstree_score_log10=0.01,
            some_unknown_field="x",
            another_prefix_field=3,
        )
        row = normalize_viz_record("hstree", record)
        self.assertEqual(set(row), set(MODEL_COLUMNS["hstree"]))
        self.assertNotIn("hstree_score_ma", row)
        self.assertNotIn("hstree_score_log10", row)
        self.assertNotIn("another_unknown_field", row)

    def test_mdragpoint_includes_internal_r(self) -> None:
        row = normalize_viz_record(
            "mdragpoint",
            _viz_record("mdragpoint", mdragpoint_internal_r=None),
        )
        self.assertIn("mdragpoint_internal_r", row)
        self.assertEqual(row["mdragpoint_internal_r"], "")
        self.assertEqual(list(row), list(MODEL_COLUMNS["mdragpoint"]))

    def test_non_mapping_record_rejected(self) -> None:
        for bad in ("string", 42, None, ["x"]):
            with self.assertRaises(ValueError, msg=repr(bad)):
                normalize_viz_record("hstree", bad)


class NumericEncodingTest(TestCase):
    """Family 3: None / NaN / infinities / numbers, exact v1 tokens."""

    def _score_cell(self, value) -> str:
        return normalize_viz_record("hstree", _viz_record("hstree", hstree_score=value))["score"]

    def test_null_becomes_empty_cell(self) -> None:
        self.assertEqual(self._score_cell(None), "")
        self.assertEqual(
            normalize_viz_record("hstree", _viz_record("hstree", hstree_threshold=None))["threshold"],
            "",
        )

    def test_nan_and_infinities_use_exact_tokens(self) -> None:
        self.assertEqual(self._score_cell(float("nan")), "NaN")
        self.assertEqual(self._score_cell(float("inf")), "Infinity")
        self.assertEqual(self._score_cell(float("-inf")), "-Infinity")
        # No platform-dependent nan spelling ever reaches the cells.
        for value in (float("nan"), float("inf"), float("-inf")):
            cell = self._score_cell(value)
            self.assertIn(cell, {"NaN", "Infinity", "-Infinity"})

    def test_finite_numbers_use_repr_float(self) -> None:
        self.assertEqual(self._score_cell(1), "1.0")
        self.assertEqual(self._score_cell(1.5), "1.5")
        self.assertEqual(self._score_cell(0), "0.0")
        self.assertEqual(self._score_cell(-2.25), "-2.25")
        self.assertEqual(self._score_cell(0.1), repr(float(0.1)))
        self.assertEqual(self._score_cell(1e16), repr(float(1e16)))

    def test_booleans_are_never_numbers(self) -> None:
        for value in (True, False):
            with self.assertRaises(ValueError):
                normalize_viz_record("hstree", _viz_record("hstree", hstree_score=value))
            with self.assertRaises(ValueError):
                normalize_viz_record(
                    "mdragpoint", _viz_record("mdragpoint", mdragpoint_internal_r=True),
                )

    def test_non_numeric_values_rejected(self) -> None:
        for value in ("1.5", [1], {"x": 1}, object()):
            with self.assertRaises(ValueError):
                normalize_viz_record("hstree", _viz_record("hstree", hstree_score=value))
            with self.assertRaises(ValueError):
                normalize_viz_record(
                    "mdragpoint", _viz_record("mdragpoint", mdragpoint_internal_r=value),
                )

    def test_missing_numeric_field_is_an_error_even_for_none_capable(self) -> None:
        record = _viz_record("hstree")
        del record["hstree_threshold"]
        with self.assertRaises(ValueError) as ctx:
            normalize_viz_record("hstree", record)
        self.assertIn("hstree_threshold", str(ctx.exception))

    def test_mdragpoint_internal_r_is_mandatory(self) -> None:
        record = _viz_record("mdragpoint")
        del record["mdragpoint_internal_r"]
        with self.assertRaises(ValueError) as ctx:
            normalize_viz_record("mdragpoint", record)
        self.assertIn("mdragpoint_internal_r", str(ctx.exception))
        # Explicit null remains a valid empty cell.
        row = normalize_viz_record("mdragpoint", _viz_record("mdragpoint", mdragpoint_internal_r=None))
        self.assertEqual(row["mdragpoint_internal_r"], "")


class KeysAndOrderTest(TestCase):
    """Family 4: canonical key, lexical order, duplicate rejection."""

    def test_canonical_key_is_group_then_event_id(self) -> None:
        self.assertEqual(
            canonical_key({"Group": "A", "source_event_id": "event-1"}),
            ("A", "event-1"),
        )

    def test_canonical_key_rejects_missing_or_empty(self) -> None:
        for row in ({}, {"Group": "A"}, {"source_event_id": "e"}, {"Group": "", "source_event_id": "e"}, {"Group": "A", "source_event_id": ""}):
            with self.assertRaises(ValueError):
                canonical_key(row)

    def test_no_group_lowercase_fallback(self) -> None:
        record = _viz_record("hstree")
        record["group"] = "fallback"
        del record["Group"]
        with self.assertRaises(ValueError) as ctx:
            normalize_viz_record("hstree", record)
        # The error names the mandatory source key, never the lowercase twin.
        self.assertIn("'Group'", str(ctx.exception))

    def test_lexical_order_of_tuples(self) -> None:
        rows = [
            {"Group": "A", "source_event_id": "event-2"},
            {"Group": "A", "source_event_id": "event-10"},
            {"Group": "B", "source_event_id": "event-1"},
        ]
        sorted_rows = sort_canonical_rows(rows)
        self.assertEqual(
            [canonical_key(row) for row in sorted_rows],
            [("A", "event-10"), ("A", "event-2"), ("B", "event-1")],
        )

    def test_sort_canonical_rows_rejects_duplicates(self) -> None:
        rows = [
            {"Group": "A", "source_event_id": "event-1"},
            {"Group": "A", "source_event_id": "event-1"},
        ]
        with self.assertRaises(ValueError) as ctx:
            sort_canonical_rows(rows)
        self.assertIn("duplicate", str(ctx.exception))

    def test_sort_canonical_rows_copies_input(self) -> None:
        original = {"Group": "A", "source_event_id": "event-1", "score": "1.0"}
        rows = [original]
        result = sort_canonical_rows(rows)
        result[0]["score"] = "changed"
        self.assertEqual(original["score"], "1.0")

    def test_identifiers_are_not_stripped(self) -> None:
        row = normalize_viz_record(
            "hstree", _viz_record("hstree", Group="  A  ", source_event_id=" e1 "),
        )
        self.assertEqual(row["Group"], "  A  ")
        self.assertEqual(row["source_event_id"], " e1 ")


class VizRecordMandatoryFieldsTest(TestCase):
    """Families 4+5: identifiers, anomaly and status typing."""

    def test_missing_identifier_is_an_error_without_fallback(self) -> None:
        record = _viz_record("hstree")
        del record["source_event_id"]
        with self.assertRaises(ValueError):
            normalize_viz_record("hstree", record)

        record = _viz_record("hstree")
        del record["Group"]
        record["group"] = "lowercase"
        with self.assertRaises(ValueError):
            normalize_viz_record("hstree", record)

    def test_null_or_empty_identifier_rejected(self) -> None:
        with self.assertRaises(ValueError):
            normalize_viz_record("hstree", _viz_record("hstree", source_event_id=None))
        with self.assertRaises(ValueError):
            normalize_viz_record("hstree", _viz_record("hstree", source_event_id=""))
        with self.assertRaises(ValueError):
            normalize_viz_record("hstree", _viz_record("hstree", Group=None))

    def test_timestamp_optional(self) -> None:
        row = normalize_viz_record("hstree", _viz_record("hstree", timestamp=None))
        self.assertEqual(row["timestamp"], "")
        row = normalize_viz_record("hstree", _viz_record("hstree", timestamp=12345))
        self.assertEqual(row["timestamp"], "12345")

    def test_non_numeric_score_rejected(self) -> None:
        for value in ("0.5", [0.5], {"v": 0.5}):
            with self.assertRaises(ValueError):
                normalize_viz_record("hstree", _viz_record("hstree", hstree_score=value))

    def test_anomaly_true_false_null_and_errors(self) -> None:
        self.assertEqual(
            normalize_viz_record("hstree", _viz_record("hstree", hstree_anomaly=True))["anomaly"],
            "true",
        )
        self.assertEqual(
            normalize_viz_record("hstree", _viz_record("hstree", hstree_anomaly=False))["anomaly"],
            "false",
        )
        self.assertEqual(
            normalize_viz_record("hstree", _viz_record("hstree", hstree_anomaly=None))["anomaly"],
            "",
        )
        for value in (1, "true", 0.0):
            with self.assertRaises(ValueError):
                normalize_viz_record("hstree", _viz_record("hstree", hstree_anomaly=value))

    def test_status_string_or_null_only(self) -> None:
        self.assertEqual(
            normalize_viz_record("hstree", _viz_record("hstree", hstree_status=None))["status"],
            "",
        )
        self.assertEqual(
            normalize_viz_record("hstree", _viz_record("hstree", hstree_status="normal"))["status"],
            "normal",
        )
        for value in (1, True, 0.5):
            with self.assertRaises(ValueError):
                normalize_viz_record("hstree", _viz_record("hstree", hstree_status=value))

    def test_degraded_output_preserved(self) -> None:
        """All nullable model fields may be None: cells stay empty, no error."""
        row = normalize_viz_record(
            "hstree",
            _viz_record(
                "hstree",
                hstree_score=None,
                hstree_status=None,
                hstree_threshold=None,
                hstree_anomaly=None,
                timestamp=None,
            ),
        )
        self.assertEqual(
            row,
            {
                "source_event_id": "event-000000000001",
                "timestamp": "",
                "Group": "G1",
                "score": "",
                "status": "",
                "threshold": "",
                "anomaly": "",
            },
        )


class DecodeV1RowTest(TestCase):
    """Family: symmetric CSV v1 decoding (P2-5 comparator reuse)."""

    def _cells(self, model: str, **overrides):
        base = {
            "source_event_id": "event-1",
            "timestamp": "2026-09-15 10:00:00",
            "Group": "A",
            "score": "0.0123",
            "status": "normal",
            "threshold": "1.5",
            "anomaly": "false",
            "mdragpoint_internal_r": "0.75",
        }
        base.update(overrides)
        return base

    def test_round_trip_all_models(self) -> None:
        for model in MODELS:
            encoded = normalize_viz_record(model, _viz_record(model))
            decoded = decode_v1_row(model, encoded)
            expected = {
                "source_event_id": "event-000000000001",
                "timestamp": "2026-09-15 10:00:00",
                "Group": "G1",
                "score": 0.0123,
                "status": "normal",
                "threshold": 1.5,
                "anomaly": False,
            }
            if model == "mdragpoint":
                expected["mdragpoint_internal_r"] = 0.75
            self.assertEqual(decoded, expected, model)

    def test_null_and_boolean_and_special_numbers(self) -> None:
        decoded = decode_v1_row(
            "hstree",
            self._cells(
                "hstree",
                score="NaN",
                status="",
                threshold="",
                anomaly="true",
            ),
        )
        self.assertIsNone(decoded["threshold"])
        self.assertIsNone(decoded["status"])
        self.assertIs(decoded["anomaly"], True)
        self.assertTrue(math.isnan(decoded["score"]))

        decoded = decode_v1_row(
            "hstree",
            self._cells("hstree", score="Infinity", threshold="-Infinity", anomaly=""),
        )
        self.assertTrue(math.isinf(decoded["score"]) and decoded["score"] > 0)
        self.assertTrue(math.isinf(decoded["threshold"]) and decoded["threshold"] < 0)

    def test_finite_numbers_and_negative_zero(self) -> None:
        decoded = decode_v1_row(
            "hstree", self._cells("hstree", score="-0.0", threshold="1e+16")
        )
        self.assertEqual(decoded["score"], 0.0)
        self.assertTrue(math.copysign(1.0, decoded["score"]) < 0)
        self.assertEqual(decoded["threshold"], 1e16)

    def test_invalid_numeric_grammars_rejected(self) -> None:
        for cell in ("nan", "NaN ", " 1.0", "1.50", "1e5", "inf", "Infinity ", "+1.0", "1_0", "true"):
            with self.assertRaises(ValueError, msg=cell):
                decode_v1_row("hstree", self._cells("hstree", score=cell))

    def test_invalid_boolean_and_missing_columns_rejected(self) -> None:
        for cell in ("1", "yes", "TRUE", "0"):
            with self.assertRaises(ValueError, msg=cell):
                decode_v1_row("hstree", self._cells("hstree", anomaly=cell))
        record = self._cells("hstree")
        del record["threshold"]
        with self.assertRaises(ValueError):
            decode_v1_row("hstree", record)
        with self.assertRaises(ValueError):
            decode_v1_row("hstree", self._cells("hstree", source_event_id=""))


class MetadataContractTest(TestCase):
    """Family 6: metadata v1 companion dictionary."""

    def test_exact_keys_and_defaults(self) -> None:
        metadata = build_metadata(
            model="hstree",
            source_role="source-1",
            campaign_id="campaign-42",
            campaign_parameters={},
            image="",
        )
        self.assertEqual(
            list(metadata),
            [
                "format_version",
                "model",
                "source_role",
                "campaign_id",
                "columns",
                "campaign_parameters",
                "image",
            ],
        )
        self.assertEqual(metadata["format_version"], 1)
        self.assertEqual(metadata["model"], "hstree")
        self.assertEqual(metadata["source_role"], "source-1")
        self.assertEqual(metadata["campaign_id"], "campaign-42")
        self.assertEqual(metadata["columns"], list(COMMON_COLUMNS))
        self.assertEqual(metadata["campaign_parameters"], {})
        self.assertEqual(metadata["image"], "")
        # Standard JSON, no NaN token.
        json.dumps(metadata, allow_nan=False)

    def test_columns_always_derive_from_the_model(self) -> None:
        metadata = build_metadata(
            model="mdragpoint",
            source_role="source-2",
            campaign_id="campaign-42",
            campaign_parameters={},
            image="",
        )
        self.assertEqual(
            metadata["columns"],
            list(COMMON_COLUMNS) + ["mdragpoint_internal_r"],
        )

    def test_campaign_parameters_deep_copied(self) -> None:
        parameters = {"num_trees": 75, "nested": {"mode": "batch"}}
        metadata = build_metadata(
            model="hstree",
            source_role="source-1",
            campaign_id="campaign-42",
            campaign_parameters=parameters,
            image="",
        )
        parameters["num_trees"] = 999
        parameters["nested"]["mode"] = "realtime"
        self.assertEqual(metadata["campaign_parameters"]["num_trees"], 75)
        self.assertEqual(metadata["campaign_parameters"]["nested"]["mode"], "batch")

    def test_invalid_role_and_campaign_id_rejected(self) -> None:
        for role in ("", "source-3", "SOURCE-1"):
            with self.assertRaises(ValueError, msg=role) as ctx:
                build_metadata(
                    model="hstree",
                    source_role=role,
                    campaign_id="campaign-42",
                    campaign_parameters={},
                    image="",
                )
            self.assertIn("source role", str(ctx.exception))

        for campaign_id in ("", "  ", "cam paign", "cam/paign", "..", ".hidden", "a" * 129):
            with self.assertRaises(ValueError, msg=campaign_id) as ctx:
                build_metadata(
                    model="hstree",
                    source_role="source-1",
                    campaign_id=campaign_id,
                    campaign_parameters={},
                    image="",
                )
            self.assertIn("campaign-id", str(ctx.exception))

    def test_unknown_model_rejected(self) -> None:
        with self.assertRaises(ValueError):
            build_metadata(
                model="explainability",
                source_role="source-1",
                campaign_id="campaign-42",
                campaign_parameters={},
                image="",
            )

    def test_non_serializable_or_nan_parameters_rejected(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            build_metadata(
                model="hstree",
                source_role="source-1",
                campaign_id="campaign-42",
                campaign_parameters={"ensemble": {1, 2}},
                image="",
            )
        self.assertIn("campaign_parameters", str(ctx.exception))

        with self.assertRaises(ValueError) as ctx:
            build_metadata(
                model="hstree",
                source_role="source-1",
                campaign_id="campaign-42",
                campaign_parameters={"window": float("nan")},
                image="",
            )
        self.assertIn("campaign_parameters", str(ctx.exception))
