"""Output contract tests for the explainability-v2 schema and message builder.

OC-01: the v2 Avro schema and ``build_output_record`` must be able to represent
the event identity (``source_event_id``), the HSTree verdict fields
(``hstree_status``, ``hstree_anomaly``, ``hstree_threshold``) and the explanation
outcome (``explanation_status``, ``explanation_error_code``) — without changing
the SHAP computation or the runtime status attribution (deferred to OC-02).

Coercion rules locked by these tests (strict: invalid means null):

- strings (``source_event_id``, ``hstree_status``,
  ``explanation_error_code``): only real ``str`` values pass, everything else
  becomes ``None`` — no silent stringification, since ``source_event_id`` is
  the event identity key;
- ``explanation_status``: only members of ``EXPLANATION_STATUSES`` pass,
  everything else becomes ``None``;
- ``hstree_anomaly``: only real booleans pass, everything else becomes ``None``
  (``bool("false") is True`` makes any coercion dangerous);
- ``hstree_threshold``: numeric values are floated, booleans and non-numeric
  values become ``None`` (``0.0`` would be a meaningful threshold, so invalid
  values must not fall back to it).

OC-02 will make ``source_event_id`` and ``explanation_status`` mandatory in the
runtime; the builder only refuses invalid values.
"""

from __future__ import annotations

import hashlib
import json
import re
from unittest import TestCase

from fastavro.validation import validate

from dasma_kafka.explainability_v2.config import (
    EXPLAINABILITY_V2_VALUE_SCHEMA_STR,
    EXPLANATION_PROFILE,
    EXPLANATION_STATUSES,
    HSTREE_MODEL_VERSION,
)
from dasma_kafka.explainability_v2.features import (
    build_output_record,
    build_viz_json_payload,
    compute_feature_fingerprint,
    compute_hstree_model_fingerprint,
)


SHA256_HEX_RE = re.compile(r"^[0-9a-f]{64}$")


def _parse_schema() -> dict:
    return json.loads(EXPLAINABILITY_V2_VALUE_SCHEMA_STR)


def _field(schema: dict, name: str) -> dict:
    matches = [field for field in schema["fields"] if field["name"] == name]
    assert len(matches) == 1, f"field {name} must be declared exactly once"
    return matches[0]


class ExplainabilityV2SchemaContractTest(TestCase):
    def setUp(self) -> None:
        self.schema = _parse_schema()

    def test_record_name_and_namespace_are_unchanged(self) -> None:
        self.assertEqual(self.schema["name"], "ExplainabilityV2Event")
        self.assertEqual(self.schema["namespace"], "dasma.kafka")

    def test_legacy_fields_are_preserved_nullable_with_null_default(self) -> None:
        expected = {
            "timestamp": ["null", "string"],
            "Group": ["null", "string"],
            "hstree_score": ["null", "double"],
            "features": ["null", {"type": "map", "values": "double"}],
            "shap_values": ["null", {"type": "map", "values": "double"}],
            "shap_base_value": ["null", "double"],
            "shap_computation_time_ms": ["null", "double"],
            "local_hstree_score": ["null", "double"],
        }
        for name, avro_type in expected.items():
            field = _field(self.schema, name)
            self.assertEqual(field["type"], avro_type, name)
            self.assertIsNone(field["default"], name)

    def test_new_fields_are_nullable_with_null_default(self) -> None:
        expected = {
            "source_event_id": "string",
            "hstree_status": "string",
            "hstree_anomaly": "boolean",
            "hstree_threshold": "double",
            "explanation_status": "string",
            "explanation_error_code": "string",
        }
        for name, base_type in expected.items():
            field = _field(self.schema, name)
            self.assertEqual(field["type"], ["null", base_type], name)
            self.assertIsNone(field["default"], name)

    def test_explanation_status_vocabulary_is_declared(self) -> None:
        self.assertEqual(
            EXPLANATION_STATUSES,
            frozenset({"disabled", "warmup", "background_not_ready", "computed", "failed", "model_mismatch"}),
        )

    def test_legacy_message_without_new_fields_remains_valid(self) -> None:
        legacy_record = {
            "timestamp": "2026-01-01 00:00:00",
            "Group": "1A",
            "hstree_score": 0.5,
            "features": {"A": 1.0},
            "shap_values": {"A": 0.25},
            "shap_base_value": 0.1,
            "shap_computation_time_ms": 12.5,
            "local_hstree_score": 0.48,
        }
        self.assertTrue(validate(legacy_record, self.schema))

    def test_full_record_with_every_status_field_is_valid(self) -> None:
        record = {
            "timestamp": "2026-01-01 00:00:00",
            "Group": "1A",
            "hstree_score": 0.5,
            "features": {"A": 1.0},
            "shap_values": None,
            "shap_base_value": None,
            "shap_computation_time_ms": None,
            "local_hstree_score": 0.48,
            "source_event_id": "row-000042",
            "hstree_status": "anomaly",
            "hstree_anomaly": True,
            "hstree_threshold": 0.9,
            "explanation_status": "computed",
            "explanation_error_code": None,
        }
        self.assertTrue(validate(record, self.schema))

    def test_failed_message_with_null_shap_values_is_valid(self) -> None:
        record = {
            "timestamp": "2026-01-01 00:00:00",
            "Group": "1A",
            "hstree_score": 0.5,
            "features": {"A": 1.0},
            "shap_values": None,
            "shap_base_value": None,
            "shap_computation_time_ms": None,
            "local_hstree_score": 0.48,
            "source_event_id": "row-000043",
            "hstree_status": "normal",
            "hstree_anomaly": False,
            "hstree_threshold": 0.9,
            "explanation_status": "failed",
            "explanation_error_code": "shap_exception",
        }
        self.assertTrue(validate(record, self.schema))


class BuildOutputRecordContractTest(TestCase):
    def setUp(self) -> None:
        self.record = {
            "timestamp": "2026-01-01 00:00:01",
            "Group": "2B",
            "source_event_id": "row-000007",
            "hstree_score": 1.25,
            "hstree_status": "anomaly",
            "hstree_anomaly": True,
            "hstree_threshold": 0.75,
            "A": 1.5,
            "B": 2.5,
        }
        self.shap_result = {
            "shap_values": {"A": 0.5, "B": -0.25},
            "shap_base_value": 0.1,
            "shap_computation_time_ms": 8.0,
        }

    def _build(self, record=None, shap_result=None, **kwargs):
        kwargs.setdefault("explained_model", "hstree")
        return build_output_record(
            record=self.record if record is None else record,
            group="2B",
            feature_names=["A", "B"],
            shap_result=self.shap_result if shap_result is None else shap_result,
            local_score=1.2,
            **kwargs,
        )

    def test_builder_rejects_missing_or_unknown_explained_model(self) -> None:
        for explained_model in (None, "", "unknown"):
            with self.subTest(explained_model=explained_model):
                with self.assertRaisesRegex(ValueError, "explained_model must be"):
                    self._build(explained_model=explained_model)

    def test_builder_accepts_kitnet_and_keeps_generic_local_score(self) -> None:
        out = self._build(explained_model="kitnet")
        self.assertEqual(out["explained_model"], "kitnet")
        self.assertEqual(out["local_score"], 1.2)
        self.assertIsNone(out["local_hstree_score"])

    def test_present_values_are_transmitted(self) -> None:
        out = self._build(explanation_status="computed")

        self.assertEqual(out["source_event_id"], "row-000007")
        self.assertEqual(out["hstree_status"], "anomaly")
        self.assertIs(out["hstree_anomaly"], True)
        self.assertEqual(out["hstree_threshold"], 0.75)
        self.assertEqual(out["explanation_status"], "computed")
        self.assertIsNone(out["explanation_error_code"])

    def test_missing_values_fall_back_to_null(self) -> None:
        out = build_output_record(
            record={"timestamp": None, "Group": "2B", "A": 1.0},
            group="2B",
            feature_names=["A"],
            shap_result={
                "shap_values": None,
                "shap_base_value": None,
                "shap_computation_time_ms": None,
            },
            local_score=None,
            explained_model="hstree",
        )

        for name in (
            "source_event_id",
            "hstree_status",
            "hstree_anomaly",
            "hstree_threshold",
            "explanation_status",
            "explanation_error_code",
        ):
            self.assertIsNone(out[name], name)
        # Legacy behavior is preserved.
        self.assertIsNone(out["shap_values"])
        self.assertIsNone(out["shap_base_value"])
        self.assertIsNone(out["local_hstree_score"])

    def test_invalid_values_are_rejected_to_null(self) -> None:
        out = self._build(
            record={
                "Group": "2B",
                "source_event_id": 42,
                "hstree_status": 7,
                "hstree_anomaly": "yes",
                "hstree_threshold": "abc",
            },
            explanation_status="bogus",
        )

        self.assertIsNone(out["source_event_id"])
        self.assertIsNone(out["hstree_status"])
        self.assertIsNone(out["hstree_anomaly"])
        self.assertIsNone(out["hstree_threshold"])
        self.assertIsNone(out["explanation_status"])

    def test_explanation_status_must_belong_to_the_declared_vocabulary(self) -> None:
        for status in sorted(EXPLANATION_STATUSES):
            out = self._build(explanation_status=status)
            self.assertEqual(out["explanation_status"], status, status)

        for invalid in ("bogus", "", "Computed", None, 5):
            out = self._build(explanation_status=invalid)
            self.assertIsNone(out["explanation_status"], repr(invalid))

    def test_boolean_threshold_is_not_coerced_to_float(self) -> None:
        out = self._build(record={"Group": "2B", "hstree_threshold": True})

        self.assertIsNone(out["hstree_threshold"])

    def test_existing_call_signature_still_works(self) -> None:
        out = build_output_record(
            self.record, "2B", ["A", "B"], self.shap_result, 1.2,
            explained_model="hstree",
        )

        self.assertIsNone(out["explanation_status"])
        self.assertIsNone(out["explanation_error_code"])
        self.assertEqual(out["hstree_score"], 1.25)
        self.assertEqual(out["features"], {"A": 1.5, "B": 2.5})

    def test_builder_output_validates_against_the_schema(self) -> None:
        out = self._build(
            explanation_status="failed",
            explanation_error_code="shap_exception",
        )

        self.assertTrue(validate(out, _parse_schema()))

    def test_builder_output_without_new_fields_validates_against_the_schema(self) -> None:
        out = self._build(
            record={"timestamp": None, "Group": "2B", "A": 1.0},
            shap_result={
                "shap_values": None,
                "shap_base_value": None,
                "shap_computation_time_ms": None,
            },
        )

        self.assertTrue(validate(out, _parse_schema()))


class ExplainabilityV2MetadataContractTest(TestCase):
    """OC-03: computation metadata fields, constants and fingerprints."""

    def setUp(self) -> None:
        self.schema = _parse_schema()

    def test_schema_declares_metadata_fields_nullable_with_null_default(self) -> None:
        expected = {
            "explanation_profile": "string",
            "shap_nsamples": "int",
            "shap_background_size": "int",
            "explainability_seed": "long",
            "hstree_model_version": "string",
            "hstree_model_fingerprint": "string",
            "feature_fingerprint": "string",
        }
        for name, base_type in expected.items():
            field = _field(self.schema, name)
            self.assertEqual(field["type"], ["null", base_type], name)
            self.assertIsNone(field["default"], name)

    def test_message_without_metadata_fields_remains_valid(self) -> None:
        legacy_record = {
            "timestamp": "2026-01-01 00:00:00",
            "Group": "1A",
            "hstree_score": 0.5,
            "features": {"A": 1.0},
            "shap_values": {"A": 0.25},
            "shap_base_value": 0.1,
            "shap_computation_time_ms": 12.5,
            "local_hstree_score": 0.48,
            "source_event_id": "row-000042",
            "hstree_status": "normal",
            "hstree_anomaly": False,
            "hstree_threshold": 0.9,
            "explanation_status": "computed",
            "explanation_error_code": None,
        }
        self.assertTrue(validate(legacy_record, self.schema))

    def test_constants_are_the_contracted_values(self) -> None:
        self.assertEqual(EXPLANATION_PROFILE, "exhaustive")
        self.assertEqual(HSTREE_MODEL_VERSION, "local-hstree-v3")

    def test_feature_fingerprint_is_deterministic_sha256_hex(self) -> None:
        first = compute_feature_fingerprint(["A", "B", "C"])
        second = compute_feature_fingerprint(["A", "B", "C"])

        self.assertEqual(first, second)
        self.assertRegex(first, SHA256_HEX_RE)

    def test_feature_fingerprint_matches_independent_sha256(self) -> None:
        expected = hashlib.sha256(
            json.dumps(["A", "B"], ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        ).hexdigest()

        self.assertEqual(compute_feature_fingerprint(["A", "B"]), expected)

    def test_feature_fingerprint_changes_with_order(self) -> None:
        self.assertNotEqual(
            compute_feature_fingerprint(["A", "B"]),
            compute_feature_fingerprint(["B", "A"]),
        )

    def test_model_fingerprint_matches_independent_sha256(self) -> None:
        expected = hashlib.sha256(
            json.dumps(
                {
                    "hstree_model_version": "local-hstree-v3",
                    "hstree_window_size": 2048,
                    "hstree_num_trees": 25,
                    "hstree_max_depth": 15,
                    "hstree_initial_window_size": 512,
                    "hstree_seed": 42,
                },
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()

        self.assertEqual(
            compute_hstree_model_fingerprint(
                hstree_model_version="local-hstree-v3",
                hstree_window_size=2048,
                hstree_num_trees=25,
                hstree_max_depth=15,
                hstree_initial_window_size=512,
                hstree_seed=42,
            ),
            expected,
        )

    def test_model_fingerprint_changes_with_any_hyperparameter(self) -> None:
        baseline_kwargs = {
            "hstree_model_version": "local-hstree-v3",
            "hstree_window_size": 2048,
            "hstree_num_trees": 25,
            "hstree_max_depth": 15,
            "hstree_initial_window_size": 512,
            "hstree_seed": 42,
        }
        baseline = compute_hstree_model_fingerprint(**baseline_kwargs)

        self.assertRegex(baseline, SHA256_HEX_RE)
        for changed_param, changed_value in (
            ("hstree_model_version", "local-hstree-v2"),
            ("hstree_window_size", 1024),
            ("hstree_num_trees", 50),
            ("hstree_max_depth", 10),
            ("hstree_initial_window_size", 128),
            ("hstree_seed", 43),
        ):
            kwargs = dict(baseline_kwargs, **{changed_param: changed_value})
            with self.subTest(changed_param=changed_param):
                self.assertNotEqual(
                    compute_hstree_model_fingerprint(**kwargs),
                    baseline,
                )

    def test_builder_transmits_the_metadata_fields(self) -> None:
        out = build_output_record(
            record={"Group": "2B", "source_event_id": "row-1"},
            group="2B",
            feature_names=["A"],
            shap_result={"shap_values": None, "shap_base_value": None, "shap_computation_time_ms": None},
            local_score=None,
            explanation_status="disabled",
            explanation_profile=EXPLANATION_PROFILE,
            explained_model="hstree",
            shap_nsamples=100,
            shap_background_size=100,
            explainability_seed=42,
            hstree_model_version=HSTREE_MODEL_VERSION,
            hstree_model_fingerprint="a" * 64,
            feature_fingerprint="b" * 64,
        )

        self.assertEqual(out["explanation_profile"], "exhaustive")
        self.assertEqual(out["shap_nsamples"], 100)
        self.assertEqual(out["shap_background_size"], 100)
        self.assertEqual(out["explainability_seed"], 42)
        self.assertEqual(out["hstree_model_version"], "local-hstree-v3")
        self.assertEqual(out["hstree_model_fingerprint"], "a" * 64)
        self.assertEqual(out["feature_fingerprint"], "b" * 64)
        self.assertTrue(validate(out, self.schema))

    def test_builder_without_metadata_defaults_to_null(self) -> None:
        out = build_output_record(
            record={"Group": "2B", "source_event_id": "row-1"},
            group="2B",
            feature_names=["A"],
            shap_result={"shap_values": None, "shap_base_value": None, "shap_computation_time_ms": None},
            local_score=None,
            explained_model="hstree",
        )

        for name in (
            "explanation_profile",
            "shap_nsamples",
            "shap_background_size",
            "explainability_seed",
            "hstree_model_version",
            "hstree_model_fingerprint",
            "feature_fingerprint",
        ):
            self.assertIsNone(out[name], name)
        self.assertTrue(validate(out, self.schema))


class BuildVizJsonPayloadContractTest(TestCase):
    """The JSON viz copy carries the event identity inside ``shap_values``.

    Telegraf json_v2 (1.30.3) resolves an object block's ``tags`` only from
    keys located inside the object at that path. The per-feature SHAP points
    therefore need Group/source_event_id INSIDE the ``shap_values`` map of
    the JSON viz copy, while the Avro record (strict ``map<double>`` schema)
    and the OC exports stay untouched.
    """

    def setUp(self) -> None:
        self.record = {
            "timestamp": "2026-01-01 00:00:01",
            "Group": "2B",
            "source_event_id": "row-000007",
            "hstree_score": 1.25,
            "hstree_anomaly": True,
            "shap_values": {"A": 0.5, "B": -0.25},
            "shap_base_value": 0.1,
        }

    def test_identity_is_injected_inside_shap_values(self) -> None:
        payload = json.loads(build_viz_json_payload(self.record))
        self.assertEqual(payload["shap_values"]["Group"], "2B")
        self.assertEqual(payload["shap_values"]["source_event_id"], "row-000007")
        # The SHAP contributions themselves are untouched.
        self.assertEqual(payload["shap_values"]["A"], 0.5)
        self.assertEqual(payload["shap_values"]["B"], -0.25)
        # The rest of the record is unchanged.
        self.assertEqual(payload["Group"], "2B")
        self.assertEqual(payload["source_event_id"], "row-000007")
        self.assertEqual(payload["shap_base_value"], 0.1)

    def test_identity_values_come_from_the_record_itself(self) -> None:
        modified = dict(self.record)
        modified["shap_values"] = {"A": 0.5, "B": -0.25, "Group": "WRONG"}
        payload = json.loads(build_viz_json_payload(modified))
        # The record-level identity wins over any stale value.
        self.assertEqual(payload["shap_values"]["Group"], "2B")
        self.assertEqual(payload["shap_values"]["source_event_id"], "row-000007")

    def test_warmup_record_without_shap_values_is_unchanged(self) -> None:
        warmup = dict(self.record)
        warmup["shap_values"] = None
        warmup["source_event_id"] = None
        self.assertEqual(
            build_viz_json_payload(warmup), json.dumps(warmup, ensure_ascii=False)
        )

    def test_avro_record_has_no_identity_inside_shap_values(self) -> None:
        # build_output_record (Avro channel) must stay a strict map<double>:
        # the identity injection happens only in the viz JSON payload.
        out = build_output_record(
            record=self.record,
            group="2B",
            feature_names=["A", "B"],
            shap_result={
                "shap_values": {"A": 0.5, "B": -0.25},
                "shap_base_value": 0.1,
                "shap_computation_time_ms": 8.0,
            },
            local_score=1.2,
            explanation_status="computed",
            explained_model="hstree",
        )
        self.assertEqual(out["shap_values"], {"A": 0.5, "B": -0.25})

    def test_payload_is_valid_json_with_ascii_escaping(self) -> None:
        self.record["Group"] = "Sortie CTA"
        self.record["shap_values"] = {"A": 0.5, "B": -0.25}
        payload = build_viz_json_payload(self.record)
        parsed = json.loads(payload)
        self.assertEqual(parsed["Group"], "Sortie CTA")
        self.assertEqual(parsed["shap_values"]["Group"], "Sortie CTA")
