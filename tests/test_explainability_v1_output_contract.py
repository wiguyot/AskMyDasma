"""Output contract tests for the explainability V1 schema and message builder.

V1-01: the V1 Avro schema and ``build_output_record`` must carry the event
identity (``source_event_id``), the HSTree verdict fields (``hstree_status``,
``hstree_anomaly``, ``hstree_threshold``), the explanation outcome
(``explanation_status``, ``explanation_error_code``) and the computation
metadata, with the exact same canonical representations as V2 so the two
flows are comparable.

Coercion rules locked by these tests (strict: invalid means null):

- strings (``source_event_id``, ``hstree_status``, ``explanation_error_code``):
  only real ``str`` values pass, everything else becomes ``None`` — no silent
  stringification, since ``source_event_id`` is the event identity key;
- ``explanation_status``: only members of ``EXPLANATION_STATUSES`` pass;
- ``hstree_anomaly``: only real booleans pass (``bool("false") is True``
  makes any coercion dangerous);
- ``hstree_threshold``: numeric values are floated, booleans and non-numeric
  values become ``None``.

The runtime identity gate (worker) is covered separately in
``test_explainability_v1_runtime_statuses.py``.
"""

from __future__ import annotations

import hashlib
import json
import re
from unittest import TestCase

from fastavro.validation import validate

from dasma_kafka.explainability.config import (
    EXPLAINABILITY_VALUE_SCHEMA_STR,
    EXPLANATION_PROFILE,
    EXPLANATION_STATUSES,
    HSTREE_MODEL_VERSION,
)
from dasma_kafka.explainability.features import (
    build_output_record,
    compute_feature_fingerprint,
    compute_hstree_model_fingerprint,
)
from dasma_kafka.explainability_v2.features import (
    compute_feature_fingerprint as v2_compute_feature_fingerprint,
    compute_hstree_model_fingerprint as v2_compute_hstree_model_fingerprint,
)


SHA256_HEX_RE = re.compile(r"^[0-9a-f]{64}$")


def _parse_schema() -> dict:
    return json.loads(EXPLAINABILITY_VALUE_SCHEMA_STR)


def _field(schema: dict, name: str) -> dict:
    matches = [field for field in schema["fields"] if field["name"] == name]
    assert len(matches) == 1, f"field {name} must be declared exactly once"
    return matches[0]


class ExplainabilityV1SchemaContractTest(TestCase):
    def setUp(self) -> None:
        self.schema = _parse_schema()

    def test_record_name_and_namespace_are_unchanged(self) -> None:
        self.assertEqual(self.schema["name"], "ExplainabilityEvent")
        self.assertEqual(self.schema["namespace"], "dasma.kafka")

    def test_historical_fields_are_preserved_nullable_with_null_default(self) -> None:
        for name, avro_type in (
            ("timestamp", "string"),
            ("Group", "string"),
            ("hstree_score", "double"),
            ("features", {"type": "map", "values": "double"}),
            ("shap_values", {"type": "map", "values": "double"}),
            ("shap_base_value", "double"),
            ("shap_computation_time_ms", "double"),
            ("local_hstree_score", "double"),
        ):
            field = _field(self.schema, name)
            self.assertEqual(field["type"], ["null", avro_type], name)
            self.assertIsNone(field["default"], name)

    def test_new_fields_are_nullable_with_null_default(self) -> None:
        for name, avro_type in (
            ("source_event_id", "string"),
            ("hstree_status", "string"),
            ("hstree_anomaly", "boolean"),
            ("hstree_threshold", "double"),
            ("explanation_status", "string"),
            ("explanation_error_code", "string"),
            ("explanation_profile", "string"),
            ("shap_nsamples", "int"),
            ("shap_background_size", "int"),
            ("explainability_seed", "long"),
            ("hstree_model_version", "string"),
            ("hstree_model_fingerprint", "string"),
            ("feature_fingerprint", "string"),
        ):
            field = _field(self.schema, name)
            self.assertEqual(field["type"], ["null", avro_type], name)
            self.assertIsNone(field["default"], name)

    def test_legacy_message_without_new_fields_remains_valid(self) -> None:
        legacy = {
            "timestamp": "2026-01-01 00:00:00",
            "Group": "G1",
            "hstree_score": -1.5,
            "features": {"a": 1.0},
            "shap_values": None,
            "shap_base_value": None,
            "shap_computation_time_ms": None,
            "local_hstree_score": -1.4,
        }
        self.assertTrue(validate(legacy, self.schema))

    def test_full_record_with_every_status_field_is_valid(self) -> None:
        record = {
            "timestamp": "2026-01-01 00:00:00",
            "Group": "G1",
            "hstree_score": -1.5,
            "features": {"a": 1.0},
            "shap_values": {"a": 0.1},
            "shap_base_value": 0.01,
            "shap_computation_time_ms": 3.2,
            "local_hstree_score": -1.4,
            "source_event_id": "row-000001",
            "hstree_status": "anomaly",
            "hstree_anomaly": True,
            "hstree_threshold": 0.7,
            "explanation_status": "computed",
            "explanation_error_code": None,
            "explanation_profile": "exhaustive",
            "shap_nsamples": 100,
            "shap_background_size": 100,
            "explainability_seed": 42,
            "hstree_model_version": "local-hstree-v3",
            "hstree_model_fingerprint": "a" * 64,
            "feature_fingerprint": "b" * 64,
        }
        self.assertTrue(validate(record, self.schema))

    def test_failed_message_with_null_shap_values_is_valid(self) -> None:
        record = {
            "timestamp": "2026-01-01 00:00:00",
            "Group": "G1",
            "hstree_score": -1.5,
            "features": {"a": 1.0},
            "shap_values": None,
            "shap_base_value": None,
            "shap_computation_time_ms": None,
            "local_hstree_score": -1.4,
            "source_event_id": "row-000001",
            "explanation_status": "failed",
            "explanation_error_code": "shap_exception",
        }
        self.assertTrue(validate(record, self.schema))


class BuildOutputRecordV1ContractTest(TestCase):
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

    def test_builder_propagates_identity_verdict_status_and_metadata(self) -> None:
        out = self._build(
            explanation_status="computed",
            explanation_error_code=None,
            explanation_profile=EXPLANATION_PROFILE,
            shap_nsamples=100,
            shap_background_size=100,
            explainability_seed=42,
            hstree_model_version=HSTREE_MODEL_VERSION,
            hstree_model_fingerprint="a" * 64,
            feature_fingerprint="b" * 64,
        )
        self.assertEqual(out["source_event_id"], "row-000007")
        self.assertEqual(out["hstree_status"], "anomaly")
        self.assertIs(out["hstree_anomaly"], True)
        self.assertEqual(out["hstree_threshold"], 0.75)
        self.assertEqual(out["explanation_status"], "computed")
        self.assertIsNone(out["explanation_error_code"])
        self.assertEqual(out["explanation_profile"], "exhaustive")
        self.assertEqual(out["shap_nsamples"], 100)
        self.assertEqual(out["shap_background_size"], 100)
        self.assertEqual(out["explainability_seed"], 42)
        self.assertEqual(out["hstree_model_version"], "local-hstree-v3")
        self.assertEqual(out["hstree_model_fingerprint"], "a" * 64)
        self.assertEqual(out["feature_fingerprint"], "b" * 64)
        # The historical fields are untouched.
        self.assertEqual(out["shap_values"], {"A": 0.5, "B": -0.25})
        self.assertEqual(out["local_hstree_score"], 1.2)

    def test_builder_accepts_kitnet_and_keeps_generic_local_score(self) -> None:
        out = self._build(explained_model="kitnet")
        self.assertEqual(out["explained_model"], "kitnet")
        self.assertEqual(out["local_score"], 1.2)
        self.assertIsNone(out["local_hstree_score"])

    def test_builder_output_validates_against_the_schema(self) -> None:
        out = self._build(
            explanation_status="computed",
            explanation_profile=EXPLANATION_PROFILE,
            shap_nsamples=100,
            shap_background_size=100,
            explainability_seed=42,
            hstree_model_version=HSTREE_MODEL_VERSION,
            hstree_model_fingerprint="a" * 64,
            feature_fingerprint="b" * 64,
        )
        self.assertTrue(validate(out, _parse_schema()))

    def test_strict_coercions_reject_wrongly_typed_values(self) -> None:
        out = self._build(
            record={
                "timestamp": "2026-01-01 00:00:01",
                "Group": "2B",
                "source_event_id": 42,          # non-string identity
                "hstree_status": 7,             # non-string status
                "hstree_anomaly": "false",      # string, not bool
                "hstree_threshold": True,       # bool is not a threshold
                "A": 1.5,
                "B": 2.5,
            },
            explanation_status="not_a_status",  # unknown vocabulary entry
            explanation_error_code=123,         # non-string error code
        )
        self.assertIsNone(out["source_event_id"])
        self.assertIsNone(out["hstree_status"])
        self.assertIsNone(out["hstree_anomaly"])
        self.assertIsNone(out["hstree_threshold"])
        self.assertIsNone(out["explanation_status"])
        self.assertIsNone(out["explanation_error_code"])

    def test_every_declared_status_passes_the_builder(self) -> None:
        self.assertEqual(
            EXPLANATION_STATUSES,
            {"disabled", "warmup", "background_not_ready", "computed", "failed", "model_mismatch"},
        )
        for status in sorted(EXPLANATION_STATUSES):
            out = self._build(explanation_status=status)
            self.assertEqual(out["explanation_status"], status)

    def test_failed_status_keeps_null_shap_values_and_error_code(self) -> None:
        out = self._build(
            shap_result={
                "shap_values": None,
                "shap_base_value": None,
                "shap_computation_time_ms": 12.0,
            },
            explanation_status="failed",
            explanation_error_code="shap_exception",
        )
        self.assertIsNone(out["shap_values"])
        self.assertIsNone(out["shap_base_value"])
        self.assertEqual(out["shap_computation_time_ms"], 12.0)
        self.assertEqual(out["explanation_status"], "failed")
        self.assertEqual(out["explanation_error_code"], "shap_exception")


class ExplainabilityV1FingerprintParityTest(TestCase):
    """V1 and V2 fingerprints must be identical for identical canonical inputs."""

    def test_feature_fingerprint_matches_v2(self) -> None:
        names = ["AA", "Acetone", "Methanol", "Toluene"]
        self.assertEqual(
            compute_feature_fingerprint(names),
            v2_compute_feature_fingerprint(names),
        )

    def test_model_fingerprint_matches_v2(self) -> None:
        kwargs = dict(
            hstree_model_version=HSTREE_MODEL_VERSION,
            hstree_window_size=2048,
            hstree_num_trees=25,
            hstree_max_depth=15,
            hstree_initial_window_size=512,
            hstree_seed=42,
        )
        self.assertEqual(
            compute_hstree_model_fingerprint(**kwargs),
            v2_compute_hstree_model_fingerprint(**kwargs),
        )
        self.assertEqual(HSTREE_MODEL_VERSION, "local-hstree-v3")
        self.assertEqual(EXPLANATION_PROFILE, "exhaustive")

    def test_feature_order_changes_the_fingerprint(self) -> None:
        self.assertNotEqual(
            compute_feature_fingerprint(["A", "B"]),
            compute_feature_fingerprint(["B", "A"]),
        )

    def test_model_parameter_change_changes_the_fingerprint(self) -> None:
        base = dict(
            hstree_model_version=HSTREE_MODEL_VERSION,
            hstree_window_size=2048,
            hstree_num_trees=25,
            hstree_max_depth=15,
            hstree_initial_window_size=512,
            hstree_seed=42,
        )
        changed = dict(base, hstree_num_trees=30)
        self.assertNotEqual(
            compute_hstree_model_fingerprint(**base),
            compute_hstree_model_fingerprint(**changed),
        )
        changed_seed = dict(base, hstree_seed=43)
        self.assertNotEqual(
            compute_hstree_model_fingerprint(**base),
            compute_hstree_model_fingerprint(**changed_seed),
        )

    def test_fingerprints_are_sha256_hex(self) -> None:
        fp = compute_feature_fingerprint(["A"])
        self.assertRegex(fp, SHA256_HEX_RE)
        self.assertEqual(len(fp), 64)

    def test_independent_canonical_representation(self) -> None:
        # Independently recomputed expectation (not via the helpers) so the
        # canonical JSON representation stays pinned.
        expected = hashlib.sha256(
            json.dumps(["A", "B"], ensure_ascii=False, separators=(",", ":")).encode(
                "utf-8"
            )
        ).hexdigest()
        self.assertEqual(compute_feature_fingerprint(["A", "B"]), expected)
