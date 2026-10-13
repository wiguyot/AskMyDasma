"""Offline Q5.3 prototype for per-group HSTree initialization envelopes.

Reads a source-1500 JSONL export. It replays upstream/local models in a single
group at a time, serializes the construction state through JSON, and compares
real scores plus KernelSHAP-style coalition predictions. No service is used.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import importlib.metadata
import json
import logging
import platform
import random
import struct
from collections import defaultdict
from pathlib import Path

import numpy as np

from dasma_kafka.common import isolated_rng
from dasma_kafka.explainability.model import HSTreeModelManager
from dasma_kafka.explainability.shap_engine import SHAPExplainer, seed_for_message
from experiments.explainability.v1_v2.q2_state_injection_diagnostic import (
    capture_upstream_states,
    config_for_replay,
    vector_for,
)
from experiments.explainability.v1_v2.q2_model_construction_audit import load_rows
from runtime.detectors.hstree.models.hstree import HSTreeGlobalModel


class EnvelopeModelManager(HSTreeModelManager):
    """Diagnostic-only manager that rebuilds from the upstream envelope."""

    def __init__(self, config, logger, envelopes):
        super().__init__(config, logger)
        self.envelopes = envelopes

    def _create_model(self, n_features, feature_mins, feature_maxes,
                      initial_window_x=None):
        # Match the detector exactly, including constant-feature bounds.
        warmup = np.asarray(initial_window_x, dtype=np.float64)
        return super()._create_model(
            n_features=n_features,
            feature_mins=warmup.min(axis=0),
            feature_maxes=warmup.max(axis=0),
            initial_window_x=initial_window_x,
        )

    def fit_score(self, group, features):
        group_model = self.get_or_create_model(group)
        at_initialization = (
            not group_model.is_initialized
            and len(group_model.warmup_buffer) + 1 == group_model.initial_window_size
        )
        if not at_initialization:
            return super().fit_score(group, features)
        old_state = np.random.get_state()
        try:
            np.random.set_state(state_from_envelope(self.envelopes[group]))
            return super().fit_score(group, features)
        finally:
            np.random.set_state(old_state)


def numpy_state_payload(state):
    return {
        "algorithm": state[0],
        "keys": [int(value) for value in state[1]],
        "position": int(state[2]),
        "has_gaussian": int(state[3]),
        "cached_gaussian": float(state[4]),
    }


def state_from_envelope(envelope):
    state = envelope["numpy_state"]
    return (
        state["algorithm"],
        np.asarray(state["keys"], dtype=np.uint32),
        state["position"],
        state["has_gaussian"],
        state["cached_gaussian"],
    )


def canonical_json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def vector_digest(vectors):
    array = np.asarray(vectors, dtype="<f8")
    digest = hashlib.sha256(struct.pack("<II", *array.shape) + array.tobytes())
    return digest.hexdigest()


def array_digest(array):
    values = np.asarray(array, dtype="<f8")
    digest = hashlib.sha256(struct.pack("<I", values.ndim))
    digest.update(struct.pack("<" + "I" * values.ndim, *values.shape))
    digest.update(values.tobytes(order="C"))
    return digest.hexdigest()


def build_envelopes(rows, feature_names, config):
    raw_states, first_scores, below_window = capture_upstream_states(
        rows, feature_names, config,
    )
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["Group"]].append(row)
    envelopes = {}
    for group, state in raw_states.items():
        warmup = grouped[group][:config.hstree_initial_window_size]
        vectors = [vector_for(row, feature_names) for row in warmup]
        matrix = np.asarray(vectors, dtype=np.float64)
        envelope = {
            "format": "dasma-hstree-initialization-envelope-v1",
            "group": group,
            "generation_id": warmup[-1]["source_event_id"],
            "initialization_event_id": warmup[-1]["source_event_id"],
            "warmup_event_ids": [row["source_event_id"] for row in warmup],
            "warmup_values_sha256": vector_digest(vectors),
            "feature_names": feature_names,
            "feature_mins": dict(zip(feature_names, matrix.min(axis=0).tolist())),
            "feature_maxes": dict(zip(feature_names, matrix.max(axis=0).tolist())),
            "parameters": {
                "window_size": config.hstree_window_size,
                "num_trees": config.hstree_num_trees,
                "max_depth": config.hstree_max_depth,
                "initial_window_size": config.hstree_initial_window_size,
            },
            "versions": {
                "python": platform.python_version(),
                "numpy": np.__version__,
                "pysad": importlib.metadata.version("pysad"),
            },
            "numpy_state": numpy_state_payload(state),
        }
        wire = canonical_json(envelope)
        restored = json.loads(wire)
        if canonical_json(restored) != wire:
            raise ValueError(f"envelope JSON round-trip changed for {group}")
        restored["envelope_sha256"] = hashlib.sha256(wire.encode("utf-8")).hexdigest()
        restored["wire_bytes"] = len(wire.encode("utf-8"))
        envelopes[group] = restored
    return envelopes, first_scores, below_window


def coalition_inputs(background, foreground):
    """Construct deterministic empty/full/singleton/complement masks.

    Three real rows from the final SHAP background are used to keep the audit
    bounded while probing each feature at least once in each mask direction.
    """
    background = np.asarray(background, dtype=np.float64)
    foreground = np.asarray(foreground, dtype=np.float64)
    feature_count = foreground.size
    positions = sorted({0, len(background) // 2, len(background) - 1})
    masks = [np.zeros(feature_count, dtype=bool), np.ones(feature_count, dtype=bool)]
    masks.extend(np.eye(feature_count, dtype=bool))
    masks.extend(~np.eye(feature_count, dtype=bool))
    rows = []
    for position in positions:
        base = background[position]
        rows.extend(np.where(mask, foreground, base) for mask in masks)
    return np.asarray(rows, dtype=np.float64), positions


def summarize(pairs):
    gaps = [abs(reference - candidate) for reference, candidate in pairs]
    divergent = [pair for pair in pairs if pair[0] != pair[1]]
    return {
        "compared": len(pairs),
        "exact": len(pairs) - len(divergent),
        "divergent": len(divergent),
        "max_absolute_gap": max(gaps, default=None),
    }


def evaluate_group(rows, feature_names, config, envelope, *, mode):
    logger = logging.getLogger("q5-envelope-prototype")
    logger.addHandler(logging.NullHandler())
    vectors = [vector_for(row, feature_names) for row in rows]
    if mode == "upstream":
        state = state_from_envelope(envelope)
        old_state = np.random.get_state()
        try:
            np.random.set_state(state)
            manager = HSTreeGlobalModel(
                num_features=len(feature_names),
                window_size=config.hstree_window_size,
                num_trees=config.hstree_num_trees,
                max_depth=config.hstree_max_depth,
                initial_window_size=config.hstree_initial_window_size,
            )
            scores = [manager.partial_fit_score(vector) for vector in vectors]
            model = manager._model
            if model is None:
                raise ValueError(f"upstream model not initialized for {envelope['group']}")
            background = np.asarray(vectors[-config.shap_background_size:], dtype=np.float64)
            coalition_matrix, positions = coalition_inputs(background, vectors[-1])
            explainer = SHAPExplainer(config, logger)
            explainer.set_feature_names(feature_names)
            predict = explainer._create_predict_function(model)
            coalitions = predict(coalition_matrix)
            source_offset = int(rows[-1]["source_event_id"].rsplit("-", 1)[1]) - 1
            message_seed = seed_for_message(
                config.explainability_seed, envelope["group"], 0, source_offset,
            )
            with isolated_rng(message_seed):
                shap_result = explainer.compute_shap_values(
                    model, vectors[-1], background,
                )
            del predict, explainer, model, manager
        finally:
            np.random.set_state(old_state)
    elif mode == "envelope":
        np.random.seed(config.explainability_seed)
        random.seed(config.explainability_seed)
        manager = EnvelopeModelManager(config, logger, {envelope["group"]: envelope})
        scores = []
        for vector in vectors:
            scores.append(manager.fit_score(envelope["group"], vector))
        model = manager.get_model(envelope["group"])
        if model is None:
            raise ValueError(f"local model not initialized for {envelope['group']}")
        background = manager.get_background_data(envelope["group"])
        if not np.array_equal(background, np.asarray(vectors[-config.shap_background_size:])):
            raise ValueError(f"local SHAP background differs for {envelope['group']}")
        coalition_matrix, positions = coalition_inputs(background, vectors[-1])
        explainer = SHAPExplainer(config, logger)
        explainer.set_feature_names(feature_names)
        predict = explainer._create_predict_function(model)
        coalitions = predict(coalition_matrix)
        source_offset = int(rows[-1]["source_event_id"].rsplit("-", 1)[1]) - 1
        message_seed = seed_for_message(
            config.explainability_seed, envelope["group"], 0, source_offset,
        )
        with isolated_rng(message_seed):
            shap_result = explainer.compute_shap_values(
                model, vectors[-1], background,
            )
        del predict, explainer, model, manager
    else:
        raise ValueError(f"unknown mode {mode!r}")
    if shap_result["shap_values"] is None:
        raise ValueError(
            f"SHAP failed for {envelope['group']}: "
            f"{shap_result.get('explanation_error_code')}"
        )
    shap_output = np.asarray(
        [shap_result["shap_base_value"]]
        + [shap_result["shap_values"][name] for name in feature_names],
        dtype="<f8",
    )
    gc.collect()
    return (
        np.asarray(scores[config.hstree_initial_window_size - 1:], dtype="<f8"),
        np.asarray(coalitions, dtype="<f8"), positions, shap_output,
    )


def run(rows, feature_names, envelopes, config):
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["Group"]].append(row)
    result = {}
    repeat_score_pairs = []
    repeat_coalition_pairs = []
    repeat_shap_pairs = []
    for group, envelope in envelopes.items():
        group_rows = grouped[group]
        upstream_scores, upstream_coalitions, positions, upstream_shap = evaluate_group(
            group_rows, feature_names, config, envelope, mode="upstream",
        )
        local_scores, local_coalitions, local_positions, local_shap = evaluate_group(
            group_rows, feature_names, config, envelope, mode="envelope",
        )
        repeated_scores, repeated_coalitions, repeat_positions, repeated_shap = evaluate_group(
            group_rows, feature_names, config, envelope, mode="envelope",
        )
        published_scores = np.asarray(
            [row["hstree_score"] for row in group_rows[config.hstree_initial_window_size - 1:]],
            dtype="<f8",
        )
        if not np.array_equal(upstream_scores, published_scores):
            bad = int(np.flatnonzero(upstream_scores != published_scores)[0])
            raise ValueError(
                f"upstream replay differs from published score for {group} at "
                f"{group_rows[config.hstree_initial_window_size - 1 + bad]['source_event_id']}"
            )
        if positions != local_positions or positions != repeat_positions:
            raise ValueError(f"coalition background positions differ for {group}")
        if upstream_coalitions.shape != local_coalitions.shape:
            raise ValueError(f"coalition output shape differs for {group}")
        score_pairs = list(zip(upstream_scores.tolist(), local_scores.tolist()))
        coalition_pairs = list(zip(upstream_coalitions.tolist(), local_coalitions.tolist()))
        reproducible_score_pairs = list(zip(local_scores.tolist(), repeated_scores.tolist()))
        reproducible_coalition_pairs = list(zip(local_coalitions.tolist(), repeated_coalitions.tolist()))
        shap_pairs = list(zip(upstream_shap.tolist(), local_shap.tolist()))
        reproducible_shap_pairs = list(zip(local_shap.tolist(), repeated_shap.tolist()))
        repeat_score_pairs.extend(reproducible_score_pairs)
        repeat_coalition_pairs.extend(reproducible_coalition_pairs)
        repeat_shap_pairs.extend(reproducible_shap_pairs)
        result[group] = {
            "generation_id": envelope["generation_id"],
            "envelope_sha256": envelope["envelope_sha256"],
            "envelope_wire_bytes": envelope["wire_bytes"],
            "score_comparison": summarize(score_pairs),
            "score_matches_published_upstream": bool(np.array_equal(upstream_scores, published_scores)),
            "coalition_comparison": summarize(coalition_pairs),
            "coalition_rows": int(upstream_coalitions.size),
            "coalition_background_positions": positions,
            "shap_values_and_base_comparison": summarize(shap_pairs),
            "shap_contributions_compared": len(feature_names),
            "repeated_score_comparison": summarize(reproducible_score_pairs),
            "repeated_coalition_comparison": summarize(reproducible_coalition_pairs),
            "repeated_shap_comparison": summarize(reproducible_shap_pairs),
            "score_sha256": array_digest(local_scores),
            "coalition_sha256": array_digest(local_coalitions),
            "shap_sha256": array_digest(local_shap),
        }
    return {
        "groups": result,
        "scores_compared": sum(item["score_comparison"]["compared"] for item in result.values()),
        "scores_exact": sum(item["score_comparison"]["exact"] for item in result.values()),
        "coalition_rows_compared": sum(item["coalition_rows"] for item in result.values()),
        "coalition_rows_exact": sum(item["coalition_comparison"]["exact"] for item in result.values()),
        "repeat_scores_compared": len(repeat_score_pairs),
        "repeat_scores_exact": sum(a == b for a, b in repeat_score_pairs),
        "repeat_coalition_predictions_compared": len(repeat_coalition_pairs),
        "repeat_coalition_predictions_exact": sum(a == b for a, b in repeat_coalition_pairs),
        "shap_values_and_bases_compared": sum(item["shap_values_and_base_comparison"]["compared"] for item in result.values()),
        "shap_values_and_bases_exact": sum(item["shap_values_and_base_comparison"]["exact"] for item in result.values()),
        "repeat_shap_values_and_bases_compared": len(repeat_shap_pairs),
        "repeat_shap_values_and_bases_exact": sum(a == b for a, b in repeat_shap_pairs),
    }


def audit(rows, feature_names):
    config = config_for_replay()
    envelopes, first_scores, below_window = build_envelopes(rows, feature_names, config)
    run_result = run(rows, feature_names, envelopes, config)
    return {
        "source_records": len(rows),
        "parameters": {
            "seed": config.explainability_seed,
            "window_size": config.hstree_window_size,
            "num_trees": config.hstree_num_trees,
            "max_depth": config.hstree_max_depth,
            "initial_window_size": config.hstree_initial_window_size,
            "feature_names": feature_names,
            "shap_background_size": config.shap_background_size,
        },
        "initialization_order": list(envelopes),
        "groups_below_initial_window": below_window,
        "envelope_capture_first_scores": first_scores,
        "envelope_payload_bytes_total": sum(e["wire_bytes"] for e in envelopes.values()),
        "envelope_payload_bytes_max": max((e["wire_bytes"] for e in envelopes.values()), default=0),
        "result": run_result,
        "envelopes": envelopes,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--export-jsonl", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--limit", type=int, default=1500)
    args = parser.parse_args()
    if args.limit <= 0:
        parser.error("--limit must be positive")
    rows, feature_names = load_rows(args.export_jsonl, args.limit)
    result = audit(rows, feature_names)
    result["export_sha256"] = hashlib.sha256(args.export_jsonl.read_bytes()).hexdigest()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, sort_keys=True, ensure_ascii=False, indent=2) + "\n",
                           encoding="utf-8")
    print(json.dumps({key: value for key, value in result.items()
                      if key not in ("envelopes", "parameters")},
                     sort_keys=True, ensure_ascii=False, indent=2))
    print(f"report: {args.output}")


if __name__ == "__main__":
    main()
