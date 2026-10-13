"""Audit the HSTree construction state on a mono-partition V1/V2 export.

This is an offline diagnostic: it replays only the initial window of each
group. No Kafka or Docker service is accessed, and no runtime is changed.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import logging
import random
import struct
from collections import defaultdict
from pathlib import Path

import numpy as np

from dasma_kafka.explainability.config import ExplainabilityConfig
from dasma_kafka.explainability.model import HSTreeModelManager
from runtime.detectors.hstree.models.hstree import HSTreeGlobalModel


INITIAL_WINDOW_SIZE = 100
SEED = 42


def numpy_state_digest() -> str:
    name, keys, position, has_gauss, cached_gaussian = np.random.get_state()
    digest = hashlib.sha256()
    digest.update(name.encode("ascii"))
    digest.update(keys.tobytes())
    digest.update(struct.pack("<iid", position, has_gauss, cached_gaussian))
    return digest.hexdigest()


def python_state_digest() -> str:
    return hashlib.sha256(repr(random.getstate()).encode("ascii")).hexdigest()


def array_digest(values: np.ndarray) -> str:
    array = np.asarray(values, dtype="<f8")
    digest = hashlib.sha256()
    digest.update(struct.pack("<II", *array.shape))
    digest.update(array.tobytes(order="C"))
    return digest.hexdigest()


def tree_digest(model) -> tuple[str, str, str, int]:
    """Fingerprint topology, split values and masses in tree preorder."""
    digest = hashlib.sha256()
    topology_digest = hashlib.sha256()
    mass_digest = hashlib.sha256()
    count = 0
    for root in model.roots:
        stack = [root]
        while stack:
            node = stack.pop()
            digest.update(struct.pack(
                "<idddi", int(node.split_att), float(node.split_value),
                float(node.r_mass), float(node.l_mass), int(node.k),
            ))
            topology_digest.update(struct.pack("<ii", int(node.split_att), int(node.k)))
            mass_digest.update(struct.pack("<dd", float(node.r_mass), float(node.l_mass)))
            count += 1
            if node.right is not None:
                stack.append(node.right)
            if node.left is not None:
                stack.append(node.left)
    return digest.hexdigest(), topology_digest.hexdigest(), mass_digest.hexdigest(), count


def load_rows(path: Path, limit: int) -> tuple[list[dict], list[str]]:
    with path.open(encoding="utf-8") as handle:
        rows = [json.loads(line) for line in handle if line.strip()]
    rows.sort(key=lambda row: row["source_event_id"])
    rows = rows[:limit]
    if len(rows) != limit:
        raise ValueError(f"expected {limit} rows, got {len(rows)}")
    for index, row in enumerate(rows, start=1):
        if row["source_event_id"] != f"event-{index:012d}":
            raise ValueError("audit requires sequential source-1500 event IDs")
    feature_names = sorted(rows[0]["features"])
    if any(sorted(row["features"]) != feature_names for row in rows):
        raise ValueError("feature names vary across the export")
    return rows, feature_names


def construction_record(group: str, window: list[tuple[dict, np.ndarray]], model,
                        feature_names: list[str], score: float) -> dict:
    values = np.asarray([vector for _, vector in window], dtype=np.float64)
    mins = values.min(axis=0)
    maxes = values.max(axis=0)
    # Record both the raw window bounds and the actual bounds used by the
    # resulting model.  Local explainability and upstream now share this
    # construction contract, including constant feature bounds.
    tree_hash, topology_hash, mass_hash, node_count = tree_digest(model)
    return {
        "group": group,
        "initialization_event_id": window[-1][0]["source_event_id"],
        "initialization_timestamp": window[-1][0]["timestamp"],
        "warmup_event_ids": [row["source_event_id"] for row, _ in window],
        "warmup_values_sha256": array_digest(values),
        "raw_feature_mins": dict(zip(feature_names, mins.tolist())),
        "raw_feature_maxes": dict(zip(feature_names, maxes.tolist())),
        "model_feature_mins": dict(zip(feature_names, model.feature_mins.tolist())),
        "model_feature_maxes": dict(zip(feature_names, model.feature_maxes.tolist())),
        "tree_sha256": tree_hash,
        "tree_topology_sha256": topology_hash,
        "tree_mass_sha256": mass_hash,
        "tree_node_count": node_count,
        "first_score": score,
    }


def audit(rows: list[dict], feature_names: list[str], *, num_trees: int = 50,
          max_depth: int = 15, initial_window_size: int = INITIAL_WINDOW_SIZE) -> dict:
    vectors = [np.asarray([float(row["features"][name]) for name in feature_names],
                          dtype=np.float64) for row in rows]
    np.random.seed(SEED)
    random.seed(SEED)
    upstream_windows: dict[str, list[tuple[dict, np.ndarray]]] = defaultdict(list)
    upstream = {}
    for row, vector in zip(rows, vectors):
        group = row["Group"]
        window = upstream_windows[group]
        if len(window) >= initial_window_size:
            continue
        window.append((row, vector))
        if len(window) != initial_window_size:
            continue
        before_numpy = numpy_state_digest()
        before_python = python_state_digest()
        model = HSTreeGlobalModel(
            num_features=len(feature_names), window_size=2048,
            num_trees=num_trees, max_depth=max_depth,
            initial_window_size=initial_window_size,
        )
        score = 0.0
        for _, buffered_vector in window:
            score = model.partial_fit_score(buffered_vector)
        record = construction_record(group, window, model._model, feature_names, score)
        record.update({"numpy_rng_before_sha256": before_numpy,
                       "python_rng_before_sha256": before_python,
                       "numpy_rng_after_sha256": numpy_state_digest(),
                       "python_rng_after_sha256": python_state_digest()})
        upstream[group] = record
        del model
        gc.collect()

    local = {}
    logger = logging.getLogger("q2-construction-audit")
    logger.addHandler(logging.NullHandler())
    config = ExplainabilityConfig(
        hstree_window_size=2048, hstree_num_trees=num_trees,
        hstree_max_depth=max_depth,
        hstree_initial_window_size=initial_window_size,
        shap_background_size=100, explainability_seed=SEED,
    )
    for group in upstream:
        np.random.seed(SEED)
        random.seed(SEED)
        window = upstream_windows[group]
        manager = HSTreeModelManager(config, logger)
        before_numpy = numpy_state_digest()
        before_python = python_state_digest()
        score = None
        for _, vector in window:
            score = manager.fit_score(group, vector)
        model = manager.get_model(group)
        record = construction_record(group, window, model, feature_names, score)
        record.update({"numpy_rng_before_sha256": before_numpy,
                       "python_rng_before_sha256": before_python,
                       "numpy_rng_after_sha256": numpy_state_digest(),
                       "python_rng_after_sha256": python_state_digest()})
        local[group] = record
        del manager
        gc.collect()

    comparisons = []
    for group in upstream:
        amont, explanation = upstream[group], local[group]
        comparisons.append({
            "group": group,
            "initialization_event_id": amont["initialization_event_id"],
            "same_warmup_values": amont["warmup_values_sha256"] == explanation["warmup_values_sha256"],
            "same_raw_bounds": (
                amont["raw_feature_mins"] == explanation["raw_feature_mins"]
                and amont["raw_feature_maxes"] == explanation["raw_feature_maxes"]
            ),
            "same_model_bounds": (
                amont["model_feature_mins"] == explanation["model_feature_mins"]
                and amont["model_feature_maxes"] == explanation["model_feature_maxes"]
            ),
            "same_numpy_rng_before": amont["numpy_rng_before_sha256"] == explanation["numpy_rng_before_sha256"],
            "same_python_rng_before": amont["python_rng_before_sha256"] == explanation["python_rng_before_sha256"],
            "same_tree": amont["tree_sha256"] == explanation["tree_sha256"],
            "same_tree_topology": (
                amont["tree_topology_sha256"] == explanation["tree_topology_sha256"]
            ),
            "same_tree_masses": amont["tree_mass_sha256"] == explanation["tree_mass_sha256"],
            "same_first_score": amont["first_score"] == explanation["first_score"],
            "amont_first_score": amont["first_score"],
            "local_first_score": explanation["first_score"],
        })
    return {
        "parameters": {"seed": SEED, "window_size": 2048,
                       "initial_window_size": initial_window_size,
                       "num_trees": num_trees, "max_depth": max_depth,
                       "feature_names": feature_names},
        "source_records": len(rows),
        "group_initialization_order": list(upstream),
        "groups_below_initial_window": {
            group: len(window) for group, window in sorted(upstream_windows.items())
            if len(window) < initial_window_size
        },
        "comparisons": comparisons,
        "upstream": upstream,
        "local": local,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--export-jsonl", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--limit", type=int, default=1500)
    args = parser.parse_args()
    if args.limit <= 0:
        parser.error("--limit must be positive")
    rows, feature_names = load_rows(args.export_jsonl, args.limit)
    result = audit(rows, feature_names)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
                           encoding="utf-8")
    print(json.dumps(result["comparisons"], ensure_ascii=False, indent=2))
    print(f"report: {args.output}")


if __name__ == "__main__":
    main()
