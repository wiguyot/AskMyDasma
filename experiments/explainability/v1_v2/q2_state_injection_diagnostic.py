"""Q2.2: replay local HSTree scores with the upstream construction RNG state.

The diagnostic reads an existing V1 JSONL export and never contacts services.
It changes neither the detector nor the Explainability runtime.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import logging
import random
from collections import defaultdict
from pathlib import Path

import numpy as np

from dasma_kafka.explainability.config import ExplainabilityConfig
from dasma_kafka.explainability.model import HSTreeModelManager
from runtime.detectors.hstree.models.hstree import HSTreeGlobalModel

from .q2_model_construction_audit import load_rows


class RawBoundsModelManager(HSTreeModelManager):
    """Diagnostic-only variant using the detector's unexpanded bounds."""

    def _create_model(self, n_features, feature_mins, feature_maxes,
                      initial_window_x=None):
        warmup = np.asarray(initial_window_x, dtype=np.float64)
        return super()._create_model(
            n_features=n_features,
            feature_mins=warmup.min(axis=0),
            feature_maxes=warmup.max(axis=0),
            initial_window_x=initial_window_x,
        )


def config_for_replay(*, num_trees=50, max_depth=15, initial_window_size=100):
    return ExplainabilityConfig(
        hstree_window_size=2048, hstree_num_trees=num_trees,
        hstree_max_depth=max_depth,
        hstree_initial_window_size=initial_window_size,
        shap_background_size=100, explainability_seed=42,
    )


def vector_for(row, feature_names):
    return np.asarray([float(row["features"][name]) for name in feature_names],
                      dtype=np.float64)


def capture_upstream_states(rows, feature_names, config):
    """Capture NumPy state at each group's 100th event in source order.

    HSTree construction consumes NumPy randomness. Subsequent fit/score calls
    do not, so models can be discarded once their construction state and first
    score have been recorded.
    """
    np.random.seed(config.explainability_seed)
    random.seed(config.explainability_seed)
    windows = defaultdict(list)
    states = {}
    first_scores = {}
    for row in rows:
        group = row["Group"]
        window = windows[group]
        if len(window) >= config.hstree_initial_window_size:
            continue
        window.append(row)
        if len(window) != config.hstree_initial_window_size:
            continue
        states[group] = np.random.get_state()
        model = HSTreeGlobalModel(
            num_features=len(feature_names),
            window_size=config.hstree_window_size,
            num_trees=config.hstree_num_trees,
            max_depth=config.hstree_max_depth,
            initial_window_size=config.hstree_initial_window_size,
        )
        for buffered_row in window:
            score = model.partial_fit_score(vector_for(buffered_row, feature_names))
        first_scores[group] = score
        if score != row["hstree_score"]:
            raise ValueError(
                f"upstream replay differs from published first score for {group}"
            )
        del model
        gc.collect()
    return states, first_scores, {
        group: len(window) for group, window in sorted(windows.items())
        if len(window) < config.hstree_initial_window_size
    }


def summarize_scores(pairs):
    gaps = [abs(upstream - local) for _, upstream, local in pairs]
    divergent = [
        {"source_event_id": row["source_event_id"], "Group": row["Group"],
         "upstream": upstream, "local": local,
         "absolute_gap": abs(upstream - local)}
        for row, upstream, local in pairs if upstream != local
    ]
    return {
        "compared": len(pairs),
        "exact": len(pairs) - len(divergent),
        "divergent": len(divergent),
        "max_absolute_gap": max(gaps, default=None),
        "first_divergence": divergent[0] if divergent else None,
    }


def replay_local(rows, feature_names, config, upstream_states, *, inject_state,
                 raw_bounds):
    grouped_rows = defaultdict(list)
    for row in rows:
        grouped_rows[row["Group"]].append(row)
    logger = logging.getLogger("q2-state-injection")
    logger.addHandler(logging.NullHandler())
    manager_type = RawBoundsModelManager if raw_bounds else HSTreeModelManager
    pairs = []
    published_local_mismatches = []
    by_group = {}
    for group, group_rows in sorted(grouped_rows.items()):
        np.random.seed(config.explainability_seed)
        random.seed(config.explainability_seed)
        manager = manager_type(config, logger)
        group_pairs = []
        for group_index, row in enumerate(group_rows, start=1):
            vector = vector_for(row, feature_names)
            if inject_state and group_index == config.hstree_initial_window_size:
                if group not in upstream_states:
                    raise ValueError(f"missing upstream state for {group}")
                local_rng_state = np.random.get_state()
                try:
                    np.random.set_state(upstream_states[group])
                    score = manager.fit_score(group, vector)
                finally:
                    np.random.set_state(local_rng_state)
            else:
                score = manager.fit_score(group, vector)
            if score is None:
                continue
            upstream_score = row["hstree_score"]
            if type(upstream_score) not in (int, float):
                raise ValueError(f"missing upstream score for {row['source_event_id']}")
            pair = (row, float(upstream_score), score)
            pairs.append(pair)
            group_pairs.append(pair)
            if not inject_state and not raw_bounds and score != row["local_hstree_score"]:
                published_local_mismatches.append(row["source_event_id"])
        by_group[group] = summarize_scores(group_pairs)
        del manager
        gc.collect()
    pairs.sort(key=lambda pair: pair[0]["source_event_id"])
    return {
        **summarize_scores(pairs),
        "by_group": by_group,
        "published_local_mismatches": published_local_mismatches,
    }


def audit(rows, feature_names, *, num_trees=50, max_depth=15,
          initial_window_size=100):
    config = config_for_replay(
        num_trees=num_trees, max_depth=max_depth,
        initial_window_size=initial_window_size,
    )
    states, first_scores, below_window = capture_upstream_states(
        rows, feature_names, config,
    )
    variants = {}
    for name, inject_state, raw_bounds in (
        ("baseline", False, False),
        ("injected_rng_local_bounds", True, False),
        ("injected_rng_upstream_bounds", True, True),
    ):
        variants[name] = replay_local(
            rows, feature_names, config, states,
            inject_state=inject_state, raw_bounds=raw_bounds,
        )
    return {
        "parameters": {
            "seed": config.explainability_seed,
            "initial_window_size": initial_window_size,
            "window_size": config.hstree_window_size,
            "num_trees": num_trees,
            "max_depth": max_depth,
            "feature_names": feature_names,
        },
        "source_records": len(rows),
        "group_initialization_order": list(states),
        "groups_below_initial_window": below_window,
        "upstream_first_scores": first_scores,
        "variants": variants,
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
    print(json.dumps({name: {key: value for key, value in variant.items()
                             if key not in ("by_group", "published_local_mismatches")}
                      for name, variant in result["variants"].items()},
                     ensure_ascii=False, indent=2))
    print(f"report: {args.output}")


if __name__ == "__main__":
    main()
