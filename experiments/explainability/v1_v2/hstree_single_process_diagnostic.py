#!/usr/bin/env python3
"""Rejoue HSTree et Explainability sequentiellement sur un export JSONL V1-06.

Usage reserve a la campagne source-1500 a une partition : les identites
source_event_id y correspondent aux offsets Kafka (index zero). Aucun service
Docker/Kafka n’est contacte et aucun fichier du runtime n’est modifie.
"""

import argparse
from contextlib import nullcontext
import hashlib
import json
import logging
import random
import statistics
from pathlib import Path

import numpy as np

from dasma_kafka.common import isolated_rng
from dasma_kafka.explainability.config import ExplainabilityConfig
from dasma_kafka.explainability.model import HSTreeModelManager
from dasma_kafka.explainability.shap_engine import SHAPExplainer, seed_for_message
from runtime.detectors.hstree.models.hstree import HSTreeGlobalModel


def rng_digest():
    state = np.random.get_state()
    return hashlib.sha256(state[1].tobytes()).hexdigest()[:16]


def summary(pairs):
    diffs = [abs(a - b) for a, b in pairs]
    return {
        'count': len(diffs),
        'exact': sum(diff == 0 for diff in diffs),
        'mean_abs': statistics.mean(diffs) if diffs else None,
        'max_abs': max(diffs) if diffs else None,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--export-jsonl', required=True, type=Path)
    parser.add_argument('--output-dir', required=True, type=Path)
    parser.add_argument('--limit', type=int, default=1500)
    parser.add_argument('--with-shap', action='store_true')
    parser.add_argument('--seed-only', action='store_true')
    parser.add_argument('--preserve-rng', action='store_true')
    args = parser.parse_args()
    if args.with_shap and args.seed_only:
        parser.error('--with-shap and --seed-only are mutually exclusive')
    if args.preserve_rng and not (args.with_shap or args.seed_only):
        parser.error('--preserve-rng requires --with-shap or --seed-only')
    if args.limit <= 0:
        parser.error('--limit must be positive')

    rows = sorted((json.loads(line) for line in args.export_jsonl.open(encoding='utf-8')),
                  key=lambda row: row['source_event_id'])[:args.limit]
    if len(rows) != args.limit:
        raise SystemExit(f'expected {args.limit} rows, found {len(rows)}')
    for index, row in enumerate(rows):
        if row['source_event_id'] != f'event-{index + 1:012d}':
            raise SystemExit('this diagnostic requires source-1500 sequential event ids')
    feature_names = sorted(rows[0]['features'])
    if any(sorted(row['features']) != feature_names for row in rows):
        raise SystemExit('feature names vary across rows')
    vectors = [np.asarray([float(row['features'][name]) for name in feature_names],
                          dtype=np.float64) for row in rows]
    config = ExplainabilityConfig(
        hstree_window_size=2048, hstree_num_trees=50, hstree_max_depth=15,
        hstree_initial_window_size=100, shap_background_size=100,
        shap_nsamples=100, explainability_seed=42,
    )
    logger = logging.getLogger('single-process-diagnostic')
    logger.addHandler(logging.NullHandler())

    np.random.seed(42)
    random.seed(42)
    upstream_models = {}
    upstream_init = []
    upstream_scores = []
    for index, (row, vector) in enumerate(zip(rows, vectors)):
        group = row['Group']
        model = upstream_models.setdefault(group, HSTreeGlobalModel(
            num_features=len(feature_names), window_size=2048,
            num_trees=50, max_depth=15, initial_window_size=100,
        ))
        before = model.is_warming_up
        seed_state = rng_digest() if before and model.samples_buffered == 99 else None
        score = model.partial_fit_score(vector)
        if before and not model.is_warming_up:
            upstream_init.append({'index': index, 'id': row['source_event_id'],
                                  'group': group, 'rng_before_init': seed_state})
        upstream_scores.append(score)

    np.random.seed(42)
    random.seed(42)
    local_manager = HSTreeModelManager(config, logger)
    explainer = SHAPExplainer(config, logger)
    explainer.set_feature_names(feature_names)
    local_init = []
    local_scores = []
    shap_attempts = 0
    shap_failures = 0
    for index, (row, vector) in enumerate(zip(rows, vectors)):
        group = row['Group']
        state = local_manager.get_or_create_model(group)
        before = not state.is_initialized
        seed_state = rng_digest() if before and len(state.warmup_buffer) == 99 else None
        score = local_manager.fit_score(group, vector)
        if before and state.is_initialized:
            local_init.append({'index': index, 'id': row['source_event_id'],
                               'group': group, 'rng_before_init': seed_state})
        local_scores.append(score)
        if (args.with_shap or args.seed_only) and score is not None:
            background = local_manager.get_background_data(group)
            model = local_manager.get_model(group)
            if background is not None and model is not None:
                # The dedicated Kafka topic has one partition; its offsets
                # follow the source_event_id sequence from zero.
                message_seed = seed_for_message(42, group, 0, index)
                rng_scope = isolated_rng(message_seed) if args.preserve_rng else nullcontext()
                with rng_scope:
                    if not args.preserve_rng:
                        np.random.seed(message_seed)
                        random.seed(message_seed)
                    shap_attempts += 1
                    if args.with_shap:
                        result = explainer.compute_shap_values(model, vector, background)
                        if result['shap_values'] is None:
                            shap_failures += 1
        if args.with_shap and index % 25 == 0:
            print(f'progress {index + 1}/{len(rows)}, SHAP calls={shap_attempts}', flush=True)

    pairs = [(upstream_scores[i], local_scores[i]) for i in range(len(rows))
             if local_scores[i] is not None]
    baseline = [(upstream_scores[i], float(rows[i]['hstree_score']))
                for i in range(len(rows))]
    published_local = [(local_scores[i], float(rows[i]['local_hstree_score']))
                       for i in range(len(rows)) if local_scores[i] is not None
                       and rows[i]['local_hstree_score'] is not None]
    first_divergence = next(({
        'index': i, 'id': rows[i]['source_event_id'], 'group': rows[i]['Group'],
        'upstream': upstream_scores[i], 'local': local_scores[i],
    } for i in range(len(rows)) if local_scores[i] is not None
        and upstream_scores[i] != local_scores[i]), None)
    by_group = {}
    for group in sorted(set(row['Group'] for row in rows)):
        gpairs = [(upstream_scores[i], local_scores[i]) for i, row in enumerate(rows)
                  if row['Group'] == group and local_scores[i] is not None]
        by_group[group] = summary(gpairs)
    report = {
        'mode': ('with_shap' if args.with_shap else ('seed_only' if args.seed_only else 'without_shap'))
                + ('_preserve_rng' if args.preserve_rng else ''),
        'records': len(rows), 'feature_count': len(feature_names),
        'published_upstream_check': summary(baseline),
        'published_local_check': summary(published_local),
        'upstream_vs_local': summary(pairs),
        'by_group': by_group, 'first_divergence': first_divergence,
        'upstream_initializations': upstream_init,
        'local_initializations': local_init,
        'shap_attempts': shap_attempts, 'shap_failures': shap_failures,
    }
    stem = f'single-process-{report["mode"]}-{args.limit}'
    args.output_dir.mkdir(parents=True, exist_ok=True)
    path = args.output_dir / f'{stem}.json'
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    print(f'report: {path}', flush=True)


if __name__ == '__main__':
    main()
