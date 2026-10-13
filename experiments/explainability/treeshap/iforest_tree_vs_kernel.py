#!/usr/bin/env python3
"""Reproducible staged iForest TreeSHAP / Kernel qualification, without services."""
from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
import hashlib
import importlib.util
import json
import logging
import os
from pathlib import Path
import platform
import subprocess
import sys
import time
from types import SimpleNamespace

import numpy as np
import shap
import sklearn

from dasma_kafka.common import isolated_rng
from dasma_kafka.common.explained_models import IForestModelManager
from dasma_kafka.common.iforest_treeshap import convert_iforest, explain_tree, mean_path_length
from dasma_kafka.explainability.features import IForestFeatureExtractor
from dasma_kafka.explainability.shap_engine import seed_for_message
from dasma_kafka.explainability_v2.stability import compare_to_reference, jaccard_at_k, spearman


@dataclass
class Event:
    identity: str
    group: str
    offset: int
    ordinal: int
    x: np.ndarray
    background: np.ndarray
    forest: object
    generation: int
    refit: bool
    score: float


def load_events(source):
    # Reuse the CSV/Avro name conversion used in the conformance study, not its models.
    path = Path(__file__).parents[2] / "iforest" / "iforest-paper-conformance-study.py"
    spec = importlib.util.spec_from_file_location("iforest_conformance_loader", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    records = module.load_records(Path(source))
    config = SimpleNamespace(iforest_max_samples=256, iforest_initial_window_size=512,
                             iforest_window_size=2048, iforest_n_estimators=100, iforest_refit_every=128,
                             hstree_seed=42, shap_background_size=100)
    manager = IForestModelManager(config, logging.getLogger("benchmark"))
    extractor = IForestFeatureExtractor(logging.getLogger("benchmark"))
    extractor.initialize_from_record(records[0])
    counters = defaultdict(int)
    events = []
    for offset, record in enumerate(records):
        group = str(record["Group"])
        counters[group] += 1
        x = extractor.extract(record)
        score = manager.fit_score(group, x)
        if score is None:
            continue
        generation = manager.get_scoring_generation(group)
        events.append(Event(record["source_event_id"], group, offset, counters[group], x,
                            manager.get_background_data(group).copy(), manager.get_scoring_model(group),
                            generation, manager.models[group].generation != generation, score))
    return events, extractor.feature_names, dict(counters)


def select_events(events, profile, sample_size=300):
    groups = defaultdict(list)
    for event in events:
        groups[event.group].append(event)
    mandatory = {}
    for group in sorted(groups):
        rows = groups[group]
        boundary = next((i for i, event in enumerate(rows) if event.refit), None)
        if boundary is not None:
            for i in (boundary-1, boundary, boundary+1):
                if 0 <= i < len(rows):
                    mandatory[rows[i].identity] = rows[i]
        highest = max(rows, key=lambda e: (e.score, -e.offset))
        mandatory[highest.identity] = highest
    if profile == "short":
        return sorted(mandatory.values(), key=lambda e: e.offset)[:20]
    for event in sorted(events, key=lambda e: (-e.score, e.offset))[:max(1, int(np.ceil(len(events)*.01)))]:
        mandatory[event.identity] = event
    # Round robin over group x score decile, stable offset tie breaking.
    buckets = []
    for group in sorted(groups):
        ranked = sorted(groups[group], key=lambda e: (e.score, e.offset))
        for bucket in np.array_split(np.arange(len(ranked)), 10):
            buckets.append([ranked[i] for i in bucket if ranked[i].identity not in mandatory])
    while len(mandatory) < min(sample_size, len(events)) and any(buckets):
        for bucket in buckets:
            if bucket and len(mandatory) < sample_size:
                event = bucket.pop(len(bucket)//2)
                mandatory[event.identity] = event
    return sorted(mandatory.values(), key=lambda e: e.offset)


def summarize(rows, conversions):
    result = {}
    for method in sorted({r["method"] for r in rows}):
        subset = [r for r in rows if r["method"] == method]
        metrics = {}
        for key in sorted({key for r in subset for key in r["metrics"]}):
            values = [r["metrics"][key] for r in subset if r["metrics"].get(key) is not None]
            metrics[key] = dict(mean=float(np.mean(values)), median=float(np.median(values)),
                                std=float(np.std(values))) if values else None
        times = [r["elapsed_ms"] for r in subset]
        result[method] = dict(count=len(subset), median_ms=float(np.median(times)),
                              p95_ms=float(np.percentile(times, 95)), metrics=metrics)
    return dict(methods=result, conversion_count=len(conversions),
                conversion_total_ms=sum(conversions), conversion_median_ms=float(np.median(conversions)),
                tree_conversion_amortized_ms=sum(conversions)/max(1, sum(r['method']=='TREE-int' for r in rows)))


def run(args):
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    metadata = dict(status="running", profile=args.profile, source_sha256=hashlib.sha256(Path(args.source).read_bytes()).hexdigest(),
                    versions=dict(python=platform.python_version(), numpy=np.__version__, sklearn=sklearn.__version__, shap=shap.__version__),
                    git_revision=os.environ.get('AUDIT_GIT_REVISION') or subprocess.check_output(['git','rev-parse','HEAD'], text=True).strip(),
                    worktree=os.environ['AUDIT_WORKTREE'].splitlines() if 'AUDIT_WORKTREE' in os.environ else subprocess.check_output(['git','status','--short'], text=True).splitlines(),
                    config=dict(trees=100, psi=256, initial=512, window=2048, refit=128, background=100, seed=42),
                    offset_convention="zero-based CSV row, synthetic partition=0; not historic Kafka metadata")
    metadata['runner_sha256'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    metadata['thread_environment'] = {k: os.environ.get(k) for k in ['OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS']}
    def save():
        (output / "summary.json").write_text(json.dumps(metadata, indent=2, sort_keys=True, allow_nan=False)+"\n")
    save()
    rows, conversions = [], []
    try:
        events, names, counts = load_events(args.source)
        selected = select_events(events, args.profile, args.sample_size)
        metadata.update(features=names, group_counts=counts, eligible_events=len(events), selected=[
            dict(identity=e.identity, group=e.group, ordinal=e.ordinal, generation=e.generation, refit=e.refit)
            for e in selected])
        # Separate warm-up, never included in published timings.
        c = convert_iforest(selected[0].forest)
        explain_tree(c, selected[0].x, selected[0].background)
        with isolated_rng(42):
            shap.KernelExplainer(lambda X: -selected[0].forest.score_samples(X), selected[0].background).shap_values(
                selected[0].x.reshape(1,-1), nsamples=100, silent=True)
        cache = {}
        dense_groups = set()
        with (output / "events.jsonl").open("w") as stream:
            for index, event in enumerate(selected):
                key = (event.group, event.generation)
                if key not in cache:
                    start = time.perf_counter()
                    cache[key] = convert_iforest(event.forest)
                    conversions.append((time.perf_counter()-start)*1000)
                converted = cache[key]
                independent = float(mean_path_length(event.forest, event.x)[0])
                if not np.isclose(converted.predict(event.x)[0], independent, atol=1e-12, rtol=1e-12):
                    raise ValueError("conversion fidelity failure")
                start = time.perf_counter()
                components = {}
                reference, base, value = explain_tree(converted, event.x, event.background, timings=components)
                elapsed = (time.perf_counter()-start)*1000
                cases = [("TREE-int", reference, elapsed, base, None)]
                start = time.perf_counter()
                pd = shap.TreeExplainer(converted.model, feature_perturbation="tree_path_dependent", model_output="raw")
                phi = pd.shap_values(converted.encoder.encode(event.x), approximate=False)[0]
                cases.append(("TREE-pd", phi, (time.perf_counter()-start)*1000, float(np.asarray(pd.expected_value).item()), None))
                production_seed = seed_for_message(42, event.group, 0, event.offset)
                start = time.perf_counter()
                with isolated_rng(production_seed):
                    kernel = shap.KernelExplainer(lambda X: -event.forest.score_samples(X), event.background)
                    phi = kernel.shap_values(event.x.reshape(1,-1), nsamples=100, silent=True)[0]
                cases.append(("K-prod", phi, (time.perf_counter()-start)*1000, float(kernel.expected_value), production_seed))
                dense = args.profile == "full" or event.group not in dense_groups
                if dense:
                    dense_groups.add(event.group)
                    for budget in (100,1000,2000):
                        for seed in (42,43,44):
                            start = time.perf_counter()
                            with isolated_rng(seed):
                                kernel = shap.KernelExplainer(lambda X: mean_path_length(event.forest, X), event.background)
                                phi = kernel.shap_values(event.x.reshape(1,-1), nsamples=budget, l1_reg=0, silent=True)[0]
                            cases.append((f"K-Eh-{budget}", phi, (time.perf_counter()-start)*1000, float(kernel.expected_value), seed))
                for method, phi, elapsed, base_value, seed in cases:
                    metrics = (dict(spearman=spearman(-phi,reference), jaccard_at_10=jaccard_at_k(-phi,reference,10))
                               if method == "K-prod" else compare_to_reference(phi, reference))
                    if method != "K-prod":
                        metrics["rmse"] = float(np.sqrt(np.mean((phi-reference)**2)))
                    metrics = {k: float(v) if np.isfinite(v) else None for k,v in metrics.items()}
                    row = dict(identity=event.identity, group=event.group, ordinal=event.ordinal, generation=event.generation,
                               refit=event.refit, method=method, seed=seed, score=event.score, explained_value=independent,
                               base=base_value, phi=phi.tolist(), elapsed_ms=elapsed, metrics=metrics,
                               additivity_error=float(abs(base_value+phi.sum()-(event.score if method=='K-prod' else value))))
                    if method == 'TREE-int':
                        row['time_components_ms'] = components
                    stream.write(json.dumps(row, sort_keys=True, allow_nan=False)+"\n")
                    stream.flush()
                    rows.append(row)
                print(f"{args.profile}: {index+1}/{len(selected)} {event.group} generation={event.generation}", flush=True)
        metadata.update(status="complete", **summarize(rows, conversions))
        metadata['tree_additivity_max'] = max(r['additivity_error'] for r in rows if r['method']=='TREE-int')
        tree = metadata['methods']['TREE-int']['median_ms'] + metadata['tree_conversion_amortized_ms']
        metadata['cost_gate'] = tree <= metadata['methods']['K-prod']['median_ms']
        save()
    except BaseException as error:
        metadata.update(status="incomplete", error=f"{type(error).__name__}: {error}")
        save()
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', default='data/source-5000.csv')
    parser.add_argument('--profile', choices=['short','full'], required=True)
    parser.add_argument('--sample-size', type=int, default=300)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    if args.sample_size < 1:
        parser.error('--sample-size must be positive')
    run(args)


if __name__ == '__main__':
    main()
