#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
KernelSHAP benchmark for the HSTree streaming context
=====================================================

File status
-----------
This script is a study and benchmarking tool.
It is not part of the application pipeline launched by `docker compose up`.

It is used to:
- measure SHAP cost
- compare settings
- document performance limits

It must not be confused with the active service:
- `explainability-multi-thread-by-group.py`

This script evaluates:
1. SHAP computation time vs number of features
2. Impact of background size
3. Impact of the nsamples parameter
4. Memory overhead
5. Stability of explanations over time
6. Comparison with/without SHAP

Author: Mélissa GHEMARI
Date: December 2025
"""

import os
import sys
import time
import json
import tracemalloc
from typing import Dict, List, Tuple, Any
from dataclasses import dataclass
from collections import deque

import numpy as np
import pandas as pd
import shap
from pysad.models import HalfSpaceTrees

# =============================================================================
# BENCHMARK CONFIGURATION
# =============================================================================

@dataclass
class BenchmarkConfig:
    """Benchmark test configuration."""
    # Number of features to test
    feature_counts: List[int] = None
    # Background sizes to test
    background_sizes: List[int] = None
    # nsamples values to test
    nsamples_values: List[int] = None
    # Number of observations for each test
    n_observations: int = 50
    # Number of repetitions for averaging
    n_repetitions: int = 3
    # HSTree configuration
    hstree_window_size: int = 2048
    hstree_num_trees: int = 25
    hstree_max_depth: int = 15
    
    def __post_init__(self):
        if self.feature_counts is None:
            self.feature_counts = [5, 10, 20, 30, 40, 50, 100, 150]
        if self.background_sizes is None:
            self.background_sizes = [20, 50, 100, 200]
        if self.nsamples_values is None:
            self.nsamples_values = [5, 10, 20, 30, 40, 50, 100, 150]


# =============================================================================
# UTILITIES
# =============================================================================

def generate_synthetic_data(n_samples: int, n_features: int, seed: int = 42) -> np.ndarray:
    """Generate synthetic data for tests."""
    np.random.seed(seed)
    return np.random.rand(n_samples, n_features)


def create_hstree_model(
    n_features: int,
    window_size: int = 2048,
    num_trees: int = 25,
    max_depth: int = 15,
    warmup_data: np.ndarray = None,
) -> HalfSpaceTrees:
    """Create and initialize an HSTree model."""
    feature_mins = np.zeros(n_features)
    feature_maxes = np.ones(n_features)
    
    model = HalfSpaceTrees(
        window_size=window_size,
        num_trees=num_trees,
        max_depth=max_depth,
        feature_mins=feature_mins,
        feature_maxes=feature_maxes,
        initial_window_X=warmup_data.tolist() if warmup_data is not None else None,
    )
    
    return model


def compute_shap_values(
    model: HalfSpaceTrees,
    observation: np.ndarray,
    background: np.ndarray,
    nsamples: int = 100,
) -> Tuple[np.ndarray, float, float]:
    """
    Compute SHAP values and measure execution time.
    
    Returns:
        (shap_values, base_value, computation_time_ms)
    """
    def predict_fn(X):
        return np.array([model.score_partial(row) for row in X])
    
    start_time = time.perf_counter()
    
    explainer = shap.KernelExplainer(predict_fn, background)
    shap_values = explainer.shap_values(
        observation.reshape(1, -1),
        nsamples=nsamples,
        silent=True,
    )
    
    elapsed_ms = (time.perf_counter() - start_time) * 1000
    
    return shap_values[0], explainer.expected_value, elapsed_ms


# =============================================================================
# BENCHMARK TESTS
# =============================================================================

class ShapBenchmark:
    """Main class for SHAP benchmarks."""
    
    def __init__(self, config: BenchmarkConfig):
        self.config = config
        self.results: Dict[str, List[Dict]] = {
            "feature_scaling": [],
            "background_scaling": [],
            "nsamples_scaling": [],
            "memory_usage": [],
            "stability": [],
            "streaming_overhead": [],
        }
    
    def run_all(self) -> Dict[str, Any]:
        """Run all benchmarks."""
        print("=" * 70)
        print("SHAP BENCHMARK FOR STREAMING CONTEXT")
        print("=" * 70)
        
        self.test_feature_scaling()
        self.test_background_scaling()
        self.test_nsamples_scaling()
        self.test_memory_usage()
        self.test_stability()
        self.test_streaming_overhead()
        
        return self.generate_report()
    
    def test_feature_scaling(self):
        """Test 1: Impact of number of features on computation time."""
        print("\n[TEST 1] Impact of number of features")
        print("-" * 50)
        print(f"  Tested features: {self.config.feature_counts}")
        print("-" * 50)
        
        for n_features in self.config.feature_counts:
            times = []
            
            for rep in range(self.config.n_repetitions):
                # Generate data
                warmup_data = generate_synthetic_data(100, n_features, seed=rep)
                background = generate_synthetic_data(100, n_features, seed=rep + 100)
                test_obs = generate_synthetic_data(1, n_features, seed=rep + 200)[0]
                
                # Create model
                model = create_hstree_model(
                    n_features=n_features,
                    warmup_data=warmup_data,
                )
                
                # Train model
                for row in warmup_data:
                    model.fit_partial(row)
                
                # Compute SHAP
                _, _, elapsed_ms = compute_shap_values(
                    model, test_obs, background, nsamples=100
                )
                times.append(elapsed_ms)
            
            avg_time = np.mean(times)
            std_time = np.std(times)
            
            self.results["feature_scaling"].append({
                "n_features": n_features,
                "avg_time_ms": avg_time,
                "std_time_ms": std_time,
                "min_time_ms": np.min(times),
                "max_time_ms": np.max(times),
            })
            
            print(f"  {n_features:3d} features → {avg_time:8.1f} ms (±{std_time:.1f})")
    
    def test_background_scaling(self):
        """Test 2: Impact of background size."""
        print("\n[TEST 2] Impact of background size")
        print("-" * 50)
        print(f"  Tested background sizes: {self.config.background_sizes}")
        print("-" * 50)

        n_features = 100  # Fixed
        
        for bg_size in self.config.background_sizes:
            times = []
            
            for rep in range(self.config.n_repetitions):
                warmup_data = generate_synthetic_data(100, n_features, seed=rep)
                background = generate_synthetic_data(bg_size, n_features, seed=rep + 100)
                test_obs = generate_synthetic_data(1, n_features, seed=rep + 200)[0]
                
                model = create_hstree_model(n_features=n_features, warmup_data=warmup_data)
                for row in warmup_data:
                    model.fit_partial(row)
                
                _, _, elapsed_ms = compute_shap_values(
                    model, test_obs, background, nsamples=100
                )
                times.append(elapsed_ms)
            
            avg_time = np.mean(times)
            std_time = np.std(times)
            
            self.results["background_scaling"].append({
                "background_size": bg_size,
                "avg_time_ms": avg_time,
                "std_time_ms": std_time,
                "min_time_ms": np.min(times),
                "max_time_ms": np.max(times),
            })
            
            print(f"  Background {bg_size:3d} → {avg_time:8.1f} ms (±{std_time:.1f})")
    
    def test_nsamples_scaling(self):
        """Test 3: Impact of the nsamples parameter."""
        print("\n[TEST 3] Impact of the nsamples parameter")
        print("-" * 50)
        print(f"  Tested nsamples values: {self.config.nsamples_values}")
        print("-" * 50)

        n_features = 100  # Fixed
        bg_size = 100  # Fixed
        
        for nsamples in self.config.nsamples_values:
            times = []
            
            for rep in range(self.config.n_repetitions):
                warmup_data = generate_synthetic_data(100, n_features, seed=rep)
                background = generate_synthetic_data(bg_size, n_features, seed=rep + 100)
                test_obs = generate_synthetic_data(1, n_features, seed=rep + 200)[0]
                
                model = create_hstree_model(n_features=n_features, warmup_data=warmup_data)
                for row in warmup_data:
                    model.fit_partial(row)
                
                _, _, elapsed_ms = compute_shap_values(
                    model, test_obs, background, nsamples=nsamples
                )
                times.append(elapsed_ms)
            
            avg_time = np.mean(times)
            std_time = np.std(times)
            
            self.results["nsamples_scaling"].append({
                "nsamples": nsamples,
                "avg_time_ms": avg_time,
                "std_time_ms": std_time,
                "min_time_ms": np.min(times),
                "max_time_ms": np.max(times),
            })
            
            print(f"  nsamples {nsamples:3d} → {avg_time:8.1f} ms (±{std_time:.1f})")
    
    def test_memory_usage(self):
        """Test 4: Memory usage."""
        print("\n[TEST 4] Memory usage")
        print("-" * 50)
        
        for n_features in [50, 100, 150]:
            tracemalloc.start()
            
            warmup_data = generate_synthetic_data(100, n_features)
            background = generate_synthetic_data(100, n_features)
            test_obs = generate_synthetic_data(1, n_features)[0]
            
            model = create_hstree_model(n_features=n_features, warmup_data=warmup_data)
            for row in warmup_data:
                model.fit_partial(row)
            
            # Measurement before SHAP
            snapshot1 = tracemalloc.take_snapshot()

            # SHAP computation
            def predict_fn(X):
                return np.array([model.score_partial(row) for row in X])
            
            explainer = shap.KernelExplainer(predict_fn, background)
            shap_values = explainer.shap_values(
                test_obs.reshape(1, -1), nsamples=100, silent=True
            )
            
            # Measurement after SHAP
            snapshot2 = tracemalloc.take_snapshot()
            
            tracemalloc.stop()
            
            # Compute the difference
            top_stats = snapshot2.compare_to(snapshot1, 'lineno')
            total_diff = sum(stat.size_diff for stat in top_stats)
            
            self.results["memory_usage"].append({
                "n_features": n_features,
                "memory_diff_mb": total_diff / (1024 * 1024),
            })
            
            print(f"  {n_features:3d} features → {total_diff / (1024 * 1024):.2f} MB")
    
    def test_stability(self):
        """Test 5: Stability of explanations over time (concept drift)."""
        print("\n[TEST 5] Stability of explanations (concept drift)")
        print("-" * 50)
        
        n_features = 50
        n_observations = 100
        
        # Create model
        warmup_data = generate_synthetic_data(100, n_features)
        model = create_hstree_model(n_features=n_features, warmup_data=warmup_data)
        for row in warmup_data:
            model.fit_partial(row)
        
        # Sliding buffer for the background
        background_buffer = deque(maxlen=100)
        for row in warmup_data:
            background_buffer.append(row)
        
        # Simulate a data stream
        shap_values_over_time = []
        
        for i in range(n_observations):
            # Generate a new observation
            # Introduce slight drift after 50 observations
            if i < 50:
                obs = np.random.rand(n_features)
            else:
                # Drift: increase the mean of some features
                obs = np.random.rand(n_features)
                obs[:10] += 0.3  # Drift on the first 10 features

            # Update the model
            model.fit_partial(obs)
            background_buffer.append(obs)

            # Compute SHAP (every 10 observations)
            if i % 10 == 0 and i > 0:
                background = np.array(list(background_buffer))
                shap_vals, _, _ = compute_shap_values(
                    model, obs, background, nsamples=50
                )
                shap_values_over_time.append({
                    "observation": i,
                    "shap_values": shap_vals,
                    "top_feature_idx": np.argmax(np.abs(shap_vals)),
                })
        
        # Analyze stability
        top_features = [s["top_feature_idx"] for s in shap_values_over_time]
        unique_top = len(set(top_features))
        
        self.results["stability"].append({
            "n_observations": n_observations,
            "unique_top_features": unique_top,
            "top_features_sequence": top_features,
        })
        
        print(f"  Unique top feature count: {unique_top} across {len(top_features)} measurements")
        print(f"  Sequence: {top_features}")
    
    def test_streaming_overhead(self):
        """Test 6: Streaming vs batch overhead."""
        print("\n[TEST 6] Streaming vs batch overhead")
        print("-" * 50)
        
        n_features = 100
        n_observations = 20
        
        warmup_data = generate_synthetic_data(100, n_features)
        test_data = generate_synthetic_data(n_observations, n_features)
        
        # Streaming mode (recreate the explainer every time)
        model = create_hstree_model(n_features=n_features, warmup_data=warmup_data)
        for row in warmup_data:
            model.fit_partial(row)
        
        background = warmup_data.copy()
        
        streaming_times = []
        start_total = time.perf_counter()
        
        for obs in test_data:
            model.fit_partial(obs)
            _, _, elapsed = compute_shap_values(model, obs, background, nsamples=50)
            streaming_times.append(elapsed)
        
        streaming_total = (time.perf_counter() - start_total) * 1000
        
        # Batch mode (reuse the explainer)
        model2 = create_hstree_model(n_features=n_features, warmup_data=warmup_data)
        for row in warmup_data:
            model2.fit_partial(row)
        
        def predict_fn(X):
            return np.array([model2.score_partial(row) for row in X])
        
        start_total = time.perf_counter()
        explainer = shap.KernelExplainer(predict_fn, background)
        
        batch_times = []
        for obs in test_data:
            model2.fit_partial(obs)
            start = time.perf_counter()
            shap_vals = explainer.shap_values(obs.reshape(1, -1), nsamples=50, silent=True)
            batch_times.append((time.perf_counter() - start) * 1000)
        
        batch_total = (time.perf_counter() - start_total) * 1000
        
        self.results["streaming_overhead"].append({
            "n_observations": n_observations,
            "streaming_avg_ms": np.mean(streaming_times),
            "streaming_total_ms": streaming_total,
            "batch_avg_ms": np.mean(batch_times),
            "batch_total_ms": batch_total,
            "overhead_percent": ((streaming_total - batch_total) / batch_total) * 100,
        })
        
        print(f"  Streaming: {np.mean(streaming_times):.1f} ms/obs (total: {streaming_total:.1f} ms)")
        print(f"  Batch:     {np.mean(batch_times):.1f} ms/obs (total: {batch_total:.1f} ms)")
        print(f"  Overhead:  {((streaming_total - batch_total) / batch_total) * 100:.1f}%")
    
    def generate_report(self) -> Dict[str, Any]:
        """Generate the final report."""
        print("\n" + "=" * 70)
        print("FINAL REPORT")
        print("=" * 70)
        
        report = {
            "config": {
                "feature_counts": self.config.feature_counts,
                "background_sizes": self.config.background_sizes,
                "nsamples_values": self.config.nsamples_values,
                "n_repetitions": self.config.n_repetitions,
            },
            "summary": {},
            "details": self.results,
            "recommendations": [],
            "limits": [],
        }
        
        # 1. Feature scaling analysis
        if self.results["feature_scaling"]:
            feature_data = self.results["feature_scaling"]
            min_features = min(feature_data, key=lambda x: x["avg_time_ms"])
            max_features = max(feature_data, key=lambda x: x["avg_time_ms"])
            
            report["summary"]["feature_scaling"] = {
                "best_case": f"{min_features['n_features']} features → {min_features['avg_time_ms']:.1f} ms",
                "worst_case": f"{max_features['n_features']} features → {max_features['avg_time_ms']:.1f} ms",
                "scaling_factor": max_features["avg_time_ms"] / min_features["avg_time_ms"],
            }
            
            print(f"\n1. FEATURE SCALING:")
            print(f"   Best:  {min_features['n_features']} features → {min_features['avg_time_ms']:.1f} ms")
            print(f"   Worst: {max_features['n_features']} features → {max_features['avg_time_ms']:.1f} ms")
            print(f"   Ratio: x{max_features['avg_time_ms'] / min_features['avg_time_ms']:.1f}")

        # 2. nsamples scaling analysis
        if self.results["nsamples_scaling"]:
            nsamples_data = self.results["nsamples_scaling"]
            min_nsamples = min(nsamples_data, key=lambda x: x["avg_time_ms"])
            max_nsamples = max(nsamples_data, key=lambda x: x["avg_time_ms"])
            
            report["summary"]["nsamples_scaling"] = {
                "best_case": f"nsamples={min_nsamples['nsamples']} → {min_nsamples['avg_time_ms']:.1f} ms",
                "worst_case": f"nsamples={max_nsamples['nsamples']} → {max_nsamples['avg_time_ms']:.1f} ms",
                "scaling_factor": max_nsamples["avg_time_ms"] / min_nsamples["avg_time_ms"],
            }
            
            print(f"\n2. NSAMPLES SCALING:")
            print(f"   Best:  nsamples={min_nsamples['nsamples']} → {min_nsamples['avg_time_ms']:.1f} ms")
            print(f"   Worst: nsamples={max_nsamples['nsamples']} → {max_nsamples['avg_time_ms']:.1f} ms")
            print(f"   Ratio: x{max_nsamples['avg_time_ms'] / min_nsamples['avg_time_ms']:.1f}")

        # 3. Background scaling analysis
        if self.results["background_scaling"]:
            bg_data = self.results["background_scaling"]
            min_bg = min(bg_data, key=lambda x: x["avg_time_ms"])
            max_bg = max(bg_data, key=lambda x: x["avg_time_ms"])
            
            report["summary"]["background_scaling"] = {
                "best_case": f"background={min_bg['background_size']} → {min_bg['avg_time_ms']:.1f} ms",
                "worst_case": f"background={max_bg['background_size']} → {max_bg['avg_time_ms']:.1f} ms",
                "scaling_factor": max_bg["avg_time_ms"] / min_bg["avg_time_ms"],
            }
            
            print(f"\n3. BACKGROUND SCALING:")
            print(f"   Best:  background={min_bg['background_size']} → {min_bg['avg_time_ms']:.1f} ms")
            print(f"   Worst: background={max_bg['background_size']} → {max_bg['avg_time_ms']:.1f} ms")
            print(f"   Ratio: x{max_bg['avg_time_ms'] / min_bg['avg_time_ms']:.1f}")

        # 4. Identified limits
        print("\n4. IDENTIFIED LIMITS:")
        limits = []

        # Check latency
        if self.results["feature_scaling"]:
            avg_times = [r["avg_time_ms"] for r in self.results["feature_scaling"]]
            max_avg = max(avg_times)
            if max_avg > 1000:
                limits.append(f"LATENCY: Max time {max_avg:.0f} ms > 1s (slow streaming)")
            if max_avg > 2000:
                limits.append(f"CRITICAL LATENCY: Max time {max_avg:.0f} ms > 2s")

        # Check throughput
        if self.results["nsamples_scaling"]:
            for r in self.results["nsamples_scaling"]:
                throughput = 1000 / r["avg_time_ms"]
                if throughput < 1:
                    limits.append(f"THROUGHPUT: nsamples={r['nsamples']} → {throughput:.2f} msg/s < 1 msg/s")

        # Check stability
        if self.results["stability"]:
            stability = self.results["stability"][0]
            if stability["unique_top_features"] > 5:
                limits.append(f"STABILITY: {stability['unique_top_features']} different features as top")

        # Check overhead
        if self.results["streaming_overhead"]:
            overhead = self.results["streaming_overhead"][0]["overhead_percent"]
            if overhead > 20:
                limits.append(f"OVERHEAD: Streaming +{overhead:.1f}% vs batch")
        
        report["limits"] = limits
        for limit in limits:
            print(f"   - {limit}")
        
        # 5. Recommendations
        print("\n5. RECOMMENDATIONS:")

        if self.results["nsamples_scaling"]:
            # Find the best time/quality tradeoff
            for r in self.results["nsamples_scaling"]:
                if r["nsamples"] <= 50 and r["avg_time_ms"] < 1000:
                    rec = f"Use nsamples={r['nsamples']} for a good tradeoff ({r['avg_time_ms']:.0f} ms)"
                    report["recommendations"].append(rec)
                    print(f"   - {rec}")
                    break

        if self.results["background_scaling"]:
            for r in self.results["background_scaling"]:
                if r["background_size"] <= 50 and r["avg_time_ms"] < 1000:
                    rec = f"Use background_size={r['background_size']} ({r['avg_time_ms']:.0f} ms)"
                    report["recommendations"].append(rec)
                    print(f"   - {rec}")
                    break

        if self.results["streaming_overhead"]:
            overhead = self.results["streaming_overhead"][0]["overhead_percent"]
            if overhead > 20:
                rec = f"Reuse the explainer when possible to reduce overhead by {overhead:.1f}%"
                report["recommendations"].append(rec)
                print(f"   - {rec}")
        
        return report


# =============================================================================
# ENTRY POINT
# =============================================================================

def main():
    """Benchmark entry point."""
    config = BenchmarkConfig(
        feature_counts=[5, 10, 20, 30, 40, 50, 100, 150],
        background_sizes=[20, 50, 100, 200],
        nsamples_values=[5, 10, 20, 30, 40, 50, 100, 150],
        n_repetitions=3,
    )
    
    benchmark = ShapBenchmark(config)
    report = benchmark.run_all()
    
    # Save the report
    output_file = "/tmp/shap_benchmark_report.json"
    with open(output_file, "w") as f:
        # Convert numpy arrays to lists for JSON
        def convert(obj):
            if isinstance(obj, np.ndarray):
                return obj.tolist()
            if isinstance(obj, np.floating):
                return float(obj)
            if isinstance(obj, np.integer):
                return int(obj)
            return obj
        
        json.dump(report, f, indent=2, default=convert)
    
    print(f"\n✓ Report saved: {output_file}")


if __name__ == "__main__":
    main()
