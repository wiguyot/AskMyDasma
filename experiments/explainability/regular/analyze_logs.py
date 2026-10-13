#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Streaming SHAP Limits Analyzer
==========================================

File status
-----------------
This script is an offline analysis utility.
It is not part of the main application pipeline.

It analyzes `explainability` service logs after the fact to produce a summary
report such as `shap_report.txt`.

It identifies SHAP limits in a real-time streaming context.

Analyzed limits:
    1. Time performance (latency, throughput)
    2. Explanation stability
    3. Group variability

Usage:
    docker logs explainability --tail 20000 | docker exec -i explainability python /app/experiments/explainability/regular/analyze_logs.py > ~/dasma-kafka/experiments/explainability/regular/shap_report.txt

Author: Melissa GHEMARI
Date: January 2026
"""

import sys
import re
import json
import numpy as np
from collections import defaultdict
from datetime import datetime


def parse_logs(logs: str) -> dict:
    """Parse logs and extract metrics."""
    
    json_pattern = r'\{"Group":\s*"([^"]+)",\s*"hstree_score":\s*(-?[\d.]+),\s*"local_score":\s*(-?[\d.]+),\s*"shap_computed":\s*(true|false),\s*"shap_time_ms":\s*([\d.]+)\}'
    
    data = {
        "shap_times_ms": [],
        "groups": defaultdict(list),
        "top_features": [],
        "top_features_by_group": defaultdict(list),
    }
    
    top_contrib_pattern = r'Top contrib\w+:\s*(\{[^}]+\})'
    current_group = None
    
    lines = logs.split('\n')
    
    for i, line in enumerate(lines):
        match = re.search(json_pattern, line)
        if match:
            group = match.group(1)
            current_group = group
            shap_computed = match.group(4) == "true"
            shap_time_ms = float(match.group(5))
            
            if shap_computed and shap_time_ms > 0:
                data["shap_times_ms"].append(shap_time_ms)
                data["groups"][group].append(shap_time_ms)
        
        match = re.search(top_contrib_pattern, line)
        if match:
            try:
                contrib_str = match.group(1)
                contrib_dict = json.loads(contrib_str)
                if contrib_dict:
                    top_feature = max(contrib_dict.keys(), key=lambda k: abs(contrib_dict[k]))
                    data["top_features"].append(top_feature)
                    if current_group:
                        data["top_features_by_group"][current_group].append(top_feature)
            except (json.JSONDecodeError, ValueError):
                pass
    
    return data


def analyze_performance_limits(data: dict) -> dict:
    """Analyze performance limits."""
    
    limits = {}
    
    if not data["shap_times_ms"]:
        return limits
    
    times = data["shap_times_ms"]
    
    # Basic metrics.
    avg_time = float(np.mean(times))
    max_throughput = 1000 / avg_time
    
    limits["performance"] = {
        "samples": len(times),
        "avg_time_ms": round(avg_time, 2),
        "median_time_ms": round(float(np.median(times)), 2),
        "min_time_ms": round(float(np.min(times)), 2),
        "max_time_ms": round(float(np.max(times)), 2),
        "std_dev_ms": round(float(np.std(times)), 2),
        "p90_ms": round(float(np.percentile(times, 90)), 2),
        "p95_ms": round(float(np.percentile(times, 95)), 2),
        "p99_ms": round(float(np.percentile(times, 99)), 2),
        "max_throughput_per_sec": round(max_throughput, 2),
    }
    
    # Identified limits.
    limits["performance_limits"] = {
        "high_latency": avg_time > 1000,
        "critical_latency": avg_time > 2000,
        "insufficient_throughput": max_throughput < 1,
        "high_variability": float(np.std(times)) > 100,
    }
    
    # Streaming capacity.
    limits["streaming_capacity"] = {
        "messages_per_second": round(max_throughput, 2),
        "messages_per_minute": round(max_throughput * 60, 0),
        "messages_per_hour": round(max_throughput * 3600, 0),
        "time_for_1000_messages_sec": round(1000 / max_throughput, 0),
    }
    
    return limits


def analyze_stability_limits(data: dict) -> dict:
    """Analyze explanation stability limits."""
    
    limits = {}
    
    if not data["top_features"]:
        return limits
    
    features = data["top_features"]
    unique = len(set(features))
    most_common = max(set(features), key=features.count)
    count_most_common = features.count(most_common)
    consistency = count_most_common / len(features) * 100
    
    # Top feature distribution.
    feature_counts = defaultdict(int)
    for f in features:
        feature_counts[f] += 1
    top_10 = sorted(feature_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    
    limits["global_stability"] = {
        "observations": len(features),
        "unique_features": unique,
        "dominant_feature": most_common,
        "dominant_frequency": count_most_common,
        "consistency_pct": round(consistency, 2),
        "top_10_features": dict(top_10),
    }
    
    # Stability by group.
    limits["stability_by_group"] = {}
    for group, group_features in data["top_features_by_group"].items():
        if group_features:
            g_unique = len(set(group_features))
            g_most_common = max(set(group_features), key=group_features.count)
            g_count = group_features.count(g_most_common)
            g_consistency = g_count / len(group_features) * 100
            
            limits["stability_by_group"][group] = {
                "observations": len(group_features),
                "unique_features": g_unique,
                "dominant_feature": g_most_common,
                "consistency_pct": round(g_consistency, 2),
            }
    
    # Identified limits.
    limits["stability_limits"] = {
        "unstable_explanations": consistency < 30,
        "too_many_different_features": unique > 50,
        "no_clear_pattern": consistency < 10,
    }
    
    return limits


def analyze_group_limits(data: dict) -> dict:
    """Analyze limits by group."""
    
    limits = {}
    
    if not data["groups"]:
        return limits
    
    limits["by_group"] = {}
    
    for group, times in data["groups"].items():
        if times:
            limits["by_group"][group] = {
                "observations": len(times),
                "avg_time_ms": round(float(np.mean(times)), 2),
                "min_time_ms": round(float(np.min(times)), 2),
                "max_time_ms": round(float(np.max(times)), 2),
                "std_dev_ms": round(float(np.std(times)), 2),
            }
    
    # Group comparison.
    group_avgs = [np.mean(times) for times in data["groups"].values() if times]
    if group_avgs:
        limits["group_comparison"] = {
            "min_avg_time_ms": round(float(np.min(group_avgs)), 2),
            "max_avg_time_ms": round(float(np.max(group_avgs)), 2),
            "inter_group_gap_ms": round(float(np.max(group_avgs) - np.min(group_avgs)), 2),
            "homogeneous_groups": float(np.max(group_avgs) - np.min(group_avgs)) < 100,
        }
    
    return limits


def print_report(results: dict):
    """Print the limit analysis report."""
    
    print("\n" + "=" * 80)
    print("STREAMING SHAP LIMITS ANALYSIS")
    print("=" * 80)
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # LIMIT 1: PERFORMANCE
    if "performance" in results:
        p = results["performance"]
        lim = results.get("performance_limits", {})
        cap = results.get("streaming_capacity", {})
        
        print("\n" + "-" * 80)
        print("LIMIT 1: TIME PERFORMANCE")
        print("-" * 80)
        
        print(f"\n   Analyzed samples: {p['samples']}")
        print(f"\n   SHAP computation time:")
        print(f"      Average:     {p['avg_time_ms']} ms")
        print(f"      Median:      {p['median_time_ms']} ms")
        print(f"      Min:         {p['min_time_ms']} ms")
        print(f"      Max:         {p['max_time_ms']} ms")
        print(f"      Std dev:     {p['std_dev_ms']} ms")
        print(f"      P90:         {p['p90_ms']} ms")
        print(f"      P95:         {p['p95_ms']} ms")
        print(f"      P99:         {p['p99_ms']} ms")
        
        print(f"\n   Streaming capacity:")
        print(f"      Max throughput:        {cap['messages_per_second']} msg/s")
        print(f"      Messages per minute:   {cap['messages_per_minute']}")
        print(f"      Messages per hour:     {cap['messages_per_hour']}")
        print(f"      Time for 1000 msg:     {cap['time_for_1000_messages_sec']} seconds")
        
        print(f"\n   Evaluation:")
        if lim.get("critical_latency"):
            print("      [CRITICAL] Latency > 2s - incompatible with real-time streaming")
        elif lim.get("high_latency"):
            print("      [WARN] Latency > 1s - slow streaming")
        else:
            print("      [OK] Acceptable latency")
        
        if lim.get("insufficient_throughput"):
            print("      [WARN] Throughput < 1 msg/s - congestion risk")
        
        if lim.get("high_variability"):
            print("      [WARN] High variability - unpredictable timings")
        else:
            print("      [OK] Stable and predictable timings")
    
    # LIMIT 2: STABILITY
    if "global_stability" in results:
        s = results["global_stability"]
        lim = results.get("stability_limits", {})
        
        print("\n" + "-" * 80)
        print("LIMIT 2: EXPLANATION STABILITY")
        print("-" * 80)
        
        print(f"\n   Observations: {s['observations']}")
        print(f"   Unique features as top feature: {s['unique_features']}")
        print(f"   Dominant feature: {s['dominant_feature']} ({s['dominant_frequency']} times)")
        print(f"   Consistency: {s['consistency_pct']}%")
        
        print(f"\n   Top 10 most frequent features:")
        for feature, count in s["top_10_features"].items():
            pct = count / s['observations'] * 100
            print(f"      - {feature}: {count} ({pct:.1f}%)")
        
        print(f"\n   Evaluation:")
        if lim.get("no_clear_pattern"):
            print("      [CRITICAL] Consistency < 10% - no clear pattern")
        elif lim.get("unstable_explanations"):
            print("      [WARN] Consistency < 30% - variable explanations")
        else:
            print("      [OK] Stable explanations")
        
        if lim.get("too_many_different_features"):
            print("      [WARN] > 50 different features - difficult interpretability")
    
    # STABILITY BY GROUP
    if "stability_by_group" in results:
        print(f"\n   Stability by group:")
        for group, g_stats in results["stability_by_group"].items():
            print(f"      {group}: {g_stats['consistency_pct']}% consistency, "
                  f"{g_stats['unique_features']} unique features, "
                  f"dominant: {g_stats['dominant_feature']}")
    
    # LIMIT 3: PERFORMANCE BY GROUP
    if "by_group" in results:
        print("\n" + "-" * 80)
        print("LIMIT 3: PERFORMANCE BY GROUP")
        print("-" * 80)
        
        for group, g_stats in results["by_group"].items():
            print(f"\n   {group}:")
            print(f"      Observations: {g_stats['observations']}")
            print(f"      Avg time:     {g_stats['avg_time_ms']} ms")
            print(f"      Min time:     {g_stats['min_time_ms']} ms")
            print(f"      Max time:     {g_stats['max_time_ms']} ms")
        
        if "group_comparison" in results:
            comp = results["group_comparison"]
            print(f"\n   Comparison:")
            print(f"      Inter-group gap: {comp['inter_group_gap_ms']} ms")
            if comp["homogeneous_groups"]:
                print("      [OK] Homogeneous groups")
            else:
                print("      [WARN] Heterogeneous groups - some groups are slower")
    
    # LIMIT SUMMARY
    print("\n" + "=" * 80)
    print("IDENTIFIED LIMITS SUMMARY")
    print("=" * 80)
    
    all_limits = []
    
    if results.get("performance_limits", {}).get("high_latency"):
        all_limits.append("1. HIGH LATENCY: ~1.5s per SHAP computation")
    
    if results.get("performance_limits", {}).get("insufficient_throughput"):
        all_limits.append("2. LOW THROUGHPUT: < 1 msg/s")
    
    if results.get("stability_limits", {}).get("unstable_explanations"):
        all_limits.append("3. UNSTABLE EXPLANATIONS: consistency < 30%")
    
    if all_limits:
        for limit in all_limits:
            print(f"   {limit}")
    else:
        print("   No critical limit identified")
    
    # PROPOSED SOLUTIONS
    print("\n" + "=" * 80)
    print("POTENTIAL SOLUTIONS")
    print("=" * 80)
    
    if results.get("performance_limits", {}).get("high_latency"):
        print("\n   To reduce latency:")
        print("      - Reduce nsamples, for example 50 instead of 100")
        print("      - Reduce background_size, for example 30 instead of 50")
        print("      - Use asynchronous SHAP computation")
        print("      - Precompute explanations for frequent cases")
    
    if results.get("stability_limits", {}).get("unstable_explanations"):
        print("\n   To stabilize explanations:")
        print("      - Increase nsamples; more samples make results more stable")
        print("      - Analyze whether variability comes from the data")
        print("      - Group similar features")
    
    print("\n" + "=" * 80)
    
    # JSON
    results["timestamp"] = datetime.now().isoformat()
    print("\nJSON REPORT:")
    print(json.dumps(results, indent=2, ensure_ascii=False))


def main():
    """Main entry point."""
    
    print("Reading logs...")
    
    if not sys.stdin.isatty():
        logs = sys.stdin.read()
    else:
        print("[ERROR] No log received.")
        print("Usage: docker logs explainability | docker exec -i explainability python /app/experiments/explainability/regular/analyze_logs.py")
        return
    
    if not logs:
        print("[ERROR] Empty logs")
        return
    
    print(f"Received logs: {len(logs)} characters")
    
    data = parse_logs(logs)
    
    if not data["shap_times_ms"]:
        print("[WARN] No SHAP metric found in logs")
        return
    
    # Analyze all limits.
    results = {}
    
    perf_limits = analyze_performance_limits(data)
    results.update(perf_limits)
    
    stability_limits = analyze_stability_limits(data)
    results.update(stability_limits)
    
    group_limits = analyze_group_limits(data)
    results.update(group_limits)
    
    print_report(results)


if __name__ == "__main__":
    main()
