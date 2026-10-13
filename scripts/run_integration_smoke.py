#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class LogExpectation:
    service: str
    pattern: str
    timeout_s: int
    tail_lines: int


@dataclass(frozen=True)
class Scenario:
    name: str
    label: str
    services: tuple[str, ...]
    expectations: tuple[LogExpectation, ...]


SCENARIOS: dict[str, Scenario] = {
    "hstree": Scenario(
        name="hstree",
        label="[integration]",
        services=("kafka1", "kafka2", "kafka3", "schema-registry", "init-topic", "producer", "hstree-analysis"),
        expectations=(
            LogExpectation("init-topic", "Done.", 240, 200),
            LogExpectation("producer", "Finished writing & sending to Kafka", 300, 200),
            LogExpectation("hstree-analysis", "Processed=", 300, 500),
        ),
    ),
    "metrics": Scenario(
        name="metrics",
        label="[integration][metrics]",
        services=("kafka1", "kafka2", "kafka3", "schema-registry", "init-topic", "producer", "hstree-analysis", "build-metrics"),
        expectations=(
            LogExpectation("init-topic", "Done.", 240, 200),
            LogExpectation("producer", "Finished writing & sending to Kafka", 300, 200),
            LogExpectation("hstree-analysis", "Processed=", 300, 500),
            LogExpectation("build-metrics", "Metrics published in 'topic-metrics'", 300, 200),
        ),
    ),
    "iforest": Scenario(
        name="iforest",
        label="[integration][iforest]",
        services=("kafka1", "kafka2", "kafka3", "schema-registry", "init-topic", "producer", "iforest-analysis"),
        expectations=(
            LogExpectation("init-topic", "Done.", 240, 200),
            LogExpectation("producer", "Finished writing & sending to Kafka", 300, 200),
            LogExpectation("iforest-analysis", "iforest_score", 300, 500),
        ),
    ),
    "explainability": Scenario(
        name="explainability",
        label="[integration][explainability]",
        services=("kafka1", "kafka2", "kafka3", "schema-registry", "init-topic", "producer", "hstree-analysis", "explainability"),
        expectations=(
            LogExpectation("init-topic", "Done.", 240, 200),
            LogExpectation("producer", "Finished writing & sending to Kafka", 300, 200),
            LogExpectation("hstree-analysis", "Processed=", 300, 500),
            LogExpectation("explainability", "Worker started for group", 420, 400),
            LogExpectation("explainability", '"local_score"', 420, 500),
        ),
    ),
}

SCENARIO_ORDER: tuple[str, ...] = ("hstree", "metrics", "iforest", "explainability")


def run_command(args: list[str], *, capture: bool = False, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=ROOT_DIR,
        check=check,
        text=True,
        capture_output=capture,
    )


def cleanup() -> None:
    run_command(["docker", "compose", "down", "-v", "--remove-orphans"], capture=True, check=False)


def fetch_logs(service: str, tail_lines: int) -> str:
    result = run_command(
        ["docker", "compose", "logs", "--no-color", "--tail", str(tail_lines), service],
        capture=True,
        check=False,
    )
    return result.stdout + result.stderr


def wait_for_pattern(label: str, expectation: LogExpectation) -> None:
    deadline = time.time() + expectation.timeout_s
    while time.time() < deadline:
        logs = fetch_logs(expectation.service, expectation.tail_lines)
        if expectation.pattern in logs:
            return
        time.sleep(5)
    raise RuntimeError(
        f"{label} timeout waiting for pattern {expectation.pattern!r} in service {expectation.service!r}"
    )


def run_scenario(scenario: Scenario, keep_up: bool = False) -> None:
    print(f"{scenario.label} starting smoke test", flush=True)
    cleanup()
    try:
        run_command(
            ["bash", str(ROOT_DIR / "scripts" / "docker" / "pull_missing_images.sh"), *scenario.services],
            capture=True,
        )
        run_command(["docker", "compose", "up", "--pull", "never", "-d", *scenario.services], capture=True)
        for expectation in scenario.expectations:
            wait_for_pattern(scenario.label, expectation)
        print(f"{scenario.label} smoke test succeeded", flush=True)
    finally:
        if not keep_up:
            cleanup()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Docker-based integration smoke tests for dasma-kafka.")
    parser.add_argument(
        "scenario",
        choices=sorted((*SCENARIOS.keys(), "all")),
        help="Smoke-test scenario to run.",
    )
    parser.add_argument(
        "--keep-up",
        action="store_true",
        help="Do not tear down docker compose services after the scenario completes.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.scenario == "all":
            for scenario_name in SCENARIO_ORDER:
                run_scenario(SCENARIOS[scenario_name], keep_up=False)
        else:
            run_scenario(SCENARIOS[args.scenario], keep_up=args.keep_up)
    except subprocess.CalledProcessError as exc:
        if exc.stdout:
            sys.stdout.write(exc.stdout)
        if exc.stderr:
            sys.stderr.write(exc.stderr)
        return exc.returncode or 1
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
