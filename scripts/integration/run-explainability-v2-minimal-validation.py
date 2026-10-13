#!/usr/bin/env python3
"""OC-15 — non-destructive Docker runner for the minimal validation of
explainability-v2 content.

Chain under test, per run:
CSV fixture -> oc15 producer -> oc15 hstree -> oc15 v2 -> dedicated JSON viz
topic -> raw export.

The runner executes, in order:
1. TWO isolated campaigns with the SAME fixture and the SAME reference seed;
2. the pure minimal validator on both reference exports;
3. ONE campaign per secondary seed;
4. the pure exploratory seed-stability measurement against the reference;
5. versioned Markdown and JSON reports.

Non-destructive by construction (OC-12R.1..OC-14 guarantees):
- every created resource carries the per-run OC15_RUN_ID and the seed;
- the runner NEVER deletes topics, consumer groups, Avro subjects or data
  (created resources stay available for audit);
- the runner REFUSES to start when the standard compute services are running;
- only the oc15-* services are started and stopped;
- artifacts are always preserved: they are the validation proof.

This script is never executed by pytest; no Docker call happens at import
time.

Usage:
    PYTHONPATH=src:. .venv/bin/python \\
        scripts/integration/run-explainability-v2-minimal-validation.py \\
        [--artifacts-dir DIR] [--keep-up] [--global-timeout S]
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Callable, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_DIR = PROJECT_ROOT / "tests" / "fixtures" / "explainability_v2"
MANIFEST = FIXTURE_DIR / "oc15_validation_manifest.json"
VALIDATOR = PROJECT_ROOT / "experiments" / "explainability" / "v2" / "validate_minimal_explainability.py"
SEED_STABILITY = PROJECT_ROOT / "experiments" / "explainability" / "v2" / "measure_shap_seed_stability.py"
REPORTS_DIR = PROJECT_ROOT / "experiments" / "explainability" / "v2" / "reports"

BASE_COMPOSE = ["-f", "docker-compose.yml"]
OC15_COMPOSE = BASE_COMPOSE + ["-f", "docker/compose/experiments/docker-compose.oc15.yml"]

# Standard compute services: if any of them is running, the runner refuses to
# start (it never stops or restarts shared resources).
STANDARD_COMPUTE_SERVICES = [
    "producer",
    "hstree-analysis",
    "explainability-v2",
    "explainability",
    "telegraf",
]

# Infrastructure services: started if missing, never stopped by the runner.
INFRA_SERVICES = ["kafka1", "kafka2", "kafka3", "schema-registry"]

OC15_SERVICES = ["oc15-producer", "oc15-hstree", "oc15-v2"]

OC15_TOPICS = [
    "oc15-s{seed}-source-{run_id}",
    "oc15-s{seed}-hstree-{run_id}",
    "oc15-s{seed}-hstree-viz-{run_id}",
    "oc15-s{seed}-v2-{run_id}",
    "oc15-s{seed}-v2-viz-{run_id}",
]


class RunnerError(RuntimeError):
    """Fatal campaign error; artifacts are preserved for diagnosis."""


def log(message: str) -> None:
    print(f"[oc15 {dt.datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def generate_run_id(now: Optional[dt.datetime] = None) -> str:
    """Build a unique OC-15 run id: oc15-YYYYMMDD-HHMMSS-<8 hex chars>."""
    stamp = (now or dt.datetime.now()).strftime("%Y%m%d-%H%M%S")
    return f"oc15-{stamp}-{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# Subprocess helpers (bounded timeouts everywhere)
# ---------------------------------------------------------------------------


def run_command(
    command: List[str],
    *,
    timeout: float,
    env_extra: Optional[Dict[str, str]] = None,
    check: bool = True,
) -> subprocess.CompletedProcess:
    merged_env = os.environ.copy()
    if env_extra:
        merged_env.update(env_extra)
    try:
        result = subprocess.run(
            command,
            cwd=str(PROJECT_ROOT),
            env=merged_env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise RunnerError(f"command timed out after {timeout}s: {' '.join(command)}") from exc
    if check and result.returncode != 0:
        raise RunnerError(
            f"command failed ({result.returncode}): {' '.join(command)}\n"
            f"stderr: {result.stderr[-2000:]}"
        )
    return result


def compose(
    *args: str,
    timeout: float,
    files: Optional[List[str]] = None,
    check: bool = True,
    env_extra: Optional[Dict[str, str]] = None,
) -> subprocess.CompletedProcess:
    return run_command(
        ["docker", "compose", *(files or BASE_COMPOSE), *args],
        timeout=timeout,
        env_extra=env_extra,
        check=check,
    )


def wait_until(
    predicate: Callable[[], bool],
    timeout: float,
    description: str,
    interval: float = 2.0,
) -> None:
    deadline = time.monotonic() + timeout
    last_error: Optional[str] = None
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except RunnerError:
            raise
        except Exception as exc:  # transient docker/kafka errors are retried
            last_error = str(exc)
        time.sleep(interval)
    detail = f" (last error: {last_error})" if last_error else ""
    raise RunnerError(f"timeout after {timeout:.0f}s waiting for {description}{detail}")


# ---------------------------------------------------------------------------
# Kafka state helpers (CLI inside the kafka1 container)
# ---------------------------------------------------------------------------


def topic_message_count(topic: str, env_extra: Dict[str, str]) -> int:
    result = compose(
        "exec", "-T", "kafka1",
        "/opt/kafka/bin/kafka-get-offsets.sh",
        "--bootstrap-server", "kafka1:9092",
        "--topic", topic,
        timeout=30.0,
        env_extra=env_extra,
    )
    total = 0
    for line in result.stdout.splitlines():
        parts = line.strip().split(":")
        if len(parts) >= 3:
            try:
                total += max(int(parts[-1]), 0)
            except ValueError:
                continue
    return total


def consumer_group_lag(group: str, topic: str, env_extra: Dict[str, str]) -> Optional[int]:
    """Sum the LAG column for `topic`; Kafka 3.9 refuses --topic with
    --describe. The header must carry BOTH the TOPIC and LAG columns;
    None when the output is ambiguous."""
    result = compose(
        "exec", "-T", "kafka1",
        "/opt/kafka/bin/kafka-consumer-groups.sh",
        "--bootstrap-server", "kafka1:9092",
        "--describe", "--group", group,
        timeout=30.0,
        check=False,
        env_extra=env_extra,
    )
    lines = result.stdout.splitlines()
    header_index = None
    topic_index = None
    lag_index = None
    for index, line in enumerate(lines):
        tokens = line.split()
        upper_tokens = [token.upper() for token in tokens]
        if "LAG" in upper_tokens and "TOPIC" in upper_tokens:
            header_index = index
            lag_index = upper_tokens.index("LAG")
            topic_index = upper_tokens.index("TOPIC")
            break
    if header_index is None:
        return None
    total = 0
    topic_seen = False
    for line in lines[header_index + 1 :]:
        tokens = line.split()
        if not tokens:
            continue
        if len(tokens) <= lag_index or len(tokens) <= topic_index:
            return None
        if tokens[topic_index] != topic:
            continue
        topic_seen = True
        if not tokens[lag_index].isdigit():
            return None
        total += int(tokens[lag_index])
    return total if topic_seen else None


def producer_finished(env_extra: Dict[str, str]) -> bool:
    result = compose(
        "ps", "--all", "--format", "json", "oc15-producer",
        timeout=30.0, check=False, files=OC15_COMPOSE, env_extra=env_extra,
    )
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if payload.get("Service") == "oc15-producer":
            return payload.get("State") == "exited" and int(payload.get("ExitCode") or 0) == 0
    return False


def create_oc15_topics(seed: int, run_id: str, env_extra: Dict[str, str]) -> None:
    for template in OC15_TOPICS:
        topic = template.format(seed=seed, run_id=run_id)
        compose(
            "exec", "-T", "kafka1",
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server", "kafka1:9092",
            "--create", "--if-not-exists",
            "--topic", topic,
            "--partitions", "1",
            "--replication-factor", "3",
            timeout=30.0,
            env_extra=env_extra,
        )
        log(f"topic ready: {topic}")


# ---------------------------------------------------------------------------
# Docker guards and lifecycle (never destructive)
# ---------------------------------------------------------------------------


def services_running(services: List[str], files: Optional[List[str]] = None, env_extra: Optional[Dict[str, str]] = None) -> List[str]:
    result = compose("ps", "--all", "--format", "json", *services, timeout=30.0, check=False, files=files, env_extra=env_extra)
    running = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if payload.get("State") == "running":
            running.append(payload.get("Service"))
    return running


def oc15_containers_running(env_extra: Dict[str, str]) -> bool:
    return bool(services_running(OC15_SERVICES, files=OC15_COMPOSE, env_extra=env_extra))


def stop_oc15_services(env_extra: Dict[str, str]) -> None:
    """Stop and remove ONLY the oc15-* campaign services of this run.

    Created topics, consumer groups and Avro subjects are never deleted:
    they stay available for audit.
    """
    compose("stop", *OC15_SERVICES, timeout=120.0, check=False, files=OC15_COMPOSE, env_extra=env_extra)
    compose("rm", "-f", *OC15_SERVICES, timeout=60.0, check=False, files=OC15_COMPOSE, env_extra=env_extra)
    try:
        wait_until(
            lambda: not oc15_containers_running(env_extra),
            timeout=60.0,
            description="all oc15 containers stopped and removed",
            interval=1.0,
        )
    except RunnerError as exc:
        # A cleanup problem must never mask the campaign result.
        print(f"[oc15] WARNING: cleanup incomplete: {exc}", file=sys.stderr)


def assert_standard_services_stopped() -> None:
    running = services_running(STANDARD_COMPUTE_SERVICES)
    if running:
        raise RunnerError(
            "standard compute services are still running: "
            + ", ".join(running)
            + ". Stop them first; the OC-15 runner never stops shared services."
        )


def dump_service_logs(env_extra: Dict[str, str], artifacts: Path) -> None:
    try:
        result = compose(
            "logs", "--tail", "60", *OC15_SERVICES,
            timeout=60.0, check=False, files=OC15_COMPOSE, env_extra=env_extra,
        )
        (artifacts / "services-failure-logs.txt").write_text(result.stdout, encoding="utf-8")
        print(f"[oc15] failure logs saved to {artifacts / 'services-failure-logs.txt'}")
    except Exception as exc:
        print(f"[oc15] could not dump service logs: {exc}")


def export_jsonl_topic(topic: str, output_path: Path, expected: int, env_extra: Dict[str, str], timeout: float) -> int:
    result = compose(
        "exec", "-T", "kafka1",
        "/opt/kafka/bin/kafka-console-consumer.sh",
        "--bootstrap-server", "kafka1:9092",
        "--from-beginning",
        "--timeout-ms", "5000",
        "--topic", topic,
        timeout=timeout,
        env_extra=env_extra,
    )
    raw_path = output_path.with_suffix(".raw")
    raw_path.write_text(result.stdout, encoding="utf-8")

    valid_lines = 0
    invalid_lines = 0
    with open(output_path, "w", encoding="utf-8") as handle:
        for line in result.stdout.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            try:
                json.loads(stripped)
            except json.JSONDecodeError:
                invalid_lines += 1
                continue
            handle.write(stripped + "\n")
            valid_lines += 1
    if invalid_lines:
        raise RunnerError(
            f"export of {topic} contains {invalid_lines} invalid JSON lines "
            f"(raw output kept at {raw_path})"
        )
    if valid_lines != expected:
        raise RunnerError(
            f"export of {topic} holds {valid_lines} events, expected {expected}"
        )
    return valid_lines


# ---------------------------------------------------------------------------
# Structural single-export validation (OC-15R2), pure module reused here
# ---------------------------------------------------------------------------

sys.path.insert(0, str(PROJECT_ROOT / "experiments" / "explainability" / "v2"))
from validate_minimal_explainability import load_jsonl, validate_single_export  # noqa: E402


def validate_export_or_fail(manifest: Dict, export_path: Path, export_name: str, artifacts: Path) -> Dict:
    """Validate ONE export structurally; fail the campaign explicitly if it
    is invalid (BEFORE any cross-run comparison or stability measurement)."""
    structure = validate_single_export(manifest, load_jsonl(export_path), export_name)
    if not structure["valid"]:
        raise RunnerError(
            f"{export_name} is structurally INVALID: {structure['error_count']} error(s), "
            f"duplicates={structure['duplicates'][:5]}, "
            f"missing={structure['coverage']['missing'][:5]}, "
            f"unexpected={structure['coverage']['unexpected'][:5]}"
        )
    log(
        f"{export_name}: structurally valid "
        f"({structure['coverage']['observed_events']}/{structure['coverage']['expected_total']} events)"
    )
    return structure


# ---------------------------------------------------------------------------
# One isolated campaign (one seed, one run id)
# ---------------------------------------------------------------------------


def run_single_campaign(
    seed: int,
    manifest: Dict,
    artifacts: Path,
    deadline: float,
    keep_up: bool,
) -> Path:
    """Run one isolated campaign; return its v2 JSONL export path."""
    global _ACTIVE_ENVS
    run_id = generate_run_id()
    env_extra = {"OC15_RUN_ID": run_id, "OC15_SEED": str(seed)}
    expected_total = int(manifest["expected_total_events"])
    run_dir = artifacts / f"seed{seed}-{run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "campaign-env.json").write_text(
        json.dumps({**env_extra, "manifest": str(MANIFEST)}, indent=2) + "\n",
        encoding="utf-8",
    )
    log(f"=== campaign seed={seed} (run id {run_id}) ===")

    def remaining(cap: float = 300.0) -> float:
        left = deadline - time.monotonic()
        if left <= 0:
            raise RunnerError("global deadline exceeded")
        return min(left, cap)

    create_oc15_topics(seed, run_id, env_extra)
    _ACTIVE_ENVS.append(env_extra)
    compose("up", "-d", *OC15_SERVICES, timeout=remaining(), files=OC15_COMPOSE, env_extra=env_extra)

    hstree_topic = f"oc15-s{seed}-hstree-{run_id}"
    v2_viz_topic = f"oc15-s{seed}-v2-viz-{run_id}"
    v2_group = f"oc15-s{seed}-v2-consumer-{run_id}"

    try:
        log("waiting for the oc15 producer to finish")
        wait_until(lambda: producer_finished(env_extra), remaining(), "producer completion", interval=1.0)
        log(f"waiting for {expected_total} events in {v2_viz_topic}")
        wait_until(
            lambda: topic_message_count(v2_viz_topic, env_extra) >= expected_total,
            remaining(),
            f"{expected_total} events in {v2_viz_topic}",
        )
        log(f"waiting for a zero {v2_group} lag")
        wait_until(
            lambda: consumer_group_lag(v2_group, hstree_topic, env_extra) == 0,
            remaining(),
            "zero v2 consumer lag",
        )
        log("exporting the v2 JSON topic")
        export_path = run_dir / "oc15-v2-viz.jsonl"
        export_jsonl_topic(v2_viz_topic, export_path, expected_total, env_extra, remaining(120.0))
    finally:
        if not keep_up:
            log(f"stopping the oc15 services of seed {seed} (only oc15-* services)")
            stop_oc15_services(env_extra)
            if env_extra in _ACTIVE_ENVS:
                _ACTIVE_ENVS.remove(env_extra)
        else:
            log(f"keeping the oc15 services of seed {seed} up (--keep-up)")
    return run_dir / "oc15-v2-viz.jsonl"


# ---------------------------------------------------------------------------
# Versioned Markdown report (pure function, unit-testable)
# ---------------------------------------------------------------------------


def render_minimal_validation_markdown(
    campaign_run_id: str,
    manifest: Dict,
    validation: Dict,
    stability: Optional[Dict],
    reference_runs: Dict[str, str],
    secondary_validations: Optional[Dict[str, Dict]] = None,
) -> str:
    """Deterministic Markdown summary of the OC-15 minimal validation.

    Three clearly separated sections:
    - semantic and structural validation (per export, additivity vs score);
    - same-seed reproducibility between the two reference runs;
    - inter-seed stability MEASURED without any decision threshold.
    """
    lines = [
        "# OC-15 — Validation minimale du contenu explainability-v2",
        "",
        f"- run de campagne : `{campaign_run_id}`",
        f"- fixture : `{manifest['source_csv']}` (v{manifest['fixture_version']},"
        f" sha256 `{manifest['source_csv_sha256'][:16]}…`)",
        f"- groupes : {', '.join(manifest['groups'])}"
        f" | {manifest['expected_total_events']} événements"
        f" | {manifest['expected_warmup_events']} warmup"
        f" | {manifest['expected_shap_events']} éligibles SHAP",
        f"- seed de référence : {manifest['reference_seed']}"
        f" | seeds secondaires : {', '.join(str(s) for s in manifest['secondary_seeds'])}",
        f"- exécutions de référence : {json.dumps(reference_runs, sort_keys=True)}",
        "",
        "## 1. Validation sémantique et structurelle",
        "",
        f"- verdict global : **{validation['verdict']}**",
        f"- erreurs totales : {validation['error_count']}",
    ]
    for run_name in ("run_a", "run_b"):
        structure = validation["structural_validation"][run_name]
        lines.append(
            f"- {run_name} : {'valide' if structure['valid'] else 'INVALIDE'}"
            f" (couverture {structure['coverage']['observed_events']}/"
            f"{structure['coverage']['expected_total']},"
            f" doublons {len(structure['duplicates'])},"
            f" erreurs {structure['error_count']})"
        )
    additivity = validation["configuration"].get(
        "additivity_tolerance", validation["configuration"]["numeric_tolerances"]
    )
    lines.append(
        f"- additivité SHAP vérifiée contre local_hstree_score par export"
        f" (tolérance dédiée : abs {additivity['absolute_tolerance']},"
        f" rel {additivity['relative_tolerance']})"
    )
    if validation["errors_by_key"]:
        lines += ["", "Erreurs par clé (extrait) :"]
        for key, errors in list(validation["errors_by_key"].items())[:20]:
            for error in errors:
                lines.append(f"- `{key}` : {error['code']}")
    else:
        lines += [
            "",
            "Chaque export couvre exactement les clés attendues, avec des"
            " statuts, verdicts, structures SHAP et identités conformes au"
            " manifeste, et une additivité SHAP dans la tolérance dédiée.",
        ]

    lines += [
        "",
        "## 2. Reproductibilité même seed (runs de référence)",
        "",
        f"- verdict : **{validation['verdict']}**"
        f" ({validation['error_count']} erreur(s) au total,",
        "  dont les divergences de reproductibilité listées ci-dessus)",
        f"- statuts (run A) : {json.dumps(validation['status_counts']['run_a'], sort_keys=True)}",
        f"- verdicts (run A) : {json.dumps(validation['verdict_counts']['run_a'], sort_keys=True)}",
    ]

    if secondary_validations:
        lines += ["", "### Exports des seeds secondaires (validation structurelle individuelle)", ""]
        for export_name, structure in sorted(secondary_validations.items()):
            lines.append(
                f"- {export_name} : {'valide' if structure['valid'] else 'INVALIDE'}"
                f" (couverture {structure['coverage']['observed_events']}/"
                f"{structure['coverage']['expected_total']},"
                f" erreurs {structure['error_count']})"
            )
        lines += [
            "",
            "Les valeurs SHAP des seeds secondaires ne sont PAS comparées à la"
            " référence ici : seule la mesure exploratoire multi-seed le fait.",
        ]

    if stability is not None:
        lines += [
            "",
            "## 3. Stabilité inter-seed (mesure exploratoire, sans seuil de décision)",
            "",
            "| seed | événements comparés | Spearman moyen | Jaccard@1 moyen | Jaccard@2 moyen | sign-flip@1 moyen | écart relatif moyen | identité/statut/verdict conservés |",
            "|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
        for seed, data in stability["seeds"].items():
            aggregate = data["aggregate"]
            lines.append(
                f"| {seed} | {aggregate['computed_events_compared']}"
                f" | {aggregate['spearman_mean']}"
                f" | {aggregate.get('jaccard_at_1_mean')}"
                f" | {aggregate.get('jaccard_at_2_mean')}"
                f" | {aggregate['sign_flip_rate_top1_mean']}"
                f" | {aggregate['relative_mean_abs_diff_mean']}"
                f" | {'oui' if aggregate['identity_status_verdict_all_conserved'] else 'non'} |"
            )
        lines += ["", f"> {stability['disclaimer']}"]

    lines += [
        "",
        "Portée : validation minimale du CONTENU (sémantique, structure,"
        " couverture, reproductibilité exacte à configuration et seed"
        " identiques). Aucune optimisation de KernelSHAP n'est recommandée"
        " ici ; la stabilité inter-seed est rapportée sans seuil de rejet.",
    ]
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

_ACTIVE_ENVS: List[Dict[str, str]] = []


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--artifacts-dir", default=None, help="artifacts directory (default: /tmp/oc15-minimal-validation-<run-id>)")
    parser.add_argument("--keep-up", action="store_true", help="keep the oc15 services running after the run (success AND failure) for manual inspection")
    parser.add_argument("--global-timeout", type=float, default=1200.0, help="global deadline in seconds")
    args = parser.parse_args(argv)

    campaign_run_id = generate_run_id()
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    reference_seed = int(manifest["reference_seed"])
    secondary_seeds = [int(seed) for seed in manifest["secondary_seeds"]]

    artifacts = Path(args.artifacts_dir) if args.artifacts_dir else Path("/tmp") / f"oc15-minimal-validation-{campaign_run_id}"
    artifacts.mkdir(parents=True, exist_ok=True)
    (artifacts / "campaign-env.json").write_text(
        json.dumps({"campaign_run_id": campaign_run_id, "manifest": str(MANIFEST)}, indent=2) + "\n",
        encoding="utf-8",
    )
    log(f"campaign run id: {campaign_run_id} | artifacts: {artifacts}")

    deadline = time.monotonic() + args.global_timeout
    error_message: Optional[str] = None
    try:
        log("checking that the standard compute services are stopped")
        assert_standard_services_stopped()
        compose("up", "-d", *INFRA_SERVICES, timeout=min(300.0, max(60.0, deadline - time.monotonic())))

        reference_run_a = run_single_campaign(
            reference_seed, manifest, artifacts, deadline, args.keep_up
        )
        reference_run_b = run_single_campaign(
            reference_seed, manifest, artifacts, deadline, args.keep_up
        )
        reference_runs = {
            "run_a": str(reference_run_a.parent.name),
            "run_b": str(reference_run_b.parent.name),
        }

        # OC-15R4 step 2: validate each reference export individually BEFORE
        # the strict comparison.
        log("validating each reference export individually (structural)")
        structure_a = validate_export_or_fail(
            manifest, reference_run_a, "run_a", artifacts
        )
        structure_b = validate_export_or_fail(
            manifest, reference_run_b, "run_b", artifacts
        )
        (artifacts / "oc15-structural-reference.json").write_text(
            json.dumps({"run_a": structure_a, "run_b": structure_b}, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

        # OC-15R4 step 3: strict same-seed comparison of both references.
        log("running the pure minimal validator on both reference exports")
        validation_path = artifacts / "oc15-minimal-validation.json"
        result = run_command(
            [
                sys.executable, str(VALIDATOR),
                "--manifest", str(MANIFEST),
                "--run-a", str(reference_run_a),
                "--run-b", str(reference_run_b),
                "--output", str(validation_path),
            ],
            timeout=120.0,
            check=False,
            env_extra={"PYTHONPATH": "src:."},
        )
        if result.returncode != 0:
            raise RunnerError("minimal validation FAILED (see oc15-minimal-validation.json)")
        validation = json.loads(validation_path.read_text(encoding="utf-8"))
        log(f"validator verdict: {validation['verdict']} (errors={validation['error_count']})")

        log(f"running one campaign per secondary seed {secondary_seeds}")
        secondary_exports: Dict[int, Path] = {}
        for seed in secondary_seeds:
            secondary_exports[seed] = run_single_campaign(
                seed, manifest, artifacts, deadline, args.keep_up
            )

        # OC-15R4 step 5: validate each secondary export individually. The
        # SHAP values are NOT compared to the reference here: only the
        # exploratory multi-seed measurement does that afterwards.
        log("validating each secondary export individually (structural)")
        secondary_validations: Dict[str, Dict] = {}
        for seed, export in sorted(secondary_exports.items()):
            secondary_validations[f"seed{seed}"] = validate_export_or_fail(
                manifest, export, f"seed{seed}", artifacts
            )
        (artifacts / "oc15-structural-secondary.json").write_text(
            json.dumps(secondary_validations, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

        # OC-15R4 step 6: only now measure the inter-seed stability.
        log("measuring the exploratory seed stability against the reference")
        stability_path = artifacts / "oc15-seed-stability.json"
        stability_command = [
            sys.executable, str(SEED_STABILITY),
            "--manifest", str(MANIFEST),
            "--reference", str(reference_run_a),
            "--output", str(stability_path),
        ]
        for seed, export in secondary_exports.items():
            stability_command += ["--secondary", f"{seed}={export}"]
        run_command(stability_command, timeout=120.0, check=True, env_extra={"PYTHONPATH": "src:."})
        stability = json.loads(stability_path.read_text(encoding="utf-8"))

        markdown = render_minimal_validation_markdown(
            campaign_run_id, manifest, validation, stability, reference_runs,
            secondary_validations,
        )
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        versioned = REPORTS_DIR / f"oc15-minimal-validation-{campaign_run_id}.md"
        versioned.write_text(markdown, encoding="utf-8")
        (artifacts / "oc15-summary.json").write_text(
            json.dumps(
                {
                    "campaign_run_id": campaign_run_id,
                    "reference_runs": reference_runs,
                    "validation": validation,
                    "seed_stability": stability,
                },
                indent=2,
                sort_keys=True,
                ensure_ascii=False,
            )
            + "\n",
            encoding="utf-8",
        )
        log(f"artifacts: {artifacts}")
        log(f"versioned report: {versioned}")
    except RunnerError as exc:
        error_message = str(exc)
    except Exception as exc:  # the runner must never hang or fail silently
        error_message = f"unexpected failure: {exc}"
    finally:
        if error_message is not None:
            print(f"[oc15] FAILED: {error_message}", file=sys.stderr)
            # Diagnostics are written BEFORE the cleanup so a stopped service
            # can no longer report its state.
            for env_extra in _ACTIVE_ENVS:
                try:
                    dump_service_logs(env_extra, artifacts)
                except Exception as log_exc:
                    print(f"[oc15] could not dump service logs: {log_exc}", file=sys.stderr)
        if _ACTIVE_ENVS and not args.keep_up:
            # Default behavior: the oc15-* containers are cleaned up on
            # success AND on failure. A cleanup failure never masks the
            # campaign result above.
            for env_extra in list(_ACTIVE_ENVS):
                try:
                    log("stopping the remaining oc15 services (only oc15-* services)")
                    stop_oc15_services(env_extra)
                except Exception as cleanup_exc:
                    print(f"[oc15] WARNING: cleanup failed: {cleanup_exc}", file=sys.stderr)
        if error_message is not None:
            print(f"[oc15] artifacts kept for diagnosis: {artifacts}", file=sys.stderr)
            return 1

    log(f"OC-15 minimal validation PASSED — artifacts: {artifacts}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
