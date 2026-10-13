import os
from pathlib import Path
import re
import subprocess


ROOT = Path(__file__).resolve().parents[1]


def _compose_service_section(compose: str, service_name: str) -> str:
    marker = f"  {service_name}:"
    section = compose.split(marker, 1)[1]
    next_service = re.search(r"\n  [A-Za-z0-9_-]+:", section)
    if next_service is None:
        return section
    return section[: next_service.start()]


def test_start_script_detaches_backend_into_its_own_session() -> None:
    script = (ROOT / "start.sh").read_text(encoding="utf-8")

    assert "start_backend()" in script
    assert '"${ROOT_DIR}/scripts/web/spawn_backend.py"' in script
    assert '--pid-file "${BACKEND_PID_FILE}"' in script
    assert '--log-file "${BACKEND_LOG_FILE}"' in script


def test_start_script_honors_explicit_backend_python() -> None:
    script = (ROOT / "start.sh").read_text(encoding="utf-8")

    assert 'if [ -n "${ASKMYDASMA_PYTHON:-}" ]; then' in script
    assert '"${ASKMYDASMA_PYTHON}" -c "import fastapi, uvicorn"' in script
    assert 'echo "${ASKMYDASMA_PYTHON}"' in script


def test_start_script_does_not_wait_for_prepared_campaign_when_auto_prepare_is_disabled() -> None:
    script = (ROOT / "start.sh").read_text(encoding="utf-8")

    assert 'if [ "${AUTO_PREPARE_ON_START}" != "0" ]; then' in script
    wait_block = script.split('if [ "${AUTO_PREPARE_ON_START}" != "0" ]; then', 1)[1]
    assert "wait_for_platform_ready" in wait_block.split("fi", 1)[0]


def test_start_image_preparation_builds_local_compose_images() -> None:
    script = (ROOT / "scripts" / "docker" / "pull_missing_images.sh").read_text(
        encoding="utf-8"
    )

    assert "docker compose config --format json" in script
    assert 'has_build = "1" if "build" in service_config else "0"' in script
    assert 'if [ "${has_build}" = "1" ]; then' in script
    assert 'docker compose build "${service_name}"' in script
    assert 'docker pull "${image}"' in script


def test_start_image_preparation_builds_instead_of_pulling_local_images(
    tmp_path: Path,
) -> None:
    docker_bin = tmp_path / "docker"
    calls_log = tmp_path / "calls.log"
    built_marker = tmp_path / "built-local"
    pulled_marker = tmp_path / "pulled-remote"

    docker_bin.write_text(
        f"""#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' "$*" >> "{calls_log}"
args="$*"

if [ "$args" = "compose config --format json init-topic" ]; then
  cat <<'JSON'
{{
  "services": {{
    "init-topic": {{
      "build": {{"context": ".", "dockerfile": "services/kafka-tools/Dockerfile"}},
      "image": "dasma-kafka-kafka-tools:0.1.0"
    }},
    "schema-registry": {{
      "image": "ghcr.io/aiven/karapace:latest"
    }}
  }}
}}
JSON
  exit 0
fi

if [ "$1 $2" = "image inspect" ]; then
  if [ "$3" = "dasma-kafka-kafka-tools:0.1.0" ] && [ -f "{built_marker}" ]; then
    exit 0
  fi
  if [ "$3" = "ghcr.io/aiven/karapace:latest" ] && [ -f "{pulled_marker}" ]; then
    exit 0
  fi
  exit 1
fi

if [ "$args" = "compose build init-topic" ]; then
  touch "{built_marker}"
  exit 0
fi

if [ "$1" = "pull" ] && [ "$2" = "ghcr.io/aiven/karapace:latest" ]; then
  touch "{pulled_marker}"
  exit 0
fi

exit 2
""",
        encoding="utf-8",
    )
    docker_bin.chmod(0o755)

    env = os.environ.copy()
    env["PATH"] = f"{tmp_path}:{env['PATH']}"
    subprocess.run(
        ["bash", str(ROOT / "scripts" / "docker" / "pull_missing_images.sh"), "init-topic"],
        check=True,
        env=env,
        cwd=ROOT,
    )

    calls = calls_log.read_text(encoding="utf-8")
    assert "compose build init-topic" in calls
    assert "pull dasma-kafka-kafka-tools:0.1.0" not in calls
    assert "pull ghcr.io/aiven/karapace:latest" in calls


def test_backend_spawner_double_forks_and_execs_uvicorn() -> None:
    script = (ROOT / "scripts" / "web" / "spawn_backend.py").read_text(
        encoding="utf-8"
    )

    assert "os.fork()" in script
    assert "os.setsid()" in script
    assert 'pid_file.write_text(f"{os.getpid()}\\n"' in script
    assert '"uvicorn"' in script
    assert '"dasma_kafka.web.app:app"' in script


def test_deepant_service_uses_dedicated_prebuilt_torch_image() -> None:
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    deepant_section = compose.split("  deepant-analysis:", 1)[1].split(
        "  mdragstream-point-analysis:", 1
    )[0]

    assert "dockerfile: services/deepant/Dockerfile" in deepant_section
    assert "image: dasma-kafka-deepant:0.1.0" in deepant_section
    assert "python -m pip install" not in deepant_section
    assert (
        'entrypoint: ["python", "/app/runtime/detectors/deepant/deepant_analysis.py"]'
        in deepant_section
    )
    assert 'DASMA_ENABLED: "${DASMA_DEEPANT_ENABLED:-1}"' in deepant_section


def test_producer_compose_receives_selected_groups_filter() -> None:
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    producer_section = compose.split("  producer:", 1)[1].split("  hstree-analysis:", 1)[0]

    assert 'DASMA_SELECTED_GROUPS: "${DASMA_SELECTED_GROUPS:-}"' in producer_section


def test_kitnet_compose_receives_threshold_runtime_env() -> None:
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    kitnet_section = _compose_service_section(compose, "kitnet-analysis")

    assert 'KITNET_THRESHOLD_MODE: "${KITNET_THRESHOLD_MODE:-welford-windowed}"' in kitnet_section
    assert 'KITNET_THRESHOLD_WINDOW: "${KITNET_THRESHOLD_WINDOW:-500}"' in kitnet_section
    assert (
        'KITNET_THRESHOLD_PERCENTILE: "${KITNET_THRESHOLD_PERCENTILE:-99.0}"'
        in kitnet_section
    )
    assert (
        'KITNET_ANOMALY_WARMUP_WINDOW: "${KITNET_ANOMALY_WARMUP_WINDOW:-50}"'
        in kitnet_section
    )


def test_anomaly_light_services_use_prebuilt_dependency_image() -> None:
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

    for service_name, script_path in {
        "hstree-analysis": "/app/runtime/detectors/hstree/hstree_analysis.py",
        "kitnet-analysis": "/app/runtime/detectors/kitnet/kitnet_analysis_pysad_cpu.py",
    }.items():
        section = _compose_service_section(compose, service_name)
        assert "dockerfile: services/anomaly-light/Dockerfile" in section
        assert "image: dasma-kafka-anomaly-light:0.1.0" in section
        assert "python -m pip install" not in section
        assert f'entrypoint: ["python", "{script_path}"]' in section


def test_anomaly_light_image_pins_runtime_dependencies() -> None:
    dockerfile = (ROOT / "services" / "anomaly-light" / "Dockerfile").read_text(
        encoding="utf-8"
    )
    requirements = (
        ROOT / "services" / "anomaly-light" / "requirements.txt"
    ).read_text(encoding="utf-8")

    assert "python:3.12.13-slim-trixie" in dockerfile
    assert "pip==26.0.1" in dockerfile
    for dependency in (
        "confluent-kafka==2.14.0",
        "fastavro==1.12.1",
        "numpy==2.4.4",
        "pandas==3.0.2",
        "pysad==0.3.4",
    ):
        assert dependency in requirements
    assert "torch" not in requirements
    assert "shap" not in requirements


def test_kafka_tools_services_use_prebuilt_dependency_image() -> None:
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

    expected_entrypoints = {
        "init-topic": "python /app/runtime/bootstrap/init-topic/init-topics.py",
        "consensus-analysis": 'entrypoint: ["python", "/app/runtime/consensus/consensus_analysis.py"]',
        "build-metrics": 'entrypoint: ["python", "/app/observability/metrics/build-metrics.py"]',
    }

    for service_name, entrypoint_fragment in expected_entrypoints.items():
        section = _compose_service_section(compose, service_name)
        assert "dockerfile: services/kafka-tools/Dockerfile" in section
        assert "image: dasma-kafka-kafka-tools:0.1.0" in section
        assert "python -m pip install" not in section
        assert entrypoint_fragment in section


def test_init_topic_bootstrap_entrypoint_resolves_the_repository_src_directory() -> None:
    entrypoint = (
        ROOT / "runtime" / "bootstrap" / "init-topic" / "init-topics.py"
    ).read_text(encoding="utf-8")

    assert 'ROOT_DIR = Path(__file__).resolve().parents[3]' in entrypoint
    assert 'SRC_DIR = ROOT_DIR / "src"' in entrypoint


def test_consensus_compose_exposes_late_join_tuning_env() -> None:
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    section = _compose_service_section(compose, "consensus-analysis")

    assert 'CONSENSUS_TIMEOUT_SEC: "${CONSENSUS_TIMEOUT_SEC:-30}"' in section
    assert 'CONSENSUS_RETENTION_SEC: "${CONSENSUS_RETENTION_SEC:-300}"' in section
    assert 'CONSENSUS_LATE_POLICY: "${CONSENSUS_LATE_POLICY:-missing}"' in section


def test_kafka_tools_image_pins_runtime_dependencies() -> None:
    dockerfile = (ROOT / "services" / "kafka-tools" / "Dockerfile").read_text(
        encoding="utf-8"
    )
    requirements = (
        ROOT / "services" / "kafka-tools" / "requirements.txt"
    ).read_text(encoding="utf-8")

    assert "python:3.12.13-slim-trixie" in dockerfile
    assert "pip==26.0.1" in dockerfile
    assert "confluent-kafka==2.14.0" in requirements
    assert "fastavro" not in requirements
    assert "numpy" not in requirements
    assert "torch" not in requirements
    assert "shap" not in requirements


def test_anomaly_sklearn_services_use_prebuilt_dependency_image() -> None:
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

    for service_name, script_path in {
        "iforest-analysis": "/app/runtime/detectors/iforest/iforest_analysis.py",
        "mdragstream-point-analysis": "/app/runtime/detectors/mdragpoint/mdragstream_point_analysis.py",
    }.items():
        section = _compose_service_section(compose, service_name)
        assert "dockerfile: services/anomaly-sklearn/Dockerfile" in section
        assert "image: dasma-kafka-anomaly-sklearn:0.1.0" in section
        assert "python -m pip install" not in section
        assert f'entrypoint: ["python", "{script_path}"]' in section


def test_anomaly_sklearn_image_pins_runtime_dependencies() -> None:
    dockerfile = (ROOT / "services" / "anomaly-sklearn" / "Dockerfile").read_text(
        encoding="utf-8"
    )
    requirements = (
        ROOT / "services" / "anomaly-sklearn" / "requirements.txt"
    ).read_text(encoding="utf-8")

    assert "python:3.12.13-slim-trixie" in dockerfile
    assert "pip==26.0.1" in dockerfile
    for dependency in (
        "confluent-kafka==2.14.0",
        "fastavro==1.12.1",
        "numpy==2.4.4",
        "pandas==3.0.2",
        "pyod==2.0.5",
        "pysad==0.3.4",
        "scikit-learn==1.8.0",
    ):
        assert dependency in requirements
    assert "torch" not in requirements
    assert "shap" not in requirements


def test_explainability_services_use_prebuilt_dependency_image() -> None:
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

    for service_name, script_path in {
        "explainability": "/app/runtime/explainability/regular/explainability-multi-thread-by-group.py",
        "explainability-v2": "/app/runtime/explainability/v2/explainability_v2.py",
    }.items():
        section = _compose_service_section(compose, service_name)
        assert "dockerfile: services/explainability/Dockerfile" in section
        assert "image: dasma-kafka-explainability:0.1.0" in section
        assert "python -m pip install" not in section
        assert f'entrypoint: ["python", "{script_path}"]' in section


def test_explainability_image_pins_runtime_dependencies() -> None:
    dockerfile = (ROOT / "services" / "explainability" / "Dockerfile").read_text(
        encoding="utf-8"
    )
    requirements = (
        ROOT / "services" / "explainability" / "requirements.txt"
    ).read_text(encoding="utf-8")

    assert "python:3.12.13-slim-trixie" in dockerfile
    assert "pip==26.0.1" in dockerfile
    for dependency in (
        "confluent-kafka==2.14.0",
        "fastavro==1.12.1",
        "numpy==2.4.4",
        "pandas==3.0.2",
        "pysad==0.3.4",
        "scikit-learn==1.8.0",
        "shap==0.51.0",
    ):
        assert dependency in requirements
    assert "torch" not in requirements


def test_deepant_image_is_the_only_profile_with_torch() -> None:
    dockerfile = (ROOT / "services" / "deepant" / "Dockerfile").read_text(
        encoding="utf-8"
    )
    requirements = (ROOT / "services" / "deepant" / "requirements.txt").read_text(
        encoding="utf-8"
    )

    assert "python:3.12.13-slim-trixie" in dockerfile
    assert "pip==26.0.1" in dockerfile
    assert "torch==2.6.0" in dockerfile
    assert "https://download.pytorch.org/whl/cpu" in dockerfile
    for dependency in (
        "confluent-kafka==2.14.0",
        "fastavro==1.12.1",
        "numpy==2.4.4",
        "pandas==3.0.2",
    ):
        assert dependency in requirements
    assert "shap" not in requirements

    for relative_path in (
        "services/anomaly-light/requirements.txt",
        "services/anomaly-sklearn/requirements.txt",
        "services/explainability/requirements.txt",
        "services/kafka-tools/requirements.txt",
    ):
        assert "torch" not in (ROOT / relative_path).read_text(encoding="utf-8")


def test_compose_receives_all_welford_window_runtime_env_vars() -> None:
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    config = (ROOT / "scripts" / "web" / "campaign_config.py").read_text(
        encoding="utf-8"
    )
    expected_by_service = {
        "hstree-analysis": 'HSTREE_WELFORD_WINDOW: "${HSTREE_WELFORD_WINDOW:-2048}"',
        "iforest-analysis": 'IFOREST_WELFORD_WINDOW: "${IFOREST_WELFORD_WINDOW:-2048}"',
        "kitnet-analysis": 'KITNET_WELFORD_WINDOW: "${KITNET_WELFORD_WINDOW:-500}"',
        "deepant-analysis": 'DEEPANT_WELFORD_WINDOW: "${DEEPANT_WELFORD_WINDOW:-2048}"',
        "mdragstream-point-analysis": 'MDRAGPOINT_WELFORD_WINDOW: "${MDRAGPOINT_WELFORD_WINDOW:-900}"',
    }

    for service_name, compose_env in expected_by_service.items():
        env_name = compose_env.split(":", 1)[0]
        assert f'"{env_name}": str(config[' in config
        assert compose_env in _compose_service_section(compose, service_name)


def test_askmydasma_service_uses_prebuilt_image_behind_profile() -> None:
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    section = _compose_service_section(compose, "askmydasma-web")

    assert "dockerfile: services/askmydasma/Dockerfile" in section
    assert "image: dasma-kafka-askmydasma:0.1.0" in section
    assert "profiles:" in section
    assert "askmydasma-container" in section
    assert "/var/run/docker.sock" in section
    assert "ASKMYDASMA_PROJECT_ROOT" in section
    assert "8000:8000" in section


def test_askmydasma_dockerfile_installs_docker_cli_and_web_deps() -> None:
    dockerfile = (ROOT / "services" / "askmydasma" / "Dockerfile").read_text(encoding="utf-8")

    assert "python:3.12.13-slim-trixie" in dockerfile
    assert "pip==26.0.1" in dockerfile
    assert "docker-ce-cli" in dockerfile
    assert "docker-compose-plugin" in dockerfile
    assert '".[web]"' in dockerfile
    assert "uvicorn" in dockerfile
    assert "8000" in dockerfile


def test_deepant_compose_exposes_parallel_by_group_knobs() -> None:
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    section = _compose_service_section(compose, "deepant-analysis")

    # Parallel-by-group is the default after E2E validation (2026-06-04).
    assert 'DEEPANT_PARALLEL_BY_GROUP: "${DEEPANT_PARALLEL_BY_GROUP:-1}"' in section
    assert 'DEEPANT_MAX_GROUP_WORKERS: "${DEEPANT_MAX_GROUP_WORKERS:-2}"' in section
    assert 'DEEPANT_GROUP_QUEUE_MAXSIZE: "${DEEPANT_GROUP_QUEUE_MAXSIZE:-1000}"' in section
    assert 'DEEPANT_CPU_THREADS_PER_WORKER: "${DEEPANT_CPU_THREADS_PER_WORKER:-1}"' in section


def test_deepant_python_kill_switch_runs_before_heavy_imports() -> None:
    script = (
        ROOT / "runtime" / "detectors" / "deepant" / "deepant_analysis.py"
    ).read_text(
        encoding="utf-8"
    )

    kill_switch_index = script.index('os.getenv("DASMA_ENABLED", "1") == "0"')

    assert kill_switch_index < script.index("import numpy")
    assert kill_switch_index < script.index("import torch")
