"""Exercise shutdown with a fake Docker CLI; no real resources are changed."""

import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]

FAKE_DOCKER = r'''#!/usr/bin/env python3
import json
import os
import sys

args = sys.argv[1:]
with open(os.environ["MOCK_DOCKER_LOG"], "a") as handle:
    handle.write(json.dumps(args) + "\n")
project = os.environ.get("COMPOSE_PROJECT_NAME", "askmydasma")
if args[0] == "compose":
    if "config" in args:
        if os.environ.get("MOCK_CONFIG_FAIL"):
            sys.exit(2)
        print(json.dumps({"name": project}))
    else:
        assert args[args.index("--project-name") + 1] == project
elif args[:2] == ["volume", "ls"]:
    if os.environ.get("MOCK_NO_PROJECT_VOLUMES"):
        print("other-project-data")
        sys.exit(0)
    print("\n".join([
        project + "_influxdb-data",
        project + "_pip-cache",
        project + "_foreign-volume",
        "other-project-data",
        "unlabeled-data",
        "pip-cache",
    ]))
elif args[:2] == ["volume", "inspect"]:
    print(project if args[2] == project + "_influxdb-data" else "other-project")
elif args[:2] == ["volume", "rm"]:
    assert args[2:] == [project + "_influxdb-data"]
elif args == ["inspect", "askmydasma-web"]:
    pass
else:
    raise SystemExit("Unexpected Docker mutation: " + repr(args))
'''


def run_stop(tmp_path: Path, overrides: dict[str, str]):
    project = tmp_path / "AskMyDasma"
    project.mkdir()
    shutil.copy2(ROOT / "stop.sh", project / "stop.sh")
    (project / "docker-compose.yml").write_text("services: {}\n")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    docker = bin_dir / "docker"
    docker.write_text(FAKE_DOCKER)
    docker.chmod(0o755)
    log = tmp_path / "docker-calls.jsonl"
    env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}",
           "MOCK_DOCKER_LOG": str(log)}
    env.pop("COMPOSE_PROJECT_NAME", None)
    env.update(overrides)
    result = subprocess.run(["bash", str(project / "stop.sh")],
                            env=env, capture_output=True, text=True, timeout=10)
    calls = [json.loads(line) for line in log.read_text().splitlines()]
    return result, calls


@pytest.mark.parametrize("project_name", [None, "custom-project"])
def test_stop_uses_resolved_project_and_removes_only_its_volumes(tmp_path, project_name):
    overrides = {"COMPOSE_PROJECT_NAME": project_name} if project_name else {}
    result, calls = run_stop(tmp_path, overrides)
    assert result.returncode == 0, result.stderr
    expected = project_name or "askmydasma"
    compose_calls = [call for call in calls if call[0] == "compose" and "config" not in call]
    assert len(compose_calls) == 3  # stop backend container, remove it, then down
    assert all(call[call.index("--project-name") + 1] == expected for call in compose_calls)
    assert [call for call in calls if call[:2] == ["volume", "rm"]] == [
        ["volume", "rm", expected + "_influxdb-data"]
    ]
    assert not any("prune" in call for call in calls)


def test_invalid_compose_config_fails_before_shutdown(tmp_path):
    result, calls = run_stop(tmp_path, {"MOCK_CONFIG_FAIL": "1"})
    assert result.returncode != 0
    assert len(calls) == 1
    assert calls[0][0] == "compose" and "config" in calls[0]


def test_stop_with_no_project_volumes_leaves_other_volumes_alone(tmp_path):
    result, calls = run_stop(tmp_path, {"MOCK_NO_PROJECT_VOLUMES": "1"})
    assert result.returncode == 0, result.stderr
    assert "No labeled project volume" in result.stdout
    assert not any(call[:2] == ["volume", "rm"] or "prune" in call for call in calls)
