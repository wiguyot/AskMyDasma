"""Thin wrapper around the `docker compose` CLI."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path


class ComposeError(Exception):
    """Raised when a docker compose command exits with a non-zero status."""

    def __init__(self, args: list[str], returncode: int, stderr: str) -> None:
        self.args_ = args
        self.returncode = returncode
        self.stderr = stderr
        super().__init__(f"docker compose {args[2:]} failed (exit {returncode}): {stderr.strip()}")


class ComposeRunner:
    """Run `docker compose` commands from a fixed project root.

    All commands use --pull never at runtime so that only pre-pulled or
    pre-built images are used. The caller is responsible for ensuring images
    are available before invoking start commands.
    """

    def __init__(self, project_root: Path, host_project_root: Path | None = None) -> None:
        self.project_root = project_root
        # host_project_root is the real on-disk path Docker Desktop can access.
        # In container mode project_root=/project but host paths differ, so
        # --project-directory must point to the host path so that volume mounts
        # in docker-compose.yml resolve correctly.
        self._host_project_root = host_project_root or project_root
        # When host_project_root differs from project_root we are running inside
        # the orchestrator container against the host Docker socket. In that case
        # `docker compose build` cannot work: there is no buildx plugin and the
        # build context resolves to the host path (--project-directory), which is
        # absent inside the container. Building is the host's job (start.sh).
        self._in_container = self._host_project_root != self.project_root
        # The compose file is read from the in-container path (project_root),
        # while --project-directory uses the host path. Without an explicit -f,
        # docker compose would look for the compose file under --project-directory,
        # which does not exist inside the container.
        self._compose_file = project_root / "docker-compose.yml"

    def _run(
        self,
        *args: str,
        check: bool = True,
        env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        cmd = [
            "docker", "compose",
            "-f", str(self._compose_file),
            "--project-directory", str(self._host_project_root),
            *args,
        ]
        # Compose interpolates ${VAR:-default} from the process environment.
        # When env is given, overlay it on top of os.environ so campaign
        # runtime variables (DASMA_*_ENABLED, thresholds, etc.) reach the
        # docker compose invocation while PATH/COMPOSE_PROJECT_NAME survive.
        run_env = {**os.environ, **env} if env else None
        result = subprocess.run(
            cmd,
            cwd=self.project_root,
            capture_output=True,
            text=True,
            env=run_env,
        )
        if check and result.returncode != 0:
            raise ComposeError(cmd, result.returncode, result.stderr)
        return result

    def up(
        self,
        *services: str,
        force_recreate: bool = False,
        wait: bool = False,
        wait_timeout: int | None = None,
        env: dict[str, str] | None = None,
    ) -> None:
        args = ["up", "--pull", "never", "-d"]
        if force_recreate:
            args.append("--force-recreate")
        if wait:
            args.append("--wait")
        if wait_timeout is not None:
            args += ["--wait-timeout", str(wait_timeout)]
        args += list(services)
        self._run(*args, env=env)

    def stop(self, *services: str) -> None:
        self._run("stop", *services, check=False)

    def rm(self, *services: str) -> None:
        self._run("rm", "-f", *services, check=False)

    def exec(self, service: str, *cmd: str, no_tty: bool = True) -> subprocess.CompletedProcess[str]:
        args = ["exec"]
        if no_tty:
            args.append("-T")
        args.append(service)
        args += list(cmd)
        return self._run(*args)

    def ps(self) -> str:
        return self._run("ps", "--format", "json").stdout

    def pull_missing(self, *services: str, env: dict[str, str] | None = None) -> None:
        """Pull or build each service image only if not already present locally.

        When env is given it overlays the process environment for the internal
        ``docker compose config`` and ``docker compose build`` calls, so image
        and entrypoint interpolations (e.g. REPEATABILITY_IMAGE) resolve
        against the campaign environment. Callers without env keep the legacy
        behaviour of resolving against os.environ only.
        """
        for service in services:
            config = self._run("config", "--format", "json", service, check=False, env=env)
            if config.returncode != 0:
                continue
            import json
            try:
                svc_config = json.loads(config.stdout).get("services", {}).get(service, {})
            except (json.JSONDecodeError, AttributeError):
                continue
            if "build" in svc_config:
                image = svc_config.get("image", "")
                inspect = subprocess.run(["docker", "image", "inspect", image], capture_output=True)
                if inspect.returncode != 0:
                    if self._in_container:
                        raise ComposeError(
                            ["docker", "compose", "build", service],
                            1,
                            f"image '{image}' for service '{service}' is missing and the "
                            f"orchestrator container cannot build images (no buildx, build "
                            f"context unavailable). Build it on the host first, e.g. run "
                            f"./start.sh or scripts/docker/pull_missing_images.sh.",
                        )
                    self._run("build", service, env=env)
            else:
                image = svc_config.get("image", "")
                if image:
                    inspect = subprocess.run(["docker", "image", "inspect", image], capture_output=True)
                    if inspect.returncode != 0:
                        subprocess.run(["docker", "pull", image], check=True)
