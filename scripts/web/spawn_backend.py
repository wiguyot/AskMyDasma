#!/usr/bin/env python3
"""Start the AskMyDasma FastAPI backend as a detached daemon process."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root-dir", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", required=True)
    parser.add_argument("--python", required=True)
    parser.add_argument("--pid-file", required=True)
    parser.add_argument("--log-file", required=True)
    return parser.parse_args()


def fork_and_exit_parent() -> None:
    pid = os.fork()
    if pid > 0:
        _, status = os.waitpid(pid, 0)
        if status != 0:
            raise SystemExit(os.waitstatus_to_exitcode(status))
        raise SystemExit(0)


def detach_from_session() -> None:
    os.setsid()
    pid = os.fork()
    if pid > 0:
        os._exit(0)


def redirect_stdio(log_file: Path) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    stdin = os.open(os.devnull, os.O_RDONLY)
    stdout = os.open(log_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    os.dup2(stdin, 0)
    os.dup2(stdout, 1)
    os.dup2(stdout, 2)
    os.close(stdin)
    os.close(stdout)


def main() -> None:
    args = parse_args()
    root_dir = Path(args.root_dir).resolve()
    pid_file = Path(args.pid_file)
    log_file = Path(args.log_file)

    fork_and_exit_parent()
    detach_from_session()

    os.chdir(root_dir)
    redirect_stdio(log_file)
    pid_file.parent.mkdir(parents=True, exist_ok=True)
    pid_file.write_text(f"{os.getpid()}\n", encoding="utf-8")

    env = os.environ.copy()
    env["ASKMYDASMA_HOST"] = args.host
    env["ASKMYDASMA_PORT"] = args.port
    env["ASKMYDASMA_PYTHON"] = args.python

    command = [
        args.python,
        "-m",
        "uvicorn",
        "--app-dir",
        "src",
        "--host",
        args.host,
        "--port",
        args.port,
        "dasma_kafka.web.app:app",
    ]
    os.execvpe(args.python, command, env)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[spawn-backend] failed: {exc}", file=sys.stderr, flush=True)
        raise
