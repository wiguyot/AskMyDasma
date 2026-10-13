import subprocess
import unittest
from pathlib import Path
from unittest.mock import MagicMock, call, patch

from src.dasma_kafka.web.orchestration.compose_runner import ComposeError, ComposeRunner

ROOT = Path("/fake/project")


def _completed(returncode: int = 0, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=[], returncode=returncode, stdout=stdout, stderr=stderr)


class ComposeRunnerUpTest(unittest.TestCase):
    @patch("subprocess.run")
    def test_up_builds_correct_command(self, mock_run: MagicMock) -> None:
        mock_run.return_value = _completed()
        ComposeRunner(ROOT).up("kafka1", "kafka2")
        mock_run.assert_called_once_with(
            ["docker", "compose", "-f", str(ROOT / "docker-compose.yml"),
             "--project-directory", str(ROOT),
             "up", "--pull", "never", "-d", "kafka1", "kafka2"],
            cwd=ROOT,  # project_root (in-container view); host_project_root goes to --project-directory
            capture_output=True,
            text=True,
            env=None,
        )

    @patch("subprocess.run")
    def test_up_force_recreate(self, mock_run: MagicMock) -> None:
        mock_run.return_value = _completed()
        ComposeRunner(ROOT).up("producer", force_recreate=True)
        cmd = mock_run.call_args[0][0]
        self.assertIn("--force-recreate", cmd)

    @patch("subprocess.run")
    def test_up_overlays_env_on_os_environ(self, mock_run: MagicMock) -> None:
        mock_run.return_value = _completed()
        ComposeRunner(ROOT).up("producer", env={"DASMA_MDRAGPOINT_ENABLED": "1"})
        passed_env = mock_run.call_args.kwargs["env"]
        self.assertEqual(passed_env["DASMA_MDRAGPOINT_ENABLED"], "1")
        # os.environ keys must survive the overlay (PATH is always present).
        self.assertIn("PATH", passed_env)

    @patch("subprocess.run")
    def test_up_wait_with_timeout(self, mock_run: MagicMock) -> None:
        mock_run.return_value = _completed()
        ComposeRunner(ROOT).up("kafka1", wait=True, wait_timeout=120)
        cmd = mock_run.call_args[0][0]
        self.assertIn("--wait", cmd)
        self.assertIn("--wait-timeout", cmd)
        self.assertIn("120", cmd)

    @patch("subprocess.run")
    def test_up_raises_compose_error_on_failure(self, mock_run: MagicMock) -> None:
        mock_run.return_value = _completed(returncode=1, stderr="service not found")
        with self.assertRaises(ComposeError) as ctx:
            ComposeRunner(ROOT).up("missing-service")
        self.assertEqual(ctx.exception.returncode, 1)
        self.assertIn("service not found", ctx.exception.stderr)


class ComposeRunnerStopRmTest(unittest.TestCase):
    @patch("subprocess.run")
    def test_stop_does_not_raise_on_failure(self, mock_run: MagicMock) -> None:
        mock_run.return_value = _completed(returncode=1, stderr="not running")
        ComposeRunner(ROOT).stop("producer")  # must not raise

    @patch("subprocess.run")
    def test_rm_passes_force_flag(self, mock_run: MagicMock) -> None:
        mock_run.return_value = _completed()
        ComposeRunner(ROOT).rm("producer", "hstree-analysis")
        cmd = mock_run.call_args[0][0]
        self.assertIn("-f", cmd)
        self.assertIn("producer", cmd)
        self.assertIn("hstree-analysis", cmd)


class ComposeRunnerExecTest(unittest.TestCase):
    @patch("subprocess.run")
    def test_exec_no_tty_by_default(self, mock_run: MagicMock) -> None:
        mock_run.return_value = _completed()
        ComposeRunner(ROOT).exec("kafka1", "bash", "-lc", "echo hi")
        cmd = mock_run.call_args[0][0]
        self.assertEqual(
            cmd[:8],
            ["docker", "compose", "-f", str(ROOT / "docker-compose.yml"),
             "--project-directory", str(ROOT), "exec", "-T"],
        )
        self.assertIn("kafka1", cmd)
        self.assertIn("echo hi", cmd)

    @patch("subprocess.run")
    def test_exec_raises_on_failure(self, mock_run: MagicMock) -> None:
        mock_run.return_value = _completed(returncode=1, stderr="exec failed")
        with self.assertRaises(ComposeError):
            ComposeRunner(ROOT).exec("producer", "sh", "-c", "touch /signal")


class ComposeRunnerPullMissingTest(unittest.TestCase):
    _CONFIG = '{"services": {"hstree-analysis": {"image": "img:1", "build": {"context": "."}}}}'

    def _side_effect(self):
        def run(cmd, *args, **kwargs):
            if "config" in cmd:
                return _completed(stdout=self._CONFIG)
            if cmd[:3] == ["docker", "image", "inspect"]:
                return _completed(returncode=1)  # image missing
            return _completed()  # build succeeds
        return run

    @patch("subprocess.run")
    def test_container_mode_refuses_to_build(self, mock_run: MagicMock) -> None:
        mock_run.side_effect = self._side_effect()
        runner = ComposeRunner(ROOT, host_project_root=Path("/host/project"))
        with self.assertRaises(ComposeError) as ctx:
            runner.pull_missing("hstree-analysis")
        self.assertIn("cannot build images", str(ctx.exception))
        # No `build` subcommand should have been dispatched.
        self.assertFalse(
            any("build" in c.args[0] for c in mock_run.call_args_list if c.args)
        )

    @patch("subprocess.run")
    def test_host_mode_builds_missing_image(self, mock_run: MagicMock) -> None:
        mock_run.side_effect = self._side_effect()
        ComposeRunner(ROOT).pull_missing("hstree-analysis")
        self.assertTrue(
            any(
                c.args and "build" in c.args[0] and "hstree-analysis" in c.args[0]
                for c in mock_run.call_args_list
            )
        )

    @patch("subprocess.run")
    def test_pull_missing_passes_env_to_compose_config(self, mock_run: MagicMock) -> None:
        env = {"REPEATABILITY_IMAGE": "dasma-kafka-anomaly-sklearn:0.1.0"}
        mock_run.return_value = _completed(returncode=1)  # config fails -> service skipped
        ComposeRunner(ROOT).pull_missing("repeatability-v1", env=env)
        first_call = mock_run.call_args_list[0]
        self.assertIn("config", first_call.args[0])
        self.assertEqual(
            first_call.kwargs["env"]["REPEATABILITY_IMAGE"],
            "dasma-kafka-anomaly-sklearn:0.1.0",
        )

    @patch("subprocess.run")
    def test_pull_missing_without_env_keeps_legacy_behaviour(self, mock_run: MagicMock) -> None:
        mock_run.return_value = _completed(returncode=1)
        ComposeRunner(ROOT).pull_missing("init-topic")
        self.assertIsNone(mock_run.call_args_list[0].kwargs["env"])

    @patch("subprocess.run")
    def test_pull_missing_builds_with_campaign_env(self, mock_run: MagicMock) -> None:
        import json as _json

        config = _json.dumps({
            "services": {
                "repeatability-v1": {
                    "image": "dasma-kafka-anomaly-sklearn:0.1.0",
                    "build": {"context": "."},
                }
            }
        })

        def run(cmd, *args, **kwargs):
            if "config" in cmd:
                return _completed(stdout=config)
            if cmd[:3] == ["docker", "image", "inspect"]:
                return _completed(returncode=1)  # image missing
            return _completed()  # build succeeds

        mock_run.side_effect = run
        env = {"REPEATABILITY_IMAGE": "dasma-kafka-anomaly-sklearn:0.1.0"}
        ComposeRunner(ROOT).pull_missing("repeatability-v1", env=env)

        build_calls = [c for c in mock_run.call_args_list if c.args and "build" in c.args[0]]
        self.assertEqual(len(build_calls), 1)
        self.assertEqual(build_calls[0].kwargs["env"]["REPEATABILITY_IMAGE"], env["REPEATABILITY_IMAGE"])


class ComposeRunnerPsTest(unittest.TestCase):
    @patch("subprocess.run")
    def test_ps_returns_stdout(self, mock_run: MagicMock) -> None:
        mock_run.return_value = _completed(stdout='[{"Service":"kafka1"}]')
        result = ComposeRunner(ROOT).ps()
        self.assertEqual(result, '[{"Service":"kafka1"}]')
        cmd = mock_run.call_args[0][0]
        self.assertIn("--format", cmd)
        self.assertIn("json", cmd)
