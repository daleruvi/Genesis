import os
import subprocess
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"


def run_settings_snippet(code: str, extra_env: dict[str, str] | None = None):
    env = os.environ.copy()
    env["PYTHONPATH"] = str(SRC_DIR)
    env.pop("GENESIS_PROFILE", None)
    env.pop("GENESIS_ALLOW_LIVE", None)
    if extra_env:
        env.update(extra_env)
    return subprocess.run(
        [sys.executable, "-c", code],
        cwd=PROJECT_ROOT,
        env=env,
        text=True,
        capture_output=True,
    )


class SettingsProfileTest(unittest.TestCase):
    def test_default_profile_is_research(self):
        result = run_settings_snippet(
            "from genesis.config.settings import GENESIS_PROFILE; print(GENESIS_PROFILE)"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "research")

    def test_invalid_profile_fails_with_clear_error(self):
        result = run_settings_snippet(
            "import genesis.config.settings",
            {"GENESIS_PROFILE": "paper"},
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Invalid GENESIS_PROFILE", result.stderr)

    def test_live_is_not_authorized_without_allow_flag(self):
        result = run_settings_snippet(
            "from genesis.config.settings import live_trading_enabled; print(live_trading_enabled())",
            {"GENESIS_PROFILE": "live", "GENESIS_ALLOW_LIVE": "false"},
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "False")


if __name__ == "__main__":
    unittest.main()
