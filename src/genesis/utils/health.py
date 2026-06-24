from __future__ import annotations

import importlib
import sys
from dataclasses import dataclass
from pathlib import Path

from genesis.config import settings


@dataclass(frozen=True)
class HealthCheck:
    status: str
    name: str
    detail: str


class HealthReport:
    def __init__(self, checks: list[HealthCheck]):
        self.checks = checks

    @property
    def has_failures(self) -> bool:
        return any(check.status == "FAIL" for check in self.checks)

    @property
    def exit_code(self) -> int:
        return 1 if self.has_failures else 0

    def render_text(self) -> str:
        lines = ["GENESIS health check"]
        lines.append(f"Profile: {settings.GENESIS_PROFILE}")
        lines.append(f"Trading style: {settings.TRADING_STYLE}")
        lines.append("")
        for check in self.checks:
            lines.append(f"[{check.status}] {check.name}: {check.detail}")
        return "\n".join(lines)


def _check_import(module_name: str, fail_when_missing: bool = True) -> HealthCheck:
    try:
        importlib.import_module(module_name)
    except Exception as exc:
        status = "FAIL" if fail_when_missing else "WARN"
        return HealthCheck(status, f"Import {module_name}", str(exc))
    return HealthCheck("OK", f"Import {module_name}", "available")


def _check_path(path: Path, name: str, require_non_empty: bool = False) -> HealthCheck:
    if not path.exists():
        return HealthCheck("FAIL", name, f"missing: {path}")
    if require_non_empty and not any(path.iterdir()):
        return HealthCheck("WARN", name, f"empty: {path}")
    return HealthCheck("OK", name, str(path))


def _check_writable_dir(path: Path, name: str) -> HealthCheck:
    if not path.exists():
        return HealthCheck("FAIL", name, f"missing: {path}")
    probe = path / ".genesis_healthcheck.tmp"
    try:
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
    except Exception as exc:
        return HealthCheck("FAIL", name, f"not writable: {path} ({exc})")
    return HealthCheck("OK", name, f"writable: {path}")


def _check_python_version() -> HealthCheck:
    current = sys.version_info
    if current < (3, 12):
        return HealthCheck("FAIL", "Python version", f"requires >=3.12, found {current.major}.{current.minor}")
    return HealthCheck("OK", "Python version", f"{current.major}.{current.minor}.{current.micro}")


def _check_profile() -> HealthCheck:
    profile = settings.GENESIS_PROFILE
    if profile == "live":
        missing = []
        if not settings.GENESIS_ALLOW_LIVE:
            missing.append("GENESIS_ALLOW_LIVE=true")
        if not settings.BINGX_API_KEY:
            missing.append("BINGX_API_KEY")
        if not settings.BINGX_SECRET:
            missing.append("BINGX_SECRET")
        if missing:
            return HealthCheck("FAIL", "Live profile safety", "missing " + ", ".join(missing))
    return HealthCheck("OK", "Profile configuration", f"GENESIS_PROFILE={profile}")


def _check_raw_data() -> HealthCheck:
    files = sorted(settings.RAW_DATA_DIR.glob("*.parquet"))
    if not files:
        return HealthCheck("WARN", "Raw market data", f"no parquet files in {settings.RAW_DATA_DIR}")
    return HealthCheck("OK", "Raw market data", f"{len(files)} parquet file(s) found")


def _artifact_status_for_profile() -> str:
    return "FAIL" if settings.GENESIS_PROFILE in {"demo", "live"} else "WARN"


def _check_alpha_artifacts() -> list[HealthCheck]:
    required = [
        settings.ALPHA_STORE_DIR / "selected_alpha_rankings.csv",
        settings.ALPHA_STORE_DIR / "alpha_regime_performance.csv",
    ]
    missing = [path for path in required if not path.exists()]
    if not missing:
        return [HealthCheck("OK", "Demo alpha artifacts", "required files found")]
    detail = "missing: " + ", ".join(str(path) for path in missing)
    return [HealthCheck(_artifact_status_for_profile(), "Demo alpha artifacts", detail)]


def run_health_checks() -> HealthReport:
    checks: list[HealthCheck] = [
        _check_python_version(),
        _check_import("pandas"),
        _check_import("pyarrow"),
        _check_import("yaml"),
        _check_import("genesis"),
        _check_import("bingx", fail_when_missing=settings.GENESIS_PROFILE in {"demo", "live"}),
        _check_path(settings.ENV_FILE, ".env"),
        _check_path(settings.DATA_DIR, "Data directory"),
        _check_writable_dir(settings.DATA_DIR, "Data directory writable"),
        _check_path(settings.RAW_DATA_DIR, "Raw data directory", require_non_empty=True),
        _check_path(settings.ALPHA_STORE_DIR, "Alpha store directory"),
        _check_profile(),
        _check_raw_data(),
    ]
    checks.extend(_check_alpha_artifacts())
    return HealthReport(checks)
