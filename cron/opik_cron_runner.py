from __future__ import annotations

import argparse
import importlib
import shutil
import subprocess
import sys
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from typing import Sequence

_BUILDERS = {
    "hourly": ("hermes-opik-hourly", "cron.opik_hourly_watch_builder"),
    "daily": ("hermes-opik-daily", "cron.opik_daily_digest_builder"),
}


def _load_runtime_env() -> None:
    """Load Hermes runtime env without shell-sourcing ~/.hermes/.env."""
    from hermes_cli.env_loader import load_hermes_dotenv

    project_env = Path(__file__).resolve().parent.parent / ".env"
    load_hermes_dotenv(project_env=project_env)


def _cronitor_binary() -> str | None:
    explicit = Path("/usr/local/bin/cronitor")
    if explicit.exists():
        return str(explicit)
    return shutil.which("cronitor")


def _ping(monitor_key: str, state: str, message: str | None = None) -> None:
    """Best-effort Cronitor ping.

    Report generation is more important than alert transport. If Cronitor is
    unavailable, write a compact warning to stderr and continue, except the
    original builder exception still propagates after a fail ping attempt.
    """
    binary = _cronitor_binary()
    if not binary:
        print(f"warning: cronitor binary not found for {monitor_key} {state}", file=sys.stderr)
        return

    args = [binary, "ping", monitor_key, f"--{state}"]
    if message:
        args.extend(["--msg", message[:240]])
    result = subprocess.run(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
    if result.returncode != 0:
        print(
            f"warning: cronitor ping failed for {monitor_key} {state} exit={result.returncode}",
            file=sys.stderr,
        )


def run_builder(kind: str) -> str:
    if kind not in _BUILDERS:
        raise ValueError(f"unknown Opik builder kind: {kind}")

    monitor_key, module_name = _BUILDERS[kind]
    _load_runtime_env()
    _ping(monitor_key, "run")

    buffer = StringIO()
    try:
        module = importlib.import_module(module_name)
        with redirect_stdout(buffer):
            module.main()
    except BaseException as exc:
        _ping(monitor_key, "fail", f"{type(exc).__name__}: {exc}")
        raise

    _ping(monitor_key, "complete")
    return buffer.getvalue()


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run deterministic Hermes Opik cron builders")
    parser.add_argument("kind", choices=sorted(_BUILDERS))
    args = parser.parse_args(list(argv) if argv is not None else None)
    print(run_builder(args.kind), end="")


if __name__ == "__main__":
    main()
