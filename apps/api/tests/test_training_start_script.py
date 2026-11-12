"""The API entrypoint bounds uvicorn's graceful shutdown.

PR 410 review M2 (remainder): on SIGTERM uvicorn waits for in-flight
requests before it runs the lifespan teardown, so an admin
``/training-agent/run/sync`` or ``/copilot/train`` request kept the
TrainingRuntime's stop event unset until the run ended. The teardown
(which stops the run) is reached within ``--timeout-graceful-shutdown``.
PR 406 sets the same flag and default (10 s); this test matches its
``test_start_script_bounds_the_graceful_shutdown``.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

START_API = Path(__file__).resolve().parents[3] / "scripts" / "start-api.sh"


def _start_api_argv(env: dict[str, str] | None = None) -> list[str]:
    """The uvicorn command line scripts/start-api.sh runs (python3 replaced by an echo)."""
    shim = Path(os.environ.get("TMPDIR") or "/tmp") / f"start-api-shim-{os.getpid()}"
    shim.mkdir(parents=True, exist_ok=True)
    (shim / "python3").write_text('#!/bin/sh\nfor a in "$@"; do printf "%s\\n" "$a"; done\n')
    (shim / "python3").chmod(0o755)
    try:
        out = subprocess.run(
            ["sh", str(START_API)],
            env={**os.environ, **(env or {}), "PATH": f"{shim}:{os.environ.get('PATH', '')}", "PORT": "8000"},
            capture_output=True, text=True, timeout=10, check=True,
        ).stdout
    finally:
        (shim / "python3").unlink()
        shim.rmdir()
    return out.splitlines()


def _flag(argv: list[str], name: str) -> str | None:
    return argv[argv.index(name) + 1] if name in argv else None


def test_start_script_bounds_the_graceful_shutdown(monkeypatch):
    monkeypatch.delenv("DATAWRAP_GRACEFUL_SHUTDOWN_SECONDS", raising=False)
    monkeypatch.delenv("DATAFLOW_GRACEFUL_SHUTDOWN_SECONDS", raising=False)
    argv = _start_api_argv()
    assert argv[:3] == ["-m", "uvicorn", "src.main:app"]
    assert _flag(argv, "--timeout-graceful-shutdown") == "10"
    overridden = _start_api_argv({"DATAWRAP_GRACEFUL_SHUTDOWN_SECONDS": "7"})
    assert _flag(overridden, "--timeout-graceful-shutdown") == "7"
