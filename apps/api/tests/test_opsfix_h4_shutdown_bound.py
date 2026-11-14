"""Ops review High 4: an open job stream must not stop the API from shutting down.

uvicorn runs the lifespan teardown (where the API stops and joins its
background work) only after every connection has closed. The job progress
stream stays open for hours, so with one viewer the pod ignored SIGTERM until
Kubernetes killed it and the teardown never ran. These tests start the API
the way ``scripts/start-api.sh`` does, open the real job stream, and send
SIGTERM.
"""

from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

API_ROOT = Path(__file__).resolve().parents[1]
REPO = API_ROOT.parents[1]
START_API = REPO / "scripts" / "start-api.sh"

APP = r'''
import importlib
from contextlib import asynccontextmanager

from fastapi import FastAPI

connectors_router = importlib.import_module("src.routers.connectors_router")


class _Store:
    def get_job(self, job_id):
        return {"_id": job_id, "job_id": job_id, "status": "running", "workspace_id": ""}


connectors_router.get_mongodb_service = lambda: _Store()
connectors_router._can_access_job = lambda _request, _job: True


@asynccontextmanager
async def lifespan(app):
    try:  # what src.main's lifespan does at startup
        from services import shutdown_signal
    except ImportError:
        shutdown_signal = None
    if shutdown_signal is not None:
        shutdown_signal.install()
    yield
    print("TEARDOWN_RAN", flush=True)


app = FastAPI(lifespan=lifespan)
app.include_router(connectors_router.router, prefix="/api/v1")
'''


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


def test_start_script_bounds_the_graceful_shutdown():
    argv = _start_api_argv()
    assert _flag(argv, "--timeout-graceful-shutdown") == "10"
    overridden = _start_api_argv({"DATAWRAP_GRACEFUL_SHUTDOWN_SECONDS": "7"})
    assert _flag(overridden, "--timeout-graceful-shutdown") == "7"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_sigterm_ends_an_open_job_stream_and_runs_the_teardown(tmp_path):
    (tmp_path / "opsfix_h4_app.py").write_text(APP)
    port = _free_port()
    argv = _start_api_argv()
    assert argv[:2] == ["-m", "uvicorn"] and "src.main:app" in argv
    argv[argv.index("src.main:app")] = "opsfix_h4_app:app"
    argv[argv.index("--port") + 1] = str(port)
    argv[argv.index("--host") + 1] = "127.0.0.1"
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join([str(API_ROOT), str(REPO / "packages/preflight/src"), str(tmp_path)]),
        "DATAFLOW_DATA_DIR": str(tmp_path / "data"),
        "DATAFLOW_JOB_STORE": "memory",
        "DATAFLOW_CONTROL_STORE_BACKEND": "file",
    }
    proc = subprocess.Popen(
        [sys.executable, *argv], cwd=str(API_ROOT), env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    client = None
    try:
        deadline = time.monotonic() + 60
        while True:
            try:
                client = socket.create_connection(("127.0.0.1", port), timeout=1)
                break
            except OSError:
                if proc.poll() is not None or time.monotonic() > deadline:
                    pytest.fail(f"the API did not start: {proc.communicate()[0][-2000:]}")
                time.sleep(0.2)
        client.sendall(b"GET /api/v1/connectors/jobs/job-1/stream HTTP/1.1\r\nHost: x\r\n\r\n")
        client.settimeout(15)
        received = client.recv(65536).decode(errors="replace")
        while "data:" not in received:
            received += client.recv(65536).decode(errors="replace")

        sent = time.monotonic()
        proc.send_signal(signal.SIGTERM)
        try:
            output, _ = proc.communicate(timeout=20)
        except subprocess.TimeoutExpired:
            proc.kill()
            output, _ = proc.communicate()
            pytest.fail("the API was still running 20 s after SIGTERM with one job stream open")
        took = time.monotonic() - sent
        try:
            while chunk := client.recv(65536):
                received += chunk.decode(errors="replace")
        except OSError:
            pass
    finally:
        if client is not None:
            client.close()
        if proc.poll() is None:
            proc.kill()
            proc.communicate()
    assert "TEARDOWN_RAN" in output, "the lifespan teardown (background drain) never ran"
    assert took < 5, f"shutdown took {took:.1f}s: the stream did not end on SIGTERM"
    assert "event: shutdown" in received, "the client was not told to reconnect elsewhere"


def test_installed_flag_runs_before_the_previous_handler_and_chains_it():
    from services import shutdown_signal

    seen: list[tuple[int, bool]] = []
    previous_int = signal.getsignal(signal.SIGINT)
    previous = signal.signal(signal.SIGTERM, lambda signum, _frame: seen.append((signum, shutdown_signal.requested())))
    try:
        shutdown_signal.reset()
        assert shutdown_signal.install() is True
        assert shutdown_signal.install() is True  # a second install does not chain twice
        signal.raise_signal(signal.SIGTERM)
        assert shutdown_signal.requested() is True
        assert seen == [(signal.SIGTERM, True)]
    finally:
        signal.signal(signal.SIGTERM, previous)
        signal.signal(signal.SIGINT, previous_int)
        shutdown_signal.reset()


def test_main_lifespan_installs_the_shutdown_flag():
    import inspect

    from src import main

    source = inspect.getsource(main.lifespan)
    assert "shutdown_signal" in source and ".install()" in source and ".request()" in source


def test_chart_keeps_the_graceful_shutdown_inside_the_grace_period():
    from tests._helm_lite import ChartFail, render_chart

    chart = REPO / "deploy/enterprise/helm/dataflow"
    choice = {"controlPlane": {"podLocalStoresAck": "transfer_plans,pilot_results"}}  # see Medium 5
    api = next(
        o for o in render_chart(chart, overrides=choice)
        if o.get("kind") == "Deployment" and o["metadata"]["labels"]["app.kubernetes.io/component"] == "api"
    )
    pod = api["spec"]["template"]["spec"]
    container = pod["containers"][0]
    env = {e["name"]: e.get("value") for e in container["env"]}
    graceful = int(env["DATAFLOW_GRACEFUL_SHUTDOWN_SECONDS"])
    pre_stop = int(container["lifecycle"]["preStop"]["exec"]["command"][1])
    assert 0 < graceful
    # The teardown's drain is the API's default WORKER_DRAIN_SECONDS (the chart sets none for it).
    assert "DATAFLOW_WORKER_DRAIN_SECONDS" not in env
    assert pod["terminationGracePeriodSeconds"] >= pre_stop + graceful + 25 + 5
    with pytest.raises(ChartFail, match="gracefulShutdownSeconds"):
        render_chart(chart, overrides={**choice, "api": {"gracefulShutdownSeconds": 40}})
