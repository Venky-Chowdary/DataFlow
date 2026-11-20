"""Give each BigQuery test module a fresh goccy/bigquery-emulator process in CI.

The emulator parses SQL through go-googlesql, a WebAssembly build whose bridge
keeps memory addresses as int32. Once the process's wasm memory passes 2 GiB,
every query fails with ``googlesqlite: panic runtime error: slice bounds out of
range [-2147467216:]`` (upstream goccy/go-googlesql#10, open, no fix). Memory
grows with the work one process has done, so whichever BigQuery test runs late
in a busy shard fails, whatever it tests (PR #433 CI: shard 2, after the shards
were re-balanced).

CI starts the emulator with ``--database`` (tables live in a file) and
``--restart unless-stopped``, so restarting the container drops the parser's
memory and keeps every table. This helper restarts it before the first test of
each module whose test ids name BigQuery. It is opt-in:
``DATAFLOW_CI_BIGQUERY_CONTAINER`` names the container; unset (local runs, or no
emulator), it does nothing.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import time

_ENV = "DATAFLOW_CI_BIGQUERY_CONTAINER"
_PORT = 9050
_READY_PATH = "/bigquery/v2/projects/dataflow-test/datasets/dataflow"

_last_module: str | None = None


def _wants_restart(nodeid: str) -> bool:
    return "bigquery" in nodeid.lower()


def _serving(timeout_s: float) -> bool:
    """True once the emulator answers the tests' dataset over HTTP."""
    import urllib.request

    deadline = time.monotonic() + timeout_s
    url = f"http://127.0.0.1:{_PORT}{_READY_PATH}"
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:  # noqa: S310 - fixed loopback URL
                if resp.status == 200:
                    return True
        except OSError:
            pass
        time.sleep(0.5)
    return False


def _port_open() -> bool:
    try:
        with socket.create_connection(("127.0.0.1", _PORT), timeout=1):
            return True
    except OSError:
        return False


def before_test(nodeid: str) -> None:
    """Restart the emulator when a new BigQuery test module starts (CI only)."""
    global _last_module
    container = os.getenv(_ENV, "").strip()
    if not container or not _wants_restart(nodeid):
        return
    module = nodeid.split("::", 1)[0]
    if module == _last_module:
        return
    _last_module = module
    if not shutil.which("docker") or not _port_open():
        return
    proc = subprocess.run(  # noqa: S603 - fixed argv, container name from CI config
        ["docker", "restart", "--time", "5", container],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"could not restart {container}: {proc.stderr.strip()[:300]}")
    if not _serving(60):
        raise RuntimeError(f"{container} did not serve the tests' dataset within 60 s of a restart")
