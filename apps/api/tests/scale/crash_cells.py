"""Crash injection mid-stream, then resume: no loss, no double-apply.

Resume is the only claim CDC cannot make on paper. An in-process exception is
not evidence, because it unwinds ``finally`` blocks and lets the engine release
its lease, flush its cursor and close its slot — the cleanup a crash is defined
by *not* getting. So the transfer runs in a child process (``crash_child``) and
the parent sends ``SIGKILL``: no signal handler, no atexit, no flush. What is
left behind — a replication slot holding WAL, a lease with a live heartbeat, a
watermark that may be ahead of or behind the rows actually landed — is exactly
the state an operator finds after a pod eviction.

The kill is aimed at a *witnessed* partial write: the parent polls the
destination on its own connection and only kills once rows are provably landing,
so the crash lands mid-stream rather than before the first byte or after the
last. Resume then has to satisfy two independent reads:

* **no loss** — destination ``COUNT(*)`` equals the source, and the content
  checksum over the mapped projection matches;
* **no double-apply** — ``COUNT(DISTINCT id)`` equals ``COUNT(*)``, so replayed
  changes were absorbed by key rather than appended a second time.

That pair is what licenses an *at-least-once with idempotent apply* claim. It is
not exactly-once: the log is re-read from the persisted cursor, so a change can
be delivered twice, and the harness records that honestly instead of upgrading
the claim because the row counts happened to match.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from tests.scale import live_engines as L
from tests.scale.cdc_cells import SHAPE, CdcRoute, cdc_summary
from tests.scale.matrix import Cell, Matrix, fill

API_DIR = Path(__file__).resolve().parents[2]

#: Rows that must be visible at the destination before the kill, so the crash is
#: provably mid-stream. Kept small relative to 100K: the point is that the child
#: is inside the write loop, not that it got far.
KILL_AFTER_ROWS = 500

#: Ceiling on waiting for the destination to show those rows. A route that
#: writes in one final commit never shows a partial, which is a fact about the
#: route and is recorded rather than retried away.
KILL_WAIT_SECONDS = float(os.getenv("DATAFLOW_SCALE_KILL_WAIT", "240"))


def _dest_rows(route: CdcRoute) -> int:
    """Destination count that tolerates the table not existing yet."""
    try:
        return route.dest_count()
    except Exception:  # noqa: BLE001 — pre-create is a count of zero
        return 0


def _dest_distinct_ids(route: CdcRoute) -> int:
    if route.dest == "postgresql":
        return int(
            L.pg_fetch(f'SELECT count(DISTINCT "id") FROM public."{route.dst_object}"')[0][0]
        )
    return int(
        L.mysql_fetch(f"SELECT count(DISTINCT `id`) FROM `{route.dst_object}`")[0][0]
    )


def spawn_child(route: CdcRoute, *, snapshot_mode: str = "initial") -> subprocess.Popen:
    """Start the transfer in its own process group so the kill is unambiguous."""
    env = dict(os.environ)
    env["PYTHONPATH"] = str(API_DIR) + os.pathsep + env.get("PYTHONPATH", "")
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "tests.scale.crash_child",
            route.dialect,
            route.dest,
            snapshot_mode,
            route.job_slug,
            route.tag,
        ],
        cwd=str(API_DIR),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    return proc


def kill_mid_stream(proc: subprocess.Popen, route: CdcRoute) -> dict[str, Any]:
    """Wait for witnessed rows at the destination, then ``SIGKILL`` the child."""
    deadline = time.time() + KILL_WAIT_SECONDS
    witnessed = 0
    while time.time() < deadline:
        if proc.poll() is not None:
            return {
                "killed": False,
                "reason": "child exited before a partial write was witnessed",
                "child_returncode": proc.returncode,
                "rows_witnessed": _dest_rows(route),
            }
        witnessed = _dest_rows(route)
        if witnessed >= KILL_AFTER_ROWS:
            break
        time.sleep(0.2)
    if witnessed < KILL_AFTER_ROWS:
        return {
            "killed": False,
            "reason": f"destination never showed {KILL_AFTER_ROWS} rows within "
            f"{KILL_WAIT_SECONDS:.0f}s — no mid-stream point to crash at",
            "rows_witnessed": witnessed,
        }
    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        pass
    return {
        "killed": True,
        "rows_witnessed": witnessed,
        # -9 proves the process died by SIGKILL and not by its own error path.
        "child_returncode": proc.returncode,
        "child_tail": (proc.stdout.read() if proc.stdout else "")[-400:],
    }


def crash_and_resume(matrix: Matrix, route: CdcRoute, rows: int) -> list[Cell]:
    """Kill a live CDC consumer mid-write, then prove the resume is clean."""
    label = f"{route.dialect}→{route.dest}"
    cell = Cell(route=label, mode="cdc crash mid-stream + resume", schema_shape=SHAPE)
    route.drop()
    cell.detail["state_reset"] = route.reset_state()
    route.seed(rows)
    cell.source_rows = route.source_count()

    proc = spawn_child(route)
    kill = kill_mid_stream(proc, route)
    cell.detail["crash"] = kill
    if not kill.get("killed"):
        cell.mark(False, str(kill.get("reason") or "crash injection did not fire"))
        matrix.add(cell)
        return [cell]

    partial = _dest_rows(route)
    cell.detail["dest_rows_after_crash"] = partial
    cell.detail["cursor_after_crash"] = route.cursor()

    # A lease whose holder is dead still looks live until its TTL lapses. Which
    # of the two happens is a product behaviour, so it is recorded, not hidden:
    # either resume works straight away, or it is refused and the operator has
    # to break the lease (the path the harness then takes).
    first = route.run(route.job_slug)
    resumed, elapsed, run_id = first
    blocked = ""
    if not resumed.success and "concurrent consumer" in str(resumed.error or "").lower():
        blocked = str(resumed.error or "")[:200]
        cell.detail["resume_blocked_by_dead_holder_lease"] = blocked
        cell.detail["operator_force_release"] = route.reset_state()["leases"]
        resumed, elapsed, run_id = route.run(route.job_slug)
    fill(cell, resumed, elapsed, run_id)
    if blocked:
        cell.notes = ""

    if resumed.success:
        cell.dest_rows = route.dest_count()
        cell.source_checksum = L.checksum(route.source_projection())
        cell.dest_checksum = L.checksum(route.dest_projection())
        distinct = _dest_distinct_ids(route)
        cell.detail["dest_distinct_ids_after_resume"] = distinct
        summary = cdc_summary(resumed)
        cell.delivery = str(summary.get("cdc_delivery") or "")
        cell.detail["cursor_after_resume"] = route.cursor()
        no_loss = cell.dest_rows == cell.source_rows and (
            cell.source_checksum == cell.dest_checksum
        )
        no_double = distinct == cell.dest_rows
        cell.mark(
            no_loss and no_double,
            f"resumed after SIGKILL at {partial} landed rows: "
            f"no loss (dest={cell.dest_rows}=src), no double-apply "
            f"(distinct ids={distinct})"
            if no_loss and no_double
            else f"dest={cell.dest_rows} src={cell.source_rows} "
            f"checksum_match={cell.source_checksum == cell.dest_checksum} "
            f"distinct_ids={distinct}",
        )
    matrix.add(cell)
    return [cell]


def crash_cells(matrix: Matrix, rows: int) -> None:
    """One crash/resume cell per CDC source engine that is actually available."""
    if L.reachable("localhost", 5432):
        crash_and_resume(
            matrix, CdcRoute("crash", dialect="postgresql", dest="postgresql", tag="x"), rows
        )
    else:
        matrix.add(
            Cell(route="postgresql→postgresql", mode="cdc crash mid-stream + resume").skip(
                "PostgreSQL 5432 unreachable"
            )
        )
    if L.reachable("localhost", 3306):
        crash_and_resume(
            matrix, CdcRoute("crash", dialect="mysql", dest="postgresql", tag="x"), rows
        )
    else:
        matrix.add(
            Cell(route="mysql→postgresql", mode="cdc crash mid-stream + resume").skip(
                "MySQL 3306 unreachable"
            )
        )
    if L.reachable("localhost", 27017) and L.mongo_replica_set():
        crash_and_resume(
            matrix, CdcRoute("crash", dialect="mongodb", dest="postgresql", tag="x"), rows
        )
    else:
        matrix.add(
            Cell(route="mongodb→postgresql", mode="cdc crash mid-stream + resume").skip(
                "MongoDB replica set unavailable: change streams impossible"
            )
        )


__all__ = ["crash_and_resume", "crash_cells", "kill_mid_stream", "spawn_child"]
