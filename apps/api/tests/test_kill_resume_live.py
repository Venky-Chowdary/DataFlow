"""Kill a transfer mid-run with SIGKILL, resume it, and lose or duplicate nothing.

Each case runs a real engine transfer in a child process
(``tests/helpers/kill_resume_child.py``) on the live local PostgreSQL /
MySQL-or-MariaDB, waits until the durable checkpoint shows committed batches,
SIGKILLs the child (no cleanup handler runs: that is the point), then starts a
second process that resumes the same job. The destination must then hold
every source row exactly once with every cell equal (NULL stays NULL, ``''``
stays ``''``), checked independently of the engine by streaming both tables
in key order (``benchmarks.db_throughput.independent_check``).

The kill lands while rows are being written: the checkpoint is polled, not
the clock. A run whose checkpoint never shows a committed batch before it
finishes fails the test instead of passing vacuously.
"""

from __future__ import annotations

import json
import os
import pickle
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

from tests.helpers.live_env import mysql_creds, mysql_up, pg_creds, pg_up

_API_ROOT = Path(__file__).resolve().parents[1]
_CHILD = _API_ROOT / "tests" / "helpers" / "kill_resume_child.py"
ROWS = int(os.environ.get("KILL_RESUME_ROWS", "6000"))
CHUNK = 250


def _engine_up(engine: str) -> bool:
    return pg_up() if engine == "pg" else mysql_up()


def _endpoint(engine: str, table: str) -> dict:
    if engine == "pg":
        c = pg_creds()
        return {"kind": "database", "format": "postgresql", "host": c["host"], "port": c["port"],
                "username": c["username"], "password": c["password"], "database": c["database"],
                "table": table}
    c = mysql_creds()
    return {"kind": "database", "format": "mysql", "host": c["host"], "port": c["port"],
            "username": c["username"], "password": c["password"], "database": c["database"],
            "table": table}


@pytest.fixture()
def bench_env(monkeypatch):
    """Point the benchmark helpers at the same live engines the test uses."""
    p, m = pg_creds(), mysql_creds()
    for key, value in {
        "PGHOST": p["host"], "PGPORT": str(p["port"]), "PGUSER": p["username"],
        "PGPASSWORD": p["password"], "PGDATABASE": p["database"],
        "MYSQL_HOST": m["host"], "MYSQL_PORT": str(m["port"]), "MYSQL_USER": m["username"],
        "MYSQL_PASSWORD": m["password"], "MYSQL_DATABASE": m["database"],
    }.items():
        monkeypatch.setenv(key, value)
    from benchmarks import db_throughput

    return db_throughput


def _committed_rows(store_path: Path, job_id: str) -> int:
    """Rows the durable checkpoint says are committed (0 when none yet)."""
    try:
        with store_path.open("rb") as fh:
            state = pickle.load(fh)  # noqa: S301 - this test's own file
    except (OSError, EOFError, pickle.UnpicklingError):
        return 0
    job = (state.get("_jobs") or {}).get(job_id) or {}
    best = 0

    def walk(node):
        nonlocal best
        if isinstance(node, dict):
            for key in ("rows_processed",):
                value = node.get(key)
                if isinstance(value, int):
                    best = max(best, value)
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(job.get("checkpoint") or {})
    return best


def _spawn(spec: dict, tmp: Path, name: str) -> tuple[subprocess.Popen, Path]:
    spec_path = tmp / f"{name}.json"
    spec_path.write_text(json.dumps(spec), encoding="utf-8")
    env = dict(os.environ)
    env["PYTHONPATH"] = str(_API_ROOT)
    data_dir = tmp / "data"
    data_dir.mkdir(exist_ok=True)
    env["DATAWRAP_DATA_DIR"] = str(data_dir)
    log = (tmp / f"{name}.log").open("wb")
    proc = subprocess.Popen(  # noqa: S603 - this repo's own helper
        [sys.executable, str(_CHILD), str(spec_path)], cwd=str(_API_ROOT), env=env,
        stdout=log, stderr=subprocess.STDOUT,
    )
    return proc, tmp / f"{name}.log"


ROUTES = [("pg", "pg"), ("mysql", "mysql"), ("pg", "mysql"), ("mysql", "pg")]
#: Append modes are the ones a plain rerun would duplicate: they prove the second
#: process resumed after the committed batches instead of starting over.
MODES = ["full_refresh_overwrite", "incremental_deduped", "full_refresh_append",
         "incremental_append"]
_APPEND_MODES = {"full_refresh_append", "incremental_append"}


def _count_writes(bench_env, engine: str, table: str, tag: str) -> str:
    """Count every row the destination receives from now on (a trigger)."""
    counter = f"kr_cnt_{tag}"
    if engine == "pg":
        bench_env._exec("pg", [
            (f"CREATE TABLE {counter} (n int)", None),
            (f"CREATE FUNCTION {counter}_f() RETURNS trigger LANGUAGE plpgsql AS "
             f"$$ BEGIN INSERT INTO {counter} VALUES (1); RETURN NULL; END $$", None),
            (f"CREATE TRIGGER {counter}_t AFTER INSERT OR UPDATE ON {table} "
             f"FOR EACH ROW EXECUTE FUNCTION {counter}_f()", None),
        ])
    else:
        bench_env._exec("mysql", [
            (f"CREATE TABLE {counter} (n int)", None),
            (f"CREATE TRIGGER {counter}_i AFTER INSERT ON {table} FOR EACH ROW "
             f"INSERT INTO {counter} VALUES (1)", None),
            (f"CREATE TRIGGER {counter}_u AFTER UPDATE ON {table} FOR EACH ROW "
             f"INSERT INTO {counter} VALUES (1)", None),
        ])
    return counter


def _scalar(bench_env, engine: str, sql: str) -> int:
    conn = bench_env._connect(engine)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return int(cur.fetchone()[0])
    finally:
        conn.close()


def _drop_counter(bench_env, engine: str, counter: str) -> None:
    stmts = [(f"DROP TABLE IF EXISTS {counter}", None)]
    if engine == "pg":
        stmts.append((f"DROP FUNCTION IF EXISTS {counter}_f() CASCADE", None))
    bench_env._exec(engine, stmts)


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("route", ROUTES, ids=lambda r: f"{r[0]}-{r[1]}")
def test_sigkill_mid_write_then_resume_loses_and_duplicates_nothing(
    route, mode, tmp_path, bench_env
):
    src_engine, dst_engine = route
    for engine in {src_engine, dst_engine}:
        if not _engine_up(engine):
            pytest.skip(f"{'PostgreSQL' if engine == 'pg' else 'MySQL'} not reachable")
    tag = uuid.uuid4().hex[:8]
    src_table, dst_table = f"kr_src_{tag}", f"kr_dst_{tag}"
    bench_env.create_source(src_engine, src_table, ROWS)
    job_id = uuid.uuid4().hex[:24]
    base = {
        "job_id": job_id,
        "store_path": str(tmp_path / "store.pickle"),
        "cursor_path": str(tmp_path / "sync_cursors.json"),
        "chunk_size": CHUNK,
        "mode": mode,
        "source": _endpoint(src_engine, src_table),
        "destination": _endpoint(dst_engine, dst_table),
        "contracts": [{"name": src_table, "cursor_field": "id", "primary_key": ["id"],
                       "sync_mode": mode, "selected": True}],
        # Pass-through filter keeps the row path (a server-side COPY has no
        # batch to kill between).
        "source_filter": {"column": "id", "op": "gt", "value": 0},
    }
    try:
        first = dict(base, result_path=str(tmp_path / "first.result"),
                     started_path=str(tmp_path / "first.started"))
        proc, log = _spawn(first, tmp_path, "first")
        store_path = Path(base["store_path"])
        deadline = time.monotonic() + 600
        killed_at = 0
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                break
            committed = _committed_rows(store_path, job_id)
            if committed >= 4 * CHUNK:
                proc.send_signal(signal.SIGKILL)
                proc.wait(timeout=30)
                killed_at = committed
                break
            time.sleep(0.05)
        else:
            proc.kill()
            pytest.fail("first run neither committed a batch nor finished in time")
        assert killed_at, (
            "the first run finished before a kill could land mid-write; "
            + log.read_text(errors="replace")[-3000:]
        )
        assert killed_at < ROWS, killed_at
        assert not Path(first["result_path"]).exists()
        counter = ""
        landed_before = 0
        resume_point = _committed_rows(store_path, job_id)  # the checkpoint the resume reads
        if mode in _APPEND_MODES:
            landed_before = _scalar(bench_env, dst_engine, f"SELECT COUNT(*) FROM {dst_table}")
            assert landed_before >= 4 * CHUNK, landed_before  # committed batches survived the kill
            counter = _count_writes(bench_env, dst_engine, dst_table, tag)

        second = dict(base, resume=True, result_path=str(tmp_path / "second.result"),
                      started_path=str(tmp_path / "second.started"))
        proc2, log2 = _spawn(second, tmp_path, "second")
        try:
            proc2.wait(timeout=900)
        except subprocess.TimeoutExpired:
            proc2.kill()
            pytest.fail("resumed run did not finish in time")
        result_path = Path(second["result_path"])
        assert result_path.exists(), log2.read_text(errors="replace")[-4000:]
        result = json.loads(result_path.read_text(encoding="utf-8"))
        assert result["success"], (result, log2.read_text(errors="replace")[-4000:])
        assert result["reconciliation_passed"] is True, result

        check = bench_env.independent_check(src_engine, src_table, dst_engine, dst_table, ROWS)
        assert check["passed"], check
        assert check["rows_compared"] == ROWS
        if counter:
            # The resumed process started at the durable checkpoint: it wrote no
            # more than the rows after it (rows that landed after the last
            # checkpoint save are re-applied through the write ledger, not
            # duplicated), never the whole table a rerun would have written.
            written = _scalar(bench_env, dst_engine, f"SELECT COUNT(*) FROM {counter}")
            assert 0 < resume_point <= landed_before, (resume_point, landed_before)
            assert ROWS - landed_before <= written <= ROWS - resume_point, (
                written, landed_before, resume_point)
    finally:
        if mode in _APPEND_MODES:
            try:
                _drop_counter(bench_env, dst_engine, f"kr_cnt_{tag}")
            except Exception:  # noqa: BLE001, S110 - cleanup only
                pass
        for engine, table in ((src_engine, src_table), (dst_engine, dst_table)):
            try:
                bench_env.drop_table(engine, table)
            except Exception:  # noqa: BLE001 - cleanup only
                pass
