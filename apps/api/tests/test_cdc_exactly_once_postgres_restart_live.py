"""Live PostgreSQL → PostgreSQL CDC exactly-once across forced mid-batch restarts.

Real logical slot, real runner (``run_cdc_database_transfer``), real dest
transactions. Proves, on the destination engine itself:

1. crash after apply but before the dest COMMIT → dest rows and dest offset
   roll back together (no partial apply);
2. crash after the dest COMMIT but before the control-plane watermark / slot
   ack → the slot redelivers, the dest offset makes the redelivery a no-op;
3. the next clean run resumes and the destination holds every source row
   exactly once.

Also proves two writers racing the first offset commit for one stream cannot
both commit (PostgreSQL unique key on ``_df_cdc_eos_watermarks``).
"""

from __future__ import annotations

import logging
import socket
import sys
import threading
import uuid
from pathlib import Path

import psycopg2
import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import connectors.cdc_eos_sa as eos_sa  # noqa: E402
import connectors.cdc_eos_sql as eos_sql  # noqa: E402
from connectors.postgresql_conn import get_connection  # noqa: E402
from services.cdc_engine import ChangeBatch  # noqa: E402
from services.cdc_exactly_once import (  # noqa: E402
    REASON_CONCURRENT_COMMIT,
    WATERMARK_TABLE,
    EosCrash,
    ExactlyOnceRouteError,
)
from src.transfer.cdc_transfer import run_cdc_database_transfer  # noqa: E402
from src.transfer.models import EndpointConfig  # noqa: E402

_logger = logging.getLogger(__name__)

CFG = {
    "host": "localhost",
    "port": 5432,
    "database": "dataflow",
    "username": "dataflow",
    "password": "dataflow",
    "connection_string": "",
    "ssl": False,
}


def _logical_decoding_ready() -> bool:
    try:
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError:
        return False
    try:
        with get_connection(**CFG) as conn, conn.cursor() as cur:
            cur.execute("SHOW wal_level")
            row = cur.fetchone()
            return bool(row) and row[0] == "logical"
    except psycopg2.Error:
        return False


pytestmark = pytest.mark.skipif(
    not _logical_decoding_ready(),
    reason="PostgreSQL with wal_level=logical not reachable on localhost:5432",
)


def _exec(sql: str, params: tuple = ()) -> None:
    with get_connection(**CFG) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        conn.commit()


def _fetch(sql: str, params: tuple = ()) -> list[tuple]:
    with get_connection(**CFG) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def _dest_rows(table: str) -> list[tuple]:
    return _fetch(f"SELECT id, amount FROM {table} ORDER BY id")  # nosec B608


def _dest_offsets(table: str) -> list[tuple]:
    return _fetch(
        f"SELECT stream_key, committed_lsn, epoch FROM {WATERMARK_TABLE} "  # nosec B608
        "WHERE dest_object = %s ORDER BY stream_key",
        (table,),
    )


def _drop_slot(slot_name: str) -> None:
    if not slot_name:
        return
    _exec(
        "SELECT pg_drop_replication_slot(%s) "
        "WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = %s)",
        (slot_name, slot_name),
    )


def _run(src, dst, mappings, schema, stream, job_id):
    return run_cdc_database_transfer(
        src,
        dst,
        mappings,
        schema,
        sync_mode="cdc",
        stream_contracts=stream,
        job_id=job_id,
        limit=2,
        delivery_guarantee="exactly_once",
        delivery_pinned=True,
    )


def test_postgres_cdc_exactly_once_survives_forced_mid_batch_restarts(monkeypatch):
    src_table = "cdc_eos_rs_" + uuid.uuid4().hex[:8]
    dst_table = src_table + "_dest"
    job_id = "eos-restart-" + uuid.uuid4().hex[:8]
    slot_name = ""
    try:
        _exec(f"CREATE TABLE {src_table} (id INT PRIMARY KEY, amount NUMERIC(10,2))")
        _exec(f"INSERT INTO {src_table} (id, amount) VALUES (1, 10.00), (2, 20.00)")
        src = EndpointConfig(kind="database", format="postgresql", **CFG, schema="public", table=src_table)
        dst = EndpointConfig(kind="database", format="postgresql", **CFG, schema="public", table=dst_table)
        mappings = [
            {"source": "id", "target": "id", "source_type": "INTEGER", "target_type": "INTEGER"},
            {"source": "amount", "target": "amount", "source_type": "NUMERIC", "target_type": "NUMERIC"},
        ]
        schema = {"id": "INTEGER", "amount": "NUMERIC(10,2)"}
        stream = [{"name": src_table, "selected": True, "snapshot_mode": "initial", "primary_key": "id"}]

        rows1, _, summary1, _ = _run(src, dst, mappings, schema, stream, job_id)
        slot_name = summary1.get("cdc", {}).get("cdc_slot_name", "")
        assert rows1 == 2
        assert _dest_rows(dst_table) == [(1, 10), (2, 20)]
        offsets1 = _dest_offsets(dst_table)
        assert offsets1, "dest-owned offset row must exist after the first commit"

        _exec(f"INSERT INTO {src_table} (id, amount) VALUES (3, 30.00), (4, 40.00)")

        # Restart 1: rows applied, offset written, process dies before COMMIT.
        real_write = eos_sa._sa_write_watermark

        def write_then_die(*args, **kwargs):
            real_write(*args, **kwargs)
            raise EosCrash("after_watermark_before_commit")

        monkeypatch.setattr(eos_sa, "_sa_write_watermark", write_then_die)
        with pytest.raises(EosCrash):
            _run(src, dst, mappings, schema, stream, job_id)
        monkeypatch.undo()
        assert _dest_rows(dst_table) == [(1, 10), (2, 20)], "partial apply leaked past rollback"
        assert _dest_offsets(dst_table) == offsets1, "dest offset moved without its rows"

        # Restart 2: dest COMMIT succeeds, process dies before the runner
        # advances the control-plane watermark or acks the slot.
        def die_after_commit(**_kwargs):
            raise EosCrash("after_commit_before_ack")

        monkeypatch.setattr(eos_sql, "log_apply_outcome", die_after_commit)
        with pytest.raises(EosCrash):
            _run(src, dst, mappings, schema, stream, job_id)
        monkeypatch.undo()
        committed = _dest_rows(dst_table)
        offsets2 = _dest_offsets(dst_table)
        assert offsets2 != offsets1, "dest offset must advance with the committed batch"
        assert len(committed) == len(set(committed))

        # Restart 3: slot redelivers what was never acked; dest offset skips it.
        _exec(f"INSERT INTO {src_table} (id, amount) VALUES (5, 50.00)")
        for _ in range(3):
            _run(src, dst, mappings, schema, stream, job_id)
            if len(_dest_rows(dst_table)) == 5:
                break
        assert _dest_rows(dst_table) == [(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)], (
            "destination must hold every source row exactly once across restarts"
        )
        dup = _fetch(f"SELECT id, COUNT(*) FROM {dst_table} GROUP BY id HAVING COUNT(*) > 1")  # nosec B608
        assert dup == []
    finally:
        try:
            _drop_slot(slot_name)
        except psycopg2.Error as exc:
            _logger.debug("slot cleanup failed: %s", exc)
        if _fetch("SELECT to_regclass(%s)", (WATERMARK_TABLE,))[0][0]:
            _exec(f"DELETE FROM {WATERMARK_TABLE} WHERE dest_object = %s", (dst_table,))  # nosec B608
        _exec(f"DROP TABLE IF EXISTS {dst_table}")
        _exec(f"DROP TABLE IF EXISTS {src_table}")


def test_postgres_two_writers_cannot_both_commit_first_offset(monkeypatch):
    table = "eos_race_" + uuid.uuid4().hex[:8]
    key = f"pg|race|{table}"
    dest_cfg = {"type": "postgresql", **{k: CFG[k] for k in ("host", "port", "database", "username", "password")}}
    mappings = [
        {"source": "id", "target": "id", "confidence": 1.0},
        {"source": "v", "target": "v", "confidence": 1.0},
    ]
    # Create the target and offset tables up front so the race is only on the
    # first offset commit for ``key`` (not on concurrent DDL).
    eos_sql.apply_change_batch_exactly_once(
        dest_type="postgresql",
        dest_cfg=dest_cfg,
        dest_table=table,
        change=ChangeBatch(inserts=[{"id": "0", "v": "seed"}], updates=[], deletes=[], resume_token={"lsn": "0/100"}),
        mappings=mappings,
        column_types={"id": "string", "v": "string"},
        headers=["id", "v"],
        pk_target_cols=["id"],
        cursor_key=key + "|seed",
    )
    real_lock = eos_sa._lock_watermark
    barrier = threading.Barrier(2, timeout=20)

    def lock_then_wait(conn, dialect, stream_key):
        view = real_lock(conn, dialect, stream_key)
        barrier.wait()  # both writers have read "no offset" before either writes
        return view

    monkeypatch.setattr(eos_sa, "_lock_watermark", lock_then_wait)
    outcomes: dict[str, object] = {}

    def worker(name: str, lsn: str, row_id: str) -> None:
        try:
            outcomes[name] = eos_sql.apply_change_batch_exactly_once(
                dest_type="postgresql",
                dest_cfg=dest_cfg,
                dest_table=table,
                change=ChangeBatch(
                    inserts=[{"id": row_id, "v": name}], updates=[], deletes=[], resume_token={"lsn": lsn}
                ),
                mappings=mappings,
                column_types={"id": "string", "v": "string"},
                headers=["id", "v"],
                pk_target_cols=["id"],
                cursor_key=key,
                writer_fence=1,
            )
        except Exception as exc:  # noqa: BLE001 — recorded and asserted below
            outcomes[name] = exc

    try:
        threads = [
            threading.Thread(target=worker, args=("a", "0/A00", "1")),
            threading.Thread(target=worker, args=("b", "0/B00", "2")),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)
        errors = [v for v in outcomes.values() if isinstance(v, Exception)]
        wins = [v for v in outcomes.values() if not isinstance(v, Exception)]
        assert len(wins) == 1 and len(errors) == 1, outcomes
        assert isinstance(errors[0], ExactlyOnceRouteError)
        assert errors[0].reason == REASON_CONCURRENT_COMMIT
        rows = _fetch(f"SELECT id FROM {table} WHERE id <> '0'")  # nosec B608
        assert len(rows) == 1, "loser's rows must roll back with its offset write"
        offs = _fetch(f"SELECT committed_lsn FROM {WATERMARK_TABLE} WHERE stream_key = %s", (key,))  # nosec B608
        assert len(offs) == 1
    finally:
        monkeypatch.undo()
        _exec(f"DROP TABLE IF EXISTS {table}")
        if _fetch("SELECT to_regclass(%s)", (WATERMARK_TABLE,))[0][0]:
            _exec(
                f"DELETE FROM {WATERMARK_TABLE} WHERE stream_key IN (%s, %s)",  # nosec B608
                (key, key + "|seed"),
            )
